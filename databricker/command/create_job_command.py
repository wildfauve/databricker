from typing import Dict, Tuple
from functools import reduce
import sys

from databricker.util import config, job, artefacts, cli_helpers, monad, value, cluster, env, error
from databricker.validator import validator


def run():
    """
    Creates a new job if not already created
    """
    cfg = config.config_value()
    if cfg.is_left():
        cli_helpers.echo("Unable to load the configurations.")
        return None

    result = (cfg
              >> idempotent_check
              >> create_validator
              >> test_artefact_folder_exists
              >> copy_to_dbfs
              >> build_job_request
              >> create
              >> update_infra_toml)

    if result.is_right():
        cli_helpers.echo("Completed")
        if env.Env().env == "test":
            return result
        sys.exit(0)

    cli_helpers.echo("Error: {}".format(result.error()))
    if env.Env().env == "test":
        return result
    sys.exit(1)

def create_validator(cfg):
    if cluster.cluster_type(cfg) == cluster.ClusterType.NEW:
        result = validator.new_job_new_cluster_validator(cfg)
    else:
        result = validator.new_job_existing_cluster_validator(cfg)
    if result.is_right():
        cli_helpers.echo("Infra File Validated OK")
        return monad.Right(cfg)
    return result

def idempotent_check(cfg):
    job_id = job.job_id(cfg)
    if job_id:
        cli_helpers.echo("Job found in toml file with ID: {}".format(job_id))
        return monad.Left("Job is already created with ID: {}. To fix delete the job first.".format(job_id))
    return monad.Right(cfg)

def test_artefact_folder_exists(cfg):
    exists = artefacts.check_folder_exists(artefacts.artefacts_root(cfg))
    if exists.is_left():
        cli_helpers.echo(f"Cant find artefacts folder: {artefacts.artefacts_root(cfg)}")
        return monad.Left(f"Artefact folder root doesnt exists, create before rerunning: {artefacts.artefacts_root(cfg)}")
    return monad.Right(cfg)

def copy_to_dbfs(cfg):
    cli_helpers.echo("Copy {} to DBFS Location {}".format(config.dist_path(cfg), cfg.infra['artefacts']['root']))
    result = cli_helpers.run_command(["poetry",
                                      "run",
                                      "databricks",
                                      "fs",
                                      "cp",
                                      config.dist_path(cfg),
                                      cfg.infra['artefacts']['root']],
                                     message="Copy to DBFS")

    if result.is_right():
        return monad.Right(cfg)
    return result


def build_job_request(cfg) -> Tuple[value.ConfigValue, Dict]:
    cli_helpers.echo("Building Create Job Request")

    create_req = reduce(lambda req, job_fn: job_fn(cfg, req), request_builder_fns(), create_job_base_request(cfg))
    return monad.Right((cfg, create_req))


def create(cfg_req_tuple: Tuple[value.ConfigValue, Dict]) -> Tuple[value.ConfigValue, Dict, Dict]:
    cfg, req = cfg_req_tuple
    cli_helpers.echo(
        "Creating Job: {}, {}".format(job.job_name(cfg), job.task(cfg)))

    result = job.create_job_caller(cfg, req)

    if result.is_right():
        cli_helpers.echo("Create Job Success, with new job id: {}".format(result.value.json()['job_id']))
        return monad.Right((cfg, req, result.value.json()))
    cli_helpers.echo(f"Create Job Failure: {error.error_message(result)}", ctx=error.error_ctx(result))
    return result


def update_infra_toml(cfg_req_job_tuple: Tuple[value.ConfigValue, Dict, Dict]):
    cli_helpers.echo("Updating infra tomli with job id")
    cfg, req, created_job = cfg_req_job_tuple

    config.update_infra_job_id(cfg, created_job['job_id'])
    return monad.Right(cfg_req_job_tuple)


def request_builder_fns():
    return [add_notifications,
            add_task,
            configure_cluster,
            add_libraries,
            add_tags]


def add_notifications(cfg, req):
    if job.on_failure_notification(cfg):
        req['email_notifications']['on_failure'] = [job.on_failure_notification(cfg)]
    return req


def add_task(cfg, req):
    cli_helpers.echo("Building Task Configuration")

    task = {
        "task_key": job.task(cfg),
        "python_wheel_task": {
            "package_name": job.job_name(cfg),
            "entry_point": job.entry_point(cfg),
            "parameters": job.parameters(cfg)
        },
        "timeout_seconds": 0,
        "email_notifications": {}
    }
    req['tasks'] = [task]
    return req


def configure_cluster(cfg, req):
    if cluster.cluster_type(cfg) == cluster.ClusterType.NEW:
        cli_helpers.echo("Building Cluster Configuration: NewCluster")

        spark_version, node_type, num_workers = cluster.new_cluster_cfg(cfg)
        cluster_cfg = {
            "spark_version": spark_version,
            "node_type_id": node_type,
            "num_workers": int(num_workers)
        }
        req['tasks'][0]['new_cluster'] = cluster_cfg
        return req
    req['tasks'][0]['existing_cluster_id'] = cluster.cluster_id(cfg)
    return req


def add_libraries(cfg, req):
    """
    "libraries": [
        {
            "whl": "dbfs:/artifacts/cbor/cbor_builder/dist/cbor_builder-0.1.79-py3-none-any.whl"
        },
        {
            "maven": {"coordinates": "com.azure.cosmos.spark:azure-cosmos-spark_3-2_2-12:4.12.1"}
        }
    ],
    """
    libs = job.library_builder(cfg)
    req['tasks'][0]['libraries'] = libs
    return req

def add_tags(cfg, req):
    req['tags'] = job.tag_builder(cfg)
    return req

def create_job_base_request(cfg):
    return {
        "name": job.job_name(cfg),
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
        ],
        "format": "SINGLE_TASK"
    }
