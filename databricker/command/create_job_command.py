from typing import Dict, Tuple
from functools import reduce
import sys

from . import actions
from databricker.util import config, job, artefacts, cli_helpers, monad, value, cluster, env, error
from databricker.validator import validator


def run(bump, no_version=False, profile: str = "DEFAULT"):
    """
    Creates a new job if not already created
    """
    cfg = config.config_value()
    if cfg.is_left():
        cli_helpers.echo("Unable to load the configurations.")
        return None

    cfg.value.replace('args', {'bump': bump, 'no_version': no_version, 'profile': profile})

    result = (cfg
              >> idempotent_check
              >> actions.validate_token_config
              >> create_validator
              >> test_artefact_folder_exists
              >> version
              >> build
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

def idempotent_check(cfg):
    job_id = job.job_id(cfg)
    if job_id:
        cli_helpers.echo("Job found in toml file with ID: {}".format(job_id))
        return monad.Left("Job is already created with ID: {}. To fix delete the job first.".format(job_id))
    return monad.Right(cfg)

def create_validator(cfg):
    if cluster.cluster_type(cfg) == cluster.ClusterType.NEW:
        result = validator.new_job_new_cluster_validator(cfg)
    else:
        result = validator.new_job_existing_cluster_validator(cfg)
    if result.is_right():
        cli_helpers.echo("Infra File Validated OK")
        return monad.Right(cfg)
    return result

def test_artefact_folder_exists(cfg):
    exists = artefacts.check_folder_exists(artefacts.artefacts_root(cfg))
    if exists.is_left():
        cli_helpers.echo(f"Cant find artefacts folder: {artefacts.artefacts_root(cfg)}")
        return monad.Left(f"Artefact folder root doesnt exists, create before rerunning: {artefacts.artefacts_root(cfg)}")
    return monad.Right(cfg)

def version(cfg):
    return actions.version_artefact(cfg)

def build(cfg):
    return actions.build_artefact(cfg)



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

    create_req = reduce(lambda req, job_fn: job_fn(cfg, req), request_builder_fns(), job.create_job_base_request(cfg))
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
    return [job.add_notifications,
            job.add_task,
            job.configure_cluster,
            job.add_libraries,
            job.add_tags]
