import sys

from databricker.util import config, job, cli_helpers, monad, cluster, env, error
from databricker.validator import validator


def run(bump, no_version=False):
    """
    Builds and deploys the project.

    The steps are as follows:

    \b
    + Runs poetry version <bump>
    + Builds the wheel.
    + Copies the wheel to the cluster at the location defined in the infra.toml file at artefacts.root
    + Updates the job with the new artefact.
    """
    cfg = config.config_value()
    if cfg.is_left():
        return None

    cfg.value.replace('args', {'bump': bump, 'no_version': no_version})

    result = cfg >> build_pipeline

    if result.is_right():
        cli_helpers.echo("Completed")
        if env.Env().env == "test":
            return result
        sys.exit(0)

    cli_helpers.echo("Error: {}".format(result.error()))
    if env.Env().env == "test":
        return result
    sys.exit(1)


def build_pipeline(cfg):
    return pipeline_fn(config.pipeline_type(cfg))(cfg)


def pipeline_fn(pipeline_type):
    pipeline_map = {
        config.PipelineType.LIB: build_deploy_library,
        config.PipelineType.CLUSTERLIB: build_deploy_cluster_library,
        config.PipelineType.JOB: build_deploy_job,
        config.PipelineType.NOOP: noop_build_deploy
    }
    return pipeline_map[pipeline_type]


def build_deploy_library(cfg):
    cli_helpers.echo("Building and Deploying Library")
    return monad.Right(cfg) >> lib_validator >> version >> build >> copy_to_dbfs


def build_deploy_cluster_library(cfg):
    cli_helpers.echo("Building and Deploying Cluster Library")
    return monad.Right(cfg) >> cluster_lib_validator >> version >> build >> copy_to_dbfs >> install_on_cluster


def build_deploy_job(cfg):
    cli_helpers.echo("Building and Deploying Library")
    return monad.Right(cfg) >> job_validator >> version >> build >> copy_to_dbfs >> update_job


def noop_build_deploy(cfg):
    cli_helpers.echo(
        "The infra toml does not contain a recognised deployment type, must be a job, library or cluster-library")
    return monad.Right(None)


def job_validator(cfg):
    result = validator.existing_job_validator(cfg)
    if result.is_right():
        cli_helpers.echo("Infra File Validated OK")
        return monad.Right(cfg)
    return result


def cluster_lib_validator(cfg):
    result = validator.cluster_library_validator(cfg)
    if result.is_right():
        cli_helpers.echo("Infra File Validated OK")
        return monad.Right(cfg)
    return result


def lib_validator(cfg):
    result = validator.library_validator(cfg)
    if result.is_right():
        cli_helpers.echo("Infra File Validated OK")
        return monad.Right(cfg)
    return result


def version(cfg):
    if args_switch_check(cfg, 'no_version', False):
        cli_helpers.echo("No Version update performed")
        return monad.Right(cfg)
    result = cli_helpers.run_command(["poetry", "version", cfg.args['bump']], message="Bump Version")
    if result.is_right():
        return monad.Right(cfg)
    return result


def build(cfg):
    current_version = cfg.project['tool']['poetry']['version']
    result = cli_helpers.run_command(["poetry", "build"], message="Poetry build")
    if result.is_right():
        cfg.replace('project', config.read_project_toml())
        new_version = cfg.project['tool']['poetry']['version']
        cli_helpers.echo("Existing Version: {} New Version: {}".format(current_version, new_version))
        return monad.Right(cfg)
    return result


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


def update_job(cfg):
    cli_helpers.echo(
        "Update Job Artefact: {}, {}, {}".format(job.job_id(cfg), job.task(cfg), config.dbfs_artefact(cfg)))

    result = job.update_job(cfg)

    if result.is_right():
        cli_helpers.echo("Update Job Artefact Success")
        return monad.Right(cfg)
    cli_helpers.echo(f"Update Job Artefact Failure: {error.error_message(result)}", ctx=error.error_ctx(result))
    return result


def install_on_cluster(cfg):
    cli_helpers.echo(
        "Install library on cluster: {}, {}".format(cluster.cluster_id(cfg),
                                                    config.dbfs_artefact(cfg)))

    result = cluster.install_library(cfg)

    if result.is_right():
        cli_helpers.echo("Library Installed on Cluster")
        return monad.Right(cfg)
    cli_helpers.echo(f"Library installation failed: {error.error_message(result)}", ctx=error.error_ctx(result))
    return result


def args_switch_check(cfg, bool_arg_name, missing_is: bool = True):
    bool_arg = cfg.args.get(bool_arg_name)
    if bool_arg:
        return True
    return missing_is
