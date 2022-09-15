from databricker.util import config, job, cli_helpers, monad, fn


def run(bump, no_version=False, no_job=False):
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
    cfg.value.replace('args', {'bump': bump, 'no_version': no_version, 'no_job': no_job})

    result = cfg >> version >> build >> copy_to_dbfs >> update_job

    if result.is_right():
        cli_helpers.echo("Completed")
        return result

    cli_helpers.echo("Error: {}".format(result.error()))
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
    if args_switch_check(cfg, 'no_job', False):
        cli_helpers.echo("No Job Update performed as this is a library")
        return monad.Right(None)

    cli_helpers.echo(
        "Update Job Artefact: {}, {}, {}".format(job.job_id(cfg), job.task(cfg), config.dbfs_artefact(cfg)))

    result = job.update_job(cfg)

    if result.is_right():
        cli_helpers.echo("Update Job Artefact Success")
        return monad.Right(cfg)
    cli_helpers.echo("Update Job Artefact Failure: {}".format(result.error().json()))
    return result


def args_switch_check(cfg, bool_arg_name, missing_is: bool = True):
    bool_arg = cfg.args.get(bool_arg_name)
    if bool_arg:
        return True
    return missing_is
