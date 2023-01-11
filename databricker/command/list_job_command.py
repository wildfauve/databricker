import sys
import json

from . import actions
from databricker.util import config, job, cli_helpers, monad, error, env, cluster
from databricker.validator import validator


def run(profile: str = "DEFAULT"):
    """
    Lists the job with the job id defined in the infra.toml file.
    """
    cfg = config.config_value()
    if cfg.is_left():
        cli_helpers.echo("Unable to load the configurations.")
        return None

    cfg.value.replace('args', {'profile': profile})

    result = cfg >> actions.validate_token_config >> validate_job >> show_job
    if env.Env().env == "test":
        return result
    if not result:
        sys.exit(1)
    sys.exit(0)


def validate_job(cfg):
    if cluster.cluster_type(cfg) == cluster.ClusterType.NOTAJOB:
        cli_helpers.echo("This app is not a job.  Cant list the job")
        return monad.Left(error.CliError("Not a Job"))
    result = validator.existing_job_validator(cfg)
    if result.is_right():
        cli_helpers.echo("Infra File Validated OK")
        return monad.Right(cfg)
    return result


def show_job(cfg) -> monad.EitherMonad:
    result = job.get_job(cfg)
    if result.is_right():
        job_config = result.value.json()
        cli_helpers.echo("SUCCESS: {}".format(json.dumps(job_config, indent=4)))
        return job_config
    cli_helpers.echo(f"List Job Failure: {error.error_message(result)}", ctx=error.error_ctx(result))
    return result
