import sys

from databricker.util import config, job, cli_helpers, monad, error, env

def run():
    """
    Lists the job with the job id defined in the infra.toml file.
    """
    result = config.config_value() >> show_job
    if env.Env().env == "test":
        return result
    if not result:
        sys.exit(1)
    sys.exit(0)


def show_job(cfg) -> monad.EitherMonad:
    result = job.get_job(cfg)
    if result.is_right():
        job_config = result.value.json()
        cli_helpers.echo("SUCCESS: {}".format(job_config))
        return job_config
    cli_helpers.echo("FAILURE: {}".format(result.error().json()))
    return None

