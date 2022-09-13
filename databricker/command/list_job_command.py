from databricker.util import config, job, cli_helpers

def run():
    """
    Lists the job with the job id defined in the infra.toml file.
    """
    return config.config_value() >> show_job


def show_job(cfg) -> dict:
    result = job.get_job(cfg)
    if result.is_right():
        job_config = result.value.json()
        cli_helpers.echo("SUCCESS: {}".format(job_config))
        return job_config
    else:
        cli_helpers.echo("FAILURE: {}".format(result.error().json()))
    pass

