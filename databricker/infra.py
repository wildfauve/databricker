import click


from databricker.command import list_job_command, create_job_command, build_deploy_command
from databricker.util import config


@click.group()
def cli():
    pass


@click.command()
def list_job():
    """
    Lists the job with the job id defined in the infra.toml file.
    """
    list_job_command.run()
    pass

@click.command()
def create_job():
    """
    Creates a Spark job from the configuration provided in the infra.toml
    """
    create_job_command.run()
    pass


@click.command()
@click.option("--bump", "-b", default="patch", type=click.Choice(['patch', 'minor', 'major'], case_sensitive=False),
              help="States the version update type to be passed to the poetry version command.  Default is patch")
@click.option("--no-version", "-n", 'no_version', flag_value="no_version", default=False,
              help="Don't version the artefact.  This assumes that the version has already been updated")
def build_deploy(bump, no_version):
    """
    Builds and deploys the project.

    The steps are as follows:

    \b
    + Runs poetry version <bump>
    + Builds the wheel.
    + Copies the wheel to the cluster at the location defined in the infra.toml file at artefacts.root
    + Updates the job with the new artefact.
    """
    build_deploy_command.run(bump, no_version)
    pass

def configurator():
    return config.configure

cli.add_command(list_job)
cli.add_command(build_deploy)
cli.add_command(create_job)

def init_cli():
    cli()
