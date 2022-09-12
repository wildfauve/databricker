from typing import Dict, Any, List
import click
import os
import requests
import pendulum
from dataclasses import dataclass
from pathlib import Path
import glob
import subprocess
import tomli
import configparser

from .util import monad, fn, singleton


class InfraConfig(singleton.Singleton):

    def configure(self, infra_config_file: str, dist: str = "dist"):
        self.infra_config_file = infra_config_file
        self.dist = dist
        pass



@dataclass
class DataClassAbstract:
    def replace(self, key, value):
        setattr(self, key, value)
        return self


@dataclass
class ConfigValue(DataClassAbstract):
    project: Dict
    infra: Dict
    databrickcfg: Dict
    args: Dict = None
    artefact_location: str = None


@click.group()
def cli():
    pass


@click.command()
def list_job():
    """
    Lists the job with the job id defined in the infra.toml file.
    """
    config_value() >> show_job
    pass


@click.command()
@click.option("--bump", "-b", default="patch", type=click.Choice(['patch', 'minor', 'major'], case_sensitive=False),
              help="States the version update type to be passed to the poetry version command.  Default is patch")
def build_deploy(bump):
    """
    Builds and deploys the project.

    The steps are as follows:

    \b
    + Runs poetry version <bump>
    + Builds the wheel.
    + Copies the wheel to the cluster at the location defined in the infra.toml file at artefacts.root
    + Updates the job with the new artefact.
    """
    config = config_value()
    if config.is_left():
        return None
    config.value.replace('args', {'bump': bump})

    result = config >> version >> build >> copy_to_dbfs >> update_job

    if result.is_right():
        echo("Completed")
    else:
        echo("Error: {}".format(result.error()))
    pass


def show_job(config):
    result = get_job(config)
    if result.is_right():
        echo("SUCCESS: {}".format(result.value.json()))
    else:
        echo("FAILURE: {}".format(result.error().json()))
    pass


def http_error_test_fn(result) -> monad.EitherMonad:
    if result.is_left():
        return result
    if result.is_right() and result.value.status_code in [200, 201]:
        return result
    return monad.Left(result.value)


@monad.monadic_try(exception_test_fn=http_error_test_fn)
def get_job(config):
    hdrs = {"Authorization": "Bearer {}".format(config.databrickcfg.get('DEFAULT', 'token'))}
    result = requests.get(url_for_job_get(config), params={"job_id": job_id(config)}, headers=hdrs)

    return result


def version(config):
    result = run_command(["poetry", "version", config.args['bump']], message="Bump Version")
    if result.is_right():
        return monad.Right(config)
    return result


def build(config):
    current_version = config.project['tool']['poetry']['version']
    result = run_command(["poetry", "build"], message="Poetry build")
    if result.is_right():
        config.replace('project', read_project_toml())
        new_version = config.project['tool']['poetry']['version']
        echo("Existing Version: {} New Version: {}".format(current_version, new_version))
        return monad.Right(config)
    return result


def copy_to_dbfs(config):
    echo("Copy {} to DBFS Location {}".format(dist_path(config), config.infra['artefacts']['root']))
    result = run_command(["poetry",
                          "run",
                          "databricks",
                          "fs",
                          "cp",
                          dist_path(config),
                          config.infra['artefacts']['root']],
                         message="Copy to DBFS")

    if result.is_right():
        return monad.Right(config)
    return result


def update_job(config):
    echo("Update Job Artefact: {}, {}, {}".format(job_id(config), task(config), dbfs_artefact(config)))
    result = update_job_caller(config, update_job_request(job_id=job_id(config),
                                                          task_key=task(config),
                                                          wheel=dbfs_artefact(config),
                                                          schedule=cron(config)))

    if result.is_right():
        echo("Update Job Artefact Success")
        return monad.Right(config)
    echo("Update Job Artefact Failure: {}".format(result.error().json()))
    return result


@monad.monadic_try(exception_test_fn=http_error_test_fn)
def update_job_caller(config, request):
    hdrs = {"Authorization": "Bearer {}".format(config.databrickcfg.get('DEFAULT', 'token'))}
    result = requests.post(url_for_job_update(config), json=request, headers=hdrs)
    return result


def job_id(config):
    return fn.deep_get(config.infra, ['job', 'id'])


def task(config):
    return fn.deep_get(config.infra, ['job', 'task_key'])


def dist_path(config):
    return glob.glob(os.path.join(InfraConfig().dist, wheel_pattern(config)))[0]


def dist_file(config):
    return os.path.split(dist_path(config))[-1]


def wheel_pattern(config):
    return "*{}*.whl".format(fn.deep_get(config.project, ['tool', 'poetry', 'version']))


def dbfs_artefact(config):
    return "{}/{}".format(fn.deep_get(config.infra, ['artefacts', 'root']), dist_file(config))


def cron(config):
    return fn.deep_get(config.infra, ['job', 'schedule'])


def run_command(cmd: List, message: str = "") -> monad.EitherMonad:
    pipe = subprocess.Popen(" ".join(cmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    result = pipe.communicate()

    if pipe.returncode == 0:
        echo("SUCCESS: {}".format(message))
        return monad.Right(None)
    echo("FAILURE: {}".format(message))
    echo("FAILURE: {}".format(result[0].decode()))
    return monad.Left("Failure executing command {}".format(" ".join(cmd)))


def config_value():
    # infra = tomli.loads(Path().joinpath("_infra", "infra.toml").read_text(encoding="utf-8"))
    infra = tomli.loads(Path().joinpath(InfraConfig().infra_config_file).read_text(encoding="utf-8"))
    dbcfg = read_databricks_config(databricks_config_path(infra))
    if dbcfg.is_left():
        echo("FAILURE: reading databricks config")
        return monad.Left("FAILURE: reading databricks config")
    return monad.Right(ConfigValue(project=read_project_toml(),
                                   infra=infra,
                                   databrickcfg=dbcfg.value))


@monad.monadic_try()
def read_databricks_config(path):
    dbcfg = configparser.ConfigParser()
    dbcfg.read(path)
    return dbcfg


def read_project_toml():
    return tomli.loads(Path("pyproject.toml").read_text(encoding="utf-8"))


def databricks_config_path(infra):
    if os.environ.get('HOME', None):
        home_path = os.environ.get('HOME', None)
    elif os.environ.get('HOMEPATH', None):
        home_path = os.environ.get('HOMEPATH', None)
    else:
        ""
    return os.path.join(home_path, ".databrickscfg")


def echo(message: Any):
    formatted_time = pendulum.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    formatted_message = f"[infra][{formatted_time}] {message}"
    click.echo(formatted_message)


def update_job_request(job_id: str, task_key: str, wheel: str, schedule: str = None):
    base_job = {
        "job_id": job_id,
        "new_settings": {
            "tasks": [
                {
                    "task_key": task_key,
                    "libraries": [
                        {
                            "whl": wheel
                        }
                    ]
                }
            ]
        }
    }
    if schedule:
        base_job['new_settings']['schedule'] = build_schedule_config(schedule)
    return base_job


def build_schedule_config(schedule: dict) -> dict:
    return {
        "quartz_cron_expression": schedule['cron'],
        "timezone_id": schedule['tz'],
        "pause_status": schedule['pause_status']
    }


def url_for_job_get(config):
    return "{cluster_url}/api/2.0/jobs/get".format(cluster_url=config.infra['cluster']['url'])


def url_for_job_update(config):
    return "{cluster_url}/api/2.0/jobs/update".format(cluster_url=config.infra['cluster']['url'])


cli.add_command(list_job)
cli.add_command(build_deploy)

def init_cli():
    cli()
