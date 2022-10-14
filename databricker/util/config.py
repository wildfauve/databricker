import tomli
import tomli_w
from pathlib import Path
import configparser
import os
import glob
from enum import Enum

from . import value, monad, cli_helpers, fn, error

class PipelineType(Enum):
    JOB = 'job'
    LIB = 'library'
    CLUSTERLIB = 'cluster_library'
    NOOP = "noop"


def pipeline_type(cfg):
    if not set(['job', 'library', 'cluster_library']).intersection(set(cfg.infra.keys())):
        return PipelineType('noop')
    if 'job' in cfg.infra.keys():
        return PipelineType('job')
    if 'cluster_library' in cfg.infra.keys():
        return PipelineType('cluster_library')
    if 'library' in cfg.infra.keys():
        return PipelineType('library')
    return PipelineType('noop')

def configure(infra_config_file, pyproject="pyproject.toml" ,dist="dist"):
    value.InfraConfig().configure(infra_config_file=infra_config_file, pyproject= pyproject, dist=dist)
    pass


def infra_config():
    return value.InfraConfig()


def config_value():
    if not infra_config().configured():
        infra_config().configure()
    infra = tomli.loads(Path().joinpath(value.InfraConfig().infra_config_file).read_text(encoding="utf-8"))
    dbcfg = read_databricks_config(databricks_config_path(infra))
    if dbcfg.is_left():
        cli_helpers.echo("FAILURE: reading databricks config")
        return monad.Left("FAILURE: reading databricks config")
    return monad.Right(value.ConfigValue(project=read_project_toml(),
                                         infra=infra,
                                         databrickcfg=dbcfg.value))


def read_project_toml():
    return tomli.loads(Path().joinpath(value.InfraConfig().pyproject).read_text(encoding="utf-8"))


def update_infra_job_id(cfg, job_id):
    cfg.infra['job']['id'] = job_id
    write_infra_toml(cfg.infra)


def write_infra_toml(toml: dict):
    with open(infra_config().infra_config_file, mode="wb") as fp:
        tomli_w.dump(toml, fp)


@monad.monadic_try(error_cls=error.CliError)
def read_databricks_config(path):
    dbcfg = configparser.ConfigParser()
    dbcfg.read(path)
    return dbcfg


def databricks_config_path(infra):
    if os.environ.get('HOME', None):
        home_path = os.environ.get('HOME', None)
    elif os.environ.get('HOMEPATH', None):
        home_path = os.environ.get('HOMEPATH', None)
    else:
        ""
    return os.path.join(home_path, ".databrickscfg")


def schedule_config(config):
    return fn.deep_get(config.infra, ['job', 'schedule'])


def dist_path(cfg):
    return glob.glob(os.path.join(infra_config().dist, wheel_pattern(cfg)))[0]


def dist_file(cfg):
    return os.path.split(dist_path(cfg))[-1]


def wheel_pattern(cfg):
    return "*{}*.whl".format(fn.deep_get(cfg.project, ['tool', 'poetry', 'version']))


def dbfs_artefact(cfg):
    return "{}/{}".format(fn.deep_get(cfg.infra, ['artefacts', 'root']), dist_file(cfg))
