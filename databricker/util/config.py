import tomli
from pathlib import Path
import configparser
import os
import glob

from . import value, monad, cli_helpers, fn


def configure(infra_config_file, dist="dist"):
    value.InfraConfig().configure(infra_config_file=infra_config_file, dist=dist)
    pass

def infra_config():
    return value.InfraConfig()

def config_value():
    infra = tomli.loads(Path().joinpath(value.InfraConfig().infra_config_file).read_text(encoding="utf-8"))
    dbcfg = read_databricks_config(databricks_config_path(infra))
    if dbcfg.is_left():
        cli_helpers.echo("FAILURE: reading databricks config")
        return monad.Left("FAILURE: reading databricks config")
    return monad.Right(value.ConfigValue(project=read_project_toml(),
                                         infra=infra,
                                         databrickcfg=dbcfg.value))


def read_project_toml():
    return tomli.loads(Path("pyproject.toml").read_text(encoding="utf-8"))


@monad.monadic_try()
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
