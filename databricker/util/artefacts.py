from . import fn, cli_helpers, config


def check_folder_exists(folder_name):
    return cli_helpers.run_command(['databricks', 'fs', 'ls', folder_name], return_result=True)


def build():
    return cli_helpers.run_command(["poetry", "build"], message="Poetry build")


def version(cfg):
    return cli_helpers.run_command(["poetry", "version", cfg.args['bump']], message="Bump Version")


def copy_to_dbfs(cfg):
    return cli_helpers.run_command(["poetry",
                                    "run",
                                    "databricks",
                                    "fs",
                                    "cp",
                                    config.dist_path(cfg),
                                    cfg.infra['artefacts']['root'],
                                    f"--profile {cfg.args['profile']}"],
                                   message="Copy to DBFS")


def maven_artefacts(cfg):
    return fn.deep_get(cfg.infra, ['artefacts', 'maven_artefacts'])


def whl_artefacts(cfg):
    return fn.deep_get(cfg.infra, ['artefacts', 'whl_artefacts'])


def artefacts_root(cfg):
    return fn.deep_get(cfg.infra, ['artefacts', 'root'])
