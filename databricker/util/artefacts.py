from . import fn, cli_helpers, monad

def check_folder_exists(folder_name):
    return cli_helpers.run_command(['databricks', 'fs', 'ls', folder_name], return_result=True)


def maven_artefacts(cfg):
    return fn.deep_get(cfg.infra, ['artefacts', 'maven_artefacts'])


def whl_artefacts(cfg):
    return fn.deep_get(cfg.infra, ['artefacts', 'whl_artefacts'])


def artefacts_root(cfg):
    return fn.deep_get(cfg.infra, ['artefacts', 'root'])
