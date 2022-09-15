from . import fn

def maven_artefacts(cfg):
    return fn.deep_get(cfg.infra, ['artefacts', 'maven_artefacts'])


def whl_artefacts(cfg):
    return fn.deep_get(cfg.infra, ['artefacts', 'whl_artefacts'])

