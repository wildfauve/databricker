from . import env

def get_databricks_token(cfg):
    """
    Defaults to obtaining the token from the DATABRICKS_TOKEN env var, which is most likely the case in a CI pipeline.
    Otherwise, it assumes there is a local .databrickscfg file
    :param cfg:
    :return:
    """
    token_from_env = env.Env().databricks_token()
    if token_from_env:
        return token_from_env
    return cfg.databrickcfg.get(cfg.args['profile'], 'token')
