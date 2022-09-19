from enum import Enum
import requests
from functools import reduce

from . import fn, error, config, monad, databricks


class ClusterType(Enum):
    NEW = 'newCluster'
    EXISTING = 'existingCluster'
    NOTAJOB = None


def cluster_type(cfg):
    return ClusterType(fn.deep_get(cfg.infra, ['cluster', 'type']))


def cluster_id(cfg):
    return fn.deep_get(cfg.infra, ['cluster', 'cluster_id'])


def cluster_ids(cfg):
    return fn.deep_get(cfg.infra, ['cluster', 'cluster_ids'])


def cluster_url(cfg):
    return fn.deep_get(cfg.infra, ['cluster', 'url'])


def new_cluster_cfg(cfg):
    return (
        fn.deep_get(cfg.infra, ['cluster', 'spark_version']),
        fn.deep_get(cfg.infra, ['cluster', 'node_type_id']),
        fn.deep_get(cfg.infra, ['cluster', 'num_workers'])
    )


def install_library(cfg) -> monad.EitherMonad:
    """
    Either store on multiple clusters or a single cluster.  The multi-cluster option is only available for cluster libs
    :param cfg:
    :return:
    """
    if cluster_id(cfg):
        return install(cfg, install_request(cfg=cfg, cid=cluster_id(cfg)))
    result = [install(cfg, install_request(cfg=cfg, cid=cid)) for cid in cluster_ids(cfg)]
    if all(map(monad.maybe_value_ok, result)):
        return monad.Right(None)
    return monad.Left(composite_install_error(result))


@monad.monadic_try(exception_test_fn=error.http_error_test_fn, error_cls=error.CliError)
def install(cfg, req):
    hdrs = {"Authorization": "Bearer {}".format(databricks.get_databricks_token(cfg))}
    result = requests.post(url_for_install(cfg), json=req, headers=hdrs)
    return result


def composite_install_error(result):
    return error.CliError(
        message=reduce(build_composite_error_msg, filter(monad.maybe_value_fail, result), ""),
        code=500, ctx={'errors': reduce(build_composite_error_ctx, filter(monad.maybe_value_fail, result), [])}
    )


def build_composite_error_msg(msg, error):
    return msg + error.error().message + "; "

def build_composite_error_ctx(ctx_list, error):
    ctx_list.append(error.error().ctx)
    return ctx_list



def url_for_install(cfg):
    return "{cluster_url}/api/2.0/libraries/install".format(cluster_url=cluster_url(cfg))


def install_request(cfg, cid):
    return {
        "cluster_id": cid,
        "libraries": [
            {
                "whl": config.dbfs_artefact(cfg)
            }
        ]
    }
