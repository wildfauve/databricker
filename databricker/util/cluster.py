from enum import Enum
import requests

from . import fn, error, config, monad


class ClusterType(Enum):
    NEW = 'newCluster'
    EXISTING = 'existingCluster'


def cluster_type(cfg):
    return ClusterType(fn.deep_get(cfg.infra, ['cluster', 'type']))


def cluster_id(cfg):
    return fn.deep_get(cfg.infra, ['cluster', 'cluster_id'])


def cluster_url(cfg):
    return fn.deep_get(cfg.infra, ['cluster', 'url'])


def new_cluster_cfg(cfg):
    return (
        fn.deep_get(cfg.infra, ['cluster', 'spark_version']),
        fn.deep_get(cfg.infra, ['cluster', 'node_type_id']),
        fn.deep_get(cfg.infra, ['cluster', 'num_workers'])
    )


def install_library(cfg):
    return install(cfg, install_request(cfg=cfg))


@monad.monadic_try(exception_test_fn=error.http_error_test_fn)
def install(cfg, req):
    hdrs = {"Authorization": "Bearer {}".format(cfg.databrickcfg.get('DEFAULT', 'token'))}
    result = requests.post(url_for_install(cfg), json=req, headers=hdrs)
    return result


def url_for_install(cfg):
    return "{cluster_url}/api/2.0/libraries/install".format(cluster_url=cluster_url(cfg))


def install_request(cfg):
    return {
        "cluster_id": cluster_id(cfg),
        "libraries": [
            {
                "whl": config.dbfs_artefact(cfg)
            }
        ]
    }
