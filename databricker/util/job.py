import requests
from enum import Enum
from functools import reduce

from . import monad, error, fn, config, artefacts, env, databricks


def update_job(cfg):
    return update_job_caller(cfg, update_job_request(cfg=cfg,
                                                     job_id=job_id(cfg),
                                                     task_key=task(cfg),
                                                     schedule=config.schedule_config(cfg)))


@monad.monadic_try(exception_test_fn=error.http_error_test_fn, error_cls=error.CliError)
def update_job_caller(cfg, req):
    hdrs = {"Authorization": "Bearer {}".format(get_databricks_token(cfg).value)}
    result = requests.post(url_for_job_update(cfg), json=req, headers=hdrs)
    return result

@monad.monadic_try(error_cls=error.ValidationError)
def get_databricks_token(cfg):
    return databricks.get_databricks_token(cfg)

@monad.monadic_try(exception_test_fn=error.http_error_test_fn, error_cls=error.CliError)
def create_job_caller(cfg, req):
    hdrs = {"Authorization": "Bearer {}".format(databricks.get_databricks_token(cfg))}
    result = requests.post(url_for_job_create(cfg), json=req, headers=hdrs)
    return result


def job_name(cfg):
    return fn.deep_get(cfg.infra, ['job', 'name'])


def job_id(cfg):
    return fn.deep_get(cfg.infra, ['job', 'id'])


def task(cfg):
    return fn.deep_get(cfg.infra, ['job', 'task_key'])


def entry_point(cfg):
    return fn.deep_get(cfg.infra, ['job', 'entry_point'])


def parameters(cfg):
    return fn.deep_get(cfg.infra, ['job', 'parameters'])


def email_notifications(cfg):
    return fn.deep_get(cfg.infra, ['emailNotifications'])


def on_failure_notification(cfg):
    return fn.deep_get(cfg.infra, ['emailNotifications', 'on_failure'])


@monad.monadic_try(exception_test_fn=error.http_error_test_fn, error_cls=error.CliError)
def get_job(cfg):
    hdrs = {"Authorization": "Bearer {}".format(cfg.databrickcfg.get('DEFAULT', 'token'))}
    result = requests.get(url_for_job_get(cfg), params={"job_id": job_id(cfg)}, headers=hdrs)

    return result


def url_for_job_get(cfg):
    return "{cluster_url}/api/2.0/jobs/get".format(cluster_url=cfg.infra['cluster']['url'])


def url_for_job_update(cfg):
    return "{cluster_url}/api/2.0/jobs/update".format(cluster_url=cfg.infra['cluster']['url'])


def url_for_job_create(cfg):
    return "{cluster_url}/api/2.0/jobs/create".format(cluster_url=cfg.infra['cluster']['url'])


def library_builder(cfg):
    libraries = [{"whl": config.dbfs_artefact(cfg)}]
    maven = artefacts.maven_artefacts(cfg)
    python = artefacts.whl_artefacts(cfg)
    if maven:
        reduce(add_maven_artefact, maven, libraries)
    if python:
        reduce(add_whl_artefact, python, libraries)
    return libraries


def tag_builder(cfg):
    tags = fn.deep_get(cfg.infra, ['job', 'tags'])
    if not tags:
        return {}
    return tags


def add_maven_artefact(libraries, artefact):
    libraries.append({'maven': {'coordinates': artefact}})
    return libraries


def add_whl_artefact(libraries, artefact):
    libraries.append({'whl': artefact})
    return libraries


def update_job_request(cfg, job_id: str, task_key: str, schedule: str = None):
    base_job = {
        "job_id": job_id,
        "new_settings": {
            "parameters": parameters(cfg),
            "tasks": [
                {
                    "task_key": task_key,
                    "libraries": library_builder(cfg)
                }
            ],
            "tags": tag_builder(cfg)
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
