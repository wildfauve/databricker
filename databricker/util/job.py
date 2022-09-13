import requests

from . import monad, error, fn


@monad.monadic_try(exception_test_fn=error.http_error_test_fn)
def update_job_caller(config, request):
    hdrs = {"Authorization": "Bearer {}".format(config.databrickcfg.get('DEFAULT', 'token'))}
    result = requests.post(url_for_job_update(config), json=request, headers=hdrs)
    return result


def job_id(config):
    return fn.deep_get(config.infra, ['job', 'id'])


def task(config):
    return fn.deep_get(config.infra, ['job', 'task_key'])


@monad.monadic_try(exception_test_fn=error.http_error_test_fn)
def get_job(config):
    hdrs = {"Authorization": "Bearer {}".format(config.databrickcfg.get('DEFAULT', 'token'))}
    result = requests.get(url_for_job_get(config), params={"job_id": job_id(config)}, headers=hdrs)

    return result


def url_for_job_get(config):
    return "{cluster_url}/api/2.0/jobs/get".format(cluster_url=config.infra['cluster']['url'])


def url_for_job_update(config):
    return "{cluster_url}/api/2.0/jobs/update".format(cluster_url=config.infra['cluster']['url'])


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
