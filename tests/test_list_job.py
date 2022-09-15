import pytest

from databricker.command import list_job_command, build_deploy_command, create_job_command
from databricker.util import config, job


def setup_module():
    pass


def test_list_job(existing_job_config, requests_mock):
    requests_mock.get("https://example.databricks.com/api/2.0/jobs/get",
                      json=job_list_result(),
                      status_code=200,
                      headers={'Content-Type': 'application/json; charset=utf-8'})

    result = list_job_command.run()

    assert result['job_id'] == 1


def job_list_result():
    return {'job_id': 1, 'creator_user_name': 'admin@nzsuperfund.co.nz',
            'run_as_user_name': 'admim@nzsuperfund.co.nz', 'run_as_owner': True,
            'settings': {'name': 'cbor_builder', 'existing_cluster_id': '0613-232015-oltihh6r', 'libraries': [
                {'whl': 'dbfs:/artifacts/cbor/cbor_builder/dist/cbor_builder-0.1.79-py3-none-any.whl'}],
                         'email_notifications': {'on_failure': ['admin@nzsuperfund.co.nz'],
                                                 'no_alert_for_skipped_runs': False}, 'timeout_seconds': 0,
                         'max_retries': 1, 'min_retry_interval_millis': 900000, 'retry_on_timeout': False,
                         'schedule': {'quartz_cron_expression': '0 0 * * * ?', 'timezone_id': 'UTC',
                                      'pause_status': 'UNPAUSED'},
                         'python_wheel_task': {'package_name': 'cbor_builder', 'entry_point': 'job_main',
                                               'parameters': ['--all-batches']}, 'max_concurrent_runs': 1,
                         'format': 'SINGLE_TASK'}, 'created_time': 1659471493033}


def job_create_result():
    return {
        "job_id": 1
    }
