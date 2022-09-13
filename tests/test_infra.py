import pytest

from databricker.command import list_job, build_deploy, create_job
from databricker.util import config, job


def setup_module():
    config.configure(infra_config_file="tests/fixtures/infra.toml",
                     dist="dist")


def test_infra_config():
    result = config.config_value()

    assert result.is_right

    cfg = result.value

    assert set(cfg.infra.keys()) == set(['job', 'artefacts', 'cluster', 'emailNotifications'])


def test_generates_dbfs_location_for_wheel(config_value):
    dbfs_loc = config.dbfs_artefact(config_value)

    assert "dbfs:/artifacts/job/job/dist/databricker-0.1.3-py3-none-any.whl" in dbfs_loc


def test_builds_job_update_request_with_schedule():
    schedule_config = {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}
    request = job.update_job_request(job_id="1",
                                     task_key="cbor_builder",
                                     wheel="dbfs:/location-of-artefact",
                                     schedule=schedule_config)

    expected_request = {'job_id': '1', 'new_settings': {
        'tasks': [{'task_key': 'cbor_builder', 'libraries': [{'whl': 'dbfs:/location-of-artefact'}]}],
        'schedule': {'quartz_cron_expression': '0 0 * * * ?', 'timezone_id': 'UTC', 'pause_status': 'UNPAUSED'}}}

    assert request == expected_request


def test_get_schedule_from_config(config_value):
    assert config.schedule_config(config_value) == {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}


def test_get_schedule(config_value):
    assert config_value.infra['job']['schedule'] == {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}


def test_list_job(config_value, requests_mock):
    requests_mock.get("https://adb-575697367950122.2.azuredatabricks.net/api/2.0/jobs/get",
                      json=job_list_result(),
                      status_code=200,
                      headers={'Content-Type': 'application/json; charset=utf-8'})

    result = list_job.run()

    assert result['job_id'] == 314471534377936


def test_build_deploy(config_value):
    result = build_deploy.run("patch")
    breakpoint()

@pytest.fixture
def config_value():
    return config.config_value().value


def job_list_result():
    return {'job_id': 314471534377936, 'creator_user_name': 'admin@nzsuperfund.co.nz',
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
