import pytest

from databricker.command import list_job_command, build_deploy_command, create_job_command
from databricker.util import config, job


def setup_module():
    pass

def test_infra_config(existing_job_config):
    result = config.config_value()

    assert result.is_right

    cfg = result.value

    assert set(cfg.infra.keys()) == set(['job', 'artefacts', 'cluster', 'emailNotifications'])


def test_generates_dbfs_location_for_wheel(existing_job_config, config_value):
    dbfs_loc = config.dbfs_artefact(config_value)

    assert "dbfs:/artifacts/job/job/dist/databricker-0.1.9-py3-none-any.whl" in dbfs_loc


def test_builds_job_update_request_with_schedule(existing_job_config):
    schedule_config = {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}
    request = job.update_job_request(job_id="1",
                                     task_key="cbor_builder",
                                     wheel="dbfs:/location-of-artefact",
                                     schedule=schedule_config)

    expected_request = {'job_id': '1', 'new_settings': {
        'tasks': [{'task_key': 'cbor_builder', 'libraries': [{'whl': 'dbfs:/location-of-artefact'}]}],
        'schedule': {'quartz_cron_expression': '0 0 * * * ?', 'timezone_id': 'UTC', 'pause_status': 'UNPAUSED'}}}

    assert request == expected_request


def test_get_schedule_from_config(existing_job_config, config_value):
    assert config.schedule_config(config_value) == {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}


def test_get_schedule(existing_job_config, config_value):
    assert config_value.infra['job']['schedule'] == {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}


def test_list_job(existing_job_config, requests_mock):
    requests_mock.get("https://adb-575697367950122.2.azuredatabricks.net/api/2.0/jobs/get",
                      json=job_list_result(),
                      status_code=200,
                      headers={'Content-Type': 'application/json; charset=utf-8'})

    result = list_job_command.run()

    assert result['job_id'] == 1


# def test_build_deploy(config_value):
#     result = build_deploy_command.run("patch")
#     breakpoint()

def test_create_job_fails_idempotent_check(existing_job_config):
    result = create_job_command.run()

    assert result.is_left()
    assert result.error() == "Job is already created with ID: 314471534377936. To fix delete the job first."

def test_create_job(new_job_config, requests_mock, mocker):
    requests_mock.post("https://adb-575697367950122.2.azuredatabricks.net/api/2.0/jobs/create",
                       json=job_create_result(),
                       status_code=200,
                       headers={'Content-Type': 'application/json; charset=utf-8'})

    mocker.patch('databricker.util.config.write_infra_toml', return_value=None)

    result = create_job_command.run()

    assert result.is_right()


def test_create_job_with_existing_cluster(existing_cluster_job_config, requests_mock, mocker):
    requests_mock.post("https://adb-575697367950122.2.azuredatabricks.net/api/2.0/jobs/create",
                       json=job_create_result(),
                       status_code=200,
                       headers={'Content-Type': 'application/json; charset=utf-8'})

    mocker.patch('databricker.util.config.write_infra_toml', return_value=None)

    result = create_job_command.run()

    assert result.is_right()

    _, create_job_req, _ = result.value

    assert create_job_req['tasks'][0]['existing_cluster_id'] == "0914-001041-jbnfazlx"


@pytest.fixture
def config_value():
    return config.config_value().value

@pytest.fixture
def existing_job_config():
    config.configure(infra_config_file="tests/fixtures/infra.toml",
                     dist="dist")
    pass

@pytest.fixture
def new_job_config():
    config.configure(infra_config_file="tests/fixtures/infra_new.toml",
                     dist="dist")
    pass


@pytest.fixture
def existing_cluster_job_config():
    config.configure(infra_config_file="tests/fixtures/infra_existing_cluster.toml",
                     dist="dist")
    pass


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
