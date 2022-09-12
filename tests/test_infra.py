import pytest

from databricker import infra

def setup_module():
    infra.InfraConfig().configure(infra_config_file="tests/fixtures/infra.toml",
                                  dist="dist")

def test_infra_config():
    result = infra.config_value()

    assert result.is_right

    config = result.value

    assert list(config.infra.keys()) == ['job', 'artefacts', 'cluster']


def test_generates_dbfs_location_for_wheel(config_value):
    dbfs_loc = infra.dbfs_artefact(config_value)

    assert "dbfs:/artifacts/job/job/dist/databricker-0.1.1-py3-none-any.whl" in dbfs_loc


def test_builds_job_update_request_with_schedule():
    schedule_config = {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}
    request = infra.update_job_request(job_id="1",
                                     task_key="cbor_builder",
                                     wheel="dbfs:/location-of-artefact",
                                     schedule=schedule_config)

    expected_request = {'job_id': '1', 'new_settings': {
        'tasks': [{'task_key': 'cbor_builder', 'libraries': [{'whl': 'dbfs:/location-of-artefact'}]}],
        'schedule': {'quartz_cron_expression': '0 0 * * * ?', 'timezone_id': 'UTC', 'pause_status': 'UNPAUSED'}}}

    assert request == expected_request

def test_get_schedule_from_config(config_value):
    assert infra.cron(config_value) == {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}


def test_get_schedule(config_value):
    assert config_value.infra['job']['schedule'] == {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}



@pytest.fixture
def config_value():
    return infra.config_value().value
