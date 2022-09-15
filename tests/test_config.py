from databricker.util import config


def test_infra_config(existing_job_config):
    result = config.config_value()

    assert result.is_right

    cfg = result.value

    assert set(cfg.infra.keys()) == set(['job', 'artefacts', 'cluster', 'emailNotifications'])


def test_generates_dbfs_location_for_wheel(existing_job_config, config_value):
    dbfs_loc = config.dbfs_artefact(config_value)

    assert "dbfs:/artifacts/job/job/dist/databricker" in dbfs_loc


def test_get_schedule_from_config(existing_job_config, config_value):
    assert config.schedule_config(config_value) == {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}


def test_get_schedule(existing_job_config, config_value):
    assert config_value.infra['job']['schedule'] == {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}
