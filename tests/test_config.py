import pytest

from databricker.util import config, value

def test_default_config():
    value.InfraConfig().configure()

    result = config.config_value()

    assert result.value.infra['job']['name'] == 'Testing-default-config'


def test_infra_config(existing_job_config):
    result = config.config_value()

    assert result.is_right

    cfg = result.value

    assert set(cfg.infra.keys()) == set(['job', 'artefacts', 'emailNotifications', 'cluster'])


def test_generates_dbfs_location_for_wheel(existing_job_config, config_value):
    dbfs_loc = config.dbfs_artefact(config_value)

    assert "dbfs:/artifacts/job/job/dist/app-0.1.0-py3-none-any.whl" in dbfs_loc


def test_get_schedule_from_config(existing_job_config, config_value):
    assert config.schedule_config(config_value) == {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}


def test_get_schedule(existing_job_config, config_value):
    assert config_value.infra['job']['schedule'] == {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}


def test_determines_pipeline_type_for_job(existing_job_config, config_value):
    pipeline = config.pipeline_type(config_value)

    assert pipeline == config.PipelineType.JOB

def test_determines_pipeline_type_for_lib(library_config, config_value):
    pipeline = config.pipeline_type(config_value)

    assert pipeline == config.PipelineType.LIB

def test_cant_determine_pipeline_type(noop_config, config_value):
    pipeline = config.pipeline_type(config_value)

    assert pipeline == config.PipelineType.NOOP