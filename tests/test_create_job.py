from databricker.command import create_job_command

from tests.shared import *


def setup_module():
    pass


def test_create_job_fails_idempotent_check(existing_job_config):
    result = create_job_command.run(bump="patch", no_version=False)

    assert result.is_left()
    assert result.error() == "Job is already created with ID: 314471534377936. To fix delete the job first."


def test_create_job(new_job_on_new_cluster_job_config, requests_mock, mocker):
    req_mock = requests_mock.post("https://example.databricks.com/api/2.0/jobs/create",
                                  json=job_create_result(),
                                  status_code=200,
                                  headers={'Content-Type': 'application/json; charset=utf-8'})
    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper())
    mocker.patch('databricker.util.config.write_infra_toml', return_value=None)

    result = create_job_command.run(bump="patch", no_version=False)

    assert result.is_right()
    assert len(req_mock.request_history) == 1

    expected_create_request = {'name': 'job',
                               'email_notifications': {'no_alert_for_skipped_runs': False,
                                                       'on_failure': ['admin@example.com']},
                               'timeout_seconds': 0,
                               'max_concurrent_runs': 1,
                               'tasks': [
                                   {'task_key': 'job',
                                    'python_wheel_task': {
                                        'package_name': 'cbor_builder',
                                        'entry_point': 'job_main',
                                        'parameters': [
                                            '--all-batches']},
                                    'timeout_seconds': 0,
                                    'email_notifications': {},
                                    'new_cluster': {
                                        'spark_version': '11.1.x-scala2.12',
                                        'node_type_id': 'Standard_DS3_v2',
                                        'num_workers': 1},
                                    'libraries': [
                                        {
                                            'whl': 'dbfs:/artifacts/job/job/dist/app-0.1.0-py3-none-any.whl'
                                        },
                                        {'maven': {
                                            'coordinates': 'java-artefact-1'}
                                        },
                                        {
                                            'whl': 'python-wheel-1'
                                        }
                                    ]
                                    }
                               ],
                               'tags': {},
                               'format': 'SINGLE_TASK'}

    assert req_mock.request_history[0].json() == expected_create_request


def test_create_job_with_existing_cluster(new_job_on_existing_cluster_job_config, requests_mock, mocker):
    requests_mock.post("https://example.databricks.com/api/2.0/jobs/create",
                       json=job_create_result(),
                       status_code=200,
                       headers={'Content-Type': 'application/json; charset=utf-8'})

    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper())
    mocker.patch('databricker.util.config.write_infra_toml', return_value=None)

    result = create_job_command.run(bump="patch", no_version=False)

    assert result.is_right()

    _, create_job_req, _ = result.value

    assert create_job_req['tasks'][0]['existing_cluster_id'] == "0914-001041-jbnfazlx"


def test_serialises_additional_artefacts(new_job_on_existing_cluster_job_config, requests_mock, mocker):
    requests_mock.post("https://example.databricks.com/api/2.0/jobs/create",
                       json=job_create_result(),
                       status_code=200,
                       headers={'Content-Type': 'application/json; charset=utf-8'})

    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper())
    mocker.patch('databricker.util.config.write_infra_toml', return_value=None)

    create_job_command.run(bump="patch", no_version=False)

    libs = requests_mock.request_history[0].json()['tasks'][0]['libraries']

    assert len(libs) == 5

    expected_artefacts = [
        {'whl': 'dbfs:/artifacts/job/job/dist/app-0.1.0-py3-none-any.whl'},
        {'maven': {'coordinates': 'java-artefact-1'}},
        {'maven': {'coordinates': 'java_artefact_2'}},
        {'whl': 'wheel-1'},
        {'whl': 'wheel-2'}
    ]

    assert libs == expected_artefacts


def test_adds_tags_to_job(new_job_on_existing_cluster_job_config, requests_mock, mocker):
    requests_mock.post("https://example.databricks.com/api/2.0/jobs/create",
                       json=job_create_result(),
                       status_code=200,
                       headers={'Content-Type': 'application/json; charset=utf-8'})

    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper())
    mocker.patch('databricker.util.config.write_infra_toml', return_value=None)

    res = create_job_command.run(bump="patch", no_version=False)

    tags = requests_mock.request_history[0].json()['tags']

    assert tags == {'domain': 'portfolio', 'team': 'awesome-team', 'dataproduct': 'cbor'}


def test_checks_artefact_root_exists(new_job_on_existing_cluster_job_config, requests_mock, mocker):
    CliCommandSpy().commands = []
    requests_mock.post("https://example.databricks.com/api/2.0/jobs/create",
                       json=job_create_result(),
                       status_code=200,
                       headers={'Content-Type': 'application/json; charset=utf-8'})

    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper())
    mocker.patch('databricker.util.config.write_infra_toml', return_value=None)

    create_job_command.run(bump="patch", no_version=False)

    assert CliCommandSpy().commands[0]['cmd'] == ['databricks', 'fs', 'ls', 'dbfs:/artifacts/job/job/dist']
    assert CliCommandSpy().commands[1]['cmd'] == ['poetry', 'version', 'patch']
    assert CliCommandSpy().commands[2]['cmd'] == ['poetry', 'build']
    assert CliCommandSpy().commands[3]['cmd'] == ['poetry', 'run', 'databricks', 'fs', 'cp',
                                                  'tests/fixtures/test_dist/app-0.1.0-py3-none-any.whl',
                                                  'dbfs:/artifacts/job/job/dist']

def test_fails_when_folder_doesnt_exist(new_job_on_existing_cluster_job_config, mocker):
    CliCommandSpy().commands = []
    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper(cli_failure_returner))
    mocker.patch('databricker.util.config.write_infra_toml', return_value=None)

    result = create_job_command.run(bump="patch", no_version=False)

    assert result.is_left()
    assert result.error() == 'Artefact folder root doesnt exists, create before rerunning: dbfs:/artifacts/job/job/dist'


#
# Helpers
#

def job_create_result():
    return {
        "job_id": 1
    }
