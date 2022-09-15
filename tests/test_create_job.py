from databricker.command import list_job_command, build_deploy_command, create_job_command
from databricker.util import config, job


def setup_module():
    pass


def test_create_job_fails_idempotent_check(existing_job_config):
    result = create_job_command.run()

    assert result.is_left()
    assert result.error() == "Job is already created with ID: 314471534377936. To fix delete the job first."


def test_create_job(new_job_config, requests_mock, mocker):
    req_mock = requests_mock.post("https://adb-575697367950122.2.azuredatabricks.net/api/2.0/jobs/create",
                                  json=job_create_result(),
                                  status_code=200,
                                  headers={'Content-Type': 'application/json; charset=utf-8'})

    mocker.patch('databricker.util.config.write_infra_toml', return_value=None)

    result = create_job_command.run()

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
                                        'package_name': 'job',
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
                                            'whl': 'dbfs:/artifacts/job/job/dist/databricker-0.1.19-py3-none-any.whl'
                                        },
                                        {'mavin': {
                                            'coordinates': 'com.azure.cosmos.spark:azure-cosmos-spark_3-2_2-12:4.12.1'}
                                        },
                                        {
                                            'whl': 'dbfs:/artifacts/common/python/databricker-0.1.9-py3-none-any.whl'
                                        }
                                    ]
                                    }
                               ],
                               'format': 'SINGLE_TASK'}

    assert req_mock.request_history[0].json() == expected_create_request


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


def job_create_result():
    return {
        "job_id": 1
    }
