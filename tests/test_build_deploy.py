from databricker.util import monad, singleton
from databricker.command import build_deploy_command

from tests.shared import *


def test_builds_and_deploys(existing_job_config, mocker, requests_mock):
    CliCommandSpy().commands = []
    req_mock = requests_mock.post("https://example.databricks.com/api/2.0/jobs/update",
                                  json={},
                                  status_code=200,
                                  headers={'Content-Type': 'application/json; charset=utf-8'})

    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper())

    result = build_deploy_command.run(bump="patch", no_version=False)

    assert result.is_right()

    cmds = list(map(lambda cmd: cmd['cmd'], CliCommandSpy().commands))

    ls_cmd, patch_cmd, build_cmd, cp_cmd = cmds

    assert ls_cmd == ['databricks', 'fs', 'ls', 'dbfs:/artifacts/job/job/dist']
    assert patch_cmd == ['poetry', 'version', 'patch']
    assert build_cmd == ['poetry', 'build']
    assert cp_cmd == ['poetry', 'run', 'databricks', 'fs', 'cp', 'tests/fixtures/test_dist/app-0.1.0-py3-none-any.whl',
                      'dbfs:/artifacts/job/job/dist']


def test_does_not_bump_version(existing_job_config, mocker, requests_mock):
    CliCommandSpy().commands = []
    req_mock = requests_mock.post("https://example.databricks.com/api/2.0/jobs/update",
                                  json={},
                                  status_code=200,
                                  headers={'Content-Type': 'application/json; charset=utf-8'})

    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper())

    result = build_deploy_command.run(bump="patch", no_version="no_version")

    assert result.is_right()

    cmds = list(map(lambda cmd: cmd['cmd'], CliCommandSpy().commands))

    ls_cmd, build_cmd, cp_cmd = cmds

    assert build_cmd == ['poetry', 'build']
    assert cp_cmd == ['poetry', 'run', 'databricks', 'fs', 'cp', 'tests/fixtures/test_dist/app-0.1.0-py3-none-any.whl',
                      'dbfs:/artifacts/job/job/dist']


def test_error_on_databricks_cp(existing_job_config, mocker, requests_mock):
    CliCommandSpy().commands = []
    req_mock = requests_mock.post("https://example.databricks.com/api/2.0/jobs/update",
                                  json={},
                                  status_code=200,
                                  headers={'Content-Type': 'application/json; charset=utf-8'})

    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper(returner_fn=throw_error_on_cp))

    result = build_deploy_command.run(bump="patch", no_version="no_version")

    assert result.is_left()
    assert result.error() == "Failure executing command poetry run databricks fs cp ..."


def test_error_on_update_job_api(existing_job_config, mocker, requests_mock):
    CliCommandSpy().commands = []
    req_mock = requests_mock.post("https://example.databricks.com/api/2.0/jobs/update",
                                  text=html_unauthorised_response(),
                                  status_code=401,
                                  headers={'Content-Type': 'text/html; charset=utf-8'})

    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper())

    result = build_deploy_command.run(bump="patch", no_version="no_version")

    assert result.is_left()
    assert result.error().code == 401


def test_error_in_config_of_no_job_or_cluster(error_new_job_on_new_cluster_job_config, config_value):
    result = build_deploy_command.run(bump="patch", no_version="no_version")

    assert result.error()
    assert result.error().ctx == {'cluster': [{'cluster_id': ['required field']}], 'job': [{'id': ['required field']}]}


def test_deploys_a_library(library_config, mocker):
    CliCommandSpy().commands = []
    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper())

    result = build_deploy_command.run(bump="patch", no_version=True)

    assert result.is_right()

    cmds = list(map(lambda cmd: cmd['cmd'], CliCommandSpy().commands))

    assert len(cmds) == 3


def test_deploys_a_cluster_library_to_multiple_clusters(cluster_library_config, mocker, requests_mock):
    CliCommandSpy().commands = []
    req_mock = requests_mock.post("https://example.databricks.com/api/2.0/libraries/install",
                                  json={},
                                  status_code=200,
                                  headers={'Content-Type': 'application/json; charset=utf-8'})

    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper())

    result = build_deploy_command.run(bump="patch", no_version=True)

    cmds = list(map(lambda cmd: cmd['cmd'], CliCommandSpy().commands))

    assert cmds == [
        ['databricks', 'fs', 'ls', 'dbfs:/artifacts/common/python'],
        ['poetry', 'build'],
        ['poetry', 'run', 'databricks', 'fs', 'cp', 'tests/fixtures/test_dist/app-0.1.0-py3-none-any.whl',
         'dbfs:/artifacts/common/python']]

    expected_request_1 = {'cluster_id': 'spark_cluster_1',
                          'libraries': [{'whl': 'dbfs:/artifacts/common/python/app-0.1.0-py3-none-any.whl'}]}
    expected_request_2 = {'cluster_id': 'spark_cluster_2',
                          'libraries': [{'whl': 'dbfs:/artifacts/common/python/app-0.1.0-py3-none-any.whl'}]}

    assert len(req_mock.request_history) == 2
    assert req_mock.request_history[0].json() == expected_request_1
    assert req_mock.request_history[1].json() == expected_request_2


def test_builder_composite_error_when_partial_failure_on_deploy_cluster_lib(cluster_library_config, mocker,
                                                                            requests_mock):
    CliCommandSpy().commands = []
    req_mock = requests_mock.post("https://example.databricks.com/api/2.0/libraries/install",
                                  [
                                      {'json': {}, 'status_code': 200},
                                      {'text': html_unauthorised_response(),
                                       'status_code': 401,
                                       'headers': {'Content-Type': 'text/html; charset=utf-8'}}
                                  ])

    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper())

    result = build_deploy_command.run(bump="patch", no_version=True)

    assert result.error().message == 'Http Error when calling https://example.databricks.com/api/2.0/libraries/install with statuscode: 401; '


def test_noop_configuration(noop_config):
    result = build_deploy_command.run(bump="patch")

    assert result.is_right()


#
# Helpers
#
def throw_error_on_cp(cmd):
    if 'fs' in cmd and 'cp' in cmd:
        return monad.Left('Failure executing command poetry run databricks fs cp ...')
    return monad.Right(None)


def html_unauthorised_response():
    return '<html>\n<head>\n<meta http-equiv="Content-Type" content="text/html;charset=utf-8"/>\n<title>Error 401 Unauthorized</title>\n</head>\n<body><h2>HTTP ERROR 401</h2>\n<p>Problem accessing /api/2.0//jobs/update. Reason:\n<pre>    Unauthorized</pre></p>\n</body>\n</html>\n'
