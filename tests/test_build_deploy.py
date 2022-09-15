from pymonad.tools import curry

from databricker.util import monad, singleton, fn
from databricker.command import build_deploy_command


class CliCommandSpy(singleton.Singleton):
    commands = []


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

    patch_cmd, build_cmd, cp_cmd = cmds

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

    build_cmd, cp_cmd = cmds

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


def test_deploys_a_library(library_config, mocker, requests_mock):
    CliCommandSpy().commands = []
    req_mock = requests_mock.post("https://example.databricks.com/api/2.0/jobs/update",
                                  json={},
                                  status_code=200,
                                  headers={'Content-Type': 'application/json; charset=utf-8'})

    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper())

    result = build_deploy_command.run(bump="patch", no_version=True)

    assert result.is_right()

    cmds = list(map(lambda cmd: cmd['cmd'], CliCommandSpy().commands))

    assert len(cmds) == 2
    assert not req_mock.request_history


def test_noop_configuration(noop_config):
    result = build_deploy_command.run(bump="patch")

    assert result.is_right()


#
# Helpers
#
def success_returner(cmd):
    return monad.Right(None)


def throw_error_on_cp(cmd):
    if 'fs' in cmd and 'cp' in cmd:
        return monad.Left('Failure executing command poetry run databricks fs cp ...')
    return monad.Right(None)


def cli_spy_wrapper(returner_fn=success_returner):
    def cli_spy(cmd: list, message: str = ""):
        CliCommandSpy().commands.append({'cmd': cmd, 'message': message})
        return returner_fn(cmd)

    return cli_spy
