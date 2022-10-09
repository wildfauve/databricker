from databricker.util import artefacts

from tests.shared import *


def test_folder_exists(mocker):
    CliCommandSpy().commands = []
    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper())

    result = artefacts.check_folder_exists("dbfs:/artifacts/common/python/dist")

    assert result.is_right()


def test_folder_does_not_exist(mocker):
    CliCommandSpy().commands = []
    mocker.patch('databricker.util.cli_helpers.run_command', cli_spy_wrapper(cli_failure_returner))


    result = artefacts.check_folder_exists("dbfs:/blah/blah")

    assert result.is_left()


#
# Helpers
#
