from typing import Any, List
import click
import pendulum
import subprocess

from . import monad


def echo(message: Any):
    formatted_time = pendulum.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    formatted_message = f"[infra][{formatted_time}] {message}"
    click.echo(formatted_message)


def run_command(cmd: List, message: str = "") -> monad.EitherMonad:
    pipe = subprocess.Popen(" ".join(cmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    result = pipe.communicate()

    if pipe.returncode == 0:
        echo("SUCCESS: {}".format(message))
        return monad.Right(None)
    echo("FAILURE: {}".format(message))
    echo("FAILURE: {}".format(result[0].decode()))
    return monad.Left("Failure executing command {}".format(" ".join(cmd)))


