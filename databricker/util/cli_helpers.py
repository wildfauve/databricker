from typing import Any, List
import click
import pendulum
import subprocess

from . import monad


def echo(msg: Any, ctx: dict = None):
    formatted_time = pendulum.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if ctx:
        click.echo(f"[infra][{formatted_time}] {msg} {ctx}")
        return None
    click.echo(f"[infra][{formatted_time}] {msg}")



def run_command(cmd: List, message: str = "") -> monad.EitherMonad:
    pipe = subprocess.Popen(" ".join(cmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    result = pipe.communicate()

    if pipe.returncode == 0:
        echo("SUCCESS: {}".format(message))
        return monad.Right(None)
    echo("FAILURE: {}".format(message))
    echo("FAILURE: {}".format(result[0].decode()))
    return monad.Left("Failure executing command {}".format(" ".join(cmd)))


