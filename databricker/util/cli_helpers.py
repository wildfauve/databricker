from typing import Any, List, Callable
import click
import pendulum
import subprocess

from . import monad

CLI_NAME = 'databricker'

def echo(msg: Any, ctx: dict = None):
    formatted_time = pendulum.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if ctx:
        click.echo(f"[{CLI_NAME}][{formatted_time}] {msg} {ctx}")
        return None
    click.echo(f"[{CLI_NAME}][{formatted_time}] {msg}")

def new_line_splitter(res):
    return res.decode('utf-8').split('\n')


def run_command(cmd: List,
                message: str = "",
                return_result: bool = False,
                result_parser: Callable = new_line_splitter) -> monad.EitherMonad:
    pipe = subprocess.Popen(" ".join(cmd), shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    result = pipe.communicate()

    if pipe.returncode == 0:
        echo("SUCCESS: {}".format(message))
        return result_returner(result, return_result, result_parser)

    echo("FAILURE: {}".format(message))
    echo("FAILURE: {}".format(result[0].decode()))
    return monad.Left("Failure executing command {}".format(" ".join(cmd)))


def result_returner(result, return_result, result_parser):
    if not return_result:
        return monad.Right(None)
    res, status = result
    return monad.Right(result_parser(res))

