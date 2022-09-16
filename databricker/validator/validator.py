from typing import Optional, Dict
from cerberus import Validator

from databricker.util import monad, cli_helpers, error
from . import schema


def validator(schema: Dict):
    """
    Validator Decorator
    """

    def inner(fn):
        def val(*args, **kwargs):
            sc = kwargs.get('schema', schema)
            result = validate(fn(*args, **kwargs), sc)
            if result.is_right():
                return result
            log(result.error().message, ctx=result.error().ctx)
            return result

        return val

    return inner


def validate(data, schema) -> monad.EitherMonad[Optional[Dict]]:
    v = Validator()
    result = v.validate(data, schema)
    if result:
        return monad.Right(None)
    return monad.Left(error.ValidationError("Infra toml Validation Error", ctx=v.errors))


def log(log_message: str, ctx: Dict) -> None:
    cli_helpers.echo(msg=log_message, ctx=ctx)
    pass


@validator(schema=schema.existing_job_schema)
def existing_job_validator(cfg):
    return cfg.infra


@validator(schema=schema.new_job_schema_new_cluster)
def new_job_new_cluster_validator(cfg):
    return cfg.infra


@validator(schema=schema.new_job_schema_existing_cluster)
def new_job_existing_cluster_validator(cfg):
    return cfg.infra


@validator(schema=schema.library_schema)
def library_validator(cfg):
    return cfg.infra


@validator(schema=schema.cluster_library_schema)
def cluster_library_validator(cfg):
    return cfg.infra
