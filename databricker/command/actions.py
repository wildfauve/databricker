from databricker.util import artefacts, monad, cli_helpers, config, job, error

def validate_token_config(cfg):
    if not job.get_databricks_token(cfg).is_right():
        cli_helpers.echo("Token is not configured, check databrickcfg or DATABRICKS_TOKEN")
        return monad.Left(error.ValidationError("Token is not configured, check databrickcfg or DATABRICKS_TOKEN"))
    return monad.Right(cfg)


def build_artefact(cfg):
    current_version = cfg.project['tool']['poetry']['version']
    result = artefacts.build()
    if result.is_right():
        cfg.replace('project', config.read_project_toml())
        new_version = cfg.project['tool']['poetry']['version']
        cli_helpers.echo("Existing Version: {} New Version: {}".format(current_version, new_version))
        return monad.Right(cfg)
    return result


def version_artefact(cfg):
    if args_switch_check(cfg, 'no_version', False):
        cli_helpers.echo("No Version update performed")
        return monad.Right(cfg)
    result = artefacts.version(cfg)
    if result.is_right():
        return monad.Right(cfg)
    return result


def copy_artefact_to_dbfs(cfg):
    if cfg.args.get('skip_copy', None):
        cli_helpers.echo(f"SKIP Copy {config.dist_path(cfg)} to DBFS Location {cfg.infra['artefacts']['root']}")
        return monad.Right(cfg)

    cli_helpers.echo(f"Copy {config.dist_path(cfg)} to DBFS Location {cfg.infra['artefacts']['root']}")
    result = artefacts.copy_to_dbfs(cfg)

    if result.is_right():
        return monad.Right(cfg)
    return result


def args_switch_check(cfg, bool_arg_name, missing_is: bool = True):
    bool_arg = cfg.args.get(bool_arg_name)
    if bool_arg:
        return True
    return missing_is


def check_artefact_folder_exists(cfg):
    exists = artefacts.check_folder_exists(artefacts.artefacts_root(cfg))
    if exists.is_left():
        cli_helpers.echo(f"Cant find artefacts folder: {artefacts.artefacts_root(cfg)}")
        return monad.Left(f"Artefact folder root doesnt exists, create before rerunning: {artefacts.artefacts_root(cfg)}")
    return monad.Right(cfg)
