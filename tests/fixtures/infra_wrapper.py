from databricker import infra

"""
To inject the infra script into the host app as a cli implement the code in this module, place it in a folder at the
root called `_infra.cli` and add the following to the pyproject.toml

[tool.poetry.scripts]
infra = "_infra.cli:infra_cli"
 
"""

infra.configurator()(infra_config_file="tests/fixtures/infra.toml", dist="tests/fixtures/dist")

def infra_cli():
    infra.init_cli()


