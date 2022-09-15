import pytest

from databricker.util import config

@pytest.fixture
def config_value():
    return config.config_value().value



@pytest.fixture
def existing_job_config():
    config.configure(infra_config_file="tests/fixtures/infra.toml",
                     dist="dist")
    pass


@pytest.fixture
def new_job_config():
    config.configure(infra_config_file="tests/fixtures/infra_new.toml",
                     dist="dist")
    pass


@pytest.fixture
def existing_cluster_job_config():
    config.configure(infra_config_file="tests/fixtures/infra_existing_cluster.toml",
                     dist="dist")
    pass


@pytest.fixture
def library_config():
    config.configure(infra_config_file="tests/fixtures/infra_library.toml",
                     dist="dist")
    pass
