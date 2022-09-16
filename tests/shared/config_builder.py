import pytest

from databricker.util import config

@pytest.fixture
def config_value():
    return config.config_value().value



@pytest.fixture
def existing_job_config():
    config.configure(infra_config_file="tests/fixtures/already_created_job.toml",
                     pyproject="tests/fixtures/test_pyproject.toml",
                     dist="tests/fixtures/test_dist")
    pass

@pytest.fixture
def existing_job_on_existing_cluster_config():
    config.configure(infra_config_file="tests/fixtures/already_created_job_on_existing_cluster_infra.toml",
                     pyproject="tests/fixtures/test_pyproject.toml",
                     dist="tests/fixtures/test_dist")



@pytest.fixture
def new_job_on_existing_cluster_job_config():
    config.configure(infra_config_file="tests/fixtures/create_job_on_existing_cluster_infra.toml",
                     pyproject="tests/fixtures/test_pyproject.toml",
                     dist="tests/fixtures/test_dist")
    pass

@pytest.fixture
def new_job_on_new_cluster_job_config():
    config.configure(infra_config_file="tests/fixtures/create_job_on_new_cluster_infra.toml",
                     pyproject="tests/fixtures/test_pyproject.toml",
                     dist="tests/fixtures/test_dist")


@pytest.fixture
def error_new_job_on_new_cluster_job_config():
    config.configure(infra_config_file="tests/fixtures/error_create_job_on_new_cluster_infra.toml",
                     pyproject="tests/fixtures/test_pyproject.toml",
                     dist="tests/fixtures/test_dist")



@pytest.fixture
def library_config():
    config.configure(infra_config_file="tests/fixtures/library_infra.toml",
                     pyproject="tests/fixtures/test_pyproject.toml",
                     dist="tests/fixtures/test_dist")
    pass

@pytest.fixture
def cluster_library_config():
    config.configure(infra_config_file="tests/fixtures/cluster_lib_infra.toml",
                     pyproject="tests/fixtures/test_pyproject.toml",
                     dist="tests/fixtures/test_dist")
    pass


@pytest.fixture
def noop_config():
    config.configure(infra_config_file="tests/fixtures/noop_infra.toml",
                     pyproject="tests/fixtures/test_pyproject.toml",
                     dist="tests/fixtures/test_dist")
    pass
