from databricker.validator import validator


def test_existing_job_valid(existing_job_config, config_value):
    assert validator.existing_job_validator(config_value)


def test_existing_job_on_existing_cluster_valid(existing_job_on_existing_cluster_config, config_value):
    assert validator.existing_job_validator(config_value)


def test_new_job_on_new_cluster(error_new_job_on_new_cluster_job_config, config_value):
    assert validator.new_job_new_cluster_validator(config_value)


def test_new_job_on_existing_cluster(new_job_on_existing_cluster_job_config, config_value):
    assert validator.new_job_existing_cluster_validator(config_value)


def test_cluster_library_valid(cluster_library_config, config_value):
    assert validator.cluster_library_validator(config_value)


def test_library_valid(library_config, config_value):
    assert validator.library_validator(config_value)


#
# Errors
#
def test_error_new_job_on_new_cluster(error_new_job_on_new_cluster_job_config, config_value):
    result = validator.new_job_new_cluster_validator(config_value)

    assert result.error()
    assert result.error().ctx == {'cluster': [
        {'node_type_id': ['required field'], 'num_workers': ['required field'], 'spark_version': ['required field']}]}
