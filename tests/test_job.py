from databricker.util import job

def test_builds_job_update_request_with_schedule(existing_job_config):
    schedule_config = {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}
    request = job.update_job_request(job_id="1",
                                     task_key="cbor_builder",
                                     wheel="dbfs:/location-of-artefact",
                                     schedule=schedule_config)

    expected_request = {'job_id': '1', 'new_settings': {
        'tasks': [{'task_key': 'cbor_builder', 'libraries': [{'whl': 'dbfs:/location-of-artefact'}]}],
        'schedule': {'quartz_cron_expression': '0 0 * * * ?', 'timezone_id': 'UTC', 'pause_status': 'UNPAUSED'}}}

    assert request == expected_request
