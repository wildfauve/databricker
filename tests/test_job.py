from databricker.util import job


def test_builds_job_update_request_with_schedule(existing_job_config, config_value):
    schedule_config = {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}

    request = job.update_job_request(cfg=config_value,
                                     job_id="1",
                                     task_key="cbor_builder",
                                     schedule=schedule_config)

    expected_request = {'job_id': '1',
                        'new_settings': {
                            'tasks': [
                                {'task_key': 'cbor_builder',
                                 'libraries': [{'whl': 'dbfs:/artifacts/job/job/dist/app-0.1.0-py3-none-any.whl'}]
                                 }
                            ],
                            'schedule': {'quartz_cron_expression': '0 0 * * * ?', 'timezone_id': 'UTC',
                                         'pause_status': 'UNPAUSED'}}}
    assert request == expected_request
