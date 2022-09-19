"""
{'job': {'id': 314471534377936, 'name': 'job', 'task_key': 'job',
         'schedule': {'cron': '0 0 * * * ?', 'pause_status': 'UNPAUSED', 'tz': 'UTC'}},
 'cluster': {'url': 'https://example.databricks.com', 'type': 'newCluster', 'spark_version': '11.1.x-scala2.12',
             'num_workers': 1, 'node_type_id': 'Standard_DS3_v2'},
 'emailNotifications': {'on_failure': 'admin@example.com'}, 'artefacts': {'root': 'dbfs:/artifacts/job/job/dist'}}
"""

required_non_empty_str = {'type': 'string', 'empty': False, 'required': True}

required_non_empty_int = {'type': 'integer', 'empty': False, 'required': True}

optional_str = {'type': 'string', 'nullable': True, 'required': False}

optional_int = {'type': 'integer', 'nullable': True, 'required': False}

optional_list_strs = {'type': 'list', 'schema': optional_str}

required_list_strs = {'type': 'list', 'schema': required_non_empty_str}

tags = {'type': 'dict',
        'required': False,
        'nullable': True,
        'empty': True,
        'keysrules': {'type': 'string', 'regex': '[a-z]+'},
        'valuesrules': {'type': 'string', 'regex': '[a-z]+'}}

schedule = {'cron': required_non_empty_str,
            'pause_status': required_non_empty_str,
            'tz': required_non_empty_str}

job_base = {
    'name': required_non_empty_str,
    'task_key': required_non_empty_str,
    'entry_point': required_non_empty_str,
    'package_name': required_non_empty_str,
    'schedule': {'nullable': True, 'empty': True, 'schema': schedule},
    'parameters': optional_list_strs,
    'tags': tags
}

existing_job = {
    'type': 'dict',
    'nullable': False,
    'empty': False,
    'schema': {**{'id': required_non_empty_int}, **job_base}
}

new_job = {
    'type': 'dict',
    'nullable': False,
    'empty': False,
    'schema': job_base
}

cluster_base = {
    'url': required_non_empty_str
}

new_cluster = {
    'type': 'dict',
    'nullable': False,
    'empty': False,
    'schema': {
        **cluster_base,
        **{'type': {'type': 'string', 'allowed': ['newCluster']},
           'spark_version': required_non_empty_str,
           'num_workers': required_non_empty_int,
           'node_type_id': required_non_empty_str}
    }
}

existing_cluster = {
    'type': 'dict',
    'nullable': False,
    'empty': False,
    'schema': {**cluster_base,
               **{'cluster_id': required_non_empty_str,
                  'type': {'type': 'string', 'allowed': ['newCluster', 'existingCluster']},
                           'spark_version': optional_str,
                            'num_workers': optional_int,
                            'node_type_id': optional_str}
               }
}

existing_cluster_with_multi_ids = {
    'type': 'dict',
    'nullable': False,
    'empty': False,
    'schema': {**cluster_base,
               **{'cluster_ids': required_list_strs,
                  'type': {'type': 'string', 'allowed': ['newCluster', 'existingCluster']},
                           'spark_version': optional_str,
                            'num_workers': optional_int,
                            'node_type_id': optional_str}
               }
}


existing_cluster_base = {
    'type': 'dict',
    'nullable': False,
    'empty': False,
    'schema': cluster_base
}


existing_typed_cluster = {
    'type': 'dict',
    'nullable': False,
    'empty': False,
    'schema': {
        **cluster_base,
        **{'type': {'type': 'string', 'allowed': ['existingCluster']},
           'cluster_id': required_non_empty_str}
    }
}

email_notifications = {
    'type': 'dict',
    'nullable': True,
    'empty': True,
    'schema': {'on_failure': optional_str}
}

artefacts = {
    'type': 'dict',
    'nullable': False,
    'empty': False,
    'schema': {'root': required_non_empty_str,
               'maven_artefacts': optional_list_strs,
               'whl_artefacts': optional_list_strs
               }
}

library = {
    'type': "dict",
    'nullable': False,
    'empty': False,
    'schema': {
        'name': optional_str
    }
}

existing_job_schema = {'job': existing_job,
                       'cluster': existing_cluster,
                       'emailNotifications': email_notifications,
                       'artefacts': artefacts}

new_job_schema_new_cluster = {'job': new_job,
                              'cluster': new_cluster,
                              'emailNotifications': email_notifications,
                              'artefacts': artefacts}

new_job_schema_existing_cluster = {'job': new_job,
                                   'cluster': existing_typed_cluster,
                                   'emailNotifications': email_notifications,
                                   'artefacts': artefacts}

library_schema = {
    'library': library,
    'cluster': existing_cluster_base,
    'artefacts': artefacts
}

cluster_library_schema = {
    'cluster_library': library,
    'cluster': existing_cluster_with_multi_ids,
    'artefacts': artefacts
}
