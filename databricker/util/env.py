import os

class Env:
    env = os.environ.get('ENVIRONMENT', default=None)

    @staticmethod
    def all_envs_keys():
        return ",".join([i for i in os.environ.keys()])

    @staticmethod
    def databricks_token():
        return os.environ.get('DATABRICKS_TOKEN', default=None)



