import yaml
import json

# from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable


class Configuration:
    def __init__(self):
        self.description = "All the configuration will be set here"
        # pipeline_variable = Variable.get(PIPELINE_CONFIGS, deserialize_json=True)
        # pth = r"C:\Users\anukumar0\PycharmProjects\airflow_example\src\main\com\config\pipeline_config.json"
        pth = r"/home/anupam/git/airflow_example/src/main/com/config/pipeline_config.json"
        with open(pth) as f:
            self.variable = json.load(f)

    def logger(self):
        logger_config = self.variable["config"]["logging_config"]
        return logger_config

    def demo(self):
        print(self.variable)
