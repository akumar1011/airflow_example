import logging
import os
import yaml

from com.config.app_config import Configuration
from com.python.etl.first import First


class Airflow_Logger(object):
    def __init__(self, name=''):
        logging.root.handlers = []
        self.config = Configuration()
        self.logger_file_path = self.config.logger()
        with open(self.logger_file_path) as f:
            self.logger_config = yaml.load(f, Loader=yaml.SafeLoader)
        self.logger_default_file_name = self.logger_config["config"]["logging"]["default_filename"]
        self.log_dir = self.logger_config["config"]["logging"]["log_dir"]
        self.level = self.logger_config["config"]["logging"]["level"]
        self.formatter = self.logger_config["config"]["formatter"]["default"]
        self.file_name = self.logger_default_file_name if name == '' else name
        self.file_path = os.path.join(self.log_dir, self.file_name)
        logging.basicConfig(filename=self.file_path, filemode='a', format=self.formatter)
        self.logger = logging.getLogger()

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg, exc_info=True)

    def debug(self, msg):
        self.logger.debug(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def load(self):
        """this is for testing the code"""
        logging.warning('under def! new hardcode')
        logging.warning('This will get logged to a file--logging')
        self.logger.warning('This will get logged to a file--logger')
        self.logger.debug('This will get logged to a file--logger')


# a = Airflow_Logger()
# # a = Airflow_Logger("custom_logger.log")
# a.load()
# a.info("This is for info.")
# a.error("This is for error.")
# a.debug("This is for debug.")
# a.warning("This is for warning.")
