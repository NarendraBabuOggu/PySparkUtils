"""
logger

This module contains a class that wraps the log4j object instantiated
by the active SparkContext, enabling Log4j logging for PySpark using.
"""
from pyspark.sql.session import SparkSession
from typing import Callable


class Log4j(object):
    """
    Wrapper class for Log4j JVM object.
    """

    def __init__(self: Callable, spark: SparkSession): 
        """
        To Initialize the Log4j class

        Args:
            self (Callable): [description]
            spark (SparkSession): [description]
        """
        # get spark app details with which to prefix all messages
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self: Callable, message: str):
        """Log an error.

        Args:
            self (Callable): Log4j Class
            message (str): Information message to write to log

        Returns: None
        """
        self.logger.error(message)
        return None

    def warn(self: Callable, message: str):
        """
        Log an warning.
        Args:
            self (Callable): Log4j Class
            message (str): Information message to write to log

        Returns: None
        """
        self.logger.warn(message)
        return None

    def info(self: Callable, message: str):
        """
        Log information.

        Args:
            self (Callable): Log4j Class
            message (str): Information message to write to log

        Returns: None
        """
        self.logger.info(message)
        return None

    def setLevel(self: Callable, loglevel: str = 'WARN'):
        """
        Log Level to use

        Args:
            self (Callable): Log4j Class
            loglevel (str, optional): loglevel to use. Defaults to 'WARN'.
        """

        self.logger.setLevel(loglevel)
        return None
