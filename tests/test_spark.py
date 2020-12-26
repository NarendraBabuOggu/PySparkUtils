from pysparkutils.dependencies.utils import get_spark
import unittest
from typing import Callable


class SparkTest(unittest.TestCase):
    def setUp(self: Callable):
        """
        Method to setup spark session

        Args:
            self (Callable): Test Case
        """

        self.spark, self.conf, self.logger = get_spark(
            'tests/configs/hocon.conf', 
            loglevel='INFO',
            env='dev'
        )
        self.spark_conf = self.spark.sparkContext.getConf()

    def tearDown(self: Callable):
        """
        Method to Stop spark session

        Args:
            self (Callable): Test Case
        """

        self.spark.stop()

    def test_appname(self: Callable):
        """
        Method to validate Name of Spark Application

        Args:
            self (Callable): Test Case
        """

        self.assertEqual(
            self.spark_conf.get('spark.app.name'), 
            self.conf['spark.appname'],
            "The Spark Appname should equal to the appname mentioned in Config"
        )

    def test_master(self: Callable):
        """
        Method to validate master of Spark Application

        Args:
            self (Callable): Test Case
        """

        self.assertEqual(
            self.spark_conf.get('spark.master'), 
            self.conf['spark.master'],
            "The Spark master should equal to the master mentioned in Config"
        )


if __name__ == '__main__':
    unittest.main()
