from pysparkutils.dependencies.utils import get_spark
from typing import Callable, Optional, List
from logging import Logger
from pyspark.sql import SparkSession


class FileSystemUtility(object):
    """
    Class to interact with File System from Spark
    """

    def __init__(self: Callable, spark: SparkSession, logger: Logger):
        """
        To Initializa the FileSystemUtility Class with given config

        Args:
            self (Callable): Uninitialized FileSystemUtility Class
            config (ConfigTree): The config Object
            logger (Logger): Logger to log messages
        """

        try:
            if logger:
                self.logger = logger
            else:
                self.logger = None
            hadoop = spark.sparkContext._jvm.org.apache.hadoop
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            filesystem = hadoop.fs.FileSystem
            self.fs = filesystem.get(hadoop_conf)
            self.fs_path = hadoop.fs.Path
        except Exception as e:
            if logger:
                logger.warning("Exception Occured while initializing the FileSystem Utility")
                logger.error(e)
            raise(e)

    def ls(self: Callable, path: str) -> List[str]:
        """
        Method to iterate over the given path and list the contents

        Args:
            path (str): Path to get the contents
        """
        try:
            fs_path = self.fs_path(path)
            if self.logger:
                self.logger.info(f"Listing the given path {str(fs_path)}")
            return [str(f.getPath()) for f in self.fs.listStatus(fs_path)]
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Exception Occured while Listing the directory at {path} using FileSystem Utility")
                self.logger.error(e)
            raise(e)

    def write_to_single_file(self: Callable, path: str, ext: str) -> bool:
        """
        To write the Spark generated directory to a single file

        Args:
            path (str): Path written by spark
            ext (str): file extension to use

        Returns:
            Boolean: Returns Succcess or not
        """
        if self.logger:
            self.logger.warning(f"Writing the part file in directory {path} to single file {path+'.'+ext}")
        try:
            files = self.ls(path)
            files = [file for file in files if not 'SUCCESS' in file and not file.endswith('.crc')]
            self.logger.warning(files)
            self.logger.warning(f"Renaming the file {self.fs_path(files[-1])} to {self.fs_path(path+'.'+ext)}")
            self.fs.rename(self.fs_path(files[-1]), self.fs_path(path+'.'+ext))
            self.fs.delete(self.fs_path(path), True)
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Unable to write the part file in directory {path} to single file {path+'.'+ext}")
                self.logger.error(e)
            raise(e)


if __name__ == '__main__':

    spark, conf, logger = get_spark('configs/hocon.conf', loglevel='WARN', env='dev')
    fs_utility = FileSystemUtility(spark, logger)
    print(fs_utility.ls('/'))
    spark.stop() 