from pysparkutils.dependencies.utils import get_spark
from pysparkutils.dependencies.config_parser import Config
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pysparkutils.dependencies.logger import Log4j


def write_sngle_file(path: str, ext: str, logger: callable=None) -> bool:
    """
    To write the Spark generated directory to a single file

    Args:
        path (str): Path written by spark
        ext (str): file extension to use

    Returns:
        Boolean: Returns Succcess or not
    """
    if logger:
        logger.warn(f"Writing the part file in directory {path} to single file {path+'.'+ext}")
    try:
        from os import rename, removedirs, scandir, remove
        for file in scandir(path):
            if file.name.endswith('.csv'):
                rename(file.path, path+'.'+ext)
            else:
                remove(file.path)
        removedirs(path)
        return True
    except Exception as e:
        if logger:
            logger.warn(f"Unable to write the part file in directory {path} to single file {path+'.'+ext}")
        logger.error(e)


def write_sngle_file_hdfs(path: str, ext: str, logger: callable=None) -> bool:
    """
    To write the Spark generated directory to a single file

    Args:
        path (str): Path written by spark
        ext (str): file extension to use

    Returns:
        Boolean: Returns Succcess or not
    """
    if logger:
        logger.warn(f"Writing the part file in directory {path} to single file {path+'.'+ext}")
    try:
        from subprocess import check_output, getoutput
        files = getoutput(f"hadoop fs -ls {path}").split('\n')
        files = [file for file in files if 'SUCCESS' not in file and 'FOUND' not in file]
        check_output(["hadoop", "fs", "-mv", files[-1].split()[-1], path+'.'+ext])
        check_output(["hadoop", "fs", "-rm", "-r", path])
        return True
    except Exception as e:
        if logger:
            logger.warn(f"Unable to write the part file in directory {path} to single file {path+'.'+ext}")
        logger.error(e)


def read_dataframe(
    path: str, 
    spark: SparkSession,
    logger: Log4j = None,
    **kwargs
) -> DataFrame:
    """
    Function to read CSV as a Spark DataFrame

    Args:
        path (str): Path of input Data (CSV)
        spark (SparkSession): Spark Session Object
        logger (Log4j, optional): Logger Object to log the process.
            Defaults to None.

    Returns:
        DataFrame: Returns DataFrame with data from given path
    """

    if logger:
        logger.info(f"Reading CSV Data from {path} with parameters {kwargs}")
    try:
        df = (
            spark.read.format('csv')
            .option('path', path)
            .options(**kwargs)
            .load()
        )
        return df
    except Exception as e:
        logger.warn(
            f"Unable to read CSV data from {path} with parameters {kwargs}"
        )
        logger.error(e)

def write_dataframe(
    df:DataFrame, 
    path: str, 
    spark: SparkSession,
    logger: Log4j = None,
    n_partitions: int = 1,
    **kwargs
) -> bool:
    """
    Function to write CSV as a Spark DataFrame

    Args:
        df (DataFrame): Spark DataFrame to write
        path (str): Path of target Data (CSV)
        spark (SparkSession): Spark Session Object
        logger (Log4j, optional): Logger Object to log the process.
            Defaults to None.
        n_partitions (int): Number of partitions to write

    Returns:
        Boolean: Returns whether write is success or not
    """

    if logger:
        logger.info(f"Writing CSV Data to {path} with parameters {kwargs}")
    try:

        num_parts = df.rdd.getNumPartitions()
        if num_parts < n_partitions:
            df_writer = df.repartition(n_partitions).write
        elif num_parts > n_partitions:
            df_writer = df.coalesce(n_partitions).write
        else:
            df_writer = df.write

        path_ext = path.split('.')[-1]
        path = path[:len(path)-len(path_ext)-1]
        (
            df_writer.format('csv')
            .option('path', path)
            .options(**kwargs)
            .save()
        )

        if n_partitions == 1:
            write_sngle_file_hdfs(path, path_ext, logger)
        return True
    except Exception as e:
        logger.warn(
            f"Unable to write CSV data to {path} with parameters {kwargs}"
        )
        logger.error(e)


if __name__ == '__main__':
    config = Config('./configs/config.ini')
    spark, logger = get_spark(spark_config=config, env='dev')

    df = read_dataframe(
        path='/user/narendra/resources/data.csv',
        spark=spark,
        logger=logger,
        header=True,
        inferSchema=True,
        mode='FAILFAST'
    )

    write_dataframe(
        df, 
        path='/user/narendra/practice/out.csv',
        spark=spark,
        logger=logger,
        mode='overwrite', 
        header=True
    )