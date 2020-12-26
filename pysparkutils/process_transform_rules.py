from pysparkutils.dependencies.utils import get_spark
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import importlib
from logging import Logger
from pysparkutils.process_data import read_dataframe, write_dataframe
from pysparkutils.dependencies.file_system_utility import FileSystemUtility
import subprocess


def process_all_transform_rules(
    data_path: str, rule_path: str,
    error_path: str, output_path: str,
    spark: SparkSession, logger: Logger = None, 
    **kwargs
):
    """
    To process all the Transform rules available in the Rule Path on the data
    given by Data Path. if any of the rule fails then the file is
    placed in Error Path.

    Args:
        data_path (str): Path of the Input Data
        rule_path (str): Path of the config file containing the Scrub Rules
        error_path (str): Path to place the files that got errored.
        spark (SparkSession): Spark Session Object.
        output_path (str): Output Path to store the processed DataFrame
        logger (Log4j, optional): Logger Object to log the process.
        Defaults to None.
    """

    try:
        if logger:
            logger.info(f"Reading Data from {data_path} as a DataFrame.")
        df = read_dataframe(data_path, spark, logger, **kwargs)
        old_count = df.count()
        if logger:
            logger.warning(f"Input DataFrame contains {old_count} records.")
        
        if logger:
            logger.warning("Processing the DataFrame using the transform rules at {rule_path}")
        with open(rule_path, 'r') as f:
            data = f.readlines()
        
        for row in data:
            col_name, rule_no = tuple(row.split('|'))
            transform_rule = importlib.import_module('pysparkutils.transform_rules.transform_rule_'+rule_no)
            if logger:
                logger.warning(f"Processing the transform rule {rule_no} on column {col_name}")
            df = transform_rule.transform_data(df, col_name)
        
        new_count = df.count()
        if logger:
            logger.warning(f"Processed DataFrame has {new_count} records.")
            logger.warning(f"Processing the DataFrame using the transform rules at {rule_path} is completed.")

        write_dataframe(df, output_path, spark, logger, **kwargs)

        fs_utility = FileSystemUtility(spark, logger)
        fs_utility.write_to_single_file(output_path, 'csv')
        return True
    except Exception as e:
        if logger:
            logger.warning(
                f"""Got Exception while processing the file {data_path} with rules at {rule_path}"""
            )
            logger.error(e)
            logger.warning(f"Moving the file at {data_path} to {error_path}")
        subprocess.getoutput(f'hadoop fs -mv {data_path} {error_path}')


if __name__ == '__main__':
    spark, conf, logger = get_spark('configs/hocon.conf', env='dev')

    process_all_transform_rules(
        '/user/narendra/resources/data.csv', 
        'transforms/transform_rules.txt',
        '/user/narendra/errors/', 
        '/user/narendra/output/data',
        spark, logger, header=True, inferSchema=True, 
        save_mode='overwrite', mode='FAILFAST'
    )


