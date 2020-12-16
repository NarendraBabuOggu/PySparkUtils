from src.dependencies.utils import get_spark
from src.dependencies.config_parser import Config
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from src.dependencies.logger import Log4j


def process_scrub_rule(
    spark: SparkSession, query: str,
    input: DataFrame, logger: Log4j = None
) -> DataFrame:
    """
    Function to process the given scrub rule query on the given dataframe

    Args:
        spark (SparkSession): Spark Session Object
        query (str): the Query string to be used on the DataFrame
        input (DataFrame): Input DataFrame
        logger (Log4j, optional): Logger Object to log the process.
            Defaults to None.

    Returns:
        DataFrame: Returns DataFrame after processing the scrub query
    """
    try:
        # Store input DataFrame as a view
        input.createOrReplaceTempView('df')
        if logger:
            logger.info("Stored the input DataFrame as a temperory view (df)")

        # Run the Scrubbing query on the input DataFrame
        df = spark.sql(query)
        if logger:
            logger.info("The scrub rule is processed on the given DataFrame")
        return df
    except Exception as e:
        if logger:
            logger.info("Exception Occured while processing the scrub rule")
            logger.error(e)


def read_dataframe(
    path: str, spark: SparkSession,
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
            .options(kwargs)
            .load()
        )
        return df
    except Exception as e:
        logger.warn(
            f"Unable to read CSV data from {path} with parameters {kwargs}"
        )
        logger.error(e)


def process_all_scrub_rules(
    data_path: str, rule_path: str,
    spark: SparkSession, logger: Log4j = None
):
    """
    To process the ccrub rules available in the rule_path on the data
    given by data_path. if any of the rule fails then the file is
    placed in Error Que Path.

    Args:
        data_path (str): Path of the Input Data
        rule_path (str): Path of the config file containing the Scrub Rules
        spark (SparkSession): Spark Session Object
        logger (Log4j, optional): Logger Object to log the process.
        Defaults to None.
    """

    try:
        scrub_rules = Config(rule_path)
        processed_df = None
        for rule in scrub_rules:
            if logger:
                logger.info(f"Processing the Scrub Rule {rule.split('.')[0]}")

            if not processed_df:
                processed_df = read_dataframe(data_path, spark, logger)

            processed_df = process_scrub_rule(
                spark, scrub_rules[rule], processed_df, logger
            )
    except Exception as e:
        logger.warn(
            f"""Got Exception while processing the file {data_path} 
            with rules at {rule_path}"""
        )
        logger.error(e)
