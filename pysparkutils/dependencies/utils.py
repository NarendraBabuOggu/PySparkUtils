from pyspark.sql import SparkSession
from pysparkutils.dependencies.hocon_config_parser import get_config
from pyhocon.config_tree import ConfigTree
from pysparkutils.dependencies.logger import get_logger
from typing import Tuple, List
from logging import Logger


def get_spark(
    config_path: str,
    loglevel: str = 'WARN',
    env: str = 'dev'
) -> Tuple[SparkSession, ConfigTree, Logger]:
    """
    Start Spark session and get Spark logger using the configuration 
    from the file present at given path
    
    Args:
        config_path (str): Path containing the config file
        loglevel (str, optional):
            Loglevel to use. Defaults to 'WARN'.
        spark_config (Config, optional):
            The configuration class. Defaults to None.

    Returns:
        Tuple[SparkSession, ConfigTree, Logger]: 
            Returns sparksession, configuration, logger objects 
    """

    try:
        conf = get_config(config_path)[env]
    
        spark_builder = (
            SparkSession
            .builder
            .appName(conf['spark.app.name'])
            .master(conf['spark.master'])
        )

        # create Spark JAR packages string
        for key, value in conf.items():
            spark_builder.config(key, value)

        # create session and retrieve Spark logger object
        spark_sess = spark_builder.getOrCreate()
        loglevel = conf['loglevel'] if 'loglevel' in conf else loglevel
        spark_sess.sparkContext.setLogLevel(loglevel)
        logger = get_logger(loglevel)
        
        spark_sess.sparkContext._jsc.hadoopConfiguration().set('fs.defaultFS', conf['spark.hadoop.fs.defaultFS'])

        return spark_sess, conf, logger
    except Exception as e:
        print("Exception occured while initialising sparksession object.")
        raise(e)
