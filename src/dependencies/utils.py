# Importing PySpark Utilities
from pyspark.sql import SparkSession
# Importing Project related Utilities
from src.dependencies.config_parser import Config
from src.dependencies.logger import Log4j
from typing import Tuple, List
import __main__


def get_spark(
    appname: str = 'spark_app', master: str = 'local[*]',
    jar_packages: List[str] = [], files: List[str] = [],
    loglevel: str = 'WARN', spark_config: Config = None,
    env: str = None
) -> Tuple[SparkSession, Log4j]:
    """
    Start Spark session and get Spark logger
    Start a Spark session on the worker node and register the Spark
    application with the cluster. Note, that only the appname argument
    will apply when this is called from a script sent to spark-submit.
    All other arguments exist solely for testing the script from within
    an interactive Python console.
    The function checks the enclosing environment to see if it is being
    run from inside an interactive console session.
    In this scenario, the function uses all available function arguments
    to start a PySpark driver from the local PySpark package as opposed
    to using the spark-submit and Spark cluster defaults. This will also
    use local module imports, as opposed to those in the zip archive
    sent to spark via the --py-files flag in spark-submit.

    Args:
        appname (str, optional) :
            Name of Spark Application. Defaults to spark_app.
        master (str, optional) :
            Spark Mater URL. Defaults to local[*].
        jar_packages (list, optional) :
            Packages to use when calling spark. Defaults to [].
        files (list, optional) :
            Extra files to submit to the cluster. Defaults to [].
        loglevel (str, optional) :
            Loglevel to use. Defaults to 'WARN'.
        spark_config (Config, optional) :
            The configuration class. Defaults to None.

    Returns:
        Tuple[SparkSession, SparkContext,: [description]
    """

    # detect execution environment
    flag_repl = not(hasattr(__main__, '__file__'))

    if spark_config:
        appname = (
            spark_config[env+'.appname']
            if env+'.appname' in spark_config
            else appname
        )
        master = (
            spark_config[env+'.master']
            if env+'.master' in spark_config
            else master
        )
        jar_packages = (
            spark_config[env+'.packages']
            if env+'.packages' in spark_config
            else jar_packages
        )
        files = (
            spark_config[env+'.files']
            if env+'.files' in spark_config
            else files
        )

    if not (flag_repl):
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .appName(appname)
        )
    else:
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .master(master)
            .appName(appname)
        )

        # create Spark JAR packages string
        spark_jars_packages = (
            ','.join(list(jar_packages))
            if isinstance(jar_packages, list)
            else jar_packages
        )
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = (
            ','.join(list(files))
            if isinstance(files, list)
            else files
        )
        spark_builder.config('spark.files', spark_files)

        # add other config params
        for key in spark_config:
            spark_builder.config(key, spark_config[key])

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = Log4j(spark_sess)
    spark_sess.sparkContext.setLogLevel(loglevel)

    return spark_sess, spark_logger
