from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import upper, trim, regexp_replace, col
from pysparkutils.dependencies.utils import get_spark
from pyspark.sql.types import StringType, StructField, StructType


def transform_data(df: DataFrame, column_name: str) -> DataFrame:
    """
    Function to transform the given column data using Transform rule

    Args:
        df (DataFrame): PySpark Data Frame to apply transformation
        col (Column): PySpark Data Frame Column name to apply transformation 

    Returns:
        DataFrame: PySpark Data Frame Column after applying the transformations
    """

    return (
        df
        .withColumn(column_name, regexp_replace(column_name, "[^\\'0-9a-zA-Z .-]", ''))
        .withColumn(column_name, regexp_replace(column_name, "[\\'.-]", ' '))
        .withColumn(column_name, regexp_replace(column_name, '\\s+', ' '))
        .withColumn(column_name, trim(upper(col(column_name))))
    )


if __name__ == '__main__':
    spark, conf, logger = get_spark('configs/hocon.conf', loglevel = 'ERROR', env='dev')

    df = spark.createDataFrame(
        [
            ('Narendra     ', ), ('Narendra    Babu-', ), ('Oggu.Bab*u', )
        ], 
        StructType(
            [
                StructField('name', StringType(), True)
            ]
        ) 
    )

    res_df = transform_data(df, 'name')
    df.show()
    res_df.show()
