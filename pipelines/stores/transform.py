from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper


def transform_stores(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["store_id"])
        .dropna(subset=["store_id", "store_name"])
        .withColumn("store_name", trim(col("store_name")))
        .withColumn("city",       trim(col("city")))
        .withColumn("region",     upper(trim(col("region"))))
        .withColumn("store_id",   col("store_id").cast("integer"))
    )