from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper


def transform_stores(df: DataFrame) -> DataFrame:
    """
    Silver transformation for stores table.
    - Removes duplicates on store_id
    - Drops rows with null store_id or store_name
    - Trims whitespace from store_name, city, region
    - Uppercases country and store_type
    - Casts store_id to integer
    """
    return (
        df
        .dropDuplicates(["store_id"])
        .dropna(subset=["store_id", "store_name"])
        .withColumn("store_name", trim(col("store_name")))
        .withColumn("city",       trim(col("city")))
        .withColumn("region",     trim(col("region")))
        .withColumn("country",    upper(trim(col("country"))))
        .withColumn("store_type", upper(trim(col("store_type"))))
        .withColumn("store_id",   col("store_id").cast("integer"))
    )