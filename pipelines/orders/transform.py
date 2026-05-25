from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper
from pyspark.sql.types import DecimalType


def transform_orders(df: DataFrame) -> DataFrame:
    """
    Silver transformation for orders table.
    - Remove duplicate order_ids
    - Drop rows where order_id is null
    - Drop rows where customer_id is null
    - Trim whitespace from status and payment_method
    - Uppercase status
    - Cast order_id to integer
    - Cast customer_id to integer
    - Cast store_id to integer
    - Cast discount_pct to decimal(10,2)
    """
    return (
        df
        .dropDuplicates(["order_id"])
        .dropna(subset=["order_id", "customer_id"])
        .withColumn("status",         upper(trim(col("status"))))
        .withColumn("payment_method", trim(col("payment_method")))
        .withColumn("order_id",       col("order_id").cast("integer"))
        .withColumn("customer_id",    col("customer_id").cast("integer"))
        .withColumn("store_id",       col("store_id").cast("integer"))
        .withColumn("discount_pct",   col("discount_pct").cast(DecimalType(10, 2)))
    )