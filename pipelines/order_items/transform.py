from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType


def transform_order_items(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["order_item_id"])
        .dropna(subset=["order_item_id", "order_id"])
        .withColumn("order_item_id", col("order_item_id").cast("integer"))
        .withColumn("order_id",      col("order_id").cast("integer"))
        .withColumn("product_id",    col("product_id").cast("integer"))
        .withColumn("quantity",      col("quantity").cast("integer"))
        .withColumn("unit_price",    col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("discount_pct",  col("discount_pct").cast(DecimalType(10, 2)))
        .filter(col("unit_price") > 0)
        .filter(col("quantity") > 0)
    )