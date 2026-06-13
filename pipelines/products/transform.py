from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper
from pyspark.sql.types import DecimalType


def transform_products(df: DataFrame) -> DataFrame:
    """
    Silver transformation for products table.
    - Remove duplicate product_ids
    - Drop rows where product_id or product_name is null
    - Trim product_name, sku, supplier
    - Uppercase category
    - Cast product_id, stock_qty to integer
    - Cast unit_price to decimal(10,2)
    - Drop rows where unit_price <= 0
    """
    return (
        df
        .dropDuplicates(["product_id"])
        .dropna(subset=["product_id", "product_name"])
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("sku",          trim(col("sku")))
        .withColumn("supplier",     trim(col("supplier")))
        .withColumn("category",     upper(trim(col("category"))))
        .withColumn("product_id",   col("product_id").cast("integer"))
        .withColumn("stock_qty",    col("stock_qty").cast("integer"))
        .withColumn("unit_price",   col("unit_price").cast(DecimalType(10, 2)))
        .filter(col("unit_price") > 0)
    )