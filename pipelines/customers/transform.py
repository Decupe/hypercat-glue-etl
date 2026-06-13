from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper


def transform_customers(df: DataFrame) -> DataFrame:
    """
    Silver transformation for customers table.
    - Remove duplicate customer_ids
    - Drop rows where customer_id or email is null
    - Trim whitespace from first_name, last_name, email
    - Uppercase region and loyalty_tier
    - Cast customer_id to integer
    - Cast is_active to integer
    """
    return (
        df
        .dropDuplicates(["customer_id"])
        .dropna(subset=["customer_id", "email"])
        .withColumn("first_name",    trim(col("first_name")))
        .withColumn("last_name",     trim(col("last_name")))
        .withColumn("email",         trim(col("email")))
        .withColumn("city",          trim(col("city")))
        .withColumn("region",        upper(trim(col("region"))))
        .withColumn("loyalty_tier",  upper(trim(col("loyalty_tier"))))
        .withColumn("customer_id",   col("customer_id").cast("integer"))
        .withColumn("is_active",     col("is_active").cast("integer"))
    )