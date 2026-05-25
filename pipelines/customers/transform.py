from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper


def transform_customers(df: DataFrame) -> DataFrame:
    """
    Silver transformation for customers table.
    - Removes duplicates on customer_id
    - Drops rows with null customer_id or email
    - Trims whitespace from first_name, last_name, email
    - Uppercases country
    - Casts customer_id to integer
    """
    return (
        df
        .dropDuplicates(["customer_id"])
        .dropna(subset=["customer_id", "email"])
        .withColumn("first_name",  trim(col("first_name")))
        .withColumn("last_name",   trim(col("last_name")))
        .withColumn("email",       trim(col("email")))
        .withColumn("country",     upper(trim(col("country"))))
        .withColumn("customer_id", col("customer_id").cast("integer"))
    )