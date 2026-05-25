from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "hypercat") -> SparkSession:
    """
    Returns a SparkSession.
    Locally: runs in-memory for testing.
    On Glue: uses the existing Glue-managed session.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )