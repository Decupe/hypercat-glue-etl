import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Shared SparkSession for ALL test files across
    ALL pipelines. Created once per test session,
    reused everywhere.

    scope="session" means:
    - One Spark instance for the entire test run
    - NOT recreated per pipeline or per test file
    - Saves ~60s startup time per pipeline
    """
    return (
        SparkSession.builder
        .appName("hypercat-test-suite")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )