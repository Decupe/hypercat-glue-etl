import os
import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType
)
from pipelines.customers.transform import transform_customers


# ── Schemas ────────────────────────────────────────────
@pytest.fixture
def customer_schema():
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("first_name",  StringType(), True),
        StructField("last_name",   StringType(), True),
        StructField("email",       StringType(), True),
        StructField("country",     StringType(), True),
        StructField("created_at",  StringType(), True),
    ])


# ── Unit test data (dirty — for testing rules) ─────────
@pytest.fixture
def raw_customer_data(spark, customer_schema):
    data = [
        ("1", "  Alice  ", "Johnson", "alice@email.com", "uk",       "2024-01-01"),
        ("2", "Bob",       "Smith",   "bob@email.com",   "USA",      "2024-01-02"),
        ("2", "Bob",       "Smith",   "bob@email.com",   "USA",      "2024-01-02"),  # duplicate
        ("3", None,        "White",   None,              "canada",   "2024-01-03"),  # null email
        (None,"David",     "Brown",   "david@email.com", "UK",       "2024-01-04"),  # null id
        ("4", "  Emma  ",  "Wilson",  "emma@email.com",  "australia","2024-01-05"),
    ]
    return spark.createDataFrame(data, schema=customer_schema)


# ── DQ test data (clean — for quality checks) ──────────
@pytest.fixture
def clean_customer_data(spark, customer_schema):
    data = [
        ("1",  "Alice",   "Johnson", "alice@email.com",   "UK",        "2024-01-01"),
        ("2",  "Bob",     "Smith",   "bob@email.com",     "USA",       "2024-01-02"),
        ("3",  "Carol",   "White",   "carol@email.com",   "CANADA",    "2024-01-03"),
        ("4",  "David",   "Brown",   "david@email.com",   "UK",        "2024-01-04"),
        ("5",  "Emma",    "Wilson",  "emma@email.com",    "AUSTRALIA", "2024-01-05"),
        ("6",  "Frank",   "Jones",   "frank@email.com",   "UK",        "2024-01-06"),
        ("7",  "Grace",   "Miller",  "grace@email.com",   "USA",       "2024-01-07"),
        ("8",  "Henry",   "Davis",   "henry@email.com",   "CANADA",    "2024-01-08"),
        ("9",  "Isabel",  "Moore",   "isabel@email.com",  "UK",        "2024-01-09"),
        ("10", "James",   "Taylor",  "james@email.com",   "AUSTRALIA", "2024-01-10"),
    ]
    return spark.createDataFrame(data, schema=customer_schema)


@pytest.fixture
def transformed_customers(clean_customer_data):
    return transform_customers(clean_customer_data)


# ── Integration test data (from fixture CSV) ───────────
@pytest.fixture
def bronze_customers_df(spark):
    fixture_path = os.path.join(
        os.path.dirname(__file__),
        "tests", "fixtures", "bronze_customers.csv"
    )
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(fixture_path)
    )


@pytest.fixture
def silver_customers_df(bronze_customers_df):
    return transform_customers(bronze_customers_df)