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
        StructField("customer_id",        StringType(), True),
        StructField("first_name",         StringType(), True),
        StructField("last_name",          StringType(), True),
        StructField("email",              StringType(), True),
        StructField("region",             StringType(), True),
        StructField("city",               StringType(), True),
        StructField("preferred_store_id", StringType(), True),
        StructField("loyalty_tier",       StringType(), True),
        StructField("registration_date",  StringType(), True),
        StructField("is_active",          StringType(), True),
        StructField("created_at",         StringType(), True),
        StructField("updated_at",         StringType(), True),
    ])


# ── Unit test data (dirty — for testing rules) ─────────
@pytest.fixture
def raw_customer_data(spark, customer_schema):
    data = [
        ("1",  "  Alice  ", "Johnson", "alice@email.com",  "north",  "Sheffield",   "6",  "silver", "19/10/2020", "1", "2024-01-01", "2024-01-01"),
        ("2",  "Bob",       "Smith",   "bob@email.com",    "SOUTH",  "London",      "2",  "GOLD",   "20/10/2020", "1", "2024-01-02", "2024-01-02"),
        ("2",  "Bob",       "Smith",   "bob@email.com",    "SOUTH",  "London",      "2",  "GOLD",   "20/10/2020", "1", "2024-01-02", "2024-01-02"),  # duplicate
        ("3",  None,        "White",   None,               "east",   "Manchester",  "3",  "bronze", "21/10/2020", "1", "2024-01-03", "2024-01-03"),  # null email
        (None, "David",     "Brown",   "david@email.com",  "WEST",   "Bristol",     "4",  "silver", "22/10/2020", "1", "2024-01-04", "2024-01-04"),  # null id
        ("4",  "  Emma  ",  "Wilson",  "emma@email.com",   "north",  "Leeds",       "5",  "gold",   "23/10/2020", "1", "2024-01-05", "2024-01-05"),
    ]
    return spark.createDataFrame(data, schema=customer_schema)


# ── DQ test data (clean — for quality checks) ──────────
@pytest.fixture
def clean_customer_data(spark, customer_schema):
    data = [
        ("1",  "Alice",   "Johnson", "alice@email.com",   "NORTH",  "Sheffield",   "6",  "SILVER", "19/10/2020", "1", "2024-01-01", "2024-01-01"),
        ("2",  "Bob",     "Smith",   "bob@email.com",     "SOUTH",  "London",      "2",  "GOLD",   "20/10/2020", "1", "2024-01-02", "2024-01-02"),
        ("3",  "Carol",   "White",   "carol@email.com",   "EAST",   "Manchester",  "3",  "BRONZE", "21/10/2020", "1", "2024-01-03", "2024-01-03"),
        ("4",  "David",   "Brown",   "david@email.com",   "WEST",   "Bristol",     "4",  "SILVER", "22/10/2020", "1", "2024-01-04", "2024-01-04"),
        ("5",  "Emma",    "Wilson",  "emma@email.com",    "NORTH",  "Leeds",       "5",  "GOLD",   "23/10/2020", "1", "2024-01-05", "2024-01-05"),
        ("6",  "Frank",   "Jones",   "frank@email.com",   "SOUTH",  "Brighton",    "6",  "SILVER", "24/10/2020", "1", "2024-01-06", "2024-01-06"),
        ("7",  "Grace",   "Miller",  "grace@email.com",   "EAST",   "Norwich",     "7",  "BRONZE", "25/10/2020", "0", "2024-01-07", "2024-01-07"),
        ("8",  "Henry",   "Davis",   "henry@email.com",   "WEST",   "Cardiff",     "8",  "GOLD",   "26/10/2020", "1", "2024-01-08", "2024-01-08"),
        ("9",  "Isabel",  "Moore",   "isabel@email.com",  "NORTH",  "Newcastle",   "9",  "SILVER", "27/10/2020", "1", "2024-01-09", "2024-01-09"),
        ("10", "James",   "Taylor",  "james@email.com",   "SOUTH",  "Portsmouth",  "10", "BRONZE", "28/10/2020", "1", "2024-01-10", "2024-01-10"),
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