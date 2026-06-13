import os
import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType
)
from pipelines.stores.transform import transform_stores


# ── Schema ─────────────────────────────────────────────
@pytest.fixture
def store_schema():
    return StructType([
        StructField("store_id",   StringType(), True),
        StructField("store_name", StringType(), True),
        StructField("region",     StringType(), True),
        StructField("city",       StringType(), True),
        StructField("open_date",  StringType(), True),
        StructField("manager",    StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])


# ── Unit test data (dirty) ─────────────────────────────
@pytest.fixture
def raw_store_data(spark, store_schema):
    data = [
        ("1",  "  Oxford St  ", "north", "London",     "01/01/2020", "John Smith",  "2024-01-01", "2024-01-01"),
        ("2",  "Westfield",     "SOUTH", "Manchester",  "02/01/2020", "Jane Doe",    "2024-01-02", "2024-01-02"),
        ("2",  "Westfield",     "SOUTH", "Manchester",  "02/01/2020", "Jane Doe",    "2024-01-02", "2024-01-02"),
        ("3",  None,            "EAST",  "Birmingham",  "03/01/2020", "Bob Brown",   "2024-01-03", "2024-01-03"),
        (None, "Leeds Central", "WEST",  "Leeds",       "04/01/2020", "Alice Jones", "2024-01-04", "2024-01-04"),
        ("4",  "  Cardiff  ",   "east",  "Cardiff",     "05/01/2020", "Mike Wilson", "2024-01-05", "2024-01-05"),
    ]
    return spark.createDataFrame(data, schema=store_schema)


# ── DQ test data (clean) ───────────────────────────────
@pytest.fixture
def clean_store_data(spark, store_schema):
    data = [
        ("1",  "Oxford Street",   "NORTH", "London",     "01/01/2020", "John Smith",  "2024-01-01", "2024-01-01"),
        ("2",  "Westfield",       "SOUTH", "Manchester",  "02/01/2020", "Jane Doe",    "2024-01-02", "2024-01-02"),
        ("3",  "Bullring",        "EAST",  "Birmingham",  "03/01/2020", "Bob Brown",   "2024-01-03", "2024-01-03"),
        ("4",  "Cardiff Bay",     "WEST",  "Cardiff",     "04/01/2020", "Alice Jones", "2024-01-04", "2024-01-04"),
        ("5",  "Leeds Central",   "NORTH", "Leeds",       "05/01/2020", "Mike Wilson", "2024-01-05", "2024-01-05"),
        ("6",  "Glasgow Fort",    "NORTH", "Glasgow",     "06/01/2020", "Sarah Davis", "2024-01-06", "2024-01-06"),
        ("7",  "Bluewater",       "SOUTH", "Kent",        "07/01/2020", "Tom Clark",   "2024-01-07", "2024-01-07"),
        ("8",  "Meadowhall",      "NORTH", "Sheffield",   "08/01/2020", "Lucy Evans",  "2024-01-08", "2024-01-08"),
        ("9",  "Trafford Centre", "NORTH", "Manchester",  "09/01/2020", "James White", "2024-01-09", "2024-01-09"),
        ("10", "Lakeside",        "SOUTH", "Essex",       "10/01/2020", "Emma Harris", "2024-01-10", "2024-01-10"),
    ]
    return spark.createDataFrame(data, schema=store_schema)


@pytest.fixture
def transformed_stores(clean_store_data):
    return transform_stores(clean_store_data)


# ── Integration test data (from fixture CSV) ───────────
@pytest.fixture
def bronze_stores_df(spark):
    fixture_path = os.path.join(
        os.path.dirname(__file__),
        "tests", "fixtures", "bronze_stores.csv"
    )
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(fixture_path)
    )


@pytest.fixture
def silver_stores_df(bronze_stores_df):
    return transform_stores(bronze_stores_df)