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
        StructField("store_id",    StringType(), True),
        StructField("store_name",  StringType(), True),
        StructField("city",        StringType(), True),
        StructField("region",      StringType(), True),
        StructField("country",     StringType(), True),
        StructField("postcode",    StringType(), True),
        StructField("opened_date", StringType(), True),
        StructField("store_type",  StringType(), True),
    ])


# ── Unit test data (dirty) ─────────────────────────────
@pytest.fixture
def raw_store_data(spark, store_schema):
    data = [
        ("1", "  Hypercat London  ", "London",      "South East",   "england",  "EC1A 1BB", "2018-03-15", "flagship"),
        ("2", "Hypercat Manchester", "Manchester",   "North West",   "England",  "M1 1AE",   "2019-06-01", "Standard"),
        ("2", "Hypercat Manchester", "Manchester",   "North West",   "England",  "M1 1AE",   "2019-06-01", "Standard"),  # duplicate
        ("3", "Hypercat Birmingham", "  Birmingham", "West Midlands","England",  "B1 1BB",   "2019-09-15", "Standard"),
        (None,"Hypercat Glasgow",    "Glasgow",      "Scotland",     "Scotland", "G1 1AA",   "2020-01-10", "Standard"),  # null id
        ("4", None,                  "Bristol",      "South West",   "England",  "BS1 1AA",  "2020-08-20", "Standard"),  # null name
        ("5", "Hypercat Leeds",      "Leeds",        "Yorkshire",    "england",  "LS1 1AA",  "2021-02-14", "standard"),
    ]
    return spark.createDataFrame(data, schema=store_schema)


# ── DQ test data (clean) ───────────────────────────────
@pytest.fixture
def clean_store_data(spark, store_schema):
    data = [
        ("1", "Hypercat London Central", "London",     "South East",   "England",  "EC1A 1BB", "2018-03-15", "Flagship"),
        ("2", "Hypercat Manchester",     "Manchester",  "North West",   "England",  "M1 1AE",   "2019-06-01", "Standard"),
        ("3", "Hypercat Birmingham",     "Birmingham",  "West Midlands","England",  "B1 1BB",   "2019-09-15", "Standard"),
        ("4", "Hypercat Glasgow",        "Glasgow",     "Scotland",     "Scotland", "G1 1AA",   "2020-01-10", "Standard"),
        ("5", "Hypercat Bristol",        "Bristol",     "South West",   "England",  "BS1 1AA",  "2020-08-20", "Standard"),
        ("6", "Hypercat Leeds",          "Leeds",       "Yorkshire",    "England",  "LS1 1AA",  "2021-02-14", "Standard"),
        ("7", "Hypercat Edinburgh",      "Edinburgh",   "Scotland",     "Scotland", "EH1 1AA",  "2021-07-01", "Standard"),
        ("8", "Hypercat Cardiff",        "Cardiff",     "Wales",        "Wales",    "CF1 1AA",  "2022-03-01", "Standard"),
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