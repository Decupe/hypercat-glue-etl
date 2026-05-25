import os
import pytest
from pyspark.sql.types import StructType, StructField, StringType
from pipelines.order_items.transform import transform_order_items


@pytest.fixture
def order_item_schema():
    return StructType([
        StructField("order_item_id", StringType(), True),
        StructField("order_id",      StringType(), True),
        StructField("product_id",    StringType(), True),
        StructField("quantity",      StringType(), True),
        StructField("unit_price",    StringType(), True),
        StructField("discount_pct",  StringType(), True),
        StructField("created_at",    StringType(), True),
        StructField("updated_at",    StringType(), True),
    ])


@pytest.fixture
def raw_order_item_data(spark, order_item_schema):
    data = [
        ("1",  "1",  "101", "2",  "29.99",  "0",  "2024-01-01", "2024-01-01"),
        ("2",  "1",  "102", "1",  "49.99",  "10", "2024-01-01", "2024-01-01"),
        ("2",  "1",  "102", "1",  "49.99",  "10", "2024-01-01", "2024-01-01"),  # duplicate
        (None, "2",  "103", "3",  "9.99",   "0",  "2024-01-02", "2024-01-02"),  # null id
        ("4",  None, "104", "1",  "99.99",  "5",  "2024-01-02", "2024-01-02"),  # null order_id
        ("5",  "3",  "105", "2",  "-15.00", "0",  "2024-01-03", "2024-01-03"),  # negative price
        ("6",  "3",  "106", "1",  "0.00",   "0",  "2024-01-03", "2024-01-03"),  # zero price
        ("7",  "4",  "107", "5",  "19.99",  "0",  "2024-01-04", "2024-01-04"),
        ("8",  "5",  "108", "2",  "79.99",  "15", "2024-01-05", "2024-01-05"),
    ]
    return spark.createDataFrame(data, schema=order_item_schema)


@pytest.fixture
def clean_order_item_data(spark, order_item_schema):
    data = [
        ("1",  "1",  "101", "2",  "29.99",  "0",  "2024-01-01", "2024-01-01"),
        ("2",  "1",  "102", "1",  "49.99",  "10", "2024-01-01", "2024-01-01"),
        ("3",  "2",  "103", "3",  "9.99",   "0",  "2024-01-02", "2024-01-02"),
        ("4",  "2",  "104", "1",  "99.99",  "5",  "2024-01-02", "2024-01-02"),
        ("5",  "3",  "105", "2",  "15.00",  "0",  "2024-01-03", "2024-01-03"),
        ("6",  "3",  "106", "1",  "19.99",  "0",  "2024-01-03", "2024-01-03"),
        ("7",  "4",  "107", "5",  "19.99",  "0",  "2024-01-04", "2024-01-04"),
        ("8",  "5",  "108", "2",  "79.99",  "15", "2024-01-05", "2024-01-05"),
        ("9",  "6",  "109", "1",  "39.99",  "0",  "2024-01-06", "2024-01-06"),
        ("10", "7",  "110", "3",  "59.99",  "10", "2024-01-07", "2024-01-07"),
    ]
    return spark.createDataFrame(data, schema=order_item_schema)


@pytest.fixture
def transformed_order_items(clean_order_item_data):
    return transform_order_items(clean_order_item_data)


@pytest.fixture
def bronze_order_items_df(spark):
    fixture_path = os.path.join(
        os.path.dirname(__file__),
        "tests", "fixtures", "bronze_order_items.csv"
    )
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(fixture_path)
    )


@pytest.fixture
def silver_order_items_df(bronze_order_items_df):
    return transform_order_items(bronze_order_items_df)