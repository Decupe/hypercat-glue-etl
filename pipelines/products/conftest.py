import os
import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType
)
from pipelines.products.transform import transform_products


# ── Schema ─────────────────────────────────────────────
@pytest.fixture
def product_schema():
    return StructType([
        StructField("product_id",   StringType(), True),
        StructField("sku",          StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category",     StringType(), True),
        StructField("unit_price",   StringType(), True),
        StructField("stock_qty",    StringType(), True),
        StructField("supplier",     StringType(), True),
        StructField("created_at",   StringType(), True),
        StructField("updated_at",   StringType(), True),
    ])


# ── Unit test data (dirty) ─────────────────────────────
@pytest.fixture
def raw_product_data(spark, product_schema):
    data = [
        ("1",  "LP-001", "  Laptop Pro 15  ", "electronics", "1299.99", "50",  "TechSupply",  "2024-01-01", "2024-06-01"),
        ("2",  "WM-002", "Wireless Mouse",    "Electronics", "49.99",   "200", "MouseCo",     "2024-01-02", "2024-06-01"),
        ("2",  "WM-002", "Wireless Mouse",    "Electronics", "49.99",   "200", "MouseCo",     "2024-01-02", "2024-06-01"),  # duplicate
        ("3",  "BS-003", "  USB-C Hub  ",     "electronics", "89.99",   "150", "HubWorld",    "2024-01-03", "2024-06-01"),
        (None, "MN-004", "Bluetooth Speaker", "Electronics", "129.99",  "75",  "SoundCo",     "2024-01-04", "2024-06-01"),  # null id
        ("4",  "MN-004", None,                "Electronics", "79.99",   "90",  "GenericCo",   "2024-01-05", "2024-06-01"),  # null name
        ("5",  "WC-005", "4K Webcam",         "Electronics", "-10.00",  "30",  "CamSupply",   "2024-01-06", "2024-06-01"),  # negative price
        ("6",  "KY-006", "Mechanical Keyboard","Electronics", "0.00",   "45",  "KeyboardCo",  "2024-01-07", "2024-06-01"),  # zero price
        ("7",  "MO-007", "  Gaming Mouse  ",  "gaming",      "59.99",   "120", "GamingSupply","2024-01-08", "2024-06-01"),
    ]
    return spark.createDataFrame(data, schema=product_schema)


# ── DQ test data (clean) ───────────────────────────────
@pytest.fixture
def clean_product_data(spark, product_schema):
    data = [
        ("1",  "LP-001", "Laptop Pro 15",      "ELECTRONICS", "1299.99", "50",  "TechSupply",   "2024-01-01", "2024-06-01"),
        ("2",  "WM-002", "Wireless Mouse",      "ELECTRONICS", "49.99",   "200", "MouseCo",      "2024-01-02", "2024-06-01"),
        ("3",  "BS-003", "USB-C Hub",           "ELECTRONICS", "89.99",   "150", "HubWorld",     "2024-01-03", "2024-06-01"),
        ("4",  "MN-004", "Bluetooth Speaker",   "ELECTRONICS", "129.99",  "75",  "SoundCo",      "2024-01-04", "2024-06-01"),
        ("5",  "WC-005", "4K Webcam",           "ELECTRONICS", "59.99",   "30",  "CamSupply",    "2024-01-05", "2024-06-01"),
        ("6",  "KY-006", "Mechanical Keyboard", "ELECTRONICS", "149.99",  "45",  "KeyboardCo",   "2024-01-06", "2024-06-01"),
        ("7",  "MO-007", "Gaming Mouse",        "GAMING",      "59.99",   "120", "GamingSupply", "2024-01-07", "2024-06-01"),
        ("8",  "HD-008", "External SSD 1TB",    "ELECTRONICS", "89.99",   "200", "StorageCo",    "2024-01-08", "2024-06-01"),
        ("9",  "WB-009", "Webcam HD",           "ELECTRONICS", "79.99",   "60",  "CamWorld",     "2024-01-09", "2024-06-01"),
        ("10", "HB-010", "USB Hub 7-Port",      "ELECTRONICS", "39.99",   "300", "HubCo",        "2024-01-10", "2024-06-01"),
    ]
    return spark.createDataFrame(data, schema=product_schema)


@pytest.fixture
def transformed_products(clean_product_data):
    return transform_products(clean_product_data)


# ── Integration test data (from fixture CSV) ───────────
@pytest.fixture
def bronze_products_df(spark):
    fixture_path = os.path.join(
        os.path.dirname(__file__),
        "tests", "fixtures", "bronze_products.csv"
    )
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(fixture_path)
    )


@pytest.fixture
def silver_products_df(bronze_products_df):
    return transform_products(bronze_products_df)