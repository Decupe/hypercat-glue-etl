import os
import pytest
from pyspark.sql.types import StructType, StructField, StringType
from pipelines.orders.transform import transform_orders


@pytest.fixture
def order_schema():
    return StructType([
        StructField("order_id",        StringType(), True),
        StructField("customer_id",     StringType(), True),
        StructField("store_id",        StringType(), True),
        StructField("order_date",      StringType(), True),
        StructField("status",          StringType(), True),
        StructField("payment_method",  StringType(), True),
        StructField("discount_pct",    StringType(), True),
        StructField("created_at",      StringType(), True),
        StructField("updated_at",      StringType(), True),
    ])


@pytest.fixture
def raw_order_data(spark, order_schema):
    data = [
        (None,  "334", "8", "27/04/2023", "Returned",    "Voucher",       "0",  "27/04/2023 17:26", "27/04/2023 17:26"),  # null order_id
        ("2",   "260", "6", "19/07/2023", "Completed",   "Card",         "15",  "19/07/2023 06:42", "19/07/2023 06:42"),
        ("2",   "260", "6", "19/07/2023", "Completed",   "Card",         "15",  "19/07/2023 06:42", "19/07/2023 06:42"),  # duplicate
        ("3",   None,  "2", "07/05/2023", "Completed",   "Voucher",       "5",  "07/05/2023 04:05", "07/05/2023 04:05"),  # null customer_id
        ("4",   "419", "3", "19/08/2023", "Cancelled",   "Bank Transfer", "5",  "19/08/2023 10:12", "19/08/2023 10:12"),
        ("5",   "245", "6", "21/07/2023", "   Cancelled","Card",          "0",  "21/07/2023 11:12", "21/07/2023 11:12"),  # whitespace in status
        ("6",   "52",  "1", "30/05/2023", "Completed",   "  Bank Transfer","5", "30/05/2023 23:08", "30/05/2023 23:08"),  # whitespace in payment
        ("7",   "206", "9", "14/10/2023", "Pending",     "Voucher",       "0",  "14/10/2023 12:53", "14/10/2023 12:53"),
        ("8",   "225", "4", "12/08/2023", "Completed",   "Bank Transfer", "0",  "12/08/2023 00:18", "12/08/2023 00:18"),
    ]
    return spark.createDataFrame(data, schema=order_schema)


@pytest.fixture
def clean_order_data(spark, order_schema):
    data = [
        ("1",  "334", "8", "27/04/2023", "RETURNED",   "Voucher",       "0",  "27/04/2023 17:26", "27/04/2023 17:26"),
        ("2",  "260", "6", "19/07/2023", "COMPLETED",  "Card",         "15",  "19/07/2023 06:42", "19/07/2023 06:42"),
        ("3",  "336", "2", "07/05/2023", "COMPLETED",  "Voucher",       "5",  "07/05/2023 04:05", "07/05/2023 04:05"),
        ("4",  "419", "3", "19/08/2023", "CANCELLED",  "Bank Transfer", "5",  "19/08/2023 10:12", "19/08/2023 10:12"),
        ("5",  "245", "6", "21/07/2023", "CANCELLED",  "Card",          "0",  "21/07/2023 11:12", "21/07/2023 11:12"),
        ("6",  "52",  "1", "30/05/2023", "COMPLETED",  "Bank Transfer", "5",  "30/05/2023 23:08", "30/05/2023 23:08"),
        ("7",  "206", "9", "14/10/2023", "PENDING",    "Voucher",       "0",  "14/10/2023 12:53", "14/10/2023 12:53"),
        ("8",  "225", "4", "12/08/2023", "COMPLETED",  "Bank Transfer", "0",  "12/08/2023 00:18", "12/08/2023 00:18"),
        ("9",  "101", "2", "01/01/2023", "COMPLETED",  "Card",          "0",  "01/01/2023 10:00", "01/01/2023 10:00"),
        ("10", "102", "3", "02/01/2023", "PENDING",    "Voucher",       "10", "02/01/2023 11:00", "02/01/2023 11:00"),
    ]
    return spark.createDataFrame(data, schema=order_schema)


@pytest.fixture
def transformed_orders(clean_order_data):
    return transform_orders(clean_order_data)


@pytest.fixture
def bronze_orders_df(spark):
    fixture_path = os.path.join(
        os.path.dirname(__file__),
        "tests", "fixtures", "bronze_orders.csv"
    )
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(fixture_path)
    )


@pytest.fixture
def silver_orders_df(bronze_orders_df):
    return transform_orders(bronze_orders_df)