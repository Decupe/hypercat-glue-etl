import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pipelines.order_items.transform import transform_order_items

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_bucket",
    "silver_bucket",
    "bronze_prefix",
    "silver_prefix",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE_PATH = f"s3://{args['bronze_bucket']}/{args['bronze_prefix']}"
SILVER_PATH = f"s3://{args['silver_bucket']}/{args['silver_prefix']}"


def run():
    logger.info("Order items job started")
    logger.info(f"Reading from: {BRONZE_PATH}")

    bronze_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(BRONZE_PATH)
    )

    bronze_count = bronze_df.count()
    logger.info(f"Bronze row count: {bronze_count}")

    silver_df = transform_order_items(bronze_df)

    silver_count = silver_df.count()
    survival_rate = silver_count / bronze_count
    logger.info(f"Silver row count: {silver_count}")
    logger.info(f"Rows dropped: {bronze_count - silver_count}")
    logger.info(f"Survival rate: {survival_rate:.1%}")

    if survival_rate < 0.75:
        raise ValueError(
            f"Survival rate {survival_rate:.1%} below threshold 75%"
        )

    # Extra order_items-specific validations
    negative_prices = silver_df.filter(
        silver_df.unit_price <= 0
    ).count()

    if negative_prices > 0:
        raise ValueError(
            f"Found {negative_prices} order items with unit_price <= 0 "
            f"in silver — pipeline aborted"
        )

    zero_quantities = silver_df.filter(
        silver_df.quantity <= 0
    ).count()

    if zero_quantities > 0:
        raise ValueError(
            f"Found {zero_quantities} order items with quantity <= 0 "
            f"in silver — pipeline aborted"
        )

    null_orders = silver_df.filter(
        silver_df.order_id.isNull()
    ).count()

    if null_orders > 0:
        raise ValueError(
            f"Found {null_orders} order items with null order_id "
            f"in silver — pipeline aborted"
        )

    logger.info(f"Writing to: {SILVER_PATH}")
    (
        silver_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )

    logger.info("Order items job completed successfully")
    job.commit()


if __name__ == "__main__":
    run()