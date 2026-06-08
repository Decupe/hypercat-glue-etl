import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pipelines.customers.transform import transform_customers

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
    logger.info("Customers job started")
    logger.info(f"Reading from: {BRONZE_PATH}")

    bronze_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(BRONZE_PATH)
    )

    bronze_count = bronze_df.count()
    logger.info(f"Bronze row count: {bronze_count}")

    silver_df = transform_customers(bronze_df)

    silver_count = silver_df.count()
    survival_rate = silver_count / bronze_count
    logger.info(f"Silver row count: {silver_count}")
    logger.info(f"Rows dropped: {bronze_count - silver_count}")
    logger.info(f"Survival rate: {survival_rate:.1%}")

    if survival_rate < 0.85:
        raise ValueError(
            f"Survival rate {survival_rate:.1%} below threshold 85%"
        )

    logger.info(f"Writing to: {SILVER_PATH}")
    (
        silver_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )

    logger.info("Customers job completed successfully")
    job.commit()


if __name__ == "__main__":
    run()