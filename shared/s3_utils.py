import boto3


def get_s3_client():
    return boto3.client("s3", region_name="eu-north-1")


def read_csv_from_s3(spark, bucket: str, key: str):
    """Read a CSV file from S3 into a Spark DataFrame"""
    path = f"s3a://{bucket}/{key}"
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(path)
    )


def write_parquet_to_s3(df, bucket: str, key: str):
    """Write a Spark DataFrame as Parquet to S3"""
    path = f"s3a://{bucket}/{key}"
    df.write.mode("overwrite").parquet(path)


def s3_path_exists(bucket: str, prefix: str) -> bool:
    """Check if any files exist at an S3 path"""
    client = get_s3_client()
    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )
    return response.get("KeyCount", 0) > 0