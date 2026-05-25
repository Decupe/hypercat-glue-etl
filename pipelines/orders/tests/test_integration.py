import os


class TestOrdersIntegration:

    def test_bronze_file_loads_successfully(self, bronze_orders_df):
        assert bronze_orders_df.count() > 0

    def test_bronze_has_required_columns(self, bronze_orders_df):
        required = {
            "order_id", "customer_id", "store_id", "order_date",
            "status", "payment_method", "discount_pct",
            "created_at", "updated_at"
        }
        missing = required - set(bronze_orders_df.columns)
        assert not missing

    def test_silver_is_not_empty(self, silver_orders_df):
        assert silver_orders_df.count() > 0

    def test_survival_rate_is_acceptable(self, bronze_orders_df, silver_orders_df):
        bronze_count = bronze_orders_df.count()
        silver_count = silver_orders_df.count()
        survival_rate = silver_count / bronze_count
        assert survival_rate >= 0.80, \
            f"Survival rate too low: {survival_rate:.0%}"

    def test_null_rate_in_key_columns(self, silver_orders_df):
        from pyspark.sql.functions import col
        total = silver_orders_df.count()
        nulls = silver_orders_df.filter(
            col("order_id").isNull() |
            col("customer_id").isNull()
        ).count()
        assert nulls / total <= 0.01

    def test_silver_schema_is_correct(self, silver_orders_df):
        required = {
            "order_id", "customer_id", "store_id", "order_date",
            "status", "payment_method", "discount_pct",
            "created_at", "updated_at"
        }
        missing = required - set(silver_orders_df.columns)
        assert not missing

    def test_silver_status_is_uppercase(self, silver_orders_df):
        from pyspark.sql.functions import col, upper
        total = silver_orders_df.count()
        invalid = silver_orders_df.filter(
            col("status") != upper(col("status"))
        ).count()
        assert invalid / total == 0

    def test_silver_duplicate_rate(self, silver_orders_df):
        total = silver_orders_df.count()
        distinct = silver_orders_df.select("order_id").distinct().count()
        assert (total - distinct) / total == 0

    def test_silver_writes_to_local_parquet(self, silver_orders_df, tmp_path):
        output_path = str(tmp_path / "orders_silver")
        silver_orders_df.write.mode("overwrite").parquet(output_path)
        parquet_files = [
            f for f in os.listdir(output_path)
            if f.endswith(".parquet")
        ]
        assert len(parquet_files) > 0

    def test_silver_parquet_is_readable(self, silver_orders_df, spark, tmp_path):
        output_path = str(tmp_path / "orders_silver_readable")
        silver_orders_df.write.mode("overwrite").parquet(output_path)
        read_back = spark.read.parquet(output_path)
        assert read_back.count() == silver_orders_df.count()
