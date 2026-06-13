import os


class TestStoresIntegration:

    def test_bronze_file_loads_successfully(self, bronze_stores_df):
        assert bronze_stores_df.count() > 0, \
            "Bronze file is empty or failed to load"

    def test_bronze_has_required_columns(self, bronze_stores_df):
        required = {
            "store_id", "store_name", "city", "region",
            "open_date", "manager", "created_at", "updated_at"
        }
        missing = required - set(bronze_stores_df.columns)
        assert not missing, \
            f"Bronze CSV missing columns: {missing}"

    def test_silver_is_not_empty(self, silver_stores_df):
        assert silver_stores_df.count() > 0, \
            "Silver output is empty"

    def test_survival_rate_is_acceptable(self, bronze_stores_df, silver_stores_df):
        bronze_count = bronze_stores_df.count()
        silver_count = silver_stores_df.count()
        survival_rate = silver_count / bronze_count
        assert survival_rate >= 0.80, \
            f"Survival rate too low: {survival_rate:.0%} " \
            f"({silver_count}/{bronze_count} rows survived)"

    def test_null_rate_in_key_columns(self, silver_stores_df):
        from pyspark.sql.functions import col
        total = silver_stores_df.count()
        nulls = silver_stores_df.filter(
            col("store_id").isNull() |
            col("store_name").isNull()
        ).count()
        assert nulls / total <= 0.01, \
            f"Null rate too high: {nulls/total:.1%}"

    def test_silver_schema_is_correct(self, silver_stores_df):
        required = {
            "store_id", "store_name", "city", "region",
            "open_date", "manager", "created_at", "updated_at"
        }
        missing = required - set(silver_stores_df.columns)
        assert not missing, \
            f"Silver missing columns: {missing}"

    def test_silver_region_is_uppercase(self, silver_stores_df):
        from pyspark.sql.functions import col, upper
        total = silver_stores_df.count()
        invalid = silver_stores_df.filter(
            col("region") != upper(col("region"))
            ).count()
        assert invalid / total == 0, \
            f"Non-uppercase region rate: {invalid/total:.1%}"

    def test_silver_duplicate_rate(self, silver_stores_df):
        total = silver_stores_df.count()
        distinct = silver_stores_df.select(
            "store_id"
        ).distinct().count()
        duplicate_rate = (total - distinct) / total
        assert duplicate_rate == 0, \
            f"Duplicate rate: {duplicate_rate:.1%}"

    def test_silver_writes_to_local_parquet(self, silver_stores_df, tmp_path):
        output_path = str(tmp_path / "stores_silver")
        silver_stores_df.write.mode("overwrite").parquet(output_path)
        parquet_files = [
            f for f in os.listdir(output_path)
            if f.endswith(".parquet")
        ]
        assert len(parquet_files) > 0

    def test_silver_parquet_is_readable(self, silver_stores_df, spark, tmp_path):
        output_path = str(tmp_path / "stores_silver_readable")
        silver_stores_df.write.mode("overwrite").parquet(output_path)
        read_back = spark.read.parquet(output_path)
        assert read_back.count() == silver_stores_df.count()