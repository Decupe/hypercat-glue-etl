import os


class TestCustomersIntegration:

    def test_bronze_file_loads_successfully(self, bronze_customers_df):
        assert bronze_customers_df is not None

    def test_bronze_has_required_columns(self, bronze_customers_df):
        required = {
            "customer_id", "first_name", "last_name",
            "email", "region", "created_at"
        }
        missing = required - set(bronze_customers_df.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_silver_is_not_empty(self, silver_customers_df):
        assert silver_customers_df.count() > 0

    def test_survival_rate_is_acceptable(
        self, bronze_customers_df, silver_customers_df
    ):
        bronze_count = bronze_customers_df.count()
        silver_count = silver_customers_df.count()
        survival_rate = silver_count / bronze_count
        assert survival_rate >= 0.75, \
            f"Survival rate too low: {survival_rate:.0%}"

    def test_null_rate_in_key_columns_is_acceptable(self, silver_customers_df):
        from pyspark.sql.functions import col
        total = silver_customers_df.count()
        nulls = silver_customers_df.filter(
            col("customer_id").isNull() | col("email").isNull()
        ).count()
        null_rate = nulls / total if total > 0 else 0
        assert null_rate <= 0.01, f"Null rate too high: {null_rate:.0%}"



    def test_silver_region_is_uppercase(self, silver_customers_df):
        from pyspark.sql.functions import col, upper
        non_upper = silver_customers_df.filter(
            col("region") != upper(col("region"))
        )
        assert non_upper.count() == 0

    def test_silver_email_format_is_valid(self, silver_customers_df):
        from pyspark.sql.functions import col
        invalid = silver_customers_df.filter(
            ~col("email").rlike(r'^[^@]+@[^@]+\.[^@]+$')
        )
        assert invalid.count() == 0

    def test_silver_customer_ids_are_unique(self, silver_customers_df):
        total = silver_customers_df.count()
        distinct = silver_customers_df.select("customer_id").distinct().count()
        assert total == distinct

    def test_silver_writes_to_local_parquet(self, silver_customers_df, tmp_path):
        output_path = str(tmp_path / "silver_customers")
        silver_customers_df.coalesce(1).write.mode("overwrite").parquet(output_path)
        assert os.path.exists(output_path)

    def test_silver_parquet_is_readable(self, silver_customers_df, tmp_path, spark):
        output_path = str(tmp_path / "silver_customers_read")
        silver_customers_df.coalesce(1).write.mode("overwrite").parquet(output_path)
        read_back = spark.read.parquet(output_path)
        assert read_back.count() > 0    
        
        def test_silver_schema_is_correct(self, silver_customers_df):
            columns = set(silver_customers_df.columns)
            required = {
            "customer_id", "first_name", "last_name",
            "email", "region", "created_at"
        }
            assert required.issubset(columns)