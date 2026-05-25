import os


class TestCustomersIntegration:

    def test_bronze_file_loads_successfully(self, bronze_customers_df):
        """Bronze must load with at least 1 row"""
        assert bronze_customers_df.count() > 0, \
            "Bronze file is empty or failed to load"

    def test_bronze_has_required_columns(self, bronze_customers_df):
        """Bronze must contain all expected columns"""
        required = {
            "customer_id", "first_name", "last_name",
            "email", "country", "created_at"
        }
        missing = required - set(bronze_customers_df.columns)
        assert not missing, \
            f"Bronze CSV missing columns: {missing}"

    def test_silver_is_not_empty(self, silver_customers_df):
        """Silver must produce at least 1 row"""
        assert silver_customers_df.count() > 0, \
            "Silver output is empty — pipeline produced nothing"

    def test_survival_rate_is_acceptable(self, bronze_customers_df, silver_customers_df):
        """At least 85% of bronze rows must survive to silver"""
        bronze_count = bronze_customers_df.count()
        silver_count = silver_customers_df.count()
        survival_rate = silver_count / bronze_count
        assert survival_rate >= 0.85, \
            f"Survival rate too low: {survival_rate:.0%} " \
            f"({silver_count}/{bronze_count} rows survived)"

    def test_null_rate_in_key_columns_is_acceptable(self, silver_customers_df):
        """Null rate in key columns must be below 1%"""
        from pyspark.sql.functions import col
        total = silver_customers_df.count()
        nulls = silver_customers_df.filter(
            col("customer_id").isNull() |
            col("email").isNull()
        ).count()
        null_rate = nulls / total
        assert null_rate <= 0.01, \
            f"Null rate too high: {null_rate:.1%}"

    def test_silver_schema_is_correct(self, silver_customers_df):
        """Silver must have all required columns"""
        required = {
            "customer_id", "first_name", "last_name",
            "email", "country", "created_at"
        }
        missing = required - set(silver_customers_df.columns)
        assert not missing, \
            f"Silver missing columns: {missing}"

    def test_silver_country_is_uppercase(self, silver_customers_df):
        """Country column must be fully uppercase"""
        from pyspark.sql.functions import col, upper
        total = silver_customers_df.count()
        invalid = silver_customers_df.filter(
            col("country") != upper(col("country"))
        ).count()
        invalid_rate = invalid / total
        assert invalid_rate == 0, \
            f"Found {invalid} non-uppercase countries"

    def test_silver_email_format_is_valid(self, silver_customers_df):
        """Email format must contain @ and dot"""
        from pyspark.sql.functions import col
        total = silver_customers_df.count()
        invalid = silver_customers_df.filter(
            ~col("email").contains("@") |
            ~col("email").contains(".")
        ).count()
        invalid_rate = invalid / total
        assert invalid_rate <= 0.01, \
            f"Invalid email rate too high: {invalid_rate:.1%}"

    def test_silver_customer_ids_are_unique(self, silver_customers_df):
        """No duplicate customer_ids allowed in silver"""
        total = silver_customers_df.count()
        distinct = silver_customers_df.select(
            "customer_id"
        ).distinct().count()
        duplicate_rate = (total - distinct) / total
        assert duplicate_rate == 0, \
            f"Duplicate rate: {duplicate_rate:.1%}"

    def test_silver_writes_to_local_parquet(self, silver_customers_df, tmp_path):
        """Silver DataFrame must be writable as Parquet"""
        output_path = str(tmp_path / "customers_silver")
        silver_customers_df.write.mode("overwrite").parquet(output_path)
        parquet_files = [
            f for f in os.listdir(output_path)
            if f.endswith(".parquet")
        ]
        assert len(parquet_files) > 0, \
            "No Parquet files written"

    def test_silver_parquet_is_readable(self, silver_customers_df, spark, tmp_path):
        """Written Parquet must be readable with correct row count"""
        output_path = str(tmp_path / "customers_silver_readable")
        silver_customers_df.write.mode("overwrite").parquet(output_path)
        read_back = spark.read.parquet(output_path)
        assert read_back.count() == silver_customers_df.count(), \
            "Row count mismatch after Parquet read"