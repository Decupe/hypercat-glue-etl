import os


class TestProductsIntegration:

    def test_bronze_file_loads_successfully(self, bronze_products_df):
        assert bronze_products_df.count() > 0, \
            "Bronze file is empty or failed to load"

    def test_bronze_has_required_columns(self, bronze_products_df):
        required = {
            "product_id", "sku", "product_name", "category",
            "unit_price", "stock_qty", "supplier",
            "created_at", "updated_at"
        }
        missing = required - set(bronze_products_df.columns)
        assert not missing, \
            f"Bronze CSV missing columns: {missing}"

    def test_silver_is_not_empty(self, silver_products_df):
        assert silver_products_df.count() > 0, \
            "Silver output is empty"

    def test_survival_rate_is_acceptable(self, bronze_products_df, silver_products_df):
        bronze_count = bronze_products_df.count()
        silver_count = silver_products_df.count()
        survival_rate = silver_count / bronze_count
        assert survival_rate >= 0.80, \
            f"Survival rate too low: {survival_rate:.0%} " \
            f"({silver_count}/{bronze_count} rows survived)"

    def test_null_rate_in_key_columns(self, silver_products_df):
        from pyspark.sql.functions import col
        total = silver_products_df.count()
        nulls = silver_products_df.filter(
            col("product_id").isNull() |
            col("product_name").isNull()
        ).count()
        assert nulls / total <= 0.01, \
            f"Null rate too high: {nulls/total:.1%}"

    def test_silver_schema_is_correct(self, silver_products_df):
        required = {
            "product_id", "sku", "product_name", "category",
            "unit_price", "stock_qty", "supplier",
            "created_at", "updated_at"
        }
        missing = required - set(silver_products_df.columns)
        assert not missing, \
            f"Silver missing columns: {missing}"

    def test_silver_category_is_uppercase(self, silver_products_df):
        from pyspark.sql.functions import col, upper
        total = silver_products_df.count()
        invalid = silver_products_df.filter(
            col("category") != upper(col("category"))
        ).count()
        assert invalid / total == 0, \
            f"Non-uppercase category rate: {invalid/total:.1%}"

    def test_silver_unit_price_is_positive(self, silver_products_df):
        from pyspark.sql.functions import col
        invalid = silver_products_df.filter(
            col("unit_price") <= 0
        ).count()
        assert invalid == 0, \
            f"Found {invalid} rows with unit_price <= 0"

    def test_silver_duplicate_rate(self, silver_products_df):
        total = silver_products_df.count()
        distinct = silver_products_df.select(
            "product_id"
        ).distinct().count()
        duplicate_rate = (total - distinct) / total
        assert duplicate_rate == 0, \
            f"Duplicate rate: {duplicate_rate:.1%}"

    def test_silver_writes_to_local_parquet(self, silver_products_df, tmp_path):
        output_path = str(tmp_path / "products_silver")
        silver_products_df.write.mode("overwrite").parquet(output_path)
        parquet_files = [
            f for f in os.listdir(output_path)
            if f.endswith(".parquet")
        ]
        assert len(parquet_files) > 0

    def test_silver_parquet_is_readable(self, silver_products_df, spark, tmp_path):
        output_path = str(tmp_path / "products_silver_readable")
        silver_products_df.write.mode("overwrite").parquet(output_path)
        read_back = spark.read.parquet(output_path)
        assert read_back.count() == silver_products_df.count()