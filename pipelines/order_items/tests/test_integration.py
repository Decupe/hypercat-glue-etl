import os


class TestOrderItemsIntegration:

    def test_bronze_file_loads_successfully(self, bronze_order_items_df):
        assert bronze_order_items_df.count() > 0

    def test_bronze_has_required_columns(self, bronze_order_items_df):
        required = {
            "order_item_id", "order_id", "product_id",
            "quantity", "unit_price", "discount_pct",
            "created_at", "updated_at"
        }
        missing = required - set(bronze_order_items_df.columns)
        assert not missing

    def test_silver_is_not_empty(self, silver_order_items_df):
        assert silver_order_items_df.count() > 0

    def test_survival_rate_is_acceptable(self, bronze_order_items_df, silver_order_items_df):
        bronze_count = bronze_order_items_df.count()
        silver_count = silver_order_items_df.count()
        survival_rate = silver_count / bronze_count
        assert survival_rate >= 0.70, \
            f"Survival rate too low: {survival_rate:.0%}"

    def test_null_rate_in_key_columns(self, silver_order_items_df):
        from pyspark.sql.functions import col
        total = silver_order_items_df.count()
        nulls = silver_order_items_df.filter(
            col("order_item_id").isNull() |
            col("order_id").isNull()
        ).count()
        assert nulls / total <= 0.01

    def test_silver_schema_is_correct(self, silver_order_items_df):
        required = {
            "order_item_id", "order_id", "product_id",
            "quantity", "unit_price", "discount_pct",
            "created_at", "updated_at"
        }
        missing = required - set(silver_order_items_df.columns)
        assert not missing

    def test_silver_unit_price_is_positive(self, silver_order_items_df):
        from pyspark.sql.functions import col
        invalid = silver_order_items_df.filter(
            col("unit_price") <= 0
        ).count()
        assert invalid == 0

    def test_silver_quantity_is_positive(self, silver_order_items_df):
        from pyspark.sql.functions import col
        invalid = silver_order_items_df.filter(
            col("quantity") <= 0
        ).count()
        assert invalid == 0

    def test_silver_duplicate_rate(self, silver_order_items_df):
        total = silver_order_items_df.count()
        distinct = silver_order_items_df.select(
            "order_item_id"
        ).distinct().count()
        assert (total - distinct) / total == 0

    def test_silver_writes_to_local_parquet(self, silver_order_items_df, tmp_path):
        output_path = str(tmp_path / "order_items_silver")
        silver_order_items_df.write.mode("overwrite").parquet(output_path)
        parquet_files = [
            f for f in os.listdir(output_path)
            if f.endswith(".parquet")
        ]
        assert len(parquet_files) > 0

    def test_silver_parquet_is_readable(self, silver_order_items_df, spark, tmp_path):
        output_path = str(tmp_path / "order_items_silver_readable")
        silver_order_items_df.write.mode("overwrite").parquet(output_path)
        read_back = spark.read.parquet(output_path)
        assert read_back.count() == silver_order_items_df.count()