from pyspark.sql.functions import col, upper, trim
from pipelines.stores.transform import transform_stores


class TestStoresDataQuality:

    def test_null_rate_store_id(self, transformed_stores):
        total = transformed_stores.count()
        nulls = transformed_stores.filter(col("store_id").isNull()).count()
        assert nulls / total <= 0.01

    def test_null_rate_store_name(self, transformed_stores):
        total = transformed_stores.count()
        nulls = transformed_stores.filter(col("store_name").isNull()).count()
        assert nulls / total <= 0.01

    def test_null_rate_city(self, transformed_stores):
        total = transformed_stores.count()
        nulls = transformed_stores.filter(col("city").isNull()).count()
        assert nulls / total <= 0.01

    def test_duplicate_rate_store_id(self, transformed_stores):
        total = transformed_stores.count()
        distinct = transformed_stores.select("store_id").distinct().count()
        assert (total - distinct) / total <= 0.01

    def test_non_uppercase_region_rate(self, transformed_stores):
        total = transformed_stores.count()
        non_upper = transformed_stores.filter(
            col("region") != upper(col("region"))
        ).count()
        assert non_upper / total == 0

    def test_whitespace_rate_in_store_name(self, transformed_stores):
        total = transformed_stores.count()
        whitespace = transformed_stores.filter(
            col("store_name") != trim(col("store_name"))
        ).count()
        assert whitespace / total == 0

    def test_minimum_row_count(self, transformed_stores):
        assert transformed_stores.count() >= 1

    def test_all_required_columns_present(self, transformed_stores):
        required = {
            "store_id", "store_name", "region",
            "city", "created_at", "updated_at"
        }
        missing = required - set(transformed_stores.columns)
        assert not missing, f"Missing columns: {missing}"