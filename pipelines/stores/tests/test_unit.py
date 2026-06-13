from pyspark.sql.functions import col, trim, upper
from pipelines.stores.transform import transform_stores


class TestStoresTransformation:

    def test_removes_duplicate_store_ids(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        assert result.count() < raw_store_data.count()

    def test_drops_null_store_id(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        assert result.filter(col("store_id").isNull()).count() == 0

    def test_drops_null_store_name(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        assert result.filter(col("store_name").isNull()).count() == 0

    def test_trims_whitespace_from_store_name(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        trimmed = result.filter(col("store_name") != trim(col("store_name")))
        assert trimmed.count() == 0

    def test_trims_whitespace_from_city(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        trimmed = result.filter(col("city") != trim(col("city")))
        assert trimmed.count() == 0

    def test_uppercases_region(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        non_upper = result.filter(col("region") != upper(col("region")))
        assert non_upper.count() == 0

    def test_output_row_count(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        assert result.count() > 0