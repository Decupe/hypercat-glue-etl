from pyspark.sql.functions import col, upper, trim


class TestStoresDataQuality:

    def test_null_rate_store_id(self, transformed_stores):
        total = transformed_stores.count()
        nulls = transformed_stores.filter(
            col("store_id").isNull()
        ).count()
        assert nulls / total == 0, \
            f"Null rate for store_id: {nulls/total:.1%}"

    def test_null_rate_store_name(self, transformed_stores):
        total = transformed_stores.count()
        nulls = transformed_stores.filter(
            col("store_name").isNull()
        ).count()
        assert nulls / total == 0, \
            f"Null rate for store_name: {nulls/total:.1%}"

    def test_null_rate_city(self, transformed_stores):
        total = transformed_stores.count()
        nulls = transformed_stores.filter(
            col("city").isNull()
        ).count()
        assert nulls / total == 0, \
            f"Null rate for city: {nulls/total:.1%}"

    def test_duplicate_rate_store_id(self, transformed_stores):
        total = transformed_stores.count()
        distinct = transformed_stores.select(
            "store_id"
        ).distinct().count()
        duplicate_rate = (total - distinct) / total
        assert duplicate_rate == 0, \
            f"Duplicate rate: {duplicate_rate:.1%}"

    def test_non_uppercase_country_rate(self, transformed_stores):
        total = transformed_stores.count()
        invalid = transformed_stores.filter(
            col("country") != upper(col("country"))
        ).count()
        assert invalid / total == 0, \
            f"Non-uppercase country rate: {invalid/total:.1%}"

    def test_non_uppercase_store_type_rate(self, transformed_stores):
        total = transformed_stores.count()
        invalid = transformed_stores.filter(
            col("store_type") != upper(col("store_type"))
        ).count()
        assert invalid / total == 0, \
            f"Non-uppercase store_type rate: {invalid/total:.1%}"

    def test_whitespace_rate_in_store_name(self, transformed_stores):
        total = transformed_stores.count()
        invalid = transformed_stores.filter(
            col("store_name") != trim(col("store_name"))
        ).count()
        assert invalid / total == 0, \
            f"Whitespace rate in store_name: {invalid/total:.1%}"

    def test_invalid_store_type_rate(self, transformed_stores):
        valid_types = ["FLAGSHIP", "STANDARD", "EXPRESS"]
        total = transformed_stores.count()
        invalid = transformed_stores.filter(
            ~col("store_type").isin(valid_types)
        ).count()
        assert invalid / total == 0, \
            f"Invalid store_type rate: {invalid/total:.1%}"

    def test_minimum_row_count(self, transformed_stores):
        assert transformed_stores.count() >= 1

    def test_all_required_columns_present(self, transformed_stores):
        required = {
            "store_id", "store_name", "city", "region",
            "country", "postcode", "opened_date", "store_type"
        }
        missing = required - set(transformed_stores.columns)
        assert not missing, f"Missing columns: {missing}"