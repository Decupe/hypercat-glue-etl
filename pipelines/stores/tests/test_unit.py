from pipelines.stores.transform import transform_stores


class TestStoresTransformation:

    def test_removes_duplicate_store_ids(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        total = result.count()
        distinct = result.select("store_id").distinct().count()
        assert total == distinct, "Duplicate store_ids found"

    def test_drops_null_store_id(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        nulls = result.filter(result.store_id.isNull()).count()
        assert nulls == 0, f"Found {nulls} null store_ids"

    def test_drops_null_store_name(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        nulls = result.filter(result.store_name.isNull()).count()
        assert nulls == 0, f"Found {nulls} null store_names"

    def test_trims_whitespace_from_store_name(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        london = result.filter(result.store_id == 1).first()
        assert london["store_name"] == "Hypercat London", \
            f"Expected 'Hypercat London' but got '{london['store_name']}'"

    def test_trims_whitespace_from_city(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        birmingham = result.filter(result.store_id == 3).first()
        assert birmingham["city"] == "Birmingham", \
            f"Expected 'Birmingham' but got '{birmingham['city']}'"

    def test_uppercases_country(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        countries = [r["country"] for r in result.collect()]
        assert all(c == c.upper() for c in countries), \
            f"Not all countries uppercased: {countries}"

    def test_uppercases_store_type(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        types = [r["store_type"] for r in result.collect()]
        assert all(t == t.upper() for t in types), \
            f"Not all store_types uppercased: {types}"

    def test_output_row_count(self, spark, raw_store_data):
        result = transform_stores(raw_store_data)
        assert result.count() == 4, \
            f"Expected 4 rows but got {result.count()}"