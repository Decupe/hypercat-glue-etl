from pipelines.orders.transform import transform_orders


class TestOrdersTransformation:

    def test_removes_duplicate_order_ids(self, spark, raw_order_data):
        result = transform_orders(raw_order_data)
        total = result.count()
        distinct = result.select("order_id").distinct().count()
        assert total == distinct

    def test_drops_null_order_id(self, spark, raw_order_data):
        result = transform_orders(raw_order_data)
        nulls = result.filter(result.order_id.isNull()).count()
        assert nulls == 0

    def test_drops_null_customer_id(self, spark, raw_order_data):
        result = transform_orders(raw_order_data)
        nulls = result.filter(result.customer_id.isNull()).count()
        assert nulls == 0

    def test_uppercases_status(self, spark, raw_order_data):
        result = transform_orders(raw_order_data)
        statuses = [r["status"] for r in result.collect()]
        assert all(s == s.upper() for s in statuses)

    def test_trims_whitespace_from_status(self, spark, raw_order_data):
        from pyspark.sql.functions import col, trim
        result = transform_orders(raw_order_data)
        invalid = result.filter(col("status") != trim(col("status"))).count()
        assert invalid == 0

    def test_trims_whitespace_from_payment_method(self, spark, raw_order_data):
        from pyspark.sql.functions import col, trim
        result = transform_orders(raw_order_data)
        invalid = result.filter(
            col("payment_method") != trim(col("payment_method"))
        ).count()
        assert invalid == 0

    def test_output_row_count(self, spark, raw_order_data):
        result = transform_orders(raw_order_data)
        # 9 rows: -1 null order_id, -1 duplicate, -1 null customer_id = 6
        assert result.count() == 6