from pipelines.order_items.transform import transform_order_items


class TestOrderItemsTransformation:

    def test_removes_duplicate_order_item_ids(self, spark, raw_order_item_data):
        result = transform_order_items(raw_order_item_data)
        total = result.count()
        distinct = result.select("order_item_id").distinct().count()
        assert total == distinct

    def test_drops_null_order_item_id(self, spark, raw_order_item_data):
        result = transform_order_items(raw_order_item_data)
        nulls = result.filter(result.order_item_id.isNull()).count()
        assert nulls == 0

    def test_drops_null_order_id(self, spark, raw_order_item_data):
        result = transform_order_items(raw_order_item_data)
        nulls = result.filter(result.order_id.isNull()).count()
        assert nulls == 0

    def test_drops_negative_unit_price(self, spark, raw_order_item_data):
        result = transform_order_items(raw_order_item_data)
        invalid = result.filter(result.unit_price <= 0).count()
        assert invalid == 0

    def test_drops_zero_quantity(self, spark, raw_order_item_data):
        result = transform_order_items(raw_order_item_data)
        invalid = result.filter(result.quantity <= 0).count()
        assert invalid == 0

    def test_output_row_count(self, spark, raw_order_item_data):
        result = transform_order_items(raw_order_item_data)
        # 9 rows: -1 duplicate, -1 null id,
        # -1 null order_id, -1 negative price, -1 zero price = 4
        assert result.count() == 4