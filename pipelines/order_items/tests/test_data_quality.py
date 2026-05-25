from pyspark.sql.functions import col


class TestOrderItemsDataQuality:

    def test_null_rate_order_item_id(self, transformed_order_items):
        total = transformed_order_items.count()
        nulls = transformed_order_items.filter(
            col("order_item_id").isNull()
        ).count()
        assert nulls / total == 0

    def test_null_rate_order_id(self, transformed_order_items):
        total = transformed_order_items.count()
        nulls = transformed_order_items.filter(
            col("order_id").isNull()
        ).count()
        assert nulls / total == 0

    def test_duplicate_rate_order_item_id(self, transformed_order_items):
        total = transformed_order_items.count()
        distinct = transformed_order_items.select(
            "order_item_id"
        ).distinct().count()
        assert (total - distinct) / total == 0

    def test_unit_price_is_positive(self, transformed_order_items):
        invalid = transformed_order_items.filter(
            col("unit_price") <= 0
        ).count()
        assert invalid == 0

    def test_quantity_is_positive(self, transformed_order_items):
        invalid = transformed_order_items.filter(
            col("quantity") <= 0
        ).count()
        assert invalid == 0

    def test_discount_pct_is_non_negative(self, transformed_order_items):
        total = transformed_order_items.count()
        invalid = transformed_order_items.filter(
            col("discount_pct") < 0
        ).count()
        assert invalid / total == 0

    def test_minimum_row_count(self, transformed_order_items):
        assert transformed_order_items.count() >= 1

    def test_all_required_columns_present(self, transformed_order_items):
        required = {
            "order_item_id", "order_id", "product_id",
            "quantity", "unit_price", "discount_pct",
            "created_at", "updated_at"
        }
        missing = required - set(transformed_order_items.columns)
        assert not missing, f"Missing columns: {missing}"