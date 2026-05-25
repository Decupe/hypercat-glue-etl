from pyspark.sql.functions import col, upper, trim


class TestOrdersDataQuality:

    def test_null_rate_order_id(self, transformed_orders):
        total = transformed_orders.count()
        nulls = transformed_orders.filter(col("order_id").isNull()).count()
        assert nulls / total == 0

    def test_null_rate_customer_id(self, transformed_orders):
        total = transformed_orders.count()
        nulls = transformed_orders.filter(col("customer_id").isNull()).count()
        assert nulls / total == 0

    def test_duplicate_rate_order_id(self, transformed_orders):
        total = transformed_orders.count()
        distinct = transformed_orders.select("order_id").distinct().count()
        assert (total - distinct) / total == 0

    def test_status_is_uppercase(self, transformed_orders):
        total = transformed_orders.count()
        invalid = transformed_orders.filter(
            col("status") != upper(col("status"))
        ).count()
        assert invalid / total == 0

    def test_valid_status_values(self, transformed_orders):
        valid = ["COMPLETED", "CANCELLED", "PENDING", "RETURNED", "PROCESSING"]
        total = transformed_orders.count()
        invalid = transformed_orders.filter(
            ~col("status").isin(valid)
        ).count()
        assert invalid / total == 0

    def test_no_whitespace_in_payment_method(self, transformed_orders):
        total = transformed_orders.count()
        invalid = transformed_orders.filter(
            col("payment_method") != trim(col("payment_method"))
        ).count()
        assert invalid / total == 0

    def test_discount_pct_is_non_negative(self, transformed_orders):
        total = transformed_orders.count()
        invalid = transformed_orders.filter(col("discount_pct") < 0).count()
        assert invalid / total == 0

    def test_minimum_row_count(self, transformed_orders):
        assert transformed_orders.count() >= 1

    def test_all_required_columns_present(self, transformed_orders):
        required = {
            "order_id", "customer_id", "store_id", "order_date",
            "status", "payment_method", "discount_pct",
            "created_at", "updated_at"
        }
        missing = required - set(transformed_orders.columns)
        assert not missing, f"Missing columns: {missing}"