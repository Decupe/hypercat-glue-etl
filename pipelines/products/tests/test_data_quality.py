from pyspark.sql.functions import col, upper, trim


class TestProductsDataQuality:

    def test_null_rate_product_id(self, transformed_products):
        total = transformed_products.count()
        nulls = transformed_products.filter(
            col("product_id").isNull()
        ).count()
        assert nulls / total == 0, \
            f"Null rate for product_id: {nulls/total:.1%}"

    def test_null_rate_product_name(self, transformed_products):
        total = transformed_products.count()
        nulls = transformed_products.filter(
            col("product_name").isNull()
        ).count()
        assert nulls / total == 0, \
            f"Null rate for product_name: {nulls/total:.1%}"

    def test_null_rate_sku(self, transformed_products):
        total = transformed_products.count()
        nulls = transformed_products.filter(
            col("sku").isNull()
        ).count()
        assert nulls / total == 0, \
            f"Null rate for sku: {nulls/total:.1%}"

    def test_duplicate_rate_product_id(self, transformed_products):
        total = transformed_products.count()
        distinct = transformed_products.select(
            "product_id"
        ).distinct().count()
        duplicate_rate = (total - distinct) / total
        assert duplicate_rate == 0, \
            f"Duplicate rate: {duplicate_rate:.1%}"

    def test_unit_price_positive_rate(self, transformed_products):
        """Unit price <= 0 is never acceptable — hard zero tolerance"""
        invalid = transformed_products.filter(
            col("unit_price") <= 0
        ).count()
        assert invalid == 0, \
            f"Found {invalid} rows with unit_price <= 0"

    def test_unit_price_null_rate(self, transformed_products):
        total = transformed_products.count()
        nulls = transformed_products.filter(
            col("unit_price").isNull()
        ).count()
        assert nulls / total == 0, \
            f"Null rate for unit_price: {nulls/total:.1%}"

    def test_stock_qty_non_negative_rate(self, transformed_products):
        total = transformed_products.count()
        invalid = transformed_products.filter(
            col("stock_qty") < 0
        ).count()
        assert invalid / total == 0, \
            f"Negative stock_qty rate: {invalid/total:.1%}"

    def test_non_uppercase_category_rate(self, transformed_products):
        total = transformed_products.count()
        invalid = transformed_products.filter(
            col("category") != upper(col("category"))
        ).count()
        assert invalid / total == 0, \
            f"Non-uppercase category rate: {invalid/total:.1%}"

    def test_whitespace_rate_in_product_name(self, transformed_products):
        total = transformed_products.count()
        invalid = transformed_products.filter(
            col("product_name") != trim(col("product_name"))
        ).count()
        assert invalid / total == 0, \
            f"Whitespace rate in product_name: {invalid/total:.1%}"

    def test_minimum_row_count(self, transformed_products):
        assert transformed_products.count() >= 1

    def test_all_required_columns_present(self, transformed_products):
        required = {
            "product_id", "sku", "product_name", "category",
            "unit_price", "stock_qty", "supplier",
            "created_at", "updated_at"
        }
        missing = required - set(transformed_products.columns)
        assert not missing, f"Missing columns: {missing}"