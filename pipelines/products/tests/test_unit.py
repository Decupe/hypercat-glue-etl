from pipelines.products.transform import transform_products


class TestProductsTransformation:

    def test_removes_duplicate_product_ids(self, spark, raw_product_data):
        result = transform_products(raw_product_data)
        total = result.count()
        distinct = result.select("product_id").distinct().count()
        assert total == distinct, "Duplicate product_ids found"

    def test_drops_null_product_id(self, spark, raw_product_data):
        result = transform_products(raw_product_data)
        nulls = result.filter(result.product_id.isNull()).count()
        assert nulls == 0, f"Found {nulls} null product_ids"

    def test_drops_null_product_name(self, spark, raw_product_data):
        result = transform_products(raw_product_data)
        nulls = result.filter(result.product_name.isNull()).count()
        assert nulls == 0, f"Found {nulls} null product_names"

    def test_drops_negative_unit_price(self, spark, raw_product_data):
        result = transform_products(raw_product_data)
        invalid = result.filter(result.unit_price <= 0).count()
        assert invalid == 0, \
            f"Found {invalid} rows with unit_price <= 0"

    def test_trims_whitespace_from_product_name(self, spark, raw_product_data):
        result = transform_products(raw_product_data)
        laptop = result.filter(result.product_id == 1).first()
        assert laptop["product_name"] == "Laptop Pro 15", \
            f"Expected 'Laptop Pro 15' but got '{laptop['product_name']}'"

    def test_trims_whitespace_from_sku(self, spark, raw_product_data):
        result = transform_products(raw_product_data)
        usb_hub = result.filter(result.product_id == 3).first()
        assert usb_hub["sku"] == "BS-003", \
            f"Expected 'BS-003' but got '{usb_hub['sku']}'"

    def test_uppercases_category(self, spark, raw_product_data):
        result = transform_products(raw_product_data)
        categories = [r["category"] for r in result.collect()]
        assert all(c == c.upper() for c in categories), \
            f"Not all categories uppercased: {categories}"

    def test_output_row_count(self, spark, raw_product_data):
        result = transform_products(raw_product_data)
        # 9 rows:
        # -1 duplicate  (product_id=2)
        # -1 null id    (product_id=None)
        # -1 null name  (product_id=4)
        # -1 negative price (product_id=5)
        # -1 zero price (product_id=6)
        # = 4 valid rows
        assert result.count() == 4, \
            f"Expected 4 rows but got {result.count()}"