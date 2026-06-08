from pipelines.customers.transform import transform_customers


class TestCustomersTransformation:

    def test_removes_duplicate_customer_ids(self, spark, raw_customer_data):
        result = transform_customers(raw_customer_data)
        total = result.count()
        distinct = result.select("customer_id").distinct().count()
        assert total == distinct, \
            "Duplicate customer_ids found in output"

    def test_drops_null_customer_id(self, spark, raw_customer_data):
        result = transform_customers(raw_customer_data)
        nulls = result.filter(result.customer_id.isNull()).count()
        assert nulls == 0, \
            f"Found {nulls} null customer_ids"

    def test_drops_null_email(self, spark, raw_customer_data):
        result = transform_customers(raw_customer_data)
        nulls = result.filter(result.email.isNull()).count()
        assert nulls == 0, \
            f"Found {nulls} null emails"

    def test_trims_whitespace_from_first_name(self, spark, raw_customer_data):
        result = transform_customers(raw_customer_data)
        alice = result.filter(result.customer_id == 1).first()
        assert alice["first_name"] == "Alice", \
            f"Expected 'Alice' but got '{alice['first_name']}'"

    def test_trims_whitespace_from_email(self, spark, raw_customer_data):
        result = transform_customers(raw_customer_data)
        emma = result.filter(result.customer_id == 4).first()
        assert emma["first_name"] == "Emma", \
            f"Expected 'Emma' but got '{emma['first_name']}'"

    def test_uppercases_country(self, spark, raw_customer_data):
        result = transform_customers(raw_customer_data)
        countries = [r["country"] for r in result.collect()]
        assert all(c == c.upper() for c in countries), \
            f"Not all countries uppercased: {countries}"

    def test_output_row_count(self, spark, raw_customer_data):
        result = transform_customers(raw_customer_data)
        assert result.count() == 3, \
            f"Expected 3 rows but got {result.count()}"