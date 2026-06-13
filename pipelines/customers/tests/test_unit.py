from pipelines.customers.transform import transform_customers


class TestCustomersTransformation:

    def test_removes_duplicate_customer_ids(self, raw_customer_data):
        result = transform_customers(raw_customer_data)
        assert result.count() < raw_customer_data.count()

    def test_drops_null_customer_id(self, raw_customer_data, spark):
        from pyspark.sql.types import StructType, StringType, StructField
        null_row = spark.createDataFrame(
            [(None, "John", "Doe", "john@email.com", "NORTH", "Leeds", "1", "GOLD", "01/01/2020", "1", "01/01/2020", "01/01/2020")],
            raw_customer_data.schema
        )
        df = raw_customer_data.union(null_row)
        result = transform_customers(df)
        assert result.filter(result.customer_id.isNull()).count() == 0

    def test_drops_null_email(self, raw_customer_data, spark):
        null_row = spark.createDataFrame(
            [("99", "John", "Doe", None, "NORTH", "Leeds", "1", "GOLD", "01/01/2020", "1", "01/01/2020", "01/01/2020")],
            raw_customer_data.schema
        )
        df = raw_customer_data.union(null_row)
        result = transform_customers(df)
        assert result.filter(result.email.isNull()).count() == 0

    def test_trims_whitespace_from_first_name(self, raw_customer_data):
        result = transform_customers(raw_customer_data)
        from pyspark.sql.functions import col, trim
        trimmed = result.filter(col("first_name") != trim(col("first_name")))
        assert trimmed.count() == 0

    def test_trims_whitespace_from_email(self, raw_customer_data):
        result = transform_customers(raw_customer_data)
        from pyspark.sql.functions import col, trim
        trimmed = result.filter(col("email") != trim(col("email")))
        assert trimmed.count() == 0

    def test_uppercases_region(self, raw_customer_data):
        result = transform_customers(raw_customer_data)
        from pyspark.sql.functions import col, upper
        non_upper = result.filter(col("region") != upper(col("region")))
        assert non_upper.count() == 0

    def test_uppercases_loyalty_tier(self, raw_customer_data):
        result = transform_customers(raw_customer_data)
        from pyspark.sql.functions import col, upper
        non_upper = result.filter(col("loyalty_tier") != upper(col("loyalty_tier")))
        assert non_upper.count() == 0

    def test_output_row_count(self, raw_customer_data):
        result = transform_customers(raw_customer_data)
        assert result.count() > 0