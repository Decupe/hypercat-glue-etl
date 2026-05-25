from pyspark.sql.functions import col, upper, trim


class TestCustomersDataQuality:

    def test_null_rate_customer_id(self, transformed_customers):
        """Null rate for customer_id must be 0%"""
        total = transformed_customers.count()
        nulls = transformed_customers.filter(
            col("customer_id").isNull()
        ).count()
        assert nulls / total == 0, \
            f"Null rate for customer_id: {nulls/total:.1%}"

    def test_null_rate_email(self, transformed_customers):
        """Null rate for email must be below 1%"""
        total = transformed_customers.count()
        nulls = transformed_customers.filter(
            col("email").isNull()
        ).count()
        assert nulls / total <= 0.01, \
            f"Null rate for email: {nulls/total:.1%}"

    def test_duplicate_rate_customer_id(self, transformed_customers):
        """Duplicate rate for customer_id must be 0%"""
        total = transformed_customers.count()
        distinct = transformed_customers.select(
            "customer_id"
        ).distinct().count()
        duplicate_rate = (total - distinct) / total
        assert duplicate_rate == 0, \
            f"Duplicate rate: {duplicate_rate:.1%}"

    def test_duplicate_rate_email(self, transformed_customers):
        """Duplicate rate for email must be 0%"""
        total = transformed_customers.count()
        distinct = transformed_customers.select(
            "email"
        ).distinct().count()
        duplicate_rate = (total - distinct) / total
        assert duplicate_rate == 0, \
            f"Duplicate rate: {duplicate_rate:.1%}"

    def test_invalid_email_rate(self, transformed_customers):
        """Invalid email rate must be below 1%"""
        total = transformed_customers.count()
        invalid = transformed_customers.filter(
            ~col("email").contains("@") |
            ~col("email").contains(".")
        ).count()
        assert invalid / total <= 0.01, \
            f"Invalid email rate: {invalid/total:.1%}"

    def test_non_uppercase_country_rate(self, transformed_customers):
        """Non-uppercase country rate must be 0%"""
        total = transformed_customers.count()
        invalid = transformed_customers.filter(
            col("country") != upper(col("country"))
        ).count()
        assert invalid / total == 0, \
            f"Non-uppercase rate: {invalid/total:.1%}"

    def test_whitespace_rate_in_names(self, transformed_customers):
        """Whitespace in names rate must be 0%"""
        total = transformed_customers.count()
        invalid = transformed_customers.filter(
            (col("first_name") != trim(col("first_name"))) |
            (col("last_name") != trim(col("last_name")))
        ).count()
        assert invalid / total == 0, \
            f"Whitespace rate: {invalid/total:.1%}"

    def test_minimum_row_count(self, transformed_customers):
        """Output must have at least 1 row"""
        assert transformed_customers.count() >= 1

    def test_all_required_columns_present(self, transformed_customers):
        """All expected columns must be present"""
        required = {
            "customer_id", "first_name", "last_name",
            "email", "country", "created_at"
        }
        missing = required - set(transformed_customers.columns)
        assert not missing, f"Missing columns: {missing}"