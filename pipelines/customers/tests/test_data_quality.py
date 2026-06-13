import re
import pytest
from pyspark.sql.functions import col, upper, trim


class TestCustomersDataQuality:

    def test_null_rate_customer_id(self, transformed_customers):
        total = transformed_customers.count()
        nulls = transformed_customers.filter(
            col("customer_id").isNull()
        ).count()
        null_rate = nulls / total if total > 0 else 0
        assert null_rate <= 0.01, f"Null rate too high: {null_rate:.0%}"

    def test_null_rate_email(self, transformed_customers):
        total = transformed_customers.count()
        nulls = transformed_customers.filter(
            col("email").isNull()
        ).count()
        null_rate = nulls / total if total > 0 else 0
        assert null_rate <= 0.01, f"Null rate too high: {null_rate:.0%}"

    def test_duplicate_rate_customer_id(self, transformed_customers):
        total = transformed_customers.count()
        distinct = transformed_customers.select("customer_id").distinct().count()
        dup_rate = (total - distinct) / total if total > 0 else 0
        assert dup_rate <= 0.01, f"Duplicate rate too high: {dup_rate:.0%}"

    def test_duplicate_rate_email(self, transformed_customers):
        total = transformed_customers.count()
        distinct = transformed_customers.select("email").distinct().count()
        dup_rate = (total - distinct) / total if total > 0 else 0
        assert dup_rate <= 0.01, f"Duplicate rate too high: {dup_rate:.0%}"

    def test_invalid_email_rate(self, transformed_customers):
        total = transformed_customers.count()
        valid = transformed_customers.filter(
            col("email").rlike(r'^[^@]+@[^@]+\.[^@]+$')
        ).count()
        invalid_rate = (total - valid) / total if total > 0 else 0
        assert invalid_rate <= 0.01, f"Invalid email rate too high: {invalid_rate:.0%}"

    def test_non_uppercase_region_rate(self, transformed_customers):
        total = transformed_customers.count()
        non_upper = transformed_customers.filter(
            col("region") != upper(col("region"))
        ).count()
        rate = non_upper / total if total > 0 else 0
        assert rate == 0, f"Non-uppercase region rate: {rate:.0%}"

    def test_non_uppercase_loyalty_tier_rate(self, transformed_customers):
        total = transformed_customers.count()
        non_upper = transformed_customers.filter(
            col("loyalty_tier") != upper(col("loyalty_tier"))
        ).count()
        rate = non_upper / total if total > 0 else 0
        assert rate == 0, f"Non-uppercase loyalty_tier rate: {rate:.0%}"

    def test_whitespace_rate_in_names(self, transformed_customers):
        total = transformed_customers.count()
        whitespace = transformed_customers.filter(
            (col("first_name") != trim(col("first_name"))) |
            (col("last_name") != trim(col("last_name")))
        ).count()
        rate = whitespace / total if total > 0 else 0
        assert rate == 0, f"Whitespace rate too high: {rate:.0%}"

    def test_minimum_row_count(self, transformed_customers):
        assert transformed_customers.count() >= 1

    def test_all_required_columns_present(self, transformed_customers):
        required = {
            "customer_id", "first_name", "last_name",
            "email", "region", "created_at"
        }
        missing = required - set(transformed_customers.columns)
        assert not missing, f"Missing columns: {missing}"