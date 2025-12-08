"""
Example: PostgreSQL database validation with decorators
"""

from db_expectations import DatabaseValidator, validate_before, validate_after
from db_expectations.suites import ExpectationSuites
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL connection string
# Format: postgresql://user:password@host:port/database
connection_string = os.getenv(
    "POSTGRES_CONNECTION",
    "postgresql://postgres:password@localhost:5432/testdb"
)

# Initialize validator
validator = DatabaseValidator(connection_string)

print("✓ Connected to PostgreSQL")

# Example function with pre-validation decorator
@validate_before(
    validator,
    table_name="orders",
    expectations=ExpectationSuites.row_count_check(min_rows=0, max_rows=10000)
)
def insert_order(order_id, customer_id, amount):
    """Insert order with pre-validation to ensure table isn't at capacity."""
    query = f"""
        INSERT INTO orders (order_id, customer_id, amount, status)
        VALUES ({order_id}, {customer_id}, {amount}, 'pending')
    """
    # Execute insert (simplified for example)
    print(f"Inserting order {order_id}")
    return order_id


# Example function with post-validation decorator
@validate_after(
    validator,
    table_name="customers",
    expectations=ExpectationSuites.unique_checks(["email"])
)
def update_customer_email(customer_id, new_email):
    """Update customer email with post-validation to ensure uniqueness."""
    query = f"""
        UPDATE customers
        SET email = '{new_email}'
        WHERE customer_id = {customer_id}
    """
    # Execute update (simplified for example)
    print(f"Updated customer {customer_id} email to {new_email}")
    return True


# Example: Complex validation suite
print("\n=== Complex Validation Suite ===")

# Combine multiple validation types
expectations = ExpectationSuites.combine(
    # Null checks for required fields
    ExpectationSuites.null_checks([
        "order_id",
        "customer_id",
        "amount",
        "status",
        "created_at"
    ]),
    
    # Type checks
    ExpectationSuites.type_checks({
        "order_id": "int",
        "customer_id": "int",
        "amount": "float",
        "status": "str",
        "created_at": "datetime"
    }),
    
    # Range checks
    ExpectationSuites.range_checks({
        "amount": {"min": 0.01, "max": 1000000.00}
    }),
    
    # Set membership for status
    ExpectationSuites.set_membership_checks({
        "status": ["pending", "processing", "shipped", "delivered", "cancelled"]
    }),
    
    # Row count
    ExpectationSuites.row_count_check(min_rows=0, max_rows=100000)
)

results = validator.validate_table(
    table_name="orders",
    suite_name="orders_comprehensive_suite",
    expectations=expectations
)

print(f"Comprehensive validation success: {results.success}")

# Example: Validate data freshness
print("\n=== Data Freshness Validation ===")

freshness_expectations = ExpectationSuites.data_freshness_check(
    timestamp_column="created_at",
    max_age_hours=24
)

results_freshness = validator.validate_query(
    query="SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '24 HOURS'",
    asset_name="recent_orders",
    suite_name="freshness_suite",
    expectations=freshness_expectations
)

print(f"Freshness validation success: {results_freshness.success}")

# Example: Completeness check (allow some nulls)
print("\n=== Completeness Validation ===")

# Expect at least 95% of rows to have non-null values
completeness_expectations = ExpectationSuites.completeness_check(
    columns=["email", "phone"],
    threshold=0.95
)

results_completeness = validator.validate_table(
    table_name="customers",
    suite_name="completeness_suite",
    expectations=completeness_expectations
)

print(f"Completeness validation success: {results_completeness.success}")

# Cleanup
validator.close()
print("\n✓ PostgreSQL validation complete")
