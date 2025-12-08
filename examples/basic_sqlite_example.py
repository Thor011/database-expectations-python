"""
Example: Basic SQLite database validation
"""

from db_expectations import DatabaseValidator
from db_expectations.suites import ExpectationSuites
import sqlite3
import os

# Create sample SQLite database
db_path = "example.db"

# Remove if exists
if os.path.exists(db_path):
    os.remove(db_path)

# Create and populate database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create users table
cursor.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        age INTEGER,
        status TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

# Insert sample data
sample_users = [
    (1, "Alice Johnson", "alice@example.com", 28, "active"),
    (2, "Bob Smith", "bob@example.com", 35, "active"),
    (3, "Charlie Brown", "charlie@example.com", 42, "inactive"),
    (4, "Diana Prince", "diana@example.com", 31, "active"),
    (5, "Eve Adams", "eve@example.com", 26, "pending"),
]

cursor.executemany(
    "INSERT INTO users (id, name, email, age, status) VALUES (?, ?, ?, ?, ?)",
    sample_users
)

conn.commit()
conn.close()

print("✓ Sample database created")

# Initialize validator
connection_string = f"sqlite:///{db_path}"
validator = DatabaseValidator(connection_string)

print("✓ Validator initialized")

# Example 1: Validate table with pre-built suites
print("\n=== Example 1: Pre-built Expectation Suites ===")

# Combine multiple suite types
expectations = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["id", "name", "email"]),
    ExpectationSuites.unique_checks(["email"]),
    ExpectationSuites.range_checks({"age": {"min": 18, "max": 100}}),
    ExpectationSuites.set_membership_checks({"status": ["pending", "active", "inactive"]}),
    ExpectationSuites.row_count_check(min_rows=1, max_rows=1000)
)

results = validator.validate_table(
    table_name="users",
    suite_name="users_validation_suite",
    expectations=expectations
)

print(f"Validation success: {results.success}")
print(f"Statistics: {results.statistics}")

# Example 2: Custom query validation
print("\n=== Example 2: Custom Query Validation ===")

query = "SELECT * FROM users WHERE age > 30"

expectations_query = [
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "kwargs": {"column": "email"}
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "age", "min_value": 31, "max_value": 100}
    }
]

results_query = validator.validate_query(
    query=query,
    asset_name="senior_users",
    suite_name="senior_users_suite",
    expectations=expectations_query
)

print(f"Query validation success: {results_query.success}")

# Example 3: Get table metadata
print("\n=== Example 3: Table Metadata ===")

table_info = validator.get_table_info("users")
print(f"Table: {table_info['table_name']}")
print("Columns:")
for col in table_info['columns']:
    print(f"  - {col['name']}: {col['type']} (nullable: {col.get('nullable', True)})")

# Example 4: Query to DataFrame
print("\n=== Example 4: Query to DataFrame ===")

df = validator.query_to_dataframe("SELECT name, email, age FROM users WHERE status = 'active'")
print(f"Active users:\n{df}")

# Example 5: Row count
print("\n=== Example 5: Row Count ===")

row_count = validator.get_row_count("users")
print(f"Total users: {row_count}")

# Cleanup
validator.close()
print("\n✓ Validation complete")

# Optional: Remove database file
# os.remove(db_path)
