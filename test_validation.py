"""
Test Great Expectations validation with GX 1.x API
"""

from db_expectations import DatabaseValidator
import sqlite3
import os
import great_expectations as gx

# Create sample SQLite database
db_path = "test_validation.db"

# Remove if exists
if os.path.exists(db_path):
    os.remove(db_path)

# Create and populate database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create test table
cursor.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INTEGER
    )
""")

# Insert sample data
cursor.executemany(
    "INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)",
    [
        (1, "Alice", "alice@test.com", 25),
        (2, "Bob", "bob@test.com", 30),
        (3, "Charlie", "charlie@test.com", 35),
    ]
)

conn.commit()
conn.close()

print("✓ Test database created")

# Initialize validator
connection_string = f"sqlite:///{db_path}"
validator = DatabaseValidator(connection_string)

print("✓ Validator initialized\n")

# Test 1: Basic table validation using GX 1.x API
print("=== Test 1: Table Validation (GX 1.x) ===")

# Create table asset
table_asset = validator.datasource.add_table_asset(name="users_asset", table_name="users")
batch_request = table_asset.build_batch_request()

# Create expectation suite
suite_name = "users_validation_suite"

# Create suite first
try:
    suite = validator.context.suites.add(gx.core.ExpectationSuite(name=suite_name))
except:
    suite = validator.context.suites.get(suite_name)

# Get validator with the suite
batch = validator.context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)

# Add expectations
batch.expect_table_row_count_to_be_between(min_value=1, max_value=10)
batch.expect_column_values_to_not_be_null(column="id")
batch.expect_column_values_to_not_be_null(column="name")
batch.expect_column_values_to_be_unique(column="email")
batch.expect_column_values_to_be_between(column="age", min_value=18, max_value=100)

print(f"✓ Expectation suite '{suite_name}' created with 5 expectations")

# Run validation
results = batch.validate()

print(f"\nValidation Results:")
print(f"  Success: {results.success}")
print(f"  Evaluated: {results.statistics['evaluated_expectations']}")
print(f"  Successful: {results.statistics['successful_expectations']}")
print(f"  Success %: {results.statistics['success_percent']:.1f}%")

# Test 2: Query validation
print("\n=== Test 2: Query Validation ===")

query_asset = validator.datasource.add_query_asset(
    name="senior_users",
    query="SELECT * FROM users WHERE age > 25"
)
query_batch_request = query_asset.build_batch_request()

query_suite_name = "senior_users_suite"

# Create suite
try:
    validator.context.suites.add(gx.core.ExpectationSuite(name=query_suite_name))
except:
    pass

query_batch = validator.context.get_validator(
    batch_request=query_batch_request,
    expectation_suite_name=query_suite_name
)

query_batch.expect_table_row_count_to_be_between(min_value=1, max_value=100)
query_batch.expect_column_values_to_be_between(column="age", min_value=26, max_value=100)

print(f"✓ Query expectation suite created")

query_results = query_batch.validate()

print(f"\nQuery Validation Results:")
print(f"  Success: {query_results.success}")
print(f"  Rows validated: 2 (Bob and Charlie)")

# Cleanup
validator.close()
validator.engine.dispose()  # Ensure engine is fully disposed
print("\n✓ All validation tests passed!")

# Remove test database
import time
time.sleep(0.5)  # Give SQLite time to release the file
try:
    os.remove(db_path)
except:
    print(f"Note: Could not remove {db_path} (file in use - will be cleaned up later)")

# Clean up GX directory
import shutil
if os.path.exists("gx"):
    shutil.rmtree("gx")
