"""
Simple test to verify basic functionality
"""

from db_expectations import DatabaseValidator
import sqlite3
import os

# Create sample SQLite database
db_path = "test_simple.db"

# Remove if exists
if os.path.exists(db_path):
    os.remove(db_path)

# Create and populate database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create simple table
cursor.execute("""
    CREATE TABLE test_table (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        value INTEGER
    )
""")

# Insert sample data
cursor.executemany(
    "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
    [
        (1, "Alice", 100),
        (2, "Bob", 200),
        (3, "Charlie", 300),
    ]
)

conn.commit()
conn.close()

print("✓ Test database created")

# Initialize validator
connection_string = f"sqlite:///{db_path}"
validator = DatabaseValidator(connection_string)

print("✓ Validator initialized")
print(f"✓ Datasource: {validator.datasource.name}")

# Test get_table_info
print("\n=== Table Metadata ===")
table_info = validator.get_table_info("test_table")
print(f"Table: {table_info['table_name']}")
print(f"Columns: {[col['name'] for col in table_info['columns']]}")

# Test query_to_dataframe
print("\n=== Query to DataFrame ===")
df = validator.query_to_dataframe("SELECT * FROM test_table")
print(f"Rows: {len(df)}")
print(df)

# Test get_row_count
print("\n=== Row Count ===")
row_count = validator.get_row_count("test_table")
print(f"Total rows: {row_count}")

# Cleanup
validator.close()
print("\n✓ Basic tests complete!")

# Remove test database
os.remove(db_path)
