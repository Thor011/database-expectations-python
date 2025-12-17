"""
Test with a more complex ETL scenario using the decorator pattern.
This demonstrates how to use db_expectations in a real data pipeline.
"""

import sqlite3
import os
import sys
from datetime import datetime, timedelta

# Clean up any existing database
DB_PATH = "sales_etl_test.db"
if os.path.exists(DB_PATH):
    try:
        os.remove(DB_PATH)
    except:
        pass

from db_expectations import DatabaseValidator
from db_expectations.decorators import validate_before, validate_after, validate_both
from db_expectations.suites import ExpectationSuites

# Create a test database with sales data
DB_PATH = "sales_etl_test.db"

def setup_database():
    """Create a sample sales database with realistic data."""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Source table: raw_sales
    cursor.execute("""
        CREATE TABLE raw_sales (
            sale_id INTEGER PRIMARY KEY,
            customer_name TEXT,
            product TEXT,
            quantity INTEGER,
            unit_price REAL,
            sale_date TEXT,
            region TEXT
        )
    """)
    
    # Insert sample data
    raw_data = [
        (1, "John Doe", "Laptop", 2, 999.99, "2025-12-01", "North"),
        (2, "Jane Smith", "Mouse", 5, 25.50, "2025-12-01", "South"),
        (3, "Bob Johnson", "Keyboard", 3, 75.00, "2025-12-02", "East"),
        (4, "Alice Brown", "Monitor", 1, 299.99, "2025-12-02", "West"),
        (5, "Charlie Wilson", "Laptop", 1, 999.99, "2025-12-03", "North"),
        (6, None, "Mouse", 10, 25.50, "2025-12-03", "South"),  # Missing customer name
        (7, "Diana Prince", "Keyboard", -1, 75.00, "2025-12-04", "East"),  # Invalid quantity
        (8, "Eve Taylor", "Monitor", 2, 299.99, "2025-12-04", None),  # Missing region
    ]
    
    cursor.executemany(
        "INSERT INTO raw_sales VALUES (?, ?, ?, ?, ?, ?, ?)",
        raw_data
    )
    
    # Create target table for cleaned data
    cursor.execute("""
        CREATE TABLE cleaned_sales (
            sale_id INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL,
            product TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            total_amount REAL NOT NULL,
            sale_date TEXT NOT NULL,
            region TEXT NOT NULL
        )
    """)
    
    # Create aggregation table
    cursor.execute("""
        CREATE TABLE daily_sales_summary (
            summary_date TEXT PRIMARY KEY,
            total_sales REAL,
            total_transactions INTEGER,
            avg_transaction_value REAL
        )
    """)
    
    conn.commit()
    conn.close()
    print(f"✓ Created test database: {DB_PATH}")

# Setup
setup_database()

# Initialize validator with absolute path
import pathlib
db_path_abs = str(pathlib.Path(DB_PATH).absolute())
connection_string = f"sqlite:///{db_path_abs}"

print(f"Database: {db_path_abs}")
print(f"Connection: {connection_string}")

validator = DatabaseValidator(connection_string)

# Verify database connection
try:
    test_df = validator.query_to_dataframe("SELECT name FROM sqlite_master WHERE type='table'")
    print(f"Available tables: {list(test_df['name'])}")
except Exception as e:
    print(f"Database connection failed: {e}")
    sys.exit(1)

print("\n" + "="*70)
print("ETL PIPELINE WITH DATA VALIDATION")
print("="*70)

# Step 1: Validate raw data
print("\n[STEP 1] Validating raw sales data...")

raw_expectations = ExpectationSuites.combine(
    ExpectationSuites.row_count_check(min_rows=1),
    ExpectationSuites.null_checks(["sale_id", "product", "quantity", "unit_price"])
)

raw_results = validator.validate_table(
    table_name="raw_sales",
    suite_name="raw_data_check",
    expectations=raw_expectations
)

print(f"Raw data validation: {raw_results['success']}")
print(f"Issues found: {raw_results['statistics']['unsuccessful_expectations']} / {raw_results['statistics']['evaluated_expectations']}")

# Show the raw data
import pandas as pd
raw_df = validator.query_to_dataframe("SELECT * FROM raw_sales")
print("\nRaw Sales Data:")
print(raw_df.to_string())

# Step 2: ETL Function with validation decorators
@validate_before(
    validator=validator,
    table_name="raw_sales",
    expectations=ExpectationSuites.row_count_check(min_rows=1),
    raise_on_failure=False
)
@validate_after(
    validator=validator,
    query="SELECT * FROM cleaned_sales",
    expectations=ExpectationSuites.combine(
        ExpectationSuites.null_checks(["customer_name", "product", "quantity", "unit_price", "region"]),
        ExpectationSuites.range_checks({
            "quantity": {"min": 1},
            "unit_price": {"min": 0.01},
            "total_amount": {"min": 0.01}
        })
    ),
    raise_on_failure=False
)
def clean_and_load_sales():
    """ETL function: Clean raw sales and load into target table."""
    print("\n[STEP 2] Running ETL: Clean and Load...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Clean data: remove nulls, fix negative quantities, add total_amount
    cursor.execute("""
        INSERT INTO cleaned_sales 
        SELECT 
            sale_id,
            COALESCE(customer_name, 'Unknown') as customer_name,
            product,
            CASE WHEN quantity < 0 THEN 1 ELSE quantity END as quantity,
            unit_price,
            CASE WHEN quantity < 0 THEN 1 ELSE quantity END * unit_price as total_amount,
            sale_date,
            COALESCE(region, 'Unknown') as region
        FROM raw_sales
        WHERE product IS NOT NULL 
          AND unit_price > 0
    """)
    
    rows_loaded = cursor.rowcount
    conn.commit()
    conn.close()
    
    return rows_loaded

# Execute ETL
rows_loaded = clean_and_load_sales()
print(f"✓ Loaded {rows_loaded} cleaned records")

# Show cleaned data
cleaned_df = validator.query_to_dataframe("SELECT * FROM cleaned_sales")
print("\nCleaned Sales Data:")
print(cleaned_df.to_string())

# Step 3: Aggregation with validation
@validate_both(
    validator=validator,
    table_name="cleaned_sales",
    expectations_before=ExpectationSuites.row_count_check(min_rows=1),
    query_after="SELECT * FROM daily_sales_summary",
    expectations_after=ExpectationSuites.combine(
        ExpectationSuites.null_checks(["summary_date", "total_sales"]),
        ExpectationSuites.range_checks({
            "total_sales": {"min": 0},
            "total_transactions": {"min": 1}
        })
    ),
    raise_on_failure=True
)
def create_daily_summary():
    """Aggregate sales by day."""
    print("\n[STEP 3] Creating daily summary...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO daily_sales_summary
        SELECT 
            sale_date as summary_date,
            SUM(total_amount) as total_sales,
            COUNT(*) as total_transactions,
            AVG(total_amount) as avg_transaction_value
        FROM cleaned_sales
        GROUP BY sale_date
        ORDER BY sale_date
    """)
    
    rows_created = cursor.rowcount
    conn.commit()
    conn.close()
    
    return rows_created

# Execute aggregation
summary_rows = create_daily_summary()
print(f"✓ Created {summary_rows} daily summaries")

# Show summary
summary_df = validator.query_to_dataframe("SELECT * FROM daily_sales_summary ORDER BY summary_date")
print("\nDaily Sales Summary:")
print(summary_df.to_string())

print("\n" + "="*70)
print("FINAL VALIDATION: Data Quality Checks")
print("="*70)

# Comprehensive validation of final data
final_expectations = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["summary_date", "total_sales", "total_transactions"]),
    ExpectationSuites.range_checks({
        "total_sales": {"min": 0},
        "total_transactions": {"min": 1},
        "avg_transaction_value": {"min": 0}
    })
)

final_results = validator.validate_table(
    table_name="daily_sales_summary",
    suite_name="final_validation",
    expectations=final_expectations
)

print(f"\nFinal Validation: {'✓ PASSED' if final_results['success'] else '✗ FAILED'}")
print(f"Success Rate: {final_results['statistics']['success_percent']:.1f}%")

# Business insights
print("\n" + "="*70)
print("BUSINESS INSIGHTS")
print("="*70)

insights = validator.query_to_dataframe("""
    SELECT 
        COUNT(*) as total_days,
        ROUND(SUM(total_sales), 2) as total_revenue,
        ROUND(AVG(total_sales), 2) as avg_daily_revenue,
        MAX(total_sales) as best_day_revenue,
        SUM(total_transactions) as total_orders
    FROM daily_sales_summary
""")

print(insights.to_string(index=False))

# Product analysis
product_sales = validator.query_to_dataframe("""
    SELECT 
        product,
        SUM(quantity) as units_sold,
        ROUND(SUM(total_amount), 2) as revenue,
        ROUND(AVG(unit_price), 2) as avg_price
    FROM cleaned_sales
    GROUP BY product
    ORDER BY revenue DESC
""")

print("\nProduct Performance:")
print(product_sales.to_string(index=False))

# Regional analysis
regional_sales = validator.query_to_dataframe("""
    SELECT 
        region,
        COUNT(*) as transactions,
        ROUND(SUM(total_amount), 2) as revenue,
        ROUND(AVG(total_amount), 2) as avg_order_value
    FROM cleaned_sales
    GROUP BY region
    ORDER BY revenue DESC
""")

print("\nRegional Performance:")
print(regional_sales.to_string(index=False))

print("\n" + "="*70)
print("ETL PIPELINE COMPLETE!")
print("="*70)
print("\nPipeline Summary:")
print(f"  ✓ Raw records processed: {len(raw_df)}")
print(f"  ✓ Clean records loaded: {rows_loaded}")
print(f"  ✓ Daily summaries created: {summary_rows}")
print(f"  ✓ All validations passed: {final_results['success']}")
print(f"\nTotal Revenue: ${insights['total_revenue'].iloc[0]}")
print(f"Best performing product: {product_sales['product'].iloc[0]}")
print(f"Best performing region: {regional_sales['region'].iloc[0]}")

# Cleanup
validator.close()
print("\n✓ Database connection closed")
print(f"✓ Test database available at: {DB_PATH}")
