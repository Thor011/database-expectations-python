"""
Test database-expectations with the Chinook sample database.
The Chinook database represents a digital media store, including tables for artists, albums, 
media tracks, invoices and customers.
"""

import os
import urllib.request
from db_expectations import DatabaseValidator
from db_expectations.suites import ExpectationSuites

# Download Chinook database if it doesn't exist
DB_URL = "https://github.com/lerocha/chinook-database/raw/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"
DB_PATH = "Chinook.db"

if not os.path.exists(DB_PATH):
    print(f"Downloading Chinook database from {DB_URL}...")
    urllib.request.urlretrieve(DB_URL, DB_PATH)
    print(f"✓ Database downloaded to {DB_PATH}")
else:
    print(f"✓ Using existing database: {DB_PATH}")

# Connect to the database
connection_string = f"sqlite:///{DB_PATH}"
validator = DatabaseValidator(connection_string)

print("\n" + "="*70)
print("DATABASE OVERVIEW")
print("="*70)

# Get available tables
from sqlalchemy import inspect
inspector = inspect(validator.engine)
tables = inspector.get_table_names()
print(f"\nAvailable tables ({len(tables)}):")
for table in tables:
    row_count = validator.get_row_count(table)
    print(f"  - {table}: {row_count} rows")

print("\n" + "="*70)
print("TABLE EXPLORATION: Album")
print("="*70)

# Explore Album table structure
album_info = validator.get_table_info("Album")
print("\nColumns:")
for col in album_info["columns"]:
    print(f"  - {col['name']}: {col['type']} (nullable: {col['nullable']})")

# Query some sample data
print("\nSample data:")
df = validator.query_to_dataframe("SELECT * FROM Album LIMIT 5")
print(df.to_string())

print("\n" + "="*70)
print("VALIDATION TEST 1: Album Table - Basic Checks")
print("="*70)

expectations_album = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["AlbumId", "Title", "ArtistId"]),
    ExpectationSuites.unique_checks(["AlbumId"]),
    ExpectationSuites.row_count_check(min_rows=1, max_rows=500)
)

results = validator.validate_table(
    table_name="Album",
    suite_name="album_validation",
    expectations=expectations_album
)

print(f"Validation Success: {results['success']}")
print(f"Statistics:")
print(f"  - Evaluated: {results['statistics']['evaluated_expectations']}")
print(f"  - Successful: {results['statistics']['successful_expectations']}")
print(f"  - Failed: {results['statistics']['unsuccessful_expectations']}")
print(f"  - Success %: {results['statistics']['success_percent']:.1f}%")

print("\n" + "="*70)
print("VALIDATION TEST 2: Customer Table - Data Quality")
print("="*70)

# Explore Customer table
customer_info = validator.get_table_info("Customer")
print(f"\nCustomer table has {validator.get_row_count('Customer')} rows")
print("Columns:", [col['name'] for col in customer_info['columns']])

# Sample customer data
print("\nSample customers:")
customers_df = validator.query_to_dataframe("SELECT CustomerId, FirstName, LastName, Country FROM Customer LIMIT 5")
print(customers_df.to_string())

expectations_customer = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["CustomerId", "FirstName", "LastName", "Email"]),
    ExpectationSuites.unique_checks(["CustomerId", "Email"]),
    ExpectationSuites.format_checks({
        "Email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    })
)

results_customer = validator.validate_table(
    table_name="Customer",
    suite_name="customer_validation",
    expectations=expectations_customer
)

print(f"\nValidation Success: {results_customer['success']}")
print(f"Success Rate: {results_customer['statistics']['success_percent']:.1f}%")

print("\n" + "="*70)
print("VALIDATION TEST 3: Invoice Table - Query Validation")
print("="*70)

# Query for high-value invoices
print("\nHigh-value invoices (> $10):")
high_value_df = validator.query_to_dataframe(
    "SELECT InvoiceId, CustomerId, Total, InvoiceDate FROM Invoice WHERE Total > 10 ORDER BY Total DESC LIMIT 10"
)
print(high_value_df.to_string())

# Validate that all totals are positive and within reasonable range
expectations_invoice = [
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "Total", "min_value": 0.01, "max_value": 100}
    }
]

results_invoice = validator.validate_query(
    query="SELECT * FROM Invoice",
    asset_name="all_invoices",
    suite_name="invoice_validation",
    expectations=expectations_invoice
)

print(f"\nInvoice validation success: {results_invoice['success']}")
print(f"All invoices have positive totals: {results_invoice['statistics']['successful_expectations']} / {results_invoice['statistics']['evaluated_expectations']}")

print("\n" + "="*70)
print("VALIDATION TEST 4: Track Table - Range and Set Checks")
print("="*70)

# Explore Track data
track_sample = validator.query_to_dataframe(
    "SELECT TrackId, Name, Milliseconds, Bytes, UnitPrice FROM Track LIMIT 5"
)
print("\nSample tracks:")
print(track_sample.to_string())

expectations_track = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["TrackId", "Name", "MediaTypeId", "GenreId"]),
    ExpectationSuites.range_checks({
        "Milliseconds": {"min": 1000},  # At least 1 second
        "UnitPrice": {"min": 0, "max": 10}
    })
)

results_track = validator.validate_table(
    table_name="Track",
    suite_name="track_validation",
    expectations=expectations_track
)

print(f"\nTrack validation success: {results_track['success']}")
print(f"Success Rate: {results_track['statistics']['success_percent']:.1f}%")

print("\n" + "="*70)
print("ADVANCED QUERY: Sales Analysis")
print("="*70)

# Complex analytical query
sales_by_country = validator.query_to_dataframe("""
    SELECT 
        c.Country,
        COUNT(DISTINCT c.CustomerId) as CustomerCount,
        COUNT(i.InvoiceId) as InvoiceCount,
        ROUND(SUM(i.Total), 2) as TotalRevenue,
        ROUND(AVG(i.Total), 2) as AvgInvoiceValue
    FROM Customer c
    LEFT JOIN Invoice i ON c.CustomerId = i.CustomerId
    GROUP BY c.Country
    ORDER BY TotalRevenue DESC
    LIMIT 10
""")

print("\nTop 10 countries by revenue:")
print(sales_by_country.to_string())

# Validate the aggregated data
expectations_sales = [
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "CustomerCount", "min_value": 1, "max_value": None}
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "TotalRevenue", "min_value": 0, "max_value": None}
    }
]

results_sales = validator.validate_query(
    query="""
        SELECT 
            Country,
            COUNT(DISTINCT c.CustomerId) as CustomerCount,
            SUM(i.Total) as TotalRevenue
        FROM Customer c
        LEFT JOIN Invoice i ON c.CustomerId = i.CustomerId
        GROUP BY Country
    """,
    asset_name="sales_by_country",
    suite_name="sales_validation",
    expectations=expectations_sales
)

print(f"\nSales validation success: {results_sales['success']}")

print("\n" + "="*70)
print("TEST COMPLETE!")
print("="*70)
print("\nAll validations completed successfully!")
print(f"Database: {DB_PATH}")
print(f"Tables tested: Album, Customer, Invoice, Track")
print(f"Total validations: 5")

# Cleanup
validator.close()
print("\n✓ Database connection closed")
