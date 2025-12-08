"""
Example: ETL pipeline validation
Demonstrates pre and post validation for ETL processes
"""

from db_expectations import DatabaseValidator, validate_before, validate_after
from db_expectations.suites import ExpectationSuites
import pandas as pd
from datetime import datetime

# Source and target databases
source_connection = "postgresql://user:pass@source-db:5432/source_db"
target_connection = "postgresql://user:pass@target-db:5432/warehouse"

source_validator = DatabaseValidator(source_connection)
target_validator = DatabaseValidator(target_connection)

print("✓ Connected to source and target databases")


# ETL Step 1: Extract
@validate_before(
    source_validator,
    table_name="raw_transactions",
    expectations=ExpectationSuites.combine(
        ExpectationSuites.row_count_check(min_rows=1),
        ExpectationSuites.data_freshness_check("created_at", max_age_hours=1)
    ),
    raise_on_failure=True
)
def extract_transactions():
    """Extract transactions from source database."""
    print("\n[EXTRACT] Reading from source database...")
    
    query = """
        SELECT 
            transaction_id,
            customer_id,
            amount,
            currency,
            transaction_date,
            created_at
        FROM raw_transactions
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '1 DAY'
    """
    
    df = source_validator.query_to_dataframe(query)
    print(f"[EXTRACT] Extracted {len(df)} rows")
    return df


# ETL Step 2: Transform
def transform_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Transform and clean transaction data."""
    print("\n[TRANSFORM] Transforming data...")
    
    # Convert currency to USD (simplified example)
    exchange_rates = {"EUR": 1.1, "GBP": 1.3, "USD": 1.0}
    df["amount_usd"] = df.apply(
        lambda row: row["amount"] * exchange_rates.get(row["currency"], 1.0),
        axis=1
    )
    
    # Add processing timestamp
    df["processed_at"] = datetime.now()
    
    # Remove duplicates
    df = df.drop_duplicates(subset=["transaction_id"])
    
    # Validate transformed data
    assert df["amount_usd"].min() >= 0, "Negative amounts found"
    assert df["transaction_id"].is_unique, "Duplicate transaction IDs found"
    
    print(f"[TRANSFORM] Transformed {len(df)} rows")
    return df


# ETL Step 3: Load
@validate_after(
    target_validator,
    table_name="fact_transactions",
    expectations=ExpectationSuites.combine(
        ExpectationSuites.null_checks(["transaction_id", "customer_id", "amount_usd"]),
        ExpectationSuites.unique_checks(["transaction_id"]),
        ExpectationSuites.range_checks({"amount_usd": {"min": 0.01}}),
        ExpectationSuites.row_count_check(min_rows=1)
    ),
    raise_on_failure=True
)
def load_transactions(df: pd.DataFrame):
    """Load transformed data into target warehouse."""
    print("\n[LOAD] Loading to target database...")
    
    # Load to database (simplified)
    # In real scenario: df.to_sql('fact_transactions', target_validator.engine, if_exists='append')
    
    print(f"[LOAD] Loaded {len(df)} rows to fact_transactions")
    return len(df)


# Main ETL Pipeline
def run_etl_pipeline():
    """Execute complete ETL pipeline with validation."""
    print("=" * 60)
    print("ETL PIPELINE EXECUTION")
    print("=" * 60)
    
    try:
        # Extract with pre-validation
        df_extracted = extract_transactions()
        
        # Transform
        df_transformed = transform_transactions(df_extracted)
        
        # Load with post-validation
        rows_loaded = load_transactions(df_transformed)
        
        print("\n" + "=" * 60)
        print("✓ ETL PIPELINE COMPLETED SUCCESSFULLY")
        print(f"  Rows processed: {rows_loaded}")
        print("=" * 60)
        
        return True
        
    except AssertionError as e:
        print(f"\n✗ VALIDATION FAILED: {e}")
        return False
        
    except Exception as e:
        print(f"\n✗ PIPELINE FAILED: {e}")
        return False
        
    finally:
        source_validator.close()
        target_validator.close()


# Additional validation: Data reconciliation
def validate_data_reconciliation():
    """Validate source vs target row counts match."""
    print("\n[RECONCILIATION] Checking source vs target counts...")
    
    source_count = source_validator.get_row_count("raw_transactions")
    target_count = target_validator.get_row_count("fact_transactions")
    
    print(f"  Source rows: {source_count}")
    print(f"  Target rows: {target_count}")
    
    if source_count == target_count:
        print("  ✓ Counts match")
        return True
    else:
        print(f"  ✗ Count mismatch: {abs(source_count - target_count)} rows difference")
        return False


# Run if executed directly
if __name__ == "__main__":
    success = run_etl_pipeline()
    
    if success:
        validate_data_reconciliation()
