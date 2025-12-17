# Database Expectations - Python

A production-ready Great Expectations toolkit for automated database testing and data quality validation. Perfect for developers who want to ensure data integrity before and after database operations.

## 🎯 What This Does

Validates your database data automatically using Great Expectations:
- ✅ Check for NULL values in critical columns
- ✅ Validate data types and formats
- ✅ Ensure values are within expected ranges
- ✅ Verify foreign key relationships
- ✅ Monitor data quality over time
- ✅ Run validations in CI/CD pipelines

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/Thor011/database-expectations-python.git
cd database-expectations-python

# Install dependencies
pip install -r requirements.txt
```

No need to initialize Great Expectations separately - the library handles it automatically!

### Basic Usage

```python
from db_expectations import DatabaseValidator
from db_expectations.suites import ExpectationSuites

# Connect to your database
validator = DatabaseValidator(
    connection_string="postgresql://user:password@localhost:5432/mydb"
)

# Use pre-built expectation suites
expectations = ExpectationSuites.null_checks(["user_id", "email", "created_at"])
expectations += ExpectationSuites.unique_checks(["user_id", "email"])

# Run validation
results = validator.validate_table(
    table_name="users",
    expectations=expectations
)

print(f"Validation passed: {results['success']}")
```

## 📁 Project Structure

```
database-expectations-python/
├── src/
│   ├── db_expectations/
│   │   ├── __init__.py
│   │   ├── validator.py         # Core database validator
│   │   ├── decorators.py        # Validation decorators
│   │   └── suites/
│   │       └── __init__.py      # Pre-built ExpectationSuites
├── examples/
│   ├── basic_sqlite_example.py
│   ├── postgresql_decorators_example.py
│   ├── etl_pipeline_example.py
│   ├── chinook_database_example.py      # Music store database
│   ├── northwind_database_example.py    # Business operations
│   ├── world_database_example.py        # Geographic data
│   ├── banking_database_example.py      # Financial transactions
│   └── etl_validation_example.py        # ETL with decorators
├── tests/
│   ├── test_validator.py
│   └── test_decorators.py
├── docs/
│   ├── API_REFERENCE.md
│   └── QUICK_START.md
├── requirements.txt
├── setup.py
└── README.md
```

## 🔧 Features

### Pre-built Expectation Suites

```python
from db_expectations.suites import ExpectationSuites

# Null checks for critical columns
null_checks = ExpectationSuites.null_checks(["user_id", "email", "name"])

# Type validation
type_checks = ExpectationSuites.type_checks({
    "user_id": "INTEGER",
    "email": "VARCHAR",
    "age": "INTEGER"
})

# Range validation
range_checks = ExpectationSuites.range_checks({
    "age": {"min_value": 0, "max_value": 120},
    "price": {"min_value": 0, "max_value": 999999}
})

# Unique constraints
unique_checks = ExpectationSuites.unique_checks(["user_id", "email"])

# Format validation (regex)
format_checks = ExpectationSuites.format_checks({
    "email": r"^[\w\.-]+@[\w\.-]+\.\w+$",
    "phone": r"^\d{3}-\d{3}-\d{4}$"
})

# Set membership
set_checks = ExpectationSuites.set_membership_checks({
    "status": ["active", "inactive", "archived"],
    "role": ["admin", "user", "guest"]
})

# Combine multiple suites
all_checks = ExpectationSuites.combine([
    null_checks, 
    type_checks, 
    unique_checks
])
```

### Validation Decorators

```python
from db_expectations.decorators import validate_before, validate_after, validate_both

# Validate before function execution
@validate_before(
    table="users",
    expectations=ExpectationSuites.null_checks(["user_id", "email"])
)
def create_user(name, email):
    db.execute("INSERT INTO users (name, email) VALUES (?, ?)", (name, email))

# Validate after function execution
@validate_after(
    table="users",
    expectations=ExpectationSuites.unique_checks(["email"])
)
def update_user_email(user_id, new_email):
    db.execute("UPDATE users SET email = ? WHERE user_id = ?", (new_email, user_id))

# Validate both before and after
@validate_both(
    table="orders",
    expectations=ExpectationSuites.range_checks({"total_amount": {"min_value": 0}})
)
def process_order(order_id):
    # Your order processing logic
    pass
```

### Supported Databases

- ✅ PostgreSQL
- ✅ MySQL / MariaDB
- ✅ Microsoft SQL Server
- ✅ SQLite
- ✅ Oracle
- ✅ Snowflake
- ✅ BigQuery

## 📊 Real-World Examples

Check out the `examples/` directory for comprehensive examples:

### Chinook Music Store Database
```python
# examples/chinook_database_example.py
# Validates 15,707 rows across 11 tables
# - Album validation (347 albums)
# - Customer validation (59 customers)
# - Invoice validation (412 invoices)
# - Track validation (3,503 tracks)
# - Sales analysis
```

### Northwind Business Operations
```python
# examples/northwind_database_example.py
# Validates 625,000+ rows across 14 tables
# - Product inventory validation
# - Customer and order management
# - Employee records
# - Sales performance analysis
```

### World Geographic Data
```python
# examples/world_database_example.py
# Validates countries, cities, and languages
# - Continental statistics
# - Urbanization analysis
# - Language distribution
```

### Banking & Fraud Detection
```python
# examples/banking_database_example.py
# Validates financial transactions
# - Customer KYC validation
# - Account balance checks
# - Transaction monitoring
# - High-risk detection
```

### ETL Pipeline Validation
```python
# examples/etl_validation_example.py
# Multi-step data pipeline with decorators
# - Raw data validation
# - Data cleaning with @validate_before/@validate_after
# - Aggregation with @validate_both
```

## 🔄 CI/CD Integration

### GitHub Actions

```yaml
name: Data Quality Checks

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
      - name: Run data validations
        run: |
          python -m pytest tests/ -v
```

## 💡 Use Cases

### 1. Pre-deployment Validation
```python
from db_expectations import DatabaseValidator
from db_expectations.suites import ExpectationSuites

# Validate data before deploying schema changes
validator = DatabaseValidator("postgresql://localhost/prod")
expectations = ExpectationSuites.null_checks(["id", "created_at"])
results = validator.validate_table("users", expectations)
```

### 2. ETL Pipeline Quality
```python
from db_expectations.decorators import validate_after
from db_expectations.suites import ExpectationSuites

# Validate data after ETL process
@validate_after(
    table="staging_users",
    expectations=ExpectationSuites.completeness_check(columns=["email", "phone"])
)
def run_etl_pipeline():
    # ETL logic here
    pass
```

### 3. Production Monitoring
```python
from db_expectations import DatabaseValidator
from db_expectations.suites import ExpectationSuites

# Schedule daily data quality checks
validator = DatabaseValidator("postgresql://localhost/prod")

def daily_validation():
    tables = ["users", "orders", "products"]
    for table in tables:
        expectations = ExpectationSuites.data_freshness_check(
            "updated_at", 
            max_age_days=1
        )
        results = validator.validate_table(table, expectations)
        if not results["success"]:
            send_alert(f"Data quality issue in {table}")
```

## 📖 Documentation

- [Quick Start Guide](docs/QUICK_START.md)
- [API Reference](docs/API_REFERENCE.md)
- [Real-World Examples](examples/)

## 🛠️ Technology Stack

- **Great Expectations** 1.1.0+
- **SQLAlchemy** 2.0+
- **Pandas** 2.2+
- **Pytest** 8.3+
- **Python** 3.8+

## 🤝 Contributing

Contributions welcome! Please read our contributing guidelines.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License.

## 🔗 Links

- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [Pytest Documentation](https://docs.pytest.org/)

## 📧 Contact

**GitHub:** [@Thor011](https://github.com/Thor011)

---

⭐ **Star this repository** if you find it helpful!

**Keywords:** great-expectations, data-quality, database-testing, python, data-validation, etl, data-engineering, pytest, sqlalchemy, ci-cd, data-pipeline
