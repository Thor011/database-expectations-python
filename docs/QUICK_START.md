# Quick Start Guide

## Installation

```bash
# Clone repository
git clone https://github.com/Thor011/database-expectations-python.git
cd database-expectations-python

# Install package
pip install -e .

# Install with development dependencies
pip install -e ".[dev]"

# Install with Oracle support
pip install -e ".[oracle]"
```

## Basic Usage

### 1. Initialize Great Expectations Context

```bash
# Initialize GX context (one-time setup)
great_expectations init
```

### 2. Create Validator

```python
from db_expectations import DatabaseValidator

# SQLite example
validator = DatabaseValidator("sqlite:///my_database.db")

# PostgreSQL example
validator = DatabaseValidator(
    "postgresql://user:password@localhost:5432/mydb"
)
```

### 3. Validate a Table

```python
from db_expectations.suites import ExpectationSuites

# Use pre-built validation suites
expectations = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["id", "name", "email"]),
    ExpectationSuites.unique_checks(["email"]),
    ExpectationSuites.range_checks({"age": {"min": 18, "max": 100}}),
    ExpectationSuites.row_count_check(min_rows=1)
)

results = validator.validate_table(
    table_name="users",
    suite_name="users_validation",
    expectations=expectations
)

print(f"Validation success: {results.success}")
```

### 4. Use Decorators

```python
from db_expectations import validate_after

@validate_after(
    validator,
    table_name="orders",
    expectations=ExpectationSuites.null_checks(["order_id", "customer_id"])
)
def insert_order(order_data):
    # Your insert logic here
    pass
```

## Common Validation Patterns

### Null Checks
```python
ExpectationSuites.null_checks(["column1", "column2"])
```

### Type Validation
```python
ExpectationSuites.type_checks({
    "age": "int",
    "name": "str",
    "price": "float"
})
```

### Range Validation
```python
ExpectationSuites.range_checks({
    "age": {"min": 0, "max": 120},
    "price": {"min": 0.01, "max": 999999.99}
})
```

### Uniqueness
```python
ExpectationSuites.unique_checks(["email", "username"])
```

### Format Validation (Regex)
```python
ExpectationSuites.format_checks({
    "email": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
    "phone": r"^\+?1?\d{9,15}$"
})
```

### Set Membership
```python
ExpectationSuites.set_membership_checks({
    "status": ["pending", "active", "completed", "cancelled"]
})
```

## Running Examples

```bash
# SQLite example
python examples/basic_sqlite_example.py

# PostgreSQL with decorators (requires database setup)
python examples/postgresql_decorators_example.py

# ETL pipeline validation
python examples/etl_pipeline_example.py
```

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=db_expectations --cov-report=html

# Run specific test file
pytest tests/test_validator.py -v
```

## Environment Variables

Create a `.env` file from the template:

```bash
cp .env.example .env
```

Edit `.env` with your database connections:

```ini
POSTGRES_CONNECTION=postgresql://user:password@localhost:5432/database
MYSQL_CONNECTION=mysql+pymysql://user:password@localhost:3306/database
```

## Troubleshooting

### Import Errors

If you see import errors, ensure the package is installed:

```bash
pip install -e .
```

### Database Connection Issues

Verify your connection string format:

- **PostgreSQL**: `postgresql://user:password@host:port/database`
- **MySQL**: `mysql+pymysql://user:password@host:port/database`
- **SQL Server**: `mssql+pyodbc://user:password@host:port/database?driver=ODBC+Driver+17+for+SQL+Server`
- **SQLite**: `sqlite:///path/to/database.db`

### Great Expectations Context

If GX context isn't found, initialize it:

```bash
great_expectations init
```

## Next Steps

- Read the [full README](README.md)
- Explore [examples](examples/)
- Review [test files](tests/) for usage patterns
- Check [Great Expectations documentation](https://docs.greatexpectations.io/)
