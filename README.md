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

# Initialize Great Expectations
great_expectations init
```

### Basic Usage

```python
from db_expectations import DatabaseValidator

# Connect to your database
validator = DatabaseValidator(
    connection_string="postgresql://user:password@localhost:5432/mydb"
)

# Run built-in validation suite
results = validator.validate_table(
    table_name="users",
    suite_name="basic_data_quality"
)

print(f"Validation passed: {results.success}")
```

## 📁 Project Structure

```
database-expectations-python/
├── src/
│   ├── db_expectations/
│   │   ├── __init__.py
│   │   ├── validators/          # Database validators
│   │   ├── suites/              # Pre-built expectation suites
│   │   ├── connectors/          # Database connectors
│   │   └── decorators/          # Validation decorators
├── examples/
│   ├── postgres_example.py
│   ├── mysql_example.py
│   ├── sqlserver_example.py
│   └── sqlite_example.py
├── tests/
│   └── test_validators.py
├── great_expectations/          # GX configuration
├── docs/
│   ├── getting_started.md
│   └── best_practices.md
├── .github/
│   └── workflows/
│       └── data_quality.yml
├── requirements.txt
├── setup.py
└── README.md
```

## 🔧 Features

### Pre-built Expectation Suites

```python
# User table validation
validator.expect_column_values_to_not_be_null("user_id")
validator.expect_column_values_to_be_unique("email")
validator.expect_column_values_to_match_regex("email", r"^[\w\.-]+@[\w\.-]+\.\w+$")

# Product table validation
validator.expect_column_values_to_be_between("price", min_value=0, max_value=999999)
validator.expect_column_values_to_be_in_set("status", ["active", "inactive", "archived"])

# Order table validation
validator.expect_column_pair_values_A_to_be_greater_than_B("order_date", "created_date")
```

### Validation Decorators

```python
from db_expectations.decorators import validate_before, validate_after

@validate_before(table="users", suite="user_integrity")
@validate_after(table="users", suite="user_integrity")
def create_user(name, email):
    # Your database insert logic
    db.execute("INSERT INTO users (name, email) VALUES (?, ?)", (name, email))
```

### Supported Databases

- ✅ PostgreSQL
- ✅ MySQL / MariaDB
- ✅ Microsoft SQL Server
- ✅ SQLite
- ✅ Oracle
- ✅ Snowflake
- ✅ BigQuery

## 📊 Example Validation Suite

```python
import great_expectations as gx

# Create expectation suite
suite = gx.core.ExpectationSuite(expectation_suite_name="user_table_quality")

# Column existence
suite.add_expectation({
    "expectation_type": "expect_column_to_exist",
    "kwargs": {"column": "user_id"}
})

# Not null constraints
suite.add_expectation({
    "expectation_type": "expect_column_values_to_not_be_null",
    "kwargs": {"column": "user_id"}
})

# Data types
suite.add_expectation({
    "expectation_type": "expect_column_values_to_be_of_type",
    "kwargs": {"column": "user_id", "type_": "INTEGER"}
})

# Email format
suite.add_expectation({
    "expectation_type": "expect_column_values_to_match_regex",
    "kwargs": {
        "column": "email",
        "regex": r"^[\w\.-]+@[\w\.-]+\.\w+$"
    }
})

# Age range
suite.add_expectation({
    "expectation_type": "expect_column_values_to_be_between",
    "kwargs": {"column": "age", "min_value": 0, "max_value": 120}
})
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
# Validate data before deploying schema changes
validator.validate_database(suite="pre_migration_checks")
```

### 2. ETL Pipeline Quality
```python
# Validate data after ETL process
@validate_after(table="staging_users", suite="etl_quality")
def run_etl_pipeline():
    # ETL logic here
    pass
```

### 3. Production Monitoring
```python
# Schedule daily data quality checks
from apscheduler.schedulers.blocking import BlockingScheduler

scheduler = BlockingScheduler()

@scheduler.scheduled_job('cron', hour=2)
def daily_validation():
    validator.validate_all_tables()
    
scheduler.start()
```

## 📖 Documentation

- [Getting Started Guide](docs/getting_started.md)
- [Best Practices](docs/best_practices.md)
- [API Reference](docs/api_reference.md)
- [Examples](examples/)

## 🛠️ Technology Stack

- **Great Expectations** 0.18+
- **SQLAlchemy** 2.0+
- **Pandas** 2.0+
- **Pytest** 7.4+
- **Python** 3.9+

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
