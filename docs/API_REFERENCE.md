# API Reference

## DatabaseValidator

Main class for database validation using Great Expectations.

### Constructor

```python
DatabaseValidator(
    connection_string: str,
    context_root_dir: Optional[str] = None,
    data_context_config: Optional[Dict[str, Any]] = None
)
```

**Parameters:**
- `connection_string`: SQLAlchemy connection string
- `context_root_dir`: Great Expectations context directory (default: `./gx`)
- `data_context_config`: Custom data context configuration

**Example:**
```python
validator = DatabaseValidator("postgresql://user:pass@localhost:5432/db")
```

### Methods

#### validate_table

```python
validate_table(
    table_name: str,
    suite_name: Optional[str] = None,
    expectations: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]
```

Validate a database table.

**Returns:** Validation results dictionary

**Example:**
```python
results = validator.validate_table(
    table_name="users",
    suite_name="users_suite",
    expectations=[...]
)
```

#### validate_query

```python
validate_query(
    query: str,
    asset_name: str,
    suite_name: Optional[str] = None,
    expectations: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]
```

Validate results of a SQL query.

**Returns:** Validation results dictionary

#### get_table_info

```python
get_table_info(table_name: str) -> Dict[str, Any]
```

Get metadata about a table (columns, types, constraints).

**Returns:** Dictionary with table metadata

#### query_to_dataframe

```python
query_to_dataframe(query: str) -> pd.DataFrame
```

Execute a query and return results as pandas DataFrame.

#### get_row_count

```python
get_row_count(table_name: str) -> int
```

Get total row count for a table.

---

## ExpectationSuites

Helper class for creating pre-built expectation configurations.

### Methods

#### null_checks

```python
@staticmethod
null_checks(columns: List[str]) -> List[Dict[str, Any]]
```

Create null value validation expectations.

**Example:**
```python
ExpectationSuites.null_checks(["id", "name", "email"])
```

#### type_checks

```python
@staticmethod
type_checks(column_types: Dict[str, str]) -> List[Dict[str, Any]]
```

Create data type validation expectations.

**Supported Types:** `int`, `str`, `float`, `bool`, `date`, `datetime`

**Example:**
```python
ExpectationSuites.type_checks({
    "age": "int",
    "name": "str",
    "created_at": "datetime"
})
```

#### range_checks

```python
@staticmethod
range_checks(column_ranges: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]
```

Create numeric range validation expectations.

**Example:**
```python
ExpectationSuites.range_checks({
    "age": {"min": 0, "max": 120},
    "price": {"min": 0.01}
})
```

#### unique_checks

```python
@staticmethod
unique_checks(columns: List[str]) -> List[Dict[str, Any]]
```

Create uniqueness validation expectations.

#### format_checks

```python
@staticmethod
format_checks(column_formats: Dict[str, str]) -> List[Dict[str, Any]]
```

Create format validation expectations using regex patterns.

**Example:**
```python
ExpectationSuites.format_checks({
    "email": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
})
```

#### set_membership_checks

```python
@staticmethod
set_membership_checks(column_sets: Dict[str, List[Any]]) -> List[Dict[str, Any]]
```

Create value set membership expectations.

**Example:**
```python
ExpectationSuites.set_membership_checks({
    "status": ["pending", "active", "completed"]
})
```

#### row_count_check

```python
@staticmethod
row_count_check(min_rows: int = 0, max_rows: int = None) -> List[Dict[str, Any]]
```

Create table row count validation expectation.

#### data_freshness_check

```python
@staticmethod
data_freshness_check(
    timestamp_column: str,
    max_age_hours: int
) -> List[Dict[str, Any]]
```

Create data freshness validation expectation.

#### completeness_check

```python
@staticmethod
completeness_check(
    columns: List[str],
    threshold: float = 0.95
) -> List[Dict[str, Any]]
```

Create data completeness expectations (mostly non-null).

**Parameters:**
- `threshold`: Minimum fraction of non-null values (0.0 to 1.0)

#### combine

```python
@staticmethod
combine(*suite_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]
```

Combine multiple expectation suites into one.

**Example:**
```python
ExpectationSuites.combine(
    ExpectationSuites.null_checks(["id"]),
    ExpectationSuites.unique_checks(["email"]),
    ExpectationSuites.range_checks({"age": {"min": 0}})
)
```

---

## Decorators

### validate_before

```python
validate_before(
    validator: DatabaseValidator,
    table_name: Optional[str] = None,
    query: Optional[str] = None,
    suite_name: Optional[str] = None,
    expectations: Optional[List[Dict[str, Any]]] = None,
    raise_on_failure: bool = True
)
```

Decorator to validate database state BEFORE function execution.

**Example:**
```python
@validate_before(validator, table_name="users", expectations=[...])
def insert_user(name, email):
    # Insert logic
    pass
```

### validate_after

```python
validate_after(
    validator: DatabaseValidator,
    table_name: Optional[str] = None,
    query: Optional[str] = None,
    suite_name: Optional[str] = None,
    expectations: Optional[List[Dict[str, Any]]] = None,
    raise_on_failure: bool = True
)
```

Decorator to validate database state AFTER function execution.

**Example:**
```python
@validate_after(validator, table_name="orders", expectations=[...])
def process_order(order_id):
    # Processing logic
    pass
```

### validate_both

```python
validate_both(
    validator: DatabaseValidator,
    table_name: Optional[str] = None,
    query_before: Optional[str] = None,
    query_after: Optional[str] = None,
    suite_name_before: Optional[str] = None,
    suite_name_after: Optional[str] = None,
    expectations_before: Optional[List[Dict[str, Any]]] = None,
    expectations_after: Optional[List[Dict[str, Any]]] = None,
    raise_on_failure: bool = True
)
```

Decorator to validate database state BEFORE and AFTER function execution.

**Example:**
```python
@validate_both(
    validator,
    table_name="inventory",
    expectations_before=[...],
    expectations_after=[...]
)
def update_inventory():
    # Update logic
    pass
```

---

## Validation Results

All validation methods return a results object with the following structure:

```python
{
    "success": bool,  # Overall validation success
    "statistics": {
        "evaluated_expectations": int,
        "successful_expectations": int,
        "unsuccessful_expectations": int,
        "success_percent": float
    },
    "results": [...]  # Detailed results for each expectation
}
```

**Accessing Results:**
```python
results = validator.validate_table("users", ...)

if results.success:
    print("Validation passed!")
else:
    print("Validation failed")
    print(f"Success rate: {results.statistics['success_percent']}%")
```
