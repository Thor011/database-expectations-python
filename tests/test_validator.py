"""
Unit tests for DatabaseValidator class
"""

import pytest
import os
import sqlite3
from db_expectations import DatabaseValidator
from db_expectations.suites import ExpectationSuites


@pytest.fixture
def test_db():
    """Create a temporary test database."""
    db_path = "test_temp.db"
    
    # Create database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE test_users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER
        )
    """)
    
    cursor.executemany(
        "INSERT INTO test_users (id, name, email, age) VALUES (?, ?, ?, ?)",
        [
            (1, "Alice", "alice@test.com", 25),
            (2, "Bob", "bob@test.com", 30),
            (3, "Charlie", "charlie@test.com", 35),
        ]
    )
    
    conn.commit()
    conn.close()
    
    yield db_path
    
    # Cleanup
    if os.path.exists(db_path):
        os.remove(db_path)
    if os.path.exists("gx"):
        import shutil
        shutil.rmtree("gx")


@pytest.fixture
def validator(test_db):
    """Create validator instance."""
    connection_string = f"sqlite:///{test_db}"
    v = DatabaseValidator(connection_string)
    yield v
    v.close()


class TestDatabaseValidator:
    """Tests for DatabaseValidator class."""
    
    def test_initialization(self, validator):
        """Test validator initializes correctly."""
        assert validator.engine is not None
        assert validator.context is not None
        assert validator.datasource is not None
    
    def test_get_table_info(self, validator):
        """Test retrieving table metadata."""
        info = validator.get_table_info("test_users")
        
        assert info["table_name"] == "test_users"
        assert len(info["columns"]) == 4
        
        column_names = [col["name"] for col in info["columns"]]
        assert "id" in column_names
        assert "name" in column_names
        assert "email" in column_names
        assert "age" in column_names
    
    def test_query_to_dataframe(self, validator):
        """Test querying to DataFrame."""
        df = validator.query_to_dataframe("SELECT * FROM test_users")
        
        assert len(df) == 3
        assert list(df.columns) == ["id", "name", "email", "age"]
        assert df["name"].tolist() == ["Alice", "Bob", "Charlie"]
    
    def test_get_row_count(self, validator):
        """Test getting row count."""
        count = validator.get_row_count("test_users")
        assert count == 3
    
    def test_validate_table_success(self, validator):
        """Test successful table validation."""
        expectations = ExpectationSuites.combine(
            ExpectationSuites.null_checks(["id", "name"]),
            ExpectationSuites.unique_checks(["email"]),
            ExpectationSuites.row_count_check(min_rows=1, max_rows=10)
        )
        
        results = validator.validate_table(
            table_name="test_users",
            suite_name="test_suite",
            expectations=expectations
        )
        
        assert results.success is True
    
    def test_validate_query_success(self, validator):
        """Test successful query validation."""
        expectations = [
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "age", "min_value": 20, "max_value": 40}
            }
        ]
        
        results = validator.validate_query(
            query="SELECT * FROM test_users WHERE age > 20",
            asset_name="filtered_users",
            suite_name="query_suite",
            expectations=expectations
        )
        
        assert results.success is True
    
    def test_context_manager(self, test_db):
        """Test validator works as context manager."""
        connection_string = f"sqlite:///{test_db}"
        
        with DatabaseValidator(connection_string) as v:
            count = v.get_row_count("test_users")
            assert count == 3
        
        # Engine should be disposed after context exit
        assert v.engine is not None


class TestExpectationSuites:
    """Tests for ExpectationSuites helper class."""
    
    def test_null_checks(self):
        """Test null check expectations."""
        expectations = ExpectationSuites.null_checks(["col1", "col2"])
        
        assert len(expectations) == 2
        assert all(e["expectation_type"] == "expect_column_values_to_not_be_null" for e in expectations)
    
    def test_type_checks(self):
        """Test type check expectations."""
        expectations = ExpectationSuites.type_checks({"age": "int", "name": "str"})
        
        assert len(expectations) == 2
        assert all(e["expectation_type"] == "expect_column_values_to_be_of_type" for e in expectations)
    
    def test_range_checks(self):
        """Test range check expectations."""
        expectations = ExpectationSuites.range_checks({
            "age": {"min": 0, "max": 120},
            "price": {"min": 0.01}
        })
        
        assert len(expectations) == 2
    
    def test_unique_checks(self):
        """Test unique check expectations."""
        expectations = ExpectationSuites.unique_checks(["email", "username"])
        
        assert len(expectations) == 2
        assert all(e["expectation_type"] == "expect_column_values_to_be_unique" for e in expectations)
    
    def test_format_checks(self):
        """Test format check expectations."""
        expectations = ExpectationSuites.format_checks({
            "email": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        })
        
        assert len(expectations) == 1
        assert expectations[0]["expectation_type"] == "expect_column_values_to_match_regex"
    
    def test_set_membership_checks(self):
        """Test set membership expectations."""
        expectations = ExpectationSuites.set_membership_checks({
            "status": ["active", "inactive", "pending"]
        })
        
        assert len(expectations) == 1
        assert expectations[0]["expectation_type"] == "expect_column_values_to_be_in_set"
    
    def test_row_count_check(self):
        """Test row count expectations."""
        expectations = ExpectationSuites.row_count_check(min_rows=10, max_rows=100)
        
        assert len(expectations) == 1
        assert expectations[0]["expectation_type"] == "expect_table_row_count_to_be_between"
    
    def test_combine_suites(self):
        """Test combining multiple suites."""
        suite1 = ExpectationSuites.null_checks(["col1"])
        suite2 = ExpectationSuites.unique_checks(["col2"])
        
        combined = ExpectationSuites.combine(suite1, suite2)
        
        assert len(combined) == 2
        assert combined[0] in suite1
        assert combined[1] in suite2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
