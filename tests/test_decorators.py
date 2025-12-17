"""
Tests for validation decorators
"""

import pytest
from unittest.mock import Mock, MagicMock
from db_expectations.decorators import validate_before, validate_after, validate_both


@pytest.fixture
def mock_validator():
    """Create a mock validator."""
    validator = Mock()

    # Mock successful validation
    validation_result = MagicMock()
    validation_result.success = True

    validator.validate_table.return_value = validation_result
    validator.validate_query.return_value = validation_result

    return validator


class TestValidateBeforeDecorator:
    """Tests for validate_before decorator."""

    def test_validate_before_table_success(self, mock_validator):
        """Test pre-validation with table succeeds."""

        @validate_before(mock_validator, table_name="users")
        def insert_user(name):
            return f"Inserted {name}"

        result = insert_user("Alice")

        assert result == "Inserted Alice"
        assert mock_validator.validate_table.called

    def test_validate_before_query_success(self, mock_validator):
        """Test pre-validation with query succeeds."""

        @validate_before(mock_validator, query="SELECT * FROM users")
        def process_users():
            return "Processed"

        result = process_users()

        assert result == "Processed"
        assert mock_validator.validate_query.called

    def test_validate_before_failure_raises(self, mock_validator):
        """Test pre-validation failure raises error."""

        # Mock failed validation - return dict instead of object
        validation_result = {"success": False}
        mock_validator.validate_table.return_value = validation_result

        @validate_before(mock_validator, table_name="users", raise_on_failure=True)
        def insert_user(name):
            return f"Inserted {name}"

        with pytest.raises(AssertionError):
            insert_user("Alice")

    def test_validate_before_failure_no_raise(self, mock_validator):
        """Test pre-validation failure doesn't raise when configured."""

        # Mock failed validation - return dict instead of object
        validation_result = {"success": False}
        mock_validator.validate_table.return_value = validation_result

        @validate_before(mock_validator, table_name="users", raise_on_failure=False)
        def insert_user(name):
            return f"Inserted {name}"

        result = insert_user("Alice")
        assert result == "Inserted Alice"


class TestValidateAfterDecorator:
    """Tests for validate_after decorator."""

    def test_validate_after_table_success(self, mock_validator):
        """Test post-validation with table succeeds."""

        @validate_after(mock_validator, table_name="users")
        def insert_user(name):
            return f"Inserted {name}"

        result = insert_user("Alice")

        assert result == "Inserted Alice"
        assert mock_validator.validate_table.called

    def test_validate_after_query_success(self, mock_validator):
        """Test post-validation with query succeeds."""

        @validate_after(mock_validator, query="SELECT * FROM users")
        def process_users():
            return "Processed"

        result = process_users()

        assert result == "Processed"
        assert mock_validator.validate_query.called

    def test_validate_after_failure_raises(self, mock_validator):
        """Test post-validation failure raises error."""

        # Mock failed validation - return dict instead of object
        validation_result = {"success": False}
        mock_validator.validate_table.return_value = validation_result

        @validate_after(mock_validator, table_name="users", raise_on_failure=True)
        def insert_user(name):
            return f"Inserted {name}"

        with pytest.raises(AssertionError):
            insert_user("Alice")

    def test_validate_after_preserves_return_value(self, mock_validator):
        """Test post-validation preserves function return value."""

        @validate_after(mock_validator, table_name="users")
        def get_user_count():
            return 42

        result = get_user_count()
        assert result == 42


class TestValidateBothDecorator:
    """Tests for validate_both decorator."""

    def test_validate_both_success(self, mock_validator):
        """Test both pre and post validation succeed."""

        @validate_both(
            mock_validator,
            table_name="users",
            expectations_before=[{"type": "null_check"}],
            expectations_after=[{"type": "unique_check"}],
        )
        def update_users():
            return "Updated"

        result = update_users()

        assert result == "Updated"
        assert mock_validator.validate_table.call_count == 2

    def test_validate_both_pre_failure(self, mock_validator):
        """Test pre-validation failure in validate_both."""

        # First call fails (pre), second succeeds (post) - return dicts instead of objects
        validation_fail = {"success": False}
        validation_success = {"success": True}

        mock_validator.validate_table.side_effect = [
            validation_fail,
            validation_success,
        ]

        @validate_both(mock_validator, table_name="users", raise_on_failure=True)
        def update_users():
            return "Updated"

        with pytest.raises(AssertionError):
            update_users()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
