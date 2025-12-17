"""
Pre-built expectation suites for common database validation scenarios
"""

from typing import List, Dict, Any


class ExpectationSuites:
    """
    Helper class providing pre-built expectation suites for common validation scenarios.
    All methods are static and return lists of expectation dictionaries.
    """

    @staticmethod
    def null_checks(columns: List[str]) -> List[Dict[str, Any]]:
        """Ensure columns have no null values."""
        return [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": col},
            }
            for col in columns
        ]

    @staticmethod
    def type_checks(column_types: Dict[str, str]) -> List[Dict[str, Any]]:
        """Validate column data types."""
        return [
            {
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": col, "type_": dtype},
            }
            for col, dtype in column_types.items()
        ]

    @staticmethod
    def range_checks(column_ranges: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate numeric values are within expected ranges."""
        expectations = []
        for column, range_def in column_ranges.items():
            if "min" in range_def and "max" in range_def:
                expectations.append(
                    {
                        "expectation_type": "expect_column_values_to_be_between",
                        "kwargs": {
                            "column": column,
                            "min_value": range_def["min"],
                            "max_value": range_def["max"],
                        },
                    }
                )
            elif "min" in range_def:
                expectations.append(
                    {
                        "expectation_type": "expect_column_min_to_be_between",
                        "kwargs": {
                            "column": column,
                            "min_value": range_def["min"],
                            "max_value": None,
                        },
                    }
                )
            elif "max" in range_def:
                expectations.append(
                    {
                        "expectation_type": "expect_column_max_to_be_between",
                        "kwargs": {
                            "column": column,
                            "min_value": None,
                            "max_value": range_def["max"],
                        },
                    }
                )
        return expectations

    @staticmethod
    def unique_checks(columns: List[str]) -> List[Dict[str, Any]]:
        """Ensure columns contain unique values."""
        return [
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": col},
            }
            for col in columns
        ]

    @staticmethod
    def format_checks(column_formats: Dict[str, str]) -> List[Dict[str, Any]]:
        """Validate values match regex patterns."""
        return [
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {"column": col, "regex": pattern},
            }
            for col, pattern in column_formats.items()
        ]

    @staticmethod
    def set_membership_checks(
        column_sets: Dict[str, List[Any]],
    ) -> List[Dict[str, Any]]:
        """Validate values are in allowed sets."""
        return [
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": col, "value_set": values},
            }
            for col, values in column_sets.items()
        ]

    @staticmethod
    def row_count_check(
        min_rows: int = 0, max_rows: int = None
    ) -> List[Dict[str, Any]]:
        """Validate table row count."""
        kwargs = {"min_value": min_rows}
        if max_rows is not None:
            kwargs["max_value"] = max_rows
        else:
            kwargs["max_value"] = None

        return [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": kwargs,
            }
        ]

    @staticmethod
    def data_freshness_check(
        timestamp_column: str, max_age_hours: int
    ) -> List[Dict[str, Any]]:
        """Validate data is recent."""
        from datetime import datetime, timedelta

        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        return [
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": timestamp_column,
                    "min_value": cutoff,
                    "max_value": None,
                    "parse_strings_as_datetimes": True,
                },
            }
        ]

    @staticmethod
    def completeness_check(
        columns: List[str], threshold: float = 0.95
    ) -> List[Dict[str, Any]]:
        """Validate columns are mostly complete (non-null)."""
        return [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": col, "mostly": threshold},
            }
            for col in columns
        ]

    @staticmethod
    def combine(*suite_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Combine multiple expectation suites."""
        combined = []
        for suite in suite_results:
            combined.extend(suite)
        return combined


__all__ = ["ExpectationSuites"]
