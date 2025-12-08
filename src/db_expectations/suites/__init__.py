"""
Pre-built expectation suites for common database validation scenarios
"""

from typing import List, Dict, Any, Callable


def null_checks(columns: List[str]) -> List[Callable]:
    """Ensure columns have no null values."""
    return [lambda batch, col=col: batch.expect_column_values_to_not_be_null(col) for col in columns]


def type_checks(column_types: Dict[str, str]) -> List[Callable]:
    """Validate column data types."""
    return [lambda batch, col=col, dtype=dtype: batch.expect_column_values_to_be_of_type(col, dtype) 
            for col, dtype in column_types.items()]


def range_checks(column_ranges: Dict[str, Dict[str, Any]]) -> List[Callable]:
    """Validate numeric values are within expected ranges."""
    expectations = []
    for column, range_def in column_ranges.items():
        if "min" in range_def and "max" in range_def:
            expectations.append(
                lambda batch, col=column, min_val=range_def["min"], max_val=range_def["max"]: 
                batch.expect_column_values_to_be_between(col, min_value=min_val, max_value=max_val)
            )
        elif "min" in range_def:
            expectations.append(
                lambda batch, col=column, min_val=range_def["min"]: 
                batch.expect_column_min_to_be_between(col, min_value=min_val, max_value=None)
            )
        elif "max" in range_def:
            expectations.append(
                lambda batch, col=column, max_val=range_def["max"]: 
                batch.expect_column_max_to_be_between(col, min_value=None, max_value=max_val)
            )
    return expectations


def unique_checks(columns: List[str]) -> List[Callable]:
    """Ensure columns contain unique values."""
    return [lambda batch, col=col: batch.expect_column_values_to_be_unique(col) for col in columns]


def format_checks(column_formats: Dict[str, str]) -> List[Callable]:
    """Validate values match regex patterns."""
    return [lambda batch, col=col, pattern=pattern: batch.expect_column_values_to_match_regex(col, regex=pattern) 
            for col, pattern in column_formats.items()]


def set_membership_checks(column_sets: Dict[str, List[Any]]) -> List[Callable]:
    """Validate values are in allowed sets."""
    return [lambda batch, col=col, values=values: batch.expect_column_values_to_be_in_set(col, value_set=values) 
            for col, values in column_sets.items()]


def row_count_check(min_value: int = 0, max_value: int = None) -> List[Callable]:
    """Validate table row count."""
    if max_value is not None:
        return [lambda batch: batch.expect_table_row_count_to_be_between(min_value=min_value, max_value=max_value)]
    else:
        return [lambda batch: batch.expect_table_row_count_to_be_between(min_value=min_value, max_value=None)]


def data_freshness_check(timestamp_column: str, max_age_hours: int) -> List[Callable]:
    """Validate data is recent."""
    from datetime import datetime, timedelta
    cutoff = datetime.now() - timedelta(hours=max_age_hours)
    return [lambda batch: batch.expect_column_values_to_be_between(
        timestamp_column, 
        min_value=cutoff, 
        max_value=None,
        parse_strings_as_datetimes=True
    )]


def completeness_check(columns: List[str], threshold: float = 0.95) -> List[Callable]:
    """Validate columns are mostly complete (non-null)."""
    return [lambda batch, col=col, t=threshold: batch.expect_column_values_to_not_be_null(col, mostly=t) 
            for col in columns]


def combine(*suite_results: List[Callable]) -> List[Callable]:
    """Combine multiple expectation suites."""
    combined = []
    for suite in suite_results:
        combined.extend(suite)
    return combined


__all__ = [
    'null_checks',
    'type_checks',
    'range_checks',
    'unique_checks',
    'format_checks',
    'set_membership_checks',
    'row_count_check',
    'data_freshness_check',
    'completeness_check',
    'combine'
]
