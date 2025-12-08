"""
Validation decorators for automatic database testing
"""

from functools import wraps
from typing import Callable, List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


def validate_before(
    validator,
    table_name: Optional[str] = None,
    query: Optional[str] = None,
    suite_name: Optional[str] = None,
    expectations: Optional[List[Dict[str, Any]]] = None,
    raise_on_failure: bool = True
):
    """
    Decorator to validate database state BEFORE function execution.
    
    Args:
        validator: DatabaseValidator instance
        table_name: Table to validate (mutually exclusive with query)
        query: SQL query to validate (mutually exclusive with table_name)
        suite_name: Expectation suite name
        expectations: List of expectations to add
        raise_on_failure: Whether to raise exception on validation failure
    
    Example:
        @validate_before(validator, table_name="users", expectations=[
            {"expectation_type": "expect_table_row_count_to_be_between", "min_value": 0, "max_value": 1000}
        ])
        def insert_user(name, email):
            # Insert logic
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"Pre-validation for {func.__name__}")
            
            try:
                if table_name:
                    results = validator.validate_table(
                        table_name=table_name,
                        suite_name=suite_name,
                        expectations=expectations
                    )
                elif query:
                    asset_name = f"{func.__name__}_pre"
                    results = validator.validate_query(
                        query=query,
                        asset_name=asset_name,
                        suite_name=suite_name,
                        expectations=expectations
                    )
                else:
                    raise ValueError("Either table_name or query must be provided")
                
                success = results.success
                
                if not success and raise_on_failure:
                    raise AssertionError(f"Pre-validation failed for {func.__name__}")
                
                logger.info(f"Pre-validation {'passed' if success else 'failed'}")
                
            except Exception as e:
                logger.error(f"Pre-validation error: {e}")
                if raise_on_failure:
                    raise
            
            # Execute original function
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


def validate_after(
    validator,
    table_name: Optional[str] = None,
    query: Optional[str] = None,
    suite_name: Optional[str] = None,
    expectations: Optional[List[Dict[str, Any]]] = None,
    raise_on_failure: bool = True
):
    """
    Decorator to validate database state AFTER function execution.
    
    Args:
        validator: DatabaseValidator instance
        table_name: Table to validate (mutually exclusive with query)
        query: SQL query to validate (mutually exclusive with table_name)
        suite_name: Expectation suite name
        expectations: List of expectations to add
        raise_on_failure: Whether to raise exception on validation failure
    
    Example:
        @validate_after(validator, table_name="users", expectations=[
            {"expectation_type": "expect_column_values_to_be_unique", "column": "email"}
        ])
        def insert_user(name, email):
            # Insert logic
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Execute original function first
            result = func(*args, **kwargs)
            
            logger.info(f"Post-validation for {func.__name__}")
            
            try:
                if table_name:
                    validation_results = validator.validate_table(
                        table_name=table_name,
                        suite_name=suite_name,
                        expectations=expectations
                    )
                elif query:
                    asset_name = f"{func.__name__}_post"
                    validation_results = validator.validate_query(
                        query=query,
                        asset_name=asset_name,
                        suite_name=suite_name,
                        expectations=expectations
                    )
                else:
                    raise ValueError("Either table_name or query must be provided")
                
                success = validation_results.success
                
                if not success and raise_on_failure:
                    raise AssertionError(f"Post-validation failed for {func.__name__}")
                
                logger.info(f"Post-validation {'passed' if success else 'failed'}")
                
            except Exception as e:
                logger.error(f"Post-validation error: {e}")
                if raise_on_failure:
                    raise
            
            return result
        
        return wrapper
    return decorator


def validate_both(
    validator,
    table_name: Optional[str] = None,
    query_before: Optional[str] = None,
    query_after: Optional[str] = None,
    suite_name_before: Optional[str] = None,
    suite_name_after: Optional[str] = None,
    expectations_before: Optional[List[Dict[str, Any]]] = None,
    expectations_after: Optional[List[Dict[str, Any]]] = None,
    raise_on_failure: bool = True
):
    """
    Decorator to validate database state BEFORE and AFTER function execution.
    
    Args:
        validator: DatabaseValidator instance
        table_name: Table to validate
        query_before: SQL query for pre-validation
        query_after: SQL query for post-validation
        suite_name_before: Expectation suite name for pre-validation
        suite_name_after: Expectation suite name for post-validation
        expectations_before: Expectations for pre-validation
        expectations_after: Expectations for post-validation
        raise_on_failure: Whether to raise exception on validation failure
    
    Example:
        @validate_both(
            validator,
            table_name="orders",
            expectations_before=[{"expectation_type": "expect_table_row_count_to_be_between", "min_value": 0, "max_value": 10000}],
            expectations_after=[{"expectation_type": "expect_column_values_to_not_be_null", "column": "order_id"}]
        )
        def process_orders():
            # Processing logic
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Pre-validation
            logger.info(f"Pre-validation for {func.__name__}")
            
            try:
                if table_name or query_before:
                    q = query_before if query_before else None
                    t = table_name if not query_before else None
                    
                    if t:
                        results_before = validator.validate_table(
                            table_name=t,
                            suite_name=suite_name_before,
                            expectations=expectations_before
                        )
                    else:
                        asset_name = f"{func.__name__}_pre"
                        results_before = validator.validate_query(
                            query=q,
                            asset_name=asset_name,
                            suite_name=suite_name_before,
                            expectations=expectations_before
                        )
                    
                    if not results_before.success and raise_on_failure:
                        raise AssertionError(f"Pre-validation failed for {func.__name__}")
                
            except Exception as e:
                logger.error(f"Pre-validation error: {e}")
                if raise_on_failure:
                    raise
            
            # Execute original function
            result = func(*args, **kwargs)
            
            # Post-validation
            logger.info(f"Post-validation for {func.__name__}")
            
            try:
                if table_name or query_after:
                    q = query_after if query_after else None
                    t = table_name if not query_after else None
                    
                    if t:
                        results_after = validator.validate_table(
                            table_name=t,
                            suite_name=suite_name_after,
                            expectations=expectations_after
                        )
                    else:
                        asset_name = f"{func.__name__}_post"
                        results_after = validator.validate_query(
                            query=q,
                            asset_name=asset_name,
                            suite_name=suite_name_after,
                            expectations=expectations_after
                        )
                    
                    if not results_after.success and raise_on_failure:
                        raise AssertionError(f"Post-validation failed for {func.__name__}")
                
            except Exception as e:
                logger.error(f"Post-validation error: {e}")
                if raise_on_failure:
                    raise
            
            return result
        
        return wrapper
    return decorator
