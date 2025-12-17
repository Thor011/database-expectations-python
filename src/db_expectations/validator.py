"""
DatabaseValidator - Core validation engine for database testing
"""

from typing import Optional, Dict, Any, List, Callable, Union
from sqlalchemy import create_engine, inspect
import great_expectations as gx
import pandas as pd
from pathlib import Path


class DatabaseValidator:
    """
    Main validator class for database testing with Great Expectations.

    Features:
    - Multiple database support (PostgreSQL, MySQL, SQL Server, SQLite, Oracle)
    - Pre-built expectation suites
    - Custom validation logic
    - Automatic checkpoint creation
    - Detailed validation reports
    """

    def __init__(
        self,
        connection_string: str,
        context_root_dir: Optional[str] = None,
        data_context_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize database validator.

        Args:
            connection_string: SQLAlchemy connection string
            context_root_dir: Great Expectations context directory (default: ./gx)
            data_context_config: Custom data context configuration
        """
        self.connection_string = connection_string
        self.engine = create_engine(connection_string)

        # Initialize Great Expectations context
        if context_root_dir:
            self.context = gx.get_context(context_root_dir=context_root_dir)
        elif data_context_config:
            self.context = gx.get_context(project_config=data_context_config)
        else:
            # Use default context
            default_dir = Path("gx")
            if not default_dir.exists():
                self.context = gx.get_context(mode="file")
            else:
                self.context = gx.get_context()

        self._setup_datasource()
        self._asset_counter = 0

    def _setup_datasource(self):
        """Set up SQL datasource for Great Expectations."""
        import hashlib
        # Create unique datasource name based on connection string
        conn_hash = hashlib.md5(self.connection_string.encode()).hexdigest()[:8]
        datasource_name = f"database_datasource_{conn_hash}"

        try:
            # Try to get existing datasource
            self.datasource = self.context.get_datasource(datasource_name)
        except Exception:
            # Create new SQL datasource using add_sql method
            try:
                # Try modern API (GX 1.0+)
                self.datasource = self.context.data_sources.add_sql(
                    name=datasource_name,
                    connection_string=self.connection_string
                )
            except AttributeError:
                # Fallback to legacy API
                self.datasource = self.context.sources.add_sql(
                    name=datasource_name,
                    connection_string=self.connection_string
                )

    def validate_table(
        self,
        table_name: str,
        suite_name: Optional[str] = None,
        expectations: Optional[List[Union[Callable, Dict[str, Any]]]] = None
    ) -> Dict[str, Any]:
        """
        Validate a database table using Great Expectations.

        Args:
            table_name: Name of the table to validate
            suite_name: Name of expectation suite (creates default if not provided)
            expectations: List of expectation callables or configurations

        Returns:
            Validation results dictionary
        """
        self._asset_counter += 1
        if suite_name is None:
            suite_name = f"{table_name}_suite_{self._asset_counter}"

        # Create batch definition
        try:
            asset = self.datasource.add_table_asset(
                name=f"{table_name}_asset_{self._asset_counter}",
                table_name=table_name
            )
        except Exception:
            # Asset might already exist
            asset = self.datasource.get_asset(f"{table_name}_asset_{self._asset_counter}")

        batch_request = asset.build_batch_request()

        # Create or get expectation suite
        try:
            self.context.suites.get(suite_name)
        except Exception:
            self.context.suites.add(gx.core.ExpectationSuite(name=suite_name))

        # Get validator (batch)
        batch = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )

        # Run expectations
        if expectations:
            for exp in expectations:
                if callable(exp):
                    # Execute callable expectation
                    exp(batch)
                else:
                    # Add dict-based expectation
                    expectation_type = exp.get("expectation_type")
                    kwargs = exp.get("kwargs", {})
                    if expectation_type and hasattr(batch, expectation_type):
                        getattr(batch, expectation_type)(**kwargs)

        # Run validation
        results = batch.validate()

        # Return formatted results
        return self._format_results(results)

    def validate_query(
        self,
        query: str,
        expectations: Optional[List[Union[Callable, Dict[str, Any]]]] = None,
        asset_name: Optional[str] = None,
        suite_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Validate results of a SQL query.

        Args:
            query: SQL query to execute
            expectations: List of expectation callables or configurations
            asset_name: Name for the data asset (optional)
            suite_name: Name of expectation suite (optional)

        Returns:
            Validation results dictionary
        """
        self._asset_counter += 1
        if asset_name is None:
            asset_name = f"query_asset_{self._asset_counter}"
        if suite_name is None:
            suite_name = f"query_suite_{self._asset_counter}"

        # Create query asset
        try:
            asset = self.datasource.add_query_asset(name=asset_name, query=query)
        except Exception:
            # Asset might already exist
            asset = self.datasource.get_asset(asset_name)

        batch_request = asset.build_batch_request()

        # Create or get expectation suite
        try:
            self.context.suites.get(suite_name)
        except Exception:
            self.context.suites.add(gx.core.ExpectationSuite(name=suite_name))

        # Get validator (batch)
        batch = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )

        # Run expectations
        if expectations:
            for exp in expectations:
                if callable(exp):
                    # Execute callable expectation
                    exp(batch)
                else:
                    # Add dict-based expectation
                    expectation_type = exp.get("expectation_type")
                    kwargs = exp.get("kwargs", {})
                    if expectation_type and hasattr(batch, expectation_type):
                        getattr(batch, expectation_type)(**kwargs)

        # Run validation
        results = batch.validate()

        return self._format_results(results)

    def _format_results(self, results) -> Dict[str, Any]:
        """Format validation results into a clean dictionary."""
        return {
            "success": results.success,
            "statistics": {
                "evaluated_expectations": results.statistics.get("evaluated_expectations", 0),
                "successful_expectations": results.statistics.get("successful_expectations", 0),
                "unsuccessful_expectations": results.statistics.get("unsuccessful_expectations", 0),
                "success_percent": results.statistics.get("success_percent", 0),
            },
            "results": results.results if hasattr(results, "results") else []
        }

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get metadata information about a table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table metadata (columns, types, nullability, etc.)
        """
        inspector = inspect(self.engine)

        columns = inspector.get_columns(table_name)
        pk_constraint = inspector.get_pk_constraint(table_name)
        foreign_keys = inspector.get_foreign_keys(table_name)
        indexes = inspector.get_indexes(table_name)

        return {
            "table_name": table_name,
            "columns": columns,
            "primary_key": pk_constraint,
            "foreign_keys": foreign_keys,
            "indexes": indexes,
        }

    def query_to_dataframe(self, query: str) -> pd.DataFrame:
        """
        Execute a query and return results as pandas DataFrame.

        Args:
            query: SQL query to execute

        Returns:
            pandas DataFrame with query results
        """
        return pd.read_sql(query, self.engine)

    def get_row_count(self, table_name: str) -> int:
        """Get total row count for a table."""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        df = self.query_to_dataframe(query)
        return int(df["count"].iloc[0])

    def close(self):
        """Close database connection."""
        self.engine.dispose()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
