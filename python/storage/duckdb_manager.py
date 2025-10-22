"""DuckDB manager for gaming data storage and analytics."""

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


class DuckDBManager:
    """Manages DuckDB database for gaming analytics data.

    Provides methods for:
    - Appending DataFrames to tables
    - Executing SQL queries
    - Exporting data to JSON format
    - Context manager support for automatic connection handling
    """

    def __init__(self, db_path: Path) -> None:
        """Initialize DuckDB connection.

        Args:
            db_path: Path to the DuckDB database file.
                     Will be created if it doesn't exist.
        """
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))

    def append_data(self, df: pd.DataFrame, table_name: str) -> None:
        """Append DataFrame to a table in DuckDB.

        Creates table if it doesn't exist. If table exists, appends data.

        Args:
            df: DataFrame to append
            table_name: Name of the table in DuckDB

        Example:
            >>> df = pd.DataFrame({'game': ['CS2'], 'players': [1000000]})
            >>> manager.append_data(df, 'steam_data')
        """
        # Register DataFrame as a temporary view
        self.conn.register("temp_df", df)

        # Check if table exists
        table_exists = self.conn.execute(
            f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'
        """
        ).fetchone()[0]

        if table_exists:
            # Append to existing table
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
        else:
            # Create new table from DataFrame
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")

        # Unregister temp view
        self.conn.unregister("temp_df")

    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame.

        Args:
            sql: SQL query string

        Returns:
            Query results as pandas DataFrame

        Example:
            >>> result = manager.query("SELECT * FROM steam_data WHERE player_count > 1000000")
        """
        result: pd.DataFrame = self.conn.execute(sql).df()
        return result

    def export_to_json(
        self,
        output_path: Path,
        table_name: str | None = None,
        query: str | None = None,
    ) -> None:
        """Export table or query results to JSON file.

        Either table_name or query must be provided, not both.

        Args:
            output_path: Path where JSON file will be saved
            table_name: Name of table to export (mutually exclusive with query)
            query: Custom SQL query to export (mutually exclusive with table_name)

        Raises:
            ValueError: If both or neither table_name and query are provided

        Example:
            >>> manager.export_to_json(Path('data.json'), table_name='steam_data')
            >>> manager.export_to_json(Path('filtered.json'), query='SELECT * FROM steam_data WHERE player_count > 500000')
        """
        if (table_name is None and query is None) or (table_name is not None and query is not None):
            raise ValueError("Must provide exactly one of: table_name or query")

        output_path.parent.mkdir(parents=True, exist_ok=True)

        if table_name:
            sql = f"SELECT * FROM {table_name}"
        else:
            assert query is not None
            sql = query

        # Get data as DataFrame and export to JSON
        df = self.query(sql)
        df.to_json(output_path, orient="records", date_format="iso", indent=2)

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self.conn:
            self.conn.close()

    def __enter__(self) -> "DuckDBManager":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - closes connection."""
        self.close()
