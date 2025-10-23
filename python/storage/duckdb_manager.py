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

    def create_game_metadata_table(self) -> None:
        """Create game_metadata table for storing Steam game metadata.

        Table schema includes:
        - app_id (INTEGER PRIMARY KEY): Steam application ID
        - name, type, description, release_date: Text fields
        - developers, publishers, platforms, categories, genres: JSON arrays
        - metacritic_score, required_age: Numeric fields
        - metacritic_url: Text field
        - price_info, tags: JSON objects
        - is_free: Boolean
        - collected_at: Timestamp
        """
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS game_metadata (
                app_id INTEGER PRIMARY KEY,
                name VARCHAR,
                type VARCHAR,
                description TEXT,
                developers JSON,
                publishers JSON,
                is_free BOOLEAN,
                required_age INTEGER,
                release_date VARCHAR,
                platforms JSON,
                metacritic_score INTEGER,
                metacritic_url VARCHAR,
                categories JSON,
                genres JSON,
                price_info JSON,
                tags JSON,
                collected_at VARCHAR
            )
        """
        )

    def upsert_game_metadata(self, metadata: dict[str, Any]) -> None:
        """Insert or update game metadata in the database.

        Uses INSERT OR REPLACE to handle both new and existing games.

        Args:
            metadata: Dictionary containing game metadata from SteamStoreCollector.
                     Must include at minimum: app_id, name, type

        Example:
            >>> metadata = collector.collect_full_metadata(730)
            >>> manager.upsert_game_metadata(metadata)
        """
        import json

        # Prepare JSON fields
        developers_json = json.dumps(metadata.get("developers", []))
        publishers_json = json.dumps(metadata.get("publishers", []))
        platforms_json = json.dumps(metadata.get("platforms", []))
        categories_json = json.dumps(metadata.get("categories", []))
        genres_json = json.dumps(metadata.get("genres", []))
        price_info_json = json.dumps(metadata.get("price_info", {}))
        tags_json = json.dumps(metadata.get("tags", {}))

        # Use parameterized query to avoid SQL injection
        self.conn.execute(
            """
            INSERT OR REPLACE INTO game_metadata (
                app_id, name, type, description, developers, publishers,
                is_free, required_age, release_date, platforms,
                metacritic_score, metacritic_url, categories, genres,
                price_info, tags, collected_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            [
                metadata["app_id"],
                metadata.get("name", ""),
                metadata.get("type", ""),
                metadata.get("description", ""),
                developers_json,
                publishers_json,
                metadata.get("is_free", False),
                metadata.get("required_age", 0),
                metadata.get("release_date", ""),
                platforms_json,
                metadata.get("metacritic_score"),
                metadata.get("metacritic_url"),
                categories_json,
                genres_json,
                price_info_json,
                tags_json,
                metadata.get("collected_at", ""),
            ],
        )

    def get_game_metadata(self, app_id: int) -> dict[str, Any] | None:
        """Retrieve game metadata by app_id.

        Args:
            app_id: Steam application ID

        Returns:
            Dictionary with game metadata, or None if not found

        Example:
            >>> metadata = manager.get_game_metadata(730)
            >>> print(metadata['name'])
            'Counter-Strike 2'
        """
        result = self.query(f"SELECT * FROM game_metadata WHERE app_id = {app_id}")

        if len(result) == 0:
            return None

        # Convert row to dictionary
        row = result.iloc[0]
        return row.to_dict()

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
