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
        result = self.conn.execute(
            f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'
        """
        ).fetchone()
        table_exists = result[0] if result else 0

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

        # Replace NaN/inf with None to ensure valid JSON (NaN is not valid JSON)
        import numpy as np
        df = df.replace([np.nan, np.inf, -np.inf], None)

        df.to_json(output_path, orient="records", date_format="iso", indent=2)

    def create_game_list_table(self) -> None:
        """Create game_list table for discovered games.

        Lightweight table tracking discovered games and metadata collection status.
        Decouples discovery from metadata enrichment.
        """
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS game_list (
                igdb_id INTEGER PRIMARY KEY,
                game_name VARCHAR NOT NULL,
                metadata_collected BOOLEAN DEFAULT false,
                discovered_at TIMESTAMP NOT NULL,
                discovery_source VARCHAR NOT NULL,
                discovery_rank INTEGER
            )
        """
        )

        # Create indexes
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_game_list_metadata_collected
            ON game_list(metadata_collected)
        """
        )
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_game_list_source
            ON game_list(discovery_source)
        """
        )

    def create_game_metadata_table(self) -> None:
        """Create unified game_metadata table with IGDB as primary key.

        Table schema includes:
        - igdb_id (INTEGER PRIMARY KEY): IGDB ID as source of truth
        - game_name: Canonical game name
        - Platform IDs: steam_app_id, twitch_game_id, youtube_channel_id, etc.
        - IGDB metadata: summary, release date, cover (ratings collected via collect igdb-ratings)
        - Steam metadata: description, required_age (KPIs in steam_kpis)
        - Categories: genres, themes, platforms, game_modes, developers, publishers (JSON)
        - Tracking: discovery_source, dates, is_active flags
        """
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS game_metadata (
                -- Primary identifiers
                igdb_id INTEGER PRIMARY KEY,
                game_name VARCHAR NOT NULL,
                slug VARCHAR,

                -- Platform-specific IDs
                steam_app_id INTEGER,
                twitch_game_id VARCHAR,
                youtube_channel_id VARCHAR,
                epic_id VARCHAR,
                gog_id VARCHAR,

                -- IGDB metadata (ratings removed - collected via collect igdb-ratings)
                igdb_summary TEXT,
                first_release_date TIMESTAMP,
                cover_url VARCHAR,

                -- Steam metadata (static only - KPIs in steam_kpis)
                steam_description TEXT,
                steam_required_age INTEGER,

                -- Categories (JSON)
                genres JSON,
                themes JSON,
                platforms JSON,
                game_modes JSON,
                developers JSON,
                publishers JSON,
                websites JSON,

                -- Metadata
                discovery_source VARCHAR,
                discovery_date TIMESTAMP,
                last_updated TIMESTAMP,
                is_active BOOLEAN DEFAULT true,

                -- Tracking flags
                track_steam BOOLEAN DEFAULT true,
                track_twitch BOOLEAN DEFAULT true,
                track_reddit BOOLEAN DEFAULT false
            )
        """
        )

        # Create indexes
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_game_metadata_steam
            ON game_metadata(steam_app_id)
        """
        )
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_game_metadata_twitch
            ON game_metadata(twitch_game_id)
        """
        )
        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_game_metadata_active
            ON game_metadata(is_active)
        """
        )

    def upsert_game_metadata(self, metadata: dict[str, Any]) -> None:
        """Insert or update game metadata in the database.

        Uses INSERT OR REPLACE to handle both new and existing games.

        Args:
            metadata: Dictionary containing enriched game metadata from IGDBCollector.
                     Must include at minimum: igdb_id, game_name

        Example:
            >>> metadata = igdb_collector.enrich_game(2963)
            >>> manager.upsert_game_metadata(metadata)
        """
        import json

        # Prepare JSON fields
        genres_json = json.dumps(metadata.get("genres", []))
        themes_json = json.dumps(metadata.get("themes", []))
        platforms_json = json.dumps(metadata.get("platforms", []))
        game_modes_json = json.dumps(metadata.get("game_modes", []))
        developers_json = json.dumps(metadata.get("developers", []))
        publishers_json = json.dumps(metadata.get("publishers", []))
        websites_json = json.dumps(metadata.get("websites", {}))

        # Build values list
        values = [
            metadata["igdb_id"],
            metadata["game_name"],
            metadata.get("slug"),
            metadata.get("steam_app_id"),
            metadata.get("twitch_game_id"),
            metadata.get("youtube_channel_id"),
            metadata.get("epic_id"),
            metadata.get("gog_id"),
            metadata.get("igdb_summary"),
            metadata.get("first_release_date"),
            metadata.get("cover_url"),
            metadata.get("steam_description"),
            metadata.get("steam_required_age"),
            genres_json,
            themes_json,
            platforms_json,
            game_modes_json,
            developers_json,
            publishers_json,
            websites_json,
            metadata.get("discovery_source"),
            metadata.get("discovery_date"),
            metadata.get("last_updated"),
            metadata.get("is_active", True),
            metadata.get("track_steam", True),
            metadata.get("track_twitch", True),
            metadata.get("track_reddit", False),
        ]

        # Use parameterized query with ON CONFLICT (more reliable than INSERT OR REPLACE for complex tables)
        self.conn.execute(
            """
            INSERT INTO game_metadata (
                igdb_id, game_name, slug,
                steam_app_id, twitch_game_id, youtube_channel_id, epic_id, gog_id,
                igdb_summary, first_release_date, cover_url,
                steam_description, steam_required_age,
                genres, themes, platforms, game_modes, developers, publishers, websites,
                discovery_source, discovery_date, last_updated, is_active,
                track_steam, track_twitch, track_reddit
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (igdb_id) DO UPDATE SET
                game_name = EXCLUDED.game_name,
                slug = EXCLUDED.slug,
                steam_app_id = EXCLUDED.steam_app_id,
                twitch_game_id = EXCLUDED.twitch_game_id,
                youtube_channel_id = EXCLUDED.youtube_channel_id,
                epic_id = EXCLUDED.epic_id,
                gog_id = EXCLUDED.gog_id,
                igdb_summary = EXCLUDED.igdb_summary,
                first_release_date = EXCLUDED.first_release_date,
                cover_url = EXCLUDED.cover_url,
                steam_description = EXCLUDED.steam_description,
                steam_required_age = EXCLUDED.steam_required_age,
                genres = EXCLUDED.genres,
                themes = EXCLUDED.themes,
                platforms = EXCLUDED.platforms,
                game_modes = EXCLUDED.game_modes,
                developers = EXCLUDED.developers,
                publishers = EXCLUDED.publishers,
                websites = EXCLUDED.websites,
                discovery_source = EXCLUDED.discovery_source,
                discovery_date = EXCLUDED.discovery_date,
                last_updated = EXCLUDED.last_updated,
                is_active = EXCLUDED.is_active,
                track_steam = EXCLUDED.track_steam,
                track_twitch = EXCLUDED.track_twitch,
                track_reddit = EXCLUDED.track_reddit
        """,
            values,
        )

    def get_game_metadata(
        self, igdb_id: int | None = None, steam_app_id: int | None = None
    ) -> dict[str, Any] | None:
        """Retrieve game metadata by IGDB ID or Steam app ID.

        Args:
            igdb_id: IGDB game ID (preferred)
            steam_app_id: Steam application ID (fallback)

        Returns:
            Dictionary with game metadata, or None if not found

        Example:
            >>> metadata = manager.get_game_metadata(igdb_id=2963)
            >>> metadata = manager.get_game_metadata(steam_app_id=730)
            >>> print(metadata['game_name'])
            'Dota 2'
        """
        if igdb_id is not None:
            result = self.query(f"SELECT * FROM game_metadata WHERE igdb_id = {igdb_id}")
        elif steam_app_id is not None:
            result = self.query(f"SELECT * FROM game_metadata WHERE steam_app_id = {steam_app_id}")
        else:
            raise ValueError("Must provide either igdb_id or steam_app_id")

        if len(result) == 0:
            return None

        # Convert row to dictionary
        row = result.iloc[0]
        metadata_dict: dict[str, Any] = row.to_dict()
        return metadata_dict

    def get_active_games_for_platform(self, platform: str) -> list[dict[str, Any]]:
        """Get all active games that have an ID for the specified platform.

        Args:
            platform: Platform name ("steam", "twitch", "reddit")

        Returns:
            List of dictionaries with game metadata

        Example:
            >>> steam_games = manager.get_active_games_for_platform("steam")
            >>> for game in steam_games:
            ...     print(f"{game['game_name']}: {game['steam_app_id']}")
        """
        platform_column = f"{platform}_app_id" if platform == "steam" else f"{platform}_game_id"
        track_column = f"track_{platform}"

        result = self.query(
            f"""
            SELECT *
            FROM game_metadata
            WHERE is_active = true
              AND {track_column} = true
              AND {platform_column} IS NOT NULL
        """
        )

        games_list: list[dict[str, Any]] = result.to_dict("records")  # type: ignore[assignment]
        return games_list

    def create_discovery_history_table(self) -> None:
        """Create discovery_history table for audit trail.

        Table tracks all discovery operations and their results.
        """
        # Create sequence for auto-incrementing ID
        self.conn.execute(
            "CREATE SEQUENCE IF NOT EXISTS discovery_history_seq START 1"
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS discovery_history (
                id INTEGER PRIMARY KEY DEFAULT nextval('discovery_history_seq'),
                discovery_date TIMESTAMP NOT NULL,
                discovery_source VARCHAR NOT NULL,
                games_discovered INTEGER,
                games_updated INTEGER,
                execution_time_seconds REAL,
                notes TEXT
            )
        """
        )

    def log_discovery(
        self,
        source: str,
        games_discovered: int,
        games_updated: int,
        execution_time: float,
        notes: str | None = None,
    ) -> None:
        """Log a discovery operation to the history table.

        Args:
            source: Discovery source (e.g., "igdb_popular", "manual")
            games_discovered: Number of new games found
            games_updated: Number of existing games updated
            execution_time: Execution time in seconds
            notes: Optional notes about the operation
        """
        from datetime import UTC, datetime

        self.conn.execute(
            """
            INSERT INTO discovery_history (
                discovery_date, discovery_source, games_discovered,
                games_updated, execution_time_seconds, notes
            ) VALUES (?, ?, ?, ?, ?, ?)
        """,
            [
                datetime.now(UTC).isoformat(),
                source,
                games_discovered,
                games_updated,
                execution_time,
                notes,
            ],
        )

    def insert_discovered_games(
        self, games: list[dict[str, Any]], source: str
    ) -> tuple[int, int]:
        """Insert discovered games into game_list table.

        Args:
            games: List of game dictionaries with igdb_id and game_name
            source: Discovery source (e.g., "igdb-popular", "steam-top-ccu")

        Returns:
            Tuple of (new_games_added, existing_games_skipped)
        """
        from datetime import UTC, datetime

        new_count = 0
        skipped_count = 0

        for rank, game in enumerate(games, 1):
            # Check if game already exists
            existing = self.query(
                f"SELECT igdb_id FROM game_list WHERE igdb_id = {game['igdb_id']}"
            )

            if len(existing) > 0:
                skipped_count += 1
                continue

            # Insert new game
            self.conn.execute(
                """
                INSERT INTO game_list (
                    igdb_id, game_name, metadata_collected,
                    discovered_at, discovery_source, discovery_rank
                ) VALUES (?, ?, ?, ?, ?, ?)
            """,
                [
                    game["igdb_id"],
                    game["game_name"],
                    False,
                    datetime.now(UTC).isoformat(),
                    source,
                    rank,
                ],
            )
            new_count += 1

        return new_count, skipped_count

    def get_games_needing_metadata(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Get games that need metadata collection.

        Args:
            limit: Maximum number of games to return (default: all)

        Returns:
            List of game dictionaries with igdb_id and game_name
        """
        query = """
            SELECT igdb_id, game_name, discovered_at, discovery_source
            FROM game_list
            WHERE metadata_collected = false
            ORDER BY discovered_at ASC
        """

        if limit:
            query += f" LIMIT {limit}"

        result = self.query(query)
        games_list: list[dict[str, Any]] = result.to_dict("records")  # type: ignore[assignment]
        return games_list

    def get_all_games_for_metadata_refresh(self) -> list[dict[str, Any]]:
        """Get all games for full metadata refresh.

        Returns:
            List of all game dictionaries with igdb_id and game_name
        """
        result = self.query(
            """
            SELECT igdb_id, game_name, discovered_at, discovery_source
            FROM game_list
            ORDER BY discovered_at ASC
        """
        )
        games_list: list[dict[str, Any]] = result.to_dict("records")  # type: ignore[assignment]
        return games_list

    def mark_metadata_collected(self, igdb_id: int) -> None:
        """Mark a game's metadata as collected.

        Args:
            igdb_id: IGDB game ID
        """
        self.conn.execute(
            "UPDATE game_list SET metadata_collected = true WHERE igdb_id = ?",
            [igdb_id],
        )

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
