"""Tests for DuckDB manager."""

from pathlib import Path

import pandas as pd

from python.storage.duckdb_manager import DuckDBManager


class TestDuckDBManager:
    """Test suite for DuckDBManager class."""

    def test_init_creates_database_file(self, tmp_path: Path) -> None:
        """Test that initializing creates a database file."""
        db_path = tmp_path / "test.db"
        manager = DuckDBManager(db_path=db_path)
        manager.close()

        assert db_path.exists()

    def test_append_data_creates_table_and_inserts(self, tmp_path: Path) -> None:
        """Test appending data creates table and inserts rows."""
        db_path = tmp_path / "test.db"
        manager = DuckDBManager(db_path=db_path)

        # Create test data
        df = pd.DataFrame(
            {
                "game_name": ["Counter-Strike 2", "Dota 2"],
                "player_count": [1102182, 620592],
                "timestamp": pd.to_datetime(["2025-10-22 14:00:00", "2025-10-22 14:00:00"]),
            }
        )

        # Append data
        manager.append_data(df, table_name="steam_data")

        # Verify data was inserted
        result = manager.query("SELECT COUNT(*) as count FROM steam_data")
        assert result.iloc[0]["count"] == 2

        manager.close()

    def test_append_data_multiple_times(self, tmp_path: Path) -> None:
        """Test appending data multiple times accumulates rows."""
        db_path = tmp_path / "test.db"
        manager = DuckDBManager(db_path=db_path)

        df1 = pd.DataFrame(
            {
                "game_name": ["Counter-Strike 2"],
                "player_count": [1102182],
                "timestamp": pd.to_datetime(["2025-10-22 14:00:00"]),
            }
        )

        df2 = pd.DataFrame(
            {
                "game_name": ["Dota 2"],
                "player_count": [620592],
                "timestamp": pd.to_datetime(["2025-10-22 15:00:00"]),
            }
        )

        manager.append_data(df1, table_name="steam_data")
        manager.append_data(df2, table_name="steam_data")

        result = manager.query("SELECT COUNT(*) as count FROM steam_data")
        assert result.iloc[0]["count"] == 2

        manager.close()

    def test_query_returns_dataframe(self, tmp_path: Path) -> None:
        """Test that query returns a pandas DataFrame."""
        db_path = tmp_path / "test.db"
        manager = DuckDBManager(db_path=db_path)

        df = pd.DataFrame(
            {
                "game_name": ["Counter-Strike 2", "Dota 2"],
                "player_count": [1102182, 620592],
                "timestamp": pd.to_datetime(["2025-10-22 14:00:00", "2025-10-22 14:00:00"]),
            }
        )

        manager.append_data(df, table_name="steam_data")

        # Query with filtering
        result = manager.query("SELECT * FROM steam_data WHERE game_name = 'Counter-Strike 2'")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["game_name"] == "Counter-Strike 2"
        assert result.iloc[0]["player_count"] == 1102182

        manager.close()

    def test_query_with_aggregation(self, tmp_path: Path) -> None:
        """Test SQL query with aggregation functions."""
        db_path = tmp_path / "test.db"
        manager = DuckDBManager(db_path=db_path)

        df = pd.DataFrame(
            {
                "game_name": ["Counter-Strike 2", "Counter-Strike 2", "Dota 2"],
                "player_count": [1000000, 1200000, 620592],
                "timestamp": pd.to_datetime(
                    [
                        "2025-10-22 14:00:00",
                        "2025-10-22 15:00:00",
                        "2025-10-22 14:00:00",
                    ]
                ),
            }
        )

        manager.append_data(df, table_name="steam_data")

        result = manager.query(
            """
            SELECT
                game_name,
                AVG(player_count) as avg_players,
                MAX(player_count) as max_players
            FROM steam_data
            GROUP BY game_name
            ORDER BY game_name
        """
        )

        assert len(result) == 2
        cs2_row = result[result["game_name"] == "Counter-Strike 2"].iloc[0]
        assert cs2_row["avg_players"] == 1100000
        assert cs2_row["max_players"] == 1200000

        manager.close()

    def test_export_to_json(self, tmp_path: Path) -> None:
        """Test exporting table to JSON file."""
        db_path = tmp_path / "test.db"
        output_json = tmp_path / "output.json"
        manager = DuckDBManager(db_path=db_path)

        df = pd.DataFrame(
            {
                "game_name": ["Counter-Strike 2", "Dota 2"],
                "player_count": [1102182, 620592],
                "timestamp": pd.to_datetime(["2025-10-22 14:00:00", "2025-10-22 14:00:00"]),
            }
        )

        manager.append_data(df, table_name="steam_data")
        manager.export_to_json(table_name="steam_data", output_path=output_json)

        # Verify JSON file exists and has correct content
        assert output_json.exists()

        import json

        with open(output_json) as f:
            data = json.load(f)

        assert len(data) == 2
        assert data[0]["game_name"] == "Counter-Strike 2"
        assert data[0]["player_count"] == 1102182

        manager.close()

    def test_export_to_json_with_query(self, tmp_path: Path) -> None:
        """Test exporting with custom SQL query."""
        db_path = tmp_path / "test.db"
        output_json = tmp_path / "filtered.json"
        manager = DuckDBManager(db_path=db_path)

        df = pd.DataFrame(
            {
                "game_name": ["Counter-Strike 2", "Dota 2", "PUBG"],
                "player_count": [1102182, 620592, 284000],
                "timestamp": pd.to_datetime(
                    [
                        "2025-10-22 14:00:00",
                        "2025-10-22 14:00:00",
                        "2025-10-22 14:00:00",
                    ]
                ),
            }
        )

        manager.append_data(df, table_name="steam_data")

        # Export only games with > 500k players
        manager.export_to_json(
            query="SELECT * FROM steam_data WHERE player_count > 500000",
            output_path=output_json,
        )

        import json

        with open(output_json) as f:
            data = json.load(f)

        assert len(data) == 2
        assert all(game["player_count"] > 500000 for game in data)

        manager.close()

    def test_table_persistence_across_connections(self, tmp_path: Path) -> None:
        """Test that data persists when reopening database."""
        db_path = tmp_path / "test.db"

        # First connection: insert data
        manager1 = DuckDBManager(db_path=db_path)
        df = pd.DataFrame(
            {
                "game_name": ["Counter-Strike 2"],
                "player_count": [1102182],
                "timestamp": pd.to_datetime(["2025-10-22 14:00:00"]),
            }
        )
        manager1.append_data(df, table_name="steam_data")
        manager1.close()

        # Second connection: verify data exists
        manager2 = DuckDBManager(db_path=db_path)
        result = manager2.query("SELECT COUNT(*) as count FROM steam_data")
        assert result.iloc[0]["count"] == 1
        manager2.close()

    def test_context_manager(self, tmp_path: Path) -> None:
        """Test that DuckDBManager works as context manager."""
        db_path = tmp_path / "test.db"

        with DuckDBManager(db_path=db_path) as manager:
            df = pd.DataFrame(
                {
                    "game_name": ["Counter-Strike 2"],
                    "player_count": [1102182],
                    "timestamp": pd.to_datetime(["2025-10-22 14:00:00"]),
                }
            )
            manager.append_data(df, table_name="steam_data")

        # Verify connection was closed properly and data persists
        with DuckDBManager(db_path=db_path) as manager:
            result = manager.query("SELECT COUNT(*) as count FROM steam_data")
            assert result.iloc[0]["count"] == 1
