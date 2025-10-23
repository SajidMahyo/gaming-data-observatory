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

    def test_create_game_metadata_table(self, tmp_path: Path) -> None:
        """Test creating game_metadata table with correct schema."""
        db_path = tmp_path / "test.db"

        with DuckDBManager(db_path=db_path) as manager:
            manager.create_game_metadata_table()

            # Verify table exists and has correct columns
            result = manager.query(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'game_metadata' ORDER BY ordinal_position"
            )
            column_names = result["column_name"].tolist()

            # Check that essential columns exist
            assert "app_id" in column_names
            assert "name" in column_names
            assert "developers" in column_names
            assert "genres" in column_names
            assert "tags" in column_names
            assert "collected_at" in column_names

    def test_upsert_game_metadata(self, tmp_path: Path) -> None:
        """Test upserting game metadata (insert or replace)."""
        db_path = tmp_path / "test.db"

        game_metadata = {
            "app_id": 730,
            "name": "Counter-Strike 2",
            "type": "game",
            "description": "FPS game",
            "developers": ["Valve"],
            "publishers": ["Valve"],
            "is_free": True,
            "required_age": 0,
            "release_date": "2023-09-27",
            "platforms": ["windows", "linux"],
            "metacritic_score": None,
            "metacritic_url": None,
            "categories": ["Multi-player", "Online Multi-Player"],
            "genres": ["Action", "Free To Play"],
            "price_info": {"currency": "USD", "price": 0, "is_free": True},
            "tags": {"FPS": 1000, "Shooter": 900},
            "collected_at": "2025-10-23 10:00:00",
        }

        with DuckDBManager(db_path=db_path) as manager:
            manager.create_game_metadata_table()
            manager.upsert_game_metadata(game_metadata)

            # Verify data was inserted
            result = manager.query("SELECT * FROM game_metadata WHERE app_id = 730")
            assert len(result) == 1
            assert result.iloc[0]["name"] == "Counter-Strike 2"
            assert result.iloc[0]["is_free"] == True  # noqa: E712

    def test_upsert_game_metadata_updates_existing(self, tmp_path: Path) -> None:
        """Test that upserting existing game updates the record."""
        db_path = tmp_path / "test.db"

        game_v1 = {
            "app_id": 730,
            "name": "Counter-Strike 2",
            "type": "game",
            "description": "Old description",
            "developers": ["Valve"],
            "publishers": ["Valve"],
            "is_free": True,
            "required_age": 0,
            "release_date": "2023-09-27",
            "platforms": ["windows"],
            "metacritic_score": None,
            "metacritic_url": None,
            "categories": ["Multi-player"],
            "genres": ["Action"],
            "price_info": {"currency": "USD", "price": 0},
            "tags": {"FPS": 1000},
            "collected_at": "2025-10-23 10:00:00",
        }

        game_v2 = {
            "app_id": 730,
            "name": "Counter-Strike 2",
            "type": "game",
            "description": "Updated description",
            "developers": ["Valve"],
            "publishers": ["Valve"],
            "is_free": True,
            "required_age": 0,
            "release_date": "2023-09-27",
            "platforms": ["windows", "linux"],
            "metacritic_score": 85,
            "metacritic_url": "https://metacritic.com/cs2",
            "categories": ["Multi-player", "Online Multi-Player"],
            "genres": ["Action", "Free To Play"],
            "price_info": {"currency": "USD", "price": 0, "is_free": True},
            "tags": {"FPS": 2000, "Shooter": 1500},
            "collected_at": "2025-10-23 12:00:00",
        }

        with DuckDBManager(db_path=db_path) as manager:
            manager.create_game_metadata_table()

            # Insert first version
            manager.upsert_game_metadata(game_v1)
            result1 = manager.query("SELECT COUNT(*) as count FROM game_metadata")
            assert result1.iloc[0]["count"] == 1

            # Upsert second version (should update, not insert new row)
            manager.upsert_game_metadata(game_v2)
            result2 = manager.query("SELECT COUNT(*) as count FROM game_metadata")
            assert result2.iloc[0]["count"] == 1

            # Verify data was updated
            result3 = manager.query("SELECT * FROM game_metadata WHERE app_id = 730")
            assert result3.iloc[0]["description"] == "Updated description"
            assert result3.iloc[0]["metacritic_score"] == 85
            assert result3.iloc[0]["collected_at"] == "2025-10-23 12:00:00"

    def test_get_game_metadata_by_app_id(self, tmp_path: Path) -> None:
        """Test retrieving game metadata by app_id."""
        db_path = tmp_path / "test.db"

        game1 = {
            "app_id": 730,
            "name": "Counter-Strike 2",
            "type": "game",
            "description": "FPS",
            "developers": ["Valve"],
            "publishers": ["Valve"],
            "is_free": True,
            "required_age": 0,
            "release_date": "2023-09-27",
            "platforms": ["windows"],
            "metacritic_score": None,
            "metacritic_url": None,
            "categories": ["Multi-player"],
            "genres": ["Action"],
            "price_info": {"currency": "USD", "price": 0},
            "tags": {"FPS": 1000},
            "collected_at": "2025-10-23 10:00:00",
        }

        game2 = {
            "app_id": 570,
            "name": "Dota 2",
            "type": "game",
            "description": "MOBA",
            "developers": ["Valve"],
            "publishers": ["Valve"],
            "is_free": True,
            "required_age": 0,
            "release_date": "2013-07-09",
            "platforms": ["windows", "linux"],
            "metacritic_score": 90,
            "metacritic_url": "https://metacritic.com/dota2",
            "categories": ["Multi-player"],
            "genres": ["Strategy"],
            "price_info": {"currency": "USD", "price": 0},
            "tags": {"MOBA": 2000},
            "collected_at": "2025-10-23 10:00:00",
        }

        with DuckDBManager(db_path=db_path) as manager:
            manager.create_game_metadata_table()
            manager.upsert_game_metadata(game1)
            manager.upsert_game_metadata(game2)

            # Get specific game
            result = manager.get_game_metadata(730)
            assert result is not None
            assert result["name"] == "Counter-Strike 2"
            assert result["app_id"] == 730

            # Test non-existent game
            result_none = manager.get_game_metadata(99999)
            assert result_none is None
