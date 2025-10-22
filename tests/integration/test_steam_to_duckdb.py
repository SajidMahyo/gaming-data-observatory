"""Integration test: Steam collector → Parquet → DuckDB → JSON export."""

from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from python.collectors.steam import SteamCollector
from python.storage.duckdb_manager import DuckDBManager
from python.storage.parquet_writer import ParquetWriter


def test_steam_to_parquet_to_duckdb(tmp_path: Path) -> None:
    """Test full pipeline: Steam API → Parquet → DuckDB → Query."""
    # Setup paths
    parquet_dir = tmp_path / "data" / "raw" / "steam"
    db_path = tmp_path / "data" / "duckdb" / "gaming.db"

    # 1. Collect Steam data
    collector = SteamCollector()
    writer = ParquetWriter(base_path=parquet_dir)

    with patch("requests.get") as mock_get:
        # Mock CS2 data
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"response": {"player_count": 1102182, "result": 1}}
        mock_get.return_value = mock_response

        game_data = collector.get_game_data(730)  # CS2

    # 2. Save to Parquet
    writer.save([game_data], partition_cols=["date", "game_id"])

    # Verify Parquet file was created
    parquet_files = list(parquet_dir.rglob("*.parquet"))
    assert len(parquet_files) == 1

    # 3. Load Parquet into DuckDB
    with DuckDBManager(db_path=db_path) as db:
        # Read parquet file and append to DuckDB
        df = pd.read_parquet(parquet_files[0])
        db.append_data(df, table_name="steam_raw")

        # 4. Query from DuckDB
        result = db.query("SELECT * FROM steam_raw WHERE app_id = 730")

        assert len(result) == 1
        assert result.iloc[0]["app_id"] == 730
        assert result.iloc[0]["game_name"] == "Counter-Strike 2"
        assert result.iloc[0]["player_count"] == 1102182


def test_multiple_collections_to_duckdb(tmp_path: Path) -> None:
    """Test multiple hourly collections accumulating in DuckDB."""
    parquet_dir = tmp_path / "data" / "raw" / "steam"
    db_path = tmp_path / "data" / "duckdb" / "gaming.db"

    writer = ParquetWriter(base_path=parquet_dir)

    # Simulate 3 hourly collections with different player counts
    hourly_data = [
        {"hour": "14:00", "player_count": 1000000},
        {"hour": "15:00", "player_count": 1100000},
        {"hour": "16:00", "player_count": 1200000},
    ]

    with DuckDBManager(db_path=db_path) as db:
        for data in hourly_data:
            # Create collector inside loop to avoid caching
            collector = SteamCollector()

            with patch("requests.get") as mock_get:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "response": {"player_count": data["player_count"], "result": 1}
                }
                mock_get.return_value = mock_response

                game_data = collector.get_game_data(730)

            # Save each collection to Parquet
            writer.save([game_data], partition_cols=["date", "game_id"])

            # Load latest parquet into DuckDB
            parquet_files = sorted(parquet_dir.rglob("*.parquet"), key=lambda p: p.stat().st_mtime)
            latest_parquet = parquet_files[-1]
            df = pd.read_parquet(latest_parquet)
            db.append_data(df, table_name="steam_raw")

        # Query aggregated data
        result = db.query(
            """
            SELECT
                COUNT(*) as num_samples,
                AVG(player_count) as avg_players,
                MIN(player_count) as min_players,
                MAX(player_count) as max_players
            FROM steam_raw
        """
        )

        assert result.iloc[0]["num_samples"] == 3
        assert result.iloc[0]["avg_players"] == 1100000
        assert result.iloc[0]["min_players"] == 1000000
        assert result.iloc[0]["max_players"] == 1200000


def test_duckdb_read_parquet_directly(tmp_path: Path) -> None:
    """Test DuckDB reading Parquet files directly without loading."""
    parquet_dir = tmp_path / "data" / "raw" / "steam"
    db_path = tmp_path / "data" / "duckdb" / "gaming.db"

    # Create some test Parquet files
    collector = SteamCollector()
    writer = ParquetWriter(base_path=parquet_dir)

    with patch("requests.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"response": {"player_count": 1102182, "result": 1}}
        mock_get.return_value = mock_response

        cs2_data = collector.get_game_data(730)

    writer.save([cs2_data], partition_cols=["date", "game_id"])

    # DuckDB can query Parquet files directly
    with DuckDBManager(db_path=db_path) as db:
        # Query Parquet files directly using DuckDB's read_parquet
        parquet_pattern = str(parquet_dir / "**" / "*.parquet")
        result = db.query(
            f"""
            SELECT
                game_name,
                player_count
            FROM read_parquet('{parquet_pattern}', hive_partitioning=1)
            WHERE game_name = 'Counter-Strike 2'
        """
        )

        assert len(result) == 1
        assert result.iloc[0]["game_name"] == "Counter-Strike 2"
        assert result.iloc[0]["player_count"] == 1102182


def test_full_pipeline_with_json_export(tmp_path: Path) -> None:
    """Test complete pipeline: Collect → Parquet → DuckDB → Aggregate → JSON export."""
    parquet_dir = tmp_path / "data" / "raw" / "steam"
    db_path = tmp_path / "data" / "duckdb" / "gaming.db"
    json_output = tmp_path / "data" / "exports" / "kpis.json"

    collector = SteamCollector()
    writer = ParquetWriter(base_path=parquet_dir)

    # Mock multiple games
    games_data = [
        {"app_id": 730, "name": "Counter-Strike 2", "count": 1102182},
        {"app_id": 570, "name": "Dota 2", "count": 620592},
        {"app_id": 578080, "name": "PUBG: BATTLEGROUNDS", "count": 284000},
    ]

    with DuckDBManager(db_path=db_path) as db:
        for game in games_data:
            with patch("requests.get") as mock_get:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "response": {"player_count": game["count"], "result": 1}
                }
                mock_get.return_value = mock_response

                game_data = collector.get_game_data(game["app_id"])

            # Save to Parquet
            writer.save([game_data], partition_cols=["date", "game_id"])

            # Load into DuckDB
            parquet_files = list(parquet_dir.rglob("*.parquet"))
            latest = max(parquet_files, key=lambda p: p.stat().st_mtime)
            df = pd.read_parquet(latest)
            db.append_data(df, table_name="steam_raw")

        # Create aggregated KPIs table
        db.query(
            """
            CREATE TABLE daily_kpis AS
            SELECT
                game_name,
                app_id,
                AVG(player_count) as avg_ccu,
                MAX(player_count) as peak_ccu
            FROM steam_raw
            GROUP BY game_name, app_id
            ORDER BY peak_ccu DESC
        """
        )

        # Export to JSON
        db.export_to_json(table_name="daily_kpis", output_path=json_output)

    # Verify JSON export
    assert json_output.exists()

    import json

    with open(json_output) as f:
        data = json.load(f)

    assert len(data) == 3
    assert data[0]["game_name"] == "Counter-Strike 2"  # Highest CCU
    assert data[0]["peak_ccu"] == 1102182
    assert data[1]["game_name"] == "Dota 2"
    assert data[2]["game_name"] == "PUBG: BATTLEGROUNDS"


def test_duckdb_persistence_across_sessions(tmp_path: Path) -> None:
    """Test that DuckDB data persists when database file is reopened."""
    db_path = tmp_path / "gaming.db"

    # First session: Create and populate data
    with DuckDBManager(db_path=db_path) as db:
        df1 = pd.DataFrame(
            {
                "app_id": [730],
                "game_name": ["Counter-Strike 2"],
                "player_count": [1000000],
                "timestamp": pd.to_datetime(["2025-10-22 14:00:00"]),
            }
        )
        db.append_data(df1, table_name="steam_raw")

    # Second session: Add more data
    with DuckDBManager(db_path=db_path) as db:
        df2 = pd.DataFrame(
            {
                "app_id": [730],
                "game_name": ["Counter-Strike 2"],
                "player_count": [1100000],
                "timestamp": pd.to_datetime(["2025-10-22 15:00:00"]),
            }
        )
        db.append_data(df2, table_name="steam_raw")

    # Third session: Verify all data is there
    with DuckDBManager(db_path=db_path) as db:
        result = db.query("SELECT COUNT(*) as count FROM steam_raw")
        assert result.iloc[0]["count"] == 2

        result = db.query("SELECT AVG(player_count) as avg FROM steam_raw")
        assert result.iloc[0]["avg"] == 1050000
