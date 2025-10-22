"""Integration test: Steam collector to Parquet storage."""

from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from python.collectors.steam import SteamCollector
from python.storage.parquet_writer import ParquetWriter


def test_collect_and_save_cs2_data(tmp_path: Path) -> None:
    """Test end-to-end: collect CS2 data and save to Parquet."""
    # Setup
    collector = SteamCollector()
    writer = ParquetWriter(base_path=tmp_path / "data" / "raw" / "steam")

    # Mock API response
    with patch("requests.get") as mock_get:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"response": {"player_count": 987654, "result": 1}}
        mock_get.return_value = mock_response

        # Collect data
        game_data = collector.get_game_data(730)  # CS2

    # Save to Parquet with partitioning
    writer.save([game_data], partition_cols=["date", "game_id"])

    # Verify file structure
    parquet_files = list(tmp_path.rglob("*.parquet"))
    assert len(parquet_files) == 1

    # Verify partitioned structure
    assert "game_id=730" in str(parquet_files[0])
    assert "date=" in str(parquet_files[0])

    # Verify data content
    df = pd.read_parquet(parquet_files[0])
    assert len(df) == 1
    assert df.iloc[0]["app_id"] == 730
    assert df.iloc[0]["game_name"] == "Counter-Strike 2"
    assert df.iloc[0]["player_count"] == 987654
    assert "timestamp" in df.columns


def test_collect_multiple_games_and_save(tmp_path: Path) -> None:
    """Test collecting multiple top games and saving to Parquet."""
    collector = SteamCollector()
    writer = ParquetWriter(base_path=tmp_path / "data" / "raw" / "steam")

    # Mock API responses
    with patch.object(collector, "get_game_data") as mock_get_data:
        mock_get_data.side_effect = [
            {
                "app_id": 730,
                "game_name": "Counter-Strike 2",
                "player_count": 100000,
                "timestamp": "2025-01-22T14:00:00+00:00",
            },
            {
                "app_id": 570,
                "game_name": "Dota 2",
                "player_count": 50000,
                "timestamp": "2025-01-22T14:00:00+00:00",
            },
            {
                "app_id": 578080,
                "game_name": "PUBG: BATTLEGROUNDS",
                "player_count": 30000,
                "timestamp": "2025-01-22T14:00:00+00:00",
            },
        ]

        # Collect top 3 games
        games_data = collector.collect_top_games(limit=3)

    # Save all games
    writer.save(games_data, partition_cols=["date", "game_id"])

    # Verify 3 partitions created
    game_partitions = list(tmp_path.rglob("game_id=*"))
    assert len(game_partitions) == 3

    # Verify each game has data
    parquet_files = list(tmp_path.rglob("*.parquet"))
    assert len(parquet_files) == 3

    # Verify total records
    df = pd.concat([pd.read_parquet(f) for f in parquet_files])
    assert len(df) == 3
    assert set(df["app_id"].values) == {730, 570, 578080}
