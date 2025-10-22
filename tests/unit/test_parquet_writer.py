"""Tests for Parquet storage writer."""

from pathlib import Path

import pandas as pd
import pytest

from python.storage.parquet_writer import ParquetWriter


class TestParquetWriter:
    """Test suite for ParquetWriter class."""

    def test_save_single_record(self, tmp_path: Path) -> None:
        """Test saving a single game record to Parquet."""
        writer = ParquetWriter(base_path=tmp_path)

        data = {
            "app_id": 730,
            "game_name": "Counter-Strike 2",
            "player_count": 123456,
            "timestamp": "2025-01-22T14:00:00+00:00",
        }

        writer.save([data])

        # Verify file was created
        files = list(tmp_path.rglob("*.parquet"))
        assert len(files) == 1

        # Verify data can be read back
        df = pd.read_parquet(files[0])
        assert len(df) == 1
        assert df.iloc[0]["app_id"] == 730
        assert df.iloc[0]["player_count"] == 123456

    def test_save_partitioned_by_date(self, tmp_path: Path) -> None:
        """Test that data is partitioned by date."""
        writer = ParquetWriter(base_path=tmp_path)

        data = [
            {
                "app_id": 730,
                "game_name": "CS2",
                "player_count": 100000,
                "timestamp": "2025-01-22T14:00:00+00:00",
            },
            {
                "app_id": 570,
                "game_name": "Dota 2",
                "player_count": 50000,
                "timestamp": "2025-01-23T14:00:00+00:00",
            },
        ]

        writer.save(data, partition_cols=["date"])

        # Verify partitioned structure
        date_folders = list(tmp_path.glob("date=*"))
        assert len(date_folders) == 2
        assert any("2025-01-22" in str(f) for f in date_folders)
        assert any("2025-01-23" in str(f) for f in date_folders)

    def test_save_partitioned_by_date_and_game(self, tmp_path: Path) -> None:
        """Test that data is partitioned by date and game_id."""
        writer = ParquetWriter(base_path=tmp_path)

        data = [
            {
                "app_id": 730,
                "game_name": "CS2",
                "player_count": 100000,
                "timestamp": "2025-01-22T14:00:00+00:00",
            },
            {
                "app_id": 570,
                "game_name": "Dota 2",
                "player_count": 50000,
                "timestamp": "2025-01-22T14:00:00+00:00",
            },
        ]

        writer.save(data, partition_cols=["date", "game_id"])

        # Verify nested partition structure
        game_folders = list(tmp_path.rglob("game_id=*"))
        assert len(game_folders) == 2
        assert any("game_id=730" in str(f) for f in game_folders)
        assert any("game_id=570" in str(f) for f in game_folders)

    def test_append_mode(self, tmp_path: Path) -> None:
        """Test appending data to existing partitions."""
        writer = ParquetWriter(base_path=tmp_path)

        data1 = {
            "app_id": 730,
            "game_name": "CS2",
            "player_count": 100000,
            "timestamp": "2025-01-22T14:00:00+00:00",
        }
        data2 = {
            "app_id": 730,
            "game_name": "CS2",
            "player_count": 110000,
            "timestamp": "2025-01-22T15:00:00+00:00",
        }

        writer.save([data1], partition_cols=["date", "game_id"])
        writer.save([data2], partition_cols=["date", "game_id"])

        # Verify both records exist
        all_files = list(tmp_path.rglob("*.parquet"))
        df = pd.concat([pd.read_parquet(f) for f in all_files])
        assert len(df) == 2
        assert set(df["player_count"].values) == {100000, 110000}

    def test_validate_schema(self, tmp_path: Path) -> None:
        """Test that invalid data raises an error."""
        writer = ParquetWriter(base_path=tmp_path)

        invalid_data = {
            "app_id": "invalid",  # Should be int
            "player_count": 123456,
            "timestamp": "2025-01-22T14:00:00+00:00",
        }

        with pytest.raises((ValueError, TypeError)):
            writer.save([invalid_data])

    def test_empty_data(self, tmp_path: Path) -> None:
        """Test that empty data raises an error."""
        writer = ParquetWriter(base_path=tmp_path)

        with pytest.raises(ValueError):
            writer.save([])

    def test_creates_directory_structure(self, tmp_path: Path) -> None:
        """Test that writer creates necessary directories."""
        writer = ParquetWriter(base_path=tmp_path / "nested" / "path")

        data = {
            "app_id": 730,
            "game_name": "CS2",
            "player_count": 100000,
            "timestamp": "2025-01-22T14:00:00+00:00",
        }

        writer.save([data])

        assert (tmp_path / "nested" / "path").exists()
        files = list((tmp_path / "nested" / "path").rglob("*.parquet"))
        assert len(files) > 0

    def test_add_metadata_columns(self, tmp_path: Path) -> None:
        """Test that date and game_id columns are added from timestamp and app_id."""
        writer = ParquetWriter(base_path=tmp_path)

        data = {
            "app_id": 730,
            "game_name": "CS2",
            "player_count": 100000,
            "timestamp": "2025-01-22T14:00:00+00:00",
        }

        writer.save([data], partition_cols=["date", "game_id"])

        files = list(tmp_path.rglob("*.parquet"))
        df = pd.read_parquet(files[0])

        assert "date" in df.columns
        assert "game_id" in df.columns
        assert df.iloc[0]["date"] == "2025-01-22"
        assert df.iloc[0]["game_id"] == 730
