"""Tests for cleanup utility - 30-day retention policy."""

import time
from pathlib import Path

import pytest

from python.utils.cleanup import cleanup_old_data


class TestCleanup:
    """Test suite for data cleanup utility."""

    def test_cleanup_old_parquet_files(self, tmp_path: Path) -> None:
        """Test that old Parquet files are deleted."""
        data_dir = tmp_path / "data" / "raw" / "steam"
        data_dir.mkdir(parents=True)

        # Create old file (40 days old)
        old_file = data_dir / "old.parquet"
        old_file.write_text("old data")
        old_time = time.time() - (40 * 24 * 60 * 60)  # 40 days ago
        Path(old_file).touch()
        import os

        os.utime(old_file, (old_time, old_time))

        # Create recent file (10 days old)
        recent_file = data_dir / "recent.parquet"
        recent_file.write_text("recent data")

        # Cleanup files older than 30 days
        result = cleanup_old_data(base_path=data_dir, days_to_keep=30)

        # Verify old file deleted, recent file kept
        assert not old_file.exists()
        assert recent_file.exists()
        assert result["files_deleted"] == 1
        assert result["bytes_freed"] > 0

    def test_cleanup_nested_partitioned_structure(self, tmp_path: Path) -> None:
        """Test cleanup in partitioned directory structure (date=YYYY-MM-DD/game_id=XXX/)."""
        base_dir = tmp_path / "data" / "raw" / "steam"

        # Create old partitioned files (35 days old)
        old_partition = base_dir / "date=2024-09-15" / "game_id=730"
        old_partition.mkdir(parents=True)
        old_file1 = old_partition / "data_abc.parquet"
        old_file2 = old_partition / "data_def.parquet"
        old_file1.write_text("old")
        old_file2.write_text("old")

        old_time = time.time() - (35 * 24 * 60 * 60)
        import os

        for f in [old_file1, old_file2]:
            os.utime(f, (old_time, old_time))

        # Create recent partitioned files (5 days old)
        recent_partition = base_dir / "date=2025-10-17" / "game_id=730"
        recent_partition.mkdir(parents=True)
        recent_file = recent_partition / "data_xyz.parquet"
        recent_file.write_text("recent")

        # Cleanup
        result = cleanup_old_data(base_path=base_dir, days_to_keep=30)

        # Verify old files deleted
        assert not old_file1.exists()
        assert not old_file2.exists()
        assert result["files_deleted"] == 2

        # Verify recent file kept
        assert recent_file.exists()

    def test_cleanup_no_files_to_delete(self, tmp_path: Path) -> None:
        """Test cleanup when all files are recent."""
        data_dir = tmp_path / "data" / "raw" / "steam"
        data_dir.mkdir(parents=True)

        # Create only recent files
        for i in range(5):
            file = data_dir / f"recent_{i}.parquet"
            file.write_text(f"data {i}")

        result = cleanup_old_data(base_path=data_dir, days_to_keep=30)

        # Verify nothing deleted
        assert result["files_deleted"] == 0
        assert result["bytes_freed"] == 0
        assert len(list(data_dir.rglob("*.parquet"))) == 5

    def test_cleanup_empty_directory(self, tmp_path: Path) -> None:
        """Test cleanup on empty directory."""
        data_dir = tmp_path / "data" / "raw" / "steam"
        data_dir.mkdir(parents=True)

        result = cleanup_old_data(base_path=data_dir, days_to_keep=30)

        assert result["files_deleted"] == 0
        assert result["bytes_freed"] == 0

    def test_cleanup_with_custom_retention_days(self, tmp_path: Path) -> None:
        """Test cleanup with different retention periods."""
        data_dir = tmp_path / "data" / "raw" / "steam"
        data_dir.mkdir(parents=True)

        # Create file 10 days old
        file = data_dir / "data.parquet"
        file.write_text("data")
        old_time = time.time() - (10 * 24 * 60 * 60)
        import os

        os.utime(file, (old_time, old_time))

        # With 30-day retention: file should be kept
        result = cleanup_old_data(base_path=data_dir, days_to_keep=30)
        assert result["files_deleted"] == 0
        assert file.exists()

        # With 7-day retention: file should be deleted
        result = cleanup_old_data(base_path=data_dir, days_to_keep=7)
        assert result["files_deleted"] == 1
        assert not file.exists()

    def test_cleanup_only_parquet_files(self, tmp_path: Path) -> None:
        """Test that cleanup only deletes .parquet files, not other files."""
        data_dir = tmp_path / "data" / "raw" / "steam"
        data_dir.mkdir(parents=True)

        # Create old files of different types
        old_time = time.time() - (40 * 24 * 60 * 60)
        import os

        old_parquet = data_dir / "old.parquet"
        old_parquet.write_text("old parquet")
        os.utime(old_parquet, (old_time, old_time))

        old_json = data_dir / "old.json"
        old_json.write_text("{}")
        os.utime(old_json, (old_time, old_time))

        old_txt = data_dir / "old.txt"
        old_txt.write_text("text")
        os.utime(old_txt, (old_time, old_time))

        result = cleanup_old_data(base_path=data_dir, days_to_keep=30)

        # Only .parquet file should be deleted
        assert not old_parquet.exists()
        assert old_json.exists()
        assert old_txt.exists()
        assert result["files_deleted"] == 1

    def test_cleanup_calculates_bytes_freed(self, tmp_path: Path) -> None:
        """Test that cleanup correctly calculates bytes freed."""
        data_dir = tmp_path / "data" / "raw" / "steam"
        data_dir.mkdir(parents=True)

        # Create old file with known size
        old_file = data_dir / "old.parquet"
        content = "x" * 1024  # 1 KB
        old_file.write_text(content)

        old_time = time.time() - (40 * 24 * 60 * 60)
        import os

        os.utime(old_file, (old_time, old_time))

        result = cleanup_old_data(base_path=data_dir, days_to_keep=30)

        assert result["files_deleted"] == 1
        assert result["bytes_freed"] == 1024

    def test_cleanup_nonexistent_directory(self, tmp_path: Path) -> None:
        """Test cleanup on non-existent directory raises error."""
        nonexistent = tmp_path / "does_not_exist"

        with pytest.raises(FileNotFoundError):
            cleanup_old_data(base_path=nonexistent, days_to_keep=30)

    def test_cleanup_removes_empty_partition_directories(self, tmp_path: Path) -> None:
        """Test that empty partition directories are removed after cleanup."""
        base_dir = tmp_path / "data" / "raw" / "steam"

        # Create old partitioned file
        old_partition = base_dir / "date=2024-09-15" / "game_id=730"
        old_partition.mkdir(parents=True)
        old_file = old_partition / "data.parquet"
        old_file.write_text("old")

        old_time = time.time() - (40 * 24 * 60 * 60)
        import os

        os.utime(old_file, (old_time, old_time))

        result = cleanup_old_data(base_path=base_dir, days_to_keep=30, remove_empty_dirs=True)

        # Verify file deleted
        assert result["files_deleted"] == 1
        assert not old_file.exists()

        # Verify empty partition directories removed
        assert not old_partition.exists()
        assert not (base_dir / "date=2024-09-15").exists()

    def test_cleanup_dry_run_mode(self, tmp_path: Path) -> None:
        """Test cleanup in dry-run mode (no actual deletion)."""
        data_dir = tmp_path / "data" / "raw" / "steam"
        data_dir.mkdir(parents=True)

        # Create old file
        old_file = data_dir / "old.parquet"
        old_file.write_text("old data")
        old_time = time.time() - (40 * 24 * 60 * 60)
        import os

        os.utime(old_file, (old_time, old_time))

        # Run in dry-run mode
        result = cleanup_old_data(base_path=data_dir, days_to_keep=30, dry_run=True)

        # Verify file NOT deleted but counted
        assert old_file.exists()
        assert result["files_deleted"] == 1
        assert result["bytes_freed"] > 0
