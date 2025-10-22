"""Cleanup utility for data retention policy - removes old Parquet files."""

import time
from pathlib import Path
from typing import TypedDict


class CleanupResult(TypedDict):
    """Result of cleanup operation."""

    files_deleted: int
    bytes_freed: int


def cleanup_old_data(
    base_path: Path,
    days_to_keep: int = 30,
    remove_empty_dirs: bool = False,
    dry_run: bool = False,
) -> CleanupResult:
    """Remove Parquet files older than specified retention period.

    Args:
        base_path: Root directory to search for old files
        days_to_keep: Number of days to retain files (default: 30)
        remove_empty_dirs: Remove empty parent directories after cleanup
        dry_run: If True, report what would be deleted without actually deleting

    Returns:
        CleanupResult with number of files deleted and bytes freed

    Raises:
        FileNotFoundError: If base_path does not exist

    Example:
        >>> result = cleanup_old_data(Path("data/raw/steam"), days_to_keep=30)
        >>> print(f"Deleted {result['files_deleted']} files, freed {result['bytes_freed']} bytes")
    """
    if not base_path.exists():
        raise FileNotFoundError(f"Directory not found: {base_path}")

    cutoff_time = time.time() - (days_to_keep * 24 * 60 * 60)

    files_deleted = 0
    bytes_freed = 0
    empty_dirs: set[Path] = set()

    # Find all Parquet files older than retention period
    for parquet_file in base_path.rglob("*.parquet"):
        if parquet_file.stat().st_mtime < cutoff_time:
            # Track file size before deletion
            file_size = parquet_file.stat().st_size
            bytes_freed += file_size

            if not dry_run:
                parquet_file.unlink()

            files_deleted += 1

            # Track potentially empty parent directories
            if remove_empty_dirs:
                empty_dirs.add(parquet_file.parent)

    # Remove empty directories if requested
    if remove_empty_dirs and not dry_run:
        # Sort directories by depth (deepest first) to remove leaf dirs first
        sorted_dirs = sorted(empty_dirs, key=lambda p: len(p.parts), reverse=True)

        for directory in sorted_dirs:
            if directory.exists() and not any(directory.iterdir()):
                directory.rmdir()

                # Check if parent is now empty
                parent = directory.parent
                while parent != base_path and not any(parent.iterdir()):
                    parent.rmdir()
                    parent = parent.parent

    return CleanupResult(files_deleted=files_deleted, bytes_freed=bytes_freed)
