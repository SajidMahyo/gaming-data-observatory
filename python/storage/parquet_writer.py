"""Parquet file writer with partitioning support."""

from pathlib import Path
from typing import Any

import pandas as pd


class ParquetWriter:
    """Writer for saving data to partitioned Parquet files."""

    def __init__(self, base_path: Path | str) -> None:
        """
        Initialize Parquet writer.

        Args:
            base_path: Base directory for Parquet files
        """
        self.base_path = Path(base_path)

    def save(self, data: list[dict[str, Any]], partition_cols: list[str] | None = None) -> None:
        """
        Save data to Parquet files with optional partitioning.

        Args:
            data: List of dictionaries containing game data
            partition_cols: Columns to partition by (e.g., ["date", "game_id"])

        Raises:
            ValueError: If data is empty or invalid
            TypeError: If data types are incorrect
        """
        if not data:
            raise ValueError("Cannot save empty data")

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Add derived columns for partitioning
        df = self._add_partition_columns(df)

        # Validate schema
        self._validate_schema(df)

        # Create base directory
        self.base_path.mkdir(parents=True, exist_ok=True)

        if partition_cols:
            # Save with partitioning
            self._save_partitioned(df, partition_cols)
        else:
            # Save single file with game_id and timestamp
            game_id = int(df["game_id"].iloc[0])
            timestamp = pd.to_datetime(df["timestamp"].iloc[0])
            timestamp_str = timestamp.strftime("%Y-%m-%dT%H-%M-%S")
            filename = f"{game_id}_{timestamp_str}.parquet"
            filepath = self.base_path / filename
            df.to_parquet(filepath, index=False)

    def _add_partition_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived columns for partitioning.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with added columns
        """
        if "timestamp" in df.columns:
            # Extract date from timestamp
            df["date"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d")

        if "app_id" in df.columns:
            # Add game_id column (same as app_id)
            df["game_id"] = df["app_id"]

        return df

    def _validate_schema(self, df: pd.DataFrame) -> None:
        """
        Validate DataFrame schema.

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If schema is invalid
            TypeError: If data types are incorrect
        """
        required_cols = ["app_id", "player_count", "timestamp"]

        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        # Validate data types
        if not pd.api.types.is_integer_dtype(df["app_id"]):
            raise TypeError("app_id must be integer type")

        if not pd.api.types.is_integer_dtype(df["player_count"]):
            raise TypeError("player_count must be integer type")

    def _save_partitioned(self, df: pd.DataFrame, partition_cols: list[str]) -> None:
        """
        Save DataFrame with partitioning.

        Args:
            df: DataFrame to save
            partition_cols: Columns to partition by
        """
        # Group by partition columns
        for keys, group in df.groupby(partition_cols):
            # Build partition path
            if isinstance(keys, tuple):
                partition_parts = [
                    f"{col}={key}" for col, key in zip(partition_cols, keys, strict=True)
                ]
            else:
                partition_parts = [f"{partition_cols[0]}={keys}"]

            partition_path = self.base_path / Path(*partition_parts)
            partition_path.mkdir(parents=True, exist_ok=True)

            # Generate filename with game_id and timestamp for uniqueness
            game_id = int(group["game_id"].iloc[0])
            timestamp = pd.to_datetime(group["timestamp"].iloc[0])
            timestamp_str = timestamp.strftime("%Y-%m-%dT%H-%M-%S")
            filename = f"{game_id}_{timestamp_str}.parquet"
            filepath = partition_path / filename

            # Save partition
            group.to_parquet(filepath, index=False)
