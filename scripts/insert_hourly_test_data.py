#!/usr/bin/env python3
"""Insert hourly test data into DuckDB for last 48 hours."""

import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from python.storage.duckdb_manager import DuckDBManager  # noqa: E402


def generate_hourly_test_data(hours: int = 48) -> list[dict]:
    """Generate hourly test data for CS2 for the last N hours.

    Returns hourly data with realistic variations.
    """
    data = []
    now = datetime.now()
    base_peak = 1000000

    for hour_offset in range(hours, 0, -1):
        timestamp = now - timedelta(hours=hour_offset)

        # Add realistic hourly variation (higher during peak hours 18h-22h)
        hour_of_day = timestamp.hour
        if 18 <= hour_of_day <= 22:
            # Peak gaming hours - higher CCU
            variation = random.uniform(0.9, 1.1)
        elif 2 <= hour_of_day <= 6:
            # Night hours - lower CCU
            variation = random.uniform(0.6, 0.75)
        else:
            # Normal hours
            variation = random.uniform(0.75, 0.95)

        peak_ccu = int(base_peak * variation)

        data.append(
            {
                "app_id": 730,
                "game_name": "Counter-Strike 2",
                "player_count": peak_ccu,
                "timestamp": timestamp.isoformat(),
                "date": timestamp.date().isoformat(),
                "game_id": 730,
            }
        )

    return data


def main() -> None:
    """Insert hourly test data into DuckDB."""
    db_path = project_root / "data" / "duckdb" / "gaming.db"

    if not db_path.exists():
        print("❌ Database not found")
        return

    print("📊 Generating hourly test data for last 48 hours...")
    hourly_data = generate_hourly_test_data(hours=48)

    with DuckDBManager(db_path=db_path) as db:
        import pandas as pd

        # Insert into steam_raw (which will be aggregated into hourly_kpis)
        df = pd.DataFrame(hourly_data)

        # Register as temp table
        db.conn.register("temp_hourly", df)

        # Insert into steam_raw
        db.query(
            """
            INSERT INTO steam_raw
            SELECT * FROM temp_hourly
        """
        )

        db.conn.unregister("temp_hourly")

        # Verify insertion
        count = db.query("SELECT COUNT(*) as count FROM steam_raw WHERE app_id = 730").iloc[0][
            "count"
        ]
        print(f"✅ Inserted hourly test data. Total rows in steam_raw for CS2: {count}")

        # Show sample
        sample = db.query(
            """
            SELECT timestamp, game_name, player_count
            FROM steam_raw
            WHERE game_name = 'Counter-Strike 2'
            ORDER BY timestamp DESC
            LIMIT 5
        """
        )
        print("\n📈 Sample data (last 5 records):")
        print(sample)


if __name__ == "__main__":
    main()
