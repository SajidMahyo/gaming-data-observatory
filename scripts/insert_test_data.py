#!/usr/bin/env python3
"""Insert fake historical data for testing time series visualizations."""

import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from python.storage.duckdb_manager import DuckDBManager  # noqa: E402


def generate_fake_daily_kpis(days: int = 7, app_ids: list[int] | None = None) -> list[dict]:
    """Generate fake daily KPIs for the last N days.

    Creates realistic player count variations for top games.

    Args:
        days: Number of days to generate
        app_ids: Optional list of specific app IDs to generate data for
    """
    all_games = [
        {"app_id": 730, "game_name": "Counter-Strike 2", "base_peak": 1000000},
        {"app_id": 570, "game_name": "Dota 2", "base_peak": 500000},
        {"app_id": 578080, "game_name": "PUBG: BATTLEGROUNDS", "base_peak": 250000},
        {"app_id": 1172470, "game_name": "Apex Legends", "base_peak": 50000},
        {"app_id": 271590, "game_name": "Grand Theft Auto V", "base_peak": 40000},
    ]

    # Filter games if specific app_ids requested
    if app_ids:
        games = [g for g in all_games if g["app_id"] in app_ids]
    else:
        games = all_games

    data = []
    today = datetime.now()

    for day_offset in range(days, 0, -1):
        date = (today - timedelta(days=day_offset)).date()

        for game in games:
            # Add some variation (+/- 20%)
            variation = random.uniform(0.8, 1.2)
            base_peak: int = game["base_peak"]  # type: ignore
            peak_ccu = int(base_peak * variation)
            avg_ccu = int(peak_ccu * random.uniform(0.6, 0.8))
            min_ccu = int(peak_ccu * random.uniform(0.4, 0.6))

            data.append(
                {
                    "date": date.isoformat(),
                    "game_name": game["game_name"],
                    "app_id": game["app_id"],
                    "avg_ccu": avg_ccu,
                    "peak_ccu": peak_ccu,
                    "min_ccu": min_ccu,
                    "samples": 24,
                }
            )

    return data


def main() -> None:
    """Insert test data into DuckDB."""
    import argparse

    parser = argparse.ArgumentParser(description="Insert test data into DuckDB")
    parser.add_argument(
        "--days", type=int, default=120, help="Number of days to generate (default: 120)"
    )
    parser.add_argument("--app-ids", type=str, help="Comma-separated app IDs (default: all games)")
    args = parser.parse_args()

    # Parse app IDs if provided
    app_ids = None
    if args.app_ids:
        app_ids = [int(x.strip()) for x in args.app_ids.split(",")]

    db_path = project_root / "data" / "duckdb" / "gaming.db"

    if not db_path.exists():
        print("❌ Database not found")
        return

    print(f"📊 Generating fake historical data for last {args.days} days...")
    if app_ids:
        print(f"   For app IDs: {', '.join(map(str, app_ids))}")
    fake_data = generate_fake_daily_kpis(days=args.days, app_ids=app_ids)

    with DuckDBManager(db_path=db_path) as db:
        # Import pandas for DataFrame
        import pandas as pd

        df = pd.DataFrame(fake_data)

        # Register as temp table
        db.conn.register("temp_data", df)

        # Insert into daily_kpis (using INSERT OR REPLACE to avoid duplicates)
        db.query(
            """
            INSERT OR REPLACE INTO daily_kpis
            SELECT * FROM temp_data
        """
        )

        db.conn.unregister("temp_data")

        # Verify insertion
        count = db.query("SELECT COUNT(*) as count FROM daily_kpis").iloc[0]["count"]
        print(f"✅ Inserted test data. Total rows in daily_kpis: {count}")

        # Show sample
        sample = db.query(
            """
            SELECT date, game_name, peak_ccu
            FROM daily_kpis
            WHERE game_name = 'Counter-Strike 2'
            ORDER BY date DESC
            LIMIT 7
        """
        )
        print("\n📈 Sample data (CS2 last 7 days):")
        print(sample)


if __name__ == "__main__":
    main()
