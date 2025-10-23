#!/usr/bin/env python3
"""Insert comprehensive test data into DuckDB (daily, weekly, monthly KPIs)."""

import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from python.storage.duckdb_manager import DuckDBManager  # noqa: E402


def generate_test_data(days: int = 120) -> list[dict]:
    """Generate test data for CS2 for the last N days.

    Returns daily data with realistic variations.
    """
    data = []
    today = datetime.now()
    base_peak = 1000000

    for day_offset in range(days, 0, -1):
        date = (today - timedelta(days=day_offset)).date()

        # Add realistic variation (+/- 20%)
        variation = random.uniform(0.8, 1.2)
        peak_ccu = int(base_peak * variation)
        avg_ccu = int(peak_ccu * random.uniform(0.6, 0.8))
        min_ccu = int(peak_ccu * random.uniform(0.4, 0.6))

        data.append(
            {
                "date": date.isoformat(),
                "game_name": "Counter-Strike 2",
                "app_id": 730,
                "avg_ccu": avg_ccu,
                "peak_ccu": peak_ccu,
                "min_ccu": min_ccu,
                "samples": 24,
            }
        )

    return data


def main() -> None:
    """Insert test data into all KPI tables."""
    db_path = project_root / "data" / "duckdb" / "gaming.db"

    if not db_path.exists():
        print("❌ Database not found")
        return

    print("📊 Generating 120 days of test data for Counter-Strike 2...")
    daily_data = generate_test_data(days=120)

    with DuckDBManager(db_path=db_path) as db:
        import pandas as pd

        # 1. Insert into daily_kpis
        print("1️⃣ Inserting into daily_kpis...")
        df_daily = pd.DataFrame(daily_data)
        db.conn.register("temp_daily", df_daily)
        db.query(
            """
            INSERT OR REPLACE INTO daily_kpis
            SELECT * FROM temp_daily
        """
        )
        db.conn.unregister("temp_daily")

        # 2. Calculate and insert into weekly_kpis
        print("2️⃣ Calculating and inserting into weekly_kpis...")
        db.query(
            """
            INSERT OR REPLACE INTO weekly_kpis
            SELECT
                DATE_TRUNC('week', date) as week_start,
                game_name,
                app_id,
                AVG(peak_ccu) as avg_peak,
                MAX(peak_ccu) as max_peak,
                SUM(samples) as total_samples,
                COUNT(DISTINCT date) as days_in_week
            FROM daily_kpis
            WHERE app_id = 730
            GROUP BY week_start, game_name, app_id
        """
        )

        # 3. Calculate and insert into monthly_kpis
        print("3️⃣ Calculating and inserting into monthly_kpis...")
        db.query(
            """
            INSERT OR REPLACE INTO monthly_kpis
            SELECT
                DATE_TRUNC('month', week_start) as month_start,
                game_name,
                app_id,
                AVG(avg_peak) as avg_peak,
                MAX(max_peak) as max_peak,
                SUM(total_samples) as total_samples,
                COUNT(DISTINCT week_start) as weeks_in_month
            FROM weekly_kpis
            WHERE app_id = 730
            GROUP BY month_start, game_name, app_id
        """
        )

        # Verify data
        daily_count = db.query("SELECT COUNT(*) as count FROM daily_kpis WHERE app_id = 730").iloc[
            0
        ]["count"]
        weekly_count = db.query(
            "SELECT COUNT(*) as count FROM weekly_kpis WHERE app_id = 730"
        ).iloc[0]["count"]
        monthly_count = db.query(
            "SELECT COUNT(*) as count FROM monthly_kpis WHERE app_id = 730"
        ).iloc[0]["count"]

        print("\n✅ Data inserted successfully:")
        print(f"   • Daily KPIs: {daily_count} rows")
        print(f"   • Weekly KPIs: {weekly_count} rows")
        print(f"   • Monthly KPIs: {monthly_count} rows")

        # Show sample
        sample = db.query(
            """
            SELECT date, game_name, peak_ccu
            FROM daily_kpis
            WHERE app_id = 730
            ORDER BY date DESC
            LIMIT 7
        """
        )
        print("\n📈 Sample (CS2 last 7 days):")
        print(sample)


if __name__ == "__main__":
    main()
