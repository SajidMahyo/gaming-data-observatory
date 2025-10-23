#!/usr/bin/env python3
"""Data loader for weekly KPIs from DuckDB."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from python.storage.duckdb_manager import DuckDBManager  # noqa: E402


def main() -> None:
    """Load weekly KPIs from DuckDB and output as JSON."""
    db_path = project_root / "data" / "duckdb" / "gaming.db"

    if not db_path.exists():
        print("[]")
        return

    try:
        with DuckDBManager(db_path=db_path) as db:
            result = db.query(
                """
                SELECT
                    week_start,
                    game_name,
                    app_id,
                    avg_peak,
                    max_peak,
                    total_samples,
                    days_in_week
                FROM weekly_kpis
                ORDER BY week_start DESC, avg_peak DESC
            """
            )
            print(result.to_json(orient="records", date_format="iso", indent=2))
    except Exception:
        print("[]")


if __name__ == "__main__":
    main()
