#!/usr/bin/env python3
"""Data loader for game rankings from DuckDB."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from python.storage.duckdb_manager import DuckDBManager  # noqa: E402


def main() -> None:
    """Calculate game rankings from DuckDB and output as JSON."""
    db_path = project_root / "data" / "duckdb" / "gaming.db"

    if not db_path.exists():
        print("[]")
        return

    try:
        with DuckDBManager(db_path=db_path) as db:
            result = db.query(
                """
                SELECT
                    game_name,
                    app_id,
                    AVG(peak_ccu) as avg_peak,
                    MAX(peak_ccu) as all_time_peak,
                    COUNT(DISTINCT date) as days_tracked
                FROM daily_kpis
                GROUP BY game_name, app_id
                ORDER BY avg_peak DESC
            """
            )
            print(result.to_json(orient="records", indent=2))
    except Exception:
        print("[]")


if __name__ == "__main__":
    main()
