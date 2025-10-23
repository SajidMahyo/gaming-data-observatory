#!/usr/bin/env python3
"""Data loader for latest KPIs from DuckDB."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from python.storage.duckdb_manager import DuckDBManager  # noqa: E402


def main() -> None:
    """Load latest 7 days of KPIs from DuckDB and output as JSON."""
    db_path = project_root / "data" / "duckdb" / "gaming.db"

    if not db_path.exists():
        print("[]")
        return

    try:
        with DuckDBManager(db_path=db_path) as db:
            result = db.query(
                """
                SELECT * FROM daily_kpis
                WHERE CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '7' DAY
                ORDER BY date DESC, peak_ccu DESC
            """
            )
            print(result.to_json(orient="records", date_format="iso", indent=2))
    except Exception:
        print("[]")


if __name__ == "__main__":
    main()
