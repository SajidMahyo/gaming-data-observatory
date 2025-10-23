#!/usr/bin/env python3
"""Export static JSON files from DuckDB without re-aggregating."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from python.processors.aggregator import KPIAggregator  # noqa: E402


def main() -> None:
    """Export all JSON files from existing DuckDB tables."""
    db_path = project_root / "data" / "duckdb" / "gaming.db"
    output_dir = project_root / "src" / "data"

    if not db_path.exists():
        print("❌ Database not found")
        return

    print("📤 Exporting JSON files from DuckDB...")
    output_dir.mkdir(parents=True, exist_ok=True)

    with KPIAggregator(db_path=db_path) as aggregator:
        # Export all JSON files (without re-aggregating)
        aggregator.export_game_metadata(output_path=output_dir / "game-metadata.json")
        aggregator.export_game_rankings(output_path=output_dir / "game_rankings.json")
        aggregator.export_all_daily_kpis(output_path=output_dir / "daily_kpis.json")
        aggregator.export_latest_kpis(
            output_path=output_dir / "latest_kpis.json", days=30
        )  # 30 days for chart
        aggregator.export_weekly_kpis(output_path=output_dir / "weekly_kpis.json")
        aggregator.export_monthly_kpis(output_path=output_dir / "monthly_kpis.json")

    print(f"✅ All JSON files exported to {output_dir}/")
    print("  • game-metadata.json")
    print("  • game_rankings.json")
    print("  • daily_kpis.json")
    print("  • latest_kpis.json (last 30 days)")
    print("  • weekly_kpis.json")
    print("  • monthly_kpis.json")


if __name__ == "__main__":
    main()
