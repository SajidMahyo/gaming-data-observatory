"""KPI aggregation module for Steam gaming data."""

from pathlib import Path
from types import TracebackType

from python.storage.duckdb_manager import DuckDBManager


class KPIAggregator:
    """Aggregates raw Steam data into daily KPIs and exports for dashboards."""

    def __init__(self, db_path: Path):
        """Initialize the KPI aggregator.

        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = db_path
        self.db_manager: DuckDBManager | None = None

    def __enter__(self) -> "KPIAggregator":
        """Context manager entry."""
        self.db_manager = DuckDBManager(db_path=self.db_path)
        self.db_manager.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Context manager exit."""
        if self.db_manager:
            self.db_manager.__exit__(exc_type, exc_val, exc_tb)

    def create_daily_kpis(self) -> None:
        """Create daily KPIs aggregation table from raw Steam data.

        Aggregates hourly player counts into daily metrics:
        - Average CCU (Concurrent Users)
        - Peak CCU
        - Minimum CCU
        - Number of samples per day
        """
        sql = """
            CREATE OR REPLACE TABLE daily_kpis AS
            SELECT
                date,
                game_name,
                app_id,
                AVG(player_count) as avg_ccu,
                MAX(player_count) as peak_ccu,
                MIN(player_count) as min_ccu,
                COUNT(*) as samples
            FROM steam_raw
            GROUP BY date, game_name, app_id
            ORDER BY date DESC, peak_ccu DESC
        """
        if self.db_manager:
            self.db_manager.query(sql)

    def export_latest_kpis(self, output_path: Path, days: int = 7) -> None:
        """Export latest N days of KPIs to JSON.

        Args:
            output_path: Path to output JSON file
            days: Number of days to include (default: 7)
        """
        sql = f"""
            SELECT * FROM daily_kpis
            WHERE CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '{days}' DAY
            ORDER BY date DESC, peak_ccu DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_game_rankings(self, output_path: Path) -> None:
        """Export game rankings based on average peak players.

        Args:
            output_path: Path to output JSON file
        """
        sql = """
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
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_all_daily_kpis(self, output_path: Path) -> None:
        """Export all daily KPIs to JSON.

        Args:
            output_path: Path to output JSON file
        """
        if self.db_manager:
            self.db_manager.export_to_json(table_name="daily_kpis", output_path=output_path)

    def run_full_aggregation(self, output_dir: Path) -> None:
        """Run full aggregation pipeline and export all KPIs.

        Args:
            output_dir: Directory to write JSON exports
        """
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create daily KPIs table
        self.create_daily_kpis()

        # Export all KPIs
        self.export_all_daily_kpis(output_path=output_dir / "daily_kpis.json")
        self.export_latest_kpis(output_path=output_dir / "latest_kpis.json", days=7)
        self.export_game_rankings(output_path=output_dir / "game_rankings.json")
