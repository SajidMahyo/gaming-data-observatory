"""KPI aggregation module for Steam gaming data."""

from datetime import datetime, timedelta
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
        """Create or update daily KPIs table from raw Steam data.

        Uses incremental update: only updates the current day's data.
        This is called every hour to update today's aggregated stats.

        Aggregates hourly player counts into daily metrics:
        - Average CCU (Concurrent Users)
        - Peak CCU
        - Minimum CCU
        - Number of samples per day
        """
        if not self.db_manager:
            return

        # Recreate table with PRIMARY KEY constraint
        # Note: We use separate CREATE and INSERT because DuckDB doesn't support
        # PRIMARY KEY in CREATE OR REPLACE TABLE ... AS SELECT
        self.db_manager.query("DROP TABLE IF EXISTS daily_kpis")

        self.db_manager.query(
            """
            CREATE TABLE daily_kpis (
                date DATE,
                game_name VARCHAR,
                app_id INTEGER,
                avg_ccu DOUBLE,
                peak_ccu INTEGER,
                min_ccu INTEGER,
                samples INTEGER,
                PRIMARY KEY (date, app_id)
            )
        """
        )

        # Populate with all historical data from steam_raw
        self.db_manager.query(
            """
            INSERT INTO daily_kpis
            SELECT
                CAST(timestamp AS DATE) as date,
                game_name,
                app_id,
                AVG(player_count) as avg_ccu,
                MAX(player_count) as peak_ccu,
                MIN(player_count) as min_ccu,
                COUNT(*) as samples
            FROM steam_raw
            GROUP BY CAST(timestamp AS DATE), game_name, app_id
        """
        )

    def create_weekly_kpis(self) -> None:
        """Create or update weekly KPIs table from daily KPIs.

        Uses incremental update: only updates the current week's data.
        Aggregates from daily_kpis for the current week.

        Weekly metrics:
        - Average of daily peak CCU
        - Maximum peak CCU for the week
        - Total samples across all days
        - Number of days tracked in the week
        """
        if not self.db_manager:
            return

        # Create table if it doesn't exist
        self.db_manager.query(
            """
            CREATE TABLE IF NOT EXISTS weekly_kpis (
                week_start DATE,
                game_name VARCHAR,
                app_id INTEGER,
                avg_peak DOUBLE,
                max_peak INTEGER,
                total_samples INTEGER,
                days_in_week INTEGER,
                PRIMARY KEY (week_start, app_id)
            )
        """
        )

        # Update only CURRENT WEEK's data (incremental update)
        self.db_manager.query(
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
            WHERE DATE_TRUNC('week', date) = DATE_TRUNC('week', CURRENT_DATE)
            GROUP BY week_start, game_name, app_id
        """
        )

    def create_monthly_kpis(self) -> None:
        """Create or update monthly KPIs table from weekly KPIs.

        Uses incremental update: only updates the current month's data.
        Aggregates from weekly_kpis for the current month.

        Monthly metrics:
        - Average of weekly peak CCU
        - Maximum peak CCU for the month
        - Total samples across all weeks
        - Number of weeks tracked in the month
        """
        if not self.db_manager:
            return

        # Create table if it doesn't exist
        self.db_manager.query(
            """
            CREATE TABLE IF NOT EXISTS monthly_kpis (
                month_start DATE,
                game_name VARCHAR,
                app_id INTEGER,
                avg_peak DOUBLE,
                max_peak INTEGER,
                total_samples INTEGER,
                weeks_in_month INTEGER,
                PRIMARY KEY (month_start, app_id)
            )
        """
        )

        # Update only CURRENT MONTH's data (incremental update)
        self.db_manager.query(
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
            WHERE DATE_TRUNC('month', week_start) = DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY month_start, game_name, app_id
        """
        )

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

    def cleanup_old_raw_data(self, retention_days: int = 7) -> int:
        """Delete raw Steam data older than retention period.

        Implements data retention policy: keep only recent raw data,
        while preserving all aggregated daily/weekly/monthly KPIs.

        Args:
            retention_days: Number of days of raw data to keep (default: 7)

        Returns:
            Number of rows deleted

        Example:
            >>> rows_deleted = aggregator.cleanup_old_raw_data(retention_days=7)
            >>> print(f"Cleaned up {rows_deleted} old raw records")
        """
        if not self.db_manager:
            return 0

        # Count rows before deletion
        count_before = self.db_manager.query("SELECT COUNT(*) as count FROM steam_raw").iloc[0][
            "count"
        ]

        # Delete data older than retention period
        self.db_manager.query(
            f"""
            DELETE FROM steam_raw
            WHERE CAST(timestamp AS DATE) < CURRENT_DATE - INTERVAL '{retention_days}' DAY
        """
        )

        # Count rows after deletion
        count_after = self.db_manager.query("SELECT COUNT(*) as count FROM steam_raw").iloc[0][
            "count"
        ]

        return int(count_before - count_after)

    def cleanup_old_parquet_files(self, raw_data_path: Path, retention_days: int = 7) -> int:
        """Delete Parquet files older than retention period.

        Removes old Parquet files from the raw data directory to save disk space.
        Only files older than the retention period are deleted.

        Args:
            raw_data_path: Path to raw data directory (e.g., data/raw/steam)
            retention_days: Number of days of files to keep (default: 7)

        Returns:
            Number of files deleted

        Example:
            >>> files_deleted = aggregator.cleanup_old_parquet_files(
            ...     Path("data/raw/steam"), retention_days=7
            ... )
            >>> print(f"Deleted {files_deleted} old Parquet files")
        """
        if not raw_data_path.exists():
            return 0

        cutoff_date = datetime.now() - timedelta(days=retention_days)
        files_deleted = 0

        # Find all .parquet files recursively
        for parquet_file in raw_data_path.rglob("*.parquet"):
            # Get file modification time
            file_mtime = datetime.fromtimestamp(parquet_file.stat().st_mtime)

            # Delete if older than cutoff
            if file_mtime < cutoff_date:
                parquet_file.unlink()
                files_deleted += 1

                # Clean up empty parent directories
                parent = parquet_file.parent
                while parent != raw_data_path and not list(parent.iterdir()):
                    parent.rmdir()
                    parent = parent.parent

        return files_deleted

    def run_full_aggregation(self, output_dir: Path) -> None:
        """Run full cascading aggregation pipeline and export all KPIs.

        Implements incremental aggregation strategy:
        1. Update today's daily KPIs from raw data
        2. Update current week's weekly KPIs from daily data
        3. Update current month's monthly KPIs from weekly data
        4. Clean up old raw data (keep only 7 days)
        5. Export aggregated data to JSON

        Args:
            output_dir: Directory to write JSON exports
        """
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Cascading aggregation: raw → daily → weekly → monthly
        self.create_daily_kpis()
        self.create_weekly_kpis()
        self.create_monthly_kpis()

        # Clean up old raw data (retention: 7 days)
        rows_deleted = self.cleanup_old_raw_data(retention_days=7)
        if rows_deleted > 0:
            print(f"🧹 Cleaned up {rows_deleted:,} old raw records (>7 days)")

        # Clean up old Parquet files (retention: 7 days)
        raw_steam_path = Path("data/raw/steam")
        files_deleted = self.cleanup_old_parquet_files(raw_steam_path, retention_days=7)
        if files_deleted > 0:
            print(f"🧹 Deleted {files_deleted:,} old Parquet files (>7 days)")

        # Export all KPIs
        self.export_all_daily_kpis(output_path=output_dir / "daily_kpis.json")
        self.export_latest_kpis(output_path=output_dir / "latest_kpis.json", days=7)
        self.export_game_rankings(output_path=output_dir / "game_rankings.json")
