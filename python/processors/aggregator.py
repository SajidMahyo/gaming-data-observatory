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

    def create_steam_daily_kpis(self) -> None:
        """Create or update Steam daily KPIs table from steam_raw.

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

        # Create table if it doesn't exist (with PRIMARY KEY)
        self.db_manager.query(
            """
            CREATE TABLE IF NOT EXISTS steam_daily_kpis (
                date DATE,
                igdb_id INTEGER,
                steam_app_id INTEGER,
                game_name VARCHAR,
                avg_ccu DOUBLE,
                peak_ccu INTEGER,
                min_ccu INTEGER,
                samples INTEGER,
                PRIMARY KEY (date, steam_app_id)
            )
        """
        )

        # Update ONLY TODAY'S data from steam_raw (incremental update)
        # Join with game_metadata to get igdb_id
        self.db_manager.query(
            """
            INSERT OR REPLACE INTO steam_daily_kpis
            SELECT
                CAST(s.timestamp AS DATE) as date,
                m.igdb_id,
                s.steam_app_id,
                s.game_name,
                AVG(s.player_count) as avg_ccu,
                MAX(s.player_count) as peak_ccu,
                MIN(s.player_count) as min_ccu,
                COUNT(*) as samples
            FROM steam_raw s
            LEFT JOIN game_metadata m ON s.steam_app_id = m.steam_app_id
            WHERE CAST(s.timestamp AS DATE) = CURRENT_DATE
            GROUP BY CAST(s.timestamp AS DATE), m.igdb_id, s.steam_app_id, s.game_name
        """
        )

    def create_twitch_daily_kpis(self) -> None:
        """Create or update Twitch daily KPIs table from twitch_raw.

        Aggregates Twitch viewership data into daily metrics:
        - Average viewers
        - Peak viewers
        - Average channel count
        - Number of samples per day
        """
        if not self.db_manager:
            return

        # Create table if it doesn't exist
        self.db_manager.query(
            """
            CREATE TABLE IF NOT EXISTS twitch_daily_kpis (
                date DATE,
                igdb_id INTEGER,
                twitch_game_id VARCHAR,
                game_name VARCHAR,
                avg_viewers DOUBLE,
                peak_viewers INTEGER,
                min_viewers INTEGER,
                avg_channels DOUBLE,
                samples INTEGER,
                PRIMARY KEY (date, twitch_game_id)
            )
        """
        )

        # Update ONLY TODAY'S data from twitch_raw
        self.db_manager.query(
            """
            INSERT OR REPLACE INTO twitch_daily_kpis
            SELECT
                CAST(t.timestamp AS DATE) as date,
                m.igdb_id,
                t.twitch_game_id,
                t.game_name,
                AVG(t.viewer_count) as avg_viewers,
                MAX(t.viewer_count) as peak_viewers,
                MIN(t.viewer_count) as min_viewers,
                AVG(t.channel_count) as avg_channels,
                COUNT(*) as samples
            FROM twitch_raw t
            LEFT JOIN game_metadata m ON t.twitch_game_id = m.twitch_game_id
            WHERE CAST(t.timestamp AS DATE) = CURRENT_DATE
            GROUP BY CAST(t.timestamp AS DATE), m.igdb_id, t.twitch_game_id, t.game_name
        """
        )

    def create_igdb_ratings_snapshot(self) -> None:
        """Create or update IGDB ratings snapshot from igdb_ratings_raw.

        Takes the latest rating for each game per day (ratings change slowly).
        """
        if not self.db_manager:
            return

        # Create table if it doesn't exist
        self.db_manager.query(
            """
            CREATE TABLE IF NOT EXISTS igdb_ratings_snapshot (
                date DATE,
                igdb_id INTEGER,
                game_name VARCHAR,
                rating DOUBLE,
                aggregated_rating DOUBLE,
                total_rating_count INTEGER,
                PRIMARY KEY (date, igdb_id)
            )
        """
        )

        # Update with latest rating for TODAY
        # Use ROW_NUMBER to get the most recent rating of the day
        self.db_manager.query(
            """
            INSERT OR REPLACE INTO igdb_ratings_snapshot
            SELECT
                CAST(i.timestamp AS DATE) as date,
                i.igdb_id,
                m.game_name,
                i.rating,
                i.aggregated_rating,
                i.total_rating_count
            FROM (
                SELECT
                    timestamp, igdb_id, rating, aggregated_rating, total_rating_count,
                    ROW_NUMBER() OVER (PARTITION BY igdb_id, CAST(timestamp AS DATE) ORDER BY timestamp DESC) as rn
                FROM igdb_ratings_raw
                WHERE CAST(timestamp AS DATE) = CURRENT_DATE
            ) i
            LEFT JOIN game_metadata m ON i.igdb_id = m.igdb_id
            WHERE i.rn = 1
        """
        )

    def create_daily_kpis(self) -> None:
        """Create all daily KPIs from all sources (Steam, Twitch, IGDB).

        Deprecated: Use create_steam_daily_kpis(), create_twitch_daily_kpis(),
        and create_igdb_ratings_snapshot() instead.
        """
        # Call all source-specific methods
        self.create_steam_daily_kpis()
        self.create_twitch_daily_kpis()
        self.create_igdb_ratings_snapshot()

    def create_hourly_kpis(self) -> None:
        """Create or update hourly KPIs table from raw Steam data.

        Uses incremental update: only updates the last 48 hours of data.
        Aggregates from steam_raw with hourly granularity.

        Hourly metrics:
        - Average CCU for the hour
        - Peak CCU for the hour
        - Minimum CCU for the hour
        - Number of samples in the hour
        """
        if not self.db_manager:
            return

        # Create table if it doesn't exist
        self.db_manager.query(
            """
            CREATE TABLE IF NOT EXISTS hourly_kpis (
                hour TIMESTAMP,
                game_name VARCHAR,
                app_id INTEGER,
                avg_ccu DOUBLE,
                peak_ccu INTEGER,
                min_ccu INTEGER,
                samples INTEGER,
                PRIMARY KEY (hour, app_id)
            )
        """
        )

        # Update only last 48 hours (incremental update)
        self.db_manager.query(
            """
            INSERT OR REPLACE INTO hourly_kpis
            SELECT
                DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) as hour,
                game_name,
                app_id,
                AVG(player_count) as avg_ccu,
                MAX(player_count) as peak_ccu,
                MIN(player_count) as min_ccu,
                COUNT(*) as samples
            FROM steam_raw
            WHERE CAST(timestamp AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
            GROUP BY DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)), game_name, app_id
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
                DATE_TRUNC('week', CAST(date AS TIMESTAMP)) as week_start,
                game_name,
                app_id,
                AVG(peak_ccu) as avg_peak,
                MAX(peak_ccu) as max_peak,
                SUM(samples) as total_samples,
                COUNT(DISTINCT date) as days_in_week
            FROM daily_kpis
            WHERE DATE_TRUNC('week', CAST(date AS TIMESTAMP)) = DATE_TRUNC('week', CURRENT_TIMESTAMP)
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
                DATE_TRUNC('month', CAST(week_start AS TIMESTAMP)) as month_start,
                game_name,
                app_id,
                AVG(avg_peak) as avg_peak,
                MAX(max_peak) as max_peak,
                SUM(total_samples) as total_samples,
                COUNT(DISTINCT week_start) as weeks_in_month
            FROM weekly_kpis
            WHERE DATE_TRUNC('month', CAST(week_start AS TIMESTAMP)) = DATE_TRUNC('month', CURRENT_TIMESTAMP)
            GROUP BY month_start, game_name, app_id
        """
        )

    def export_steam_daily_kpis(self, output_path: Path, days: int = 30) -> None:
        """Export latest N days of Steam KPIs to JSON.

        Args:
            output_path: Path to output JSON file
            days: Number of days to include (default: 30)
        """
        sql = f"""
            SELECT * FROM steam_daily_kpis
            WHERE date >= CURRENT_DATE - INTERVAL '{days}' DAY
            ORDER BY date DESC, peak_ccu DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_twitch_daily_kpis(self, output_path: Path, days: int = 30) -> None:
        """Export latest N days of Twitch KPIs to JSON.

        Args:
            output_path: Path to output JSON file
            days: Number of days to include (default: 30)
        """
        sql = f"""
            SELECT * FROM twitch_daily_kpis
            WHERE date >= CURRENT_DATE - INTERVAL '{days}' DAY
            ORDER BY date DESC, peak_viewers DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_igdb_ratings_snapshot(self, output_path: Path, days: int = 30) -> None:
        """Export latest N days of IGDB ratings to JSON.

        Args:
            output_path: Path to output JSON file
            days: Number of days to include (default: 30)
        """
        sql = f"""
            SELECT * FROM igdb_ratings_snapshot
            WHERE date >= CURRENT_DATE - INTERVAL '{days}' DAY
            ORDER BY date DESC, aggregated_rating DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_unified_daily_kpis(self, output_path: Path, days: int = 30) -> None:
        """Export unified daily KPIs from all sources joined by igdb_id.

        Combines Steam, Twitch, and IGDB data for games that have an igdb_id.

        Args:
            output_path: Path to output JSON file
            days: Number of days to include (default: 30)
        """
        sql = f"""
            SELECT
                COALESCE(s.date, t.date, i.date) as date,
                COALESCE(s.igdb_id, t.igdb_id, i.igdb_id) as igdb_id,
                COALESCE(s.game_name, t.game_name, i.game_name) as game_name,
                s.steam_app_id,
                s.avg_ccu,
                s.peak_ccu,
                s.min_ccu as min_ccu_steam,
                s.samples as steam_samples,
                t.twitch_game_id,
                t.avg_viewers,
                t.peak_viewers,
                t.min_viewers,
                t.avg_channels,
                t.samples as twitch_samples,
                i.rating,
                i.aggregated_rating,
                i.total_rating_count
            FROM steam_daily_kpis s
            FULL OUTER JOIN twitch_daily_kpis t
                ON s.igdb_id = t.igdb_id AND s.date = t.date
            FULL OUTER JOIN igdb_ratings_snapshot i
                ON COALESCE(s.igdb_id, t.igdb_id) = i.igdb_id
                AND COALESCE(s.date, t.date) = i.date
            WHERE COALESCE(s.date, t.date, i.date) >= CURRENT_DATE - INTERVAL '{days}' DAY
            ORDER BY COALESCE(s.date, t.date, i.date) DESC,
                     COALESCE(s.peak_ccu, 0) + COALESCE(t.peak_viewers, 0) DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_hourly_kpis(self, output_path: Path, hours: int = 48) -> None:
        """Export latest N hours of KPIs to JSON.

        Args:
            output_path: Path to output JSON file
            hours: Number of hours to include (default: 48)
        """
        sql = f"""
            SELECT * FROM hourly_kpis
            WHERE hour >= CURRENT_TIMESTAMP - INTERVAL '{hours}' HOUR
            ORDER BY hour DESC, peak_ccu DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_steam_rankings(self, output_path: Path) -> None:
        """Export Steam game rankings based on average peak CCU.

        Args:
            output_path: Path to output JSON file
        """
        sql = """
            SELECT
                game_name,
                steam_app_id,
                igdb_id,
                AVG(peak_ccu) as avg_peak_ccu,
                MAX(peak_ccu) as all_time_peak_ccu,
                AVG(avg_ccu) as avg_ccu,
                COUNT(DISTINCT date) as days_tracked
            FROM steam_daily_kpis
            GROUP BY game_name, steam_app_id, igdb_id
            ORDER BY avg_peak_ccu DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_twitch_rankings(self, output_path: Path) -> None:
        """Export Twitch game rankings based on average peak viewers.

        Args:
            output_path: Path to output JSON file
        """
        sql = """
            SELECT
                game_name,
                twitch_game_id,
                igdb_id,
                AVG(peak_viewers) as avg_peak_viewers,
                MAX(peak_viewers) as all_time_peak_viewers,
                AVG(avg_viewers) as avg_viewers,
                AVG(avg_channels) as avg_channels,
                COUNT(DISTINCT date) as days_tracked
            FROM twitch_daily_kpis
            GROUP BY game_name, twitch_game_id, igdb_id
            ORDER BY avg_peak_viewers DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_unified_rankings(self, output_path: Path) -> None:
        """Export unified game rankings combining Steam and Twitch metrics.

        Args:
            output_path: Path to output JSON file
        """
        sql = """
            SELECT
                COALESCE(s.game_name, t.game_name) as game_name,
                COALESCE(s.igdb_id, t.igdb_id) as igdb_id,
                s.steam_app_id,
                t.twitch_game_id,
                s.avg_peak_ccu,
                s.all_time_peak_ccu,
                s.avg_ccu,
                s.days_tracked as steam_days_tracked,
                t.avg_peak_viewers,
                t.all_time_peak_viewers,
                t.avg_viewers,
                t.avg_channels,
                t.days_tracked as twitch_days_tracked
            FROM (
                SELECT
                    game_name, steam_app_id, igdb_id,
                    AVG(peak_ccu) as avg_peak_ccu,
                    MAX(peak_ccu) as all_time_peak_ccu,
                    AVG(avg_ccu) as avg_ccu,
                    COUNT(DISTINCT date) as days_tracked
                FROM steam_daily_kpis
                GROUP BY game_name, steam_app_id, igdb_id
            ) s
            FULL OUTER JOIN (
                SELECT
                    game_name, twitch_game_id, igdb_id,
                    AVG(peak_viewers) as avg_peak_viewers,
                    MAX(peak_viewers) as all_time_peak_viewers,
                    AVG(avg_viewers) as avg_viewers,
                    AVG(avg_channels) as avg_channels,
                    COUNT(DISTINCT date) as days_tracked
                FROM twitch_daily_kpis
                GROUP BY game_name, twitch_game_id, igdb_id
            ) t ON s.igdb_id = t.igdb_id
            ORDER BY COALESCE(s.avg_peak_ccu, 0) + COALESCE(t.avg_peak_viewers, 0) DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_weekly_kpis(self, output_path: Path) -> None:
        """Export all weekly KPIs to JSON.

        Args:
            output_path: Path to output JSON file
        """
        if self.db_manager:
            self.db_manager.export_to_json(table_name="weekly_kpis", output_path=output_path)

    def export_monthly_kpis(self, output_path: Path) -> None:
        """Export all monthly KPIs to JSON.

        Args:
            output_path: Path to output JSON file
        """
        if self.db_manager:
            self.db_manager.export_to_json(table_name="monthly_kpis", output_path=output_path)

    def export_monthly_kpis_limited(self, output_path: Path, months: int = 12) -> None:
        """Export limited monthly KPIs to JSON (last N months).

        Args:
            output_path: Path to output JSON file
            months: Number of months to include (default: 12)
        """
        sql = f"""
            SELECT * FROM monthly_kpis
            WHERE month_start >= CURRENT_DATE - INTERVAL '{months}' MONTH
            ORDER BY month_start DESC, avg_peak DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_game_metadata(self, output_path: Path) -> None:
        """Export game metadata with parsed JSON fields.

        Args:
            output_path: Path to output JSON file
        """
        import json

        if not self.db_manager:
            return

        # Query all metadata
        result = self.db_manager.query(
            """
            SELECT
                app_id, name, type, description, developers, publishers,
                is_free, required_age, release_date, platforms,
                metacritic_score, metacritic_url, categories, genres,
                price_info, tags
            FROM game_metadata
            ORDER BY name
        """
        )

        games = result.to_dict(orient="records")

        # Parse JSON strings back to objects
        for game in games:
            for field in [
                "developers",
                "publishers",
                "platforms",
                "categories",
                "genres",
                "price_info",
                "tags",
            ]:
                if game.get(field):
                    game[field] = json.loads(game[field])

        # Write to file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(games, f, indent=2)

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

    def cleanup_old_hourly_kpis(self, retention_days: int = 7) -> int:
        """Delete hourly KPIs older than retention period.

        Implements data retention policy: keep only recent hourly KPIs,
        while preserving all aggregated daily/weekly/monthly KPIs.

        Args:
            retention_days: Number of days of hourly KPIs to keep (default: 7)

        Returns:
            Number of rows deleted

        Example:
            >>> rows_deleted = aggregator.cleanup_old_hourly_kpis(retention_days=7)
            >>> print(f"Cleaned up {rows_deleted} old hourly KPI records")
        """
        if not self.db_manager:
            return 0

        # Count rows before deletion
        count_before = self.db_manager.query("SELECT COUNT(*) as count FROM hourly_kpis").iloc[0][
            "count"
        ]

        # Delete data older than retention period
        self.db_manager.query(
            f"""
            DELETE FROM hourly_kpis
            WHERE hour < CURRENT_TIMESTAMP - INTERVAL '{retention_days}' DAY
        """
        )

        # Count rows after deletion
        count_after = self.db_manager.query("SELECT COUNT(*) as count FROM hourly_kpis").iloc[0][
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
        1. Update today's daily KPIs from raw data (Steam, Twitch, IGDB)
        2. Update current week's weekly KPIs from daily data
        3. Update current month's monthly KPIs from weekly data
        4. Clean up old raw data (keep only 7 days)
        5. Export optimized JSON files for all sources

        Args:
            output_dir: Directory to write JSON exports
        """
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Cascading aggregation: raw → hourly → daily → weekly → monthly
        self.create_hourly_kpis()
        self.create_daily_kpis()  # Calls all 3 source-specific methods
        self.create_weekly_kpis()
        self.create_monthly_kpis()

        # Clean up old raw data (retention: 7 days)
        rows_deleted = self.cleanup_old_raw_data(retention_days=7)
        if rows_deleted > 0:
            print(f"🧹 Cleaned up {rows_deleted:,} old raw records (>7 days)")

        # Clean up old hourly KPIs (retention: 7 days)
        hourly_deleted = self.cleanup_old_hourly_kpis(retention_days=7)
        if hourly_deleted > 0:
            print(f"🧹 Cleaned up {hourly_deleted:,} old hourly KPI records (>7 days)")

        # Clean up old Parquet files (retention: 7 days)
        raw_steam_path = Path("data/raw/steam")
        files_deleted = self.cleanup_old_parquet_files(raw_steam_path, retention_days=7)
        if files_deleted > 0:
            print(f"🧹 Deleted {files_deleted:,} old Parquet files (>7 days)")

        # Export metadata
        self.export_game_metadata(output_path=output_dir / "game-metadata.json")

        # Export rankings (separate by source + unified)
        self.export_steam_rankings(output_path=output_dir / "steam_rankings.json")
        self.export_twitch_rankings(output_path=output_dir / "twitch_rankings.json")
        self.export_unified_rankings(output_path=output_dir / "unified_rankings.json")

        # Export daily KPIs (separate by source + unified)
        self.export_steam_daily_kpis(output_path=output_dir / "steam_daily_kpis.json", days=30)
        self.export_twitch_daily_kpis(output_path=output_dir / "twitch_daily_kpis.json", days=30)
        self.export_igdb_ratings_snapshot(
            output_path=output_dir / "igdb_ratings_snapshot.json", days=30
        )
        self.export_unified_daily_kpis(output_path=output_dir / "unified_daily_kpis.json", days=30)

        # Export hourly and monthly KPIs
        self.export_hourly_kpis(output_path=output_dir / "hourly_kpis.json", hours=48)
        self.export_monthly_kpis_limited(output_path=output_dir / "monthly_kpis.json", months=12)
