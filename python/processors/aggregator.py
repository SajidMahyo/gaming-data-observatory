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
        """Create or update Steam daily KPIs table from steam_kpis.

        Uses incremental update: only updates the current day's data.
        This is called every hour to update today's aggregated stats.

        Aggregates hourly data into daily metrics:
        - Average CCU (Concurrent Users)
        - Peak CCU
        - Minimum CCU
        - Average Metacritic score
        - Average price (cents)
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
                avg_metacritic_score DOUBLE,
                avg_price_cents DOUBLE,
                samples INTEGER,
                PRIMARY KEY (date, steam_app_id)
            )
        """
        )

        # Update ONLY TODAY'S data from steam_kpis (incremental update)
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
                AVG(s.metacritic_score) as avg_metacritic_score,
                AVG(s.price_cents) as avg_price_cents,
                COUNT(*) as samples
            FROM steam_kpis s
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
        """Create or update hourly KPIs table from Steam KPIs data.

        Uses incremental update: only updates the last 48 hours of data.
        Aggregates from steam_kpis with hourly granularity.

        Hourly metrics:
        - Average CCU for the hour
        - Peak CCU for the hour
        - Minimum CCU for the hour
        - Average Metacritic score
        - Average price (cents)
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
                steam_app_id INTEGER,
                avg_ccu DOUBLE,
                peak_ccu INTEGER,
                min_ccu INTEGER,
                avg_metacritic_score DOUBLE,
                avg_price_cents DOUBLE,
                samples INTEGER,
                PRIMARY KEY (hour, steam_app_id)
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
                steam_app_id,
                AVG(player_count) as avg_ccu,
                MAX(player_count) as peak_ccu,
                MIN(player_count) as min_ccu,
                AVG(metacritic_score) as avg_metacritic_score,
                AVG(price_cents) as avg_price_cents,
                COUNT(*) as samples
            FROM steam_kpis
            WHERE CAST(timestamp AS TIMESTAMP) >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
            GROUP BY DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)), game_name, steam_app_id
        """
        )

    def create_steam_weekly_kpis(self) -> None:
        """Create or update Steam weekly KPIs from steam_daily_kpis.

        Aggregates Steam CCU data into weekly metrics:
        - Average of daily peak CCU
        - Maximum peak CCU for the week
        - Average of avg_ccu
        - Total samples
        - Number of days tracked
        """
        if not self.db_manager:
            return

        # Create table if it doesn't exist
        self.db_manager.query(
            """
            CREATE TABLE IF NOT EXISTS steam_weekly_kpis (
                week_start DATE,
                igdb_id INTEGER,
                steam_app_id INTEGER,
                game_name VARCHAR,
                avg_peak_ccu DOUBLE,
                max_peak_ccu INTEGER,
                avg_ccu DOUBLE,
                total_samples INTEGER,
                days_in_week INTEGER,
                PRIMARY KEY (week_start, steam_app_id)
            )
        """
        )

        # Update only CURRENT WEEK's data (incremental update)
        self.db_manager.query(
            """
            INSERT OR REPLACE INTO steam_weekly_kpis
            SELECT
                CAST(CAST(DATE_TRUNC('week', CAST(date AS TIMESTAMP)) AS DATE) AS DATE) as week_start,
                igdb_id,
                steam_app_id,
                game_name,
                AVG(peak_ccu) as avg_peak_ccu,
                MAX(peak_ccu) as max_peak_ccu,
                AVG(avg_ccu) as avg_ccu,
                SUM(samples) as total_samples,
                COUNT(DISTINCT date) as days_in_week
            FROM steam_daily_kpis
            WHERE CAST(CAST(DATE_TRUNC('week', CAST(date AS TIMESTAMP)) AS DATE) AS DATE) = CAST(CAST(DATE_TRUNC('week', CURRENT_TIMESTAMP) AS DATE) AS DATE)
            GROUP BY week_start, igdb_id, steam_app_id, game_name
        """
        )

    def create_twitch_weekly_kpis(self) -> None:
        """Create or update Twitch weekly KPIs from twitch_daily_kpis.

        Aggregates Twitch viewership data into weekly metrics:
        - Average of daily peak viewers
        - Maximum peak viewers for the week
        - Average viewers and channels
        - Total samples
        - Number of days tracked
        """
        if not self.db_manager:
            return

        self.db_manager.query(
            """
            CREATE TABLE IF NOT EXISTS twitch_weekly_kpis (
                week_start DATE,
                igdb_id INTEGER,
                twitch_game_id VARCHAR,
                game_name VARCHAR,
                avg_peak_viewers DOUBLE,
                max_peak_viewers INTEGER,
                avg_viewers DOUBLE,
                avg_channels DOUBLE,
                total_samples INTEGER,
                days_in_week INTEGER,
                PRIMARY KEY (week_start, twitch_game_id)
            )
        """
        )

        self.db_manager.query(
            """
            INSERT OR REPLACE INTO twitch_weekly_kpis
            SELECT
                CAST(DATE_TRUNC('week', CAST(date AS TIMESTAMP)) AS DATE) as week_start,
                igdb_id,
                twitch_game_id,
                game_name,
                AVG(peak_viewers) as avg_peak_viewers,
                MAX(peak_viewers) as max_peak_viewers,
                AVG(avg_viewers) as avg_viewers,
                AVG(avg_channels) as avg_channels,
                SUM(samples) as total_samples,
                COUNT(DISTINCT date) as days_in_week
            FROM twitch_daily_kpis
            WHERE CAST(DATE_TRUNC('week', CAST(date AS TIMESTAMP)) AS DATE) = CAST(DATE_TRUNC('week', CURRENT_TIMESTAMP) AS DATE)
            GROUP BY week_start, igdb_id, twitch_game_id, game_name
        """
        )

    def create_igdb_ratings_weekly(self) -> None:
        """Create or update IGDB weekly ratings from igdb_ratings_snapshot.

        Aggregates IGDB ratings into weekly snapshots (latest rating of the week).
        """
        if not self.db_manager:
            return

        self.db_manager.query(
            """
            CREATE TABLE IF NOT EXISTS igdb_ratings_weekly (
                week_start DATE,
                igdb_id INTEGER,
                game_name VARCHAR,
                rating DOUBLE,
                aggregated_rating DOUBLE,
                total_rating_count INTEGER,
                PRIMARY KEY (week_start, igdb_id)
            )
        """
        )

        # Get latest rating of the week (ratings change slowly)
        self.db_manager.query(
            """
            INSERT OR REPLACE INTO igdb_ratings_weekly
            SELECT
                DATE_TRUNC('week', CAST(i.date AS TIMESTAMP)) as week_start,
                i.igdb_id,
                i.game_name,
                i.rating,
                i.aggregated_rating,
                i.total_rating_count
            FROM (
                SELECT
                    date, igdb_id, game_name, rating, aggregated_rating, total_rating_count,
                    ROW_NUMBER() OVER (
                        PARTITION BY igdb_id, CAST(DATE_TRUNC('week', CAST(date AS TIMESTAMP)) AS DATE)
                        ORDER BY date DESC
                    ) as rn
                FROM igdb_ratings_snapshot
                WHERE CAST(DATE_TRUNC('week', CAST(date AS TIMESTAMP)) AS DATE) = CAST(DATE_TRUNC('week', CURRENT_TIMESTAMP) AS DATE)
            ) i
            WHERE i.rn = 1
        """
        )

    def create_weekly_kpis(self) -> None:
        """Create all weekly KPIs from all sources (Steam, Twitch, IGDB).

        Deprecated: Use create_steam_weekly_kpis(), create_twitch_weekly_kpis(),
        and create_igdb_ratings_weekly() instead.
        """
        # Call all source-specific methods
        self.create_steam_weekly_kpis()
        self.create_twitch_weekly_kpis()
        self.create_igdb_ratings_weekly()

    def create_steam_monthly_kpis(self) -> None:
        """Create or update Steam monthly KPIs from steam_weekly_kpis.

        Aggregates Steam weekly data into monthly metrics:
        - Average of weekly peak CCU
        - Maximum peak CCU for the month
        - Average CCU
        - Total samples
        - Number of weeks tracked
        """
        if not self.db_manager:
            return

        self.db_manager.query(
            """
            CREATE TABLE IF NOT EXISTS steam_monthly_kpis (
                month_start DATE,
                igdb_id INTEGER,
                steam_app_id INTEGER,
                game_name VARCHAR,
                avg_peak_ccu DOUBLE,
                max_peak_ccu INTEGER,
                avg_ccu DOUBLE,
                total_samples INTEGER,
                weeks_in_month INTEGER,
                PRIMARY KEY (month_start, steam_app_id)
            )
        """
        )

        self.db_manager.query(
            """
            INSERT OR REPLACE INTO steam_monthly_kpis
            SELECT
                CAST(DATE_TRUNC('month', CAST(week_start AS TIMESTAMP)) AS DATE) as month_start,
                igdb_id,
                steam_app_id,
                game_name,
                AVG(avg_peak_ccu) as avg_peak_ccu,
                MAX(max_peak_ccu) as max_peak_ccu,
                AVG(avg_ccu) as avg_ccu,
                SUM(total_samples) as total_samples,
                COUNT(DISTINCT week_start) as weeks_in_month
            FROM steam_weekly_kpis
            WHERE CAST(DATE_TRUNC('month', CAST(week_start AS TIMESTAMP)) AS DATE) = CAST(DATE_TRUNC('month', CURRENT_TIMESTAMP) AS DATE)
            GROUP BY month_start, igdb_id, steam_app_id, game_name
        """
        )

    def create_twitch_monthly_kpis(self) -> None:
        """Create or update Twitch monthly KPIs from twitch_weekly_kpis.

        Aggregates Twitch weekly data into monthly metrics:
        - Average of weekly peak viewers
        - Maximum peak viewers for the month
        - Average viewers and channels
        - Total samples
        - Number of weeks tracked
        """
        if not self.db_manager:
            return

        self.db_manager.query(
            """
            CREATE TABLE IF NOT EXISTS twitch_monthly_kpis (
                month_start DATE,
                igdb_id INTEGER,
                twitch_game_id VARCHAR,
                game_name VARCHAR,
                avg_peak_viewers DOUBLE,
                max_peak_viewers INTEGER,
                avg_viewers DOUBLE,
                avg_channels DOUBLE,
                total_samples INTEGER,
                weeks_in_month INTEGER,
                PRIMARY KEY (month_start, twitch_game_id)
            )
        """
        )

        self.db_manager.query(
            """
            INSERT OR REPLACE INTO twitch_monthly_kpis
            SELECT
                CAST(DATE_TRUNC('month', CAST(week_start AS TIMESTAMP)) AS DATE) as month_start,
                igdb_id,
                twitch_game_id,
                game_name,
                AVG(avg_peak_viewers) as avg_peak_viewers,
                MAX(max_peak_viewers) as max_peak_viewers,
                AVG(avg_viewers) as avg_viewers,
                AVG(avg_channels) as avg_channels,
                SUM(total_samples) as total_samples,
                COUNT(DISTINCT week_start) as weeks_in_month
            FROM twitch_weekly_kpis
            WHERE CAST(DATE_TRUNC('month', CAST(week_start AS TIMESTAMP)) AS DATE) = CAST(DATE_TRUNC('month', CURRENT_TIMESTAMP) AS DATE)
            GROUP BY month_start, igdb_id, twitch_game_id, game_name
        """
        )

    def create_igdb_ratings_monthly(self) -> None:
        """Create or update IGDB monthly ratings from igdb_ratings_weekly.

        Aggregates IGDB weekly ratings into monthly snapshots (latest rating of the month).
        """
        if not self.db_manager:
            return

        self.db_manager.query(
            """
            CREATE TABLE IF NOT EXISTS igdb_ratings_monthly (
                month_start DATE,
                igdb_id INTEGER,
                game_name VARCHAR,
                rating DOUBLE,
                aggregated_rating DOUBLE,
                total_rating_count INTEGER,
                PRIMARY KEY (month_start, igdb_id)
            )
        """
        )

        # Get latest rating of the month
        self.db_manager.query(
            """
            INSERT OR REPLACE INTO igdb_ratings_monthly
            SELECT
                DATE_TRUNC('month', CAST(i.week_start AS TIMESTAMP)) as month_start,
                i.igdb_id,
                i.game_name,
                i.rating,
                i.aggregated_rating,
                i.total_rating_count
            FROM (
                SELECT
                    week_start, igdb_id, game_name, rating, aggregated_rating, total_rating_count,
                    ROW_NUMBER() OVER (
                        PARTITION BY igdb_id, CAST(DATE_TRUNC('month', CAST(week_start AS TIMESTAMP)) AS DATE)
                        ORDER BY week_start DESC
                    ) as rn
                FROM igdb_ratings_weekly
                WHERE CAST(DATE_TRUNC('month', CAST(week_start AS TIMESTAMP)) AS DATE) = CAST(DATE_TRUNC('month', CURRENT_TIMESTAMP) AS DATE)
            ) i
            WHERE i.rn = 1
        """
        )

    def create_monthly_kpis(self) -> None:
        """Create all monthly KPIs from all sources (Steam, Twitch, IGDB).

        Deprecated: Use create_steam_monthly_kpis(), create_twitch_monthly_kpis(),
        and create_igdb_ratings_monthly() instead.
        """
        # Call all source-specific methods
        self.create_steam_monthly_kpis()
        self.create_twitch_monthly_kpis()
        self.create_igdb_ratings_monthly()

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
            SELECT
                i.date,
                i.igdb_id,
                i.game_name,
                i.rating,
                i.aggregated_rating,
                i.total_rating_count,
                m.steam_app_id
            FROM igdb_ratings_snapshot i
            LEFT JOIN game_metadata m ON i.igdb_id = m.igdb_id
            WHERE i.date >= CURRENT_DATE - INTERVAL '{days}' DAY
            ORDER BY i.date DESC, i.aggregated_rating DESC
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
                AVG(avg_metacritic_score) as avg_metacritic_score,
                MAX(avg_metacritic_score) as latest_metacritic_score,
                AVG(avg_price_cents) as avg_price_cents,
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

    def export_steam_weekly_kpis(self, output_path: Path, weeks: int = 12) -> None:
        """Export Steam weekly KPIs to JSON.

        Args:
            output_path: Path to output JSON file
            weeks: Number of weeks to include (default: 12)
        """
        sql = f"""
            SELECT * FROM steam_weekly_kpis
            WHERE week_start >= CURRENT_DATE - INTERVAL '{weeks}' WEEK
            ORDER BY week_start DESC, max_peak_ccu DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_twitch_weekly_kpis(self, output_path: Path, weeks: int = 12) -> None:
        """Export Twitch weekly KPIs to JSON.

        Args:
            output_path: Path to output JSON file
            weeks: Number of weeks to include (default: 12)
        """
        sql = f"""
            SELECT * FROM twitch_weekly_kpis
            WHERE week_start >= CURRENT_DATE - INTERVAL '{weeks}' WEEK
            ORDER BY week_start DESC, max_peak_viewers DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_igdb_ratings_weekly(self, output_path: Path, weeks: int = 12) -> None:
        """Export IGDB weekly ratings to JSON.

        Args:
            output_path: Path to output JSON file
            weeks: Number of weeks to include (default: 12)
        """
        sql = f"""
            SELECT * FROM igdb_ratings_weekly
            WHERE week_start >= CURRENT_DATE - INTERVAL '{weeks}' WEEK
            ORDER BY week_start DESC, aggregated_rating DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_steam_monthly_kpis(self, output_path: Path, months: int = 12) -> None:
        """Export Steam monthly KPIs to JSON.

        Args:
            output_path: Path to output JSON file
            months: Number of months to include (default: 12)
        """
        sql = f"""
            SELECT * FROM steam_monthly_kpis
            WHERE month_start >= CURRENT_DATE - INTERVAL '{months}' MONTH
            ORDER BY month_start DESC, max_peak_ccu DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_twitch_monthly_kpis(self, output_path: Path, months: int = 12) -> None:
        """Export Twitch monthly KPIs to JSON.

        Args:
            output_path: Path to output JSON file
            months: Number of months to include (default: 12)
        """
        sql = f"""
            SELECT * FROM twitch_monthly_kpis
            WHERE month_start >= CURRENT_DATE - INTERVAL '{months}' MONTH
            ORDER BY month_start DESC, max_peak_viewers DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_igdb_ratings_monthly(self, output_path: Path, months: int = 12) -> None:
        """Export IGDB monthly ratings to JSON.

        Args:
            output_path: Path to output JSON file
            months: Number of months to include (default: 12)
        """
        sql = f"""
            SELECT * FROM igdb_ratings_monthly
            WHERE month_start >= CURRENT_DATE - INTERVAL '{months}' MONTH
            ORDER BY month_start DESC, aggregated_rating DESC
        """
        if self.db_manager:
            self.db_manager.export_to_json(query=sql, output_path=output_path)

    def export_game_metadata(self, output_path: Path) -> None:
        """Export game metadata with parsed JSON fields.

        Args:
            output_path: Path to output JSON file
        """
        import json
        import numpy as np

        if not self.db_manager:
            return

        # Query all metadata (static only - KPIs/ratings in separate tables)
        result = self.db_manager.query(
            """
            SELECT
                igdb_id, game_name, slug,
                steam_app_id, twitch_game_id, youtube_channel_id, epic_id, gog_id,
                igdb_summary, first_release_date, cover_url,
                steam_description, steam_required_age,
                genres, themes, platforms, game_modes, developers, publishers,
                websites,
                discovery_source, discovery_date, last_updated,
                is_active, track_steam, track_twitch, track_reddit
            FROM game_metadata
            ORDER BY game_name
        """
        )

        # Replace NaN/inf with None before converting to dict
        result = result.replace([np.nan, np.inf, -np.inf], None)

        games = result.to_dict(orient="records")

        # Parse JSON strings back to objects
        for game in games:
            # Convert timestamps to ISO format strings
            for ts_field in ["first_release_date", "discovery_date", "last_updated"]:
                if game.get(ts_field) is not None:
                    game[ts_field] = str(game[ts_field])

            # Parse JSON fields
            for field in [
                "genres",
                "themes",
                "platforms",
                "game_modes",
                "developers",
                "publishers",
                "websites",
            ]:
                if game.get(field):
                    try:
                        game[field] = json.loads(game[field])
                    except (json.JSONDecodeError, TypeError):
                        pass  # Keep as-is if not valid JSON

        # Write to file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(games, f, indent=2)

    def cleanup_old_raw_data(self, retention_days: int = 7) -> int:
        """Delete raw Steam KPIs data older than retention period.

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
        count_before = self.db_manager.query("SELECT COUNT(*) as count FROM steam_kpis").iloc[0][
            "count"
        ]

        # Delete data older than retention period
        self.db_manager.query(
            f"""
            DELETE FROM steam_kpis
            WHERE CAST(timestamp AS DATE) < CURRENT_DATE - INTERVAL '{retention_days}' DAY
        """
        )

        # Count rows after deletion
        count_after = self.db_manager.query("SELECT COUNT(*) as count FROM steam_kpis").iloc[0][
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

        # Export weekly KPIs (separate by source)
        self.export_steam_weekly_kpis(output_path=output_dir / "steam_weekly_kpis.json", weeks=12)
        self.export_twitch_weekly_kpis(output_path=output_dir / "twitch_weekly_kpis.json", weeks=12)
        self.export_igdb_ratings_weekly(
            output_path=output_dir / "igdb_ratings_weekly.json", weeks=12
        )

        # Export monthly KPIs (separate by source)
        self.export_steam_monthly_kpis(output_path=output_dir / "steam_monthly_kpis.json", months=12)
        self.export_twitch_monthly_kpis(
            output_path=output_dir / "twitch_monthly_kpis.json", months=12
        )
        self.export_igdb_ratings_monthly(
            output_path=output_dir / "igdb_ratings_monthly.json", months=12
        )

        # Export hourly KPIs
        self.export_hourly_kpis(output_path=output_dir / "hourly_kpis.json", hours=48)
