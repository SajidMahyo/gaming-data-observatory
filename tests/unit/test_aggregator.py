"""Tests for KPI aggregation module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from python.processors.aggregator import KPIAggregator


@pytest.fixture
def sample_steam_data():
    """Sample Steam raw data for testing."""
    return pd.DataFrame(
        {
            "timestamp": [
                "2025-10-22T10:00:00",
                "2025-10-22T11:00:00",
                "2025-10-22T12:00:00",
                "2025-10-21T10:00:00",
                "2025-10-21T11:00:00",
            ],
            "date": [
                "2025-10-22",
                "2025-10-22",
                "2025-10-22",
                "2025-10-21",
                "2025-10-21",
            ],
            "game_name": [
                "Counter-Strike 2",
                "Counter-Strike 2",
                "Counter-Strike 2",
                "Counter-Strike 2",
                "Counter-Strike 2",
            ],
            "app_id": [730, 730, 730, 730, 730],
            "player_count": [1000000, 1100000, 1050000, 950000, 980000],
        }
    )


@pytest.fixture
def mock_db_manager():
    """Mock DuckDB manager."""
    mock = MagicMock()
    return mock


class TestKPIAggregator:
    """Test KPI aggregation functionality."""

    def test_init_with_db_path(self, tmp_path):
        """Test aggregator initialization with database path."""
        db_path = tmp_path / "test.db"
        aggregator = KPIAggregator(db_path=db_path)
        assert aggregator.db_path == db_path

    def test_create_daily_kpis_table(self, mock_db_manager, sample_steam_data):
        """Test creation of daily KPIs aggregation tables (Steam, Twitch, IGDB)."""
        # Mock the query method to return sample data
        mock_db_manager.query.return_value = sample_steam_data

        aggregator = KPIAggregator(db_path=Path("test.db"))

        # Test the SQL generation - create_daily_kpis() calls all 3 source methods
        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.create_daily_kpis()

        # Verify query was called 6 times (3 sources × 2 queries each: CREATE + INSERT)
        assert mock_db_manager.query.call_count == 6

        # Verify Steam table creation
        steam_create_call = mock_db_manager.query.call_args_list[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS steam_daily_kpis" in steam_create_call
        assert "PRIMARY KEY (date, steam_app_id)" in steam_create_call

        # Verify Steam data insert
        steam_insert_call = mock_db_manager.query.call_args_list[1][0][0]
        assert "INSERT OR REPLACE INTO steam_daily_kpis" in steam_insert_call
        assert "FROM steam_raw s" in steam_insert_call
        assert "LEFT JOIN game_metadata m ON s.steam_app_id = m.steam_app_id" in steam_insert_call

        # Verify Twitch table creation
        twitch_create_call = mock_db_manager.query.call_args_list[2][0][0]
        assert "CREATE TABLE IF NOT EXISTS twitch_daily_kpis" in twitch_create_call

        # Verify IGDB table creation
        igdb_create_call = mock_db_manager.query.call_args_list[4][0][0]
        assert "CREATE TABLE IF NOT EXISTS igdb_ratings_snapshot" in igdb_create_call

    def test_export_latest_kpis(self, mock_db_manager, tmp_path):
        """Test export of latest Steam daily KPIs."""
        output_path = tmp_path / "steam_daily_kpis.json"

        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.export_steam_daily_kpis(output_path=output_path, days=30)

        # Verify export_to_json was called
        mock_db_manager.export_to_json.assert_called_once()

        # Check the query parameter
        call_kwargs = mock_db_manager.export_to_json.call_args[1]
        assert "query" in call_kwargs
        assert "FROM steam_daily_kpis" in call_kwargs["query"]
        assert "INTERVAL '30' DAY" in call_kwargs["query"]
        assert call_kwargs["output_path"] == output_path

    def test_export_game_rankings(self, mock_db_manager, tmp_path):
        """Test export of Steam game rankings."""
        output_path = tmp_path / "steam_rankings.json"

        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.export_steam_rankings(output_path=output_path)

        # Verify export_to_json was called
        mock_db_manager.export_to_json.assert_called_once()

        # Check the query parameter
        call_kwargs = mock_db_manager.export_to_json.call_args[1]
        assert "query" in call_kwargs
        assert "AVG(peak_ccu) as avg_peak_ccu" in call_kwargs["query"]
        assert "FROM steam_daily_kpis" in call_kwargs["query"]
        assert "GROUP BY game_name, steam_app_id, igdb_id" in call_kwargs["query"]

    def test_export_unified_daily_kpis(self, mock_db_manager, tmp_path):
        """Test export of unified daily KPIs (Steam + Twitch + IGDB)."""
        output_path = tmp_path / "unified_daily_kpis.json"

        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.export_unified_daily_kpis(output_path=output_path, days=30)

        # Verify export_to_json was called with query
        mock_db_manager.export_to_json.assert_called_once()
        call_kwargs = mock_db_manager.export_to_json.call_args[1]
        assert "query" in call_kwargs
        assert "FROM steam_daily_kpis s" in call_kwargs["query"]
        assert "FULL OUTER JOIN twitch_daily_kpis t" in call_kwargs["query"]
        assert "FULL OUTER JOIN igdb_ratings_snapshot i" in call_kwargs["query"]

    def test_run_full_aggregation(self, mock_db_manager, tmp_path):
        """Test full aggregation pipeline."""
        output_dir = tmp_path / "exports"

        # Mock different return values based on query
        def query_side_effect(sql):
            if "COUNT(*) as count FROM steam_raw" in sql:
                # Return count for cleanup
                return pd.DataFrame([{"count": 100}])
            elif "COUNT(*) as count FROM hourly_kpis" in sql:
                # Return count for hourly KPI cleanup
                return pd.DataFrame([{"count": 50}])
            elif "FROM game_metadata" in sql:
                # Return metadata for export_game_metadata
                return pd.DataFrame(
                    [
                        {
                            "app_id": 730,
                            "name": "Counter-Strike 2",
                            "type": "game",
                            "description": "Test game",
                            "developers": '["Valve"]',
                            "publishers": '["Valve"]',
                            "is_free": True,
                            "required_age": 0,
                            "release_date": "2012-08-21",
                            "platforms": '["windows"]',
                            "metacritic_score": None,
                            "metacritic_url": None,
                            "categories": '["Multi-player"]',
                            "genres": '["Action"]',
                            "price_info": '{"price": 0, "currency": "USD"}',
                            "tags": '{"FPS": 1000}',
                        }
                    ]
                )
            else:
                # Return empty DataFrame for other queries
                return pd.DataFrame()

        mock_db_manager.query.side_effect = query_side_effect

        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.run_full_aggregation(output_dir=output_dir)

        # Verify all methods were called
        assert mock_db_manager.query.called  # create_daily_kpis and others
        assert (
            mock_db_manager.export_to_json.call_count == 9
        )  # 9 exports: steam_rankings, twitch_rankings, unified_rankings,
        #  steam_daily_kpis, twitch_daily_kpis, igdb_ratings_snapshot,
        #  unified_daily_kpis, hourly_kpis, monthly_kpis_limited
        #  (game_metadata uses json.dump directly, not export_to_json)

        # Verify output directory was created
        assert output_dir.exists()

        # Verify game metadata file was created
        assert (output_dir / "game-metadata.json").exists()

    def test_date_filtering_uses_cast(self, mock_db_manager, tmp_path):
        """Test that date filtering properly filters by date column."""
        output_path = tmp_path / "steam_daily_kpis.json"

        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.export_steam_daily_kpis(output_path=output_path, days=30)

        # Check that the query filters by date
        call_kwargs = mock_db_manager.export_to_json.call_args[1]
        query = call_kwargs["query"]
        assert "WHERE date >=" in query
        assert "INTERVAL '30' DAY" in query

    def test_aggregator_context_manager(self, tmp_path):
        """Test that aggregator works as a context manager."""
        db_path = tmp_path / "test.db"

        with KPIAggregator(db_path=db_path) as aggregator:
            assert aggregator is not None
            assert hasattr(aggregator, "db_manager")

    def test_export_weekly_kpis(self, mock_db_manager, tmp_path):
        """Test export of weekly KPIs."""
        output_path = tmp_path / "weekly_kpis.json"
        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.export_weekly_kpis(output_path=output_path)

        # Verify export_to_json was called with correct parameters
        mock_db_manager.export_to_json.assert_called_once_with(
            table_name="weekly_kpis", output_path=output_path
        )

    def test_export_monthly_kpis(self, mock_db_manager, tmp_path):
        """Test export of all monthly KPIs."""
        output_path = tmp_path / "monthly_kpis.json"
        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.export_monthly_kpis(output_path=output_path)

        # Verify export_to_json was called with correct parameters
        mock_db_manager.export_to_json.assert_called_once_with(
            table_name="monthly_kpis", output_path=output_path
        )

    def test_export_monthly_kpis_limited(self, mock_db_manager, tmp_path):
        """Test export of limited monthly KPIs (last N months)."""
        output_path = tmp_path / "monthly_kpis.json"
        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.export_monthly_kpis_limited(output_path=output_path, months=12)

        # Verify export_to_json was called with a query
        assert mock_db_manager.export_to_json.called
        call_kwargs = mock_db_manager.export_to_json.call_args[1]
        assert "query" in call_kwargs
        assert "INTERVAL '12' MONTH" in call_kwargs["query"]

    def test_export_hourly_kpis(self, mock_db_manager, tmp_path):
        """Test export of hourly KPIs with time limit."""
        output_path = tmp_path / "hourly_kpis.json"
        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.export_hourly_kpis(output_path=output_path, hours=48)

        # Verify export_to_json was called with a query
        assert mock_db_manager.export_to_json.called
        call_kwargs = mock_db_manager.export_to_json.call_args[1]
        assert "query" in call_kwargs
        assert "INTERVAL '48' HOUR" in call_kwargs["query"]

    def test_export_methods_handle_none_db_manager(self, tmp_path):
        """Test that export methods handle None db_manager gracefully."""
        output_path = tmp_path / "test.json"
        aggregator = KPIAggregator(db_path=Path("test.db"))

        # Set db_manager to None
        aggregator.db_manager = None

        # These should not raise exceptions
        aggregator.export_weekly_kpis(output_path=output_path)
        aggregator.export_monthly_kpis(output_path=output_path)
        aggregator.export_monthly_kpis_limited(output_path=output_path, months=12)
        aggregator.export_hourly_kpis(output_path=output_path, hours=48)

    def test_cleanup_old_raw_data(self, mock_db_manager):
        """Test cleanup of old raw data."""
        # Mock query to return different counts (before: 100, after: 0)
        mock_db_manager.query.side_effect = [
            pd.DataFrame([{"count": 100}]),  # count before
            None,  # DELETE statement
            pd.DataFrame([{"count": 0}]),  # count after
        ]

        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            rows_deleted = aggregator.cleanup_old_raw_data(retention_days=7)

        # Verify query was called 3 times (count before, delete, count after)
        assert mock_db_manager.query.call_count == 3
        assert rows_deleted == 100

    def test_cleanup_old_hourly_kpis(self, mock_db_manager):
        """Test cleanup of old hourly KPIs."""
        # Mock query to return different counts (before: 50, after: 0)
        mock_db_manager.query.side_effect = [
            pd.DataFrame([{"count": 50}]),  # count before
            None,  # DELETE statement
            pd.DataFrame([{"count": 0}]),  # count after
        ]

        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            rows_deleted = aggregator.cleanup_old_hourly_kpis(retention_days=7)

        # Verify query was called 3 times
        assert mock_db_manager.query.call_count == 3
        assert rows_deleted == 50
