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
        """Test creation of daily KPIs aggregation table with PRIMARY KEY."""
        # Mock the query method to return sample data
        mock_db_manager.query.return_value = sample_steam_data

        aggregator = KPIAggregator(db_path=Path("test.db"))

        # Test the SQL generation
        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.create_daily_kpis()

        # Verify query was called 2 times (CREATE IF NOT EXISTS + INSERT OR REPLACE)
        assert mock_db_manager.query.call_count == 2

        # Verify first call creates table if not exists
        first_call = mock_db_manager.query.call_args_list[0][0][0]
        assert "CREATE TABLE IF NOT EXISTS daily_kpis" in first_call
        assert "PRIMARY KEY (date, app_id)" in first_call

        # Verify second call inserts only today's data (incremental)
        second_call = mock_db_manager.query.call_args_list[1][0][0]
        assert "INSERT OR REPLACE INTO daily_kpis" in second_call
        assert "WHERE CAST(timestamp AS DATE) = CURRENT_DATE" in second_call
        assert "GROUP BY CAST(timestamp AS DATE), game_name, app_id" in second_call

    def test_export_latest_kpis(self, mock_db_manager, tmp_path):
        """Test export of latest 7 days KPIs."""
        output_path = tmp_path / "latest_kpis.json"

        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.export_latest_kpis(output_path=output_path, days=7)

        # Verify export_to_json was called
        mock_db_manager.export_to_json.assert_called_once()

        # Check the query parameter
        call_kwargs = mock_db_manager.export_to_json.call_args[1]
        assert "query" in call_kwargs
        assert "INTERVAL '7' DAY" in call_kwargs["query"]
        assert call_kwargs["output_path"] == output_path

    def test_export_game_rankings(self, mock_db_manager, tmp_path):
        """Test export of game rankings."""
        output_path = tmp_path / "game_rankings.json"

        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.export_game_rankings(output_path=output_path)

        # Verify export_to_json was called
        mock_db_manager.export_to_json.assert_called_once()

        # Check the query parameter
        call_kwargs = mock_db_manager.export_to_json.call_args[1]
        assert "query" in call_kwargs
        assert "AVG(peak_ccu) as avg_peak" in call_kwargs["query"]
        assert "GROUP BY game_name, app_id" in call_kwargs["query"]

    def test_export_all_daily_kpis(self, mock_db_manager, tmp_path):
        """Test export of all daily KPIs."""
        output_path = tmp_path / "daily_kpis.json"

        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.export_all_daily_kpis(output_path=output_path)

        # Verify export_to_json was called with table_name
        mock_db_manager.export_to_json.assert_called_once()
        call_kwargs = mock_db_manager.export_to_json.call_args[1]
        assert call_kwargs["table_name"] == "daily_kpis"

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
            mock_db_manager.export_to_json.call_count == 6
        )  # 6 exports: hourly, daily, latest, rankings, weekly, monthly (not including game_metadata)

        # Verify output directory was created
        assert output_dir.exists()

        # Verify game metadata file was created
        assert (output_dir / "game-metadata.json").exists()

    def test_date_filtering_uses_cast(self, mock_db_manager, tmp_path):
        """Test that date filtering properly casts date column to DATE type."""
        output_path = tmp_path / "latest_kpis.json"

        aggregator = KPIAggregator(db_path=Path("test.db"))

        with patch.object(aggregator, "db_manager", mock_db_manager):
            aggregator.export_latest_kpis(output_path=output_path, days=7)

        # Check that the query uses CAST or :: for type conversion
        call_kwargs = mock_db_manager.export_to_json.call_args[1]
        query = call_kwargs["query"]
        assert "CAST(date AS DATE)" in query or "date::DATE" in query

    def test_aggregator_context_manager(self, tmp_path):
        """Test that aggregator works as a context manager."""
        db_path = tmp_path / "test.db"

        with KPIAggregator(db_path=db_path) as aggregator:
            assert aggregator is not None
            assert hasattr(aggregator, "db_manager")
