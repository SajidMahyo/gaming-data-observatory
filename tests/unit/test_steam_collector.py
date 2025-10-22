"""Tests for Steam API collector."""

from unittest.mock import Mock, patch

import pytest
import requests

from python.collectors.steam import SteamCollector


class TestSteamCollector:
    """Test suite for SteamCollector class."""

    def test_get_player_count_success(self) -> None:
        """Test successful player count retrieval with realistic CS2 data."""
        collector = SteamCollector()

        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            # Real Steam API response format for CS2
            mock_response.json.return_value = {"response": {"player_count": 1102182, "result": 1}}
            mock_get.return_value = mock_response

            result = collector.get_player_count(730)  # CS2

            assert result == 1102182
            mock_get.assert_called_once()
            # Verify appid parameter was passed
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs["params"]["appid"] == 730

    def test_get_player_count_with_game_name(self) -> None:
        """Test player count returns game metadata with realistic Dota 2 data."""
        collector = SteamCollector()

        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            # Real Steam API response format for Dota 2
            mock_response.json.return_value = {"response": {"player_count": 620592, "result": 1}}
            mock_get.return_value = mock_response

            data = collector.get_game_data(570)  # Dota 2

            assert data["app_id"] == 570
            assert data["player_count"] == 620592
            assert data["game_name"] == "Dota 2"
            assert "timestamp" in data

    def test_get_player_count_http_error(self) -> None:
        """Test handling of HTTP errors."""
        collector = SteamCollector()

        with patch("requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.RequestException("API Error")

            with pytest.raises(requests.exceptions.RequestException):
                collector.get_player_count(730)

    def test_get_player_count_invalid_json(self) -> None:
        """Test handling of invalid JSON response."""
        collector = SteamCollector()

        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.side_effect = ValueError("Invalid JSON")
            mock_get.return_value = mock_response

            with pytest.raises(ValueError):
                collector.get_player_count(730)

    def test_get_player_count_missing_data(self) -> None:
        """Test handling of missing player_count (real API response for invalid app_id)."""
        collector = SteamCollector()

        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            # Real Steam API response for invalid/unknown app_id
            mock_response.json.return_value = {"response": {"result": 42}}
            mock_get.return_value = mock_response

            with pytest.raises(KeyError):
                collector.get_player_count(999999999)  # Invalid app_id

    def test_collect_top_games(self) -> None:
        """Test collecting data for multiple top games."""
        collector = SteamCollector()

        with patch.object(collector, "get_game_data") as mock_get_data:
            mock_get_data.return_value = {
                "app_id": 730,
                "game_name": "Counter-Strike 2",
                "player_count": 100000,
                "timestamp": "2025-01-22T14:00:00",
            }

            results = collector.collect_top_games(limit=3)

            assert len(results) == 3
            assert mock_get_data.call_count == 3
            assert all("app_id" in game for game in results)

    def test_get_top_games_list(self) -> None:
        """Test that TOP_GAMES constant exists and has correct format."""
        collector = SteamCollector()

        top_games = collector.get_top_games()

        assert len(top_games) >= 10
        assert 730 in top_games  # CS2
        assert 570 in top_games  # Dota 2
        assert isinstance(top_games[730], str)  # Game name

    def test_retry_on_failure(self) -> None:
        """Test that collector retries on temporary failures."""
        collector = SteamCollector(max_retries=3, retry_delay=0.01)

        with patch("requests.get") as mock_get:
            # Fail twice, then succeed with realistic CS2 data
            mock_get.side_effect = [
                requests.exceptions.RequestException("Timeout"),
                requests.exceptions.RequestException("Network error"),
                Mock(
                    status_code=200,
                    json=lambda: {"response": {"player_count": 1102182, "result": 1}},
                ),
            ]

            result = collector.get_player_count(730)

            assert result == 1102182
            assert mock_get.call_count == 3  # 2 failures + 1 success

    def test_retry_exhausted(self) -> None:
        """Test that collector raises exception after max retries."""
        collector = SteamCollector(max_retries=2, retry_delay=0.01)

        with patch("requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.RequestException("Persistent error")

            with pytest.raises(requests.exceptions.RequestException):
                collector.get_player_count(730)

            assert mock_get.call_count == 3  # Initial + 2 retries
