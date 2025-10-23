"""Tests for Steam Store API collector."""

from unittest.mock import MagicMock, patch

import pytest

from python.collectors.steam_store import SteamStoreCollector


@pytest.fixture
def sample_store_response():
    """Sample Steam Store API response for CS2."""
    return {
        "730": {
            "success": True,
            "data": {
                "type": "game",
                "name": "Counter-Strike 2",
                "steam_appid": 730,
                "required_age": 0,
                "is_free": True,
                "detailed_description": "For over two decades...",
                "about_the_game": "Counter-Strike 2 is...",
                "short_description": "Counter-Strike 2 is...",
                "developers": ["Valve"],
                "publishers": ["Valve"],
                "platforms": {"windows": True, "mac": True, "linux": True},
                "metacritic": {
                    "score": 81,
                    "url": "https://www.metacritic.com/game/pc/counter-strike-2",
                },
                "categories": [
                    {"id": 1, "description": "Multi-player"},
                    {"id": 36, "description": "Online Multi-Player"},
                ],
                "genres": [{"id": "1", "description": "Action"}],
                "release_date": {"coming_soon": False, "date": "27 Sep, 2023"},
                "price_overview": {
                    "currency": "USD",
                    "initial": 0,
                    "final": 0,
                    "discount_percent": 0,
                },
            },
        }
    }


@pytest.fixture
def sample_steamspy_response():
    """Sample SteamSpy API response with tags."""
    return {
        "appid": 730,
        "name": "Counter-Strike 2",
        "tags": {
            "FPS": 1000,
            "Shooter": 950,
            "Multiplayer": 900,
            "Competitive": 850,
            "Action": 800,
        },
    }


class TestSteamStoreCollector:
    """Test Steam Store metadata collection."""

    def test_init(self):
        """Test collector initialization."""
        collector = SteamStoreCollector()
        assert collector is not None
        assert hasattr(collector, "store_api_base")
        assert hasattr(collector, "steamspy_api_base")

    @patch("requests.Session.get")
    def test_get_game_details_success(self, mock_get, sample_store_response):
        """Test successful game details retrieval."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_store_response
        mock_get.return_value = mock_response

        collector = SteamStoreCollector()
        details = collector.get_game_details(730)

        assert details is not None
        assert details["app_id"] == 730
        assert details["name"] == "Counter-Strike 2"
        assert details["developers"] == ["Valve"]
        assert details["publishers"] == ["Valve"]
        assert details["is_free"] is True
        assert details["metacritic_score"] == 81

    @patch("requests.Session.get")
    def test_get_game_details_api_failure(self, mock_get):
        """Test handling of API failure."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        collector = SteamStoreCollector()
        details = collector.get_game_details(999999)

        assert details is None

    @patch("requests.Session.get")
    def test_get_game_details_invalid_response(self, mock_get):
        """Test handling of invalid JSON response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response

        collector = SteamStoreCollector()
        details = collector.get_game_details(730)

        assert details is None

    @patch("requests.Session.get")
    def test_get_game_tags_success(self, mock_get, sample_steamspy_response):
        """Test successful tags retrieval from SteamSpy."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_steamspy_response
        mock_get.return_value = mock_response

        collector = SteamStoreCollector()
        tags = collector.get_game_tags(730)

        assert tags is not None
        assert "FPS" in tags
        assert "Shooter" in tags
        assert "Multiplayer" in tags
        assert len(tags) >= 3

    @patch("requests.Session.get")
    def test_get_game_tags_failure(self, mock_get):
        """Test handling of SteamSpy API failure."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        collector = SteamStoreCollector()
        tags = collector.get_game_tags(730)

        assert tags == {}

    @patch("requests.Session.get")
    def test_collect_full_metadata(self, mock_get, sample_store_response, sample_steamspy_response):
        """Test collecting full metadata (details + tags)."""
        # Mock both API calls
        call_count = [0]

        def side_effect(*args, **kwargs):
            response = MagicMock()
            response.status_code = 200
            # First call is Steam Store, second is SteamSpy
            if call_count[0] == 0:
                response.json.return_value = sample_store_response
            else:
                response.json.return_value = sample_steamspy_response
            call_count[0] += 1
            return response

        mock_get.side_effect = side_effect

        collector = SteamStoreCollector()
        metadata = collector.collect_full_metadata(730)

        assert metadata is not None
        assert metadata["app_id"] == 730
        assert metadata["name"] == "Counter-Strike 2"
        assert "tags" in metadata
        assert "FPS" in metadata["tags"]
        assert "developers" in metadata
        assert "genres" in metadata

    @patch("requests.Session.get")
    @patch("time.sleep")  # Mock sleep to speed up test
    def test_collect_top_games_metadata(
        self, mock_sleep, mock_get, sample_store_response, sample_steamspy_response
    ):
        """Test collecting metadata for multiple games."""

        def side_effect(*args, **kwargs):
            response = MagicMock()
            response.status_code = 200

            # Check if this is a Steam Store or SteamSpy call based on URL
            url = args[0] if args else kwargs.get("url", "")
            params = kwargs.get("params", {})

            if "steampowered.com" in url:
                # Steam Store API call - return response based on app_id
                app_id = params.get("appids")
                if app_id == 730:
                    response.json.return_value = sample_store_response
                elif app_id == 570:
                    # Create response for Dota 2
                    response.json.return_value = {
                        "570": {
                            "success": True,
                            "data": {
                                "type": "game",
                                "name": "Dota 2",
                                "steam_appid": 570,
                                "required_age": 0,
                                "is_free": True,
                                "detailed_description": "Dota 2 is a multiplayer...",
                                "about_the_game": "Dota 2 is...",
                                "short_description": "Dota 2 is...",
                                "developers": ["Valve"],
                                "publishers": ["Valve"],
                                "platforms": {"windows": True, "mac": True, "linux": True},
                                "metacritic": {
                                    "score": 90,
                                    "url": "https://www.metacritic.com/game/pc/dota-2",
                                },
                                "categories": [{"id": 1, "description": "Multi-player"}],
                                "genres": [{"id": "1", "description": "Strategy"}],
                                "release_date": {"coming_soon": False, "date": "9 Jul, 2013"},
                                "price_overview": {
                                    "currency": "USD",
                                    "initial": 0,
                                    "final": 0,
                                    "discount_percent": 0,
                                },
                            },
                        }
                    }
            else:
                # SteamSpy API call
                app_id = params.get("appid")
                if app_id == 730:
                    response.json.return_value = sample_steamspy_response
                elif app_id == 570:
                    response.json.return_value = {
                        "appid": 570,
                        "name": "Dota 2",
                        "tags": {
                            "MOBA": 1000,
                            "Strategy": 950,
                            "Multiplayer": 900,
                        },
                    }

            return response

        mock_get.side_effect = side_effect

        collector = SteamStoreCollector()
        game_ids = [730, 570]  # CS2 and Dota 2
        metadata_list = collector.collect_top_games_metadata(game_ids)

        assert len(metadata_list) == 2
        assert all("app_id" in meta for meta in metadata_list)
        assert all("name" in meta for meta in metadata_list)
        assert all("tags" in meta for meta in metadata_list)

    def test_extract_genres(self, sample_store_response):
        """Test genre extraction from API response."""
        collector = SteamStoreCollector()
        game_data = sample_store_response["730"]["data"]
        genres = collector._extract_genres(game_data)

        assert isinstance(genres, list)
        assert "Action" in genres

    def test_extract_categories(self, sample_store_response):
        """Test category extraction from API response."""
        collector = SteamStoreCollector()
        game_data = sample_store_response["730"]["data"]
        categories = collector._extract_categories(game_data)

        assert isinstance(categories, list)
        assert "Multi-player" in categories
        assert "Online Multi-Player" in categories

    def test_parse_price(self, sample_store_response):
        """Test price parsing from API response."""
        collector = SteamStoreCollector()
        game_data = sample_store_response["730"]["data"]
        price_info = collector._parse_price(game_data)

        assert price_info["currency"] == "USD"
        assert price_info["price"] == 0
        assert price_info["is_free"] is True
