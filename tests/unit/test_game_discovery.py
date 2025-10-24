"""Tests for GameDiscovery class."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from python.collectors.game_discovery import GameDiscovery


@pytest.fixture
def temp_config_path(tmp_path: Path) -> Path:
    """Create a temporary config path for testing."""
    return tmp_path / "games.json"


@pytest.fixture
def discovery(temp_config_path: Path) -> GameDiscovery:
    """Create GameDiscovery instance with temp config."""
    return GameDiscovery(config_path=temp_config_path)


@pytest.fixture
def sample_games() -> dict[int, str]:
    """Sample games dictionary."""
    return {
        730: "Counter-Strike 2",
        570: "Dota 2",
        578080: "PUBG: BATTLEGROUNDS",
    }


@pytest.fixture
def steamspy_top_response() -> dict:
    """Mock SteamSpy top100in2weeks response."""
    return {
        "730": {"name": "Counter-Strike 2"},
        "570": {"name": "Dota 2"},
        "1086940": {"name": "Baldur's Gate 3"},
        "2358720": {"name": "Black Myth: Wukong"},
    }


@pytest.fixture
def steam_featured_response() -> dict:
    """Mock Steam Store featured response."""
    return {
        "featured_win": [
            {"id": 2694490, "name": "Path of Exile 2"},
            {"id": 2246340, "name": "Monster Hunter Wilds"},
        ],
        "featured_mac": [],
        "featured_linux": [],
        "large_capsules": [],
    }


class TestGameDiscovery:
    """Tests for GameDiscovery class."""

    def test_init_default_path(self) -> None:
        """Test initialization with default config path."""
        discovery = GameDiscovery()
        assert discovery.config_path == Path("config/games.json")
        assert discovery.steamspy_api_base == "https://steamspy.com/api.php"

    def test_init_custom_path(self, temp_config_path: Path) -> None:
        """Test initialization with custom config path."""
        discovery = GameDiscovery(config_path=temp_config_path)
        assert discovery.config_path == temp_config_path

    def test_load_tracked_games_nonexistent_file(self, discovery: GameDiscovery) -> None:
        """Test loading from nonexistent config file returns empty dict."""
        games = discovery.load_tracked_games()
        assert games == {}

    def test_load_tracked_games_success(
        self, discovery: GameDiscovery, sample_games: dict[int, str]
    ) -> None:
        """Test loading tracked games from existing config file."""
        # Write sample games to config
        discovery.save_tracked_games(sample_games)

        # Load and verify
        loaded_games = discovery.load_tracked_games()
        assert loaded_games == sample_games

    def test_load_tracked_games_invalid_json(
        self, discovery: GameDiscovery, temp_config_path: Path, capsys
    ) -> None:
        """Test loading from invalid JSON file returns empty dict."""
        # Write invalid JSON
        temp_config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(temp_config_path, "w") as f:
            f.write("{invalid json")

        games = discovery.load_tracked_games()
        assert games == {}

        captured = capsys.readouterr()
        assert "Error loading games config" in captured.out

    def test_save_tracked_games(
        self, discovery: GameDiscovery, sample_games: dict[int, str]
    ) -> None:
        """Test saving tracked games to config file."""
        discovery.save_tracked_games(sample_games)

        # Verify file was created
        assert discovery.config_path.exists()

        # Verify contents
        with open(discovery.config_path) as f:
            data = json.load(f)

        # Convert keys back to int for comparison
        loaded_games = {int(k): v for k, v in data.items()}
        assert loaded_games == sample_games

    @patch("python.collectors.game_discovery.requests.Session.get")
    def test_discover_top_games_success(
        self,
        mock_get: MagicMock,
        discovery: GameDiscovery,
        steamspy_top_response: dict,
    ) -> None:
        """Test discovering top games from SteamSpy."""
        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = steamspy_top_response
        mock_get.return_value = mock_response

        # Discover games
        discovered = discovery.discover_top_games(limit=4)

        # Verify
        assert len(discovered) == 4
        assert 730 in discovered
        assert discovered[730] == "Counter-Strike 2"
        assert 1086940 in discovered
        assert discovered[1086940] == "Baldur's Gate 3"

    @patch("python.collectors.game_discovery.requests.Session.get")
    def test_discover_top_games_api_error(
        self, mock_get: MagicMock, discovery: GameDiscovery, capsys
    ) -> None:
        """Test handling API errors when discovering top games."""
        # Mock API error
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        # Discover games
        discovered = discovery.discover_top_games()

        # Verify
        assert discovered == {}
        captured = capsys.readouterr()
        assert "SteamSpy API returned status 500" in captured.out

    @patch("python.collectors.game_discovery.requests.Session.get")
    def test_discover_top_games_request_exception(
        self, mock_get: MagicMock, discovery: GameDiscovery, capsys
    ) -> None:
        """Test handling request exceptions."""
        # Mock request exception
        mock_get.side_effect = requests.RequestException("Connection error")

        # Discover games
        discovered = discovery.discover_top_games()

        # Verify
        assert discovered == {}
        captured = capsys.readouterr()
        assert "Error fetching top games" in captured.out

    @patch("python.collectors.game_discovery.requests.Session.get")
    def test_discover_featured_games_success(
        self,
        mock_get: MagicMock,
        discovery: GameDiscovery,
        steam_featured_response: dict,
    ) -> None:
        """Test discovering featured games from Steam Store API."""
        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = steam_featured_response
        mock_get.return_value = mock_response

        # Discover featured games
        discovered = discovery.discover_featured_games()

        # Verify
        assert len(discovered) == 2
        assert 2694490 in discovered
        assert discovered[2694490] == "Path of Exile 2"

    @patch("python.collectors.game_discovery.requests.Session.get")
    def test_update_tracked_games_append_only(
        self,
        mock_get: MagicMock,
        discovery: GameDiscovery,
        sample_games: dict[int, str],
        steamspy_top_response: dict,
    ) -> None:
        """Test that update_tracked_games only adds new games (append-only)."""
        # Save initial games
        discovery.save_tracked_games(sample_games)

        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = steamspy_top_response
        mock_get.return_value = mock_response

        # Update tracked games
        updated = discovery.update_tracked_games(
            include_top=True,
            include_featured=False,
            top_limit=4,
        )

        # Verify: original games still present + new games added
        assert len(updated) > len(sample_games)
        assert 730 in updated  # Original game
        assert 570 in updated  # Original game
        assert 1086940 in updated  # New game
        assert 2358720 in updated  # New game

    @patch("python.collectors.game_discovery.requests.Session.get")
    def test_update_tracked_games_skip_duplicates(
        self,
        mock_get: MagicMock,
        discovery: GameDiscovery,
        sample_games: dict[int, str],
        steamspy_top_response: dict,
        capsys,
    ) -> None:
        """Test that duplicate games are not added again."""
        # Save initial games
        discovery.save_tracked_games(sample_games)
        initial_count = len(sample_games)

        # Mock API response (contains some games already in sample_games)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = steamspy_top_response
        mock_get.return_value = mock_response

        # Update tracked games
        updated = discovery.update_tracked_games(
            include_top=True,
            include_featured=False,
            top_limit=4,
        )

        # Verify: only new games were added (not duplicates)
        # sample_games has 730, 570, 578080
        # steamspy response has 730, 570, 1086940, 2358720
        # So we should add 1086940 and 2358720 (2 new games)
        assert len(updated) == initial_count + 2

        captured = capsys.readouterr()
        # Verify that duplicate games were not printed as "Added"
        assert captured.out.count("➕ Added") == 2

    @patch("python.collectors.game_discovery.requests.Session.get")
    def test_update_tracked_games_both_sources(
        self,
        mock_get: MagicMock,
        discovery: GameDiscovery,
        steamspy_top_response: dict,
        steam_featured_response: dict,
    ) -> None:
        """Test updating from both top games and featured games."""

        # Mock API responses (both calls)
        def mock_get_side_effect(*args, **kwargs):
            mock_response = MagicMock()
            mock_response.status_code = 200

            # Check which endpoint is being called
            if "steamspy.com" in str(args[0]) if args else "":
                mock_response.json.return_value = steamspy_top_response
            else:  # Steam Store featured API
                mock_response.json.return_value = steam_featured_response

            return mock_response

        mock_get.side_effect = mock_get_side_effect

        # Update tracked games from both sources
        updated = discovery.update_tracked_games(
            include_top=True,
            include_featured=True,
            top_limit=4,
        )

        # Verify games from both sources
        assert 730 in updated  # From top games
        assert 1086940 in updated  # From top games
        assert 2694490 in updated  # From featured games
        assert 2246340 in updated  # From featured games
