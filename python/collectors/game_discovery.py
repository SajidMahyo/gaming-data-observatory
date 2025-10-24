"""Game discovery module for dynamically tracking popular games."""

import json
import time
from pathlib import Path
from typing import Any

import requests


class GameDiscovery:
    """Discovers and tracks popular games using SteamSpy API."""

    def __init__(self, config_path: Path | None = None) -> None:
        """Initialize game discovery.

        Args:
            config_path: Path to games configuration file. Defaults to config/games.json
        """
        if config_path is None:
            config_path = Path("config/games.json")
        self.config_path = Path(config_path)
        self.steamspy_api_base = "https://steamspy.com/api.php"
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Gaming-Data-Observatory/1.0"})

    def load_tracked_games(self) -> dict[int, str]:
        """Load currently tracked games from config file.

        Returns:
            Dictionary mapping app_id to game name
        """
        if not self.config_path.exists():
            return {}

        try:
            with open(self.config_path) as f:
                data = json.load(f)
                return {int(app_id): name for app_id, name in data.items()}
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error loading games config: {e}")
            return {}

    def save_tracked_games(self, games: dict[int, str]) -> None:
        """Save tracked games to config file.

        Args:
            games: Dictionary mapping app_id to game name
        """
        # Create config directory if it doesn't exist
        self.config_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert int keys to strings for JSON
        games_str = {str(app_id): name for app_id, name in games.items()}

        with open(self.config_path, "w") as f:
            json.dump(games_str, f, indent=2, ensure_ascii=False)

        print(f"✅ Saved {len(games)} tracked games to {self.config_path}")

    def discover_top_games(self, limit: int = 100) -> dict[int, str]:
        """Discover top games by playtime from SteamSpy.

        Args:
            limit: Maximum number of games to fetch

        Returns:
            Dictionary mapping app_id to game name
        """
        try:
            params: dict[str, Any] = {"request": "top100in2weeks"}
            response = self.session.get(self.steamspy_api_base, params=params, timeout=30)

            if response.status_code != 200:
                print(f"❌ SteamSpy API returned status {response.status_code}")
                return {}

            data = response.json()

            # SteamSpy returns dict with app_id as keys
            discovered = {}
            for app_id_str, game_data in list(data.items())[:limit]:
                try:
                    app_id = int(app_id_str)
                    name = game_data.get("name", f"Game {app_id}")
                    discovered[app_id] = name
                except (ValueError, KeyError) as e:
                    print(f"⚠️  Skipping invalid game data: {e}")
                    continue

            print(f"🔍 Discovered {len(discovered)} top games by playtime")
            return discovered

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching top games: {e}")
            return {}

    def discover_trending_games(self, limit: int = 100) -> dict[int, str]:
        """Discover trending games (high player count growth) from SteamSpy.

        Args:
            limit: Maximum number of games to fetch

        Returns:
            Dictionary mapping app_id to game name
        """
        try:
            # Use 'all' endpoint to get games sorted by current players
            params: dict[str, Any] = {"request": "all", "page": "0"}
            response = self.session.get(self.steamspy_api_base, params=params, timeout=30)

            if response.status_code != 200:
                print(f"❌ SteamSpy API returned status {response.status_code}")
                return {}

            data = response.json()

            # Get games with high CCU
            games_list = []
            for app_id_str, game_data in data.items():
                try:
                    app_id = int(app_id_str)
                    name = game_data.get("name", f"Game {app_id}")
                    ccu = game_data.get("ccu", 0)
                    games_list.append((app_id, name, ccu))
                except (ValueError, KeyError):
                    continue

            # Sort by current players and take top N
            games_list.sort(key=lambda x: x[2], reverse=True)
            discovered = {app_id: name for app_id, name, _ in games_list[:limit]}

            print(f"🔥 Discovered {len(discovered)} trending games by CCU")
            return discovered

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching trending games: {e}")
            return {}

    def discover_featured_games(self) -> dict[int, str]:
        """Discover featured games from Steam Store API.

        Returns:
            Dictionary mapping app_id to game name
        """
        try:
            steam_api_url = "https://store.steampowered.com/api/featured/"
            response = self.session.get(steam_api_url, timeout=30)

            if response.status_code != 200:
                print(f"❌ Steam API returned status {response.status_code}")
                return {}

            data = response.json()
            discovered = {}

            # Collect featured games from all platforms
            for category in ["featured_win", "featured_mac", "featured_linux", "large_capsules"]:
                games_list = data.get(category, [])
                for game in games_list:
                    try:
                        app_id = int(game.get("id"))
                        name = game.get("name", f"Game {app_id}")
                        # Deduplicate by app_id
                        if app_id not in discovered:
                            discovered[app_id] = name
                    except (ValueError, KeyError, TypeError) as e:
                        print(f"⚠️  Skipping invalid game data: {e}")
                        continue

            print(f"⭐ Discovered {len(discovered)} featured games from Steam")
            return discovered

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"❌ Error fetching featured games: {e}")
            return {}

    def update_tracked_games(
        self,
        include_top: bool = True,
        include_trending: bool = True,
        include_featured: bool = True,
        top_limit: int = 100,
        trending_limit: int = 50,
        delay: float = 1.0,
    ) -> dict[int, str]:
        """Update tracked games list by discovering new games (append-only).

        This method only adds new games to the list, never removes existing ones.

        Args:
            include_top: Whether to discover top games by playtime
            include_trending: Whether to discover trending games by CCU
            include_featured: Whether to discover featured games from Steam
            top_limit: Maximum number of top games to fetch
            trending_limit: Maximum number of trending games to fetch
            delay: Delay between API calls in seconds (rate limiting)

        Returns:
            Updated dictionary of all tracked games
        """
        # Load current tracked games
        tracked = self.load_tracked_games()
        initial_count = len(tracked)
        print(f"📊 Currently tracking {initial_count} games")

        # Discover top games by playtime
        if include_top:
            print("\n🔍 Discovering top games by playtime...")
            top_games = self.discover_top_games(limit=top_limit)

            # Add new games (append-only)
            new_from_top = 0
            for app_id, name in top_games.items():
                if app_id not in tracked:
                    tracked[app_id] = name
                    new_from_top += 1
                    print(f"  ➕ Added: {name} (app_id: {app_id})")

            print(f"✅ Added {new_from_top} new games from top by playtime")
            time.sleep(delay)  # Rate limiting

        # Discover trending games by CCU
        if include_trending:
            print("\n🔥 Discovering trending games by CCU...")
            trending_games = self.discover_trending_games(limit=trending_limit)

            # Add new games (append-only)
            new_from_trending = 0
            for app_id, name in trending_games.items():
                if app_id not in tracked:
                    tracked[app_id] = name
                    new_from_trending += 1
                    print(f"  ➕ Added: {name} (app_id: {app_id})")

            print(f"✅ Added {new_from_trending} new games from trending")
            time.sleep(delay)  # Rate limiting

        # Discover featured games from Steam
        if include_featured:
            print("\n⭐ Discovering featured games from Steam...")
            featured_games = self.discover_featured_games()

            # Add new games (append-only)
            new_from_featured = 0
            for app_id, name in featured_games.items():
                if app_id not in tracked:
                    tracked[app_id] = name
                    new_from_featured += 1
                    print(f"  ➕ Added: {name} (app_id: {app_id})")

            print(f"✅ Added {new_from_featured} new games from featured")
            time.sleep(delay)  # Rate limiting

        # Save updated list
        final_count = len(tracked)
        new_total = final_count - initial_count

        print(f"\n📈 Total tracked games: {initial_count} → {final_count} (+{new_total})")
        self.save_tracked_games(tracked)

        return tracked
