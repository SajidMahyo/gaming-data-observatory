"""Steam Store API collector for game metadata."""

import time
from typing import Any

import requests


class SteamStoreCollector:
    """Collects game metadata from Steam Store and SteamSpy APIs."""

    def __init__(self) -> None:
        """Initialize Steam Store collector."""
        self.store_api_base = "https://store.steampowered.com/api/appdetails"
        self.steamspy_api_base = "https://steamspy.com/api.php"
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Gaming-Data-Observatory/1.0"})

    def get_game_details(self, app_id: int) -> dict[str, Any] | None:
        """Get game details from Steam Store API.

        Args:
            app_id: Steam application ID

        Returns:
            Dictionary with game details or None if failed
        """
        try:
            params: dict[str, Any] = {"appids": app_id}
            response = self.session.get(self.store_api_base, params=params, timeout=10)

            if response.status_code != 200:
                return None

            data = response.json()
            app_data = data.get(str(app_id))

            if not app_data or not app_data.get("success"):
                return None

            game_data = app_data["data"]

            return {
                "app_id": app_id,
                "name": game_data.get("name", ""),
                "type": game_data.get("type", ""),
                "description": game_data.get("short_description", ""),
                "developers": game_data.get("developers", []),
                "publishers": game_data.get("publishers", []),
                "is_free": game_data.get("is_free", False),
                "required_age": game_data.get("required_age", 0),
                "release_date": game_data.get("release_date", {}).get("date", ""),
                "platforms": self._extract_platforms(game_data),
                "metacritic_score": game_data.get("metacritic", {}).get("score"),
                "metacritic_url": game_data.get("metacritic", {}).get("url"),
                "categories": self._extract_categories(game_data),
                "genres": self._extract_genres(game_data),
                "price_info": self._parse_price(game_data),
            }

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching game details for {app_id}: {e}")
            return None

    def get_game_tags(self, app_id: int) -> dict[str, int]:
        """Get game tags from SteamSpy API.

        Args:
            app_id: Steam application ID

        Returns:
            Dictionary of tags with their scores
        """
        try:
            params: dict[str, Any] = {"request": "appdetails", "appid": app_id}
            response = self.session.get(self.steamspy_api_base, params=params, timeout=10)

            if response.status_code != 200:
                return {}

            data = response.json()
            tags: dict[str, int] = data.get("tags", {})
            return tags

        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"Error fetching tags for {app_id}: {e}")
            return {}

    def collect_full_metadata(self, app_id: int) -> dict[str, Any] | None:
        """Collect full metadata including details and tags.

        Args:
            app_id: Steam application ID

        Returns:
            Dictionary with complete metadata or None if failed
        """
        details = self.get_game_details(app_id)
        if not details:
            return None

        # Add tags from SteamSpy
        tags = self.get_game_tags(app_id)
        details["tags"] = tags

        # Add collection timestamp
        details["collected_at"] = time.strftime("%Y-%m-%d %H:%M:%S")

        return details

    def collect_top_games_metadata(
        self, game_ids: list[int], delay: float = 1.5
    ) -> list[dict[str, Any]]:
        """Collect metadata for multiple games.

        Args:
            game_ids: List of Steam application IDs
            delay: Delay between API calls in seconds (rate limiting)

        Returns:
            List of metadata dictionaries
        """
        metadata_list = []

        for app_id in game_ids:
            print(f"Collecting metadata for app {app_id}...")
            metadata = self.collect_full_metadata(app_id)

            if metadata:
                metadata_list.append(metadata)
                print(f"✅ Collected metadata for {metadata['name']}")
            else:
                print(f"❌ Failed to collect metadata for app {app_id}")

            # Rate limiting to avoid overwhelming the APIs
            time.sleep(delay)

        return metadata_list

    def _extract_platforms(self, game_data: dict[str, Any]) -> list[str]:
        """Extract supported platforms from game data.

        Args:
            game_data: Raw game data from API

        Returns:
            List of platform names
        """
        platforms_data = game_data.get("platforms", {})
        return [platform for platform, supported in platforms_data.items() if supported]

    def _extract_genres(self, game_data: dict[str, Any]) -> list[str]:
        """Extract genres from game data.

        Args:
            game_data: Raw game data from API

        Returns:
            List of genre names
        """
        genres_data = game_data.get("genres", [])
        return [genre.get("description", "") for genre in genres_data]

    def _extract_categories(self, game_data: dict[str, Any]) -> list[str]:
        """Extract categories from game data.

        Args:
            game_data: Raw game data from API

        Returns:
            List of category names
        """
        categories_data = game_data.get("categories", [])
        return [cat.get("description", "") for cat in categories_data]

    def _parse_price(self, game_data: dict[str, Any]) -> dict[str, Any]:
        """Parse price information from game data.

        Args:
            game_data: Raw game data from API

        Returns:
            Dictionary with price information
        """
        price_overview = game_data.get("price_overview", {})
        is_free = game_data.get("is_free", False)

        if is_free or not price_overview:
            return {"currency": "USD", "price": 0, "is_free": True, "discount": 0}

        return {
            "currency": price_overview.get("currency", "USD"),
            "price": price_overview.get("final", 0) / 100,  # Convert cents to dollars
            "initial_price": price_overview.get("initial", 0) / 100,
            "is_free": False,
            "discount": price_overview.get("discount_percent", 0),
        }
