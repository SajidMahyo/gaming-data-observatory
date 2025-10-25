"""Twitch API collector for viewership data."""

import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


class TwitchCollector:
    """Collector for Twitch API viewership statistics."""

    API_BASE_URL = "https://api.twitch.tv/helix"
    AUTH_URL = "https://id.twitch.tv/oauth2/token"
    TIMEOUT_SECONDS = 10

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        db_path: Path | None = None,
    ) -> None:
        """
        Initialize Twitch collector with OAuth2 authentication.

        Args:
            client_id: Twitch Client ID (loaded from .env if not provided)
            client_secret: Twitch Client Secret (loaded from .env if not provided)
            max_retries: Maximum number of retry attempts (default: 3)
            retry_delay: Base delay between retries in seconds (default: 1.0)
            db_path: Path to DuckDB database (default: data/duckdb/gaming.db)
        """
        load_dotenv()

        self.client_id = client_id or os.getenv("TWITCH_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("TWITCH_CLIENT_SECRET")

        if not self.client_id or not self.client_secret:
            raise ValueError(
                "Twitch credentials not found. Set TWITCH_CLIENT_ID and "
                "TWITCH_CLIENT_SECRET in .env file or pass as arguments."
            )

        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.access_token: str | None = None
        self.token_expires_at: float = 0
        self.db_path = Path(db_path) if db_path else Path("data/duckdb/gaming.db")
        self._tracked_games = self._load_tracked_games()

    def _load_tracked_games(self) -> list[dict[str, Any]]:
        """Load tracked games from DuckDB game_metadata table.

        Returns:
            List of game dictionaries with twitch_game_id, game_name, steam_app_id
        """
        if not self.db_path.exists():
            print(f"⚠️  Database not found at {self.db_path}, no games to track")
            return []

        try:
            from python.storage.duckdb_manager import DuckDBManager

            with DuckDBManager(db_path=self.db_path) as db:
                games_list = db.get_active_games_for_platform("twitch")

                if not games_list:
                    print("⚠️  No active Twitch games found in database")
                    return []

                # Filter only games that have twitch_game_id
                tracked_games = [
                    {
                        "twitch_game_id": game["twitch_game_id"],
                        "game_name": game["game_name"],
                        "steam_app_id": game.get("steam_app_id"),
                    }
                    for game in games_list
                    if game.get("twitch_game_id")
                ]

                print(f"✅ Loaded {len(tracked_games)} tracked games from database")
                return tracked_games

        except Exception as e:
            print(f"❌ Error loading games from database: {e}, no games to track")
            return []

    def get_tracked_games(self) -> list[dict[str, Any]]:
        """Get the list of tracked games.

        Returns:
            List of game dictionaries with metadata
        """
        return self._tracked_games.copy()

    def _get_access_token(self) -> str:
        """
        Get OAuth2 access token for Twitch API.

        Returns:
            Access token string

        Raises:
            requests.RequestException: If authentication fails
        """
        # Check if token is still valid (with 5 min buffer)
        if self.access_token and time.time() < (self.token_expires_at - 300):
            return self.access_token

        # Request new token
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
        }

        response = requests.post(self.AUTH_URL, params=params, timeout=self.TIMEOUT_SECONDS)
        response.raise_for_status()

        data = response.json()
        self.access_token = data["access_token"]
        self.token_expires_at = time.time() + data["expires_in"]

        return self.access_token

    def _make_request(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Make authenticated request to Twitch API with retry logic.

        Args:
            endpoint: API endpoint (e.g., '/games', '/streams')
            params: Query parameters

        Returns:
            JSON response as dictionary

        Raises:
            requests.RequestException: If request fails after all retries
        """
        url = f"{self.API_BASE_URL}{endpoint}"
        headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self._get_access_token()}",
        }

        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    url, headers=headers, params=params, timeout=self.TIMEOUT_SECONDS
                )
                response.raise_for_status()
                result: dict[str, Any] = response.json()
                return result

            except requests.HTTPError as e:
                if e.response.status_code == 401:
                    # Token expired, clear and retry
                    self.access_token = None
                    headers["Authorization"] = f"Bearer {self._get_access_token()}"
                    continue

                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2**attempt))
                    continue
                raise

            except requests.RequestException:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2**attempt))
                    continue
                raise

        raise requests.RequestException("Max retries exceeded")

    def get_game_id(self, game_name: str) -> str | None:
        """
        Get Twitch game ID from game name.

        Args:
            game_name: Name of the game (e.g., "Counter-Strike 2")

        Returns:
            Game ID string or None if not found
        """
        try:
            data = self._make_request("/games", params={"name": game_name})
            games = data.get("data", [])

            if games:
                game_id: str = str(games[0]["id"])
                return game_id

            return None

        except requests.RequestException as e:
            print(f"Error fetching game ID for {game_name}: {e}")
            return None

    def discover_trending_games(self, limit: int = 50) -> list[dict[str, Any]]:
        """
        Discover trending games on Twitch by current viewership.

        Fetches top games by viewer count, then finds their IGDB IDs for discovery.

        Args:
            limit: Number of top games to fetch from Twitch (default: 50)

        Returns:
            List of dicts with igdb_id, game_name for discovery
        """
        from python.collectors.igdb import IGDBCollector

        try:
            # Get top games from Twitch
            data = self._make_request("/games/top", params={"first": min(limit, 100)})
            twitch_games = data.get("data", [])

            if not twitch_games:
                print("⚠️  No trending games found on Twitch")
                return []

            print(f"📊 Found {len(twitch_games)} trending games on Twitch")
            print(f"🔍 Resolving IGDB IDs via external_games API...")

            # Find IGDB IDs for these Twitch games
            igdb_collector = IGDBCollector()
            discovered_games = []

            for twitch_game in twitch_games[:limit]:
                twitch_game_id = str(twitch_game["id"])
                twitch_name = twitch_game["name"]

                try:
                    # Search IGDB external_games for this twitch_game_id
                    igdb_id = igdb_collector.find_igdb_id_by_twitch(twitch_game_id)

                    if igdb_id:
                        discovered_games.append(
                            {
                                "igdb_id": igdb_id,
                                "game_name": twitch_name,
                                "twitch_game_id": twitch_game_id,
                            }
                        )
                        print(f"  ✅ {twitch_name}: IGDB {igdb_id}")
                    else:
                        print(f"  ⚠️  {twitch_name}: IGDB ID not found")

                except Exception as e:
                    print(f"  ❌ {twitch_name}: {e}")
                    continue

            print(f"\n✅ Discovered {len(discovered_games)} games from Twitch trending")

            if len(discovered_games) < len(twitch_games[:limit]):
                print(
                    f"ℹ️  {len(twitch_games[:limit]) - len(discovered_games)} Twitch games not found in IGDB"
                )

            return discovered_games

        except requests.RequestException as e:
            print(f"❌ Error discovering Twitch trending games: {e}")
            return []
        except Exception as e:
            print(f"❌ Error resolving IGDB IDs: {e}")
            return []

    def get_game_viewership(self, game_id: str) -> dict[str, Any] | None:
        """
        Get current viewership data for a game.

        Args:
            game_id: Twitch game ID

        Returns:
            Dictionary with viewership data or None if failed
        """
        try:
            # Get streams for the game
            data = self._make_request("/streams", params={"game_id": game_id, "first": 100})

            streams = data.get("data", [])

            if not streams:
                return {
                    "game_id": game_id,
                    "viewer_count": 0,
                    "channel_count": 0,
                    "top_streams": [],
                }

            # Calculate total viewers and channel count
            total_viewers = sum(stream["viewer_count"] for stream in streams)
            channel_count = len(streams)

            # Get top 3 streams
            top_streams = sorted(streams, key=lambda s: s["viewer_count"], reverse=True)[:3]

            return {
                "game_id": game_id,
                "viewer_count": total_viewers,
                "channel_count": channel_count,
                "top_streams": [
                    {
                        "user_name": stream["user_name"],
                        "viewer_count": stream["viewer_count"],
                        "title": stream["title"],
                    }
                    for stream in top_streams
                ],
            }

        except requests.RequestException as e:
            print(f"Error fetching viewership for game {game_id}: {e}")
            return None

    def collect_game_data(
        self, twitch_game_id: str, game_name: str, steam_app_id: int | None = None
    ) -> dict[str, Any] | None:
        """
        Collect Twitch data for a game using pre-mapped Twitch game ID.

        Args:
            twitch_game_id: Twitch game ID (already mapped from IGDB)
            game_name: Name of the game
            steam_app_id: Steam application ID (for reference, optional)

        Returns:
            Dictionary with Twitch data or None if failed
        """
        # Get viewership data using the mapped ID
        viewership = self.get_game_viewership(twitch_game_id)
        if not viewership:
            return None

        # Combine data
        return {
            "steam_app_id": steam_app_id,
            "game_name": game_name,
            "twitch_game_id": twitch_game_id,
            "viewer_count": viewership["viewer_count"],
            "channel_count": viewership["channel_count"],
            "top_streams": viewership["top_streams"],
            "timestamp": datetime.now(UTC).isoformat(),
        }

    def collect_tracked_games(self, limit: int | None = None, delay: float = 1.0) -> list[dict[str, Any]]:
        """
        Collect Twitch data for tracked games from database.

        Args:
            limit: Number of games to collect. If None, collects all tracked games.
            delay: Delay between requests in seconds (rate limiting)

        Returns:
            List of dictionaries with Twitch data (skips games with errors)
        """
        results = []
        games_to_collect = self._tracked_games[:limit] if limit else self._tracked_games

        for game in games_to_collect:
            try:
                data = self.collect_game_data(
                    twitch_game_id=game["twitch_game_id"],
                    game_name=game["game_name"],
                    steam_app_id=game.get("steam_app_id"),
                )

                if data:
                    results.append(data)
                    print(
                        f"✓ {game['game_name']}: {data['viewer_count']:,} viewers, "
                        f"{data['channel_count']} channels"
                    )
                else:
                    print(f"✗ {game['game_name']}: No Twitch data found")

            except Exception as e:
                print(f"⚠️  Skipping {game['game_name']}: {e}")
                continue

            # Rate limiting
            if delay > 0:
                time.sleep(delay)

        return results

    def collect_multiple_games(
        self, games: dict[int, str], delay: float = 1.0
    ) -> list[dict[str, Any]]:
        """
        Collect Twitch data for multiple games (legacy method).

        DEPRECATED: Use collect_tracked_games() instead to leverage pre-mapped IDs.

        Args:
            games: Dictionary mapping Steam app_id to game name
            delay: Delay between requests in seconds (rate limiting)

        Returns:
            List of dictionaries with Twitch data
        """
        results = []

        for app_id, game_name in games.items():
            # Legacy behavior: look up Twitch game ID by name
            game_id = self.get_game_id(game_name)
            if not game_id:
                print(f"✗ {game_name}: Twitch game ID not found")
                continue

            data = self.collect_game_data(
                twitch_game_id=game_id, game_name=game_name, steam_app_id=app_id
            )

            if data:
                results.append(data)
                print(
                    f"✓ {game_name}: {data['viewer_count']:,} viewers, "
                    f"{data['channel_count']} channels"
                )
            else:
                print(f"✗ {game_name}: No Twitch data found")

            # Rate limiting
            if delay > 0:
                time.sleep(delay)

        return results
