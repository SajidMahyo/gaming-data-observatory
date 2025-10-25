"""Twitch API collector for viewership data."""

import os
import time
from datetime import UTC, datetime
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
    ) -> None:
        """
        Initialize Twitch collector with OAuth2 authentication.

        Args:
            client_id: Twitch Client ID (loaded from .env if not provided)
            client_secret: Twitch Client Secret (loaded from .env if not provided)
            max_retries: Maximum number of retry attempts (default: 3)
            retry_delay: Base delay between retries in seconds (default: 1.0)
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

    def _make_request(self, endpoint: str, params: dict[str, Any] | None = None) -> dict:
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
                result: dict = response.json()
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

    def collect_game_data(self, game_name: str, steam_app_id: int) -> dict[str, Any] | None:
        """
        Collect Twitch data for a game.

        Args:
            game_name: Name of the game
            steam_app_id: Steam application ID (for reference)

        Returns:
            Dictionary with Twitch data or None if failed
        """
        # Get Twitch game ID
        game_id = self.get_game_id(game_name)
        if not game_id:
            return None

        # Get viewership data
        viewership = self.get_game_viewership(game_id)
        if not viewership:
            return None

        # Combine data
        return {
            "steam_app_id": steam_app_id,
            "game_name": game_name,
            "twitch_game_id": game_id,
            "viewer_count": viewership["viewer_count"],
            "channel_count": viewership["channel_count"],
            "top_streams": viewership["top_streams"],
            "timestamp": datetime.now(UTC).isoformat(),
        }

    def collect_multiple_games(
        self, games: dict[int, str], delay: float = 1.0
    ) -> list[dict[str, Any]]:
        """
        Collect Twitch data for multiple games.

        Args:
            games: Dictionary mapping Steam app_id to game name
            delay: Delay between requests in seconds (rate limiting)

        Returns:
            List of dictionaries with Twitch data
        """
        results = []

        for app_id, game_name in games.items():
            data = self.collect_game_data(game_name, app_id)

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
