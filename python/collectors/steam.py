"""Steam API collector for player count data."""

import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests


class SteamCollector:
    """Collector for Steam API player statistics."""

    # Top 10 Steam games by concurrent players (fallback if database not available)
    TOP_GAMES: dict[int, str] = {
        730: "Counter-Strike 2",
        570: "Dota 2",
        578080: "PUBG: BATTLEGROUNDS",
        1172470: "Apex Legends",
        271590: "Grand Theft Auto V",
        440: "Team Fortress 2",
        252490: "Rust",
        1938090: "Call of Duty",
        1623730: "Palworld",
        2357570: "Elden Ring",
    }

    API_BASE_URL = "https://api.steampowered.com"
    TIMEOUT_SECONDS = 10

    def __init__(
        self,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        db_path: Path | None = None,
    ) -> None:
        """
        Initialize Steam collector.

        Args:
            max_retries: Maximum number of retry attempts (default: 3)
            retry_delay: Base delay between retries in seconds (default: 1.0)
            db_path: Path to DuckDB database (default: data/duckdb/gaming.db)
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.db_path = Path(db_path) if db_path else Path("data/duckdb/gaming.db")
        self._tracked_games = self._load_tracked_games()

    def _load_tracked_games(self) -> dict[int, str]:
        """Load tracked games from DuckDB game_metadata table.

        Returns:
            Dictionary mapping steam_app_id to game_name
        """
        if not self.db_path.exists():
            print(f"⚠️  Database not found at {self.db_path}, using default TOP_GAMES")
            return self.TOP_GAMES.copy()

        try:
            from python.storage.duckdb_manager import DuckDBManager

            with DuckDBManager(db_path=self.db_path) as db:
                games_list = db.get_active_games_for_platform("steam")

                if not games_list:
                    print("⚠️  No active Steam games found in database, using default TOP_GAMES")
                    return self.TOP_GAMES.copy()

                games = {int(game["steam_app_id"]): game["game_name"] for game in games_list}
                print(f"✅ Loaded {len(games)} tracked games from database")
                return games

        except Exception as e:
            print(f"❌ Error loading games from database: {e}, using default TOP_GAMES")
            return self.TOP_GAMES.copy()

    def get_player_count(self, app_id: int) -> int:
        """
        Get current player count for a Steam game with retry logic.

        Args:
            app_id: Steam application ID

        Returns:
            Current number of players

        Raises:
            requests.exceptions.RequestException: If API request fails after retries
            ValueError: If response JSON is invalid
            KeyError: If player_count is missing from response
        """
        url = f"{self.API_BASE_URL}/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
        params = {"appid": app_id}

        last_exception = None

        for attempt in range(self.max_retries + 1):
            try:
                response = requests.get(url, params=params, timeout=self.TIMEOUT_SECONDS)
                response.raise_for_status()

                data = response.json()
                player_count: int = data["response"]["player_count"]
                return player_count

            except requests.exceptions.RequestException as e:
                last_exception = e
                if attempt < self.max_retries:
                    # Exponential backoff
                    delay = self.retry_delay * (2**attempt)
                    time.sleep(delay)
                    continue
                raise

        # Should never reach here, but for type safety
        if last_exception:
            raise last_exception
        raise RuntimeError("Unexpected retry loop exit")

    def get_game_data(self, app_id: int) -> dict[str, Any]:
        """
        Get complete game data including player count and metadata.

        Args:
            app_id: Steam application ID

        Returns:
            Dictionary with app_id, game_name, player_count, timestamp
        """
        player_count = self.get_player_count(app_id)
        game_name = self._tracked_games.get(app_id, f"Game {app_id}")

        return {
            "app_id": app_id,
            "game_name": game_name,
            "player_count": player_count,
            "timestamp": datetime.now(UTC).isoformat(),
        }

    def collect_top_games(self, limit: int | None = None) -> list[dict[str, Any]]:
        """
        Collect player data for tracked games.

        Args:
            limit: Number of games to collect. If None, collects all tracked games.

        Returns:
            List of game data dictionaries (skips games with errors)
        """
        results = []
        game_ids = list(self._tracked_games.keys())

        if limit is not None:
            game_ids = game_ids[:limit]

        for app_id in game_ids:
            try:
                game_data = self.get_game_data(app_id)
                results.append(game_data)
            except Exception as e:
                game_name = self._tracked_games.get(app_id, f"Game {app_id}")
                print(f"⚠️  Skipping {game_name} (ID: {app_id}): {e}")
                continue

        return results

    def get_top_games(self) -> dict[int, str]:
        """
        Get the dictionary of tracked games.

        Returns:
            Dictionary mapping app_id to game_name
        """
        return self._tracked_games.copy()
