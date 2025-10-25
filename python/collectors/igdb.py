"""IGDB API collector for game discovery and metadata enrichment."""

import os
import time
from datetime import UTC, datetime
from typing import Any

import requests
from dotenv import load_dotenv


class IGDBCollector:
    """Collector for IGDB API - game discovery and metadata."""

    API_BASE_URL = "https://api.igdb.com/v4"
    AUTH_URL = "https://id.twitch.tv/oauth2/token"
    TIMEOUT_SECONDS = 10

    # IGDB external game category codes
    PLATFORM_CATEGORIES = {
        1: "steam",
        5: "gog",
        10: "youtube",
        11: "microsoft",
        13: "steam_alt",
        14: "twitch",
        15: "android",
        20: "discord",
        26: "epic",
        28: "oculus",
        33: "battlenet",
        34: "origin",
        35: "uplay",
    }

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        """
        Initialize IGDB collector with OAuth2 authentication.

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
        """Get OAuth2 access token for IGDB API."""
        if self.access_token and time.time() < (self.token_expires_at - 300):
            return self.access_token

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

    def _make_request(self, endpoint: str, query: str) -> list[dict[str, Any]]:
        """Make authenticated request to IGDB API."""
        url = f"{self.API_BASE_URL}/{endpoint}"
        headers = {
            "Client-ID": self.client_id,
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "text/plain",
        }

        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    url, headers=headers, data=query, timeout=self.TIMEOUT_SECONDS
                )
                response.raise_for_status()
                result: list[dict[str, Any]] = response.json()
                return result

            except requests.HTTPError as e:
                if e.response.status_code == 401:
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

    def discover_popular_games(self, limit: int = 100) -> list[dict[str, Any]]:
        """
        Discover popular games by total rating count.

        Args:
            limit: Maximum number of games to return

        Returns:
            List of dicts with basic game info
        """
        query = f"""
        fields id,name,slug,first_release_date,rating,aggregated_rating,total_rating_count;
        where total_rating_count > 50;
        sort total_rating_count desc;
        limit {limit};
        """

        try:
            games = self._make_request("games", query)
            print(f"✅ Discovered {len(games)} popular games from IGDB")
            return games

        except requests.RequestException as e:
            print(f"❌ Error discovering games: {e}")
            return []

    def discover_recent_games(self, limit: int = 100, days_back: int = 90) -> list[dict[str, Any]]:
        """
        Discover recently released games.

        Args:
            limit: Maximum number of games to return
            days_back: Look back this many days from today (default: 90)

        Returns:
            List of dicts with basic game info
        """
        from datetime import UTC, datetime, timedelta

        # Calculate timestamp for days_back
        cutoff_date = datetime.now(UTC) - timedelta(days=days_back)
        cutoff_timestamp = int(cutoff_date.timestamp())

        query = f"""
        fields id,name,slug,first_release_date,rating,aggregated_rating,total_rating_count;
        where first_release_date >= {cutoff_timestamp} & total_rating_count > 10;
        sort first_release_date desc;
        limit {limit};
        """

        try:
            games = self._make_request("games", query)
            print(f"✅ Discovered {len(games)} recent games from IGDB")
            return games

        except requests.RequestException as e:
            print(f"❌ Error discovering recent games: {e}")
            return []

    def discover_highest_rated_games(
        self, limit: int = 100, min_ratings: int = 100
    ) -> list[dict[str, Any]]:
        """
        Discover highest rated games.

        Args:
            limit: Maximum number of games to return
            min_ratings: Minimum number of ratings required (default: 100)

        Returns:
            List of dicts with basic game info
        """
        query = f"""
        fields id,name,slug,first_release_date,rating,aggregated_rating,total_rating_count;
        where total_rating_count >= {min_ratings} & aggregated_rating != null;
        sort aggregated_rating desc;
        limit {limit};
        """

        try:
            games = self._make_request("games", query)
            print(f"✅ Discovered {len(games)} highest rated games from IGDB")
            return games

        except requests.RequestException as e:
            print(f"❌ Error discovering highest rated games: {e}")
            return []

    def discover_upcoming_games(
        self, limit: int = 100, days_ahead: int = 180
    ) -> list[dict[str, Any]]:
        """
        Discover upcoming games (not yet released).

        Args:
            limit: Maximum number of games to return
            days_ahead: Look ahead this many days from today (default: 180)

        Returns:
            List of dicts with basic game info
        """
        from datetime import UTC, datetime, timedelta

        # Calculate timestamps
        now_timestamp = int(datetime.now(UTC).timestamp())
        future_timestamp = int((datetime.now(UTC) + timedelta(days=days_ahead)).timestamp())

        query = f"""
        fields id,name,slug,first_release_date,rating,aggregated_rating,total_rating_count;
        where first_release_date > {now_timestamp} & first_release_date < {future_timestamp};
        sort first_release_date asc;
        limit {limit};
        """

        try:
            games = self._make_request("games", query)
            print(f"✅ Discovered {len(games)} upcoming games from IGDB")
            return games

        except requests.RequestException as e:
            print(f"❌ Error discovering upcoming games: {e}")
            return []

    def get_game_metadata(self, igdb_id: int) -> dict[str, Any] | None:
        """
        Get full metadata for a game.

        Args:
            igdb_id: IGDB game ID

        Returns:
            Dictionary with complete game metadata or None if failed
        """
        query = f"""
        where id = {igdb_id};
        fields name,slug,summary,first_release_date,cover.url,
               genres.name,themes.name,platforms.name,game_modes.name,
               player_perspectives.name,
               involved_companies.company.name,involved_companies.developer,
               involved_companies.publisher,
               websites.url,websites.category;
        """

        try:
            results = self._make_request("games", query)

            if not results:
                return None

            game = results[0]

            # Extract and structure the data (KPIs removed, collected via collect igdb-ratings)
            return {
                "igdb_id": game["id"],
                "game_name": game.get("name"),
                "slug": game.get("slug"),
                "igdb_summary": game.get("summary"),
                "first_release_date": (
                    datetime.fromtimestamp(game["first_release_date"], UTC).isoformat()
                    if game.get("first_release_date")
                    else None
                ),
                "cover_url": (
                    f"https:{game['cover']['url'].replace('t_thumb', 't_cover_big')}"
                    if game.get("cover")
                    else None
                ),
                "genres": [g["name"] for g in game.get("genres", [])],
                "themes": [t["name"] for t in game.get("themes", [])],
                "platforms": [p["name"] for p in game.get("platforms", [])],
                "game_modes": [m["name"] for m in game.get("game_modes", [])],
                "developers": [
                    c["company"]["name"]
                    for c in game.get("involved_companies", [])
                    if c.get("developer")
                ],
                "publishers": [
                    c["company"]["name"]
                    for c in game.get("involved_companies", [])
                    if c.get("publisher")
                ],
                "websites": self._extract_websites(game.get("websites", [])),
            }

        except requests.RequestException as e:
            print(f"❌ Error fetching metadata for game {igdb_id}: {e}")
            return None

    def find_igdb_id_by_steam(self, steam_app_id: int) -> int | None:
        """
        Find IGDB game ID from Steam app ID.

        Args:
            steam_app_id: Steam application ID

        Returns:
            IGDB game ID or None if not found
        """
        query = f"""
        where uid = "{steam_app_id}" & external_game_source = 1;
        fields game;
        """

        try:
            results = self._make_request("external_games", query)

            if results and len(results) > 0:
                igdb_id: int = results[0]["game"]
                return igdb_id

            return None

        except requests.RequestException as e:
            print(f"❌ Error finding IGDB ID for Steam {steam_app_id}: {e}")
            return None

    def find_igdb_id_by_twitch(self, twitch_game_id: str) -> int | None:
        """
        Find IGDB game ID from Twitch game ID.

        Args:
            twitch_game_id: Twitch game ID (string)

        Returns:
            IGDB game ID or None if not found
        """
        query = f"""
        where uid = "{twitch_game_id}" & external_game_source = 14;
        fields game;
        """

        try:
            results = self._make_request("external_games", query)

            if results and len(results) > 0:
                igdb_id: int = results[0]["game"]
                return igdb_id

            return None

        except requests.RequestException as e:
            print(f"❌ Error finding IGDB ID for Twitch {twitch_game_id}: {e}")
            return None

    def get_external_ids(self, igdb_id: int) -> dict[str, Any]:
        """
        Get external platform IDs for a game.

        Args:
            igdb_id: IGDB game ID

        Returns:
            Dictionary mapping platform names to their IDs
        """
        query = f"where game = {igdb_id}; fields game,external_game_source,uid,name;"

        try:
            external_games = self._make_request("external_games", query)

            platform_ids = {}

            for external in external_games:
                source_id = external.get("external_game_source")
                uid = external.get("uid")

                if not uid or not isinstance(source_id, int):
                    continue

                # Map external_game_source to platform name
                platform_name = self.PLATFORM_CATEGORIES.get(source_id)

                if platform_name:
                    platform_ids[platform_name] = uid
                elif source_id:
                    # Store unknown sources for debugging
                    platform_ids[f"unknown_{source_id}"] = uid

            return platform_ids

        except requests.RequestException as e:
            print(f"❌ Error fetching external IDs for game {igdb_id}: {e}")
            return {}

    def enrich_game(self, igdb_id: int) -> dict[str, Any] | None:
        """
        Get complete enriched metadata for a game (metadata + external IDs).

        Args:
            igdb_id: IGDB game ID

        Returns:
            Dictionary with all metadata and platform IDs
        """
        # Get metadata
        metadata = self.get_game_metadata(igdb_id)
        if not metadata:
            return None

        # Get external IDs
        external_ids = self.get_external_ids(igdb_id)

        # Merge data
        metadata.update(
            {
                "steam_app_id": (int(external_ids["steam"]) if external_ids.get("steam") else None),
                "twitch_game_id": external_ids.get("twitch"),
                "youtube_channel_id": external_ids.get("youtube"),
                "epic_id": external_ids.get("epic"),
                "gog_id": external_ids.get("gog"),
                "discovery_source": "igdb",
                "discovery_date": datetime.now(UTC).isoformat(),
                "last_updated": datetime.now(UTC).isoformat(),
            }
        )

        return metadata

    def _extract_websites(self, websites: list[dict[str, Any]]) -> dict[str, str]:
        """Extract and categorize website URLs."""
        # Website category mapping (from IGDB docs)
        categories = {
            1: "official",
            2: "wikia",
            3: "wikipedia",
            4: "facebook",
            5: "twitter",
            6: "twitch",
            8: "instagram",
            9: "youtube",
            10: "iphone",
            11: "ipad",
            12: "android",
            13: "steam",
            14: "reddit",
            15: "itch",
            16: "epicgames",
            17: "gog",
            18: "discord",
        }

        result = {}
        for site in websites:
            site_category = site.get("category")
            category = (
                categories.get(site_category, "other")
                if isinstance(site_category, int)
                else "other"
            )
            url = site.get("url")
            if url:
                result[category] = url

        return result

    def get_game_ratings(self, igdb_id: int) -> dict[str, Any] | None:
        """
        Get rating data for a game (time-series KPI).

        Args:
            igdb_id: IGDB game ID

        Returns:
            Dictionary with rating data or None if failed
        """
        query = f"""
        where id = {igdb_id};
        fields rating,aggregated_rating,total_rating_count;
        """

        try:
            results = self._make_request("games", query)

            if not results:
                return None

            game = results[0]

            return {
                "igdb_id": igdb_id,
                "rating": game.get("rating"),
                "aggregated_rating": game.get("aggregated_rating"),
                "total_rating_count": game.get("total_rating_count"),
                "timestamp": datetime.now(UTC).isoformat(),
            }

        except requests.RequestException as e:
            print(f"❌ Error fetching ratings for game {igdb_id}: {e}")
            return None

    def discover_and_enrich(self, limit: int = 100, delay: float = 0.5) -> list[dict[str, Any]]:
        """
        Discover popular games and enrich them with full metadata.

        Args:
            limit: Number of games to discover
            delay: Delay between enrichment calls (rate limiting)

        Returns:
            List of enriched game dictionaries
        """
        # Discover games
        discovered = self.discover_popular_games(limit)

        if not discovered:
            return []

        print(f"\n📦 Enriching {len(discovered)} games with metadata...\n")

        enriched_games = []

        for i, game in enumerate(discovered, 1):
            igdb_id = game["id"]
            game_name = game.get("name", f"Game {igdb_id}")

            print(f"[{i}/{len(discovered)}] Enriching: {game_name}")

            enriched = self.enrich_game(igdb_id)

            if enriched:
                enriched_games.append(enriched)
                print(f"  ✅ Steam ID: {enriched.get('steam_app_id') or 'N/A'}")
                print(f"  ✅ Twitch ID: {enriched.get('twitch_game_id') or 'N/A'}")
            else:
                print("  ❌ Failed to enrich")

            # Rate limiting
            if delay > 0 and i < len(discovered):
                time.sleep(delay)

        print(f"\n✅ Enriched {len(enriched_games)}/{len(discovered)} games")

        return enriched_games
