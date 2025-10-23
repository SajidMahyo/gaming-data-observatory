#!/usr/bin/env python3
"""Data loader for game metadata from DuckDB."""

import json
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from python.storage.duckdb_manager import DuckDBManager  # noqa: E402


def main() -> None:
    """Load all game metadata from DuckDB and output as JSON."""
    db_path = project_root / "data" / "duckdb" / "gaming.db"

    if not db_path.exists():
        print("[]")
        return

    try:
        with DuckDBManager(db_path=db_path) as db:
            result = db.query(
                """
                SELECT
                    app_id,
                    name,
                    type,
                    description,
                    developers,
                    publishers,
                    is_free,
                    required_age,
                    release_date,
                    platforms,
                    metacritic_score,
                    metacritic_url,
                    categories,
                    genres,
                    price_info,
                    tags,
                    collected_at
                FROM game_metadata
                ORDER BY name
            """
            )

            games = result.to_dict(orient="records")

            # Parse JSON strings back to objects
            for game in games:
                for field in [
                    "developers",
                    "publishers",
                    "platforms",
                    "categories",
                    "genres",
                    "price_info",
                    "tags",
                ]:
                    if game.get(field):
                        game[field] = json.loads(game[field])

            print(json.dumps(games, indent=2))

    except Exception:
        print("[]")


if __name__ == "__main__":
    main()
