"""Gaming Data Observatory - Main CLI entrypoint."""

from pathlib import Path

import click

from python.collectors.steam import SteamCollector
from python.processors.aggregator import KPIAggregator


@click.group()
def cli() -> None:
    """Gaming Data Observatory - Data pipeline for Steam, Twitch, Reddit."""
    pass


@cli.group()
def collect() -> None:
    """Collect time-series KPIs from gaming platforms."""
    pass


@collect.command()
@click.option(
    "--limit",
    "-l",
    default=None,
    help="Number of games to collect (default: all tracked games)",
    type=int,
)
@click.option(
    "--db-path",
    "-d",
    default="data/duckdb/gaming.db",
    help="Path to DuckDB database",
    type=click.Path(),
)
def steam(limit: int | None, db_path: str) -> None:
    """Collect Steam concurrent player counts (CCU) for tracked games.

    Reads games from game_metadata table (requires steam_app_id).
    Saves time-series data to steam_raw table in DuckDB.
    """
    from python.storage.duckdb_manager import DuckDBManager

    db_path_obj = Path(db_path)

    click.echo("🎮 Collecting Steam CCU data...\n")

    try:
        # Initialize collector
        collector = SteamCollector(db_path=db_path_obj)

        total_games = len(collector.get_top_games())
        games_to_collect = limit if limit is not None else total_games

        click.echo(f"📊 Collecting data for {games_to_collect}/{total_games} tracked games...\n")

        # Collect data
        games_data = collector.collect_top_games(limit=limit)

        click.echo(f"\n✅ Collected data for {len(games_data)} games")

        if not games_data:
            click.echo("⚠️  No data collected")
            return

        # Save to DuckDB
        with DuckDBManager(db_path=db_path_obj) as db:
            # Create steam_raw table if not exists
            db.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS steam_raw (
                    timestamp TIMESTAMP NOT NULL,
                    steam_app_id INTEGER NOT NULL,
                    game_name VARCHAR NOT NULL,
                    player_count INTEGER NOT NULL,
                    PRIMARY KEY (timestamp, steam_app_id)
                )
            """
            )

            # Insert collected data
            for data in games_data:
                db.conn.execute(
                    """
                    INSERT INTO steam_raw (timestamp, steam_app_id, game_name, player_count)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (timestamp, steam_app_id) DO UPDATE SET
                        player_count = EXCLUDED.player_count
                    """,
                    [
                        data["timestamp"],
                        data["app_id"],
                        data["game_name"],
                        data["player_count"],
                    ],
                )

            # Get stats
            count_result = db.query(
                "SELECT COUNT(*) as count, COUNT(DISTINCT steam_app_id) as games FROM steam_raw"
            )
            total_records = int(count_result["count"][0])
            total_games_in_db = int(count_result["games"][0])

            click.echo("💾 Saved to steam_raw table")
            click.echo(f"   📊 Total records: {total_records:,}")
            click.echo(f"   🎮 Total games tracked: {total_games_in_db}")

        # Display summary of current collection
        click.echo("\n📊 Current collection summary:")
        sorted_data = sorted(games_data, key=lambda x: x["player_count"], reverse=True)
        for data in sorted_data[:10]:
            click.echo(f"  • {data['game_name']}: {data['player_count']:,} concurrent players")

        click.echo(f"\n✨ Steam collection complete! Data saved to {db_path}")

    except Exception as e:
        click.echo(f"❌ Error during Steam collection: {e}", err=True)
        raise click.Abort() from e


@collect.command()
@click.option(
    "--limit",
    "-l",
    default=None,
    help="Number of games to collect (default: all tracked games)",
    type=int,
)
@click.option(
    "--db-path",
    "-d",
    default="data/duckdb/gaming.db",
    help="Path to DuckDB database",
    type=click.Path(),
)
@click.option(
    "--delay",
    default=1.0,
    help="Delay between API requests in seconds (rate limiting)",
    type=float,
)
def twitch(limit: int | None, db_path: str, delay: float) -> None:
    """Collect Twitch viewership data for tracked games.

    Reads games from game_metadata table (requires twitch_game_id).
    Saves time-series data to twitch_raw table in DuckDB.
    """
    from python.collectors.twitch import TwitchCollector
    from python.storage.duckdb_manager import DuckDBManager

    db_path_obj = Path(db_path)

    click.echo("🎮 Collecting Twitch viewership data...\n")

    try:
        # Initialize collector
        collector = TwitchCollector(db_path=db_path_obj)

        tracked_games = collector.get_tracked_games()
        total_games = len(tracked_games)
        games_to_collect = limit if limit is not None else total_games

        click.echo(f"📊 Collecting data for {games_to_collect}/{total_games} tracked games...\n")

        # Collect data
        twitch_data = collector.collect_tracked_games(limit=limit, delay=delay)

        click.echo(f"\n✅ Collected data for {len(twitch_data)}/{games_to_collect} games")

        if not twitch_data:
            click.echo("⚠️  No data collected")
            return

        # Save to DuckDB
        with DuckDBManager(db_path=db_path_obj) as db:
            # Create twitch_raw table if not exists
            db.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS twitch_raw (
                    timestamp TIMESTAMP NOT NULL,
                    twitch_game_id VARCHAR NOT NULL,
                    game_name VARCHAR NOT NULL,
                    viewer_count INTEGER NOT NULL,
                    channel_count INTEGER NOT NULL,
                    PRIMARY KEY (timestamp, twitch_game_id)
                )
            """
            )

            # Insert collected data
            for data in twitch_data:
                db.conn.execute(
                    """
                    INSERT INTO twitch_raw (timestamp, twitch_game_id, game_name, viewer_count, channel_count)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (timestamp, twitch_game_id) DO UPDATE SET
                        viewer_count = EXCLUDED.viewer_count,
                        channel_count = EXCLUDED.channel_count
                    """,
                    [
                        data["timestamp"],
                        data["twitch_game_id"],
                        data["game_name"],
                        data["viewer_count"],
                        data["channel_count"],
                    ],
                )

            # Get stats
            count_result = db.query(
                "SELECT COUNT(*) as count, COUNT(DISTINCT twitch_game_id) as games FROM twitch_raw"
            )
            total_records = int(count_result["count"][0])
            total_games_in_db = int(count_result["games"][0])

            click.echo("💾 Saved to twitch_raw table")
            click.echo(f"   📊 Total records: {total_records:,}")
            click.echo(f"   🎮 Total games tracked: {total_games_in_db}")

        # Display summary of current collection
        click.echo("\n📊 Current collection summary:")
        sorted_data = sorted(twitch_data, key=lambda x: x["viewer_count"], reverse=True)
        for data in sorted_data[:10]:
            click.echo(
                f"  • {data['game_name']}: {data['viewer_count']:,} viewers, "
                f"{data['channel_count']} channels"
            )

        click.echo(f"\n✨ Twitch collection complete! Data saved to {db_path}")

    except ValueError as e:
        click.echo(f"❌ Authentication error: {e}", err=True)
        click.echo("\n💡 Make sure to set your Twitch credentials in .env file:", err=True)
        click.echo("   TWITCH_CLIENT_ID=your_client_id", err=True)
        click.echo("   TWITCH_CLIENT_SECRET=your_client_secret", err=True)
        click.echo("\n   Get credentials at: https://dev.twitch.tv/console", err=True)
        raise click.Abort() from e

    except Exception as e:
        click.echo(f"❌ Error during Twitch collection: {e}", err=True)
        raise click.Abort() from e


@collect.command()
@click.option(
    "--limit",
    "-l",
    default=None,
    help="Number of games to collect per source (default: all tracked games)",
    type=int,
)
@click.option(
    "--db-path",
    "-d",
    default="data/duckdb/gaming.db",
    help="Path to DuckDB database",
    type=click.Path(),
)
def all(limit: int | None, db_path: str) -> None:
    """Collect all KPIs from all sources (orchestrator).

    Runs all collection commands in sequence:
    1. Steam CCU
    2. Twitch viewership
    3. IGDB ratings

    Provides a summary report of all collections.
    """
    from python.collectors.igdb import IGDBCollector
    from python.collectors.steam import SteamCollector
    from python.collectors.twitch import TwitchCollector
    from python.storage.duckdb_manager import DuckDBManager

    db_path_obj = Path(db_path)

    click.echo("🚀 Starting full KPI collection from all sources...\n")
    click.echo("=" * 60)

    results: dict[str, dict[str, int | str | None]] = {
        "steam": {"collected": 0, "failed": 0, "error": None},
        "twitch": {"collected": 0, "failed": 0, "error": None},
        "igdb_ratings": {"collected": 0, "failed": 0, "error": None},
    }

    # 1. Collect Steam CCU
    click.echo("\n📊 [1/3] Collecting Steam CCU data...")
    click.echo("-" * 60)
    try:
        collector = SteamCollector(db_path=db_path_obj)
        games_data = collector.collect_top_games(limit=limit)

        if games_data:
            with DuckDBManager(db_path=db_path_obj) as db:
                db.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS steam_raw (
                        timestamp TIMESTAMP NOT NULL,
                        steam_app_id INTEGER NOT NULL,
                        game_name VARCHAR NOT NULL,
                        player_count INTEGER NOT NULL,
                        PRIMARY KEY (timestamp, steam_app_id)
                    )
                """
                )

                for data in games_data:
                    db.conn.execute(
                        """
                        INSERT INTO steam_raw (timestamp, steam_app_id, game_name, player_count)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT (timestamp, steam_app_id) DO UPDATE SET
                            player_count = EXCLUDED.player_count
                        """,
                        [
                            data["timestamp"],
                            data["app_id"],
                            data["game_name"],
                            data["player_count"],
                        ],
                    )

            results["steam"]["collected"] = len(games_data)
            click.echo(f"✅ Steam: {len(games_data)} games collected")
        else:
            click.echo("⚠️  Steam: No data collected")

    except Exception as e:
        results["steam"]["error"] = str(e)
        click.echo(f"❌ Steam collection failed: {e}")

    # 2. Collect Twitch viewership
    click.echo("\n📊 [2/3] Collecting Twitch viewership data...")
    click.echo("-" * 60)
    try:
        collector_twitch = TwitchCollector(db_path=db_path_obj)
        twitch_data = collector_twitch.collect_tracked_games(limit=limit, delay=1.0)

        if twitch_data:
            with DuckDBManager(db_path=db_path_obj) as db:
                db.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS twitch_raw (
                        timestamp TIMESTAMP NOT NULL,
                        twitch_game_id VARCHAR NOT NULL,
                        game_name VARCHAR NOT NULL,
                        viewer_count INTEGER NOT NULL,
                        channel_count INTEGER NOT NULL,
                        PRIMARY KEY (timestamp, twitch_game_id)
                    )
                """
                )

                for data in twitch_data:
                    db.conn.execute(
                        """
                        INSERT INTO twitch_raw (timestamp, twitch_game_id, game_name, viewer_count, channel_count)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (timestamp, twitch_game_id) DO UPDATE SET
                            viewer_count = EXCLUDED.viewer_count,
                            channel_count = EXCLUDED.channel_count
                        """,
                        [
                            data["timestamp"],
                            data["twitch_game_id"],
                            data["game_name"],
                            data["viewer_count"],
                            data["channel_count"],
                        ],
                    )

            results["twitch"]["collected"] = len(twitch_data)
            click.echo(f"✅ Twitch: {len(twitch_data)} games collected")
        else:
            click.echo("⚠️  Twitch: No data collected")

    except Exception as e:
        results["twitch"]["error"] = str(e)
        click.echo(f"❌ Twitch collection failed: {e}")

    # 3. Collect IGDB ratings
    click.echo("\n📊 [3/3] Collecting IGDB ratings data...")
    click.echo("-" * 60)
    try:
        import time

        with DuckDBManager(db_path=db_path_obj) as db:
            games_df = db.query(
                f"""
                SELECT igdb_id, game_name
                FROM game_metadata
                ORDER BY igdb_id
                {"LIMIT " + str(limit) if limit else ""}
                """
            )
            games = games_df.to_dict("records")

            if games:
                collector_igdb = IGDBCollector()

                db.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS igdb_ratings_raw (
                        timestamp TIMESTAMP NOT NULL,
                        igdb_id INTEGER NOT NULL,
                        rating FLOAT,
                        aggregated_rating FLOAT,
                        total_rating_count INTEGER,
                        PRIMARY KEY (timestamp, igdb_id)
                    )
                """
                )

                collected_count = 0
                for game in games:
                    igdb_id = int(game["igdb_id"])
                    try:
                        ratings_data = collector_igdb.get_game_ratings(igdb_id)
                        if ratings_data:
                            db.conn.execute(
                                """
                                INSERT INTO igdb_ratings_raw (timestamp, igdb_id, rating, aggregated_rating, total_rating_count)
                                VALUES (?, ?, ?, ?, ?)
                                ON CONFLICT (timestamp, igdb_id) DO UPDATE SET
                                    rating = EXCLUDED.rating,
                                    aggregated_rating = EXCLUDED.aggregated_rating,
                                    total_rating_count = EXCLUDED.total_rating_count
                                """,
                                [
                                    ratings_data["timestamp"],
                                    ratings_data["igdb_id"],
                                    ratings_data["rating"],
                                    ratings_data["aggregated_rating"],
                                    ratings_data["total_rating_count"],
                                ],
                            )
                            collected_count += 1
                        time.sleep(0.5)
                    except Exception:
                        continue

                results["igdb_ratings"]["collected"] = collected_count
                click.echo(f"✅ IGDB Ratings: {collected_count} games collected")
            else:
                click.echo("⚠️  IGDB Ratings: No games in metadata table")

    except Exception as e:
        results["igdb_ratings"]["error"] = str(e)
        click.echo(f"❌ IGDB ratings collection failed: {e}")

    # Summary report
    click.echo("\n" + "=" * 60)
    click.echo("📊 COLLECTION SUMMARY")
    click.echo("=" * 60)

    total_collected = sum(int(r["collected"]) for r in results.values())  # type: ignore[arg-type]
    total_errors = sum(1 for r in results.values() if r["error"])

    click.echo("\n🎮 Steam CCU:")
    click.echo(f"   ✅ Collected: {results['steam']['collected']}")
    if results["steam"]["error"]:
        click.echo(f"   ❌ Error: {results['steam']['error']}")

    click.echo("\n📺 Twitch Viewership:")
    click.echo(f"   ✅ Collected: {results['twitch']['collected']}")
    if results["twitch"]["error"]:
        click.echo(f"   ❌ Error: {results['twitch']['error']}")

    click.echo("\n⭐ IGDB Ratings:")
    click.echo(f"   ✅ Collected: {results['igdb_ratings']['collected']}")
    if results["igdb_ratings"]["error"]:
        click.echo(f"   ❌ Error: {results['igdb_ratings']['error']}")

    click.echo("\n" + "=" * 60)
    click.echo(f"✨ Total data points collected: {total_collected}")
    if total_errors > 0:
        click.echo(f"⚠️  Total sources with errors: {total_errors}")
    click.echo(f"💾 Database: {db_path}")
    click.echo("=" * 60 + "\n")


@collect.command(name="igdb-ratings")
@click.option(
    "--limit",
    "-l",
    default=None,
    help="Number of games to collect (default: all games with metadata)",
    type=int,
)
@click.option(
    "--db-path",
    "-d",
    default="data/duckdb/gaming.db",
    help="Path to DuckDB database",
    type=click.Path(),
)
@click.option(
    "--delay",
    default=0.5,
    help="Delay between API requests in seconds (rate limiting)",
    type=float,
)
def igdb_ratings(limit: int | None, db_path: str, delay: float) -> None:
    """Collect IGDB ratings for tracked games (time-series KPI).

    Reads games from game_metadata table (all games with igdb_id).
    Saves time-series ratings data to igdb_ratings_raw table in DuckDB.

    Note: Ratings evolve over time as more users rate games.
    """
    import time

    from python.collectors.igdb import IGDBCollector
    from python.storage.duckdb_manager import DuckDBManager

    db_path_obj = Path(db_path)

    click.echo("🎮 Collecting IGDB ratings data...\n")

    try:
        with DuckDBManager(db_path=db_path_obj) as db:
            # Get all games with metadata
            games_df = db.query(
                f"""
                SELECT igdb_id, game_name
                FROM game_metadata
                ORDER BY igdb_id
                {"LIMIT " + str(limit) if limit else ""}
                """
            )

            # Convert DataFrame to list of dicts
            games = games_df.to_dict("records")

            total_games = len(games)
            click.echo(f"📊 Collecting ratings for {total_games} games...\n")

            if total_games == 0:
                click.echo("⚠️  No games found in game_metadata table")
                return

            # Initialize collector
            collector = IGDBCollector()

            # Create igdb_ratings_raw table if not exists
            db.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS igdb_ratings_raw (
                    timestamp TIMESTAMP NOT NULL,
                    igdb_id INTEGER NOT NULL,
                    rating FLOAT,
                    aggregated_rating FLOAT,
                    total_rating_count INTEGER,
                    PRIMARY KEY (timestamp, igdb_id)
                )
            """
            )

            collected_count = 0
            failed_count = 0

            # Collect ratings for each game
            for i, game in enumerate(games, 1):
                igdb_id = int(game["igdb_id"])
                game_name = game["game_name"]

                try:
                    ratings_data = collector.get_game_ratings(igdb_id)

                    if ratings_data:
                        # Insert into database
                        db.conn.execute(
                            """
                            INSERT INTO igdb_ratings_raw (timestamp, igdb_id, rating, aggregated_rating, total_rating_count)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT (timestamp, igdb_id) DO UPDATE SET
                                rating = EXCLUDED.rating,
                                aggregated_rating = EXCLUDED.aggregated_rating,
                                total_rating_count = EXCLUDED.total_rating_count
                            """,
                            [
                                ratings_data["timestamp"],
                                ratings_data["igdb_id"],
                                ratings_data["rating"],
                                ratings_data["aggregated_rating"],
                                ratings_data["total_rating_count"],
                            ],
                        )

                        collected_count += 1
                        click.echo(
                            f"[{i}/{total_games}] ✓ {game_name}: "
                            f"Rating: {ratings_data.get('rating') or 'N/A'}, "
                            f"Aggregated: {ratings_data.get('aggregated_rating') or 'N/A'}, "
                            f"Count: {ratings_data.get('total_rating_count') or 0}"
                        )
                    else:
                        failed_count += 1
                        click.echo(f"[{i}/{total_games}] ✗ {game_name}: No ratings data")

                except Exception as e:
                    failed_count += 1
                    click.echo(f"[{i}/{total_games}] ⚠️  {game_name}: {e}")
                    continue

                # Rate limiting
                if delay > 0 and i < total_games:
                    time.sleep(delay)

            # Get stats
            count_result = db.query(
                "SELECT COUNT(*) as count, COUNT(DISTINCT igdb_id) as games FROM igdb_ratings_raw"
            )
            total_records = int(count_result["count"][0])
            total_games_in_db = int(count_result["games"][0])

            click.echo("\n💾 Saved to igdb_ratings_raw table")
            click.echo(f"   📊 Total records: {total_records:,}")
            click.echo(f"   🎮 Total games tracked: {total_games_in_db}")
            click.echo(f"   ✅ Collected: {collected_count}")
            click.echo(f"   ⚠️  Failed: {failed_count}")

        click.echo(f"\n✨ IGDB ratings collection complete! Data saved to {db_path}")

    except ValueError as e:
        click.echo(f"❌ Authentication error: {e}", err=True)
        click.echo("\n💡 Make sure to set your Twitch credentials in .env file:", err=True)
        click.echo("   TWITCH_CLIENT_ID=your_client_id", err=True)
        click.echo("   TWITCH_CLIENT_SECRET=your_client_secret", err=True)
        raise click.Abort() from e

    except Exception as e:
        click.echo(f"❌ Error during IGDB ratings collection: {e}", err=True)
        raise click.Abort() from e


@cli.command()
def process() -> None:
    """Process and clean collected data."""
    click.echo("Processing data...")


@cli.command()
@click.option(
    "--db-path",
    "-d",
    default="data/duckdb/gaming.db",
    help="Path to DuckDB database",
    type=click.Path(),
)
@click.option(
    "--parquet-path",
    "-p",
    default="data/raw/steam",
    help="Path to Parquet files directory",
    type=click.Path(),
)
def store(db_path: str, parquet_path: str) -> None:
    """Load Parquet files into DuckDB."""
    click.echo("📦 Loading Parquet files into DuckDB...")

    from python.storage.duckdb_manager import DuckDBManager

    db_path_obj = Path(db_path)
    parquet_path_obj = Path(parquet_path)

    # Find all parquet files
    parquet_files = list(parquet_path_obj.rglob("*.parquet"))

    if not parquet_files:
        click.echo(f"⚠️  No Parquet files found in {parquet_path}", err=True)
        return

    click.echo(f"📁 Found {len(parquet_files)} Parquet files")

    try:
        with DuckDBManager(db_path=db_path_obj) as db:
            # Create table from Parquet schema if it doesn't exist
            db.query(
                """
                CREATE TABLE IF NOT EXISTS steam_raw AS
                SELECT * FROM read_parquet('data/raw/steam/**/*.parquet')
            """
            )

            # Insert new data (avoiding duplicates)
            db.query(
                """
                INSERT INTO steam_raw
                SELECT * FROM read_parquet('data/raw/steam/**/*.parquet')
                WHERE NOT EXISTS (
                    SELECT 1 FROM steam_raw s
                    WHERE s.timestamp = read_parquet.timestamp
                    AND s.app_id = read_parquet.app_id
                )
            """
            )

            # Get stats
            count_result = db.query(
                "SELECT COUNT(*) as count, COUNT(DISTINCT app_id) as games FROM steam_raw"
            )
            total_records = count_result["count"][0]
            total_games = count_result["games"][0]

            click.echo(f"✅ Loaded {total_records:,} records for {total_games} games into DuckDB")
            click.echo(f"💾 Database: {db_path}")

    except Exception as e:
        click.echo(f"❌ Error loading data into DuckDB: {e}", err=True)
        raise click.Abort() from e


@cli.command()
@click.option(
    "--db-path",
    "-d",
    default="data/duckdb/gaming.db",
    help="Path to DuckDB database",
    type=click.Path(),
)
@click.option(
    "--output-dir",
    "-o",
    default="src/data",
    help="Output directory for JSON exports",
    type=click.Path(),
)
def aggregate(db_path: str, output_dir: str) -> None:
    """Calculate KPIs and export to JSON for dashboards."""
    click.echo("📊 Aggregating KPIs from DuckDB...")

    db_path_obj = Path(db_path)
    output_dir_obj = Path(output_dir)

    if not db_path_obj.exists():
        click.echo(f"❌ Database not found: {db_path}", err=True)
        raise click.Abort()

    try:
        with KPIAggregator(db_path=db_path_obj) as aggregator:
            aggregator.run_full_aggregation(output_dir=output_dir_obj)

        click.echo(f"✅ KPIs aggregated and exported to {output_dir}/")
        click.echo("  • game-metadata.json")
        click.echo("  • game_rankings.json")
        click.echo("  • daily_kpis.json")
        click.echo("  • latest_kpis.json (last 7 days)")
        click.echo("  • weekly_kpis.json")
        click.echo("  • monthly_kpis.json")

    except Exception as e:
        click.echo(f"❌ Error during aggregation: {e}", err=True)
        raise click.Abort() from e


@cli.command()
@click.option(
    "--full-refresh",
    "-f",
    is_flag=True,
    help="Refresh metadata for all games (not just new ones)",
)
@click.option(
    "--limit",
    "-l",
    default=None,
    help="Limit number of games to enrich (default: all pending)",
    type=int,
)
@click.option(
    "--db-path",
    "-d",
    default="data/duckdb/gaming.db",
    help="Path to DuckDB database",
    type=click.Path(),
)
@click.option(
    "--delay",
    default=0.5,
    help="Delay between API calls (rate limiting)",
    type=float,
)
def metadata(full_refresh: bool, limit: int | None, db_path: str, delay: float) -> None:
    """Enrich games with metadata from all platforms.

    Collects metadata for games in game_list that haven't been enriched yet.
    Enriches with data from IGDB, Steam, Twitch, and other platforms.

    Use --full-refresh to re-collect metadata for all games.
    """
    import time

    from python.collectors.igdb import IGDBCollector
    from python.storage.duckdb_manager import DuckDBManager

    db_path_obj = Path(db_path)

    try:
        with DuckDBManager(db_path=db_path_obj) as db:
            # Create tables
            db.create_game_list_table()
            db.create_game_metadata_table()

            # Get games to enrich
            if full_refresh:
                games_to_enrich = db.get_all_games_for_metadata_refresh()
                click.echo("🔄 Full refresh: enriching ALL games with metadata...\n")
            else:
                games_to_enrich = db.get_games_needing_metadata(limit=limit)
                click.echo(f"📦 Enriching {len(games_to_enrich)} games with metadata...\n")

            if not games_to_enrich:
                click.echo("✅ No games need metadata enrichment!")
                return

            # Initialize collector
            collector = IGDBCollector()

            enriched_count = 0
            failed_count = 0

            for i, game in enumerate(games_to_enrich, 1):
                igdb_id = game["igdb_id"]
                game_name = game["game_name"]

                click.echo(f"[{i}/{len(games_to_enrich)}] Enriching: {game_name} (IGDB: {igdb_id})")

                try:
                    # Enrich with IGDB + external IDs
                    enriched = collector.enrich_game(igdb_id)

                    if enriched:
                        # Upsert into game_metadata
                        db.upsert_game_metadata(enriched)

                        # Mark as collected in game_list
                        db.mark_metadata_collected(igdb_id)

                        enriched_count += 1
                        click.echo(
                            f"  ✅ Steam: {enriched.get('steam_app_id') or 'N/A'}, "
                            f"Twitch: {enriched.get('twitch_game_id') or 'N/A'}"
                        )
                    else:
                        failed_count += 1
                        click.echo("  ❌ Failed to enrich")

                except Exception as e:
                    failed_count += 1
                    click.echo(f"  ⚠️  Error: {e}")
                    continue

                # Rate limiting
                if delay > 0 and i < len(games_to_enrich):
                    time.sleep(delay)

        click.echo("\n✅ Metadata enrichment complete!")
        click.echo(f"   ✅ {enriched_count} games enriched successfully")
        if failed_count > 0:
            click.echo(f"   ⚠️  {failed_count} games failed")
        click.echo(f"   💾 Database: {db_path}")

    except Exception as e:
        click.echo(f"❌ Error during metadata enrichment: {e}", err=True)
        raise click.Abort() from e


@cli.command()
@click.option(
    "--source",
    "-s",
    default="igdb-popular",
    help="Discovery source (igdb-popular, igdb-recent, steam-top-ccu, etc.)",
    type=str,
)
@click.option(
    "--limit",
    "-l",
    default=100,
    help="Number of games to discover",
    type=int,
)
@click.option(
    "--db-path",
    "-d",
    default="data/duckdb/gaming.db",
    help="Path to DuckDB database",
    type=click.Path(),
)
def discover(source: str, limit: int, db_path: str) -> None:
    """Discover games and add to game list (without metadata enrichment).

    Decoupled discovery: finds games and adds them to game_list table.
    Use 'metadata' command to enrich discovered games with full metadata.

    Available sources:
    - igdb-popular: Popular games by rating count (default)
    - igdb-recent: Recently released games (last 90 days)
    - igdb-highest-rated: Highest rated games
    - igdb-upcoming: Upcoming games (next 180 days)
    - steam-top-ccu: Top concurrent players on Steam
    - twitch-trending: Trending games on Twitch by viewership
    """
    import time

    from python.collectors.igdb import IGDBCollector
    from python.collectors.steam import SteamCollector
    from python.collectors.twitch import TwitchCollector
    from python.storage.duckdb_manager import DuckDBManager

    click.echo(f"🔍 Discovering {limit} games from source: {source}...\n")

    db_path_obj = Path(db_path)
    start_time = time.time()

    try:
        discovered_games = []

        # IGDB sources
        if source == "igdb-popular":
            igdb_collector = IGDBCollector()
            discovered_games = igdb_collector.discover_popular_games(limit=limit)
        elif source == "igdb-recent":
            igdb_collector = IGDBCollector()
            discovered_games = igdb_collector.discover_recent_games(limit=limit, days_back=90)
        elif source == "igdb-highest-rated":
            igdb_collector = IGDBCollector()
            discovered_games = igdb_collector.discover_highest_rated_games(limit=limit)
        elif source == "igdb-upcoming":
            igdb_collector = IGDBCollector()
            discovered_games = igdb_collector.discover_upcoming_games(limit=limit, days_ahead=180)

        # Steam sources
        elif source == "steam-top-ccu":
            steam_collector = SteamCollector(db_path=db_path_obj)
            discovered_games = steam_collector.discover_top_ccu_games(limit=limit)

        # Twitch sources
        elif source == "twitch-trending":
            twitch_collector = TwitchCollector(db_path=db_path_obj)
            discovered_games = twitch_collector.discover_trending_games(limit=limit)

        else:
            click.echo(f"❌ Unknown source: {source}", err=True)
            click.echo("\nAvailable sources:", err=True)
            click.echo("  • igdb-popular: Popular games by rating count", err=True)
            click.echo("  • igdb-recent: Recently released games (last 90 days)", err=True)
            click.echo("  • igdb-highest-rated: Highest rated games", err=True)
            click.echo("  • igdb-upcoming: Upcoming games (next 180 days)", err=True)
            click.echo("  • steam-top-ccu: Top concurrent players on Steam", err=True)
            click.echo("  • twitch-trending: Trending games on Twitch", err=True)
            raise click.Abort()

        if not discovered_games:
            click.echo("❌ No games discovered", err=True)
            raise click.Abort()

        # Convert to simple format for game_list
        # Handle different return formats (IGDB vs Steam/Twitch)
        games_for_list = []
        for game in discovered_games:
            if "igdb_id" in game:
                # Already in correct format (Steam/Twitch sources)
                games_for_list.append({"igdb_id": game["igdb_id"], "game_name": game["game_name"]})
            else:
                # IGDB format: {"id": ..., "name": ...}
                games_for_list.append(
                    {"igdb_id": game["id"], "game_name": game.get("name", f"Game {game['id']}")}
                )

        click.echo(f"✅ Discovered {len(games_for_list)} games\n")

        # Store in database
        with DuckDBManager(db_path=db_path_obj) as db:
            # Create tables
            db.create_game_list_table()
            db.create_discovery_history_table()

            # Insert discovered games
            new_count, skipped_count = db.insert_discovered_games(games_for_list, source)

            # Log discovery operation
            execution_time = time.time() - start_time
            db.log_discovery(
                source=source,
                games_discovered=new_count,
                games_updated=0,
                execution_time=execution_time,
                notes=f"Discovered {limit} games from {source}",
            )

        click.echo("\n✅ Discovery complete!")
        click.echo(f"   📊 {new_count} new games added to game_list")
        click.echo(f"   ⏭️  {skipped_count} games already in list")
        click.echo(f"   ⏱️  Completed in {execution_time:.1f}s")
        click.echo(f"   💾 Database: {db_path}")

        # Display sample
        click.echo("\n🎯 Sample of discovered games:")
        for game in games_for_list[:5]:
            click.echo(f"  • {game['game_name']} (IGDB ID: {game['igdb_id']})")

        if new_count > 0:
            click.echo(
                f"\n💡 Run 'metadata' command to enrich {new_count} new games with full metadata"
            )

    except ValueError as e:
        click.echo(f"❌ Authentication error: {e}", err=True)
        click.echo("\n💡 Make sure to set your Twitch credentials in .env file:", err=True)
        click.echo("   TWITCH_CLIENT_ID=your_client_id", err=True)
        click.echo("   TWITCH_CLIENT_SECRET=your_client_secret", err=True)
        click.echo("\n   Get credentials at: https://dev.twitch.tv/console", err=True)
        raise click.Abort() from e

    except Exception as e:
        click.echo(f"❌ Error during discovery: {e}", err=True)
        raise click.Abort() from e


@cli.command()
def forecast() -> None:
    """Generate 14-day forecasts using Prophet."""
    click.echo("Generating forecasts...")


@cli.command()
def export() -> None:
    """Export data to JSON for Observable Framework."""
    click.echo("Exporting data...")


if __name__ == "__main__":
    cli()
