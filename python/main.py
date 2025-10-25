"""Gaming Data Observatory - Main CLI entrypoint."""

from datetime import UTC, datetime
from pathlib import Path

import click

from python.collectors.steam import SteamCollector
from python.collectors.steam_store import SteamStoreCollector
from python.processors.aggregator import KPIAggregator
from python.storage.parquet_writer import ParquetWriter


@click.group()
def cli() -> None:
    """Gaming Data Observatory - Data pipeline for Steam, Twitch, Reddit."""
    pass


@cli.command()
@click.option(
    "--output",
    "-o",
    default="data/raw/steam",
    help="Output directory for collected data",
    type=click.Path(),
)
@click.option(
    "--limit",
    "-l",
    default=None,
    help="Number of games to collect (default: all tracked games)",
    type=int,
)
def collect(output: str, limit: int | None) -> None:
    """Collect data from Steam API for tracked games."""
    # Initialize collector and writer
    collector = SteamCollector()
    writer = ParquetWriter(base_path=Path(output))

    total_games = len(collector.get_top_games())
    games_to_collect = limit if limit is not None else total_games

    click.echo(f"🎮 Collecting data for {games_to_collect}/{total_games} tracked games...")

    # Collect data
    try:
        games_data = collector.collect_top_games(limit=limit)
        click.echo(f"✅ Collected data for {len(games_data)} games")

        # Save to Parquet
        writer.save(games_data, partition_cols=["date", "game_id"])
        click.echo(f"💾 Saved to {output}/ (partitioned by date and game_id)")

        # Display summary
        for game in games_data:
            click.echo(f"  • {game['game_name']}: {game['player_count']:,} concurrent players")

        click.echo(f"\n✨ Collection complete! Data saved to {output}/")

    except Exception as e:
        click.echo(f"❌ Error during collection: {e}", err=True)
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
    "--app-ids",
    "-a",
    default="730,570,578080",
    help="Comma-separated Steam app IDs (default: CS2, Dota 2, PUBG)",
    type=str,
)
@click.option(
    "--output",
    "-o",
    default="data/raw/metadata",
    help="Output directory for metadata JSON files",
    type=click.Path(),
)
@click.option(
    "--db-path",
    "-d",
    default="data/duckdb/gaming.db",
    help="Path to DuckDB database",
    type=click.Path(),
)
def metadata(app_ids: str, output: str, db_path: str) -> None:
    """Collect game metadata from Steam Store and SteamSpy APIs."""
    click.echo("🎮 Collecting game metadata from Steam Store API...\n")

    # Parse app IDs
    try:
        game_ids = [int(app_id.strip()) for app_id in app_ids.split(",")]
    except ValueError:
        click.echo("❌ Invalid app IDs format. Use comma-separated integers.", err=True)
        raise click.Abort() from None

    # Initialize collector
    collector = SteamStoreCollector()
    output_path = Path(output)
    output_path.mkdir(parents=True, exist_ok=True)

    # Collect metadata
    try:
        click.echo(f"📡 Collecting metadata for {len(game_ids)} games...")
        metadata_list = collector.collect_top_games_metadata(game_ids, delay=1.5)

        if not metadata_list:
            click.echo("❌ No metadata collected", err=True)
            raise click.Abort()

        # Save each game's metadata to JSON
        import json

        for metadata in metadata_list:
            app_id = metadata["app_id"]
            json_path = output_path / f"{app_id}.json"
            with open(json_path, "w") as f:
                json.dump(metadata, f, indent=2)

        click.echo(f"\n✅ Successfully collected metadata for {len(metadata_list)} games")
        click.echo(f"💾 Saved to {output}/\n")

        # Save to DuckDB
        from python.storage.duckdb_manager import DuckDBManager

        db_path_obj = Path(db_path)
        with DuckDBManager(db_path=db_path_obj) as db:
            db.create_game_metadata_table()
            for metadata in metadata_list:
                db.upsert_game_metadata(metadata)

        click.echo(f"💾 Saved to DuckDB: {db_path}\n")

        # Display summary
        for metadata in metadata_list:
            click.echo(f"  📦 {metadata['name']} (ID: {metadata['app_id']})")
            click.echo(f"     • Type: {metadata['type']}")
            click.echo(f"     • Developer: {', '.join(metadata['developers'])}")
            click.echo(f"     • Genres: {', '.join(metadata['genres'])}")
            click.echo(f"     • Free: {'Yes' if metadata['is_free'] else 'No'}")
            if metadata.get("metacritic_score"):
                click.echo(f"     • Metacritic: {metadata['metacritic_score']}/100")
            click.echo(f"     • Tags: {len(metadata['tags'])} tags")
            click.echo()

        click.echo("✨ Metadata collection complete!")

    except Exception as e:
        click.echo(f"❌ Error during metadata collection: {e}", err=True)
        raise click.Abort() from e


@cli.command()
@click.option(
    "--limit",
    "-l",
    default=100,
    help="Number of popular games to discover from IGDB",
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
def discover(limit: int, db_path: str, delay: float) -> None:
    """Discover games from IGDB and enrich with metadata.

    Uses IGDB API as the universal source of truth for game discovery.
    Discovers popular games by rating count and enriches with:
    - IGDB metadata (ratings, genres, themes, cover art)
    - Platform IDs (Steam, Twitch, YouTube, Epic, GOG, etc.)
    - Stores everything in unified game_metadata table
    """
    import time

    from python.collectors.igdb import IGDBCollector
    from python.storage.duckdb_manager import DuckDBManager

    click.echo(f"🎮 Discovering {limit} popular games from IGDB...\n")

    db_path_obj = Path(db_path)

    start_time = time.time()
    games_discovered = 0
    games_updated = 0

    try:
        # Initialize IGDB collector
        collector = IGDBCollector()

        # Discover and enrich games
        enriched_games = collector.discover_and_enrich(limit=limit, delay=delay)

        if not enriched_games:
            click.echo("❌ No games discovered", err=True)
            raise click.Abort()

        click.echo(f"\n💾 Storing {len(enriched_games)} games in DuckDB...")

        # Store in database
        with DuckDBManager(db_path=db_path_obj) as db:
            # Create tables
            db.create_game_metadata_table()
            db.create_discovery_history_table()

            # Upsert each game
            for game in enriched_games:
                # Check if game already exists
                existing = db.get_game_metadata(igdb_id=game["igdb_id"])

                if existing:
                    games_updated += 1
                else:
                    games_discovered += 1

                db.upsert_game_metadata(game)

            # Log discovery operation
            execution_time = time.time() - start_time
            db.log_discovery(
                source="igdb_popular",
                games_discovered=games_discovered,
                games_updated=games_updated,
                execution_time=execution_time,
                notes=f"Discovered top {limit} games by rating count",
            )

        click.echo("\n✅ Discovery complete!")
        click.echo(f"   📊 {games_discovered} new games discovered")
        click.echo(f"   🔄 {games_updated} existing games updated")
        click.echo(f"   ⏱️  Completed in {execution_time:.1f}s")
        click.echo(f"   💾 Database: {db_path}")

        # Display sample of discovered games
        click.echo("\n🎯 Sample of discovered games:")
        for game in enriched_games[:5]:
            steam_id = game.get("steam_app_id")
            twitch_id = game.get("twitch_game_id")
            click.echo(
                f"  • {game['game_name']} (IGDB: {game['igdb_id']}, "
                f"Steam: {steam_id or 'N/A'}, Twitch: {twitch_id or 'N/A'})"
            )

    except ValueError as e:
        click.echo(f"❌ Authentication error: {e}", err=True)
        click.echo("\n💡 Make sure to set your Twitch credentials in .env file:", err=True)
        click.echo("   TWITCH_CLIENT_ID=your_client_id", err=True)
        click.echo("   TWITCH_CLIENT_SECRET=your_client_secret", err=True)
        click.echo("\n   Get credentials at: https://dev.twitch.tv/console", err=True)
        raise click.Abort() from e

    except Exception as e:
        click.echo(f"❌ Error during IGDB discovery: {e}", err=True)
        raise click.Abort() from e


@cli.command()
def forecast() -> None:
    """Generate 14-day forecasts using Prophet."""
    click.echo("Generating forecasts...")


@cli.command()
def export() -> None:
    """Export data to JSON for Observable Framework."""
    click.echo("Exporting data...")


@cli.command()
@click.option(
    "--output",
    "-o",
    default="data/raw/twitch",
    help="Output directory for Twitch data",
    type=click.Path(),
)
@click.option(
    "--config",
    "-c",
    default="config/games.json",
    help="Path to games configuration file",
    type=click.Path(),
)
@click.option(
    "--limit",
    "-l",
    default=None,
    help="Number of games to collect (default: all tracked games)",
    type=int,
)
def twitch_collect(output: str, config: str, limit: int | None) -> None:
    """Collect viewership data from Twitch API for tracked games."""
    from python.collectors.twitch import TwitchCollector

    click.echo("🎮 Collecting Twitch viewership data...\n")

    # Load tracked games
    config_path = Path(config)
    if not config_path.exists():
        click.echo(f"❌ Config file not found: {config}", err=True)
        raise click.Abort()

    try:
        import json

        with open(config_path) as f:
            games_dict = json.load(f)
            games = {int(app_id): name for app_id, name in games_dict.items()}

        # Limit games if specified
        if limit is not None:
            games = dict(list(games.items())[:limit])

        total_games = len(games)
        click.echo(f"📊 Collecting data for {total_games} tracked games...\n")

        # Initialize collector
        collector = TwitchCollector()

        # Collect data
        twitch_data = collector.collect_multiple_games(games, delay=1.0)

        click.echo(f"\n✅ Collected data for {len(twitch_data)}/{total_games} games")

        # Save to JSON (for now, we'll add Parquet later)
        output_path = Path(output)
        output_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        output_file = output_path / f"twitch_data_{timestamp}.json"

        with open(output_file, "w") as f:
            json.dump(twitch_data, f, indent=2)

        click.echo(f"💾 Saved to {output_file}")

        # Display summary
        if twitch_data:
            click.echo("\n📊 Summary:")
            sorted_data = sorted(twitch_data, key=lambda x: x["viewer_count"], reverse=True)
            for data in sorted_data[:10]:
                click.echo(
                    f"  • {data['game_name']}: {data['viewer_count']:,} viewers, "
                    f"{data['channel_count']} channels"
                )

        click.echo("\n✨ Twitch collection complete!")

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


if __name__ == "__main__":
    cli()
