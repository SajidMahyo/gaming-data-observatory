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
                        click.echo(f"  ✅ Steam: {enriched.get('steam_app_id') or 'N/A'}, "
                                   f"Twitch: {enriched.get('twitch_game_id') or 'N/A'}")
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
    """
    import time

    from python.collectors.igdb import IGDBCollector
    from python.storage.duckdb_manager import DuckDBManager

    click.echo(f"🔍 Discovering {limit} games from source: {source}...\n")

    db_path_obj = Path(db_path)
    start_time = time.time()

    try:
        # Initialize IGDB collector
        collector = IGDBCollector()

        # Discover games (without enrichment)
        if source == "igdb-popular":
            discovered_games = collector.discover_popular_games(limit=limit)
        elif source == "igdb-recent":
            discovered_games = collector.discover_recent_games(limit=limit, days_back=90)
        elif source == "igdb-highest-rated":
            discovered_games = collector.discover_highest_rated_games(limit=limit)
        elif source == "igdb-upcoming":
            discovered_games = collector.discover_upcoming_games(limit=limit, days_ahead=180)
        else:
            click.echo(f"❌ Unknown source: {source}", err=True)
            click.echo("\nAvailable sources:", err=True)
            click.echo("  • igdb-popular: Popular games by rating count", err=True)
            click.echo("  • igdb-recent: Recently released games (last 90 days)", err=True)
            click.echo("  • igdb-highest-rated: Highest rated games", err=True)
            click.echo("  • igdb-upcoming: Upcoming games (next 180 days)", err=True)
            raise click.Abort()

        if not discovered_games:
            click.echo("❌ No games discovered", err=True)
            raise click.Abort()

        # Convert to simple format for game_list
        games_for_list = [
            {"igdb_id": game["id"], "game_name": game.get("name", f"Game {game['id']}")}
            for game in discovered_games
        ]

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
            click.echo(f"\n💡 Run 'metadata' command to enrich {new_count} new games with full metadata")

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
