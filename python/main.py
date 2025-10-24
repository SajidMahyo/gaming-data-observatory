"""Gaming Data Observatory - Main CLI entrypoint."""

from pathlib import Path

import click

from python.collectors.game_discovery import GameDiscovery
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
    "--config",
    "-c",
    default="config/games.json",
    help="Path to games configuration file",
    type=click.Path(),
)
@click.option(
    "--top-limit",
    "-t",
    default=100,
    help="Number of top games by playtime to discover",
    type=int,
)
@click.option(
    "--trending-limit",
    "-r",
    default=50,
    help="Number of trending games by CCU to discover",
    type=int,
)
@click.option(
    "--skip-top",
    is_flag=True,
    help="Skip discovering top games by playtime",
)
@click.option(
    "--skip-trending",
    is_flag=True,
    help="Skip discovering trending games by CCU",
)
@click.option(
    "--skip-featured",
    is_flag=True,
    help="Skip discovering featured games from Steam",
)
def discover(
    config: str,
    top_limit: int,
    trending_limit: int,
    skip_top: bool,
    skip_trending: bool,
    skip_featured: bool,
) -> None:
    """Discover and add new games to track (append-only).

    Discovers games from multiple sources for diversity:
    - Top games by playtime (last 2 weeks)
    - Trending games by current concurrent players
    - Featured games from Steam Store
    """
    click.echo("🔍 Discovering new games from multiple sources...\n")

    config_path = Path(config)

    try:
        discovery = GameDiscovery(config_path=config_path)

        # Update tracked games (append-only)
        updated_games = discovery.update_tracked_games(
            include_top=not skip_top,
            include_trending=not skip_trending,
            include_featured=not skip_featured,
            top_limit=top_limit,
            trending_limit=trending_limit,
        )

        click.echo(f"\n✨ Discovery complete! Now tracking {len(updated_games)} games total")
        click.echo(f"💾 Updated config: {config_path}")

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
