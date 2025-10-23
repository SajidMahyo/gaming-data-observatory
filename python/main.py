"""Gaming Data Observatory - Main CLI entrypoint."""

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
    default=10,
    help="Number of top games to collect",
    type=int,
)
def collect(output: str, limit: int) -> None:
    """Collect data from Steam API for top games."""
    click.echo(f"🎮 Collecting data for top {limit} Steam games...")

    # Initialize collector and writer
    collector = SteamCollector()
    writer = ParquetWriter(base_path=Path(output))

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
def store() -> None:
    """Store data in DuckDB and Parquet."""
    click.echo("Storing data...")


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
def forecast() -> None:
    """Generate 14-day forecasts using Prophet."""
    click.echo("Generating forecasts...")


@cli.command()
def export() -> None:
    """Export data to JSON for Observable Framework."""
    click.echo("Exporting data...")


if __name__ == "__main__":
    cli()
