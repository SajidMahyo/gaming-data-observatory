"""Gaming Data Observatory - Main CLI entrypoint."""

from pathlib import Path

import click

from python.collectors.steam import SteamCollector
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
def aggregate() -> None:
    """Calculate KPIs and Hype Index."""
    click.echo("Aggregating KPIs...")


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
