"""Gaming Data Observatory - Main CLI entrypoint."""

import click


@click.group()
def cli() -> None:
    """Gaming Data Observatory - Data pipeline for Steam, Twitch, Reddit."""
    pass


@cli.command()
def collect() -> None:
    """Collect data from APIs (Steam, Twitch, Reddit)."""
    click.echo("Collecting data...")


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
