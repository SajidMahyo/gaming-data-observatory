"""Tests for main CLI module."""

from pathlib import Path
from unittest.mock import Mock, patch

from click.testing import CliRunner

from python.main import cli


def test_cli_help() -> None:
    """Test that CLI help command works."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Gaming Data Observatory" in result.output


def test_collect_command_help() -> None:
    """Test collect command help."""
    runner = CliRunner()
    result = runner.invoke(cli, ["collect", "--help"])
    assert result.exit_code == 0
    assert "Collect data from Steam API" in result.output
    assert "--output" in result.output
    assert "--limit" in result.output


def test_collect_command_with_mocked_data(tmp_path: Path) -> None:
    """Test collect command with mocked collector."""
    runner = CliRunner()

    with patch("python.main.SteamCollector") as mock_collector_class:
        with patch("python.main.ParquetWriter") as mock_writer_class:
            # Setup mocks
            mock_collector = Mock()
            mock_collector.get_top_games.return_value = {730: "Counter-Strike 2"}
            mock_collector.collect_top_games.return_value = [
                {
                    "app_id": 730,
                    "game_name": "Counter-Strike 2",
                    "player_count": 1050028,
                    "timestamp": "2025-10-22T14:00:00+00:00",
                }
            ]
            mock_collector_class.return_value = mock_collector

            mock_writer = Mock()
            mock_writer_class.return_value = mock_writer

            # Run command
            result = runner.invoke(cli, ["collect", "--output", str(tmp_path), "--limit", "1"])

            # Verify
            assert result.exit_code == 0
            assert "Collecting data for 1/1 tracked games" in result.output
            assert "Counter-Strike 2" in result.output
            assert "1,050,028" in result.output
            mock_collector.collect_top_games.assert_called_once_with(limit=1)
            mock_writer.save.assert_called_once()


def test_collect_command_handles_errors() -> None:
    """Test collect command handles errors gracefully."""
    runner = CliRunner()

    with patch("python.main.SteamCollector") as mock_collector_class:
        mock_collector = Mock()
        mock_collector.get_top_games.return_value = {730: "Counter-Strike 2"}
        mock_collector.collect_top_games.side_effect = Exception("API Error")
        mock_collector_class.return_value = mock_collector

        result = runner.invoke(cli, ["collect", "--limit", "1"])

        assert result.exit_code == 1
        assert "Error during collection" in result.output


def test_process_command() -> None:
    """Test process command runs without error."""
    runner = CliRunner()
    result = runner.invoke(cli, ["process"])
    assert result.exit_code == 0
    assert "Processing data" in result.output


def test_aggregate_command_help() -> None:
    """Test aggregate command help."""
    runner = CliRunner()
    result = runner.invoke(cli, ["aggregate", "--help"])
    assert result.exit_code == 0
    assert "Calculate KPIs" in result.output
    assert "--db-path" in result.output
    assert "--output-dir" in result.output


def test_store_command_help() -> None:
    """Test store command help."""
    runner = CliRunner()
    result = runner.invoke(cli, ["store", "--help"])
    assert result.exit_code == 0
    assert "Load Parquet files into DuckDB" in result.output


def test_discover_command_help() -> None:
    """Test discover command help."""
    runner = CliRunner()
    result = runner.invoke(cli, ["discover", "--help"])
    assert result.exit_code == 0
    assert "Discover games from IGDB" in result.output
    assert "--limit" in result.output
    assert "--db-path" in result.output
    assert "--delay" in result.output


def test_discover_command_with_mocked_data(tmp_path: Path) -> None:
    """Test discover command with mocked IGDB collector."""
    runner = CliRunner()
    db_path = tmp_path / "test.db"

    with (
        patch("python.collectors.igdb.IGDBCollector") as mock_collector_class,
        patch("python.storage.duckdb_manager.DuckDBManager") as mock_db_class,
    ):
        # Mock IGDBCollector
        mock_collector = Mock()
        mock_collector.discover_and_enrich.return_value = [
            {
                "igdb_id": 1234,
                "game_name": "Counter-Strike 2",
                "steam_app_id": 730,
                "twitch_game_id": "32399",
            },
            {
                "igdb_id": 2963,
                "game_name": "Dota 2",
                "steam_app_id": 570,
                "twitch_game_id": "29595",
            },
        ]
        mock_collector_class.return_value = mock_collector

        # Mock DuckDBManager
        mock_db = Mock()
        mock_db.get_game_metadata.return_value = None  # All games are new
        mock_db.__enter__ = Mock(return_value=mock_db)
        mock_db.__exit__ = Mock(return_value=False)
        mock_db_class.return_value = mock_db

        result = runner.invoke(cli, ["discover", "--limit", "2", "--db-path", str(db_path)])

        assert result.exit_code == 0
        assert "Discovery complete" in result.output
        assert "2 new games discovered" in result.output
        mock_collector.discover_and_enrich.assert_called_once_with(limit=2, delay=0.5)


def test_metadata_command_help() -> None:
    """Test metadata command help."""
    runner = CliRunner()
    result = runner.invoke(cli, ["metadata", "--help"])
    assert result.exit_code == 0
    assert "Collect game metadata" in result.output
    assert "--app-ids" in result.output


def test_store_command_with_mocked_data(tmp_path: Path) -> None:
    """Test store command with mocked DuckDB."""
    runner = CliRunner()

    with patch("python.storage.duckdb_manager.DuckDBManager") as mock_db_class:
        mock_db = Mock()
        mock_db.query.return_value = {"count": [100], "games": [10]}
        mock_db.__enter__ = Mock(return_value=mock_db)
        mock_db.__exit__ = Mock(return_value=False)
        mock_db_class.return_value = mock_db

        # Create a dummy parquet file
        parquet_dir = tmp_path / "parquet"
        parquet_dir.mkdir()
        (parquet_dir / "test.parquet").touch()

        result = runner.invoke(
            cli,
            ["store", "--db-path", str(tmp_path / "test.db"), "--parquet-path", str(parquet_dir)],
        )

        assert result.exit_code == 0
        assert "Loading Parquet files into DuckDB" in result.output
