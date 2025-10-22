"""Tests for main CLI module."""

from click.testing import CliRunner

from python.main import cli


def test_cli_help() -> None:
    """Test that CLI help command works."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Gaming Data Observatory" in result.output


def test_collect_command() -> None:
    """Test collect command runs without error."""
    runner = CliRunner()
    result = runner.invoke(cli, ["collect"])
    assert result.exit_code == 0
    assert "Collecting data" in result.output


def test_process_command() -> None:
    """Test process command runs without error."""
    runner = CliRunner()
    result = runner.invoke(cli, ["process"])
    assert result.exit_code == 0
    assert "Processing data" in result.output
