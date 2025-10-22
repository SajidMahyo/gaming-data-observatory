# Gaming Data Observatory - Task Runner
# Run `just` or `just --list` to see all available commands

# Show all available commands
default:
    @just --list

# ============================================================================
# 📦 SETUP
# ============================================================================

# Install all Python dependencies with UV
[group('setup')]
install:
    uv sync --all-extras

# Install pre-commit git hooks
[group('setup')]
setup-hooks:
    uv run pre-commit install

# Complete project setup (install dependencies + hooks)
[group('setup')]
setup: install setup-hooks
    @echo "✅ Project setup complete!"

# Update all dependencies to latest versions
[group('setup')]
update:
    uv sync --upgrade --all-extras

# ============================================================================
# 🧪 TESTING
# ============================================================================

# Run all tests with coverage report (requires 80%+ coverage)
[group('test')]
test:
    uv run pytest -v

# Run tests quickly without coverage report
[group('test')]
test-fast:
    uv run pytest -v --no-cov

# Run only unit tests
[group('test')]
test-unit:
    uv run pytest tests/unit/ -v

# Run only integration tests
[group('test')]
test-integration:
    uv run pytest tests/integration/ -v

# Run tests and open HTML coverage report in browser
[group('test')]
test-coverage:
    uv run pytest -v
    open htmlcov/index.html

# ============================================================================
# 🎨 CODE QUALITY
# ============================================================================

# Format code with Black (100 chars line length)
[group('quality')]
format:
    uv run black python/ tests/

# Lint code with Ruff
[group('quality')]
lint:
    uv run ruff check python/ tests/

# Auto-fix lint issues with Ruff
[group('quality')]
lint-fix:
    uv run ruff check --fix python/ tests/

# Type check with mypy (strict mode)
[group('quality')]
typecheck:
    uv run mypy python/

# Run all quality checks (format + lint + typecheck)
[group('quality')]
check: format lint typecheck
    @echo "✅ Code quality checks passed!"

# Run pre-commit hooks on all files
[group('quality')]
precommit:
    uv run pre-commit run --all-files

# ============================================================================
# 📊 DATA PIPELINE
# ============================================================================

# Collect Steam player data for top games (use --limit to specify number)
[group('data')]
collect:
    uv run python -m python.main collect

# ============================================================================
# 🚀 CLI
# ============================================================================

# Show detailed CLI help with all available commands
[group('cli')]
cli-help:
    uv run python -m python.main --help

# Test Steam API with a real call to CS2 endpoint
[group('cli')]
test-steam-api:
    @echo "Testing Steam API for CS2 (app_id: 730)..."
    @curl -s "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=730" | python -m json.tool

# ============================================================================
# 🔧 DEVELOPMENT
# ============================================================================

# Start Python REPL with project context
[group('dev')]
repl:
    uv run python

# Clean build artifacts, caches, and test outputs
[group('dev')]
clean:
    rm -rf .pytest_cache __pycache__ .mypy_cache htmlcov .coverage
    find . -type d -name "__pycache__" -exec rm -rf {} +
    find . -type f -name "*.pyc" -delete

# Clean collected data files (DANGEROUS: cannot be undone!)
[group('dev')]
clean-data:
    @echo "⚠️  This will delete all collected data!"
    @read -p "Are you sure? (y/N): " confirm && [ "$$confirm" = "y" ]
    rm -rf data/

# Full cleanup: artifacts + data
[group('dev')]
clean-all: clean clean-data
    @echo "✅ Full cleanup complete!"

# Show project statistics (files, lines of code)
[group('dev')]
stats:
    @echo "📊 Project Statistics"
    @echo "===================="
    @echo "Python files:"
    @find python -name "*.py" | wc -l
    @echo "Test files:"
    @find tests -name "*.py" | wc -l
    @echo "Total lines:"
    @find python tests -name "*.py" -exec wc -l {} + | tail -1

# ============================================================================
# 🔄 GIT
# ============================================================================

# Show git status
[group('git')]
status:
    git status

# Run tests and quality checks before pushing
[group('git')]
pre-push: test check
    @echo "✅ Ready to push!"

# ============================================================================
# 📚 DOCS
# ============================================================================

# Show documentation links and resources
[group('docs')]
docs:
    @echo "📚 Documentation"
    @echo "README: ./README.md"
    @echo "Architecture: ./CLAUDE.md"
