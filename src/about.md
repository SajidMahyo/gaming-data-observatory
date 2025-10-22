# About Gaming Data Observatory

## 🎯 Project Overview

The **Gaming Data Observatory** is a comprehensive data engineering portfolio project that demonstrates:

- **Automated Data Collection**: Hourly collection of Steam player statistics via GitHub Actions
- **Modern Data Stack**: Python 3.13, DuckDB, Parquet, Pandas
- **Interactive Visualization**: Observable Framework with Plot.js
- **MLOps**: Forecasting with Prophet (coming soon)
- **DevOps Best Practices**: CI/CD, TDD (94% coverage), Git LFS

## 🏗️ Architecture

\`\`\`
┌─────────────────┐
│  Steam API      │
│  (Hourly)       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Parquet Files  │  ←── Git LFS Storage
│  (30-day)       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  DuckDB         │  ←── Analytics Engine
│  (Historical)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  JSON Exports   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Observable     │  ←── You are here!
│  Dashboard      │
└─────────────────┘
\`\`\`

## 🛠️ Tech Stack

### Backend
- **Python 3.13** with UV for dependency management
- **DuckDB** for analytical queries
- **Parquet** for columnar storage (partitioned by date and game)
- **Git LFS** for large file storage

### Testing & Quality
- **Pytest** with 94% coverage
- **Black**, **Ruff**, **Mypy** for code quality
- **Pre-commit hooks** for automated checks
- **TDD approach** for all features

### Frontend
- **Observable Framework** for reactive dashboards
- **Plot.js** for data visualization
- **GitHub Pages** for hosting

### DevOps
- **GitHub Actions** for CI/CD
  - Hourly data collection
  - Weekly aggregation and deployment
  - Automated testing on PRs

## 📊 Data Pipeline

1. **Collect** (Hourly): Fetch player counts from Steam API
2. **Store**: Save to partitioned Parquet files
3. **Load**: Insert into DuckDB for analytics
4. **Cleanup**: Remove files older than 30 days
5. **Aggregate** (Weekly): Calculate KPIs and rankings
6. **Export**: Generate JSON files for dashboard
7. **Deploy**: Build and publish to GitHub Pages

## 🎮 Tracked Metrics

- **CCU** (Concurrent Users): Real-time player counts
- **Peak CCU**: Maximum players in a time period
- **Average CCU**: Mean player count over time
- **Daily/Weekly Trends**: Player count evolution

## 🔮 Roadmap

- [x] Steam data collection
- [x] DuckDB storage and analytics
- [x] Git LFS configuration
- [x] GitHub Actions automation
- [x] Observable Framework dashboard
- [ ] Prophet forecasting (14-day predictions)
- [ ] Hype Index calculation
- [ ] Twitch viewership data
- [ ] Reddit sentiment analysis

## 📝 Source Code

This project is open source and available on GitHub:
[SajidMahyo/gaming-data-observatory](https://github.com/SajidMahyo/gaming-data-observatory)

---

Built with ❤️ as a data engineering portfolio project.
