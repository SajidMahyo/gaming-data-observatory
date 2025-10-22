# 🎮 Gaming Data Observatory

```js
// Load data
const latestKpis = FileAttachment("data/latest-kpis.json").json();
const gameRankings = FileAttachment("data/game-rankings.json").json();
```

```js
// Calculate summary stats
const totalGames = gameRankings.length;
const topGame = gameRankings.length > 0 ? gameRankings[0] : null;
const avgPeak = gameRankings.length > 0
  ? Math.round(gameRankings.reduce((sum, g) => sum + g.avg_peak, 0) / gameRankings.length)
  : 0;

// Helper function to get Steam image URL
function getSteamImage(appId) {
  return `https://cdn.cloudflare.steamstatic.com/steam/apps/${appId}/header.jpg`;
}
```

<div class="hero">
  ${topGame ? `
    <div class="hero-content">
      <div class="hero-text">
        <h1 class="hero-title">Top Game Right Now</h1>
        <h2 class="hero-game">${topGame.game_name}</h2>
        <div class="hero-stats">
          <div class="hero-stat">
            <span class="hero-stat-value">${topGame.avg_peak.toLocaleString()}</span>
            <span class="hero-stat-label">Avg Peak Players</span>
          </div>
          <div class="hero-stat">
            <span class="hero-stat-value">${topGame.all_time_peak.toLocaleString()}</span>
            <span class="hero-stat-label">All-Time Peak</span>
          </div>
        </div>
      </div>
      <img src="${getSteamImage(topGame.app_id)}" alt="${topGame.game_name}" class="hero-image" />
    </div>
  ` : '<p>Loading...</p>'}
</div>

---

## 📊 Key Metrics

<div class="metrics-grid">
  <div class="metric-card metric-blue">
    <div class="metric-icon">🎮</div>
    <h2 class="metric-value">${totalGames}</h2>
    <p class="metric-label">Games Tracked</p>
  </div>

  <div class="metric-card metric-green">
    <div class="metric-icon">👥</div>
    <h2 class="metric-value">${avgPeak.toLocaleString()}</h2>
    <p class="metric-label">Avg Peak Players</p>
  </div>

  <div class="metric-card metric-orange">
    <div class="metric-icon">📈</div>
    <h2 class="metric-value">${gameRankings.length > 0 ? gameRankings[0].days_tracked : 0}</h2>
    <p class="metric-label">Days Tracked</p>
  </div>
</div>

---

## 🏆 Top Games Rankings

<div class="rankings-grid">
  ${gameRankings.slice(0, 5).map((game, index) => `
    <div class="game-card rank-${index + 1}">
      <div class="game-rank">#${index + 1}</div>
      <img src="${getSteamImage(game.app_id)}" alt="${game.game_name}" class="game-image" />
      <div class="game-info">
        <h3 class="game-name">${game.game_name}</h3>
        <div class="game-stats-row">
          <div class="game-stat">
            <span class="stat-label">Avg Peak</span>
            <span class="stat-value">${game.avg_peak.toLocaleString()}</span>
          </div>
          <div class="game-stat">
            <span class="stat-label">All-Time Peak</span>
            <span class="stat-value">${game.all_time_peak.toLocaleString()}</span>
          </div>
        </div>
      </div>
    </div>
  `).join('')}
</div>

---

## 📈 Player Count Trends (Last 7 Days)

```js
// Group data by game for line chart
const gameColors = {
  "Counter-Strike 2": "#ff6b6b",
  "Dota 2": "#4ecdc4",
  "PUBG: BATTLEGROUNDS": "#ffe66d",
  "Apex Legends": "#a8dadc",
  "Grand Theft Auto V": "#f1faee"
};

Plot.plot({
  marginBottom: 60,
  height: 400,
  x: {
    label: "Date",
    type: "time"
  },
  y: {
    label: "Peak Concurrent Players",
    grid: true
  },
  color: {
    legend: true,
    domain: Object.keys(gameColors),
    range: Object.values(gameColors)
  },
  marks: [
    Plot.line(latestKpis, {
      x: "date",
      y: "peak_ccu",
      stroke: "game_name",
      strokeWidth: 2,
      tip: true
    }),
    Plot.dot(latestKpis, {
      x: "date",
      y: "peak_ccu",
      fill: "game_name",
      r: 3
    })
  ]
})
```

---

## 📅 Data Collection Info

<div class="note">
  <p><strong>📡 Data Collection:</strong> Automated hourly collection via GitHub Actions</p>
  <p><strong>💾 Storage:</strong> Parquet files (Git LFS) + DuckDB for analytics</p>
  <p><strong>🔄 Retention:</strong> 30-day rolling window for raw data</p>
  <p><strong>📊 Aggregation:</strong> Weekly KPI calculations and exports</p>
</div>

---

<style>
  /* Hero Section */
  .hero {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    border-radius: 24px;
    padding: 3rem;
    margin: 2rem 0;
    box-shadow: 0 20px 50px rgba(102, 126, 234, 0.3);
    overflow: hidden;
    position: relative;
  }

  .hero::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: linear-gradient(135deg, rgba(0,0,0,0.3) 0%, rgba(0,0,0,0.1) 100%);
    pointer-events: none;
  }

  .hero-content {
    display: flex;
    gap: 3rem;
    align-items: center;
    position: relative;
    z-index: 1;
  }

  .hero-text {
    flex: 1;
  }

  .hero-title {
    font-size: 1.5rem;
    color: rgba(255, 255, 255, 0.9);
    margin: 0 0 0.5rem 0;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 2px;
  }

  .hero-game {
    font-size: 3.5rem;
    color: white;
    margin: 0 0 2rem 0;
    font-weight: 900;
    text-shadow: 2px 2px 20px rgba(0, 0, 0, 0.5);
  }

  .hero-stats {
    display: flex;
    gap: 3rem;
  }

  .hero-stat {
    display: flex;
    flex-direction: column;
  }

  .hero-stat-value {
    font-size: 2.5rem;
    font-weight: 700;
    color: white;
    text-shadow: 2px 2px 10px rgba(0, 0, 0, 0.3);
  }

  .hero-stat-label {
    font-size: 0.9rem;
    color: rgba(255, 255, 255, 0.8);
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-top: 0.5rem;
  }

  .hero-image {
    width: 460px;
    height: auto;
    border-radius: 16px;
    box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
    transition: transform 0.3s ease;
  }

  .hero-image:hover {
    transform: scale(1.05) rotate(-2deg);
  }

  /* Metrics Grid */
  .metrics-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 1.5rem;
    margin: 2rem 0;
  }

  .metric-card {
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border-radius: 20px;
    padding: 2.5rem;
    text-align: center;
    position: relative;
    overflow: hidden;
    transition: all 0.3s ease;
    border: 1px solid #334155;
  }

  .metric-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 4px;
    background: linear-gradient(90deg, transparent, currentColor, transparent);
    opacity: 0;
    transition: opacity 0.3s ease;
  }

  .metric-card:hover {
    transform: translateY(-8px);
    box-shadow: 0 20px 40px rgba(0, 0, 0, 0.4);
  }

  .metric-card:hover::before {
    opacity: 1;
  }

  .metric-blue {
    color: #3b82f6;
  }

  .metric-green {
    color: #10b981;
  }

  .metric-orange {
    color: #f59e0b;
  }

  .metric-icon {
    font-size: 3rem;
    margin-bottom: 1rem;
    opacity: 0.3;
  }

  .metric-value {
    font-size: 3.5rem;
    margin: 0;
    color: currentColor;
    font-weight: 900;
  }

  .metric-label {
    color: #94a3b8;
    margin: 0.5rem 0 0 0;
    font-size: 0.95rem;
    text-transform: uppercase;
    letter-spacing: 1px;
    font-weight: 500;
  }

  /* Rankings Grid */
  .rankings-grid {
    display: grid;
    gap: 1.5rem;
    margin: 2rem 0;
  }

  .game-card {
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border-radius: 20px;
    padding: 1.5rem;
    display: flex;
    align-items: center;
    gap: 1.5rem;
    position: relative;
    overflow: hidden;
    transition: all 0.3s ease;
    border: 1px solid #334155;
  }

  .game-card::before {
    content: '';
    position: absolute;
    left: 0;
    top: 0;
    bottom: 0;
    width: 4px;
    background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
  }

  .game-card:hover {
    transform: translateX(8px);
    box-shadow: 0 10px 40px rgba(102, 126, 234, 0.3);
    border-color: #667eea;
  }

  .game-card.rank-1::before {
    background: linear-gradient(180deg, #ffd700 0%, #ffed4e 100%);
  }

  .game-card.rank-2::before {
    background: linear-gradient(180deg, #c0c0c0 0%, #e8e8e8 100%);
  }

  .game-card.rank-3::before {
    background: linear-gradient(180deg, #cd7f32 0%, #e8a87c 100%);
  }

  .game-rank {
    font-size: 2.5rem;
    font-weight: 900;
    color: #334155;
    min-width: 80px;
    text-align: center;
  }

  .game-card.rank-1 .game-rank {
    color: #ffd700;
    text-shadow: 0 0 20px rgba(255, 215, 0, 0.5);
  }

  .game-card.rank-2 .game-rank {
    color: #c0c0c0;
  }

  .game-card.rank-3 .game-rank {
    color: #cd7f32;
  }

  .game-image {
    width: 300px;
    height: auto;
    border-radius: 12px;
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
    transition: transform 0.3s ease;
  }

  .game-card:hover .game-image {
    transform: scale(1.05);
  }

  .game-info {
    flex: 1;
    display: flex;
    flex-direction: column;
    gap: 1rem;
  }

  .game-name {
    font-size: 1.8rem;
    margin: 0;
    color: #e2e8f0;
    font-weight: 700;
  }

  .game-stats-row {
    display: flex;
    gap: 3rem;
  }

  .game-stat {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .stat-label {
    font-size: 0.85rem;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 1px;
    font-weight: 600;
  }

  .stat-value {
    font-size: 1.5rem;
    color: #3b82f6;
    font-weight: 700;
  }

  /* Info Note */
  .note {
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border-left: 4px solid #3b82f6;
    padding: 2rem;
    border-radius: 16px;
    margin: 2rem 0;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
  }

  .note p {
    margin: 0.75rem 0;
    color: #cbd5e1;
    line-height: 1.6;
  }

  .note strong {
    color: #3b82f6;
    font-weight: 600;
  }

  /* Responsive Design */
  @media (max-width: 768px) {
    .hero-content {
      flex-direction: column;
    }

    .hero-image {
      width: 100%;
    }

    .hero-stats {
      flex-direction: column;
      gap: 1.5rem;
    }

    .game-card {
      flex-direction: column;
      text-align: center;
    }

    .game-image {
      width: 100%;
    }

    .game-stats-row {
      flex-direction: column;
      gap: 1rem;
    }
  }
</style>
