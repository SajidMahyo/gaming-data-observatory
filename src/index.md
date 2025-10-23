# 🎮 Gaming Data Observatory

```js
// Load data
const gameRankings = FileAttachment("data/game_rankings.json").json();
const gameMetadata = FileAttachment("data/game-metadata.json").json();
```

```js
// Merge rankings with metadata
const gamesWithMetadata = gameRankings.map(game => {
  const metadata = gameMetadata.find(m => m.app_id === game.app_id);
  return { ...game, ...metadata };
});

// Helper function to get Steam image URL
function getSteamImage(appId) {
  return `https://cdn.cloudflare.steamstatic.com/steam/apps/${appId}/header.jpg`;
}
```

## 🏆 Top Games Rankings

Click on a game to see detailed analysis.

```js
html`<div class="rankings-table">
  ${gamesWithMetadata.map((game, index) => html`
    <a href="./games/${game.app_id}" class="game-row">
      <div class="rank rank-${index + 1}">#${index + 1}</div>
      <img src="${getSteamImage(game.app_id)}" alt="${game.game_name}" class="game-thumbnail" />
      <div class="game-info">
        <h3 class="game-name">${game.game_name}</h3>
        <div class="game-stats">
          <span class="stat">
            <span class="stat-label">Avg Peak:</span>
            <span class="stat-value">${Math.round(game.avg_peak).toLocaleString()}</span>
          </span>
          <span class="stat">
            <span class="stat-label">All-Time Peak:</span>
            <span class="stat-value">${game.all_time_peak.toLocaleString()}</span>
          </span>
          ${game.genres ? html`
            <span class="stat">
              <span class="stat-label">Genres:</span>
              <span class="stat-value">${game.genres.join(', ')}</span>
            </span>
          ` : ''}
        </div>
      </div>
      <div class="arrow">→</div>
    </a>
  `)}
</div>`
```

<style>
  /* Rankings Table */
  .rankings-table {
    display: flex;
    flex-direction: column;
    gap: 1rem;
    margin: 2rem 0;
  }

  .game-row {
    display: flex;
    align-items: center;
    gap: 1.5rem;
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border-radius: 16px;
    padding: 1.5rem;
    border: 2px solid #334155;
    text-decoration: none;
    transition: all 0.3s ease;
    cursor: pointer;
  }

  .game-row:hover {
    transform: translateX(8px);
    border-color: #667eea;
    box-shadow: 0 10px 40px rgba(102, 126, 234, 0.3);
  }

  .rank {
    font-size: 2rem;
    font-weight: 900;
    color: #334155;
    min-width: 60px;
    text-align: center;
  }

  .rank-1 {
    color: #ffd700;
    text-shadow: 0 0 20px rgba(255, 215, 0, 0.5);
  }

  .rank-2 {
    color: #c0c0c0;
  }

  .rank-3 {
    color: #cd7f32;
  }

  .game-thumbnail {
    width: 300px;
    height: auto;
    border-radius: 8px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
  }

  .game-info {
    flex: 1;
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }

  .game-name {
    font-size: 1.5rem;
    font-weight: 700;
    color: #e2e8f0;
    margin: 0;
  }

  .game-stats {
    display: flex;
    gap: 2rem;
    flex-wrap: wrap;
  }

  .stat {
    display: flex;
    gap: 0.5rem;
    align-items: baseline;
  }

  .stat-label {
    font-size: 0.85rem;
    color: #64748b;
    font-weight: 600;
  }

  .stat-value {
    font-size: 1rem;
    color: #60a5fa;
    font-weight: 700;
  }

  .arrow {
    font-size: 2rem;
    color: #667eea;
    transition: transform 0.3s ease;
  }

  .game-row:hover .arrow {
    transform: translateX(8px);
  }

  /* Responsive */
  @media (max-width: 768px) {
    .game-row {
      flex-direction: column;
      text-align: center;
    }

    .game-thumbnail {
      width: 100%;
    }

    .game-stats {
      flex-direction: column;
      gap: 0.5rem;
    }

    .arrow {
      display: none;
    }
  }
</style>
