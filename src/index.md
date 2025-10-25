```js
// Load multi-source data
const steamRankings = FileAttachment("data/steam_rankings.json").json();
const twitchRankings = FileAttachment("data/twitch_rankings.json").json();
const unifiedRankings = FileAttachment("data/unified_rankings.json").json();
const gameMetadata = FileAttachment("data/game-metadata.json").json();
```

```js
// Helper function to get Steam image URL
function getSteamImage(appId) {
  return `https://cdn.cloudflare.steamstatic.com/steam/apps/${appId}/header.jpg`;
}

// Helper function to get game display info
function getGameDisplayInfo(game, platform) {
  if (platform === 'steam') {
    return {
      id: game.steam_app_id,
      name: game.game_name,
      avgMetric: game.avg_peak_ccu,
      peakMetric: game.all_time_peak_ccu,
      avgLabel: 'Avg Peak CCU',
      peakLabel: 'All-Time Peak CCU',
      metricSuffix: ''
    };
  } else if (platform === 'twitch') {
    return {
      id: game.twitch_game_id,
      name: game.game_name,
      avgMetric: game.avg_peak_viewers,
      peakMetric: game.all_time_peak_viewers,
      avgLabel: 'Avg Peak Viewers',
      peakLabel: 'All-Time Peak Viewers',
      metricSuffix: ''
    };
  } else { // unified
    return {
      id: game.steam_app_id || game.igdb_id,
      name: game.game_name,
      avgMetric: (game.avg_peak_ccu || 0) + (game.avg_peak_viewers || 0),
      peakMetric: (game.all_time_peak_ccu || 0) + (game.all_time_peak_viewers || 0),
      avgLabel: 'Combined Avg Peak',
      peakLabel: 'Combined All-Time Peak',
      steamCCU: game.avg_peak_ccu,
      twitchViewers: game.avg_peak_viewers,
      metricSuffix: ''
    };
  }
}
```

```js
// Platform selector
const platform = view((() => {
  const form = html`<div class="criterion-selector">
    <div class="selector-label">Platform:</div>
    <div class="selector-buttons">
      <button class="criterion-btn active" data-value="unified">🌐 Unified</button>
      <button class="criterion-btn" data-value="steam">🎮 Steam</button>
      <button class="criterion-btn" data-value="twitch">📺 Twitch</button>
    </div>
  </div>`;

  form.value = "unified";

  form.addEventListener("click", (e) => {
    if (e.target.classList.contains("criterion-btn")) {
      form.querySelectorAll(".criterion-btn").forEach(btn => btn.classList.remove("active"));
      e.target.classList.add("active");
      form.value = e.target.dataset.value;
      form.dispatchEvent(new Event("input", {bubbles: true}));
    }
  });

  return form;
})());
```

```js
// Ranking criterion selector
const rankingCriterion = view((() => {
  const form = html`<div class="criterion-selector">
    <div class="selector-label">Rank by:</div>
    <div class="selector-buttons">
      <button class="criterion-btn active" data-value="avg_peak">📊 Average Peak</button>
      <button class="criterion-btn" data-value="all_time_peak">🏆 All-Time Peak</button>
    </div>
  </div>`;

  form.value = "avg_peak";

  form.addEventListener("click", (e) => {
    if (e.target.classList.contains("criterion-btn")) {
      form.querySelectorAll(".criterion-btn").forEach(btn => btn.classList.remove("active"));
      e.target.classList.add("active");
      form.value = e.target.dataset.value;
      form.dispatchEvent(new Event("input", {bubbles: true}));
    }
  });

  return form;
})());
```

```js
// Select data based on platform
const currentRankings = platform === 'steam' ? steamRankings :
                        platform === 'twitch' ? twitchRankings :
                        unifiedRankings;

// Sort games by selected criterion
const sortedGames = [...currentRankings].sort((a, b) => {
  const aInfo = getGameDisplayInfo(a, platform);
  const bInfo = getGameDisplayInfo(b, platform);
  const aValue = rankingCriterion === 'avg_peak' ? aInfo.avgMetric : aInfo.peakMetric;
  const bValue = rankingCriterion === 'avg_peak' ? bInfo.avgMetric : bInfo.peakMetric;
  return bValue - aValue;
});

// Get top 3 and rest
const top3 = sortedGames.slice(0, 3);
const restOfGames = sortedGames.slice(3);
```

```js
html`<div class="podium-container">
  ${[1, 0, 2].map(i => {
    const game = top3[i];
    const info = getGameDisplayInfo(game, platform);
    const value = rankingCriterion === 'avg_peak' ? info.avgMetric : info.peakMetric;
    const label = rankingCriterion === 'avg_peak' ? info.avgLabel : info.peakLabel;
    const rank = i === 0 ? 1 : i === 1 ? 2 : 3;
    const medal = rank === 1 ? '🥇' : rank === 2 ? '🥈' : '🥉';
    const cardClass = rank === 1 ? 'gold' : rank === 2 ? 'silver' : 'bronze';
    const appId = game.steam_app_id || game.igdb_id;

    return html`
      <a href="./games/${appId}" class="podium-card ${cardClass}">
        <div class="podium-rank">
          <span class="rank-number">${rank}</span>
          <span class="medal">${medal}</span>
        </div>
        ${appId ? html`<img src="${getSteamImage(appId)}" alt="${info.name}" class="podium-image" />` : ''}
        <div class="podium-content">
          <h3 class="podium-title">${info.name}</h3>
          <div class="podium-stat">
            <span class="podium-value">${Math.round(value).toLocaleString()}</span>
            <span class="podium-label">${label}</span>
          </div>
          ${platform === 'unified' && info.steamCCU && info.twitchViewers ? html`
            <div class="podium-details">
              <span class="detail-item">🎮 ${Math.round(info.steamCCU).toLocaleString()} CCU</span>
              <span class="detail-item">📺 ${Math.round(info.twitchViewers).toLocaleString()} viewers</span>
            </div>
          ` : ''}
        </div>
      </a>
    `;
  })}
</div>`
```

```js
html`<div class="rankings-list">
  ${restOfGames.map((game, index) => {
    const info = getGameDisplayInfo(game, platform);
    const appId = game.steam_app_id || game.igdb_id;

    return html`
      <a href="./games/${appId}" class="ranking-row">
        <div class="rank-badge">#${index + 4}</div>
        ${appId ? html`<img src="${getSteamImage(appId)}" alt="${info.name}" class="ranking-thumbnail" />` : ''}
        <div class="ranking-info">
          <h4 class="ranking-name">${info.name}</h4>
          <div class="ranking-stats">
            <span class="ranking-stat">
              <span class="stat-label">${info.avgLabel}:</span>
              <span class="stat-value">${Math.round(info.avgMetric).toLocaleString()}</span>
            </span>
            <span class="ranking-stat">
              <span class="stat-label">${info.peakLabel}:</span>
              <span class="stat-value">${Math.round(info.peakMetric).toLocaleString()}</span>
            </span>
            ${platform === 'unified' && info.steamCCU && info.twitchViewers ? html`
              <span class="ranking-stat">
                <span class="stat-label">🎮 ${Math.round(info.steamCCU).toLocaleString()} | 📺 ${Math.round(info.twitchViewers).toLocaleString()}</span>
              </span>
            ` : ''}
          </div>
        </div>
        <div class="arrow">→</div>
      </a>
    `;
  })}
</div>`
```

<style>
  /* Criterion Selector */
  .criterion-selector {
    display: flex;
    align-items: center;
    gap: 1.5rem;
    margin: 2rem 0 3rem 0;
    padding: 1.5rem;
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.8) 100%);
    backdrop-filter: blur(20px);
    border-radius: 20px;
    border: 1px solid rgba(96, 165, 250, 0.15);
  }

  .selector-label {
    font-size: 1.1rem;
    font-weight: 600;
    color: #cbd5e1;
    letter-spacing: 0.02em;
  }

  .selector-buttons {
    display: flex;
    gap: 1rem;
    flex-wrap: wrap;
  }

  .criterion-btn {
    background: linear-gradient(135deg, rgba(59, 130, 246, 0.1) 0%, rgba(147, 51, 234, 0.1) 100%);
    border: 2px solid rgba(59, 130, 246, 0.3);
    color: #94a3b8;
    padding: 0.75rem 1.5rem;
    border-radius: 12px;
    font-size: 1rem;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.3s ease;
    backdrop-filter: blur(10px);
  }

  .criterion-btn:hover {
    background: linear-gradient(135deg, rgba(59, 130, 246, 0.2) 0%, rgba(147, 51, 234, 0.2) 100%);
    border-color: rgba(59, 130, 246, 0.5);
    color: #60a5fa;
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(59, 130, 246, 0.2);
  }

  .criterion-btn.active {
    background: linear-gradient(135deg, #3b82f6 0%, #9333ea 100%);
    border-color: #3b82f6;
    color: white;
    box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
  }

  /* Podium Container */
  .podium-container {
    display: grid;
    grid-template-columns: 1fr 1.2fr 1fr;
    gap: 2rem;
    margin: 2rem 0 4rem 0;
    align-items: end;
  }

  /* Podium Cards */
  .podium-card {
    position: relative;
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.8) 100%);
    backdrop-filter: blur(20px);
    border-radius: 24px;
    padding: 2rem;
    text-decoration: none;
    border: 2px solid rgba(96, 165, 250, 0.15);
    overflow: hidden;
    transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
    display: flex;
    flex-direction: column;
    align-items: center;
  }

  .podium-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 4px;
    opacity: 0.9;
  }

  .podium-card.gold {
    transform: scale(1.05);
  }

  .podium-card.gold::before {
    background: linear-gradient(90deg, #ffd700, #ffed4e);
  }

  .podium-card.silver::before {
    background: linear-gradient(90deg, #c0c0c0, #e8e8e8);
  }

  .podium-card.bronze::before {
    background: linear-gradient(90deg, #cd7f32, #e8a87c);
  }

  .podium-card:hover {
    transform: translateY(-12px) scale(1.02);
    border-color: rgba(96, 165, 250, 0.4);
    box-shadow: 0 20px 40px -12px rgba(96, 165, 250, 0.3);
  }

  .podium-card.gold:hover {
    transform: translateY(-12px) scale(1.07);
    box-shadow: 0 24px 48px -12px rgba(255, 215, 0, 0.4);
  }

  .podium-rank {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 1rem;
  }

  .rank-number {
    font-size: 2rem;
    font-weight: 900;
    background: linear-gradient(135deg, #60a5fa 0%, #a78bfa 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
  }

  .medal {
    font-size: 2rem;
  }

  .podium-image {
    width: 100%;
    border-radius: 16px;
    box-shadow: 0 8px 24px rgba(0, 0, 0, 0.4);
    margin-bottom: 1.5rem;
  }

  .podium-content {
    text-align: center;
    width: 100%;
  }

  .podium-title {
    font-size: 1.5rem;
    font-weight: 700;
    color: #e2e8f0;
    margin: 0 0 1rem 0;
  }

  .podium-card.gold .podium-title {
    font-size: 1.75rem;
  }

  .podium-stat {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }

  .podium-value {
    font-size: 2rem;
    font-weight: 800;
    background: linear-gradient(135deg, #60a5fa 0%, #a78bfa 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
  }

  .podium-card.gold .podium-value {
    font-size: 2.5rem;
  }

  .podium-label {
    font-size: 0.85rem;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 0.1em;
  }

  .podium-details {
    display: flex;
    gap: 1rem;
    margin-top: 0.75rem;
    flex-wrap: wrap;
    justify-content: center;
  }

  .detail-item {
    font-size: 0.9rem;
    color: #94a3b8;
    padding: 0.25rem 0.75rem;
    background: rgba(59, 130, 246, 0.1);
    border-radius: 6px;
    border: 1px solid rgba(59, 130, 246, 0.2);
  }

  /* Rankings List */
  .rankings-list {
    display: flex;
    flex-direction: column;
    gap: 1rem;
    margin: 2rem 0;
  }

  .ranking-row {
    display: flex;
    align-items: center;
    gap: 1.5rem;
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.8) 100%);
    backdrop-filter: blur(20px);
    border-radius: 16px;
    padding: 1.25rem 1.5rem;
    border: 1px solid rgba(96, 165, 250, 0.15);
    text-decoration: none;
    transition: all 0.3s ease;
    cursor: pointer;
  }

  .ranking-row:hover {
    transform: translateX(8px);
    border-color: rgba(96, 165, 250, 0.4);
    box-shadow: 0 8px 24px rgba(96, 165, 250, 0.2);
  }

  .rank-badge {
    font-size: 1.5rem;
    font-weight: 700;
    color: #64748b;
    min-width: 50px;
    text-align: center;
  }

  .ranking-thumbnail {
    width: 200px;
    height: auto;
    border-radius: 8px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
  }

  .ranking-info {
    flex: 1;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .ranking-name {
    font-size: 1.25rem;
    font-weight: 700;
    color: #e2e8f0;
    margin: 0;
  }

  .ranking-stats {
    display: flex;
    gap: 2rem;
    flex-wrap: wrap;
  }

  .ranking-stat {
    display: flex;
    gap: 0.5rem;
    align-items: baseline;
  }

  .stat-label {
    font-size: 0.85rem;
    color: #64748b;
    font-weight: 500;
  }

  .stat-value {
    font-size: 0.95rem;
    color: #60a5fa;
    font-weight: 700;
  }

  .arrow {
    font-size: 2rem;
    color: #60a5fa;
    transition: transform 0.3s ease;
  }

  .ranking-row:hover .arrow {
    transform: translateX(8px);
  }

  /* Responsive */
  @media (max-width: 1024px) {
    .podium-container {
      grid-template-columns: 1fr;
      gap: 1.5rem;
    }

    .podium-card.gold {
      transform: scale(1);
      order: -1;
    }

    .ranking-row {
      flex-direction: column;
      text-align: center;
    }

    .ranking-thumbnail {
      width: 100%;
    }

    .arrow {
      display: none;
    }
  }

  @media (max-width: 768px) {
    .criterion-selector {
      flex-direction: column;
      align-items: flex-start;
      gap: 1rem;
    }

    .selector-buttons {
      width: 100%;
    }

    .criterion-btn {
      flex: 1;
    }
  }
</style>
