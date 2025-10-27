```js
// Load all data sources
const steamDailyKPIs = FileAttachment("data/steam_daily_kpis.json").json();
const twitchDailyKPIs = FileAttachment("data/twitch_daily_kpis.json").json();
const steamWeeklyKPIs = FileAttachment("data/steam_weekly_kpis.json").json();
const twitchWeeklyKPIs = FileAttachment("data/twitch_weekly_kpis.json").json();
const steamMonthlyKPIs = FileAttachment("data/steam_monthly_kpis.json").json();
const twitchMonthlyKPIs = FileAttachment("data/twitch_monthly_kpis.json").json();
const steamRankings = FileAttachment("data/steam_rankings.json").json();
const twitchRankings = FileAttachment("data/twitch_rankings.json").json();
const gameMetadata = FileAttachment("data/game-metadata.json").json();
const igdbRatingsSnapshot = FileAttachment("data/igdb_ratings_snapshot.json").json();
```

```js
// Generic indicator configuration
const INDICATORS = {
  steam_ccu: {
    id: 'steam_ccu',
    name: 'Steam CCU',
    emoji: '🎮',
    metrics: [
      { id: 'average', label: 'Average CCU', field: 'avg_ccu' },
      { id: 'peak', label: 'Peak CCU', field: 'peak_ccu' }
    ],
    timeRanges: ['7days', '3weeks', '3months', 'alltime'],
    dataSources: {
      '7days': { data: steamDailyKPIs, type: 'daily' },
      '3weeks': { data: steamWeeklyKPIs, type: 'weekly' },
      '3months': { data: steamMonthlyKPIs, type: 'monthly' },
      'alltime': { data: steamMonthlyKPIs, type: 'monthly' }
    },
    formatValue: (val) => Math.round(val).toLocaleString(),
    getGameId: (game) => game.steam_app_id,
    hasImage: true
  },
  twitch_viewers: {
    id: 'twitch_viewers',
    name: 'Twitch Viewers',
    emoji: '📺',
    metrics: [
      { id: 'average', label: 'Average Viewers', field: 'avg_viewers' },
      { id: 'peak', label: 'Peak Viewers', field: 'peak_viewers' }
    ],
    timeRanges: ['7days', '3weeks', '3months', 'alltime'],
    dataSources: {
      '7days': { data: twitchDailyKPIs, type: 'daily' },
      '3weeks': { data: twitchWeeklyKPIs, type: 'weekly' },
      '3months': { data: twitchMonthlyKPIs, type: 'monthly' },
      'alltime': { data: twitchMonthlyKPIs, type: 'monthly' }
    },
    formatValue: (val) => Math.round(val).toLocaleString(),
    getGameId: (game) => game.twitch_game_id,
    hasImage: true
  },
  igdb_rating: {
    id: 'igdb_rating',
    name: 'IGDB Rating',
    emoji: '⭐',
    metrics: [
      { id: 'user', label: 'User Rating', field: 'rating' },
      { id: 'aggregated', label: 'Aggregated Rating', field: 'aggregated_rating' }
    ],
    timeRanges: ['latest'],
    dataSources: {
      'latest': { data: igdbRatingsSnapshot, type: 'alltime' }
    },
    formatValue: (val) => val ? val.toFixed(1) : 'N/A',
    getGameId: (game) => game.igdb_id,
    hasImage: true
  },
  steam_metacritic: {
    id: 'steam_metacritic',
    name: 'Metacritic Score',
    emoji: '🎯',
    metrics: [
      { id: 'score', label: 'Metacritic Score', field: 'avg_metacritic_score' }
    ],
    timeRanges: ['latest'],
    dataSources: {
      'latest': { data: steamRankings, type: 'alltime' }
    },
    formatValue: (val) => val ? Math.round(val).toString() : 'N/A',
    getGameId: (game) => game.steam_app_id,
    hasImage: true
  }
};

// Time range configurations
const TIME_RANGES = {
  '7days': { label: '📅 Last 7 Days', days: 7 },
  '3weeks': { label: '📅 Last 3 Weeks', weeks: 3 },
  '3months': { label: '📅 Last 3 Months', months: 3 },
  'alltime': { label: '🏆 All-Time', all: true },
  'latest': { label: '📊 Latest', latest: true }
};
```

```js
// Helper function to get IGDB cover URL from metadata (universal for all games)
function getGameCover(game) {
  const metadata = gameMetadata.find(m => m.igdb_id === game.igdb_id);
  return metadata?.cover_url || null;
}

// Helper to get game image - always use IGDB cover for consistency
function getGameImage(game, indicator) {
  if (!indicator.hasImage) return null;
  // Always use IGDB cover via igdb_id (universal)
  return getGameCover(game);
}

// Extract all unique tags (genres + themes) from game metadata
const allTags = Array.from(new Set(
  gameMetadata.flatMap(game => [...(game.genres || []), ...(game.themes || [])])
)).sort();
```

```js
// Aggregate data based on time range and metric
function aggregateData(indicator, timeRange, metricField) {
  const source = indicator.dataSources[timeRange];
  const data = source.data;
  const type = source.type;

  if (type === 'metadata' || type === 'alltime') {
    // For metadata or all-time rankings, just return the data as-is
    return data.filter(game => {
      const value = game[metricField];
      return value != null && value > 0;
    });
  }

  // For time-series data (daily/weekly/monthly), aggregate by game
  const gameMap = new Map();

  data.forEach(record => {
    const gameId = record.igdb_id || record.steam_app_id || record.twitch_game_id;
    const value = record[metricField];

    if (value == null || value <= 0) return;

    if (!gameMap.has(gameId)) {
      gameMap.set(gameId, {
        ...record,
        values: []
      });
    }

    gameMap.get(gameId).values.push(value);
  });

  // Calculate average for each game
  const aggregated = Array.from(gameMap.values()).map(game => {
    const avg = game.values.reduce((a, b) => a + b, 0) / game.values.length;
    return {
      ...game,
      [metricField]: avg,
      sample_count: game.values.length
    };
  });

  return aggregated;
}
```

```js
// Simplified selectors - just use simple inputs without complex state management for now
const selectedIndicatorInput = Inputs.select(Object.keys(INDICATORS), {
  label: "📊 Indicator",
  value: "steam_ccu",
  format: (key) => `${INDICATORS[key].emoji} ${INDICATORS[key].name}`
});
const selectedIndicator = Generators.input(selectedIndicatorInput);
```

```js
const currentIndicator = INDICATORS[selectedIndicator];
```

```js
const selectedMetricInput = Inputs.select(
  currentIndicator.metrics.map(m => m.field),
  {
    label: "📈 Metric",
    value: currentIndicator.metrics[0].field,
    format: (field) => currentIndicator.metrics.find(m => m.field === field).label
  }
);
const selectedMetric = Generators.input(selectedMetricInput);
```

```js
const selectedTimeRangeInput = Inputs.select(currentIndicator.timeRanges, {
  label: "⏰ Time Range",
  value: currentIndicator.timeRanges[0],
  format: (key) => TIME_RANGES[key].label
});
const selectedTimeRange = Generators.input(selectedTimeRangeInput);
```

```js
const tagOptions = ["All", ...allTags];
const selectedTagInput = Inputs.select(tagOptions, {
  label: "🏷️ Tags",
  value: "All"
});
const selectedTag = Generators.input(selectedTagInput);
```

```js
// Display selectors in grid
html`<div class="selectors-container">
  ${selectedIndicatorInput}
  ${selectedMetricInput}
  ${selectedTimeRangeInput}
  ${selectedTagInput}
</div>`
```

```js
// Get current metric label
const currentMetricLabel = currentIndicator.metrics.find(m => m.field === selectedMetric)?.label || 'Metric';
```

```js
// Aggregate and sort games with filters applied
const aggregatedGames = aggregateData(currentIndicator, selectedTimeRange, selectedMetric);

// Apply tag filter
const filteredGames = aggregatedGames.filter(game => {
  // If "All" is selected, show all games
  if (selectedTag === "All") {
    return true;
  }

  // Get game metadata to check tags
  const metadata = gameMetadata.find(m => m.igdb_id === game.igdb_id);
  if (!metadata) return true;

  const gameTags = [...(metadata.genres || []), ...(metadata.themes || [])];

  // Show game if it has the selected tag
  return gameTags.includes(selectedTag);
});

const sortedGames = [...filteredGames].sort((a, b) => {
  const aValue = a[selectedMetric] || 0;
  const bValue = b[selectedMetric] || 0;
  return bValue - aValue;
});

// Get top 3 and rest
const top3 = sortedGames.slice(0, 3);
const restOfGames = sortedGames.slice(3, 50); // Limit to top 50
```

```js
// Podium display
html`<div class="podium-container">
  ${[1, 0, 2].map(i => {
    if (!top3[i]) return '';

    const game = top3[i];
    const value = game[selectedMetric];
    const rank = i === 0 ? 1 : i === 1 ? 2 : 3;
    const medal = rank === 1 ? '🥇' : rank === 2 ? '🥈' : '🥉';
    const cardClass = rank === 1 ? 'gold' : rank === 2 ? 'silver' : 'bronze';
    const imageUrl = getGameImage(game, currentIndicator);

    return html`
      <a href="./games/${game.igdb_id}" class="podium-card ${cardClass}" style="background-image: url('${imageUrl}')">
        <div class="podium-rank">
          <span class="medal">${medal}</span>
        </div>
        <div class="podium-overlay"></div>
        <div class="podium-content">
          <h3 class="podium-title">${game.game_name}</h3>
          <div class="podium-stat">
            <span class="podium-value">${currentIndicator.formatValue(value)}</span>
            <span class="podium-label">${currentMetricLabel}</span>
          </div>
        </div>
      </a>
    `;
  })}
</div>`
```

```js
// Rankings list
html`<div class="rankings-list">
  ${restOfGames.map((game, index) => {
    const value = game[selectedMetric];
    const imageUrl = getGameImage(game, currentIndicator);

    return html`
      <a href="./games/${game.igdb_id}" class="ranking-row">
        <div class="rank-badge">#${index + 4}</div>
        ${imageUrl ? html`<img src="${imageUrl}" alt="${game.game_name}" class="ranking-thumbnail" />` : ''}
        <div class="ranking-info">
          <h4 class="ranking-name">${game.game_name}</h4>
          <div class="ranking-stats">
            <span class="ranking-stat">
              <span class="stat-label">${currentMetricLabel}:</span>
              <span class="stat-value">${currentIndicator.formatValue(value)}</span>
            </span>
            ${game.sample_count ? html`
              <span class="ranking-stat">
                <span class="stat-label">Samples:</span>
                <span class="stat-value">${game.sample_count}</span>
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
  /* Selectors Container */
  .selectors-container {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1.5rem;
    margin: 2rem 0 3rem 0;
    align-items: start;
  }

  /* Reset Observable form margins */
  .selectors-container form {
    margin: 0 !important;
    padding: 0 !important;
    width: 100% !important;
    display: block !important;
  }

  /* Style Observable inputs */
  .selectors-container > * {
    width: 100%;
  }

  .selectors-container label {
    display: flex !important;
    flex-direction: column !important;
    gap: 0.75rem;
    font-size: 0.875rem;
    font-weight: 600;
    color: #94a3b8;
    width: 100% !important;
    margin: 0 !important;
    padding: 0 !important;
    align-items: stretch !important;
  }

  .selectors-container label > span {
    display: block;
    width: 100%;
    margin-bottom: 0.5rem;
  }

  .selectors-container label > select,
  .selectors-container label > input {
    width: 100% !important;
  }

  .selectors-container select {
    background: rgba(30, 41, 59, 0.6);
    border: 1px solid rgba(96, 165, 250, 0.2);
    color: #e2e8f0;
    padding: 0.625rem 1rem;
    border-radius: 10px;
    font-size: 0.875rem;
    cursor: pointer;
    transition: all 0.25s ease;
  }

  .selectors-container select:hover {
    background: rgba(59, 130, 246, 0.15);
    border-color: rgba(59, 130, 246, 0.4);
  }

  .selectors-container select:focus {
    outline: none;
    border-color: rgba(59, 130, 246, 0.6);
    box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
  }

  /* Podium Container */
  .podium-container {
    display: grid;
    grid-template-columns: minmax(220px, 280px) minmax(300px, 400px) minmax(200px, 240px);
    gap: 1.5rem;
    margin: 3rem 0 4rem 0;
    align-items: end;
    justify-content: center;
  }

  /* Podium Cards */
  .podium-card {
    position: relative;
    background-size: cover;
    background-position: center;
    background-repeat: no-repeat;
    border-radius: 24px;
    padding: 2rem;
    text-decoration: none;
    border: none;
    box-shadow: inset 0 0 0 2px rgba(0, 0, 0, 0.4);
    overflow: hidden;
    transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
    display: flex;
    flex-direction: column;
    justify-content: space-between;
  }

  .podium-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 4px;
    opacity: 0.9;
    z-index: 2;
  }

  .podium-overlay {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: linear-gradient(
      to bottom,
      rgba(0, 0, 0, 0) 0%,
      rgba(0, 0, 0, 0.3) 40%,
      rgba(0, 0, 0, 0.8) 100%
    );
    z-index: 1;
  }

  .podium-card.gold {
    transform: scale(1.0);
    min-height: 420px;
    transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
  }

  .podium-card.silver {
    transform: scale(1.0);
    min-height: 380px;
    transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
  }

  .podium-card.bronze {
    transform: scale(1.0);
    min-height: 360px;
    transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
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
    transform: translateY(-8px) scale(1.02);
    border-color: rgba(96, 165, 250, 0.4);
    box-shadow: 0 20px 40px -12px rgba(96, 165, 250, 0.3);
  }

  .podium-card.gold:hover {
    transform: translateY(-8px) scale(1.12);
    box-shadow: 0 28px 56px -16px rgba(255, 215, 0, 0.5);
  }

  .podium-card.silver:hover {
    transform: translateY(-8px) scale(1.08);
    box-shadow: 0 24px 48px -14px rgba(192, 192, 192, 0.4);
  }

  .podium-card.bronze:hover {
    transform: translateY(-8px) scale(1.04);
    box-shadow: 0 20px 40px -12px rgba(205, 127, 50, 0.3);
  }

  .podium-rank {
    position: absolute;
    top: 1rem;
    left: 1rem;
    z-index: 4;
    display: flex;
    align-items: center;
    justify-content: center;
    background: rgba(0, 0, 0, 0.6);
    backdrop-filter: blur(12px);
    border-radius: 50%;
    padding: 0.6rem;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4),
                0 2px 6px rgba(0, 0, 0, 0.3);
    border: 2px solid rgba(255, 255, 255, 0.2);
  }

  .medal {
    font-size: 2rem;
    line-height: 1;
    display: block;
  }

  .podium-card.gold .medal {
    font-size: 2.5rem;
  }

  .podium-card.silver .medal {
    font-size: 2.25rem;
  }

  .podium-card.bronze .medal {
    font-size: 2rem;
  }

  .podium-content {
    position: relative;
    z-index: 3;
    text-align: center;
    width: 100%;
    margin-top: auto;
  }

  .podium-title {
    font-size: 1.5rem;
    font-weight: 700;
    color: white;
    margin: 0 0 1rem 0;
    text-shadow: 0 2px 8px rgba(0, 0, 0, 0.8);
  }

  .podium-card.gold .podium-title {
    font-size: 1.85rem;
  }

  .podium-card.silver .podium-title {
    font-size: 1.65rem;
  }

  .podium-stat {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }

  .podium-value {
    font-size: 2rem;
    font-weight: 800;
    color: white;
    text-shadow: 0 2px 12px rgba(96, 165, 250, 0.8),
                 0 4px 24px rgba(0, 0, 0, 0.8);
  }

  .podium-card.gold .podium-value {
    font-size: 2.75rem;
    text-shadow: 0 2px 12px rgba(255, 215, 0, 0.8),
                 0 4px 24px rgba(0, 0, 0, 0.8);
  }

  .podium-card.silver .podium-value {
    font-size: 2.35rem;
    text-shadow: 0 2px 12px rgba(192, 192, 192, 0.8),
                 0 4px 24px rgba(0, 0, 0, 0.8);
  }

  .podium-card.bronze .podium-value {
    text-shadow: 0 2px 12px rgba(205, 127, 50, 0.8),
                 0 4px 24px rgba(0, 0, 0, 0.8);
  }

  .podium-label {
    font-size: 0.85rem;
    color: rgba(255, 255, 255, 0.8);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    text-shadow: 0 2px 4px rgba(0, 0, 0, 0.8);
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
    width: 100px;
    aspect-ratio: 3/4;
    object-fit: cover;
    border-radius: 8px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
    flex-shrink: 0;
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
      grid-template-columns: repeat(auto-fit, minmax(200px, 280px));
      gap: 1.5rem;
      justify-content: center;
    }

    .podium-card.gold {
      transform: scale(1);
      order: -1;
    }

    .ranking-row {
      gap: 1rem;
    }

    .ranking-thumbnail {
      width: 80px;
    }
  }

  @media (max-width: 640px) {
    .podium-container {
      grid-template-columns: 1fr;
    }

    .ranking-row {
      flex-wrap: wrap;
      justify-content: center;
      text-align: center;
    }

    .ranking-thumbnail {
      width: 120px;
    }

    .arrow {
      display: none;
    }
  }

  @media (max-width: 1024px) {
    .selectors-container {
      grid-template-columns: 1fr;
      gap: 0.75rem;
      margin: 1.5rem 0 2rem 0;
    }
  }

  @media (max-width: 768px) {
    .criterion-selector {
      padding: 1rem;
    }

    .selector-label {
      font-size: 0.7rem;
    }

    .criterion-btn {
      padding: 0.55rem 0.875rem;
      font-size: 0.85rem;
    }
  }
</style>
