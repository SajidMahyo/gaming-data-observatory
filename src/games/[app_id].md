```js
// Load shared metadata and optimized KPIs (all games, but with date filtering)
const gameMetadata = FileAttachment("../data/game-metadata.json").json();
const gameRankings = FileAttachment("../data/game_rankings.json").json();

// Load KPIs (pre-filtered by date on server, but contains all games)
// - hourly: last 48 hours only
// - daily: last 30 days only
// - monthly: last 12 months only
const hourlyKpis = FileAttachment("../data/hourly_kpis.json").json();
const dailyKpis = FileAttachment("../data/daily_kpis.json").json();
const monthlyKpis = FileAttachment("../data/monthly_kpis.json").json();
```

```js
// Get app_id from URL parameter (Observable Framework syntax: observable.params.param)
const APP_ID = parseInt(observable.params.app_id);
const game = gameMetadata.find(g => g.app_id === APP_ID);
const ranking = gameRankings.find(g => g.app_id === APP_ID);

// Helper function
function getSteamImage(appId) {
  return `https://cdn.cloudflare.steamstatic.com/steam/apps/${appId}/header.jpg`;
}
```

```js
// Get top tags for hero banner
const topTags = game.tags
  ? Object.entries(game.tags).sort(([,a], [,b]) => b - a).slice(0, 5)
  : [];
```

```js
html`<div class="hero-banner" style="background-image: url('${getSteamImage(game.app_id)}')">
  <div class="hero-overlay"></div>
  <a href="../" class="back-link-hero">← Go back</a>
  <div class="hero-content">
    <h1 class="hero-title">${game.name}</h1>
    <div class="hero-tags">
      ${topTags.map(([tag]) => html`<span class="hero-tag">${tag}</span>`)}
    </div>
    <div class="hero-meta">
      <span class="badge">${game.type}</span>
      ${game.is_free ? html`<span class="badge free">FREE</span>` : ''}
      ${game.metacritic_score ? html`<span class="badge metacritic">⭐ ${game.metacritic_score}/100</span>` : ''}
    </div>
  </div>
  <div class="hero-description">
    <p>${game.description}</p>
  </div>
</div>`
```

## Player Statistics

```js
ranking && html`<div class="stats-grid">
  <div class="stat-card">
    <div class="stat-value">${Math.round(ranking.avg_peak).toLocaleString()}</div>
    <div class="stat-label">Average Peak Players</div>
  </div>
  <div class="stat-card">
    <div class="stat-value">${ranking.all_time_peak.toLocaleString()}</div>
    <div class="stat-label">All-Time Peak</div>
  </div>
  <div class="stat-card">
    <div class="stat-value">${ranking.days_tracked}</div>
    <div class="stat-label">Days Tracked</div>
  </div>
</div>`
```

## Player Trends

```js
// Period selector
const period = view((() => {
  const form = html`<div class="period-selector">
    <button class="period-btn" data-value="day">📊 Last 48 Hours</button>
    <button class="period-btn active" data-value="month">📅 Last 30 Days</button>
    <button class="period-btn" data-value="year">📆 Last 12 Months</button>
  </div>`;

  form.value = "month";

  form.addEventListener("click", (e) => {
    if (e.target.classList.contains("period-btn")) {
      form.querySelectorAll(".period-btn").forEach(btn => btn.classList.remove("active"));
      e.target.classList.add("active");
      form.value = e.target.dataset.value;
      form.dispatchEvent(new Event("input", {bubbles: true}));
    }
  });

  return form;
})());
```

```js
// Select data based on period and filter by app_id
// Data is already filtered by date on server (48h/30d/12m), just need to filter by game
const filteredData = (period === "day"
  ? hourlyKpis.filter(d => d.app_id == APP_ID).sort((a, b) => new Date(a.hour) - new Date(b.hour))
  : period === "month"
  ? dailyKpis.filter(d => d.app_id == APP_ID).sort((a, b) => new Date(a.date) - new Date(b.date))
  : monthlyKpis.filter(d => d.app_id == APP_ID).sort((a, b) => new Date(a.month_start) - new Date(b.month_start))
);
```

```js
// Layout à deux colonnes pour les graphiques
html`<div class="charts-grid">
  <!-- Graphique 1: Player Trends -->
  <div class="chart-column">
    <h3>Peak Players Over Time</h3>
    ${resize((width) => Plot.plot({
      width: width - 40, // Prend toute la largeur de la colonne (moins padding)
      height: 400,
      marginLeft: 60,
      marginRight: 20,
      marginTop: 30,
      marginBottom: 50,
      x: {
        type: "time",
        label: period === "day" ? "Hour" : period === "month" ? "Date" : "Month",
        labelAnchor: "center"
      },
      y: {
        label: "Peak CCU",
        grid: true,
        labelAnchor: "center"
      },
      marks: [
        Plot.lineY(filteredData, {
          x: period === "day" ? "hour" : period === "month" ? "date" : "month_start",
          y: "peak_ccu",
          stroke: "#60a5fa",
          strokeWidth: 3,
          curve: "catmull-rom"
        }),
        Plot.areaY(filteredData, {
          x: period === "day" ? "hour" : period === "month" ? "date" : "month_start",
          y: "peak_ccu",
          fill: "#60a5fa",
          fillOpacity: 0.15,
          curve: "catmull-rom"
        }),
        Plot.dot(filteredData, {
          x: period === "day" ? "hour" : period === "month" ? "date" : "month_start",
          y: "peak_ccu",
          fill: "#60a5fa",
          r: 4,
          stroke: "#0f172a",
          strokeWidth: 1
        }),
        Plot.ruleY([0])
      ],
      style: {
        background: "rgba(30, 41, 59, 0.5)",
        color: "#e2e8f0",
        borderRadius: "12px",
        padding: "15px"
      }
    }))}
  </div>

  <!-- Graphique 2: Average Players -->
  <div class="chart-column">
    <h3>Average Players Over Time</h3>
    ${resize((width) => Plot.plot({
      width: width - 40, // Prend toute la largeur de la colonne (moins padding)
      height: 400,
      marginLeft: 60,
      marginRight: 20,
      marginTop: 30,
      marginBottom: 50,
      x: {
        type: "time",
        label: period === "day" ? "Hour" : period === "month" ? "Date" : "Month",
        labelAnchor: "center"
      },
      y: {
        label: "Avg CCU",
        grid: true,
        labelAnchor: "center"
      },
      marks: [
        Plot.lineY(filteredData, {
          x: period === "day" ? "hour" : period === "month" ? "date" : "month_start",
          y: "avg_ccu",
          stroke: "#a78bfa",
          strokeWidth: 3,
          curve: "catmull-rom"
        }),
        Plot.areaY(filteredData, {
          x: period === "day" ? "hour" : period === "month" ? "date" : "month_start",
          y: "avg_ccu",
          fill: "#a78bfa",
          fillOpacity: 0.15,
          curve: "catmull-rom"
        }),
        Plot.dot(filteredData, {
          x: period === "day" ? "hour" : period === "month" ? "date" : "month_start",
          y: "avg_ccu",
          fill: "#a78bfa",
          r: 4,
          stroke: "#0f172a",
          strokeWidth: 1
        }),
        Plot.ruleY([0])
      ],
      style: {
        background: "rgba(30, 41, 59, 0.5)",
        color: "#e2e8f0",
        borderRadius: "12px",
        padding: "15px"
      }
    }))}
  </div>
</div>`
```

## Game Information

```js
html`<div class="info-section">
  <div class="info-grid">
    <div class="info-item">
      <h4>Developer</h4>
      <p>${game.developers?.join(', ') || 'Unknown'}</p>
    </div>
    <div class="info-item">
      <h4>Publisher</h4>
      <p>${game.publishers?.join(', ') || 'Unknown'}</p>
    </div>
    <div class="info-item">
      <h4>Release Date</h4>
      <p>${game.release_date || 'TBA'}</p>
    </div>
    <div class="info-item">
      <h4>Platforms</h4>
      <p>${game.platforms?.map(p => p.toUpperCase()).join(', ') || 'N/A'}</p>
    </div>
  </div>
</div>`
```

## Genres & Categories

```js
html`<div class="tags-section">
  <div class="tag-group">
    <h4>Genres</h4>
    <div class="tags">
      ${game.genres?.map(genre => html`<span class="tag genre">${genre}</span>`) || ''}
    </div>
  </div>

  ${game.categories && game.categories.length > 0 ? html`
    <div class="tag-group">
      <h4>Features</h4>
      <div class="tags">
        ${game.categories.slice(0, 10).map(cat => html`<span class="tag category">${cat}</span>`)}
      </div>
    </div>
  ` : ''}
</div>`
```

## Popular Tags

```js
topTags.length > 0 && html`<div class="tags-section">
  <h4>Community Tags</h4>
  <div class="tags">
    ${topTags.map(([tag, score]) => html`
      <span class="tag popular" title="${score.toLocaleString()} votes">${tag}</span>
    `)}
  </div>
</div>`
```

## Price Information

```js
game.price_info && html`<div class="price-section">
  ${game.price_info.is_free
    ? html`<div class="price free-price">Free to Play</div>`
    : html`<div class="price">$${game.price_info.price}</div>`
  }
</div>`
```

<style>
  .hero-banner {
    position: relative;
    height: 500px;
    margin: -2rem -2rem 2rem -2rem;
    background-size: cover;
    background-position: center;
    background-repeat: no-repeat;
    cursor: pointer;
    transition: transform 0.3s ease;
    overflow: hidden;
  }

  .hero-banner:hover {
    transform: scale(1.01);
  }

  .hero-overlay {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: linear-gradient(to bottom,
      rgba(15, 23, 42, 0.7) 0%,
      rgba(15, 23, 42, 0.85) 50%,
      rgba(15, 23, 42, 0.95) 100%
    );
    z-index: 1;
  }

  .back-link-hero {
    position: absolute;
    top: 1.5rem;
    left: 2rem;
    z-index: 4;
    color: #60a5fa;
    text-decoration: none;
    font-weight: 600;
    font-size: 1rem;
    transition: color 0.2s;
    background: rgba(15, 23, 42, 0.8);
    padding: 0.5rem 1rem;
    border-radius: 8px;
    backdrop-filter: blur(10px);
  }

  .back-link-hero:hover {
    color: #93c5fd;
  }

  .hero-content {
    position: relative;
    z-index: 2;
    padding: 3rem 3rem 3.5rem 3rem;
    display: flex;
    flex-direction: column;
    justify-content: flex-end;
    height: 100%;
    box-sizing: border-box;
  }

  .hero-title {
    font-size: 3.5rem;
    font-weight: 900;
    color: #e2e8f0;
    margin: 0 0 1rem 0;
    text-shadow: 2px 2px 8px rgba(0, 0, 0, 0.8);
    letter-spacing: -1px;
  }

  .hero-tags {
    display: flex;
    flex-wrap: wrap;
    gap: 0.75rem;
    margin-bottom: 1rem;
  }

  .hero-tag {
    background: rgba(96, 165, 250, 0.2);
    backdrop-filter: blur(10px);
    color: #93c5fd;
    padding: 0.5rem 1rem;
    border-radius: 12px;
    font-size: 0.9rem;
    font-weight: 600;
    border: 1px solid rgba(96, 165, 250, 0.3);
    text-shadow: 1px 1px 2px rgba(0, 0, 0, 0.5);
  }

  .hero-meta {
    display: flex;
    gap: 0.75rem;
  }

  .badge {
    background: rgba(59, 130, 246, 0.2);
    backdrop-filter: blur(10px);
    color: #60a5fa;
    padding: 0.5rem 1rem;
    border-radius: 12px;
    font-size: 0.9rem;
    font-weight: 600;
    border: 1px solid rgba(59, 130, 246, 0.3);
  }

  .badge.free {
    background: rgba(34, 197, 94, 0.2);
    color: #4ade80;
    border-color: rgba(34, 197, 94, 0.3);
  }

  .badge.metacritic {
    background: rgba(251, 191, 36, 0.2);
    color: #fbbf24;
    border-color: rgba(251, 191, 36, 0.3);
  }

  .hero-description {
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    width: 45%;
    background: linear-gradient(to right,
      transparent 0%,
      rgba(15, 23, 42, 0.05) 10%,
      rgba(15, 23, 42, 0.2) 25%,
      rgba(15, 23, 42, 0.5) 45%,
      rgba(15, 23, 42, 0.75) 65%,
      rgba(15, 23, 42, 0.92) 85%,
      rgba(15, 23, 42, 0.98) 100%
    );
    backdrop-filter: blur(10px);
    z-index: 3;
    display: flex;
    align-items: center;
    justify-content: flex-end;
    padding: 3rem 4rem 3rem 5rem;
    opacity: 0;
    transition: opacity 0.4s ease, transform 0.4s ease;
    transform: translateX(20px);
  }

  .hero-banner:hover .hero-description {
    opacity: 1;
    transform: translateX(0);
  }

  .hero-description p {
    color: #e2e8f0;
    line-height: 1.9;
    font-size: 1.15rem;
    max-width: 500px;
    text-align: left;
    margin: 0;
    text-shadow: 0 2px 4px rgba(0, 0, 0, 0.5);
    font-weight: 400;
  }

  /* Titres de section sobres et élégants */
  h2 {
    font-size: 1.75rem;
    font-weight: 600;
    margin: 3.5rem 0 1.5rem 0;
    padding-bottom: 0;
    position: relative;
    color: #f1f5f9;
    letter-spacing: -0.025em;
    pointer-events: none;
  }

  h2 a {
    pointer-events: none;
    text-decoration: none !important;
    color: #f1f5f9 !important;
    cursor: default;
  }

  h2::after {
    content: '';
    position: absolute;
    bottom: -0.5rem;
    left: 0;
    width: 60px;
    height: 2px;
    background: linear-gradient(to right, #60a5fa, transparent);
  }

  /* Period Selector */
  .period-selector {
    display: flex;
    gap: 1rem;
    margin: 2rem 0;
    flex-wrap: wrap;
  }

  .period-btn {
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

  .period-btn:hover {
    background: linear-gradient(135deg, rgba(59, 130, 246, 0.2) 0%, rgba(147, 51, 234, 0.2) 100%);
    border-color: rgba(59, 130, 246, 0.5);
    color: #60a5fa;
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(59, 130, 246, 0.2);
  }

  .period-btn.active {
    background: linear-gradient(135deg, #3b82f6 0%, #9333ea 100%);
    border-color: #3b82f6;
    color: white;
    box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
  }

  .stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 2rem;
    margin: 2rem 0;
  }

  .stat-card {
    position: relative;
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.8) 100%);
    backdrop-filter: blur(20px);
    border-radius: 20px;
    padding: 2.5rem 2rem;
    text-align: center;
    border: 1px solid rgba(96, 165, 250, 0.15);
    box-shadow:
      0 4px 24px -1px rgba(0, 0, 0, 0.25),
      0 0 0 1px rgba(255, 255, 255, 0.03) inset,
      0 -1px 0 0 rgba(255, 255, 255, 0.05) inset;
    transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
    overflow: hidden;
  }

  .stat-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 3px;
    background: linear-gradient(90deg, #60a5fa 0%, #a78bfa 50%, #f472b6 100%);
    opacity: 0.8;
  }

  .stat-card::after {
    content: '';
    position: absolute;
    inset: 0;
    background: radial-gradient(circle at 50% 0%, rgba(96, 165, 250, 0.1), transparent 60%);
    opacity: 0;
    transition: opacity 0.4s ease;
  }

  .stat-card:hover {
    transform: translateY(-8px);
    border-color: rgba(96, 165, 250, 0.4);
    box-shadow:
      0 20px 40px -12px rgba(96, 165, 250, 0.25),
      0 0 0 1px rgba(96, 165, 250, 0.1) inset,
      0 -1px 0 0 rgba(255, 255, 255, 0.1) inset;
  }

  .stat-card:hover::after {
    opacity: 1;
  }

  .stat-value {
    font-size: 3rem;
    font-weight: 800;
    background: linear-gradient(135deg, #60a5fa 0%, #a78bfa 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin-bottom: 0.75rem;
    line-height: 1.1;
    letter-spacing: -0.02em;
  }

  .stat-label {
    color: #cbd5e1;
    font-size: 0.875rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    opacity: 0.9;
  }

  .info-section {
    margin: 2rem 0;
  }

  .info-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 1.5rem;
  }

  .info-item {
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border-radius: 12px;
    padding: 1.5rem;
    border: 1px solid #334155;
  }

  .info-item h4 {
    color: #94a3b8;
    font-size: 0.85rem;
    text-transform: uppercase;
    margin: 0 0 0.5rem 0;
    letter-spacing: 0.5px;
  }

  .info-item p {
    color: #e2e8f0;
    font-size: 1.1rem;
    font-weight: 600;
    margin: 0;
  }

  .tags-section {
    margin: 2rem 0;
  }

  .tag-group {
    margin-bottom: 2rem;
  }

  .tag-group h4 {
    color: #94a3b8;
    font-size: 0.95rem;
    text-transform: uppercase;
    margin: 0 0 1rem 0;
    letter-spacing: 0.5px;
  }

  .tags {
    display: flex;
    flex-wrap: wrap;
    gap: 0.75rem;
  }

  .tag {
    padding: 0.5rem 1rem;
    border-radius: 16px;
    font-size: 0.9rem;
    font-weight: 600;
    transition: transform 0.2s;
  }

  .tag:hover {
    transform: translateY(-2px);
  }

  .tag.genre {
    background: rgba(139, 92, 246, 0.15);
    color: #a78bfa;
    border: 1px solid rgba(139, 92, 246, 0.3);
  }

  .tag.category {
    background: rgba(59, 130, 246, 0.15);
    color: #60a5fa;
    border: 1px solid rgba(59, 130, 246, 0.3);
  }

  .tag.popular {
    background: rgba(236, 72, 153, 0.15);
    color: #f472b6;
    border: 1px solid rgba(236, 72, 153, 0.3);
  }

  .price-section {
    margin: 2rem 0;
    text-align: center;
  }

  .price {
    display: inline-block;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
    padding: 1.5rem 3rem;
    border-radius: 16px;
    font-size: 2rem;
    font-weight: 900;
  }

  .free-price {
    background: linear-gradient(135deg, #10b981 0%, #059669 100%);
  }

  /* Grille de graphiques à deux colonnes */
  .charts-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 2rem;
    margin: 2rem 0;
    width: 100%;
  }

  .chart-column {
    background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
    border: 1px solid #334155;
    border-radius: 16px;
    padding: 1.5rem;
    min-width: 0; /* Important pour le responsive */
  }

  .chart-column h3 {
    color: #e2e8f0;
    font-size: 1.25rem;
    font-weight: 700;
    margin: 0 0 1rem 0;
    text-align: center;
  }

  /* Responsive : une seule colonne sur petits écrans */
  @media (max-width: 1024px) {
    .charts-grid {
      grid-template-columns: 1fr;
    }
  }
</style>
