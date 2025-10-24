// Observable Framework configuration
// https://observablehq.com/framework/config

export default {
  title: "Gaming Data Observatory",
  description: "Real-time Steam player analytics and insights",

  // Source root directory
  root: "src",

  // GitHub Pages deployment
  base: "/gaming-data-observatory/",

  // Use uv for Python data loaders (enables DuckDB and dependencies)
  interpreters: {
    ".py": ["uv", "run", "python"]
  },

  // Theme and styling - use near-midnight (dark) theme with dashboard modifiers
  theme: ["near-midnight", "alt", "wide"],

  // Custom stylesheet
  style: "custom-style.css",

  // Header with custom navbar
  header: `
    <div style="
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0.75rem 2rem;
      background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
      border-bottom: 2px solid #60a5fa;
      box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    ">
      <a href="./" style="
        display: flex;
        align-items: center;
        gap: 0.75rem;
        text-decoration: none;
        color: #f1f5f9;
        font-weight: 600;
        font-size: 1.25rem;
        transition: transform 0.2s;
      " onmouseover="this.style.transform='scale(1.05)'" onmouseout="this.style.transform='scale(1)'">
        <span style="font-size: 2rem;">🎮</span>
        <span>Gaming Data Observatory</span>
      </a>

      <nav style="display: flex; gap: 1.5rem; align-items: center;">
        <a href="./about" style="
          color: #e2e8f0;
          text-decoration: none;
          padding: 0.5rem 1.5rem;
          border-radius: 0.5rem;
          transition: all 0.2s;
          font-weight: 600;
          background: linear-gradient(135deg, rgba(59, 130, 246, 0.15) 0%, rgba(147, 51, 234, 0.15) 100%);
          border: 1px solid rgba(96, 165, 250, 0.3);
        " onmouseover="this.style.background='linear-gradient(135deg, rgba(59, 130, 246, 0.25) 0%, rgba(147, 51, 234, 0.25) 100%)'; this.style.borderColor='rgba(96, 165, 250, 0.5)'; this.style.color='#60a5fa'"
           onmouseout="this.style.background='linear-gradient(135deg, rgba(59, 130, 246, 0.15) 0%, rgba(147, 51, 234, 0.15) 100%)'; this.style.borderColor='rgba(96, 165, 250, 0.3)'; this.style.color='#e2e8f0'">
          About this project
        </a>
        <a href="./about-me" style="
          color: #e2e8f0;
          text-decoration: none;
          padding: 0.5rem 1.5rem;
          border-radius: 0.5rem;
          transition: all 0.2s;
          font-weight: 600;
          background: linear-gradient(135deg, rgba(59, 130, 246, 0.15) 0%, rgba(147, 51, 234, 0.15) 100%);
          border: 1px solid rgba(96, 165, 250, 0.3);
        " onmouseover="this.style.background='linear-gradient(135deg, rgba(59, 130, 246, 0.25) 0%, rgba(147, 51, 234, 0.25) 100%)'; this.style.borderColor='rgba(96, 165, 250, 0.5)'; this.style.color='#60a5fa'"
           onmouseout="this.style.background='linear-gradient(135deg, rgba(59, 130, 246, 0.15) 0%, rgba(147, 51, 234, 0.15) 100%)'; this.style.borderColor='rgba(96, 165, 250, 0.3)'; this.style.color='#e2e8f0'">
          About me
        </a>
      </nav>
    </div>
  `,

  footer: `
    <div style="
      text-align: center;
      padding: 1.5rem;
      background: #0f172a;
      color: #94a3b8;
      border-top: 1px solid #334155;
    ">
      Built with <a href="https://observablehq.com/framework" style="color: #60a5fa; text-decoration: none;">Observable Framework</a> •
      Data updated hourly via GitHub Actions •
      <a href="https://github.com/SajidMahyo/gaming-data-observatory" style="color: #60a5fa; text-decoration: none;">View Source</a>
    </div>
  `,

  // Disable table of contents
  toc: false,

  // Disable sidebar navigation
  sidebar: false,

  // Disable page navigation (pager)
  pager: false,

  // Dynamic paths for game pages
  async *dynamicPaths() {
    const {readFile} = await import("node:fs/promises");
    const {join} = await import("node:path");

    // Read game metadata to generate paths
    const metadataPath = join(process.cwd(), "src/data/game-metadata.json");
    try {
      const data = await readFile(metadataPath, "utf-8");
      const games = JSON.parse(data);

      // Generate a path for each game: /games/[app_id]
      for (const game of games) {
        yield `/games/${game.app_id}`;
      }
    } catch (error) {
      console.warn("Could not load game metadata for dynamic paths:", error.message);
    }
  }
};
