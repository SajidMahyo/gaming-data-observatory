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

  // Theme and styling
  theme: "dark",

  // Header and footer
  header: `
    <div style="display: flex; align-items: center; gap: 0.5rem;">
      <span style="font-size: 1.5rem;">🎮</span>
      <strong>Gaming Data Observatory</strong>
    </div>
  `,

  footer: `Built with <a href="https://observablehq.com/framework">Observable Framework</a> • Data updated hourly via GitHub Actions`,

  // Table of contents
  toc: {
    label: "Contents",
    show: true
  },

  // Pages
  pages: [
    {name: "Rankings", path: "/index"},
    {name: "About", path: "/about"}
  ],

  // Dynamic paths for game pages
  dynamicPaths: async () => {
    const fs = await import("node:fs/promises");
    const path = await import("node:path");

    // Read game metadata to generate paths
    const metadataPath = path.join(process.cwd(), "src/data/game-metadata.json");
    try {
      const data = await fs.readFile(metadataPath, "utf-8");
      const games = JSON.parse(data);

      // Generate a path for each game: /games/[app_id]
      return games.map(game => `/games/${game.app_id}`);
    } catch (error) {
      console.warn("Could not load game metadata for dynamic paths:", error.message);
      return [];
    }
  }
};
