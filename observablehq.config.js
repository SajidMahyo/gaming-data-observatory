// Observable Framework configuration
// https://observablehq.com/framework/config

export default {
  title: "Gaming Data Observatory",
  description: "Real-time Steam player analytics and insights",

  // GitHub Pages deployment
  base: "/gaming-data-observatory/",

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
    {name: "Dashboard", path: "/index"},
    {name: "Top Games", path: "/games"},
    {name: "About", path: "/about"}
  ]
};
