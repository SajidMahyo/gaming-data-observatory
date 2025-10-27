# Gaming Data Observatory

<div class="hero">
  <div class="wip-badge">🚧 Work in Progress</div>
  <h1 class="hero-title">🎮 Gaming Data Observatory</h1>
  <p class="hero-subtitle">Track gaming trends across Steam, Twitch, and beyond</p>
  <p class="hero-description">This dashboard is currently under development. Check out the Rankings page to explore game statistics!</p>

  <a href="./ranking" class="cta-button">
    View Game Rankings →
  </a>
</div>

<style>
  .hero {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    min-height: 60vh;
    text-align: center;
    gap: 2rem;
  }

  .hero-title {
    font-size: 3.5rem;
    font-weight: 900;
    background: linear-gradient(135deg, #60a5fa 0%, #a78bfa 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin: 0;
  }

  .hero-subtitle {
    font-size: 1.5rem;
    color: #94a3b8;
    margin: 0;
    font-weight: 500;
  }

  .hero-description {
    font-size: 1.1rem;
    color: #64748b;
    margin: 0.5rem 0 0 0;
    max-width: 600px;
    line-height: 1.6;
  }

  .wip-badge {
    display: inline-block;
    padding: 0.5rem 1.5rem;
    background: linear-gradient(135deg, rgba(251, 191, 36, 0.2) 0%, rgba(245, 158, 11, 0.2) 100%);
    color: #fbbf24;
    border: 2px solid rgba(251, 191, 36, 0.4);
    border-radius: 24px;
    font-size: 0.95rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 1rem;
    animation: pulse 2s ease-in-out infinite;
  }

  @keyframes pulse {
    0%, 100% {
      opacity: 1;
    }
    50% {
      opacity: 0.7;
    }
  }

  .cta-button {
    margin-top: 2rem;
    padding: 1.25rem 3rem;
    font-size: 1.25rem;
    font-weight: 700;
    background: linear-gradient(135deg, #3b82f6 0%, #9333ea 100%);
    color: white;
    border-radius: 16px;
    text-decoration: none;
    border: none;
    cursor: pointer;
    transition: all 0.3s ease;
    box-shadow: 0 8px 24px rgba(59, 130, 246, 0.3);
  }

  .cta-button:hover {
    transform: translateY(-4px);
    box-shadow: 0 12px 32px rgba(59, 130, 246, 0.4);
  }

  @media (max-width: 768px) {
    .hero-title {
      font-size: 2.5rem;
    }

    .hero-subtitle {
      font-size: 1.25rem;
    }

    .hero-description {
      font-size: 1rem;
      padding: 0 1rem;
    }

    .wip-badge {
      font-size: 0.85rem;
      padding: 0.4rem 1.2rem;
    }

    .cta-button {
      font-size: 1.1rem;
      padding: 1rem 2rem;
    }
  }
</style>
