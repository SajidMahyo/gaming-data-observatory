```js
// Skills organized by categories with real technology logos
const skillsCategories = {
  "Languages & Core": [
    { name: "Python", logo: "python", expertise: "Expert", color: "#3776AB" },
    { name: "SQL", logo: "postgresql", expertise: "Expert", color: "#4169E1" },
    { name: "Terraform", logo: "terraform", expertise: "Certifié", color: "#7B42BC" },
    { name: "Git", logo: "git", expertise: "Expert", color: "#F05032" },
  ],
  "Cloud & Data Platforms": [
    { name: "Google Cloud", logo: "googlecloud", expertise: "Certifié Pro", color: "#4285F4" },
    { name: "BigQuery", logo: "googlebigquery", expertise: "Expert", color: "#669DF6" },
    { name: "Airflow", logo: "apacheairflow", expertise: "Expert", color: "#017CEE" },
    { name: "dbt", logo: "dbt", expertise: "Expert", color: "#FF694B" },
    { name: "Looker", logo: "looker", expertise: "Avancé", color: "#4285F4" },
    { name: "Pandas", logo: "pandas", expertise: "Intermédiaire", color: "#8B5CF6" },
  ],
  "DevOps & Infrastructure": [
    { name: "Docker", logo: "docker", expertise: "Avancé", color: "#2496ED" },
    { name: "Kubernetes", logo: "kubernetes", expertise: "Intermédiaire", color: "#326CE5" },
    { name: "CI/CD", logo: "githubactions", expertise: "Avancé", color: "#2088FF" },
  ]
};

const certifications = [
  { name: "GCP Professional Data Engineer", issuer: "Google Cloud", logo: "googlecloud", color: "#4285F4" },
  { name: "Terraform Associate", issuer: "HashiCorp", logo: "terraform", color: "#7B42BC" },
  { name: "GCP Associate Cloud Engineer", issuer: "Google Cloud", logo: "googlecloud", color: "#4285F4" },
  { name: "Professional Scrum Product Owner I", issuer: "Scrum.org", logo: "scrumalliance", color: "#009FDA" }
];

const experiences = [
  {
    title: "Agile Data Engineer",
    company: "SFEIR @ Galec",
    period: "Oct. 2022 - Présent",
    duration: "2+ ans",
    highlights: [
      "Conception et développement de pipelines data complexes (Python/SQL) sur GCP",
      "Architecture et orchestration de DAGs Airflow pour plusieurs TB de données",
      "Optimisation des performances Data Warehouse (BigQuery)",
      "Implémentation de pratiques DataOps/DevOps : CI/CD, monitoring, tests",
      "Stack: Python, BigQuery, Terraform, dbt, Airflow, Cloud Run, Pub/Sub"
    ]
  },
  {
    title: "Technical Lead Data",
    company: "DEVOTEAM @ L'Oréal",
    period: "Mai - Oct. 2022",
    duration: "6 mois",
    highlights: [
      "Leadership technique sur intégration de données SAP",
      "Audit et optimisation de l'architecture data existante",
      "Infrastructure as Code avec Terraform",
      "Stack: GCP, BigQuery, Python, Terraform, Workflows"
    ]
  },
  {
    title: "Data Engineer",
    company: "DEVOTEAM @ L'Oréal",
    period: "Sept. 2021 - Août 2022",
    duration: "1 an",
    highlights: [
      "Conception from scratch d'une architecture Data Warehouse sur GCP",
      "Développement de pipelines Python pour collecte multi-sources",
      "Modélisation de schémas dimensionnels et optimisation BigQuery",
      "Stack: BigQuery, Looker, Terraform, Cloud Functions, Python"
    ]
  },
  {
    title: "Data Scientist / Analyst",
    company: "Groupama",
    period: "Fév. 2020 - Sept. 2021",
    duration: "1 an 8 mois",
    highlights: [
      "Rôle transversal apportant expertise technique aux équipes pricing",
      "Modélisation des tarifs (habitation et 2/3 roues)",
      "Développement d'un outil de simulation du moteur tarifaire pour analyse de revalorisation",
      "Mise en place d'une plateforme de restitution des études de positionnement tarifaire",
      "Développement d'un outil de cartographie pour l'établissement d'un zonier tarifaire",
      "Stack: Databricks, Python, R, Shiny"
    ]
  }
];
```

<div class="hero-section">
  <div class="hero-content">
    <h1 class="hero-name">Sajid MAHYO</h1>
    <p class="hero-title">Senior Data Engineer</p>
    <div class="hero-tagline">
      Passionné par le data engineering, le cloud et le développement en général ☁️
    </div>
    <div class="hero-contact">
      <button id="email-btn" class="contact-link email-btn">
        📧 mahyosajid@gmail.com
      </button>
      <a href="https://www.linkedin.com/in/mahyosajid/" target="_blank" class="contact-link">
        💼 LinkedIn
      </a>
      <span class="contact-info">📍 Argenteuil, France</span>
    </div>
  </div>
</div>

```js
// Copy email to clipboard
const emailButton = document.getElementById('email-btn');
if (emailButton) {
  emailButton.addEventListener('click', async () => {
    const email = 'mahyosajid@gmail.com';
    try {
      await navigator.clipboard.writeText(email);
      const originalText = emailButton.innerHTML;
      emailButton.innerHTML = '✅ Email copié !';
      emailButton.style.background = 'rgba(52, 211, 153, 0.2)';

      setTimeout(() => {
        emailButton.innerHTML = originalText;
        emailButton.style.background = 'rgba(96, 165, 250, 0.1)';
      }, 2000);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  });
}
```

<div class="profile-section">
  <h2 class="section-title">👨‍💻 Profil</h2>
  <p class="profile-text">
    <strong>Senior Data Engineer avec 3+ ans d'expérience</strong> dans la conception et l'optimisation
    de pipelines data complexes et de modèles Data Warehouse. Expert en développement Python et
    architecture cloud GCP, spécialisé dans l'orchestration Airflow et les pratiques DataOps/DevOps
    (CI/CD, monitoring, tests). Passionné par la qualité des données, la documentation rigoureuse
    et la collaboration étroite avec les équipes métier.
  </p>
</div>

<h2 class="section-title">💼 Expérience Professionnelle</h2>

```js
html`<div class="timeline">
  ${experiences.map((exp, index) => html`
    <div class="timeline-item">
      <div class="timeline-marker"></div>
      <div class="timeline-content">
        <div class="experience-header">
          <div>
            <h3 class="experience-title">${exp.title}</h3>
            <p class="experience-company">${exp.company}</p>
          </div>
          <div class="experience-period">
            <span class="period-badge">${exp.period}</span>
            <span class="duration-badge">${exp.duration}</span>
          </div>
        </div>
        <ul class="experience-highlights">
          ${exp.highlights.map(highlight => html`<li>${highlight}</li>`)}
        </ul>
      </div>
    </div>
  `)}
</div>`
```

<h2 class="section-title">🏆 Certifications</h2>

```js
html`<div class="certifications-grid">
  ${certifications.map(cert => html`
    <div class="certification-card" style="border-color: ${cert.color};">
      <div class="cert-icon-wrapper">
        <img
          src="https://cdn.simpleicons.org/${cert.logo}/${cert.color.replace('#', '')}"
          alt="${cert.name}"
          class="cert-logo"
          onerror="this.src='https://cdn.simpleicons.org/${cert.logo}'"
        />
      </div>
      <div class="cert-content">
        <h4 class="cert-name">${cert.name}</h4>
        <p class="cert-issuer">${cert.issuer}</p>
      </div>
    </div>
  `)}
</div>`
```

<h2 class="section-title">🚀 Compétences Techniques</h2>

```js
html`<div class="skills-grid-categories">
  ${Object.entries(skillsCategories).map(([category, skills]) => html`
    <div class="skill-category-column">
      <h3 class="category-title">${category}</h3>
      <div class="category-skills">
        ${skills.map(skill => html`
          <div class="skill-chip" style="--skill-color: ${skill.color}">
            <img
              src="https://cdn.simpleicons.org/${skill.logo}/${skill.color.replace('#', '')}"
              alt="${skill.name}"
              class="skill-logo"
              onerror="this.src='https://cdn.simpleicons.org/${skill.logo}'"
            />
            <div class="skill-chip-content">
              <span class="skill-chip-name">${skill.name}</span>
              <span class="skill-chip-level">${skill.expertise}</span>
            </div>
          </div>
        `)}
      </div>
    </div>
  `)}
</div>`
```

<h2 class="section-title">🎓 Formation</h2>

<div class="education-grid">
  <div class="education-card">
    <div class="education-years">2017 - 2020</div>
    <h4 class="education-title">Ingénieur Data Science</h4>
    <p class="education-school">CY Tech (ex EISTI)</p>
  </div>
  <div class="education-card">
    <div class="education-years">2019 - 2020</div>
    <h4 class="education-title">Master 2 MODO</h4>
    <p class="education-school">Paris-Dauphine - Optimisation & Décision</p>
  </div>
</div>

<h2 class="section-title">🌍 Langues</h2>

<div class="languages-grid">
  <div class="language-item">
    <span class="language-flag">🇫🇷</span>
    <span class="language-name">Français</span>
    <span class="language-level">Natif</span>
  </div>
  <div class="language-item">
    <span class="language-flag">🇬🇧</span>
    <span class="language-name">Anglais</span>
    <span class="language-level">C1 (Professionnel)</span>
  </div>
  <div class="language-item">
    <span class="language-flag">🇪🇸</span>
    <span class="language-name">Espagnol</span>
    <span class="language-level">B1</span>
  </div>
</div>

<h2 class="section-title">🎮 Centres d'Intérêt</h2>

<div class="interests-grid">
  <div class="interest-card">
    <div class="interest-icon">🎮</div>
    <div class="interest-name">Jeux Vidéo</div>
  </div>
  <div class="interest-card">
    <div class="interest-icon">📸</div>
    <div class="interest-name">Photographie</div>
  </div>
  <div class="interest-card">
    <div class="interest-icon">🎬</div>
    <div class="interest-name">Cinéma</div>
  </div>
  <div class="interest-card">
    <div class="interest-icon">✈️</div>
    <div class="interest-name">Voyage</div>
  </div>
</div>

<style>
  /* Hero Section */
  .hero-section {
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.8) 0%, rgba(15, 23, 42, 0.9) 100%);
    backdrop-filter: blur(20px);
    border-radius: 24px;
    padding: 3rem 2rem;
    margin: 2rem 0;
    border: 2px solid rgba(96, 165, 250, 0.2);
    position: relative;
    overflow: hidden;
  }

  .hero-section::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 4px;
    background: linear-gradient(90deg, #60a5fa, #a78bfa, #f472b6);
  }

  .hero-content {
    text-align: center;
  }

  .hero-name {
    font-size: 3rem;
    font-weight: 900;
    background: linear-gradient(135deg, #60a5fa 0%, #a78bfa 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin: 0 0 0.5rem 0;
    letter-spacing: -0.02em;
  }

  .hero-title {
    font-size: 1.5rem;
    color: #cbd5e1;
    font-weight: 600;
    margin: 0 0 1rem 0;
  }

  .hero-tagline {
    font-size: 1.1rem;
    color: #94a3b8;
    margin: 0 0 2rem 0;
    font-style: italic;
  }

  .hero-contact {
    display: flex;
    gap: 2rem;
    justify-content: center;
    flex-wrap: wrap;
  }

  .contact-link {
    color: #60a5fa;
    text-decoration: none;
    font-weight: 600;
    transition: all 0.2s;
    padding: 0.5rem 1rem;
    border-radius: 8px;
    background: rgba(96, 165, 250, 0.1);
  }

  .contact-link:hover {
    background: rgba(96, 165, 250, 0.2);
    transform: translateY(-2px);
  }

  .email-btn {
    border: none;
    cursor: pointer;
    font-family: inherit;
    font-size: inherit;
  }

  .email-btn:active {
    transform: translateY(0);
  }

  .contact-info {
    color: #94a3b8;
    font-weight: 500;
  }

  /* Profile Section */
  .profile-section {
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.8) 100%);
    backdrop-filter: blur(20px);
    border-radius: 20px;
    padding: 2rem;
    margin: 2rem 0;
    border: 1px solid rgba(96, 165, 250, 0.15);
  }

  .profile-text {
    color: #e2e8f0;
    font-size: 1.05rem;
    line-height: 1.8;
    margin: 0;
  }

  /* Section Titles */
  .section-title {
    font-size: 1.75rem;
    font-weight: 700;
    color: #f1f5f9;
    margin: 3rem 0 1.5rem 0;
    padding-bottom: 0.75rem;
    border-bottom: 2px solid rgba(96, 165, 250, 0.2);
    position: relative;
  }

  .section-title::after {
    content: '';
    position: absolute;
    bottom: -2px;
    left: 0;
    width: 100px;
    height: 2px;
    background: linear-gradient(90deg, #60a5fa, transparent);
  }

  /* Skills Grid with Categories */
  .skills-grid-categories {
    display: flex;
    flex-direction: column;
    gap: 2.5rem;
    margin: 2rem 0;
  }

  .skill-category-column {
    display: flex;
    flex-direction: column;
    gap: 1rem;
  }

  .category-title {
    font-size: 1.1rem;
    font-weight: 700;
    color: #cbd5e1;
    margin: 0 0 0.5rem 0;
    padding-left: 0.5rem;
    border-left: 3px solid #60a5fa;
  }

  .category-skills {
    display: flex;
    flex-wrap: wrap;
    gap: 0.75rem;
  }

  .skill-chip {
    position: relative;
    display: inline-flex;
    align-items: center;
    gap: 0.6rem;
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.7) 0%, rgba(15, 23, 42, 0.85) 100%);
    backdrop-filter: blur(20px);
    border-radius: 10px;
    padding: 0.6rem 1rem;
    border: 1.5px solid rgba(96, 165, 250, 0.2);
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    cursor: pointer;
    white-space: nowrap;
  }

  .skill-chip::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 2px;
    background: var(--skill-color);
    border-radius: 10px 10px 0 0;
    opacity: 0;
    transition: opacity 0.3s ease;
  }

  .skill-chip:hover {
    transform: translateY(-3px);
    border-color: var(--skill-color);
    box-shadow: 0 6px 16px rgba(0, 0, 0, 0.25), 0 0 16px var(--skill-color);
  }

  .skill-chip:hover::before {
    opacity: 1;
  }

  .skill-logo {
    width: 22px;
    height: 22px;
    filter: drop-shadow(0 2px 4px rgba(0, 0, 0, 0.3));
    transition: transform 0.3s ease;
    flex-shrink: 0;
  }

  .skill-chip:hover .skill-logo {
    transform: scale(1.2) rotate(-5deg);
  }

  .skill-chip-content {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .skill-chip-name {
    color: #e2e8f0;
    font-weight: 600;
    font-size: 0.9rem;
  }

  .skill-chip-level {
    color: var(--skill-color);
    font-size: 0.7rem;
    font-weight: 600;
    padding: 0.15rem 0.5rem;
    background: rgba(0, 0, 0, 0.35);
    border-radius: 6px;
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }

  /* Timeline */
  .timeline {
    position: relative;
    margin: 2rem 0;
  }

  .timeline::before {
    content: '';
    position: absolute;
    left: 20px;
    top: 0;
    bottom: 0;
    width: 2px;
    background: linear-gradient(180deg, #60a5fa, #a78bfa, #f472b6);
  }

  .timeline-item {
    position: relative;
    padding-left: 80px;
    margin-bottom: 3rem;
  }

  .timeline-marker {
    position: absolute;
    left: 9px;
    top: 8px;
    width: 18px;
    height: 18px;
    border-radius: 50%;
    background: linear-gradient(135deg, #60a5fa, #a78bfa);
    border: 3px solid #0f172a;
    box-shadow: 0 0 0 4px rgba(96, 165, 250, 0.2);
  }

  .timeline-content {
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.8) 100%);
    backdrop-filter: blur(20px);
    border-radius: 16px;
    padding: 2rem;
    border: 1px solid rgba(96, 165, 250, 0.15);
    transition: all 0.3s ease;
  }

  .timeline-content:hover {
    border-color: rgba(96, 165, 250, 0.4);
    transform: translateX(8px);
    box-shadow: 0 8px 24px rgba(96, 165, 250, 0.15);
  }

  .experience-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 1rem;
    gap: 1rem;
    flex-wrap: wrap;
  }

  .experience-title {
    font-size: 1.3rem;
    font-weight: 700;
    color: #e2e8f0;
    margin: 0;
  }

  .experience-company {
    font-size: 1rem;
    color: #60a5fa;
    margin: 0.25rem 0 0 0;
  }

  .experience-period {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    align-items: flex-end;
  }

  .period-badge {
    background: linear-gradient(135deg, rgba(96, 165, 250, 0.2), rgba(167, 139, 250, 0.2));
    color: #cbd5e1;
    padding: 0.4rem 1rem;
    border-radius: 12px;
    font-size: 0.85rem;
    font-weight: 600;
    border: 1px solid rgba(96, 165, 250, 0.3);
  }

  .duration-badge {
    color: #94a3b8;
    font-size: 0.8rem;
    font-weight: 500;
  }

  .experience-highlights {
    list-style: none;
    padding: 0;
    margin-top: 1.5rem;
    max-height: 300px;
    overflow-y: auto;
    padding-right: 0.5rem;
  }

  .experience-highlights::-webkit-scrollbar {
    width: 6px;
  }

  .experience-highlights::-webkit-scrollbar-track {
    background: rgba(30, 41, 59, 0.5);
    border-radius: 3px;
  }

  .experience-highlights::-webkit-scrollbar-thumb {
    background: rgba(96, 165, 250, 0.5);
    border-radius: 3px;
  }

  .experience-highlights::-webkit-scrollbar-thumb:hover {
    background: rgba(96, 165, 250, 0.7);
  }

  .experience-highlights li {
    color: #cbd5e1;
    padding-left: 1.5rem;
    margin-bottom: 0.5rem;
    position: relative;
    line-height: 1.6;
  }

  .experience-highlights li::before {
    content: '▸';
    position: absolute;
    left: 0;
    color: #60a5fa;
    font-weight: bold;
  }

  /* Certifications Grid */
  .certifications-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 1.5rem;
    margin: 2rem 0;
  }

  .certification-card {
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.8) 100%);
    backdrop-filter: blur(20px);
    border-radius: 16px;
    padding: 1.5rem;
    border: 2px solid;
    display: flex;
    align-items: center;
    gap: 1rem;
    transition: all 0.3s ease;
  }

  .certification-card:hover {
    transform: translateY(-4px);
    box-shadow: 0 12px 24px rgba(0, 0, 0, 0.3);
  }

  .cert-icon-wrapper {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 60px;
    height: 60px;
    flex-shrink: 0;
  }

  .cert-logo {
    width: 48px;
    height: 48px;
    filter: drop-shadow(0 2px 8px rgba(0, 0, 0, 0.3));
  }

  .cert-content {
    flex: 1;
  }

  .cert-name {
    font-size: 1rem;
    font-weight: 700;
    color: #e2e8f0;
    margin: 0 0 0.25rem 0;
  }

  .cert-issuer {
    font-size: 0.85rem;
    color: #94a3b8;
    margin: 0;
  }

  /* Education Grid */
  .education-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 2rem;
    margin: 2rem 0;
  }

  .education-card {
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.8) 100%);
    backdrop-filter: blur(20px);
    border-radius: 16px;
    padding: 2rem;
    border: 1px solid rgba(96, 165, 250, 0.15);
    transition: all 0.3s ease;
  }

  .education-card:hover {
    border-color: rgba(96, 165, 250, 0.4);
    transform: translateY(-4px);
  }

  .education-years {
    color: #60a5fa;
    font-weight: 700;
    font-size: 0.9rem;
    margin-bottom: 0.75rem;
  }

  .education-title {
    font-size: 1.2rem;
    font-weight: 700;
    color: #e2e8f0;
    margin: 0 0 0.5rem 0;
  }

  .education-school {
    color: #94a3b8;
    font-size: 0.95rem;
    margin: 0;
  }

  /* Languages Grid */
  .languages-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1.5rem;
    margin: 2rem 0;
  }

  .language-item {
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.8) 100%);
    backdrop-filter: blur(20px);
    border-radius: 16px;
    padding: 1.5rem;
    border: 1px solid rgba(96, 165, 250, 0.15);
    display: flex;
    align-items: center;
    gap: 1rem;
    transition: all 0.3s ease;
  }

  .language-item:hover {
    border-color: rgba(96, 165, 250, 0.4);
    transform: scale(1.05);
  }

  .language-flag {
    font-size: 2rem;
  }

  .language-name {
    color: #e2e8f0;
    font-weight: 700;
    flex: 1;
  }

  .language-level {
    color: #60a5fa;
    font-size: 0.85rem;
    font-weight: 600;
  }

  /* Interests Grid */
  .interests-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 1.5rem;
    margin: 2rem 0;
  }

  .interest-card {
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.6) 0%, rgba(15, 23, 42, 0.8) 100%);
    backdrop-filter: blur(20px);
    border-radius: 16px;
    padding: 2rem 1.5rem;
    border: 1px solid rgba(96, 165, 250, 0.15);
    text-align: center;
    transition: all 0.3s ease;
  }

  .interest-card:hover {
    border-color: rgba(96, 165, 250, 0.4);
    transform: translateY(-8px) scale(1.05);
    box-shadow: 0 12px 24px rgba(96, 165, 250, 0.2);
  }

  .interest-icon {
    font-size: 3rem;
    margin-bottom: 0.75rem;
  }

  .interest-name {
    color: #e2e8f0;
    font-weight: 600;
    font-size: 1rem;
  }

  /* Responsive */
  @media (max-width: 768px) {
    .hero-name {
      font-size: 2rem;
    }

    .hero-title {
      font-size: 1.2rem;
    }

    .timeline::before {
      left: 15px;
    }

    .timeline-item {
      padding-left: 50px;
    }

    .timeline-marker {
      left: 7px;
    }

    .experience-header {
      flex-direction: column;
    }

    .experience-period {
      align-items: flex-start;
    }
  }
</style>
