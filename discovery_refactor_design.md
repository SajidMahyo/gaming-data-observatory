# Discovery & Metadata Refactor Design

## 🎯 Objectif

Remplacer le système actuel (SteamSpy discovery + metadata séparées) par un système unifié basé sur IGDB avec enrichissement multi-sources.

## 📊 Architecture actuelle (à remplacer)

```
SteamSpy API → config/games.json (Steam IDs only)
                     ↓
Steam Collector → data/raw/steam/*.parquet
                     ↓
Steam Store API → data/raw/metadata/{steam_id}.json
                     ↓
            DuckDB (tables séparées)
```

**Problèmes :**
- ❌ Pas d'ID universel (dépend de Steam)
- ❌ Pas de Twitch ID mappé → matching par nom fragile
- ❌ Métadonnées Steam uniquement
- ❌ Discovery limité à SteamSpy

## 🎨 Nouvelle architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    IGDB API Discovery                       │
│  • Popular games (by rating count)                         │
│  • Trending games (future: by recent rating growth)        │
│  • Returns: IGDB ID + basic info                           │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│              Metadata Enrichment Pipeline                   │
│                                                             │
│  For each new game discovered:                             │
│  1. Get IGDB full metadata                                 │
│  2. Get IGDB external IDs (Steam, Twitch, Epic, etc.)     │
│  3. If has Steam ID → enrich with Steam Store API         │
│  4. Store everything in game_metadata table                │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────────┐
│              DuckDB: game_metadata (single source)          │
│                                                             │
│  igdb_id (PK) | steam_app_id | twitch_game_id | ...       │
│  genres       | rating        | cover_url       | ...      │
└──────────────────────┬──────────────────────────────────────┘
                       ↓
         ┌─────────────┴──────────────┐
         ↓                            ↓
┌──────────────────┐        ┌──────────────────┐
│ Steam Collector  │        │ Twitch Collector │
│ (use steam_id)   │        │ (use twitch_id)  │
└──────────────────┘        └──────────────────┘
```

## 🔧 Composants à créer/modifier

### 1. **IGDB Collector** (nouveau)
**Fichier:** `python/collectors/igdb.py`

```python
class IGDBCollector:
    def discover_popular_games(limit: int) -> List[dict]:
        """Discover popular games by rating count"""

    def get_game_details(igdb_id: int) -> dict:
        """Get full metadata for a game"""

    def get_external_ids(igdb_id: int) -> dict:
        """Get all platform IDs (Steam, Twitch, etc.)"""
```

### 2. **Metadata Enrichment Service** (nouveau)
**Fichier:** `python/services/metadata_enrichment.py`

```python
class MetadataEnrichmentService:
    def enrich_game(igdb_id: int) -> dict:
        """
        Full enrichment pipeline:
        1. IGDB metadata
        2. IGDB external IDs
        3. Steam Store metadata (if Steam ID exists)
        4. Return unified dict
        """
```

### 3. **DuckDB Manager** (à modifier)
**Fichier:** `python/storage/duckdb_manager.py`

Ajouter méthodes:
- `create_game_metadata_table_v2()`
- `upsert_game_metadata_v2(data: dict)`
- `get_games_for_platform(platform: str)` → retourne jeux avec IDs mappés
- `migrate_from_old_schema()`

### 4. **CLI Commands** (à modifier)
**Fichier:** `python/main.py`

```python
# Remplacer
@cli.command()
def discover(...):
    """Old SteamSpy discovery"""

# Par
@cli.command()
def discover(...):
    """
    IGDB-based discovery with metadata enrichment
    - Discover popular games from IGDB
    - Enrich with all platform IDs
    - Store in unified game_metadata table
    """
```

### 5. **Steam Collector** (à modifier)
**Fichier:** `python/collectors/steam.py`

```python
# Avant
games = load_from_config("config/games.json")

# Après
games = db.get_games_for_platform("steam")
# Returns: {steam_app_id: game_name, ...}
```

### 6. **Twitch Collector** (à modifier)
**Fichier:** `python/collectors/twitch.py`

```python
# Avant
twitch_id = search_by_name(game_name)  # Fragile!

# Après
games_with_twitch = db.get_games_for_platform("twitch")
# Returns: {twitch_game_id: game_name, ...}
# Direct lookup, no search needed!
```

## 📅 Plan d'implémentation (ordre)

### Phase 1: Infrastructure (✅ Base solide)
1. ✅ Créer nouveau schéma DuckDB
2. ⬜ Ajouter méthodes DuckDB pour nouveau schéma
3. ⬜ Créer IGDB collector
4. ⬜ Créer metadata enrichment service

### Phase 2: Discovery (🔄 Nouveau système)
1. ⬜ Implémenter IGDB discovery dans CLI
2. ⬜ Tester discovery + enrichment pipeline
3. ⬜ Découvrir ~100 jeux populaires

### Phase 3: Migration (🔁 Transition)
1. ⬜ Script de migration ancien → nouveau schéma
2. ⬜ Vérifier que tous les jeux actuels sont migrés
3. ⬜ Backup des anciennes tables

### Phase 4: Collectors (🔧 Mise à jour)
1. ⬜ Modifier Steam collector
2. ⬜ Modifier Twitch collector
3. ⬜ Tester pipeline complet

### Phase 5: Aggregator & Dashboard (📊 Affichage)
1. ⬜ Modifier aggregator pour lire nouveau schéma
2. ⬜ Tester exports JSON
3. ⬜ Vérifier dashboard Observable

### Phase 6: Cleanup (🧹 Nettoyage)
1. ⬜ Supprimer ancien code discovery SteamSpy
2. ⬜ Supprimer config/games.json
3. ⬜ Mettre à jour documentation

## 🔑 Décisions de design

### ID Pivot: `igdb_id`
- ✅ IGDB couvre tous les jeux (pas que Steam)
- ✅ Base de données universelle
- ✅ Metadata riches (genres, ratings, etc.)
- ✅ External IDs pour toutes les plateformes

### Enrichissement progressif
- Pas besoin de tout avoir d'un coup
- Si Twitch ID manque → NULL (on enrichira plus tard)
- Flag `is_active` pour désactiver les jeux

### Backwards compatibility
- Garder `steam_app_id` comme unique constraint
- Aggregator lit depuis nouvelle table mais même format
- Dashboard ne change pas (exports JSON identiques)

## 📝 Format de métadonnées unifiées

**Avant** (`data/raw/metadata/{steam_id}.json`):
```json
{
  "app_id": 570,
  "name": "Dota 2",
  "type": "game",
  "developers": ["Valve"],
  "genres": ["Strategy", "Free to Play"],
  "is_free": true
}
```

**Après** (DuckDB `game_metadata`):
```json
{
  "igdb_id": 2963,
  "game_name": "Dota 2",
  "steam_app_id": 570,
  "twitch_game_id": "32887",
  "genres": ["Strategy", "MOBA"],
  "themes": ["Action", "Fantasy"],
  "igdb_rating": 79.5,
  "steam_is_free": true,
  "developers": ["Valve"],
  "cover_url": "https://..."
}
```

**Avantages:**
- ✅ Toutes les IDs au même endroit
- ✅ Métadonnées enrichies (IGDB + Steam)
- ✅ Query facile pour collectors
- ✅ Une seule source de vérité

## 🎯 Résultat final

**Workflow simplifié:**
```bash
# 1. Discovery (weekly via IGDB)
uv run python -m python.main discover --source igdb --limit 100

# 2. Collection (hourly)
uv run python -m python.main collect       # Steam (uses steam_app_id from DB)
uv run python -m python.main twitch-collect  # Twitch (uses twitch_game_id from DB)

# 3. Aggregation (hourly)
uv run python -m python.main aggregate
```

**Bénéfices:**
- ✅ Discovery universel (pas limité à Steam)
- ✅ IDs mappés automatiquement
- ✅ Metadata riches
- ✅ Extensible (Reddit, YouTube Gaming, etc.)
- ✅ Future-proof
