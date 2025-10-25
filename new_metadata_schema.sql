-- ============================================================================
-- UNIFIED GAME METADATA SCHEMA
-- Single source of truth for all game information and platform IDs
-- ============================================================================

-- Main game metadata table (enriched from IGDB + Steam)
CREATE TABLE IF NOT EXISTS game_metadata (
    -- Primary identifiers
    igdb_id INTEGER PRIMARY KEY,           -- IGDB ID (source of truth)
    game_name VARCHAR NOT NULL,            -- Canonical game name
    slug VARCHAR,                          -- URL-friendly slug

    -- Platform-specific IDs (for collectors)
    steam_app_id INTEGER,                  -- Steam application ID
    twitch_game_id VARCHAR,                -- Twitch game ID
    youtube_channel_id VARCHAR,            -- YouTube channel ID
    epic_id VARCHAR,                       -- Epic Games Store ID
    gog_id VARCHAR,                        -- GOG ID

    -- IGDB metadata
    igdb_summary TEXT,                     -- Game description
    igdb_rating REAL,                      -- Critic rating (0-100)
    igdb_aggregated_rating REAL,           -- Aggregated rating (0-100)
    igdb_total_rating_count INTEGER,       -- Number of ratings
    first_release_date TIMESTAMP,          -- First release date
    cover_url VARCHAR,                     -- Cover image URL

    -- Steam metadata (complementary)
    steam_description TEXT,                -- Steam short description
    steam_is_free BOOLEAN,                 -- Is free to play
    steam_price_cents INTEGER,             -- Current price in cents
    steam_metacritic_score INTEGER,        -- Metacritic score
    steam_required_age INTEGER,            -- Age rating

    -- Categories (JSON arrays)
    genres JSON,                           -- ["Strategy", "MOBA"]
    themes JSON,                           -- ["Action", "Fantasy"]
    platforms JSON,                        -- ["PC", "Linux", "Mac"]
    game_modes JSON,                       -- ["Multiplayer", "Co-op"]
    developers JSON,                       -- ["Valve"]
    publishers JSON,                       -- ["Valve"]

    -- External links (JSON object)
    websites JSON,                         -- {wikipedia: "...", twitter: "...", ...}

    -- Metadata
    discovery_source VARCHAR,              -- "igdb", "steamspy", "manual"
    discovery_date TIMESTAMP,              -- When first discovered
    last_updated TIMESTAMP,                -- Last metadata refresh
    is_active BOOLEAN DEFAULT true,        -- Still tracking this game?

    -- Tracking flags
    track_steam BOOLEAN DEFAULT true,      -- Collect Steam data
    track_twitch BOOLEAN DEFAULT true,     -- Collect Twitch data
    track_reddit BOOLEAN DEFAULT false,    -- Collect Reddit data (future)

    UNIQUE(steam_app_id),
    UNIQUE(twitch_game_id)
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_game_metadata_steam ON game_metadata(steam_app_id);
CREATE INDEX IF NOT EXISTS idx_game_metadata_twitch ON game_metadata(twitch_game_id);
CREATE INDEX IF NOT EXISTS idx_game_metadata_active ON game_metadata(is_active);

-- ============================================================================
-- EXTERNAL IDS MAPPING (normalized for future platforms)
-- ============================================================================

CREATE TABLE IF NOT EXISTS game_external_ids (
    id INTEGER PRIMARY KEY,
    igdb_id INTEGER NOT NULL,
    platform_name VARCHAR NOT NULL,        -- "steam", "twitch", "youtube", "epic", etc.
    platform_category INTEGER,             -- IGDB category code
    platform_uid VARCHAR NOT NULL,         -- External UID
    discovered_date TIMESTAMP,

    FOREIGN KEY (igdb_id) REFERENCES game_metadata(igdb_id),
    UNIQUE(platform_name, platform_uid)
);

-- ============================================================================
-- DISCOVERY HISTORY (audit trail)
-- ============================================================================

CREATE TABLE IF NOT EXISTS discovery_history (
    id INTEGER PRIMARY KEY,
    discovery_date TIMESTAMP NOT NULL,
    discovery_source VARCHAR NOT NULL,     -- "igdb_popular", "igdb_trending", "manual"
    games_discovered INTEGER,              -- Number of new games found
    games_updated INTEGER,                 -- Number of existing games updated
    execution_time_seconds REAL,
    notes TEXT
);

-- ============================================================================
-- VIEWS FOR EASY ACCESS
-- ============================================================================

-- Active games with all platform IDs
CREATE VIEW IF NOT EXISTS vw_active_games AS
SELECT
    igdb_id,
    game_name,
    steam_app_id,
    twitch_game_id,
    genres,
    first_release_date,
    igdb_rating,
    discovery_source,
    last_updated
FROM game_metadata
WHERE is_active = true;

-- Games missing platform IDs (need enrichment)
CREATE VIEW IF NOT EXISTS vw_games_missing_ids AS
SELECT
    igdb_id,
    game_name,
    CASE WHEN steam_app_id IS NULL THEN 'steam' ELSE NULL END as missing_steam,
    CASE WHEN twitch_game_id IS NULL THEN 'twitch' ELSE NULL END as missing_twitch,
    discovery_date
FROM game_metadata
WHERE is_active = true
  AND (steam_app_id IS NULL OR twitch_game_id IS NULL);

-- ============================================================================
-- MIGRATION HELPER
-- ============================================================================

-- Check if old schema exists
-- SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'game_metadata_old';
