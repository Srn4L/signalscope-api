-- schema.sql
-- Yelhao persistence layer — run once via POST /init-db
-- All statements use CREATE TABLE IF NOT EXISTS so this is safe to re-run.
-- No tables are ever dropped here.

-- ─────────────────────────────────────────────
-- businesses
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS businesses (
    id                  SERIAL PRIMARY KEY,
    name                TEXT NOT NULL,
    category            TEXT,
    location            TEXT,
    address             TEXT,
    phone               TEXT,
    website             TEXT,
    google_place_id     TEXT UNIQUE,
    google_rating       NUMERIC,
    google_review_count INTEGER,
    instagram_url       TEXT,
    facebook_url        TEXT,
    source              TEXT,
    raw_data            JSONB,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- searches
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS searches (
    id                   SERIAL PRIMARY KEY,
    user_query           TEXT NOT NULL,
    mode                 TEXT,
    offer                TEXT,
    target_business_type TEXT,
    location             TEXT,
    filters              JSONB,
    created_at           TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- opportunities
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS opportunities (
    id                    SERIAL PRIMARY KEY,
    business_id           INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
    search_id             INTEGER REFERENCES searches(id)   ON DELETE SET NULL,
    opportunity_type      TEXT,
    matched_offer         TEXT,
    problem_detected      TEXT,
    why_it_fits           TEXT,
    why_now               TEXT,
    opportunity_fit_score INTEGER,
    saturation_score      INTEGER,
    contactability_score  INTEGER,
    source_diversity_score INTEGER,
    signals               JSONB,
    risk_flags            JSONB,
    recommended_offer     TEXT,
    best_contact_path     TEXT,
    suggested_message     TEXT,
    suggested_follow_up   TEXT,
    created_at            TIMESTAMP DEFAULT NOW(),
    updated_at            TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- opportunity_states
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS opportunity_states (
    id               SERIAL PRIMARY KEY,
    opportunity_id   INTEGER REFERENCES opportunities(id) ON DELETE CASCADE,
    status           TEXT DEFAULT 'new',
    contact_method   TEXT,
    last_action      TEXT,
    next_action      TEXT,
    next_action_date TIMESTAMP,
    notes            TEXT,
    message_used     TEXT,
    response_status  TEXT,
    outcome          TEXT,
    created_at       TIMESTAMP DEFAULT NOW(),
    updated_at       TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- do_not_contact
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS do_not_contact (
    id          SERIAL PRIMARY KEY,
    business_id INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
    reason      TEXT,
    source      TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- ADDITIONS — token-scoped Networks + discovery
-- Safe to run multiple times (IF NOT EXISTS /
-- column guard handled by existence check).
-- ─────────────────────────────────────────────

-- token_hash on opportunity_states
-- (guarded: ALTER TABLE IF NOT EXISTS column not available in older PG;
--  use a DO block so it is idempotent)
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='opportunity_states' AND column_name='token_hash'
    ) THEN
        ALTER TABLE opportunity_states ADD COLUMN token_hash TEXT;
    END IF;
END $$;

-- token_hash on searches (lets us know which user ran which search)
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='searches' AND column_name='token_hash'
    ) THEN
        ALTER TABLE searches ADD COLUMN token_hash TEXT;
    END IF;
END $$;

-- updated_at on opportunity_states (needed for PATCH)
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='opportunity_states' AND column_name='updated_at'
    ) THEN
        ALTER TABLE opportunity_states ADD COLUMN updated_at TIMESTAMP DEFAULT NOW();
    END IF;
END $$;

-- discovery_events — tracks what Scout showed, never creates Network entries
CREATE TABLE IF NOT EXISTS discovery_events (
    id             SERIAL PRIMARY KEY,
    token_hash     TEXT,
    search_id      INTEGER REFERENCES searches(id)   ON DELETE SET NULL,
    business_id    INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
    opportunity_id INTEGER,
    mode           TEXT,
    niche          TEXT,
    location       TEXT,
    shown_at       TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_opp_states_token  ON opportunity_states(token_hash);
CREATE INDEX IF NOT EXISTS idx_discovery_token   ON discovery_events(token_hash);
CREATE INDEX IF NOT EXISTS idx_discovery_biz     ON discovery_events(business_id);
CREATE INDEX IF NOT EXISTS idx_discovery_search  ON discovery_events(search_id);
