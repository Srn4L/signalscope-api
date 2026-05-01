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

-- token_hash and audit columns — ADD COLUMN IF NOT EXISTS is safe to re-run
ALTER TABLE opportunity_states ADD COLUMN IF NOT EXISTS token_hash  TEXT;
ALTER TABLE opportunity_states ADD COLUMN IF NOT EXISTS updated_at  TIMESTAMP DEFAULT NOW();
ALTER TABLE opportunity_states ADD COLUMN IF NOT EXISTS created_at  TIMESTAMP DEFAULT NOW();
ALTER TABLE searches            ADD COLUMN IF NOT EXISTS token_hash  TEXT;

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

-- ─────────────────────────────────────────────
-- PHASE 1 additions — intelligence layer
-- All idempotent. Never drops anything.
-- ─────────────────────────────────────────────

-- opportunities — service / contact intelligence columns
ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS service_angle      TEXT;
ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS user_role          TEXT;
ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS contact_target     TEXT;
ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS contact_confidence TEXT;
ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS contact_reason     TEXT;

-- opportunity_states — richer contact tracking
ALTER TABLE opportunity_states ADD COLUMN IF NOT EXISTS contact_method TEXT;
ALTER TABLE opportunity_states ADD COLUMN IF NOT EXISTS contact_target TEXT;
ALTER TABLE opportunity_states ADD COLUMN IF NOT EXISTS service_angle  TEXT;

-- business_contacts — extracted contact candidates per business
CREATE TABLE IF NOT EXISTS business_contacts (
    id             SERIAL PRIMARY KEY,
    business_id    INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
    contact_name   TEXT,
    contact_role   TEXT,
    contact_method TEXT,
    contact_value  TEXT,
    source         TEXT,
    confidence     TEXT,
    reason         TEXT,
    raw_data       JSONB,
    created_at     TIMESTAMP DEFAULT NOW(),
    updated_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE (business_id, contact_method, contact_value)
);

-- opportunity_saturation — tracks how crowded each angle/path is
CREATE TABLE IF NOT EXISTS opportunity_saturation (
    id                   SERIAL PRIMARY KEY,
    business_id          INTEGER REFERENCES businesses(id) ON DELETE CASCADE,
    opportunity_type     TEXT,
    service_angle        TEXT,
    contact_target       TEXT,
    contact_method       TEXT,
    total_discoveries    INTEGER DEFAULT 0,
    unique_discoverers   INTEGER DEFAULT 0,
    total_saves          INTEGER DEFAULT 0,
    unique_savers        INTEGER DEFAULT 0,
    total_contacted      INTEGER DEFAULT 0,
    total_replied        INTEGER DEFAULT 0,
    total_won            INTEGER DEFAULT 0,
    total_lost           INTEGER DEFAULT 0,
    last_discovered_at   TIMESTAMP,
    last_saved_at        TIMESTAMP,
    last_contacted_at    TIMESTAMP,
    last_updated_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (business_id, opportunity_type, service_angle, contact_target, contact_method)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_contacts_business ON business_contacts(business_id);
CREATE INDEX IF NOT EXISTS idx_contacts_method   ON business_contacts(contact_method);
CREATE INDEX IF NOT EXISTS idx_contacts_role     ON business_contacts(contact_role);

CREATE INDEX IF NOT EXISTS idx_sat_business ON opportunity_saturation(business_id);
CREATE INDEX IF NOT EXISTS idx_sat_angle    ON opportunity_saturation(service_angle);
CREATE INDEX IF NOT EXISTS idx_sat_contact  ON opportunity_saturation(contact_method);
CREATE INDEX IF NOT EXISTS idx_sat_lookup   ON opportunity_saturation(business_id, opportunity_type, service_angle, contact_target, contact_method);
