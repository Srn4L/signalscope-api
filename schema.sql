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
