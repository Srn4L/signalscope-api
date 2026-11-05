-- Provider-content cleanup, APPLY. Do not run until the dry run
-- (provider_cleanup_dry_run.sql) has been reviewed. One transaction.
-- Same provenance rule as the dry run. Keeps ids, google_place_id,
-- category, location, source, created_at and every relationship
-- (discovery_events, opportunities, opportunity_states use business_id).
BEGIN;

CREATE TEMP TABLE _touched ON COMMIT DROP AS
SELECT b.id
FROM businesses b
WHERE b.source IN ('google_text','google_nearby','google_places','adjacent_category')
   OR b.raw_data->>'platform' IN ('google_places','google_nearby')
   OR COALESCE(b.google_rating, 0) > 0 OR COALESCE(b.google_review_count, 0) > 0
   OR b.google_place_id IS NOT NULL;

-- Rows without a place id keep their Yelhao id and every relationship; no
-- identity key is derived from the provider name (not established as
-- permitted). Automatic matching for them waits for a permitted identifier.

-- 2. Google-derived business fields
UPDATE businesses b
SET name = '', address = NULL, phone = NULL, website = NULL,
    google_rating = NULL, google_review_count = NULL,
    raw_data = COALESCE(b.raw_data, '{}'::jsonb) - 'score_reasons' - 'score_confidence',
    updated_at = NOW()
WHERE b.id IN (SELECT id FROM _touched);

-- 3. Networks opportunities on those businesses: every field outside the
--    allowlist (business_id, search_id, opportunity_type, service_angle,
--    user_role; source_rules.OPPORTUNITY_ALLOWED) is built from provider data
UPDATE opportunities o
SET matched_offer = NULL, problem_detected = NULL, why_it_fits = NULL, why_now = NULL,
    opportunity_fit_score = NULL, saturation_score = NULL, contactability_score = NULL,
    source_diversity_score = NULL, signals = NULL, risk_flags = NULL, recommended_offer = NULL,
    best_contact_path = NULL, suggested_message = NULL, suggested_follow_up = NULL,
    contact_target = NULL, contact_confidence = NULL, contact_reason = NULL, updated_at = NOW()
WHERE o.business_id IN (SELECT id FROM _touched);

COMMIT;
