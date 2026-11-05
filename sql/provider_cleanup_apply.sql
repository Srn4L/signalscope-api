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

-- 1. Identity for rows without a place id, computed before the name is removed
--    (must equal source_rules.identity_key)
UPDATE businesses b
SET raw_data = COALESCE(b.raw_data, '{}'::jsonb) || jsonb_build_object('identity_key',
      'ik1:' || left(encode(sha256(convert_to(
        btrim(regexp_replace(lower(b.name), '[^a-z0-9]+', ' ', 'g')) || '|' ||
        btrim(regexp_replace(lower(COALESCE(b.location,'')), '[^a-z0-9]+', ' ', 'g')), 'UTF8')), 'hex'), 32))
WHERE b.id IN (SELECT id FROM _touched)
  AND b.google_place_id IS NULL AND COALESCE(b.name, '') <> '';

-- 2. Google-derived business fields
UPDATE businesses b
SET name = '', address = NULL, phone = NULL, website = NULL,
    google_rating = NULL, google_review_count = NULL,
    raw_data = COALESCE(b.raw_data, '{}'::jsonb) - 'score_reasons' - 'score_confidence',
    updated_at = NOW()
WHERE b.id IN (SELECT id FROM _touched);

-- 3. Networks opportunities on those businesses: text and scores built from
--    provider data (reason text, problem, fit score, contact reasoning)
UPDATE opportunities o
SET problem_detected = NULL, why_it_fits = NULL, opportunity_fit_score = NULL,
    signals = NULL, best_contact_path = NULL, contact_reason = NULL,
    suggested_message = NULL, updated_at = NOW()
WHERE o.business_id IN (SELECT id FROM _touched);

COMMIT;
