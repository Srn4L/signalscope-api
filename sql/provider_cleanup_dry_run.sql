-- Provider-content cleanup, DRY RUN (read-only). Lists every businesses row,
-- whether it carries Google-derived content, which fields would be cleared,
-- and which identifiers and relationships are kept.
--
-- Provenance rule (per row, conservative): a row is Google-touched when ANY
-- of these hold, because only Google writes fill them: source is a Google
-- source; raw_data.platform is google_places/google_nearby; google_rating or
-- google_review_count is above 0 (other sources write 0); google_place_id is set. Upserts keep older
-- values when a newer write lacks them, so the latest source label alone
-- can't clear a row: any Google touch marks all provider fields as Google.
-- Rows never touched by Google (Geoapify, booking listings) are left as is.
WITH b AS (
  SELECT b.*,
    (b.source IN ('google_text','google_nearby','google_places','adjacent_category')
     OR b.raw_data->>'platform' IN ('google_places','google_nearby')
     OR COALESCE(b.google_rating, 0) > 0 OR COALESCE(b.google_review_count, 0) > 0
     OR b.google_place_id IS NOT NULL) AS google_touched,
    concat_ws(', ',
      CASE WHEN b.source IN ('google_text','google_nearby','google_places','adjacent_category') THEN 'source=' || b.source END,
      CASE WHEN b.raw_data->>'platform' IN ('google_places','google_nearby') THEN 'platform=' || (b.raw_data->>'platform') END,
      CASE WHEN COALESCE(b.google_rating, 0) > 0 OR COALESCE(b.google_review_count, 0) > 0 THEN 'rating/reviews set' END,
      CASE WHEN b.google_place_id IS NOT NULL THEN 'place_id set' END) AS evidence
  FROM businesses b
)
SELECT
  b.id, b.source, b.raw_data->>'platform' AS platform, (b.google_place_id IS NOT NULL) AS has_place_id,
  b.google_touched, b.evidence,
  CASE WHEN b.google_touched THEN concat_ws(', ',
      CASE WHEN COALESCE(b.name,'') <> '' THEN 'name' END,
      CASE WHEN COALESCE(b.address,'') <> '' THEN 'address' END,
      CASE WHEN COALESCE(b.phone,'') <> '' THEN 'phone' END,
      CASE WHEN COALESCE(b.website,'') <> '' THEN 'website' END,
      CASE WHEN b.google_rating IS NOT NULL THEN 'google_rating' END,
      CASE WHEN b.google_review_count IS NOT NULL THEN 'google_review_count' END,
      CASE WHEN b.raw_data ? 'score_reasons' THEN 'raw_data.score_reasons' END,
      CASE WHEN b.raw_data ? 'score_confidence' THEN 'raw_data.score_confidence' END)
  END AS fields_to_clear,
  CASE WHEN b.google_touched AND b.google_place_id IS NULL AND COALESCE(b.name,'') <> '' THEN
    'ik1:' || left(encode(sha256(convert_to(
      btrim(regexp_replace(lower(b.name), '[^a-z0-9]+', ' ', 'g')) || '|' ||
      btrim(regexp_replace(lower(COALESCE(b.location,'')), '[^a-z0-9]+', ' ', 'g')), 'UTF8')), 'hex'), 32)
  END AS identity_key_to_add,
  CASE WHEN b.google_place_id IS NOT NULL THEN 'place_id'
       WHEN b.google_touched THEN 'identity_key'
       ELSE 'name+location (unchanged)' END AS identity_after,
  (SELECT COUNT(*) FROM discovery_events d WHERE d.business_id = b.id) AS discovery_events_kept,
  (SELECT COUNT(*) FROM opportunities o WHERE o.business_id = b.id) AS opportunities_kept,
  b.category, b.location
FROM b
ORDER BY b.google_touched DESC, has_place_id, b.id;
