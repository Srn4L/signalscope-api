-- Yelhao: Power BI / SQL reporting setup
-- Run ONCE as the main database user (yelhao_user):
--   Render dashboard -> YelhaO-db -> Connect -> "PSQL Command", paste into a terminal,
--   then paste this whole file.
-- Before running: replace CHANGE_ME_LONG_PASSWORD with a long password you choose.
-- Safe to re-run: the view is replaced, and the role step is skipped if it exists.

-- 1. A reporting view with friendly names and the user-facing tiers.
CREATE OR REPLACE VIEW bi_daily_usage AS
SELECT
    day,
    CASE WHEN kind LIKE 'api\_%' THEN 'API call' ELSE 'User action' END AS category,
    CASE kind
        WHEN 'search'             THEN 'Searches'
        WHEN 'analysis'           THEN 'Deep analyses'
        WHEN 'batch_item'         THEN 'Batch analyses'
        WHEN 'api_google_search'  THEN 'Google Places search'
        WHEN 'api_google_details' THEN 'Google Places details'
        WHEN 'api_google_geocode' THEN 'Google Geocoding'
        WHEN 'api_serpapi'        THEN 'SerpAPI'
        WHEN 'api_tavily'         THEN 'Tavily'
        WHEN 'api_scraperapi'     THEN 'ScraperAPI'
        WHEN 'api_geoapify'       THEN 'Geoapify'
        ELSE kind
    END AS source,
    -- How the source would be packaged for users (see the plan doc, Revision 4)
    CASE
        WHEN kind = 'api_geoapify'                        THEN 'Basic map search'
        WHEN kind LIKE 'api_google%'                      THEN 'Verified ratings and reviews'
        WHEN kind IN ('api_tavily', 'api_serpapi', 'api_scraperapi') THEN 'Web research'
        WHEN kind = 'analysis'                            THEN 'Deep AI report'
        WHEN kind = 'search'                              THEN 'Search'
        ELSE 'Other'
    END AS user_tier,
    -- Every action is counted once under "All users" and again under who made it.
    -- For totals, filter who = 'All users'.
    CASE scope
        WHEN 'global'  THEN 'All users'
        WHEN 'demo_ip' THEN 'Demo visitors'
        WHEN 'code'    THEN 'Access codes'
        ELSE scope
    END AS who,
    SUM(count) AS total
FROM usage_counters
GROUP BY day, kind, scope;

-- 2. A read-only login for Power BI (can only read the view above).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'powerbi_reader') THEN
        CREATE ROLE powerbi_reader LOGIN PASSWORD 'CHANGE_ME_LONG_PASSWORD';
    END IF;
END $$;
GRANT CONNECT ON DATABASE yelhao TO powerbi_reader;
GRANT USAGE ON SCHEMA public TO powerbi_reader;
GRANT SELECT ON bi_daily_usage TO powerbi_reader;

-- 3. Check it worked:
SELECT * FROM bi_daily_usage WHERE who = 'All users' ORDER BY day DESC, category, source;
