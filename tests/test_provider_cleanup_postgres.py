"""Provider-content rules on businesses / Networks saves, and the cleanup scripts.

Runs only when JOURNEY_TEST_DATABASE_URL points at a disposable database
(never production). Skipped otherwise. Tables mirror the live schema.
"""
import os
import re

import pytest

URL = os.environ.get("JOURNEY_TEST_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not URL, reason="set JOURNEY_TEST_DATABASE_URL to a disposable Postgres")

HERE = os.path.dirname(__file__)
SQL_DIR = os.path.join(HERE, "..", "sql")

SCHEMA = [
    "DROP TABLE IF EXISTS opportunity_saturation, opportunity_states, opportunities, discovery_events, businesses CASCADE",
    """CREATE TABLE businesses (
        id SERIAL PRIMARY KEY, name TEXT NOT NULL, category TEXT, location TEXT, address TEXT,
        phone TEXT, website TEXT, google_place_id TEXT UNIQUE, google_rating NUMERIC,
        google_review_count INTEGER, instagram_url TEXT, facebook_url TEXT, source TEXT, raw_data JSONB,
        created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())""",
    """CREATE TABLE discovery_events (
        id SERIAL PRIMARY KEY, token_hash TEXT, search_id INTEGER, business_id INTEGER REFERENCES businesses(id),
        opportunity_id INTEGER, mode TEXT, niche TEXT, location TEXT, shown_at TIMESTAMP DEFAULT NOW())""",
    """CREATE TABLE opportunities (
        id SERIAL PRIMARY KEY, business_id INTEGER REFERENCES businesses(id), search_id INTEGER,
        opportunity_type TEXT, matched_offer TEXT, problem_detected TEXT, why_it_fits TEXT, why_now TEXT,
        opportunity_fit_score INTEGER, saturation_score INTEGER, contactability_score INTEGER,
        source_diversity_score INTEGER, signals JSONB, risk_flags JSONB, recommended_offer TEXT,
        best_contact_path TEXT, suggested_message TEXT, suggested_follow_up TEXT,
        created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW(), service_angle TEXT,
        user_role TEXT, contact_target TEXT, contact_confidence TEXT, contact_reason TEXT)""",
    """CREATE TABLE opportunity_states (
        id SERIAL PRIMARY KEY, opportunity_id INTEGER REFERENCES opportunities(id), status TEXT,
        contact_method TEXT, last_action TEXT, next_action TEXT, next_action_date TIMESTAMP, notes TEXT,
        message_used TEXT, response_status TEXT, outcome TEXT, created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(), token_hash TEXT, contact_target TEXT, service_angle TEXT)""",
    """CREATE TABLE opportunity_saturation (
        id SERIAL PRIMARY KEY, business_id INTEGER, opportunity_type TEXT, service_angle TEXT,
        contact_target TEXT, contact_method TEXT, total_discoveries INTEGER, unique_discoverers INTEGER,
        total_saves INTEGER, unique_savers INTEGER, total_contacted INTEGER, total_replied INTEGER,
        total_won INTEGER, total_lost INTEGER, last_discovered_at TIMESTAMP, last_saved_at TIMESTAMP,
        last_contacted_at TIMESTAMP, last_updated_at TIMESTAMP,
        UNIQUE (business_id, opportunity_type, service_angle, contact_target, contact_method))""",
]


@pytest.fixture
def pg(monkeypatch):
    before = os.environ.get("DATABASE_URL")
    monkeypatch.setenv("DATABASE_URL", URL)
    import importlib
    import db
    importlib.reload(db)
    import database_service as ds
    importlib.reload(ds)
    from sqlalchemy import text
    with db.get_db_session() as s:
        for stmt in SCHEMA:
            s.execute(text(stmt))
    yield ds, db, text
    if before is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = before
    importlib.reload(db)
    importlib.reload(ds)


def _legacy_rows(db, text):
    """Rows as the old code wrote them (Google content stored)."""
    with db.get_db_session() as s:
        ids = {}
        for key, sql, params in [
            ("g_pid", "INSERT INTO businesses (name, category, location, address, phone, website, google_place_id, "
                      "google_rating, google_review_count, source, raw_data) VALUES ('Chris Spa & Nails', 'nail salon', "
                      "'Astoria, NY', '21-34 Broadway', '(718) 555-0101', 'https://chris.example', 'pid-1', 4.9, 187, "
                      "'google_text', CAST(:raw AS JSONB)) RETURNING id",
             {"raw": '{"platform": "google_places", "score_reasons": [{"signal": "4.9 stars"}], "score_confidence": "high"}'}),
            ("g_nopid", "INSERT INTO businesses (name, category, location, address, google_rating, google_review_count, "
                        "source, raw_data) VALUES ('Mama G. African Kitchen', 'restaurant', 'Bronx, NY', '1322A E Gun Hill Rd', "
                        "4.2, 466, 'google_text', CAST(:raw AS JSONB)) RETURNING id",
             {"raw": '{"platform": "google_places", "score_reasons": [{"signal": "466 reviews"}]}'}),
            ("geo", "INSERT INTO businesses (name, category, location, address, google_rating, google_review_count, source, "
                    "raw_data) VALUES ('Camila Nails Salon', 'nail salon', 'Astoria, NY', '30-01 36th Ave', 0, 0, 'geoapify', "
                    "CAST(:raw AS JSONB)) RETURNING id", {"raw": '{"platform": "geoapify"}'}),
            ("booking", "INSERT INTO businesses (name, location, website, google_rating, google_review_count, source, raw_data) "
                        "VALUES ('Glow Nails', 'Astoria, NY', 'https://fresha.com/glow', 0, 0, 'booking_platform', "
                        "CAST(:raw AS JSONB)) RETURNING id", {"raw": '{"platform": "fresha"}'}),
        ]:
            ids[key] = s.execute(text(sql), params).scalar()
        s.execute(text("INSERT INTO discovery_events (business_id, token_hash) VALUES (:a, 't'), (:b, 't')"),
                  {"a": ids["g_pid"], "b": ids["g_nopid"]})
        opp = s.execute(text("INSERT INTO opportunities (business_id, problem_detected, why_it_fits, signals, "
                             "opportunity_fit_score, best_contact_path, contact_reason) VALUES (:b, '466 reviews', "
                             "'4.2 stars', CAST('{\"score_reasons\": []}' AS JSONB), 61, 'phone', 'Call (718)') RETURNING id"),
                        {"b": ids["g_nopid"]}).scalar()
        s.execute(text("INSERT INTO opportunity_states (opportunity_id, status, token_hash) VALUES (:o, 'saved', 't')"),
                  {"o": opp})
        ids["opp"] = opp
    return ids


def _run_sql(db, name):
    sql = open(os.path.join(SQL_DIR, name)).read()
    from sqlalchemy import text
    with db.get_db_session() as s:
        if "apply" in name:
            body = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
            body = body.replace("BEGIN;", "").replace("COMMIT;", "")
            for stmt in [x.strip() for x in re.split(r";\s*\n", body) if x.strip()]:
                s.execute(text(stmt))
            return None
        return [dict(r._mapping) for r in s.execute(text(sql)).fetchall()]


def test_dry_run_lists_fields_provenance_and_kept_links(pg):
    ds, db, text = pg
    ids = _legacy_rows(db, text)
    rows = {r["id"]: r for r in _run_sql(db, "provider_cleanup_dry_run.sql")}
    g, n, geo, bk = rows[ids["g_pid"]], rows[ids["g_nopid"]], rows[ids["geo"]], rows[ids["booking"]]
    assert g["google_touched"] and "place_id set" in g["evidence"] and g["identity_after"] == "place_id"
    assert g["fields_to_clear"] == ("name, address, phone, website, google_rating, google_review_count, "
                                    "raw_data.score_reasons, raw_data.score_confidence")
    assert n["identity_after"].startswith("yelhao id only")
    assert n["discovery_events_kept"] == 1 and n["opportunities_kept"] == 1 and n["opportunities_to_clear"] == 1
    assert "identity_key_to_add" not in n
    assert not geo["google_touched"] and geo["fields_to_clear"] is None
    assert not bk["google_touched"] and bk["identity_after"] == "name+location (unchanged)"


def _row(db, text, table, i):
    with db.get_db_session() as s:
        return dict(s.execute(text(f"SELECT * FROM {table} WHERE id = :i"), {"i": i}).mappings().one())


def _assert_no_google(row):
    assert row["name"] == "" and row["address"] is None and row["phone"] is None and row["website"] is None
    assert row["google_rating"] is None and row["google_review_count"] is None
    assert "score_reasons" not in (row["raw_data"] or {}) and "score_confidence" not in (row["raw_data"] or {})


def test_apply_clears_google_fields_keeps_ids_links_and_independent_rows(pg):
    ds, db, text = pg
    ids = _legacy_rows(db, text)
    _run_sql(db, "provider_cleanup_apply.sql")
    g, n, geo = (_row(db, text, "businesses", ids[k]) for k in ("g_pid", "g_nopid", "geo"))
    for row in (g, n):
        _assert_no_google(row)
        assert row["category"] and row["location"]
    assert g["google_place_id"] == "pid-1" and "identity_key" not in (n["raw_data"] or {})
    assert geo["name"] == "Camila Nails Salon" and geo["address"] == "30-01 36th Ave"     # independent: kept
    opp = _row(db, text, "opportunities", ids["opp"])
    assert opp["business_id"] == ids["g_nopid"]
    for f in ("problem_detected", "why_it_fits", "opportunity_fit_score", "signals", "best_contact_path",
              "contact_reason", "contact_target", "suggested_message", "suggested_follow_up"):
        assert opp[f] is None, f
    with db.get_db_session() as s:
        assert s.execute(text("SELECT COUNT(*) FROM discovery_events")).scalar() == 2
        assert s.execute(text("SELECT COUNT(*) FROM opportunity_states WHERE opportunity_id = :o"),
                         {"o": ids["opp"]}).scalar() == 1


def _scout_write(ds, **over):
    data = {"name": "Chris Spa & Nails", "category": "nail salon", "location": "Astoria, NY",
            "address": "21-34 Broadway", "phone": "(718) 555-0199", "website": "https://chris.example",
            "google_place_id": "pid-1", "google_rating": 4.9, "google_review_count": 187, "source": "google_text",
            "raw_data": {"platform": "google_places", "score_reasons": [{"signal": "187 reviews"}],
                         "score_confidence": "high", "website_checked": True}}
    data.update(over)
    return ds.upsert_business(data)


def test_writes_clear_uncleared_legacy_rows(pg):
    # No cleanup run: a Scout write to a legacy row removes its old Google content
    ds, db, text = pg
    ids = _legacy_rows(db, text)
    assert _scout_write(ds) == ids["g_pid"]
    _assert_no_google(_row(db, text, "businesses", ids["g_pid"]))
    # ...even when the write itself carries no values for those fields
    with db.get_db_session() as s:
        s.execute(text("UPDATE businesses SET name = 'Old Name', address = 'Old St', phone = '1', website = 'w', "
                       "google_rating = 4, google_review_count = 9 WHERE id = :i"), {"i": ids["g_pid"]})
    assert ds.upsert_business({"google_place_id": "pid-1", "source": "google_text"}) == ids["g_pid"]
    _assert_no_google(_row(db, text, "businesses", ids["g_pid"]))


def test_records_without_a_permitted_identifier_are_not_written(pg):
    ds, db, text = pg
    ids = _legacy_rows(db, text)
    with db.get_db_session() as s:
        before = s.execute(text("SELECT COUNT(*) FROM businesses")).scalar()
    assert _scout_write(ds, name="Mama G. African Kitchen", location="Bronx, NY", google_place_id=None) is None
    assert ds.upsert_business({"name": "Camila Nails Salon", "location": "Astoria, NY", "source": "geoapify"}) is None
    with db.get_db_session() as s:
        assert s.execute(text("SELECT COUNT(*) FROM businesses")).scalar() == before
    n = _row(db, text, "businesses", ids["g_nopid"])
    assert n["name"] == "Mama G. African Kitchen"          # legacy row untouched until cleanup


def test_networks_save_cannot_store_contact_info_or_generated_text(pg, monkeypatch):
    ds, db, text = pg
    _legacy_rows(db, text)
    import contact_service
    monkeypatch.setattr(contact_service, "infer_best_contact_path", lambda biz, ctx: {
        "best_contact_path": f"Call {biz.get('phone')}", "contact_target": "owner",
        "contact_confidence": "high", "contact_reason": f"Website {biz.get('website')} lists a phone"})
    monkeypatch.setattr(contact_service, "infer_service_angle", lambda ctx, biz: "booking")
    out = ds.save_scout_result_to_network({
        "business_name": "Chris Spa & Nails", "location": "Astoria, NY", "category": "nail salon",
        "address": "21-34 Broadway", "phone": "(718) 555-0199", "website": "https://chris.example",
        "rating": 4.9, "review_count": 187, "source": "google_text", "platform": "google_places",
        "google_place_id": "pid-1", "mode": "outreach", "opportunity_score": 61,
        "why_it_fits": "4.9 stars from 187 reviews", "problem_detected": "187 reviews but no booking",
        "contact_target": "owner", "contact_reason": "call (718) 555-0199",
        "suggested_message": "Hi Chris Spa", "suggested_follow_up": "Call again",
        "score_reasons": [{"key": "demand", "signal": "187 reviews", "impact": "positive", "weight": "high"}]},
        "t2")
    assert out["ok"]
    opp = _row(db, text, "opportunities", out["opportunity_id"])
    for f in ("problem_detected", "why_it_fits", "opportunity_fit_score", "signals", "best_contact_path",
              "contact_target", "contact_confidence", "contact_reason", "suggested_message",
              "suggested_follow_up", "recommended_offer"):
        assert opp[f] is None, f
    assert opp["opportunity_type"] == "outreach" and opp["service_angle"] == "booking"
    with db.get_db_session() as s:                       # the second write: saturation
        sat = [dict(r._mapping) for r in s.execute(text(
            "SELECT contact_target, contact_method FROM opportunity_saturation WHERE business_id = :b"),
            {"b": out["business_id"]}).fetchall()]
    assert sat and all(r["contact_target"] == "unknown" and r["contact_method"] == "unknown" for r in sat)
    assert "555" not in str(sat) and "chris.example" not in str(sat)
    _assert_no_google(_row(db, text, "businesses", out["business_id"]))


def test_networks_save_without_place_id_is_refused_cleanly(pg):
    ds, db, text = pg
    out = ds.save_scout_result_to_network({"business_name": "Camila Nails Salon", "location": "Astoria, NY",
                                           "source": "geoapify", "mode": "feature"}, "t3")
    assert out["ok"] is False and "no permitted identifier" in out["error"]


def test_reason_keys_keep_key_only():
    import source_rules
    assert source_rules.reason_keys([{"key": "demand", "signal": "4.9", "impact": "positive", "weight": "high"},
                                     {"signal": "no key"}]) == [{"key": "demand"}]


def test_seen_before_still_works_after_names_are_cleared(pg):
    ds, db, text = pg
    ids = _legacy_rows(db, text)
    _run_sql(db, "provider_cleanup_apply.sql")
    stats = ds.get_candidate_exposure_stats(
        [{"business_name": "Chris Spa & Nails", "location": "Astoria, NY", "google_place_id": "pid-1"}], "t")
    assert list(stats.values())[0]["seen_count_for_token"] == 1      # matched by place id, not name


def test_apply_removes_saturation_rows_with_contact_values(pg):
    ds, db, text = pg
    ids = _legacy_rows(db, text)
    with db.get_db_session() as s:
        s.execute(text("INSERT INTO opportunity_saturation (business_id, opportunity_type, service_angle, "
                       "contact_target, contact_method, total_discoveries) VALUES "
                       "(:g, 'outreach', 'x', 'owner', 'Call (718) 555-0101', 3), "
                       "(:g, 'outreach', 'y', 'unknown', 'unknown', 2), "
                       "(:geo, 'outreach', 'x', 'owner', 'dm', 1)"), {"g": ids["g_pid"], "geo": ids["geo"]})
    rows = {r["id"]: r for r in _run_sql(db, "provider_cleanup_dry_run.sql")}
    assert rows[ids["g_pid"]]["saturation_rows_to_delete"] == 1
    _run_sql(db, "provider_cleanup_apply.sql")
    with db.get_db_session() as s:
        left = s.execute(text("SELECT business_id, contact_method FROM opportunity_saturation ORDER BY id")).fetchall()
    assert [tuple(r) for r in left] == [(ids["g_pid"], "unknown"), (ids["geo"], "dm")]
