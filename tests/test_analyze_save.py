"""Analyze & Save: a finished analysis lands in Networks; owner replay runs
never write to CRMs; HubSpot repeat analyses don't create duplicates.

Offline: the database save, CRM calls and HubSpot API are mocked.
"""
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import pytest  # noqa: E402

import app  # noqa: E402
import replay_service as rs  # noqa: E402


@pytest.fixture
def deep(monkeypatch):
    rec = {"saves": [], "crm": []}

    def fake_save(payload, token_hash):
        rec["saves"].append((payload, token_hash))
        already = len(rec["saves"]) > 1
        return {"ok": True, "business_id": 7, "opportunity_id": 42, "already_saved": already}

    monkeypatch.setattr(app, "save_scout_result_to_network", fake_save)
    monkeypatch.setattr(app, "_DB_AVAILABLE", True)
    monkeypatch.setattr(app, "run_full_pipeline", lambda *a, **k: {
        "overall_score": 64, "key_insight": "Strong reviews, no booking link.",
        "weaknesses": ["No online booking", "Thin Instagram"]})
    monkeypatch.setattr(app, "set_cache", lambda *a, **k: None)
    for name in ("push_to_hubspot", "push_to_salesforce", "push_to_airtable", "notify_n8n_after_analyze"):
        monkeypatch.setattr(app, name, lambda *a, _n=name, **k: rec["crm"].append(_n))
    monkeypatch.setattr(rs, "REPLAY_ON", True)
    rs.set_request_mode("off")
    yield rec
    rs.set_request_mode("off")


CTX = {"token_hash": "th-owner", "business_name": "Union Cafe", "industry": "cafe",
       "location": "New York, NY", "website": "", "phone": "(212) 555-0100"}


def _run(ctx=CTX, extra=None):
    app.JOBS["j"] = {"status": "running"}
    app.run_deep_job("j", "Union Cafe", "New York, NY", {}, {}, "", "", [], "gpt_only",
                     [], None, "ck", {}, objective_mode="partnership",
                     extra_report_fields=extra or {"opportunity_score_v2": {"final_score": 58}},
                     network_context=ctx)
    return app.JOBS.pop("j")


def test_finished_analysis_is_saved_to_networks(deep):
    job = _run()
    assert job["status"] == "done"
    payload, th = deep["saves"][0]
    assert th == "th-owner"
    assert payload["business_name"] == "Union Cafe" and payload["category"] == "cafe"
    assert payload["mode"] == "partnership" and payload["source"] == "analyze"
    assert payload["opportunity_fit_score"] == 58               # backend score, not GPT's 64
    assert payload["phone"] == "(212) 555-0100"
    assert payload["problem_detected"] == "No online booking"
    ns = job["result"]["report"]["network_save"]
    assert ns == {"ok": True, "already_saved": False, "opportunity_id": 42}


def test_repeat_analysis_reports_already_saved(deep):
    _run()
    job = _run()
    assert job["result"]["report"]["network_save"]["already_saved"] is True


def test_no_access_code_means_no_save(deep):
    job = _run(ctx=dict(CTX, token_hash=None))
    assert deep["saves"] == []
    assert job["result"]["report"]["network_save"]["skipped"]


def test_save_failure_never_blocks_the_report(deep, monkeypatch):
    def boom(payload, th):
        raise RuntimeError("db down")
    monkeypatch.setattr(app, "save_scout_result_to_network", boom)
    job = _run()
    assert job["status"] == "done"
    assert job["result"]["report"]["network_save"]["ok"] is False


def test_live_runs_still_sync_crms(deep):
    _run()
    assert deep["crm"] == ["push_to_hubspot", "push_to_salesforce", "push_to_airtable", "notify_n8n_after_analyze"]


@pytest.mark.parametrize("mode", ["all", "ai", "saved"])
def test_owner_replay_runs_never_write_to_crms(deep, mode):
    rs.set_request_mode(mode)
    job = _run()
    assert job["status"] == "done"
    assert deep["crm"] == []
    if mode == "saved":
        assert deep["saves"] == []                               # test rerun: no Network write
        assert "saved-data replay" in job["result"]["report"]["network_save"]["skipped"]
    else:
        assert len(deep["saves"]) == 1


def test_agent_passes_who_to_save_for(monkeypatch):
    seen = {}

    class RunNow:
        def __init__(self, target=None, **kw):
            self.target = target

        def start(self):
            self.target()

    monkeypatch.setattr(app, "Thread", RunNow)
    monkeypatch.setattr(app, "run_deep_job", lambda *a, **k: seen.update(k))
    for name in ("search_web", "search_context", "search_google_places", "nearby_competitors_google",
                 "scrape_booking_competitors", "fetch_trend_intelligence"):
        monkeypatch.setattr(app, name, lambda *a, **k: [], raising=False)
    monkeypatch.setattr(app, "fetch_trend_intelligence", lambda *a, **k: None)
    monkeypatch.setattr(app, "discover_business_signals", lambda *a, **k: {})
    monkeypatch.setattr(app, "safe_get", lambda *a, **k: (None, 403))
    monkeypatch.setattr(app, "fetch_for_analysis", lambda *a, **k: "")
    monkeypatch.setattr(app, "init_clients", lambda: None)
    monkeypatch.setattr(app, "gpt_client", object())
    monkeypatch.setattr(app, "claude_client", object())
    monkeypatch.setattr(app, "get_cached", lambda ck: None)
    r = app.app.test_client().post("/agent", json={
        "business_name": "Union Cafe", "location": "New York, NY", "industry": "cafe",
        "website": "https://unioncafe.example", "objective_mode": "partnership"},
        headers={"X-Agent-Token": app.MASTER_CODE})
    assert r.status_code == 200, r.get_json()
    ctx = seen["network_context"]
    assert ctx["token_hash"] == app.hash_token(app.MASTER_CODE.strip().lower())
    assert ctx["industry"] == "cafe" and ctx["location"] == "New York, NY"


# ── HubSpot ────────────────────────────────────────────────────────────────

@pytest.fixture
def hubspot(monkeypatch):
    calls = []
    monkeypatch.setattr(app, "HUBSPOT_TOKEN", "hs-test")
    monkeypatch.setattr(app, "extract_enrichment", lambda *a, **k: {})
    monkeypatch.setattr(app, "hs_contact_exists", lambda email, domain: None)
    monkeypatch.setattr(app, "hs_update_contact", lambda *a, **k: calls.append("update"))
    monkeypatch.setattr(app, "hs_create_contact", lambda *a, **k: calls.append("create_contact") or "c-new")
    monkeypatch.setattr(app, "hs_create_deal", lambda *a, **k: calls.append("create_deal") or "d-1")
    monkeypatch.setattr(app, "hs_create_note", lambda *a, **k: calls.append("note"))
    return calls


REPORT = {"company": "Union Cafe", "overall_score": 60, "weaknesses": [], "location": "New York, NY"}


def test_hubspot_new_business_gets_contact_and_deal(hubspot, monkeypatch):
    monkeypatch.setattr(app, "hs_find_contact", lambda **k: None)
    assert app.push_to_hubspot(dict(REPORT), {}, {}) == "c-new"
    assert hubspot == ["create_contact", "create_deal", "note"]


def test_hubspot_repeat_analysis_adds_a_note_not_a_deal(hubspot, monkeypatch):
    found = {}
    monkeypatch.setattr(app, "hs_find_contact", lambda **k: found.update(k) or "c-old")
    assert app.push_to_hubspot(dict(REPORT), {}, {}) == "c-old"
    assert hubspot == ["update", "note"]
    assert found["company"] == "Union Cafe"                      # matched without email or website


def test_hubspot_search_falls_back_to_company_name(monkeypatch):
    import requests
    monkeypatch.setattr(app, "HUBSPOT_TOKEN", "hs-test")
    sent = []

    class R:
        def __init__(self, results):
            self._r = results

        def json(self):
            return {"results": self._r}

    def post(url, headers=None, json=None, timeout=None):
        f = json["filterGroups"][0]["filters"][0]
        sent.append((f["propertyName"], f["value"]))
        return R([{"id": "c-9"}] if f["propertyName"] == "company" else [])

    monkeypatch.setattr(app.req, "post", post)
    assert app.hs_find_contact(email=None, domain=None, company="Union Cafe") == "c-9"
    assert sent == [("company", "Union Cafe")]
    sent.clear()
    assert app.hs_find_contact(email="a@b.co", domain="unioncafe.example", company="Union Cafe") == "c-9"
    assert [p for p, _ in sent] == ["email", "website", "company"]
