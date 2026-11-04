"""One analysis per business, quick pass only for batches, and Preview
Signals that treats unchecked fields as not checked.

Offline: AI, search and page fetches are all mocked.
"""
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import pytest  # noqa: E402

import app  # noqa: E402
import signal_service as ss  # noqa: E402
import contact_service as cs  # noqa: E402


# A Scout card for a Google result after Milestone 1 enrichment: website
# looked up (none on the listing), social and booking never looked up.
GOOGLE_CARD = {
    "name": "Union Cafe", "business_name": "Union Cafe", "category": "cafe",
    "location": "New York, NY", "website": "", "phone": "(212) 555-0100",
    "google_rating": 4.9, "google_review_count": 99, "instagram_url": None,
    "website_checked": True, "social_checked": False, "booking_checked": False,
}
CTX = {"mode": "outreach", "user_role": None, "signal_preferences": [], "niche": "cafe"}


# ── Preview Signals ────────────────────────────────────────────────────────

def test_unchecked_social_makes_no_visibility_claim_or_instagram_fix():
    sig = ss.build_opportunity_signals(dict(GOOGLE_CARD), dict(CTX))
    sc = sig["social_conversion"]
    assert "Instagram" not in sc["recommended_fix"]
    assert "low visibility" not in sc["business_value"]
    assert "low visibility" not in sig["business_value"]["plain_english_value"]
    att = sc["attention_signal"]
    assert att["checked"] is False and att["score"] == 50
    assert "No social" not in att["evidence"]
    assert sc["unchecked_fields"] == ["booking", "social"]


def test_unchecked_fields_are_named_in_the_fix_text():
    sc = ss.build_opportunity_signals(dict(GOOGLE_CARD), dict(CTX))["social_conversion"]
    assert sc["main_conversion_leak"] == "not_enough_checked"
    assert "booking, social" in sc["recommended_fix"]


def test_no_online_presence_flag_needs_checked_fields():
    card = dict(GOOGLE_CARD, website_checked=False)
    flags = ss.build_opportunity_signals(card, dict(CTX))["risk_flags"]
    assert not any("No online presence" in f for f in flags)


def test_checked_weak_spot_is_still_reported():
    # Low reviews are checked evidence, so trust can still be the weak spot.
    card = dict(GOOGLE_CARD, google_rating=3.0, google_review_count=2)
    sc = ss.build_opportunity_signals(card, dict(CTX))["social_conversion"]
    assert sc["main_conversion_leak"] == "trust"
    assert "reviews" in sc["recommended_fix"]


def test_callers_without_flags_are_unchanged():
    # Analyze and Network refresh send no *_checked flags: old behavior.
    card = {k: v for k, v in GOOGLE_CARD.items() if not k.endswith("_checked")}
    sc = ss.build_opportunity_signals(card, dict(CTX))["social_conversion"]
    assert sc["main_conversion_leak"] == "attention"
    assert sc["unchecked_fields"] == []
    assert "checked" not in sc["attention_signal"]


def test_plain_english_value_follows_this_business_not_a_fixed_line():
    # Old code always said "low visibility". A business with social and a
    # website but very few reviews should name trust instead.
    biz = {"name": "X", "category": "bakery", "website": "https://x.com",
           "instagram_url": "https://instagram.com/x", "tiktok": "https://tiktok.com/@x",
           "phone": "1", "google_rating": 3.2, "google_review_count": 1}
    sig = ss.build_opportunity_signals(biz, {"niche": "bakery"})
    assert "low visibility" not in sig["business_value"]["plain_english_value"]
    assert sig["business_value"]["plain_english_value"] == sig["social_conversion"]["business_value"]


@pytest.mark.parametrize("checked,expect", [
    (False, "Website not checked yet"),
    (True, "No website on the Google listing"),
    (None, "No website detected"),
])
def test_phone_contact_reason_matches_what_was_checked(checked, expect):
    biz = {"name": "Union Cafe", "phone": "(212) 555-0100", "website": ""}
    if checked is not None:
        biz["website_checked"] = checked
    c = cs.infer_best_contact_path(biz, dict(CTX))
    assert c["best_contact_path"] == "phone_call"
    assert c["contact_reason"].startswith(expect)


def test_scout_cards_carry_social_and_booking_flags():
    src = open(os.path.join(os.path.dirname(app.__file__), "app.py")).read()
    assert '"social_checked":      b.get("social_checked", True)' in src
    assert '"booking_checked":     b.get("booking_checked", True)' in src


# ── One analysis per business ──────────────────────────────────────────────

class _NoThread:
    started = []

    def __init__(self, target=None, daemon=None, **kw):
        self.target = target

    def start(self):
        _NoThread.started.append(self.target)


@pytest.fixture
def agent_env(monkeypatch):
    calls = {"fast": 0}

    def no_fast(*a, **k):
        calls["fast"] += 1
        return {"overall_score": 61}

    monkeypatch.setattr(app, "run_fast_pipeline", no_fast)
    monkeypatch.setattr(app, "verify_fast_report", lambda r, *a, **k: r)
    monkeypatch.setattr(app, "init_clients", lambda: None)
    monkeypatch.setattr(app, "gpt_client", object())
    monkeypatch.setattr(app, "claude_client", object())
    monkeypatch.setattr(app, "get_cached", lambda ck: None)
    monkeypatch.setattr(app, "safe_get", lambda *a, **k: (None, 403))
    monkeypatch.setattr(app, "search_web", lambda *a, **k: [])
    monkeypatch.setattr(app, "search_context", lambda *a, **k: [])
    monkeypatch.setattr(app, "search_google_places", lambda *a, **k: [])
    monkeypatch.setattr(app, "discover_business_signals", lambda *a, **k: {})
    monkeypatch.setattr(app, "Thread", _NoThread)
    _NoThread.started = []
    return calls


def _post(client, **extra):
    body = {"business_name": "Union Cafe", "location": "New York, NY",
            "website": "https://unioncafe.example", "industry": "cafe"}
    body.update(extra)
    return client.post("/agent", json=body, headers={"X-Agent-Token": app.MASTER_CODE})


def test_single_analyze_skips_the_quick_gpt_pass(agent_env, monkeypatch):
    monkeypatch.setattr(app, "ANALYZE_FAST_PASS", False)
    r = _post(app.app.test_client())
    assert r.status_code == 200, r.get_json()
    d = r.get_json()
    assert agent_env["fast"] == 0
    assert d["status"] == "processing" and d["job_id"]
    assert d["report"]["analysis_pending"] is True
    assert d["report"]["overall_score"] is None        # no AI score before the real one
    assert d["agent_brain"]["pipeline_mode"] == "deep_pending"
    assert len(_NoThread.started) == 1                 # deep job still starts


def test_fast_pass_comes_back_with_the_env_switch(agent_env, monkeypatch):
    monkeypatch.setattr(app, "ANALYZE_FAST_PASS", True)
    d = _post(app.app.test_client()).get_json()
    assert agent_env["fast"] == 1
    assert "analysis_pending" not in d["report"]
    assert d["agent_brain"]["pipeline_mode"] == "fast"


def test_deep_report_keeps_backend_intelligence_fields(monkeypatch):
    monkeypatch.setattr(app, "run_full_pipeline", lambda *a, **k: {"overall_score": 72, "business_value": "deep wins"})
    monkeypatch.setattr(app, "set_cache", lambda *a, **k: None)
    app.JOBS["t1"] = {"status": "running"}
    app.run_deep_job("t1", "Union Cafe", "New York, NY", {}, {}, "", "", [], "gpt_only",
                     [], None, "ck", {}, extra_report_fields={
                         "opportunity_score_v2": {"final_score": 40},
                         "business_value": "from intel"})
    job = app.JOBS.pop("t1")
    assert job["status"] == "done", job
    rep = job["result"]["report"]
    assert rep["opportunity_score_v2"] == {"final_score": 40}
    assert rep["business_value"] == "deep wins"        # deep values are never overwritten
    assert rep["overall_score"] == 72


# ── Batch: quick pass, and the call works again ────────────────────────────

def test_batch_item_runs_the_quick_pass(monkeypatch):
    seen = {}

    def fake_fast(name, location, pages, social_text, social_links=None, gp_context=None):
        seen["args"] = (name, location, pages, social_text)
        return {"overall_score": 58, "key_insight": "k", "weaknesses": ["w"]}

    monkeypatch.setattr(app, "run_fast_pipeline", fake_fast)
    monkeypatch.setattr(app, "safe_get", lambda *a, **k: ("<html><body>" + "Fresh bread daily. " * 30 + "</body></html>", 200))
    r = app._run_batch_item({"business_name": "Union Cafe", "location": "New York, NY",
                             "website": "https://unioncafe.example"}, "cafe")
    assert r.get("batch_status") == "done", r
    assert r["analysis_type"] == "quick" and r["overall_score"] == 58
    pages = seen["args"][2]
    assert len(next(iter(pages.values()))) > 200       # full page text, not cut to the status code
