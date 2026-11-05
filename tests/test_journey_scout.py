"""Scout hands every run and its returned cards, in order, to the journey record."""
import os
from contextlib import contextmanager

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import pytest  # noqa: E402

import app  # noqa: E402
import embedding_service as es  # noqa: E402
import journey_service as js  # noqa: E402
import replay_service as rs  # noqa: E402
from tests.test_scout_factual_m1 import FakeGoogle, MASTER, _setup  # noqa: E402


def _places(n=4):
    return [{"name": f"Spot {i} Nails", "place_id": f"pid-{i}", "formatted_address": f"{i} 31st St, Astoria, NY",
             "rating": 4.9 - i * 0.1, "user_ratings_total": 50 + i, "business_status": "OPERATIONAL",
             "types": ["beauty_salon"], "geometry": {"location": {"lat": 40.76, "lng": -73.92}}}
            for i in range(n)]


@pytest.fixture
def scout(monkeypatch):
    got = {}
    _setup(monkeypatch, FakeGoogle(_places()))
    monkeypatch.setattr(app, "_DB_AVAILABLE", True)
    monkeypatch.setattr(app, "save_search", lambda d: 42)
    ids = iter([101, 102, None, 104])
    monkeypatch.setattr(app, "upsert_business", lambda d: next(ids))
    monkeypatch.setattr(app, "log_discovery_event", lambda **k: None)
    monkeypatch.setattr(app, "_scout_exposure_inputs", lambda *a, **k: ({}, None))

    def fake_log(**kw):
        got.update(kw)
        return 9001
    monkeypatch.setattr(js, "log_scout_run", fake_log)

    def run(body=None, headers=None, code=MASTER):
        b = {"niche": "nail salon", "location": "Astoria, NY", "user_query": "nail salons in Astoria"}
        b.update(body or {})
        h = {"X-Agent-Token": code}
        h.update(headers or {})
        r = app.app.test_client().post("/prospect", json=b, headers=h)
        return r.get_json(), got
    return run


def test_scout_logs_the_run_and_returns_its_id(scout):
    data, got = scout({"client_run_id": "click-0001-abcd"})
    assert data["run_id"] == 9001
    assert got["search_id"] == 42 and got["user_query"] == "nail salons in Astoria"
    assert got["client_run_id"] == "click-0001-abcd"
    assert [c["business_name"] for c in got["cards"]] == [c["business_name"] for c in data["results"]]
    assert got["business_ids"] == [101, 102, None, 104]
    assert got["replay_mode"] == "off" and got["ranking"]["type"] == "stable"
    assert got["actor"] == "owner" and got["is_test"] is False


def test_owner_can_mark_a_live_run_as_a_test(scout):
    _, got = scout(headers={"X-Test-Run": "1"})
    assert got["is_test"] is True
    _, got = scout(body={"test": True})
    assert got["is_test"] is True


def test_journey_is_recorded_even_when_saving_the_search_fails(scout, monkeypatch):
    def boom(d):
        raise RuntimeError("searches table down")
    monkeypatch.setattr(app, "save_search", boom)
    data, got = scout()
    assert data["run_id"] == 9001 and got["search_id"] is None and got["business_ids"] == []


def test_no_database_means_no_run_id(monkeypatch):
    _setup(monkeypatch, FakeGoogle(_places(2)))
    r = app.app.test_client().post("/prospect", json={"niche": "nail salon", "location": "Astoria, NY"},
                                   headers={"X-Agent-Token": MASTER})
    assert r.get_json()["run_id"] is None


def test_zero_results_still_get_a_run_id(scout, monkeypatch):
    _setup(monkeypatch, FakeGoogle([]))
    monkeypatch.setattr(app, "_DB_AVAILABLE", True)        # _setup turns it off
    monkeypatch.setattr(app, "search_geoapify", lambda *a, **k: [])
    monkeypatch.setattr(app, "search_web", lambda *a, **k: [])
    data, got = scout()
    assert data["count"] == 0 and data["run_id"] == 9001 and got["cards"] == []


# ── test identification rules ──────────────────────────────────────────────

@pytest.mark.parametrize("replay,actor,flag,expected", [
    ("off", "owner", False, False),
    ("off", "owner", True, True),          # owner live test
    ("all", "owner", False, True),         # capture
    ("saved", "owner", False, True),
    ("off", "guest", True, False),         # users can't hide their searches
    ("off", "demo", True, False),
])
def test_is_test_run(replay, actor, flag, expected):
    assert js.is_test_run(replay, actor, flag) is expected


def test_actor_and_client_run_id_rules():
    assert [js.actor_for(c) for c in ("master", "guest", "demo", None)] == ["owner", "guest", "demo", "unknown"]
    assert js.clean_client_run_id("3f2a9c1e-5b7d-4e8a") == "3f2a9c1e-5b7d-4e8a"
    assert js.clean_client_run_id("short") is None
    assert js.clean_client_run_id("drop table;--x") is None


# ── tables are marked ready only after their creation commits ──────────────

class _FailingCommitSession:
    def __init__(self, fail):
        self.fail = fail
        self.statements = []

    def execute(self, stmt, params=None):
        self.statements.append(str(stmt))

        class _R:
            rowcount = 0

            def scalar(self):
                return None

            def fetchall(self):
                return []

            def fetchone(self):
                return None
        return _R()


def _session_factory(fail_first):
    state = {"n": 0}

    @contextmanager
    def factory():
        state["n"] += 1
        s = _FailingCommitSession(fail_first and state["n"] == 1)
        yield s
        if s.fail:
            raise RuntimeError("commit failed, creation rolled back")
    return factory


@pytest.mark.parametrize("mod_name,flag,ensure", [
    ("journey", "_TABLES_READY", "_ensure_tables"),
    ("embedding", "_TABLE_READY", "_ensure_table"),
    ("replay", "_TABLE_READY", "_ensure_table"),
])
def test_tables_not_marked_ready_when_creation_rolls_back(monkeypatch, mod_name, flag, ensure):
    mod = {"journey": js, "embedding": es, "replay": rs}[mod_name]
    monkeypatch.setattr(mod, flag, False)
    monkeypatch.setattr(mod, "_db_session", _session_factory(fail_first=True))
    with pytest.raises(RuntimeError):
        getattr(mod, ensure)()
    assert getattr(mod, flag) is False                 # will try again
    getattr(mod, ensure)()                              # second attempt commits
    assert getattr(mod, flag) is True


# ── retries (option A): each request is its own run with its own cards ─────

def test_retried_click_runs_again_and_gets_its_own_run(monkeypatch):
    runs = []
    google = FakeGoogle(_places())
    _setup(monkeypatch, google)
    monkeypatch.setattr(app, "_DB_AVAILABLE", True)
    monkeypatch.setattr(app, "save_search", lambda d: 1)
    monkeypatch.setattr(app, "upsert_business", lambda d: None)
    monkeypatch.setattr(app, "log_discovery_event", lambda **k: None)
    monkeypatch.setattr(app, "_scout_exposure_inputs", lambda *a, **k: ({}, None))
    monkeypatch.setattr(js, "log_scout_run", lambda **kw: runs.append(kw) or len(runs))
    body = {"niche": "nail salon", "location": "Astoria, NY", "client_run_id": "click-same-0001"}
    c = app.app.test_client()
    first = c.post("/prospect", json=body, headers={"X-Agent-Token": MASTER}).get_json()
    google.places = _places(2)                  # the retry sees different results
    second = c.post("/prospect", json=body, headers={"X-Agent-Token": MASTER}).get_json()
    assert sum(1 for k, _ in google.calls if k == "text") == 2      # searched twice
    assert (first["run_id"], second["run_id"]) == (1, 2)
    assert [x["business_name"] for x in runs[1]["cards"]] == [x["business_name"] for x in second["results"]]
    assert len(runs[0]["cards"]) == 4 and len(runs[1]["cards"]) == 2
    assert runs[0]["client_run_id"] == runs[1]["client_run_id"] == "click-same-0001"


# ── an authenticated query is embedded only after its key is registered ────

def _embed_spy(monkeypatch):
    calls = []
    monkeypatch.setattr(es, "match_query", lambda q, budget_ok=None: calls.append(q) or
                        {"terms": [], "top": [], "status": "no_match"})
    return calls


def test_registration_failure_skips_embedding_but_search_returns(scout, monkeypatch):
    calls = _embed_spy(monkeypatch)
    monkeypatch.setattr(js, "_db_session", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
    monkeypatch.setattr(js, "_TABLES_READY", False)
    data, _ = scout()
    assert calls == []                                    # no embedding work
    assert data["query_meaning"] is None and len(data["results"]) == 4


def test_registered_query_is_embedded(scout, monkeypatch):
    calls = _embed_spy(monkeypatch)
    monkeypatch.setattr(js, "register_query_key", lambda *a, **k: True)
    scout()
    assert calls == ["nail salons in Astoria"]


def test_no_journey_tracking_means_no_embedding_for_authenticated_users(monkeypatch):
    calls = _embed_spy(monkeypatch)
    _setup(monkeypatch, FakeGoogle(_places()))            # database unavailable
    r = app.app.test_client().post("/prospect", json={"niche": "nail salon", "location": "Astoria, NY"},
                                   headers={"X-Agent-Token": MASTER})
    assert calls == [] and len(r.get_json()["results"]) == 4
    monkeypatch.setattr(app, "_DB_AVAILABLE", True)
    monkeypatch.setattr(app, "journey_service", None)
    monkeypatch.setattr(app, "save_search", lambda d: 1)
    monkeypatch.setattr(app, "upsert_business", lambda d: None)
    monkeypatch.setattr(app, "_scout_exposure_inputs", lambda *a, **k: ({}, None))
    r = app.app.test_client().post("/prospect", json={"niche": "nail salon", "location": "Astoria, NY"},
                                   headers={"X-Agent-Token": MASTER})
    assert calls == [] and len(r.get_json()["results"]) == 4


def test_registration_raising_never_breaks_the_search(scout, monkeypatch):
    calls = _embed_spy(monkeypatch)

    def boom(*a, **k):
        raise RuntimeError("unexpected")
    monkeypatch.setattr(js, "register_query_key", boom)
    data, _ = scout()
    assert calls == [] and len(data["results"]) == 4
