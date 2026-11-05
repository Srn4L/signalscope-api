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
