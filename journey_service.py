"""
Search-to-visit journey records, step 1: what each Scout run returned.

Every Scout request writes one scout_runs row (the request, how it was
matched and ranked, who ran it, whether it was a test) and one
returned_results row per card, in order. Later steps (visits, feedback,
contributor permissions) link to these rows.

Runs (2026-11-04 decision, option A): every request is its own run with its
own run_id, tied to exactly the cards that request returned. A retried
click is a new search and a new run. The frontend's click id is stored
with the access code so repeated attempts can be grouped in analysis; it is
never used to merge runs.

"Returned" is deliberate: the backend knows which cards it sent, not which
ones the browser displayed. Display confirmation needs a frontend signal.

Provider content: until a source's permitted use is established, durable
records keep only identifiers and execution metadata. SOURCE_RULES names
every source and the fields it may store; a source not listed stores none.
Scores, reasons and labels are computed from provider data (ratings,
types, coordinates), so they are not stored for any source yet.

Deletion: delete_for_token() removes an access code's runs, returned
results and cached search embeddings (by exact model-specific key) in one
transaction, and records a tombstone so a run still in flight can't be
written back afterwards.

Tables are created here if missing; existing tables are not changed.
Writing never raises: a logging failure must not break a search.
"""
import json
import os
import re
import time

_TABLES_READY = False
STATS = {"runs": 0, "skipped_deleted": 0, "errors": 0}

# Every source and the identifier / content fields its cards may keep in
# durable records. Google Maps Platform terms allow keeping place IDs; other
# Places content (names, addresses, ratings, prices, hours, coordinates) is
# not stored. Geoapify (OpenStreetMap data) is not approved yet: its terms
# and attribution need checking first. Booking sources come from web search
# results and are not approved. Unlisted sources store nothing.
SOURCE_RULES = {
    "google_text":       {"ids": ("google_place_id",), "content": ()},
    "google_nearby":     {"ids": ("google_place_id",), "content": ()},
    "google_places":     {"ids": ("google_place_id",), "content": ()},
    "adjacent_category": {"ids": ("google_place_id",), "content": ()},
    "geoapify":          {"ids": (), "content": ()},
    "booking_platform":  {"ids": (), "content": ()},
}
_NO_RULE = {"ids": (), "content": ()}
# Content fields a rule may ever allow (none are allowed today)
_CONTENT_FIELDS = ("business_name", "address")

_CLIENT_RUN_ID = re.compile(r"^[A-Za-z0-9_-]{8,64}$")


def _db_session():
    from db import get_db_session  # optional dependency
    return get_db_session()


def _ensure_tables():
    """
    Create the tables in their own transaction and mark them ready only
    after it commits, so a write that later rolls back can't leave the
    process believing they exist.
    """
    global _TABLES_READY
    if _TABLES_READY:
        return
    from sqlalchemy import text
    with _db_session() as s:
        s.execute(text("""
            CREATE TABLE IF NOT EXISTS scout_runs (
                id             SERIAL PRIMARY KEY,
                client_run_id  TEXT,
                search_id      INTEGER,
                token_hash     TEXT,
                actor          TEXT,
                is_test        BOOLEAN DEFAULT FALSE,
                run_kind       TEXT,
                mode           TEXT,
                niche          TEXT,
                location       TEXT,
                user_query     TEXT,
                embed_key      TEXT,
                meaning        JSONB,
                ranking        JSONB,
                is_refresh     BOOLEAN DEFAULT FALSE,
                code_version   TEXT,
                result_count   INTEGER,
                created_at     TIMESTAMP DEFAULT NOW()
            )
        """))
        s.execute(text("CREATE INDEX IF NOT EXISTS scout_runs_token_click_idx "
                       "ON scout_runs (token_hash, client_run_id)"))
        s.execute(text("""
            CREATE TABLE IF NOT EXISTS returned_results (
                id               SERIAL PRIMARY KEY,
                run_id           INTEGER,
                position         INTEGER,
                business_id      INTEGER,
                source           TEXT,
                google_place_id  TEXT,
                content          JSONB,
                facts            JSONB,
                created_at       TIMESTAMP DEFAULT NOW()
            )
        """))
        s.execute(text("CREATE INDEX IF NOT EXISTS returned_results_run_idx ON returned_results (run_id)"))
        s.execute(text("""
            CREATE TABLE IF NOT EXISTS journey_embed_keys (
                token_hash  TEXT,
                embed_key   TEXT,
                created_at  TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (token_hash, embed_key)
            )
        """))
        s.execute(text("""
            CREATE TABLE IF NOT EXISTS journey_deleted_tokens (
                token_hash  TEXT PRIMARY KEY,
                deleted_at  TIMESTAMP DEFAULT NOW()
            )
        """))
    _TABLES_READY = True


def _lock_token(session, token_hash):
    """
    Transaction-level lock per access code. Logging and deletion for the
    same code take it first, so they run one after the other: a run is
    either committed before a deletion starts (and deleted by it) or checks
    the tombstone after the deletion committed (and is skipped).
    """
    if token_hash:
        from sqlalchemy import text
        session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"),
                        {"k": "yelhao-journey:" + token_hash})


_TOMBSTONED_SINCE = """
    SELECT 1 FROM journey_deleted_tokens d
    WHERE d.token_hash = :th AND d.deleted_at >= to_timestamp(:started)
"""


def register_query_key(token_hash, user_query, started_at=None) -> bool:
    """
    Before any embedding work: tie the exact embedding key of this search
    text to the access code, so a deletion during a first-ever search still
    finds it. Returns False when the code was deleted after this request
    started (the caller should skip embedding). True otherwise, including
    when there is no access code or the database is unavailable.
    """
    if not token_hash or not user_query:
        return True
    try:
        import embedding_service as es
        key = es.cache_key(user_query)
        _ensure_tables()
        from sqlalchemy import text
        started = started_at if started_at is not None else time.time()
        with _db_session() as s:
            _lock_token(s, token_hash)
            if s.execute(text(_TOMBSTONED_SINCE), {"th": token_hash, "started": float(started)}).scalar():
                return False
            s.execute(text("""
                INSERT INTO journey_embed_keys (token_hash, embed_key) VALUES (:t, :k)
                ON CONFLICT DO NOTHING
            """), {"t": token_hash, "k": key})
        return True
    except Exception as e:
        STATS["errors"] += 1
        if STATS["errors"] in (1, 10, 100):
            print(f"[Journey] query key unavailable ({str(e)[:80]})", flush=True)
        return True


def run_kind(replay_mode: str) -> str:
    """live, capture, saved or ai (owner replay modes) for a request's replay mode."""
    return {"all": "capture", "saved": "saved", "ai": "ai"}.get(replay_mode or "off", "live")


def actor_for(code_type) -> str:
    """Who ran the search, from the access code check: owner, guest, demo or unknown."""
    return {"master": "owner", "guest": "guest", "demo": "demo"}.get(code_type or "", "unknown")


def is_test_run(replay_mode, actor, test_flag) -> bool:
    """
    Any owner replay, or an owner run explicitly marked as a test (X-Test-Run
    header or "test": true). Only the owner can mark a run as a test, so users
    can't hide their own searches from the data.
    """
    if (replay_mode or "off") != "off":
        return True
    return actor == "owner" and bool(test_flag)


def clean_client_run_id(value):
    v = str(value or "").strip()
    return v if _CLIENT_RUN_ID.match(v) else None


def _rule(card):
    return SOURCE_RULES.get(card.get("source") or card.get("platform") or "", _NO_RULE)


def stored_card(card, business_id):
    """What a returned card may keep durably, per SOURCE_RULES."""
    rule = _rule(card)
    src = card.get("source") or card.get("platform") or None
    content = {f: card.get(f) for f in rule["content"] if f in _CONTENT_FIELDS and card.get(f)}
    seen = (card.get("seen_before") or {}).get("count")
    facts = {
        # Yelhao's own data and execution metadata, not provider content
        "seen_before":     seen if seen else None,
        "website_checked": card.get("website_checked"),
    }
    return {
        "business_id":     business_id,
        "source":          src,
        "google_place_id": (card.get("google_place_id") or None) if "google_place_id" in rule["ids"] else None,
        "content":         content or None,
        "facts":           {k: v for k, v in facts.items() if v is not None} or None,
    }


def log_scout_run(*, client_run_id=None, search_id, token_hash, actor="unknown", is_test=False,
                  replay_mode, mode, niche, location, user_query, meaning, ranking, is_refresh,
                  cards, business_ids, started_at=None):
    """
    Record one Scout request and the cards it returned, in order. Always a
    new run. Skipped (returns None) when the access code was deleted after
    this request started. Returns the run id, or None when unavailable.
    """
    global _TABLES_READY
    started = started_at if started_at is not None else time.time()
    try:
        _ensure_tables()
        from sqlalchemy import text
        try:
            import embedding_service as es
            ekey = es.cache_key(user_query) if user_query else None
        except Exception:
            ekey = None
        with _db_session() as s:
            _lock_token(s, token_hash)
            run_id = s.execute(text("""
                INSERT INTO scout_runs (client_run_id, search_id, token_hash, actor, is_test, run_kind,
                                        mode, niche, location, user_query, embed_key, meaning, ranking,
                                        is_refresh, code_version, result_count)
                SELECT :crid, :sid, :th, :actor, :test, :rk, :mode, :niche, :loc, :q, :ek,
                       CAST(:meaning AS JSONB), CAST(:ranking AS JSONB), :ref, :ver, :n
                WHERE NOT EXISTS (
                    SELECT 1 FROM journey_deleted_tokens d
                    WHERE d.token_hash = :th AND d.deleted_at >= to_timestamp(:started)
                )
                RETURNING id
            """), {
                "crid": clean_client_run_id(client_run_id), "sid": search_id, "th": token_hash,
                "actor": actor, "test": bool(is_test), "rk": run_kind(replay_mode), "mode": mode,
                "niche": niche, "loc": location, "q": user_query or None, "ek": ekey,
                "meaning": json.dumps(meaning) if meaning else None,
                "ranking": json.dumps(ranking) if ranking else None,
                "ref": bool(is_refresh),
                "ver": (os.environ.get("RENDER_GIT_COMMIT") or "")[:12] or None,
                "n": len(cards or []), "started": float(started),
            }).scalar()
            if run_id is None:
                STATS["skipped_deleted"] += 1
                return None
            for pos, card in enumerate(cards or [], 1):
                bid = business_ids[pos - 1] if business_ids and pos - 1 < len(business_ids) else None
                row = stored_card(card, bid)
                s.execute(text("""
                    INSERT INTO returned_results (run_id, position, business_id, source, google_place_id,
                                                  content, facts)
                    VALUES (:rid, :pos, :bid, :src, :pid, CAST(:content AS JSONB), CAST(:facts AS JSONB))
                """), {
                    "rid": run_id, "pos": pos, "bid": row["business_id"], "src": row["source"],
                    "pid": row["google_place_id"],
                    "content": json.dumps(row["content"]) if row["content"] else None,
                    "facts": json.dumps(row["facts"]) if row["facts"] else None,
                })
        STATS["runs"] += 1
        return run_id
    except Exception as e:
        _TABLES_READY = False        # next request re-checks the tables
        STATS["errors"] += 1
        if STATS["errors"] in (1, 10, 100):
            print(f"[Journey] run log unavailable ({str(e)[:80]})", flush=True)
        return None


def delete_for_token(token_hash: str) -> dict:
    """
    Remove an access code's journey records in ONE transaction: its runs,
    their returned results, and its cached search embeddings by exact key
    (erased and tombstoned so no worker serves or re-stores them). Records
    a tombstone for the access code so a run still in flight is not written
    afterwards. If anything fails, nothing is deleted and the call can be
    retried. Returns counts plus "ok".
    """
    counts = {"ok": False, "runs": 0, "returned_results": 0, "embeddings": 0}
    if not token_hash:
        return counts
    try:
        import embedding_service as es
        _ensure_tables()
        es._ensure_table()
        from sqlalchemy import text
        with _db_session() as s:
            _lock_token(s, token_hash)
            s.execute(text("""
                INSERT INTO journey_deleted_tokens (token_hash, deleted_at) VALUES (:t, NOW())
                ON CONFLICT (token_hash) DO UPDATE SET deleted_at = NOW()
            """), {"t": token_hash})
            rows = s.execute(text("SELECT id, embed_key FROM scout_runs WHERE token_hash = :t"),
                             {"t": token_hash}).fetchall()
            ids = [r[0] for r in rows]
            # Keys of finished runs and of searches still in progress
            pending = s.execute(text("SELECT embed_key FROM journey_embed_keys WHERE token_hash = :t"),
                                {"t": token_hash}).fetchall()
            keys = sorted({r[1] for r in rows if r[1]} | {r[0] for r in pending if r[0]})
            counts["embeddings"] = es.tombstone_keys(s, keys)
            s.execute(text("DELETE FROM journey_embed_keys WHERE token_hash = :t"), {"t": token_hash})
            if ids:
                counts["returned_results"] = s.execute(text(
                    "DELETE FROM returned_results WHERE run_id = ANY(:ids)"), {"ids": ids}).rowcount or 0
                counts["runs"] = s.execute(text(
                    "DELETE FROM scout_runs WHERE id = ANY(:ids)"), {"ids": ids}).rowcount or 0
        counts["ok"] = True
    except Exception as e:
        STATS["errors"] += 1
        print(f"[Journey] delete failed, nothing removed ({str(e)[:80]})", flush=True)
        counts.update({"runs": 0, "returned_results": 0, "embeddings": 0})
    return counts
