"""
Search-to-visit journey records, step 1: what each Scout run returned.

Every Scout run writes one scout_runs row (the request, how it was matched,
how results were ranked, who ran it and whether it was a test) and one
returned_results row per card, in order. Later steps (visits, feedback,
contributor permissions) link to these rows, so a documented visit can be
traced back to what Yelhao returned and in what order.

"Returned" is deliberate: the backend knows which cards it sent, not which
ones the browser displayed. Display confirmation needs a frontend signal.

Provider content: Google Maps Platform terms allow keeping place IDs, not
Places content (names, addresses, ratings, prices, hours). For cards that
came from Google, only the place ID, Yelhao's own business ID and Yelhao's
own labels and reasons (keys and impact, not text quoting Google) are
stored. Cards from OpenStreetMap (Geoapify) or other sources keep their
name and address with the source named.

Retries: a client_run_id sent with the request makes the run idempotent;
the same id returns the existing run instead of recording a new one.

Tables are created here if missing; existing tables are not changed.
Writing never raises: a logging failure must not break a search.
"""
import hashlib
import json
import os
import re

_TABLES_READY = False
STATS = {"runs": 0, "duplicates": 0, "errors": 0}

# Sources whose card content comes from Google Places
GOOGLE_SOURCES = ("google_text", "google_nearby", "adjacent_category", "google_places")

_CLIENT_RUN_ID = re.compile(r"^[A-Za-z0-9_-]{8,64}$")


def _db_session():
    from db import get_db_session  # optional dependency
    return get_db_session()


def _ensure_tables():
    """
    Create the tables in their own transaction and mark them ready only
    after it commits. Marking them ready inside a write that later rolls
    back would leave the process believing the tables exist when they don't.
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
                query_hash     TEXT,
                meaning        JSONB,
                ranking        JSONB,
                is_refresh     BOOLEAN DEFAULT FALSE,
                code_version   TEXT,
                result_count   INTEGER,
                created_at     TIMESTAMP DEFAULT NOW()
            )
        """))
        s.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS scout_runs_client_run_idx
            ON scout_runs (client_run_id) WHERE client_run_id IS NOT NULL
        """))
        s.execute(text("CREATE INDEX IF NOT EXISTS scout_runs_token_idx ON scout_runs (token_hash)"))
        s.execute(text("""
            CREATE TABLE IF NOT EXISTS returned_results (
                id               SERIAL PRIMARY KEY,
                run_id           INTEGER,
                position         INTEGER,
                business_id      INTEGER,
                google_place_id  TEXT,
                source           TEXT,
                business_name    TEXT,
                score            INTEGER,
                reasons          JSONB,
                facts            JSONB,
                created_at       TIMESTAMP DEFAULT NOW()
            )
        """))
        s.execute(text("CREATE INDEX IF NOT EXISTS returned_results_run_idx ON returned_results (run_id)"))
    _TABLES_READY = True


def run_kind(replay_mode: str) -> str:
    """live, capture, saved or ai (owner replay modes) for a request's replay mode."""
    return {"all": "capture", "saved": "saved", "ai": "ai"}.get(replay_mode or "off", "live")


def actor_for(code_type) -> str:
    """Who ran the search, from the access code check: owner, guest, demo or unknown."""
    return {"master": "owner", "guest": "guest", "demo": "demo"}.get(code_type or "", "unknown")


def is_test_run(replay_mode, actor, test_flag) -> bool:
    """
    A test run is any owner replay, or an owner run explicitly marked as a
    test (X-Test-Run header or "test": true). Only the owner can mark a run
    as a test, so users can't hide their own searches from the data.
    """
    if (replay_mode or "off") != "off":
        return True
    return actor == "owner" and bool(test_flag)


def clean_client_run_id(value):
    v = str(value or "").strip()
    return v if _CLIENT_RUN_ID.match(v) else None


def query_hash(text_value: str) -> str:
    """Same normalization as the embedding cache key (without the model)."""
    return hashlib.sha256((text_value or "").strip().lower().encode("utf-8")).hexdigest()[:40]


def _is_google(card) -> bool:
    return (card.get("source") or card.get("platform") or "") in GOOGLE_SOURCES


def _reasons(card):
    google = _is_google(card)
    out = []
    for r in (card.get("score_reasons") or [])[:8]:
        if isinstance(r, dict):
            keep = ("key", "impact", "weight") if google else ("key", "signal", "impact", "weight")
            out.append({k: r.get(k) for k in keep if r.get(k) is not None})
        elif not google:
            out.append({"signal": str(r)[:200]})
    return out


def _facts(card):
    """
    Yelhao's own labels for every card. Provider content (rating, reviews,
    price, open now, address) only for non-Google sources.
    """
    qm = card.get("query_match") or {}
    area = card.get("area_check") or {}
    seen = card.get("seen_before") or {}
    facts = {
        "query_match":     {"level": qm.get("level"), "terms": qm.get("terms"),
                            "searched": bool(qm.get("searched"))} if qm else None,
        "area_check":      {"level": area.get("level"), "km": area.get("km")} if area else None,
        "seen_before":     seen.get("count") if seen else None,
        "lane":            card.get("opportunity_lane"),
        "website_checked": card.get("website_checked"),
    }
    if not _is_google(card):
        facts.update({
            "rating":       card.get("rating"),
            "review_count": card.get("review_count"),
            "address":      card.get("address"),
        })
    return {k: v for k, v in facts.items() if v is not None}


def log_scout_run(*, client_run_id=None, search_id, token_hash, actor="unknown", is_test=False,
                  replay_mode, mode, niche, location, user_query, meaning, ranking, is_refresh,
                  cards, business_ids):
    """
    Record one Scout run and the cards it returned, in order.
    cards: the ranked results as returned; business_ids: same order (None allowed).
    Returns the run id (the existing one for a repeated client_run_id), or
    None when the database is unavailable.
    """
    global _TABLES_READY
    crid = clean_client_run_id(client_run_id)
    try:
        _ensure_tables()
        from sqlalchemy import text
        with _db_session() as s:
            if crid:
                existing = s.execute(text("SELECT id FROM scout_runs WHERE client_run_id = :c"),
                                     {"c": crid}).scalar()
                if existing:
                    STATS["duplicates"] += 1
                    return existing
            params = {
                "crid": crid, "sid": search_id, "th": token_hash, "actor": actor,
                "test": bool(is_test), "rk": run_kind(replay_mode), "mode": mode,
                "niche": niche, "loc": location, "q": (user_query or "")[:500],
                "qh": query_hash(user_query) if user_query else None,
                "meaning": json.dumps(meaning) if meaning else None,
                "ranking": json.dumps(ranking) if ranking else None,
                "ref": bool(is_refresh),
                "ver": (os.environ.get("RENDER_GIT_COMMIT") or "")[:12] or None,
                "n": len(cards or []),
            }
            run_id = s.execute(text("""
                INSERT INTO scout_runs (client_run_id, search_id, token_hash, actor, is_test, run_kind,
                                        mode, niche, location, user_query, query_hash, meaning, ranking,
                                        is_refresh, code_version, result_count)
                VALUES (:crid, :sid, :th, :actor, :test, :rk, :mode, :niche, :loc, :q, :qh,
                        CAST(:meaning AS JSONB), CAST(:ranking AS JSONB), :ref, :ver, :n)
                ON CONFLICT (client_run_id) WHERE client_run_id IS NOT NULL DO NOTHING
                RETURNING id
            """), params).scalar()
            if run_id is None:
                # Another request with the same client_run_id won the race
                STATS["duplicates"] += 1
                return s.execute(text("SELECT id FROM scout_runs WHERE client_run_id = :c"),
                                 {"c": crid}).scalar()
            for pos, card in enumerate(cards or [], 1):
                bid = business_ids[pos - 1] if business_ids and pos - 1 < len(business_ids) else None
                google = _is_google(card)
                s.execute(text("""
                    INSERT INTO returned_results (run_id, position, business_id, google_place_id, source,
                                                  business_name, score, reasons, facts)
                    VALUES (:rid, :pos, :bid, :pid, :src, :name, :score, CAST(:reasons AS JSONB),
                            CAST(:facts AS JSONB))
                """), {
                    "rid": run_id, "pos": pos, "bid": bid,
                    "pid": card.get("google_place_id") or None,
                    "src": card.get("source") or card.get("platform") or None,
                    "name": None if google else (card.get("business_name") or "")[:300] or None,
                    "score": card.get("opportunity_score"),
                    "reasons": json.dumps(_reasons(card)),
                    "facts": json.dumps(_facts(card), default=str),
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
    Remove an access code's journey records: its runs, their returned
    results, and the cached embeddings of the texts it searched. The
    embedding cache is shared, so a text another code also searched is
    simply embedded again the next time. Returns counts; never raises.
    """
    counts = {"runs": 0, "returned_results": 0, "embeddings": 0}
    if not token_hash:
        return counts
    try:
        _ensure_tables()
        from sqlalchemy import text
        with _db_session() as s:
            rows = s.execute(text("SELECT id, user_query FROM scout_runs WHERE token_hash = :t"),
                             {"t": token_hash}).fetchall()
            ids = [r[0] for r in rows]
            texts = sorted({r[1] for r in rows if r[1]})
            if ids:
                counts["returned_results"] = s.execute(text(
                    "DELETE FROM returned_results WHERE run_id = ANY(:ids)"), {"ids": ids}).rowcount or 0
                counts["runs"] = s.execute(text(
                    "DELETE FROM scout_runs WHERE id = ANY(:ids)"), {"ids": ids}).rowcount or 0
        if texts:
            try:
                import embedding_service as es
                counts["embeddings"] = es.delete_texts(texts)
            except Exception:
                pass
    except Exception as e:
        STATS["errors"] += 1
        print(f"[Journey] delete unavailable ({str(e)[:80]})", flush=True)
    return counts
