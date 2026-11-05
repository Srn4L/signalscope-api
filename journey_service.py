"""
Search-to-visit journey records, step 1: what each Scout run showed.

Every Scout run writes one scout_runs row (the request, how it was matched,
how results were ranked, and whether it was a live search or an owner test)
and one shown_results row per card (position, score, reasons and the card
facts as shown). Later steps (visits, feedback, contributor permissions)
link to these rows, so a documented visit can be traced back to exactly
what Yelhao recommended and in what order.

Tables are created here if missing; existing tables are not changed.
Writing never raises: a logging failure must not break a search.
"""
import json
import os

_TABLES_READY = False
STATS = {"runs": 0, "errors": 0}


def _db_session():
    from db import get_db_session  # optional dependency
    return get_db_session()


def _ensure_tables(session):
    global _TABLES_READY
    if _TABLES_READY:
        return
    from sqlalchemy import text
    session.execute(text("""
        CREATE TABLE IF NOT EXISTS scout_runs (
            id            SERIAL PRIMARY KEY,
            search_id     INTEGER,
            token_hash    TEXT,
            run_kind      TEXT,
            mode          TEXT,
            niche         TEXT,
            location      TEXT,
            user_query    TEXT,
            meaning       JSONB,
            ranking       JSONB,
            is_refresh    BOOLEAN DEFAULT FALSE,
            code_version  TEXT,
            result_count  INTEGER,
            created_at    TIMESTAMP DEFAULT NOW()
        )
    """))
    session.execute(text("""
        CREATE TABLE IF NOT EXISTS shown_results (
            id               SERIAL PRIMARY KEY,
            run_id           INTEGER,
            position         INTEGER,
            business_id      INTEGER,
            google_place_id  TEXT,
            business_name    TEXT,
            score            INTEGER,
            reasons          JSONB,
            facts            JSONB,
            created_at       TIMESTAMP DEFAULT NOW()
        )
    """))
    session.execute(text("CREATE INDEX IF NOT EXISTS shown_results_run_idx ON shown_results (run_id)"))
    _TABLES_READY = True


def run_kind(replay_mode: str) -> str:
    """live, capture, saved or ai (owner test modes) for a request's replay mode."""
    return {"all": "capture", "saved": "saved", "ai": "ai"}.get(replay_mode or "off", "live")


def _reasons(card):
    out = []
    for r in (card.get("score_reasons") or [])[:8]:
        if isinstance(r, dict):
            out.append({k: r.get(k) for k in ("key", "signal", "impact") if r.get(k) is not None})
        else:
            out.append({"signal": str(r)[:200]})
    return out


def _facts(card):
    qm = card.get("query_match") or {}
    area = card.get("area_check") or {}
    seen = card.get("seen_before") or {}
    facts = {
        "source":        card.get("source") or card.get("platform"),
        "rating":        card.get("rating"),
        "review_count":  card.get("review_count"),
        "price_level":   card.get("price_level") or None,
        "open_now":      card.get("open_now"),
        "address":       card.get("address"),
        "website_checked": card.get("website_checked"),
        "query_match":   {"level": qm.get("level"), "terms": qm.get("terms"),
                          "searched": bool(qm.get("searched"))} if qm else None,
        "area_check":    {"level": area.get("level"), "km": area.get("km")} if area else None,
        "seen_before":   seen.get("count") if seen else None,
        "lane":          card.get("opportunity_lane"),
    }
    return {k: v for k, v in facts.items() if v is not None}


def log_scout_run(*, search_id, token_hash, replay_mode, mode, niche, location, user_query,
                  meaning, ranking, is_refresh, cards, business_ids):
    """
    Record one Scout run and the cards it showed, in order.
    cards: the ranked results as shown; business_ids: same order (None allowed).
    Returns the run id, or None when the database is unavailable.
    """
    try:
        from sqlalchemy import text
        with _db_session() as s:
            _ensure_tables(s)
            run_id = s.execute(text("""
                INSERT INTO scout_runs (search_id, token_hash, run_kind, mode, niche, location,
                                        user_query, meaning, ranking, is_refresh, code_version,
                                        result_count)
                VALUES (:sid, :th, :rk, :mode, :niche, :loc, :q, CAST(:meaning AS JSONB),
                        CAST(:ranking AS JSONB), :ref, :ver, :n)
                RETURNING id
            """), {
                "sid": search_id, "th": token_hash, "rk": run_kind(replay_mode),
                "mode": mode, "niche": niche, "loc": location, "q": (user_query or "")[:500],
                "meaning": json.dumps(meaning) if meaning else None,
                "ranking": json.dumps(ranking) if ranking else None,
                "ref": bool(is_refresh),
                "ver": (os.environ.get("RENDER_GIT_COMMIT") or "")[:12] or None,
                "n": len(cards or []),
            }).scalar()
            for pos, card in enumerate(cards or [], 1):
                bid = business_ids[pos - 1] if business_ids and pos - 1 < len(business_ids) else None
                s.execute(text("""
                    INSERT INTO shown_results (run_id, position, business_id, google_place_id,
                                               business_name, score, reasons, facts)
                    VALUES (:rid, :pos, :bid, :pid, :name, :score, CAST(:reasons AS JSONB),
                            CAST(:facts AS JSONB))
                """), {
                    "rid": run_id, "pos": pos, "bid": bid,
                    "pid": card.get("google_place_id") or None,
                    "name": (card.get("business_name") or "")[:300],
                    "score": card.get("opportunity_score"),
                    "reasons": json.dumps(_reasons(card)),
                    "facts": json.dumps(_facts(card), default=str),
                })
        STATS["runs"] += 1
        return run_id
    except Exception as e:
        STATS["errors"] += 1
        if STATS["errors"] in (1, 10, 100):
            print(f"[Journey] run log unavailable ({str(e)[:80]})", flush=True)
        return None
