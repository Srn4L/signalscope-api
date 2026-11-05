"""
Meaning match for Scout searches (2026-11-04, first version: ranking only).

The user's words are embedded and compared with a small catalog of business
types and cuisines, so "jollof", "waakye spot" or "Ghanaian food" land on
West African without hand-written keyword lists. The result only decides
which business types a search is about; it never changes facts on a card and
never changes the Google query.

Storage: one table, query_embeddings, created here if missing. It holds the
embedding of every catalog entry and every search text, plus what each
search matched. The same text is never embedded twice, so repeat searches
and owner reruns cost nothing.

Owner replay: a capture (X-Replay: all) stores embeddings like any other
run; a saved rerun reads them from the table and never calls OpenAI. A
saved rerun that lacks one lists it as missing and continues with keyword
matching only.
"""
import hashlib
import json
import math
import os
import threading
import time

EMBED_MODEL = os.environ.get("EMBED_MODEL", "text-embedding-3-small")
# Minimum cosine similarity for a catalog entry to count as what a search is
# about. Tuned on owner reruns; every response lists the top scores.
MATCH_MIN = float(os.environ.get("EMBED_MATCH_MIN", "0.40") or 0.40)
# Other entries count too when within this distance of the best one.
MATCH_MARGIN = float(os.environ.get("EMBED_MATCH_MARGIN", "0.04") or 0.04)
MAX_MATCHES = 2

CATALOG_VERSION = "2026-11-04.1"

# key -> text that is embedded. Keys match app.QUERY_TYPE_TERMS.
CATALOG = {
    "nail":          "nail salon: manicure, pedicure, gel nails, acrylic nails, nail art",
    "lash":          "eyelash extensions and lash lift studio",
    "brow":          "eyebrow threading, brow shaping and brow lamination",
    "wax":           "waxing salon: body waxing and brazilian wax",
    "hair":          "hair salon: haircut, color, blowout, braids, locs, weave",
    "barber":        "barbershop: men's haircuts, fades, beard trims, shape-ups",
    "massage":       "massage therapy and bodywork",
    "spa":           "day spa: facials, body treatments, relaxation",
    "tattoo":        "tattoo and piercing studio",
    "tanning":       "tanning salon and spray tan",
    "makeup":        "makeup artist for events, bridal makeup",
    "cafe":          "coffee shop or cafe: espresso, latte, pastries",
    "bakery":        "bakery: bread, cakes, pastries, cookies",
    "pizza":         "pizza place or pizzeria: slices and pies",
    "gym":           "gym or fitness center: weights, classes, personal training",
    "yoga":          "yoga studio and yoga classes",
    "pilates":       "pilates studio, reformer classes",
    "dental":        "dentist or dental office, orthodontist",
    "west_african":  "West African restaurant: Ghanaian, Nigerian, Senegalese, Ivorian food such as jollof rice, waakye, suya, fufu, egusi soup, banku",
    "african":       "African restaurant: African cuisine and dishes from across Africa",
    "caribbean":     "Caribbean restaurant: Jamaican, Haitian, Trinidadian, Guyanese food such as jerk chicken, oxtail, roti, beef patties",
    "ethiopian":     "Ethiopian or Eritrean restaurant: injera, tibs, doro wat",
    "latin":         "Latin American restaurant: Dominican, Puerto Rican, Cuban, Colombian, Salvadoran food such as mofongo, empanadas, pupusas",
    "mexican":       "Mexican restaurant or taqueria: tacos, burritos, birria",
    "chinese":       "Chinese restaurant: dim sum, dumplings, Sichuan, Cantonese",
    "japanese":      "Japanese restaurant: sushi, ramen, izakaya",
    "korean":        "Korean restaurant: Korean barbecue, bibimbap, fried chicken",
    "thai":          "Thai restaurant: pad thai, curries, boat noodles",
    "indian":        "Indian or South Asian restaurant: curry, biryani, dosa, tandoori",
    "halal":         "halal food: halal restaurant, halal cart, chicken over rice",
    "middle_eastern":"Middle Eastern or Mediterranean restaurant: falafel, shawarma, kebab, Lebanese, Turkish, Yemeni",
    "italian":       "Italian restaurant: pasta, trattoria",
    "soul_food":     "soul food and Southern cooking: fried chicken, mac and cheese, collard greens",
    "vegan":         "vegan or vegetarian restaurant, plant-based food",
    "seafood":       "seafood restaurant: fish, oysters, crab, shrimp",
    "bbq":           "barbecue restaurant or smokehouse: brisket, ribs",
    "burger":        "burger joint: burgers and fries",
    "dessert":       "dessert shop: ice cream, gelato, donuts, cupcakes",
}

_LOCK = threading.Lock()
_MEM = {}            # text_hash -> (list[float], stored_at)
# Each worker keeps vectors in memory only this long, so a deletion made
# through another worker stops being served here within this time.
MEM_TTL_SECONDS = int(os.environ.get("EMBED_MEM_TTL_SECONDS", "300") or 300)
# After a deletion, the same text can't be stored again for this long, so a
# request still in flight when it was deleted can't write it back.
TOMBSTONE_MINUTES = 10
_TABLE_READY = False
STATS = {"live_calls": 0, "cache_hits": 0, "errors": 0}


def cache_key(text: str) -> str:
    """The exact, model-specific key a search text is cached under (full text)."""
    return _hash(text)


def _hash(text: str) -> str:
    raw = f"{EMBED_MODEL}|{(text or '').strip().lower()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40]


# ── storage ─────────────────────────────────────────────────────────────────

def _db_session():
    from db import get_db_session  # optional dependency
    return get_db_session()


def _ensure_table():
    """
    Create the table in its own transaction and mark it ready only after it
    commits (a rolled-back write must not leave it marked ready).
    """
    global _TABLE_READY
    if _TABLE_READY:
        return
    from sqlalchemy import text
    with _db_session() as session:
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS query_embeddings (
                text_hash        TEXT PRIMARY KEY,
                kind             TEXT,
                model            TEXT,
                text             TEXT,
                embedding        JSONB,
                matches          JSONB,
                catalog_version  TEXT,
                seen_count       INTEGER DEFAULT 1,
                created_at       TIMESTAMP DEFAULT NOW(),
                last_seen_at     TIMESTAMP DEFAULT NOW(),
                deleted_at       TIMESTAMP
            )
        """))
    _TABLE_READY = True


def _db_get(hashes):
    """{hash: embedding} for the hashes found in the table."""
    if not hashes:
        return {}
    try:
        from sqlalchemy import text
        _ensure_table()
        with _db_session() as s:
            rows = s.execute(text(
                "SELECT text_hash, embedding FROM query_embeddings "
                "WHERE text_hash = ANY(:h) AND deleted_at IS NULL AND embedding IS NOT NULL"
            ), {"h": list(hashes)}).fetchall()
        out = {}
        for h, emb in rows:
            vec = emb if isinstance(emb, list) else json.loads(emb)
            if vec:
                out[h] = vec
        return out
    except Exception as e:
        _store_error("read", e)
        return {}


def _db_put(rows):
    """rows: [(hash, kind, text, embedding)]"""
    if not rows:
        return
    try:
        from sqlalchemy import text
        _ensure_table()
        with _db_session() as s:
            for h, kind, txt, vec in rows:
                s.execute(text("""
                    INSERT INTO query_embeddings (text_hash, kind, model, text, embedding)
                    VALUES (:h, :k, :m, :t, CAST(:e AS JSONB))
                    ON CONFLICT (text_hash) DO UPDATE
                        SET kind = EXCLUDED.kind, model = EXCLUDED.model, text = EXCLUDED.text,
                            embedding = EXCLUDED.embedding, matches = NULL, seen_count = 1,
                            created_at = NOW(), last_seen_at = NOW(), deleted_at = NULL
                        WHERE query_embeddings.deleted_at IS NOT NULL
                          AND query_embeddings.deleted_at < NOW() - make_interval(mins => :tomb)
                """), {"h": h, "k": kind, "m": EMBED_MODEL, "t": txt[:1000], "e": json.dumps(vec),
                   "tomb": TOMBSTONE_MINUTES})
    except Exception as e:
        _store_error("write", e)


def record_match(query_text: str, matches: dict):
    """Remember what a search matched (feeds the search log). Never raises."""
    try:
        from sqlalchemy import text
        _ensure_table()
        with _db_session() as s:
            s.execute(text("""
                UPDATE query_embeddings
                SET matches = CAST(:mt AS JSONB), catalog_version = :v,
                    seen_count = COALESCE(seen_count, 0) + 1, last_seen_at = NOW()
                WHERE text_hash = :h AND deleted_at IS NULL
            """), {"h": _hash(query_text), "mt": json.dumps(matches), "v": CATALOG_VERSION})
    except Exception as e:
        _store_error("write", e)


def _recently_deleted(hashes) -> set:
    """Keys deleted within TOMBSTONE_MINUTES (never cached again until then)."""
    if not hashes:
        return set()
    try:
        from sqlalchemy import text
        _ensure_table()
        with _db_session() as s:
            rows = s.execute(text(
                "SELECT text_hash FROM query_embeddings WHERE text_hash = ANY(:h) "
                "AND deleted_at IS NOT NULL AND deleted_at >= NOW() - make_interval(mins => :m)"
            ), {"h": list(hashes), "m": TOMBSTONE_MINUTES}).fetchall()
        return {r[0] for r in rows}
    except Exception as e:
        _store_error("read", e)
        return set()


def tombstone_keys(session, keys) -> int:
    """
    Inside the caller's transaction: erase the vectors, text and matches of
    these search keys and mark them deleted, so no worker reads them from the
    table and an in-flight request can't store them again for a while.
    Catalog entries are never touched. Returns rows affected.
    """
    keys = sorted({k for k in keys or [] if k})
    if not keys:
        return 0
    from sqlalchemy import text
    r = session.execute(text("""
        UPDATE query_embeddings
        SET embedding = NULL, text = NULL, matches = NULL, deleted_at = NOW()
        WHERE text_hash = ANY(:h) AND kind = 'query'
    """), {"h": keys})
    forget_local(keys)
    return r.rowcount or 0


def forget_local(keys):
    """Drop this worker's in-memory copies of these keys."""
    with _LOCK:
        for k in keys or []:
            _MEM.pop(k, None)


def _store_error(what, e):
    global _TABLE_READY
    _TABLE_READY = False          # next call re-checks the table
    STATS["errors"] += 1
    if STATS["errors"] in (1, 10, 100):
        print(f"[Embed] store {what} unavailable ({str(e)[:80]}); memory only", flush=True)


# ── embeddings ──────────────────────────────────────────────────────────────

def _replay():
    try:
        import replay_service as rs
        return rs
    except Exception:
        return None


def _saved_run() -> bool:
    rs = _replay()
    return rs is not None and rs.current_mode() == "saved" and rs.REPLAY_ON


def _live_embed(texts, budget_ok):
    """One OpenAI call for all texts, or None (no key, cap reached, error)."""
    key = os.environ.get("OPENAI_API_KEY", "")
    if not key or not texts:
        return None
    if budget_ok is not None and not budget_ok("openai_embed"):
        print("  [Embed] skipped: daily cap reached", flush=True)
        return None
    try:
        import requests
        r = requests.post("https://api.openai.com/v1/embeddings",
                          headers={"Authorization": f"Bearer {key}"},
                          json={"model": EMBED_MODEL, "input": list(texts)}, timeout=10)
        if r.status_code != 200:
            print(f"  [Embed] OpenAI embeddings failed: HTTP {r.status_code} {r.text[:160]}", flush=True)
            return None
        data = sorted(r.json().get("data") or [], key=lambda d: d.get("index", 0))
        vecs = [d.get("embedding") for d in data]
        if len(vecs) != len(texts) or not all(vecs):
            return None
        STATS["live_calls"] += 1
        return vecs
    except Exception as e:
        print(f"  [Embed] OpenAI embeddings error: {str(e)[:160]}", flush=True)
        return None


def embed_many(items, budget_ok=None):
    """
    items: [(kind, text)]. Returns {text: vector} for every text available.
    Memory, then the table, then one live call for the rest (never in a
    saved rerun, which lists anything missing instead).
    """
    want = {}
    for kind, txt in items:
        if txt and txt.strip():
            want[_hash(txt)] = (kind, txt)
    if not want:
        return {}
    found = {}
    now = time.time()
    with _LOCK:
        for h in want:
            hit = _MEM.get(h)
            if hit and now - hit[1] < MEM_TTL_SECONDS:
                found[h] = hit[0]
            elif hit:
                _MEM.pop(h, None)
    rest = [h for h in want if h not in found]
    if rest:
        from_db = _db_get(rest)
        with _LOCK:
            for h, v in from_db.items():
                _MEM[h] = (v, now)
        found.update(from_db)
    STATS["cache_hits"] += len(found)
    rs = _replay()
    run = rs.run_state() if rs is not None else None
    if run is not None and found:
        run.add_replayed()          # one stored lookup, like one replayed call
    rest = [h for h in want if h not in found]
    if rest:
        if _saved_run():
            if run is not None:
                for h in rest:
                    run.add_missing("embedding", f"{want[h][0]}: {want[h][1][:80]}")
        else:
            vecs = _live_embed([want[h][1] for h in rest], budget_ok)
            if vecs:
                if run is not None:
                    run.add_live("api.openai.com")
                blocked = _recently_deleted(rest)
                rows = []
                with _LOCK:
                    for h, v in zip(rest, vecs):
                        found[h] = v                      # usable for this request
                        if h in blocked:
                            continue                      # deleted moments ago: don't keep it
                        _MEM[h] = (v, time.time())
                        rows.append((h, want[h][0], want[h][1], v))
                _db_put(rows)
    return {want[h][1]: v for h, v in found.items()}


def _cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


def match_query(query_text: str, budget_ok=None) -> dict:
    """
    What business types a search is about, by meaning.
    Returns {"terms": [...], "top": [[key, score], ...], "status": str}.
    status: "matched", "no_match", "unavailable" (no embedding could be made).
    """
    q = (query_text or "").strip()
    out = {"terms": [], "top": [], "status": "unavailable", "model": EMBED_MODEL,
           "min": MATCH_MIN, "catalog_version": CATALOG_VERSION}
    if not q:
        return out
    items = [("catalog", t) for t in CATALOG.values()] + [("query", q)]
    vecs = embed_many(items, budget_ok)
    qv = vecs.get(q)
    if not qv:
        return out
    scored = []
    for key, txt in CATALOG.items():
        cv = vecs.get(txt)
        if cv:
            scored.append((key, round(_cosine(qv, cv), 3)))
    if not scored:
        return out
    scored.sort(key=lambda x: -x[1])
    out["top"] = [list(x) for x in scored[:3]]
    best = scored[0][1]
    if best < MATCH_MIN:
        out["status"] = "no_match"
    else:
        out["terms"] = [k for k, s in scored if s >= MATCH_MIN and s >= best - MATCH_MARGIN][:MAX_MATCHES]
        out["status"] = "matched"
    record_match(q, {"terms": out["terms"], "top": out["top"]})
    return out
