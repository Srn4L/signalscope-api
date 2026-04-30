"""
database_service.py — High-level DB helpers for Yelhao.

All functions use get_db_session() from db.py and raw SQL / SQLAlchemy core.
No ORM models are defined here — keeping it lightweight.

Functions
---------
init_db()
save_search(search_data)                          — token_hash aware
upsert_business(business_data)
save_opportunity(opportunity_data)
create_opportunity_state(opportunity_id, ...)     — token_hash aware
update_opportunity_state(opportunity_id, updates) — legacy, kept for compat
update_opportunity_status(opportunity_id, ...)    — token_hash scoped
mark_do_not_contact(business_id, ...)
get_followups_due(token_hash)                     — token_hash filtered
get_saved_opportunities()                         — legacy, unscoped
get_opportunities(..., token_hash)                — token_hash filtered
get_opportunity_by_id(opportunity_id)
log_discovery_event(...)                          — Scout discovery tracking
save_scout_result_to_network(result_data, ...)    — manual Network save
"""

import json
import os
from datetime import datetime

from sqlalchemy import text

from db import get_db_session


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _as_json(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


# ---------------------------------------------------------------------------
# init_db
# ---------------------------------------------------------------------------

def init_db():
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    try:
        with open(schema_path, "r") as f:
            sql = f.read()
    except FileNotFoundError:
        return False, f"schema.sql not found at {schema_path}"

    try:
        from db import _get_engine
        engine = _get_engine()
        statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
        with engine.connect() as conn:
            for stmt in statements:
                conn.exec_driver_sql(stmt)
            conn.commit()
        return True, None
    except Exception as exc:
        return False, str(exc)


# ---------------------------------------------------------------------------
# save_search
# ---------------------------------------------------------------------------

def save_search(search_data: dict) -> int | None:
    """
    Insert a row into searches and return the new id.
    token_hash is None-safe — old callers that don't pass it are unaffected.
    """
    sql = text("""
        INSERT INTO searches
            (user_query, mode, offer, target_business_type, location, filters, token_hash)
        VALUES
            (:user_query, :mode, :offer, :target_business_type, :location,
             CAST(:filters AS JSONB), :token_hash)
        RETURNING id
    """)
    try:
        with get_db_session() as session:
            row = session.execute(sql, {
                "user_query":           search_data.get("user_query", ""),
                "mode":                 search_data.get("mode"),
                "offer":                search_data.get("offer"),
                "target_business_type": search_data.get("target_business_type"),
                "location":             search_data.get("location"),
                "filters":              _as_json(search_data.get("filters")),
                "token_hash":           search_data.get("token_hash"),
            }).fetchone()
            return row[0] if row else None
    except Exception as exc:
        print(f"[DB] save_search error: {exc}", flush=True)
        return None


# ---------------------------------------------------------------------------
# upsert_business
# ---------------------------------------------------------------------------

def upsert_business(business_data: dict) -> int | None:
    gp_id = (business_data.get("google_place_id") or "").strip() or None

    try:
        with get_db_session() as session:
            if gp_id:
                sql = text("""
                    INSERT INTO businesses
                        (name, category, location, address, phone, website,
                         google_place_id, google_rating, google_review_count,
                         instagram_url, facebook_url, source, raw_data, updated_at)
                    VALUES
                        (:name, :category, :location, :address, :phone, :website,
                         :google_place_id, :google_rating, :google_review_count,
                         :instagram_url, :facebook_url, :source, CAST(:raw_data AS JSONB), NOW())
                    ON CONFLICT (google_place_id)
                    DO UPDATE SET
                        name                = EXCLUDED.name,
                        category            = COALESCE(EXCLUDED.category,            businesses.category),
                        location            = COALESCE(EXCLUDED.location,            businesses.location),
                        address             = COALESCE(EXCLUDED.address,             businesses.address),
                        phone               = COALESCE(EXCLUDED.phone,               businesses.phone),
                        website             = COALESCE(EXCLUDED.website,             businesses.website),
                        google_rating       = COALESCE(EXCLUDED.google_rating,       businesses.google_rating),
                        google_review_count = COALESCE(EXCLUDED.google_review_count, businesses.google_review_count),
                        instagram_url       = COALESCE(EXCLUDED.instagram_url,       businesses.instagram_url),
                        facebook_url        = COALESCE(EXCLUDED.facebook_url,        businesses.facebook_url),
                        source              = COALESCE(EXCLUDED.source,              businesses.source),
                        raw_data            = COALESCE(EXCLUDED.raw_data,            businesses.raw_data),
                        updated_at          = NOW()
                    RETURNING id
                """)
                row = session.execute(sql, _build_params(business_data)).fetchone()
                return row[0] if row else None

            name     = (business_data.get("name") or "").strip()
            location = (business_data.get("location") or "").strip()

            if not name:
                print("[DB] upsert_business: no name provided, skipping.", flush=True)
                return None

            check = text("""
                SELECT id FROM businesses
                WHERE LOWER(name) = LOWER(:name)
                  AND LOWER(location) = LOWER(:location)
                LIMIT 1
            """)
            existing = session.execute(check, {"name": name, "location": location}).fetchone()

            if existing:
                upd = text("""
                    UPDATE businesses SET
                        category            = COALESCE(:category,            category),
                        address             = COALESCE(:address,             address),
                        phone               = COALESCE(:phone,               phone),
                        website             = COALESCE(:website,             website),
                        google_rating       = COALESCE(:google_rating,       google_rating),
                        google_review_count = COALESCE(:google_review_count, google_review_count),
                        instagram_url       = COALESCE(:instagram_url,       instagram_url),
                        facebook_url        = COALESCE(:facebook_url,        facebook_url),
                        source              = COALESCE(:source,              source),
                        raw_data            = COALESCE(CAST(:raw_data AS JSONB), raw_data),
                        updated_at          = NOW()
                    WHERE id = :id
                """)
                params = _build_params(business_data)
                params["id"] = existing[0]
                session.execute(upd, params)
                return existing[0]
            else:
                ins = text("""
                    INSERT INTO businesses
                        (name, category, location, address, phone, website,
                         google_place_id, google_rating, google_review_count,
                         instagram_url, facebook_url, source, raw_data)
                    VALUES
                        (:name, :category, :location, :address, :phone, :website,
                         :google_place_id, :google_rating, :google_review_count,
                         :instagram_url, :facebook_url, :source, CAST(:raw_data AS JSONB))
                    RETURNING id
                """)
                row = session.execute(ins, _build_params(business_data)).fetchone()
                return row[0] if row else None

    except Exception as exc:
        print(f"[DB] upsert_business error: {exc}", flush=True)
        return None


def _build_params(d: dict) -> dict:
    return {
        "name":                 (d.get("name") or "").strip(),
        "category":             d.get("category"),
        "location":             d.get("location"),
        "address":              d.get("address"),
        "phone":                d.get("phone"),
        "website":              d.get("website"),
        "google_place_id":      (d.get("google_place_id") or "").strip() or None,
        "google_rating":        d.get("google_rating"),
        "google_review_count":  d.get("google_review_count"),
        "instagram_url":        d.get("instagram_url"),
        "facebook_url":         d.get("facebook_url"),
        "source":               d.get("source"),
        "raw_data":             _as_json(d.get("raw_data")),
    }


# ---------------------------------------------------------------------------
# save_opportunity
# ---------------------------------------------------------------------------

def save_opportunity(opportunity_data: dict) -> int | None:
    sql = text("""
        INSERT INTO opportunities (
            business_id, search_id, opportunity_type, matched_offer,
            problem_detected, why_it_fits, why_now,
            opportunity_fit_score, saturation_score,
            contactability_score, source_diversity_score,
            signals, risk_flags,
            recommended_offer, best_contact_path,
            suggested_message, suggested_follow_up
        ) VALUES (
            :business_id, :search_id, :opportunity_type, :matched_offer,
            :problem_detected, :why_it_fits, :why_now,
            :opportunity_fit_score, :saturation_score,
            :contactability_score, :source_diversity_score,
            CAST(:signals AS JSONB), CAST(:risk_flags AS JSONB),
            :recommended_offer, :best_contact_path,
            :suggested_message, :suggested_follow_up
        )
        RETURNING id
    """)
    try:
        with get_db_session() as session:
            row = session.execute(sql, {
                "business_id":            opportunity_data.get("business_id"),
                "search_id":              opportunity_data.get("search_id"),
                "opportunity_type":       opportunity_data.get("opportunity_type"),
                "matched_offer":          opportunity_data.get("matched_offer"),
                "problem_detected":       opportunity_data.get("problem_detected"),
                "why_it_fits":            opportunity_data.get("why_it_fits"),
                "why_now":                opportunity_data.get("why_now"),
                "opportunity_fit_score":  opportunity_data.get("opportunity_fit_score"),
                "saturation_score":       opportunity_data.get("saturation_score"),
                "contactability_score":   opportunity_data.get("contactability_score"),
                "source_diversity_score": opportunity_data.get("source_diversity_score"),
                "signals":                _as_json(opportunity_data.get("signals")),
                "risk_flags":             _as_json(opportunity_data.get("risk_flags")),
                "recommended_offer":      opportunity_data.get("recommended_offer"),
                "best_contact_path":      opportunity_data.get("best_contact_path"),
                "suggested_message":      opportunity_data.get("suggested_message"),
                "suggested_follow_up":    opportunity_data.get("suggested_follow_up"),
            }).fetchone()
            return row[0] if row else None
    except Exception as exc:
        print(f"[DB] save_opportunity error: {exc}", flush=True)
        return None


# ---------------------------------------------------------------------------
# create_opportunity_state
# ---------------------------------------------------------------------------

def create_opportunity_state(
    opportunity_id: int,
    status: str = "new",
    token_hash: str = None,
) -> int | None:
    """
    Insert an initial state row for a newly created opportunity.
    token_hash ties the state to a specific user's Network.
    """
    sql = text("""
        INSERT INTO opportunity_states (opportunity_id, status, token_hash)
        VALUES (:opportunity_id, :status, :token_hash)
        RETURNING id
    """)
    try:
        with get_db_session() as session:
            row = session.execute(sql, {
                "opportunity_id": opportunity_id,
                "status":         status,
                "token_hash":     token_hash,
            }).fetchone()
            return row[0] if row else None
    except Exception as exc:
        print(f"[DB] create_opportunity_state error: {exc}", flush=True)
        return None


# ---------------------------------------------------------------------------
# update_opportunity_state  (legacy — not token-scoped)
# ---------------------------------------------------------------------------

def update_opportunity_state(opportunity_id: int, updates: dict) -> bool:
    allowed = {
        "status", "contact_method", "last_action", "next_action",
        "next_action_date", "notes", "message_used", "response_status", "outcome",
    }
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return False

    set_clause = ", ".join(f"{k} = :{k}" for k in filtered)
    sql = text(f"""
        UPDATE opportunity_states
        SET {set_clause}, updated_at = NOW()
        WHERE opportunity_id = :opportunity_id
    """)
    filtered["opportunity_id"] = opportunity_id

    try:
        with get_db_session() as session:
            session.execute(sql, filtered)
        return True
    except Exception as exc:
        print(f"[DB] update_opportunity_state error: {exc}", flush=True)
        return False


# ---------------------------------------------------------------------------
# mark_do_not_contact
# ---------------------------------------------------------------------------

def mark_do_not_contact(business_id: int, reason: str = None, source: str = None) -> bool:
    sql = text("""
        INSERT INTO do_not_contact (business_id, reason, source)
        VALUES (:business_id, :reason, :source)
    """)
    try:
        with get_db_session() as session:
            session.execute(sql, {"business_id": business_id, "reason": reason, "source": source})
        return True
    except Exception as exc:
        print(f"[DB] mark_do_not_contact error: {exc}", flush=True)
        return False


# ---------------------------------------------------------------------------
# get_followups_due
# ---------------------------------------------------------------------------

def get_followups_due(token_hash: str = None) -> list[dict]:
    """Return past-due opportunity states, scoped to token_hash."""
    token_clause = "AND os.token_hash = :token_hash" if token_hash else ""
    params = {}
    if token_hash:
        params["token_hash"] = token_hash

    sql = text(f"""
        SELECT
            os.id              AS state_id,
            os.opportunity_id,
            os.status,
            os.next_action,
            os.next_action_date,
            os.notes,
            b.name             AS business_name,
            b.location,
            b.website
        FROM opportunity_states os
        JOIN opportunities o ON o.id = os.opportunity_id
        JOIN businesses    b ON b.id = o.business_id
        WHERE os.next_action_date <= NOW()
          AND os.status NOT IN ('won', 'lost', 'do_not_contact', 'closed', 'disqualified')
          {token_clause}
        ORDER BY os.next_action_date ASC
    """)
    try:
        with get_db_session() as session:
            rows = session.execute(sql, params).mappings().all()
            return [dict(r) for r in rows]
    except Exception as exc:
        print(f"[DB] get_followups_due error: {exc}", flush=True)
        return []


# ---------------------------------------------------------------------------
# get_saved_opportunities  (legacy — unscoped)
# ---------------------------------------------------------------------------

def get_saved_opportunities() -> list[dict]:
    sql = text("""
        SELECT
            o.id                     AS opportunity_id,
            o.opportunity_type,
            o.matched_offer,
            o.opportunity_fit_score,
            o.recommended_offer,
            o.suggested_message,
            o.created_at             AS opportunity_created_at,
            os.status,
            os.next_action_date,
            os.outcome,
            b.id                     AS business_id,
            b.name                   AS business_name,
            b.location,
            b.website,
            b.google_rating,
            b.google_review_count
        FROM opportunities o
        LEFT JOIN opportunity_states os ON os.opportunity_id = o.id
        JOIN businesses b              ON b.id = o.business_id
        ORDER BY o.created_at DESC
    """)
    try:
        with get_db_session() as session:
            rows = session.execute(sql).mappings().all()
            return [dict(r) for r in rows]
    except Exception as exc:
        print(f"[DB] get_saved_opportunities error: {exc}", flush=True)
        return []


# ---------------------------------------------------------------------------
# get_opportunities  (Networks dashboard — token-scoped)
# ---------------------------------------------------------------------------

def get_opportunities(
    status:     str = None,
    mode:       str = None,
    source:     str = None,
    limit:      int = 200,
    offset:     int = 0,
    token_hash: str = None,
) -> list[dict]:
    """
    Return this token's saved Network opportunities.

    Uses JOIN (not LEFT JOIN) on opportunity_states so only rows where
    os.token_hash matches are returned — that's the user isolation mechanism.
    """
    where_clauses = []
    params = {"limit": limit, "offset": offset}

    if token_hash:
        where_clauses.append("os.token_hash = :token_hash")
        params["token_hash"] = token_hash
    if status:
        where_clauses.append("os.status = :status")
        params["status"] = status
    if mode:
        where_clauses.append("o.opportunity_type = :mode")
        params["mode"] = mode
    if source:
        where_clauses.append("b.source = :source")
        params["source"] = source

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    sql = text(f"""
        SELECT
            o.id                     AS opportunity_id,
            b.id                     AS business_id,
            b.name                   AS business_name,
            b.category,
            b.location,
            b.address,
            b.phone,
            b.website,
            b.source,
            b.google_rating,
            b.google_review_count,
            o.opportunity_type,
            o.matched_offer,
            o.problem_detected,
            o.why_it_fits,
            o.why_now,
            o.opportunity_fit_score,
            o.saturation_score,
            o.contactability_score,
            o.source_diversity_score,
            o.signals,
            o.recommended_offer,
            o.best_contact_path,
            o.suggested_message,
            o.suggested_follow_up,
            o.created_at,
            os.id                    AS state_id,
            os.status,
            os.token_hash,
            os.contact_method,
            os.last_action,
            os.next_action,
            os.next_action_date,
            os.notes,
            os.message_used,
            os.response_status,
            os.outcome,
            os.updated_at
        FROM opportunities o
        JOIN businesses b             ON b.id = o.business_id
        JOIN opportunity_states os    ON os.opportunity_id = o.id
        {where_sql}
        ORDER BY o.created_at DESC
        LIMIT :limit OFFSET :offset
    """)
    try:
        with get_db_session() as session:
            rows = session.execute(sql, params).mappings().all()
            return [dict(r) for r in rows]
    except Exception as exc:
        print(f"[DB] get_opportunities error: {exc}", flush=True)
        return []


# ---------------------------------------------------------------------------
# get_opportunity_by_id
# ---------------------------------------------------------------------------

def get_opportunity_by_id(opportunity_id: int) -> dict | None:
    sql = text("""
        SELECT
            o.id                     AS opportunity_id,
            b.id                     AS business_id,
            b.name                   AS business_name,
            b.category,
            b.location,
            b.address,
            b.phone,
            b.website,
            b.source,
            b.google_rating,
            b.google_review_count,
            b.instagram_url,
            b.facebook_url,
            o.opportunity_type,
            o.matched_offer,
            o.problem_detected,
            o.why_it_fits,
            o.why_now,
            o.opportunity_fit_score,
            o.saturation_score,
            o.contactability_score,
            o.source_diversity_score,
            o.signals,
            o.risk_flags,
            o.recommended_offer,
            o.best_contact_path,
            o.suggested_message,
            o.suggested_follow_up,
            o.created_at,
            o.updated_at,
            os.id                    AS state_id,
            os.status,
            os.contact_method,
            os.last_action,
            os.next_action,
            os.next_action_date,
            os.notes,
            os.message_used,
            os.response_status,
            os.outcome
        FROM opportunities o
        JOIN businesses b              ON b.id = o.business_id
        LEFT JOIN opportunity_states os ON os.opportunity_id = o.id
        WHERE o.id = :opportunity_id
        LIMIT 1
    """)
    try:
        with get_db_session() as session:
            row = session.execute(sql, {"opportunity_id": opportunity_id}).mappings().fetchone()
            return dict(row) if row else None
    except Exception as exc:
        print(f"[DB] get_opportunity_by_id error: {exc}", flush=True)
        return None


# ---------------------------------------------------------------------------
# update_opportunity_status  (token-scoped)
# ---------------------------------------------------------------------------

VALID_STATUSES = {
    "new", "saved", "contacted", "replied", "follow_up_needed",
    "in_conversation", "won", "lost", "do_not_contact",
}

def update_opportunity_status(
    opportunity_id: int,
    updates: dict,
    token_hash: str = None,
) -> dict | None:
    """
    Update mutable fields on the opportunity_state for a given opportunity_id,
    scoped to the provided token_hash.

    If no state row exists for this token + opportunity, one is auto-created
    (status='saved') before applying the update.
    """
    allowed = {
        "status", "contact_method", "last_action", "next_action",
        "next_action_date", "notes", "message_used", "response_status", "outcome",
    }
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return None

    if "status" in filtered and filtered["status"] not in VALID_STATUSES:
        raise ValueError(f"Invalid status: {filtered['status']!r}. Must be one of {sorted(VALID_STATUSES)}")

    try:
        with get_db_session() as session:
            if token_hash:
                existing = session.execute(text("""
                    SELECT id FROM opportunity_states
                    WHERE opportunity_id = :oid AND token_hash = :th
                    LIMIT 1
                """), {"oid": opportunity_id, "th": token_hash}).fetchone()
                if not existing:
                    session.execute(text("""
                        INSERT INTO opportunity_states (opportunity_id, status, token_hash)
                        VALUES (:oid, 'saved', :th)
                    """), {"oid": opportunity_id, "th": token_hash})
                    session.flush()
            else:
                existing = session.execute(text(
                    "SELECT id FROM opportunity_states WHERE opportunity_id = :oid LIMIT 1"
                ), {"oid": opportunity_id}).fetchone()
                if not existing:
                    session.execute(text("""
                        INSERT INTO opportunity_states (opportunity_id, status)
                        VALUES (:oid, 'new')
                    """), {"oid": opportunity_id})
                    session.flush()

            set_clause = ", ".join(f"{k} = :{k}" for k in filtered)
            if token_hash:
                where_clause = "opportunity_id = :opportunity_id AND token_hash = :token_hash"
                filtered["token_hash"] = token_hash
            else:
                where_clause = "opportunity_id = :opportunity_id"

            filtered["opportunity_id"] = opportunity_id
            session.execute(text(f"""
                UPDATE opportunity_states
                SET {set_clause}, updated_at = NOW()
                WHERE {where_clause}
            """), filtered)

            if token_hash:
                row = session.execute(text("""
                    SELECT id, opportunity_id, status, contact_method, last_action,
                           next_action, next_action_date, notes, message_used,
                           response_status, outcome, token_hash, created_at, updated_at
                    FROM opportunity_states
                    WHERE opportunity_id = :opportunity_id AND token_hash = :token_hash
                    LIMIT 1
                """), {"opportunity_id": opportunity_id, "token_hash": token_hash}).mappings().fetchone()
            else:
                row = session.execute(text("""
                    SELECT id, opportunity_id, status, contact_method, last_action,
                           next_action, next_action_date, notes, message_used,
                           response_status, outcome, created_at, updated_at
                    FROM opportunity_states
                    WHERE opportunity_id = :opportunity_id
                    LIMIT 1
                """), {"opportunity_id": opportunity_id}).mappings().fetchone()

            return dict(row) if row else None

    except Exception as exc:
        print(f"[DB] update_opportunity_status error: {exc}", flush=True)
        raise


# ---------------------------------------------------------------------------
# log_discovery_event
# ---------------------------------------------------------------------------

def log_discovery_event(
    token_hash: str,
    search_id: int,
    business_id: int,
    mode: str,
    niche: str,
    location: str,
) -> bool:
    """
    Record that a business was shown in a Scout result for this token.
    Does NOT create opportunities or states — discovery tracking only.
    """
    sql = text("""
        INSERT INTO discovery_events
            (token_hash, search_id, business_id, mode, niche, location)
        VALUES
            (:token_hash, :search_id, :business_id, :mode, :niche, :location)
    """)
    try:
        with get_db_session() as session:
            session.execute(sql, {
                "token_hash":  token_hash,
                "search_id":   search_id,
                "business_id": business_id,
                "mode":        mode,
                "niche":       niche,
                "location":    location,
            })
        return True
    except Exception as exc:
        print(f"[DB] log_discovery_event error: {exc}", flush=True)
        return False


# ---------------------------------------------------------------------------
# save_scout_result_to_network
# ---------------------------------------------------------------------------

def save_scout_result_to_network(result_data: dict, token_hash: str) -> dict:
    """
    Convert a Scout card payload into a Network entry for this token.

    Flow:
      1. Upsert business
      2. Duplicate check: same business_id + opportunity_type + token_hash
      3. Save opportunity
      4. Create opportunity_state(status='saved', token_hash=token_hash)

    Returns:
      {"ok": True, "business_id": int, "opportunity_id": int, "already_saved": bool}
    """
    biz_name = (result_data.get("business_name") or result_data.get("name") or "").strip()
    if not biz_name:
        return {"ok": False, "error": "business_name is required"}

    mode   = result_data.get("mode") or result_data.get("opportunity_type") or "outreach"
    source = result_data.get("source") or result_data.get("platform") or ""

    score_reasons = result_data.get("score_reasons") or []
    if score_reasons and isinstance(score_reasons[0], dict):
        why_it_fits = "; ".join(r.get("signal", "") for r in score_reasons if r.get("signal"))
        problem     = score_reasons[0].get("signal") if score_reasons else None
    else:
        why_it_fits = "; ".join(str(r) for r in score_reasons) if score_reasons else None
        problem     = str(score_reasons[0]) if score_reasons else None

    try:
        # 1. Upsert business
        biz_id = upsert_business({
            "name":                biz_name,
            "category":            result_data.get("category") or result_data.get("industry") or "",
            "location":            result_data.get("location") or "",
            "address":             result_data.get("address") or "",
            "phone":               result_data.get("phone") or None,
            "website":             result_data.get("website") or "",
            "google_place_id":     result_data.get("google_place_id") or result_data.get("place_id") or None,
            "google_rating":       result_data.get("google_rating") or result_data.get("rating") or None,
            "google_review_count": result_data.get("google_review_count") or result_data.get("review_count") or None,
            "instagram_url":       None,
            "facebook_url":        None,
            "source":              source,
            "raw_data": {
                "platform":         result_data.get("platform") or source,
                "score_confidence": result_data.get("score_confidence"),
                "score_reasons":    score_reasons,
            },
        })
        if not biz_id:
            return {"ok": False, "error": "Failed to save business"}

        # 2. Duplicate check
        with get_db_session() as session:
            dup = session.execute(text("""
                SELECT o.id FROM opportunities o
                JOIN opportunity_states os ON os.opportunity_id = o.id
                WHERE o.business_id      = :biz_id
                  AND o.opportunity_type = :mode
                  AND os.token_hash      = :th
                LIMIT 1
            """), {"biz_id": biz_id, "mode": mode, "th": token_hash}).fetchone()
            if dup:
                return {"ok": True, "business_id": biz_id, "opportunity_id": dup[0], "already_saved": True}

        # 3. Save opportunity
        signals_payload = result_data.get("signals") or {
            "score_reasons":    score_reasons,
            "score_confidence": result_data.get("score_confidence"),
            "source":           source,
            "platform":         result_data.get("platform") or source,
            "mode":             mode,
        }
        opp_id = save_opportunity({
            "business_id":            biz_id,
            "search_id":              result_data.get("search_id") or None,
            "opportunity_type":       mode,
            "matched_offer":          None,
            "problem_detected":       result_data.get("problem_detected") or problem,
            "why_it_fits":            result_data.get("why_it_fits") or why_it_fits,
            "why_now":                None,
            "opportunity_fit_score":  result_data.get("opportunity_fit_score") or result_data.get("opportunity_score"),
            "saturation_score":       None,
            "contactability_score":   None,
            "source_diversity_score": None,
            "signals":                signals_payload,
            "risk_flags":             None,
            "recommended_offer":      result_data.get("recommended_offer") or None,
            "best_contact_path":      result_data.get("best_contact_path") or None,
            "suggested_message":      result_data.get("suggested_message") or None,
            "suggested_follow_up":    result_data.get("suggested_follow_up") or None,
        })
        if not opp_id:
            return {"ok": False, "error": "Failed to save opportunity"}

        # 4. Create state tagged to this token
        create_opportunity_state(opp_id, status="saved", token_hash=token_hash)

        return {"ok": True, "business_id": biz_id, "opportunity_id": opp_id, "already_saved": False}

    except Exception as exc:
        print(f"[DB] save_scout_result_to_network error: {exc}", flush=True)
        return {"ok": False, "error": str(exc)}
