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
            suggested_message, suggested_follow_up,
            service_angle, user_role, contact_target,
            contact_confidence, contact_reason
        ) VALUES (
            :business_id, :search_id, :opportunity_type, :matched_offer,
            :problem_detected, :why_it_fits, :why_now,
            :opportunity_fit_score, :saturation_score,
            :contactability_score, :source_diversity_score,
            CAST(:signals AS JSONB), CAST(:risk_flags AS JSONB),
            :recommended_offer, :best_contact_path,
            :suggested_message, :suggested_follow_up,
            :service_angle, :user_role, :contact_target,
            :contact_confidence, :contact_reason
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
                "service_angle":          opportunity_data.get("service_angle"),
                "user_role":              opportunity_data.get("user_role"),
                "contact_target":         opportunity_data.get("contact_target"),
                "contact_confidence":     opportunity_data.get("contact_confidence"),
                "contact_reason":         opportunity_data.get("contact_reason"),
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

    # Capture the new status before filtering mutates the dict
    new_status = updates.get("status")

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

            state_result = dict(row) if row else None

    except Exception as exc:
        print(f"[DB] update_opportunity_status error: {exc}", flush=True)
        raise

    # Saturation refresh — runs AFTER the session closes, BEFORE returning
    _action_statuses = {"contacted", "replied", "won", "lost"}
    if new_status in _action_statuses:
        try:
            with get_db_session() as _s:
                opp_row = _s.execute(text("""
                    SELECT business_id, opportunity_type, service_angle,
                           contact_target, best_contact_path
                    FROM opportunities WHERE id = :oid LIMIT 1
                """), {"oid": opportunity_id}).mappings().fetchone()
            if opp_row:
                refresh_opportunity_saturation(
                    business_id      = opp_row["business_id"],
                    opportunity_type = opp_row["opportunity_type"],
                    service_angle    = opp_row.get("service_angle"),
                    contact_target   = opp_row.get("contact_target"),
                    contact_method   = opp_row.get("best_contact_path"),
                )
        except Exception as _sat_err:
            print(f"[DB] saturation refresh (status update) skipped: {_sat_err}", flush=True)

    return state_result


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

        # ── Pull intelligence fields from payload (or leave None) ──────────
        service_angle      = result_data.get("service_angle")      or None
        user_role          = result_data.get("user_role")           or None
        best_contact_path  = result_data.get("best_contact_path")   or None
        contact_target     = result_data.get("contact_target")      or None
        contact_confidence = result_data.get("contact_confidence")  or None
        contact_reason     = result_data.get("contact_reason")      or None

        # ── Infer missing fields if intelligence services are available ────
        if not service_angle or not best_contact_path:
            try:
                from contact_service import infer_service_angle, infer_best_contact_path
                ctx = {
                    "user_role":         user_role,
                    "mode":              mode,
                    "niche":             result_data.get("category") or result_data.get("industry") or "",
                    "signal_preferences": result_data.get("signal_preferences") or [],
                }
                biz_ctx = {
                    "name":         biz_name,
                    "category":     result_data.get("category") or result_data.get("industry") or "",
                    "website":      result_data.get("website") or "",
                    "phone":        result_data.get("phone") or "",
                    "instagram_url": None,
                }
                if not service_angle:
                    service_angle = infer_service_angle(ctx, biz_ctx)
                if not best_contact_path:
                    contact_info = infer_best_contact_path(biz_ctx, ctx)
                    best_contact_path  = contact_info.get("best_contact_path")
                    contact_target     = contact_target or contact_info.get("contact_target")
                    contact_confidence = contact_confidence or contact_info.get("contact_confidence")
                    contact_reason     = contact_reason or contact_info.get("contact_reason")
            except Exception as _intel_err:
                print(f"[DB] intel inference skipped: {_intel_err}", flush=True)

        # 2. Duplicate check — now includes service_angle for finer granularity
        with get_db_session() as session:
            dup_params = {"biz_id": biz_id, "mode": mode, "th": token_hash}
            if service_angle:
                dup_params["angle"] = service_angle
                dup_sql = """
                    SELECT o.id FROM opportunities o
                    JOIN opportunity_states os ON os.opportunity_id = o.id
                    WHERE o.business_id      = :biz_id
                      AND o.opportunity_type = :mode
                      AND o.service_angle    = :angle
                      AND os.token_hash      = :th
                    LIMIT 1
                """
            else:
                dup_sql = """
                    SELECT o.id FROM opportunities o
                    JOIN opportunity_states os ON os.opportunity_id = o.id
                    WHERE o.business_id      = :biz_id
                      AND o.opportunity_type = :mode
                      AND os.token_hash      = :th
                    LIMIT 1
                """
            dup = session.execute(text(dup_sql), dup_params).fetchone()
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
            "best_contact_path":      best_contact_path or result_data.get("best_contact_path") or None,
            "suggested_message":      result_data.get("suggested_message") or None,
            "suggested_follow_up":    result_data.get("suggested_follow_up") or None,
            # Phase 5 intelligence fields
            "service_angle":          service_angle,
            "user_role":              user_role,
            "contact_target":         contact_target,
            "contact_confidence":     contact_confidence,
            "contact_reason":         contact_reason,
        })
        if not opp_id:
            return {"ok": False, "error": "Failed to save opportunity"}

        # 4. Create state tagged to this token
        create_opportunity_state(opp_id, status="saved", token_hash=token_hash)

        # 5. Refresh saturation for this combination
        try:
            refresh_opportunity_saturation(
                business_id=biz_id,
                opportunity_type=mode,
                service_angle=service_angle,
                contact_target=contact_target,
                contact_method=best_contact_path,
            )
        except Exception as _sat_err:
            print(f"[DB] saturation refresh skipped: {_sat_err}", flush=True)

        return {"ok": True, "business_id": biz_id, "opportunity_id": opp_id, "already_saved": False}

    except Exception as exc:
        print(f"[DB] save_scout_result_to_network error: {exc}", flush=True)
        return {"ok": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# PHASE 4 ADDITIONS — saturation helpers
# ---------------------------------------------------------------------------

def refresh_opportunity_saturation(
    business_id:      int,
    opportunity_type: str = None,
    service_angle:    str = None,
    contact_target:   str = None,
    contact_method:   str = None,
) -> dict:
    """
    Recalculate saturation for a specific business + angle + contact combination
    from live discovery_events and opportunity_states data, then upsert into
    opportunity_saturation.

    Returns the upserted saturation row as a dict, or empty dict on error.
    """
    _null = lambda v: v or "unknown"
    opp_type = _null(opportunity_type)
    angle    = _null(service_angle)
    target   = _null(contact_target)
    method   = _null(contact_method)

    try:
        with get_db_session() as session:
            # Total and unique discoveries
            disc = session.execute(text("""
                SELECT
                    COUNT(*)                  AS total_discoveries,
                    COUNT(DISTINCT token_hash) AS unique_discoverers,
                    MAX(shown_at)             AS last_discovered_at
                FROM discovery_events
                WHERE business_id = :biz_id
            """), {"biz_id": business_id}).mappings().fetchone()

            # Save / contact / outcome counts from states joined to opportunities
            saves = session.execute(text("""
                SELECT
                    COUNT(*)                                    AS total_saves,
                    COUNT(DISTINCT os.token_hash)               AS unique_savers,
                    SUM(CASE WHEN os.status = 'contacted'      THEN 1 ELSE 0 END) AS total_contacted,
                    SUM(CASE WHEN os.status = 'replied'        THEN 1 ELSE 0 END) AS total_replied,
                    SUM(CASE WHEN os.status = 'won'            THEN 1 ELSE 0 END) AS total_won,
                    SUM(CASE WHEN os.status = 'lost'           THEN 1 ELSE 0 END) AS total_lost,
                    MAX(CASE WHEN os.status IN ('contacted','replied','won','lost') THEN os.updated_at END) AS last_contacted_at,
                    MAX(os.updated_at)                          AS last_saved_at
                FROM opportunity_states os
                JOIN opportunities o ON o.id = os.opportunity_id
                WHERE o.business_id      = :biz_id
                  AND o.opportunity_type = :opp_type
            """), {"biz_id": business_id, "opp_type": opp_type}).mappings().fetchone()

            row = {
                "total_discoveries":  int(disc["total_discoveries"]  or 0),
                "unique_discoverers": int(disc["unique_discoverers"] or 0),
                "last_discovered_at": disc["last_discovered_at"],
                "total_saves":        int(saves["total_saves"]    or 0),
                "unique_savers":      int(saves["unique_savers"]  or 0),
                "total_contacted":    int(saves["total_contacted"] or 0),
                "total_replied":      int(saves["total_replied"]  or 0),
                "total_won":          int(saves["total_won"]      or 0),
                "total_lost":         int(saves["total_lost"]     or 0),
                "last_contacted_at":  saves["last_contacted_at"],
                "last_saved_at":      saves["last_saved_at"],
            }

            # Upsert into opportunity_saturation
            session.execute(text("""
                INSERT INTO opportunity_saturation (
                    business_id, opportunity_type, service_angle,
                    contact_target, contact_method,
                    total_discoveries, unique_discoverers,
                    total_saves, unique_savers,
                    total_contacted, total_replied, total_won, total_lost,
                    last_discovered_at, last_saved_at, last_contacted_at,
                    last_updated_at
                ) VALUES (
                    :biz_id, :opp_type, :angle,
                    :target, :method,
                    :total_discoveries, :unique_discoverers,
                    :total_saves, :unique_savers,
                    :total_contacted, :total_replied, :total_won, :total_lost,
                    :last_discovered_at, :last_saved_at, :last_contacted_at,
                    NOW()
                )
                ON CONFLICT (business_id, opportunity_type, service_angle, contact_target, contact_method)
                DO UPDATE SET
                    total_discoveries  = EXCLUDED.total_discoveries,
                    unique_discoverers = EXCLUDED.unique_discoverers,
                    total_saves        = EXCLUDED.total_saves,
                    unique_savers      = EXCLUDED.unique_savers,
                    total_contacted    = EXCLUDED.total_contacted,
                    total_replied      = EXCLUDED.total_replied,
                    total_won          = EXCLUDED.total_won,
                    total_lost         = EXCLUDED.total_lost,
                    last_discovered_at = EXCLUDED.last_discovered_at,
                    last_saved_at      = EXCLUDED.last_saved_at,
                    last_contacted_at  = EXCLUDED.last_contacted_at,
                    last_updated_at    = NOW()
            """), {
                "biz_id": business_id, "opp_type": opp_type, "angle": angle,
                "target": target, "method": method, **row,
            })

        return {**row, "business_id": business_id, "opportunity_type": opp_type,
                "service_angle": angle, "contact_target": target, "contact_method": method}

    except Exception as exc:
        print(f"[DB] refresh_opportunity_saturation error: {exc}", flush=True)
        return {}


def get_opportunity_saturation(
    business_id:      int,
    opportunity_type: str = None,
    service_angle:    str = None,
    contact_target:   str = None,
    contact_method:   str = None,
) -> dict:
    """
    Retrieve the cached saturation record for a business + angle + contact combo.
    Returns empty dict if not found.
    """
    _null = lambda v: v or "unknown"
    try:
        with get_db_session() as session:
            row = session.execute(text("""
                SELECT * FROM opportunity_saturation
                WHERE business_id      = :biz_id
                  AND opportunity_type = :opp_type
                  AND service_angle    = :angle
                  AND contact_target   = :target
                  AND contact_method   = :method
                LIMIT 1
            """), {
                "biz_id":   business_id,
                "opp_type": _null(opportunity_type),
                "angle":    _null(service_angle),
                "target":   _null(contact_target),
                "method":   _null(contact_method),
            }).mappings().fetchone()
            return dict(row) if row else {}
    except Exception as exc:
        print(f"[DB] get_opportunity_saturation error: {exc}", flush=True)
        return {}


def get_saturation_label(saturation: dict) -> dict:
    """
    Convert a saturation row (or the output of get_opportunity_saturation)
    into a human-readable label with open angles and contact warnings.

    Rules:
      0 saves for this exact angle/contact = low saturation
      1–2 saves = low/medium
      3–5 saves = medium
      6+ saves = high
      contacted/replied/won raise saturation faster than saves alone
      high discoveries but low saves → don't over-penalise
    """
    if not saturation:
        return {
            "level":           "low",
            "score":           0,
            "label":           "Low Saturation",
            "summary":         "This exact angle and contact path has not been saved by anyone. It is wide open.",
            "open_angles":     [],
            "contact_warning": "",
        }

    saves      = int(saturation.get("total_saves", 0))
    contacted  = int(saturation.get("total_contacted", 0))
    replied    = int(saturation.get("total_replied", 0))
    won        = int(saturation.get("total_won", 0))

    # Weighted saturation score (0–100)
    score = (
        min(saves, 6)      * 5 +    # up to 30
        min(contacted, 5)  * 6 +    # up to 30
        min(replied, 3)    * 5 +    # up to 15
        min(won, 2)        * 12     # up to 24
    )
    score = min(score, 100)

    if score <= 15:
        level = "low"
        label = "Low Saturation"
        summary = (
            f"Only {saves} save(s) and {contacted} contact(s) for this exact angle. "
            "This opportunity is largely untouched."
        )
    elif score <= 40:
        level = "medium"
        label = "Medium Saturation"
        summary = (
            f"{saves} save(s), {contacted} contacted, {replied} replied. "
            "Some outreach has happened — differentiate your approach."
        )
    else:
        level = "high"
        label = "High Saturation"
        summary = (
            f"{saves} saves, {contacted} contacted, {won} won for this combination. "
            "This angle and contact path is crowded — try a different approach."
        )

    # Suggest open angles based on current saturation type
    angle = saturation.get("service_angle", "unknown")
    method = saturation.get("contact_method", "unknown")

    open_angles = []
    if level == "high":
        alt_angle_map = {
            "seo":              ["content_creation", "website_redesign"],
            "content_creation": ["photography", "short_form_video"],
            "website_redesign": ["seo", "booking_conversion"],
            "events_performance": ["influencer_partnership", "local_partnership"],
        }
        open_angles = alt_angle_map.get(angle, ["try a different service angle"])

    # Contact warning
    contact_warning = ""
    if level == "high" and method == "instagram_dm":
        contact_warning = "Instagram DM is saturated for this business — try email, phone, or website contact form instead."
    elif level == "high":
        contact_warning = f"{method} has been used frequently for this angle. Consider a different contact path."

    sat_log = saturation.get("service_angle", "?")
    sat_method = saturation.get("contact_method", "?")
    print(
        f"[Saturation] business_id={saturation.get('business_id', '?')}, "
        f"angle={sat_log}, contact={sat_method}, level={level}",
        flush=True,
    )

    return {
        "level":           level,
        "score":           score,
        "label":           label,
        "summary":         summary,
        "open_angles":     open_angles,
        "contact_warning": contact_warning,
    }


import re as _re


def get_candidate_exposure_stats(
    candidates: list,
    token_hash: str | None = None,
) -> dict:
    """
    Return per-candidate exposure statistics keyed by "normalized_name|location".

    Uses discovery_events (Scout tracking) and opportunity_states (Network saves/contacts).
    Falls back gracefully to empty stats if DB is unavailable or query fails.
    """
    if not candidates:
        return {}

    def _norm_key(c: dict) -> str:
        name = _re.sub(
            r"[^a-z0-9]", "",
            (c.get("business_name") or c.get("name") or "").lower()
        )[:20]
        loc = (c.get("location") or "").lower().strip()
        return f"{name}|{loc}"

    empty_stats = {
        "seen_count_for_token":       0,
        "seen_count_global":          0,
        "last_seen_at_for_token":     None,
        "already_saved_by_token":     False,
        "already_contacted_by_token": False,
    }

    name_list = list(set(
        (c.get("business_name") or c.get("name") or "").strip()
        for c in candidates
        if (c.get("business_name") or c.get("name") or "").strip()
    ))

    if not name_list:
        return {}

    result = {}

    try:
        with get_db_session() as session:
            placeholders = ", ".join([f":n{i}" for i in range(len(name_list))])
            params       = {f"n{i}": n for i, n in enumerate(name_list)}

            biz_rows = session.execute(
                text(f"SELECT id, name, location FROM businesses WHERE name IN ({placeholders})"),
                params,
            ).fetchall()

            biz_lookup = {}
            for row in biz_rows:
                nk = _re.sub(r"[^a-z0-9]", "", (row[1] or "").lower())[:20]
                biz_lookup[nk] = {"id": row[0], "location": row[2]}

            if not biz_lookup:
                recent_rows = session.execute(
                    text("""
                        SELECT id, name, location FROM businesses
                        ORDER BY COALESCE(updated_at, created_at) DESC NULLS LAST
                        LIMIT 500
                    """)
                ).fetchall()
                for row in recent_rows:
                    nk = _re.sub(r"[^a-z0-9]", "", (row[1] or "").lower())[:20]
                    if nk and nk not in biz_lookup:
                        biz_lookup[nk] = {"id": row[0], "location": row[2]}

            if not biz_lookup:
                return {_norm_key(c): dict(empty_stats) for c in candidates}

            biz_ids = [v["id"] for v in biz_lookup.values()]
            id_pl   = ", ".join([f":bid{i}" for i in range(len(biz_ids))])
            id_prm  = {f"bid{i}": bid for i, bid in enumerate(biz_ids)}

            global_counts = {}
            rows = session.execute(
                text(f"""
                    SELECT business_id, COUNT(*) AS cnt, MAX(shown_at) AS last_shown
                    FROM   discovery_events
                    WHERE  business_id IN ({id_pl})
                    GROUP  BY business_id
                """),
                id_prm,
            ).fetchall()
            for row in rows:
                global_counts[row[0]] = {"count": row[1], "last_shown": row[2]}

            token_counts    = {}
            token_saved     = {}
            token_contacted = {}

            if token_hash:
                t_prm = {**id_prm, "token": token_hash}

                t_rows = session.execute(
                    text(f"""
                        SELECT business_id, COUNT(*) AS cnt, MAX(shown_at) AS last_shown
                        FROM   discovery_events
                        WHERE  business_id IN ({id_pl})
                        AND    token_hash = :token
                        GROUP  BY business_id
                    """),
                    t_prm,
                ).fetchall()
                for row in t_rows:
                    token_counts[row[0]] = {"count": row[1], "last_shown": row[2]}

                state_rows = session.execute(
                    text(f"""
                        SELECT o.business_id, os.status
                        FROM   opportunity_states os
                        JOIN   opportunities o ON o.id = os.opportunity_id
                        WHERE  o.business_id IN ({id_pl})
                        AND    os.token_hash = :token
                    """),
                    t_prm,
                ).fetchall()
                for row in state_rows:
                    bid    = row[0]
                    status = (row[1] or "").lower()
                    if "save" in status:
                        token_saved[bid] = True
                    if "contact" in status:
                        token_contacted[bid] = True

            for c in candidates:
                key = _norm_key(c)
                nk  = _re.sub(r"[^a-z0-9]", "", (c.get("business_name") or c.get("name") or "").lower())[:20]
                biz = biz_lookup.get(nk)

                if not biz:
                    result[key] = dict(empty_stats)
                    continue

                bid = biz["id"]
                gc  = global_counts.get(bid, {})
                tc  = token_counts.get(bid,  {})

                result[key] = {
                    "seen_count_for_token":       tc.get("count", 0),
                    "seen_count_global":          gc.get("count", 0),
                    "last_seen_at_for_token":     str(tc["last_shown"]) if tc.get("last_shown") else None,
                    "already_saved_by_token":     bool(token_saved.get(bid, False)),
                    "already_contacted_by_token": bool(token_contacted.get(bid, False)),
                }

        print(f"[Rerank] exposure stats loaded for {len(result)} candidates", flush=True)
        return result

    except Exception as exc:
        print(f"[DB] get_candidate_exposure_stats error: {exc}", flush=True)
        return {_norm_key(c): dict(empty_stats) for c in candidates}
