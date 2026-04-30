"""
database_service.py — High-level DB helpers for Yelhao.

All functions use get_db_session() from db.py and raw SQL / SQLAlchemy core.
No ORM models are defined here — keeping it lightweight.

Functions
---------
init_db()
save_search(search_data)
upsert_business(business_data)
save_opportunity(opportunity_data)
create_opportunity_state(opportunity_id, status='new')
update_opportunity_state(opportunity_id, updates)
mark_do_not_contact(business_id, reason=None, source=None)
get_followups_due()
get_saved_opportunities()
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
    """
    Make a value safe to insert into a JSONB column.
    SQLAlchemy / psycopg2 accept a Python dict/list directly when the column
    is JSONB, but serialising to a JSON string is the safest fallback.
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


# ---------------------------------------------------------------------------
# init_db
# ---------------------------------------------------------------------------

def init_db():
    """
    Read schema.sql from disk and execute it.
    Safe to call multiple times — uses CREATE TABLE IF NOT EXISTS throughout.
    Never drops tables.

    Returns:
        (True, None)            — success
        (False, error_string)   — failure
    """
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    try:
        with open(schema_path, "r") as f:
            sql = f.read()
    except FileNotFoundError:
        return False, f"schema.sql not found at {schema_path}"

    try:
        from db import _get_engine
        engine = _get_engine()
        # Split on semicolons, skip blank/comment-only chunks, execute one by one.
        # This is the safest approach for multi-statement DDL with psycopg2.
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

    Expected keys (all optional except user_query):
        user_query, mode, offer, target_business_type, location, filters
    """
    sql = text("""
        INSERT INTO searches
            (user_query, mode, offer, target_business_type, location, filters)
        VALUES
            (:user_query, :mode, :offer, :target_business_type, :location, CAST(:filters AS JSONB))
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
            }).fetchone()
            return row[0] if row else None
    except Exception as exc:
        print(f"[DB] save_search error: {exc}", flush=True)
        return None


# ---------------------------------------------------------------------------
# upsert_business
# ---------------------------------------------------------------------------

def upsert_business(business_data: dict) -> int | None:
    """
    Insert or update a business row. Returns the row id.

    Dedup strategy:
      1. If google_place_id is present → use ON CONFLICT on google_place_id.
      2. Otherwise → look up by (name, location); insert if not found.

    Recognised keys:
        name, category, location, address, phone, website,
        google_place_id, google_rating, google_review_count,
        instagram_url, facebook_url, source, raw_data
    """
    gp_id = (business_data.get("google_place_id") or "").strip() or None

    try:
        with get_db_session() as session:
            # ── Path 1: we have a google_place_id ────────────────────────────
            if gp_id:
                sql = text("""
                    INSERT INTO businesses
                        (name, category, location, address, phone, website,
                         google_place_id, google_rating, google_review_count,
                         instagram_url, facebook_url, source, raw_data, updated_at)
                    VALUES
                        (:name, :category, :location, :address, :phone, :website,
                         :google_place_id, :google_rating, :google_review_count,
                         :instagram_url, :facebook_url, :source, CAST(:raw_data AS JSONB),
                         NOW())
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

            # ── Path 2: no google_place_id — name + location lookup ───────────
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
                # Update the existing row
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
                # Fresh insert
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
    """Map business_data dict to SQL parameter dict."""
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
    """
    Insert a row into opportunities and return the new id.

    Required keys: business_id
    Optional: search_id, opportunity_type, matched_offer, problem_detected,
              why_it_fits, why_now, opportunity_fit_score, saturation_score,
              contactability_score, source_diversity_score, signals,
              risk_flags, recommended_offer, best_contact_path,
              suggested_message, suggested_follow_up
    """
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

def create_opportunity_state(opportunity_id: int, status: str = "new") -> int | None:
    """
    Insert an initial state row for a newly created opportunity.
    Returns the new opportunity_states id.
    """
    sql = text("""
        INSERT INTO opportunity_states (opportunity_id, status)
        VALUES (:opportunity_id, :status)
        RETURNING id
    """)
    try:
        with get_db_session() as session:
            row = session.execute(sql, {
                "opportunity_id": opportunity_id,
                "status":         status,
            }).fetchone()
            return row[0] if row else None
    except Exception as exc:
        print(f"[DB] create_opportunity_state error: {exc}", flush=True)
        return None


# ---------------------------------------------------------------------------
# update_opportunity_state
# ---------------------------------------------------------------------------

def update_opportunity_state(opportunity_id: int, updates: dict) -> bool:
    """
    Update mutable fields on the opportunity_state row for a given
    opportunity_id. Only keys present in `updates` are changed.

    Recognised update keys:
        status, contact_method, last_action, next_action,
        next_action_date, notes, message_used, response_status, outcome
    """
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
    """
    Insert a do-not-contact record for a business.
    Multiple records per business are allowed (e.g. different reasons).
    """
    sql = text("""
        INSERT INTO do_not_contact (business_id, reason, source)
        VALUES (:business_id, :reason, :source)
    """)
    try:
        with get_db_session() as session:
            session.execute(sql, {
                "business_id": business_id,
                "reason":      reason,
                "source":      source,
            })
        return True
    except Exception as exc:
        print(f"[DB] mark_do_not_contact error: {exc}", flush=True)
        return False


# ---------------------------------------------------------------------------
# get_followups_due
# ---------------------------------------------------------------------------

def get_followups_due() -> list[dict]:
    """
    Return all opportunity states where next_action_date is in the past
    and status is not terminal (won, lost, do_not_contact, closed, disqualified).
    """
    sql = text("""
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
        ORDER BY os.next_action_date ASC
    """)
    try:
        with get_db_session() as session:
            rows = session.execute(sql).mappings().all()
            return [dict(r) for r in rows]
    except Exception as exc:
        print(f"[DB] get_followups_due error: {exc}", flush=True)
        return []


# ---------------------------------------------------------------------------
# get_saved_opportunities
# ---------------------------------------------------------------------------

def get_saved_opportunities() -> list[dict]:
    """
    Return all opportunities with their current state and business info,
    ordered by most recently created.
    """
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
# get_opportunities  (filterable list for dashboard)
# ---------------------------------------------------------------------------

def get_opportunities(
    status: str = None,
    mode:   str = None,
    source: str = None,
    limit:  int = 50,
    offset: int = 0,
) -> list[dict]:
    """
    Return opportunities joined with businesses and opportunity_states.
    Optional filters: status, mode (opportunity_type), source (businesses.source).
    """
    where_clauses = []
    params = {"limit": limit, "offset": offset}

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
            os.status,
            os.contact_method,
            os.last_action,
            os.next_action,
            os.next_action_date,
            os.notes,
            os.outcome
        FROM opportunities o
        JOIN businesses b              ON b.id = o.business_id
        LEFT JOIN opportunity_states os ON os.opportunity_id = o.id
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
    """
    Return one full opportunity record joined with business and state.
    Returns None if not found.
    """
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
# update_opportunity_status
# ---------------------------------------------------------------------------

VALID_STATUSES = {
    "new", "saved", "contacted", "replied", "follow_up_needed",
    "in_conversation", "won", "lost", "do_not_contact",
}

def update_opportunity_status(opportunity_id: int, updates: dict) -> dict | None:
    """
    Update mutable fields on the opportunity_state for a given opportunity_id.
    Creates the state row if it does not exist yet.
    Returns the updated state as a dict, or None on error.
    """
    allowed = {
        "status", "contact_method", "last_action", "next_action",
        "next_action_date", "notes", "message_used", "response_status", "outcome",
    }
    filtered = {k: v for k, v in updates.items() if k in allowed}
    if not filtered:
        return None

    # Validate status value if provided
    if "status" in filtered and filtered["status"] not in VALID_STATUSES:
        raise ValueError(f"Invalid status: {filtered['status']!r}. Must be one of {sorted(VALID_STATUSES)}")

    try:
        with get_db_session() as session:
            # Check whether a state row exists
            check = text("SELECT id FROM opportunity_states WHERE opportunity_id = :oid LIMIT 1")
            existing = session.execute(check, {"oid": opportunity_id}).fetchone()

            if not existing:
                # Create initial row
                ins = text("""
                    INSERT INTO opportunity_states (opportunity_id, status)
                    VALUES (:oid, 'new')
                """)
                session.execute(ins, {"oid": opportunity_id})
                session.flush()

            # Build UPDATE
            set_clause = ", ".join(f"{k} = :{k}" for k in filtered)
            upd = text(f"""
                UPDATE opportunity_states
                SET {set_clause}, updated_at = NOW()
                WHERE opportunity_id = :opportunity_id
            """)
            filtered["opportunity_id"] = opportunity_id
            session.execute(upd, filtered)

            # Return the updated row
            fetch = text("""
                SELECT id, opportunity_id, status, contact_method, last_action,
                       next_action, next_action_date, notes, message_used,
                       response_status, outcome, created_at, updated_at
                FROM opportunity_states
                WHERE opportunity_id = :opportunity_id
                LIMIT 1
            """)
            row = session.execute(fetch, {"opportunity_id": opportunity_id}).mappings().fetchone()
            return dict(row) if row else None

    except Exception as exc:
        print(f"[DB] update_opportunity_status error: {exc}", flush=True)
        raise

