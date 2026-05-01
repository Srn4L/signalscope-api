"""
discovery_service.py - Multi-source discovery helpers for Yelhao.

PHASE 7 STUBS - safe to import, not yet wired into /prospect.
These are tested helpers for the next discovery expansion pass.

Wire-in instructions (for next pass):
    In app.py /prospect route, after the existing candidates are scored:
        from discovery_service import merge_and_dedupe_candidates, classify_discovery_source
        candidates = merge_and_dedupe_candidates(candidates)
        candidates = [classify_discovery_source(c) for c in candidates]

Do NOT modify /prospect routing logic in this file.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# discover_social_first_candidates
# ---------------------------------------------------------------------------

def discover_social_first_candidates(
    niche:    str,
    location: str,
    context:  dict | None = None,
    limit:    int = 20,
) -> list[dict]:
    """
    Stub: Discover businesses that are social-first (active on Instagram/TikTok
    but potentially lacking a web booking or conversion path).

    Currently returns an empty list. The real implementation would:
    1. Query a social API or database of known social profiles.
    2. Filter by niche and location.
    3. Score by social engagement vs conversion infrastructure gap.

    Wire this into /prospect after Google Places candidates to extend coverage
    to social-only businesses that don't appear in places APIs.

    Args:
        niche:    Business category/type to search for.
        location: City, area, or region.
        context:  Request context (user_role, mode, signal_preferences).
        limit:    Max candidates to return.

    Returns:
        list[dict]: Each dict uses the same schema as Google Places results.
    """
    # TODO: implement with Instagram Graph API, Apify social scraper,
    # or a local index of known social profiles by niche + location.
    print(
        f"[Discovery] discover_social_first_candidates called: "
        f"niche={niche!r}, location={location!r}, limit={limit} (stub - returning empty)",
        flush=True,
    )
    return []


# ---------------------------------------------------------------------------
# merge_and_dedupe_candidates
# ---------------------------------------------------------------------------

def merge_and_dedupe_candidates(candidates: list[dict]) -> list[dict]:
    """
    Merge candidates from multiple sources and remove duplicates.

    Deduplication strategy:
    1. Primary: google_place_id (exact match).
    2. Secondary: normalised business name + location string (fuzzy match stub).
    3. When duplicate found, keep the record with more data fields populated.

    Args:
        candidates: Combined list of business dicts from any discovery source.

    Returns:
        Deduplicated list, preserving the richest record per business.
    """
    if not candidates:
        return []

    seen_place_ids: dict[str, int] = {}   # place_id -> index in result
    seen_name_loc:  dict[str, int] = {}   # "name|location" -> index in result
    result: list[dict] = []

    def _richness(c: dict) -> int:
        """Count non-empty fields - higher = richer record."""
        return sum(1 for v in c.values() if v is not None and v != "" and v != [] and v != {})

    def _name_loc_key(c: dict) -> str:
        name = (c.get("business_name") or c.get("name") or "").lower().strip()
        loc  = (c.get("location") or "").lower().strip()
        return f"{name}|{loc}"

    for c in candidates:
        place_id = (c.get("google_place_id") or c.get("place_id") or "").strip()
        nk       = _name_loc_key(c)

        # Check place_id dedup
        if place_id and place_id in seen_place_ids:
            idx = seen_place_ids[place_id]
            if _richness(c) > _richness(result[idx]):
                # Prefer richer record
                result[idx] = {**result[idx], **{k: v for k, v in c.items() if v}}
            continue

        # Check name+location dedup
        if nk in seen_name_loc:
            idx = seen_name_loc[nk]
            if _richness(c) > _richness(result[idx]):
                result[idx] = {**result[idx], **{k: v for k, v in c.items() if v}}
            # Update place_id index if this record has one
            if place_id:
                seen_place_ids[place_id] = idx
            continue

        # New candidate
        idx = len(result)
        result.append(c)
        if place_id:
            seen_place_ids[place_id] = idx
        if nk:
            seen_name_loc[nk] = idx

    return result


# ---------------------------------------------------------------------------
# classify_discovery_source
# ---------------------------------------------------------------------------

def classify_discovery_source(candidate: dict) -> dict:
    """
    Tag a candidate with a source classification for downstream scoring.

    Classification logic:
    - google_places:   has google_place_id
    - social_first:    has instagram/tiktok but no website or google_place_id
    - booking_platform: has booking_url or detected via booksy/fresha/vagaro
    - web_only:        has website but no social and no place_id
    - unknown:         none of the above

    Adds:
        candidate["_discovery_source_class"]: str
        candidate["_discovery_source_confidence"]: "high"|"medium"|"low"

    Returns the candidate dict (mutated in place for efficiency, also returned).
    """
    if not candidate:
        return candidate

    has_place_id = bool(
        (candidate.get("google_place_id") or candidate.get("place_id") or "").strip()
    )
    has_social   = any(
        bool(candidate.get(k))
        for k in ("instagram_url", "instagram", "tiktok", "facebook_url", "facebook")
    )
    has_website  = bool((candidate.get("website") or "").strip())
    has_booking  = any(
        kw in (candidate.get("website") or "").lower()
        for kw in ("booksy", "fresha", "vagaro", "mindbody", "calendly")
    ) or bool((candidate.get("raw_data") or {}).get("booking_url"))

    if has_place_id:
        source_class = "google_places"
        confidence   = "high"
    elif has_booking:
        source_class = "booking_platform"
        confidence   = "high"
    elif has_social and not has_website:
        source_class = "social_first"
        confidence   = "medium"
    elif has_website and not has_social and not has_place_id:
        source_class = "web_only"
        confidence   = "medium"
    else:
        source_class = "unknown"
        confidence   = "low"

    candidate["_discovery_source_class"]      = source_class
    candidate["_discovery_source_confidence"] = confidence
    return candidate
