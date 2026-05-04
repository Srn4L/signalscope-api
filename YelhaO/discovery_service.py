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


# ---------------------------------------------------------------------------
# expand_adjacent_categories
# ---------------------------------------------------------------------------

_ADJACENT_MAP: dict[str, list[str]] = {
    "restaurant":        ["cafe", "dessert shop", "bakery", "bar", "lounge", "food truck", "bistro"],
    "cafe":              ["restaurant", "bakery", "dessert shop", "coffee shop"],
    "nail salon":        ["beauty salon", "spa", "hair salon", "brow & lash salon", "wax salon"],
    "beauty salon":      ["nail salon", "hair salon", "spa", "brow salon"],
    "hair salon":        ["beauty salon", "barbershop", "nail salon", "brow salon"],
    "barbershop":        ["hair salon", "barber"],
    "spa":               ["nail salon", "beauty salon", "med spa", "massage studio", "wellness center"],
    "med spa":           ["spa", "skin clinic", "aesthetic clinic", "wellness center"],
    "gym":               ["fitness studio", "yoga studio", "pilates studio", "boxing gym", "crossfit"],
    "fitness studio":    ["gym", "yoga studio", "pilates studio", "dance studio"],
    "yoga studio":       ["fitness studio", "pilates studio", "gym", "wellness center"],
    "bar":               ["lounge", "restaurant", "event venue", "nightclub"],
    "lounge":            ["bar", "restaurant", "event venue"],
    "cleaning service":  ["maid service", "home service", "janitorial service", "pressure washing"],
    "photographer":      ["event venue", "hair salon", "med spa", "fitness studio"],
    "event venue":       ["bar", "restaurant", "lounge", "photography studio"],
    "dentist":           ["orthodontist", "oral surgeon", "dental clinic"],
    "real estate":       ["property management", "mortgage broker", "home inspector"],
    "law firm":          ["legal services", "attorney", "notary"],
    "bakery":            ["cafe", "dessert shop", "restaurant"],
    "dog grooming":      ["pet store", "veterinarian", "dog daycare"],
    "tutoring":          ["learning center", "test prep", "educational services"],
    "tax":               ["accounting", "bookkeeping", "financial services"],
    "chiropractor":      ["physical therapist", "massage therapist", "wellness center"],
    "car wash":          ["auto detailing", "auto repair", "mechanic"],
    "laundromat":        ["dry cleaner", "alterations", "cleaning service"],
}


def expand_adjacent_categories(business_category: str) -> list[str]:
    """
    Return adjacent business categories for a given category.

    Used during refresh to discover businesses in related niches without
    drifting too far from the user's intent.

    Args:
        business_category: The primary category string (e.g. "nail salon").

    Returns:
        List of adjacent category strings. Empty list if no mapping found.
    """
    if not business_category:
        return []

    cat_lower = business_category.lower().strip()

    # Exact match
    if cat_lower in _ADJACENT_MAP:
        return _ADJACENT_MAP[cat_lower]

    # Partial match (e.g. "Brooklyn nail salon" → "nail salon")
    for key, adjacents in _ADJACENT_MAP.items():
        if key in cat_lower or cat_lower in key:
            return adjacents

    # Generic fallback — return common local service adjacents
    return ["local service", "small business"]


# ---------------------------------------------------------------------------
# choose_refresh_source_strategy
# ---------------------------------------------------------------------------

_CURSOR_ROTATION = [
    "google_deeper",
    "help_needed",
    "social_indexed",
    "booking_platforms",
    "adjacent_categories",
    "mixed_low_saturation",
]

_STRATEGY_DESCRIPTIONS = {
    "google_deeper":          "Deeper Google search with alternate keyword variations and page 2 results.",
    "help_needed":            "Prioritizing businesses with the weakest digital infrastructure signals.",
    "social_indexed":         "Discovering businesses indexed through social platforms — Instagram, TikTok, Facebook, Yelp.",
    "booking_platforms":      "Finding businesses listed on Booksy, Fresha, Vagaro, Yelp, and StyleSeat.",
    "adjacent_categories":    "Expanding to adjacent niches that match your service angle.",
    "mixed_low_saturation":   "Mixed sources with strongest suppression of previously shown businesses.",
}


def choose_refresh_source_strategy(
    refresh_cursor: str | None,
    input_intelligence: dict | None = None,
    prior_results: list | None = None,
) -> dict:
    """
    Choose the next source strategy for a Scout refresh.

    Rotates through discovery strategies in a deterministic order based on
    the current refresh_cursor. Each call returns the strategy to use and
    the next cursor for the subsequent refresh.

    Args:
        refresh_cursor:      Current cursor string (e.g. "refresh_1", "refresh_3").
                             None or missing = initial search (no refresh needed).
        input_intelligence:  Parsed intent context (used to adjust strategy).
        prior_results:       Previously shown results (used for context logging).

    Returns:
        {
            "strategy":     str,   # one of _CURSOR_ROTATION values
            "next_cursor":  str,   # cursor to pass on next refresh call
            "description":  str,   # human-readable explanation
        }
    """
    if not refresh_cursor:
        # Initial non-refresh search — caller decides source
        return {
            "strategy":    "google_standard",
            "next_cursor": "refresh_1",
            "description": "Standard Google Places discovery.",
        }

    # Parse cursor index (refresh_1 → 0, refresh_2 → 1, …)
    try:
        idx = int(refresh_cursor.replace("refresh_", "")) - 1
    except (ValueError, AttributeError):
        idx = 0

    # Wrap around after exhausting the rotation
    strategy = _CURSOR_ROTATION[idx % len(_CURSOR_ROTATION)]
    next_idx = idx + 1
    next_cursor = f"refresh_{next_idx + 1}"

    # Adjust strategy based on input_intelligence if available
    if input_intelligence:
        problem_signal = (input_intelligence.get("problem_signal") or "").lower()
        service_angle  = (input_intelligence.get("service_angle") or "").lower()

        # Social-gap problems → social_indexed earlier
        if strategy == "google_deeper" and "social" in problem_signal:
            strategy = "social_indexed"

        # Booking-gap problems → booking_platforms earlier
        if strategy == "help_needed" and "booking" in service_angle:
            strategy = "booking_platforms"

    description = _STRATEGY_DESCRIPTIONS.get(strategy, "Mixed discovery strategy.")

    prior_count = len(prior_results) if prior_results else 0
    print(
        f"[ScoutRefresh] cursor={refresh_cursor} → strategy={strategy} "
        f"next={next_cursor} prior_shown={prior_count}",
        flush=True,
    )

    return {
        "strategy":    strategy,
        "next_cursor": next_cursor,
        "description": description,
    }


# ---------------------------------------------------------------------------
# discover_social_indexed_businesses
# ---------------------------------------------------------------------------

def discover_social_indexed_businesses(
    query_context: dict,
    search_fn=None,
    limit: int = 10,
) -> list[dict]:
    """
    Discover businesses indexed through social platforms via search queries.

    Uses search-indexed discovery (site:instagram.com, site:tiktok.com, etc.)
    rather than direct platform scraping. Safe and lightweight.

    Does NOT aggressively scrape Instagram, TikTok, or Facebook.

    Args:
        query_context:  Dict with keys: niche, location, mode, service_angle.
        search_fn:      Callable(query, max_results) → list[{title, url, body}].
                        Pass the app's search_web function.
        limit:          Max candidates to return.

    Returns:
        List of candidate dicts shaped like existing Scout cards.
    """
    if not query_context or not search_fn:
        return []

    niche    = (query_context.get("niche") or query_context.get("business_category") or "").strip()
    location = (query_context.get("location") or "").strip()
    mode     = (query_context.get("mode") or "outreach").lower()

    if not niche or not location:
        return []

    _SOCIAL_PLATFORMS = [
        ("instagram", "instagram.com"),
        ("facebook",  "facebook.com"),
        ("yelp",      "yelp.com/biz"),
    ]

    candidates: list[dict] = []
    seen_names: set[str]   = set()

    queries = [
        f'site:{domain} "{niche}" "{location}"'
        for _, domain in _SOCIAL_PLATFORMS
    ] + [
        f'site:yelp.com/biz "{niche}" "{location}"',
    ]

    import re

    for query in queries:
        try:
            results = search_fn(query, 4)
        except Exception:
            results = []

        for r in results:
            title = (r.get("title") or "").split("|")[0].split("–")[0].split("-")[0].strip()
            url   = r.get("url") or ""
            body  = r.get("body") or ""

            if not title or len(title) < 3:
                continue

            # Skip obviously bad results
            bad = ["near me", "top 10", "top 20", "best ", "directory",
                   "search results", "list of", "guide to", "reviews for"]
            if any(b in title.lower() for b in bad):
                continue

            # Detect platform
            platform = "social_indexed"
            for pname, pdomain in _SOCIAL_PLATFORMS:
                if pdomain in url.lower():
                    platform = pname
                    break

            name_key = re.sub(r'[^a-z0-9]', '', title.lower())[:20]
            if name_key in seen_names:
                continue
            seen_names.add(name_key)

            # Infer social and website states
            has_social  = platform in ("instagram", "tiktok", "facebook")
            website_val = "" if platform in ("instagram", "tiktok", "facebook") else url

            candidates.append({
                "business_name":       title,
                "category":            niche,
                "location":            location,
                "website":             website_val,
                "source":              "social_indexed",
                "platform":            platform,
                "opportunity_type":    mode,
                "opportunity_score":   55,   # default mid-range; will be rescored
                "score_reasons":       [{"signal": "Social-indexed discovery", "impact": "positive", "weight": "medium"}],
                "problem_detected":    "Digital conversion path not yet verified",
                "why_it_fits":         f"Found via social platform index for {niche} in {location}.",
                "discovery_reason":    "social_indexed_refresh",
                "verification_status": "inferred",
                "website_state":       "website not verified" if not website_val else "detected",
                "_social_url":         url if has_social else "",
            })

            if len(candidates) >= limit:
                break

        if len(candidates) >= limit:
            break

    print(
        f"[ScoutRefresh] social-indexed candidates found: {len(candidates)}",
        flush=True,
    )
    return candidates[:limit]


# ---------------------------------------------------------------------------
# discover_booking_indexed_businesses
# ---------------------------------------------------------------------------

_BOOKING_DOMAINS = {
    "booksy":    "booksy.com",
    "fresha":    "fresha.com",
    "vagaro":    "vagaro.com",
    "yelp":      "yelp.com",
    "styleseat": "styleseat.com",
}

_BOOKING_RELEVANT_NICHES = {
    "nail salon", "hair salon", "barbershop", "spa", "med spa",
    "massage", "fitness", "wellness", "gym", "yoga", "beauty salon",
    "brow", "lash", "wax", "skin care", "skincare", "barber",
}


def discover_booking_indexed_businesses(
    query_context: dict,
    search_fn=None,
    limit: int = 10,
) -> list[dict]:
    """
    Discover businesses listed on booking platforms via search-indexed queries.

    Covers Booksy, Fresha, Vagaro, Yelp, and StyleSeat.
    Especially useful for beauty, wellness, and fitness niches.

    Args:
        query_context:  Dict with keys: niche, location, mode.
        search_fn:      Callable(query, max_results) → list[{title, url, body}].
        limit:          Max candidates to return.

    Returns:
        List of candidate dicts shaped like existing Scout cards.
    """
    if not query_context or not search_fn:
        return []

    niche    = (query_context.get("niche") or query_context.get("business_category") or "").strip()
    location = (query_context.get("location") or "").strip()
    mode     = (query_context.get("mode") or "outreach").lower()

    if not niche or not location:
        return []

    candidates: list[dict] = []
    seen_names: set[str]   = set()

    import re

    for platform_name, domain in _BOOKING_DOMAINS.items():
        queries = [
            f'site:{domain} "{niche}" "{location}"',
            f'site:{domain} {niche} {location}',
        ]

        for query in queries:
            try:
                results = search_fn(query, 3)
            except Exception:
                results = []

            for r in results:
                url   = r.get("url") or ""
                if domain not in url.lower():
                    continue

                title = (r.get("title") or "").split("|")[0].split("–")[0].split("-")[0].strip()
                body  = r.get("body") or ""

                # Skip list/directory pages
                bad = ["near me", "top 10", "top 20", "best ", "directory",
                       "search results", "list of", "guide", "reviews for"]
                if any(b in title.lower() for b in bad):
                    continue
                bad_url = ["/search", "near-me", "top-", "/s/"]
                if any(b in url.lower() for b in bad_url):
                    continue

                if not title or len(title) < 3:
                    continue

                name_key = re.sub(r'[^a-z0-9]', '', title.lower())[:20]
                if name_key in seen_names:
                    continue
                seen_names.add(name_key)

                candidates.append({
                    "business_name":       title,
                    "category":            niche,
                    "location":            location,
                    "website":             url,
                    "source":              "booking_indexed",
                    "platform":            platform_name,
                    "opportunity_type":    mode,
                    "opportunity_score":   58,
                    "score_reasons":       [{"signal": f"Listed on {platform_name.title()}", "impact": "positive", "weight": "medium"}],
                    "problem_detected":    "Booking platform presence detected; website conversion path unclear",
                    "why_it_fits":         f"Found on {platform_name.title()} for {niche} in {location}.",
                    "discovery_reason":    "booking_indexed_refresh",
                    "verification_status": "detected",
                })

                if len(candidates) >= limit:
                    break

            if len(candidates) >= limit:
                break

        if len(candidates) >= limit:
            break

    print(
        f"[ScoutRefresh] booking-indexed candidates found: {len(candidates)}",
        flush=True,
    )
    return candidates[:limit]
