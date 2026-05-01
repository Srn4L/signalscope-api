"""
intent_service.py — Input intelligence for Yelhao's "What are you looking for?" field.

PHASE 3 + PHASE 4.

Public API
----------
parse_user_intent(raw_query, explicit_fields) -> dict
build_niche_intelligence(query_context, business, signals) -> dict
"""

from __future__ import annotations
import re


# ─────────────────────────────────────────────────────────────────────────────
# Location normalization
# ─────────────────────────────────────────────────────────────────────────────

_LOCATION_ABBREVS: dict[str, str] = {
    "bk":    "Brooklyn, NY",
    "nyc":   "New York, NY",
    "ny":    "New York, NY",
    "la":    "Los Angeles, CA",
    "jc":    "Jersey City, NJ",
    "bx":    "Bronx, NY",
    "qns":   "Queens, NY",
    "si":    "Staten Island, NY",
    "mia":   "Miami, FL",
    "chi":   "Chicago, IL",
    "sf":    "San Francisco, CA",
    "atl":   "Atlanta, GA",
    "hou":   "Houston, TX",
    "phx":   "Phoenix, AZ",
    "lax":   "Los Angeles, CA",
    "dc":    "Washington, DC",
    "philly": "Philadelphia, PA",
    "htx":   "Houston, TX",
    "nola":  "New Orleans, LA",
}


# ─────────────────────────────────────────────────────────────────────────────
# Business category normalization
# ─────────────────────────────────────────────────────────────────────────────

_CATEGORY_MAP: list[tuple[list[str], str, list[str]]] = [
    # (trigger words, canonical category, expanded_categories)
    (["restaurant", "food spot", "food spots", "eatery", "diner", "spot to eat"],
     "restaurant", ["restaurant", "cafe", "food truck", "bistro"]),
    (["cafe", "coffee shop", "coffee"],
     "cafe", ["cafe", "coffee shop", "bakery"]),
    (["bar", "lounge", "nightclub", "club"],
     "bar", ["bar", "lounge", "nightclub"]),
    (["salon", "hair salon", "hair"],
     "hair salon", ["hair salon", "barbershop", "nail salon"]),
    (["barber", "barbershop", "barbershops"],
     "barbershop", ["barbershop", "hair salon"]),
    (["nail", "nails", "nail salon"],
     "nail salon", ["nail salon", "beauty salon"]),
    (["gym", "fitness", "crossfit", "yoga", "pilates"],
     "gym", ["gym", "fitness studio", "yoga studio", "pilates"]),
    (["spa", "med spa", "medspa", "wellness"],
     "spa", ["spa", "med spa", "wellness center"]),
    (["photographer", "photography"],
     "photography", ["photography studio", "photographer"]),
    (["boutique", "clothing", "fashion", "apparel"],
     "boutique", ["boutique", "clothing store", "fashion"]),
    (["real estate", "realtor", "property"],
     "real estate", ["real estate", "property management"]),
    (["cleaning", "maid", "janitorial"],
     "cleaning service", ["cleaning service", "maid service"]),
    (["plumber", "plumbing", "electrician", "contractor", "handyman"],
     "home service", ["plumber", "electrician", "handyman", "contractor"]),
    (["auto", "car", "mechanic", "detailing", "dealership"],
     "auto", ["auto repair", "car dealership", "auto detailing"]),
    (["dentist", "dental"],
     "dental", ["dentist", "dental clinic"]),
    (["vet", "veterinary", "pet"],
     "veterinarian", ["veterinarian", "pet groomer", "pet store"]),
    (["hotel", "motel", "inn", "bnb", "airbnb"],
     "hotel", ["hotel", "motel", "bed and breakfast"]),
    (["lawyer", "law firm", "attorney"],
     "law firm", ["law firm", "legal services"]),
    (["therapist", "therapy", "counselor", "mental health"],
     "therapy", ["therapist", "counselor", "mental health clinic"]),
    (["tutor", "tutoring", "school", "academy", "learning center"],
     "education", ["tutoring center", "school", "academy"]),
    (["local business", "businesses", "business", "local service"],
     "local business", ["restaurant", "salon", "gym", "cleaning service"]),
]


# ─────────────────────────────────────────────────────────────────────────────
# Role normalization
# ─────────────────────────────────────────────────────────────────────────────

_ROLE_MAP: list[tuple[list[str], str]] = [
    (["content creator", "creator", "ugc"], "content creator"),
    (["photographer", "photo"], "photographer"),
    (["videographer", "video"], "videographer"),
    (["web designer", "web design", "web developer", "webdev"], "web designer"),
    (["seo", "seo specialist", "search engine"], "seo specialist"),
    (["comedian", "stand-up", "standup"], "comedian"),
    (["dj"], "dj"),
    (["musician", "singer", "artist", "performer"], "performer"),
    (["marketer", "marketing", "digital marketer"], "marketer"),
    (["crm", "email marketer", "email marketing"], "crm specialist"),
    (["influencer"], "influencer"),
    (["social media manager", "smm"], "social media manager"),
    (["freelancer"], "freelancer"),
]


# ─────────────────────────────────────────────────────────────────────────────
# Problem signal normalization
# ─────────────────────────────────────────────────────────────────────────────

_PROBLEM_MAP: list[tuple[list[str], str, str]] = [
    # (trigger words, problem_signal, inferred_service_angle)
    (["bad ig", "weak ig", "low ig", "no ig", "bad instagram", "weak instagram",
      "low social", "bad social", "no social", "low followers", "weak social", "bad socials"],
     "low_social_engagement", "content_creation"),
    (["no website", "bad website", "no web", "outdated website", "website redesign"],
     "no_website", "website_redesign"),
    (["no booking", "no online booking", "no reservation", "booking gap"],
     "no_booking_path", "booking_conversion"),
    (["seo", "not ranking", "no seo", "weak seo", "invisible online"],
     "weak_seo", "seo"),
    (["few reviews", "no reviews", "low reviews", "review problem", "bad reviews"],
     "reputation_gap", "reputation_management"),
    (["no events", "needs performers", "needs dj", "needs comedian", "entertainment"],
     "no_events", "events_performance"),
    (["no email", "no crm", "retention", "follow up", "follow-up"],
     "weak_retention", "crm_follow_up"),
    (["paid ads", "google ads", "facebook ads", "instagram ads"],
     "no_paid_ads", "paid_ads"),
    (["content gap", "no content", "no photos", "no video", "bad photos", "bad visuals"],
     "content_gap", "content_creation"),
    (["needs help", "digital gap", "digital presence", "online presence"],
     "general_digital_gap", "content_creation"),
]


# ─────────────────────────────────────────────────────────────────────────────
# parse_user_intent
# ─────────────────────────────────────────────────────────────────────────────

def parse_user_intent(raw_query: str, explicit_fields: dict | None = None) -> dict:
    """
    Parse free-text intent from "What are you looking for?" field.

    Explicit form fields always override inferred values:
        business_type  → business_category
        location       → location
        mode           → service_angle / mode

    Returns:
        {
          "input_quality":         "strong|medium|weak|bad",
          "confidence":            "high|medium|low",
          "user_role":             str or None,
          "business_category":     str or None,
          "location":              str or None,
          "problem_signal":        str or None,
          "service_angle":         str or None,
          "expanded_categories":   list[str],
          "signal_preferences":    list[str],
          "source_preferences":    list[str],
          "ranking_focus":         list[str],
          "interpreted_query":     str,
          "user_feedback":         str,
          "suggested_better_queries": list[str],
        }
    """
    explicit_fields = explicit_fields or {}
    raw = (raw_query or "").strip()
    haystack = raw.lower()

    # ── Normalize abbreviations ───────────────────────────────────────────────
    for abbrev, full in _LOCATION_ABBREVS.items():
        pattern = r"\b" + re.escape(abbrev) + r"\b"
        haystack = re.sub(pattern, full.lower(), haystack)
        raw = re.sub(pattern, full, raw, flags=re.IGNORECASE)

    # ── Extract user role ─────────────────────────────────────────────────────
    user_role: str | None = explicit_fields.get("user_role")
    if not user_role:
        for triggers, role in _ROLE_MAP:
            if any(t in haystack for t in triggers):
                user_role = role
                break

    # ── Extract business category ─────────────────────────────────────────────
    business_category: str | None = (
        explicit_fields.get("business_type") or
        explicit_fields.get("business_category")
    )
    expanded_categories: list[str] = []
    if not business_category:
        for triggers, cat, expanded in _CATEGORY_MAP:
            if any(t in haystack for t in triggers):
                business_category = cat
                expanded_categories = expanded
                break

    # ── Extract location ──────────────────────────────────────────────────────
    location: str | None = explicit_fields.get("location")
    if not location:
        # Try to match "in <City, ST>" or "in <City>"
        loc_match = re.search(
            r"\bin\s+([A-Z][a-zA-Z\s]+(?:,\s*[A-Z]{2})?)",
            raw,
        )
        if loc_match:
            location = loc_match.group(1).strip()
        else:
            # Check normalized abbreviations
            for abbrev, full in _LOCATION_ABBREVS.items():
                if full.lower() in haystack:
                    location = full
                    break

    # ── Extract problem signal ────────────────────────────────────────────────
    problem_signal: str | None    = None
    service_angle:  str | None    = explicit_fields.get("mode")
    signal_preferences: list[str] = []

    for triggers, prob, angle in _PROBLEM_MAP:
        if any(t in haystack for t in triggers):
            problem_signal = prob
            if not service_angle:
                service_angle = angle
            signal_preferences.append(prob)
            break

    # ── Infer service angle from role if still missing ────────────────────────
    if not service_angle and user_role:
        _role_angle_map = {
            "content creator":     "content_creation",
            "photographer":        "photography",
            "videographer":        "short_form_video",
            "web designer":        "website_redesign",
            "seo specialist":      "seo",
            "comedian":            "events_performance",
            "dj":                  "events_performance",
            "performer":           "events_performance",
            "marketer":            "content_creation",
            "crm specialist":      "crm_follow_up",
            "influencer":          "influencer_partnership",
            "social media manager": "content_creation",
        }
        service_angle = _role_angle_map.get(user_role)

    # ── Ranking focus ─────────────────────────────────────────────────────────
    ranking_focus: list[str] = []
    if problem_signal:
        _focus_map = {
            "low_social_engagement": ["social_gap", "content_gap", "trust_gap"],
            "no_website":            ["web_gap", "conversion_gap", "intent_gap"],
            "no_booking_path":       ["booking_gap", "conversion_gap"],
            "weak_seo":              ["seo_gap", "intent_gap"],
            "reputation_gap":        ["trust_gap", "review_gap"],
            "content_gap":           ["content_gap", "attention_gap"],
            "general_digital_gap":   ["clear_gap", "contactability", "demand"],
        }
        ranking_focus = _focus_map.get(problem_signal, ["clear_gap", "contactability"])
    else:
        ranking_focus = ["clear_gap", "contactability", "demand"]

    # ── Input quality scoring ─────────────────────────────────────────────────
    signals_found = sum([
        bool(user_role),
        bool(business_category and business_category != "local business"),
        bool(location),
        bool(problem_signal),
    ])

    if signals_found >= 3:
        input_quality = "strong"
        confidence    = "high"
    elif signals_found == 2:
        input_quality = "medium"
        confidence    = "medium"
    elif signals_found == 1:
        input_quality = "weak"
        confidence    = "low"
    elif not raw or len(raw) < 3:
        input_quality = "bad"
        confidence    = "low"
    else:
        input_quality = "weak"
        confidence    = "low"

    # ── Human-readable interpretation ─────────────────────────────────────────
    parts: list[str] = []
    if business_category and expanded_categories:
        parts.append(f"{' and '.join(expanded_categories[:2])}")
    elif business_category:
        parts.append(business_category)
    if location:
        parts.append(f"in {location}")
    if problem_signal:
        parts.append(f"with {problem_signal.replace('_', ' ')}")
    if user_role:
        parts.append(f"(for a {user_role})")

    interpreted_query = " ".join(parts) if parts else raw

    # ── Feedback and suggestions ──────────────────────────────────────────────
    if input_quality == "strong":
        user_feedback = (
            f"Interpreting your search as {interpreted_query}."
        )
        suggested_better_queries: list[str] = []

    elif input_quality == "medium":
        missing: list[str] = []
        if not user_role:
            missing.append("your role (e.g. content creator, web designer)")
        if not location:
            missing.append("a location")
        if not problem_signal:
            missing.append("a specific gap you're targeting")
        user_feedback = (
            f"Found partial intent: {interpreted_query or raw}. "
            f"Add {' and '.join(missing)} for sharper results."
        )
        suggested_better_queries = _suggest_better_queries(
            user_role, business_category, location, problem_signal
        )

    elif input_quality in ("weak", "bad"):
        user_feedback = (
            "This is broad, so Yelhao will look for local businesses with "
            "visible digital gaps. Add a location and your role for sharper results."
        )
        suggested_better_queries = _suggest_better_queries(
            user_role, business_category, location, problem_signal
        )

    else:
        user_feedback = f"Searching for: {raw}"
        suggested_better_queries = []

    print(
        f"[Intent] parsed: quality={input_quality}, "
        f"category={business_category}, location={location}, "
        f"angle={service_angle}",
        flush=True,
    )

    return {
        "input_quality":         input_quality,
        "confidence":            confidence,
        "user_role":             user_role,
        "business_category":     business_category,
        "location":              location,
        "problem_signal":        problem_signal,
        "service_angle":         service_angle,
        "expanded_categories":   expanded_categories,
        "signal_preferences":    signal_preferences,
        "source_preferences":    [],  # extensible for future source routing
        "ranking_focus":         ranking_focus,
        "interpreted_query":     interpreted_query,
        "user_feedback":         user_feedback,
        "suggested_better_queries": suggested_better_queries,
    }


def _suggest_better_queries(
    user_role: str | None,
    category:  str | None,
    location:  str | None,
    problem:   str | None,
) -> list[str]:
    """Generate 2–3 example queries that would produce sharper results."""
    defaults = [
        "I'm a content creator looking for restaurants in Brooklyn with weak Instagram.",
        "I'm a web designer looking for salons in Queens with no booking path.",
        "I'm a photographer looking for gyms in Jersey City with weak visuals.",
    ]
    custom: list[str] = []

    role_ex  = user_role   or "content creator"
    cat_ex   = category    or "restaurants"
    loc_ex   = location    or "Brooklyn, NY"
    prob_ex  = problem     or "weak Instagram"

    prob_str = prob_ex.replace("_", " ")

    custom.append(
        f"I'm a {role_ex} looking for {cat_ex} in {loc_ex} with {prob_str}."
    )
    if not location:
        custom.append(
            f"I'm a {role_ex} looking for {cat_ex} in [your city] with {prob_str}."
        )
    if not user_role:
        custom.append(
            f"I'm a [your role] looking for {cat_ex} in {loc_ex} with no booking path."
        )

    return (custom + defaults)[:3]


# ─────────────────────────────────────────────────────────────────────────────
# build_niche_intelligence
# ─────────────────────────────────────────────────────────────────────────────

def build_niche_intelligence(
    query_context: dict,
    business:      dict | None = None,
    signals:       dict | None = None,
) -> dict:
    """
    Build a structured niche definition from context + business + signals.

    Formula: user_role + business_category + problem_signal + service_angle

    Niche is NOT just business category. It's the specific opportunity.

    Examples:
        content_creator + restaurant + low_social_engagement + short_form_content
        web_designer + nail_salon + no_booking_path + booking_conversion
        comedian + bar + no_events + events_performance

    Returns:
        {
          "user_role":           str or None,
          "business_category":   str or None,
          "problem_signal":      str or None,
          "service_angle":       str or None,
          "niche_key":           str,  # snake_case composite
          "plain_english":       str,
          "saturation_key_parts": {...},
        }
    """
    query_context = query_context or {}
    business      = business      or {}
    signals       = signals       or {}

    user_role = (
        query_context.get("user_role") or
        signals.get("contact_target") or
        None
    )

    business_category = (
        query_context.get("business_category") or
        query_context.get("niche") or
        business.get("category") or
        business.get("industry") or
        None
    )

    # Infer problem signal from signals if not in context
    problem_signal = (
        query_context.get("problem_signal") or
        _infer_problem_from_signals(signals, business) or
        None
    )

    service_angle = (
        query_context.get("service_angle") or
        signals.get("service_angle") or
        signals.get("primary_angle") or
        None
    )

    # If still missing service angle, infer from role and problem
    if not service_angle:
        service_angle = _infer_angle(user_role, problem_signal, business_category)

    # Build niche key — lowercase snake_case composite
    _parts = [
        _snake(user_role        or ""),
        _snake(business_category or ""),
        _snake(problem_signal    or ""),
        _snake(service_angle     or ""),
    ]
    niche_key = "__".join(p for p in _parts if p) or "general_local_opportunity"

    # Plain English
    plain_english = _build_plain_english(user_role, business_category, problem_signal, service_angle)

    print(f"[Niche] key={niche_key}", flush=True)

    return {
        "user_role":         user_role,
        "business_category": business_category,
        "problem_signal":    problem_signal,
        "service_angle":     service_angle,
        "niche_key":         niche_key,
        "plain_english":     plain_english,
        "saturation_key_parts": {
            "business_id":   business.get("id"),
            "user_role":     user_role,
            "problem_signal": problem_signal,
            "service_angle":  service_angle,
            "contact_path":  None,  # filled by caller if needed
        },
    }


def _infer_problem_from_signals(signals: dict, business: dict) -> str | None:
    """Infer primary problem signal from opportunity signals dict."""
    sc = signals.get("social_conversion") or {}

    scores: dict[str, int] = {}
    for k in ("attention_signal", "intent_signal", "trust_signal",
              "funnel_clarity_signal", "conversion_path_signal"):
        scores[k] = (sc.get(k) or {}).get("score", 50)

    if not scores:
        return None

    lowest_key = min(scores, key=scores.get)
    _map = {
        "attention_signal":        "low_social_engagement",
        "intent_signal":           "no_website",
        "trust_signal":            "reputation_gap",
        "funnel_clarity_signal":   "no_booking_path",
        "conversion_path_signal":  "no_booking_path",
    }
    return _map.get(lowest_key)


def _infer_angle(user_role: str | None, problem: str | None, category: str | None) -> str | None:
    """Infer service angle from available fields."""
    _role_map = {
        "content creator":     "content_creation",
        "photographer":        "photography",
        "videographer":        "short_form_video",
        "web designer":        "website_redesign",
        "seo specialist":      "seo",
        "comedian":            "events_performance",
        "dj":                  "events_performance",
        "performer":           "events_performance",
        "marketer":            "content_creation",
        "crm specialist":      "crm_follow_up",
        "influencer":          "influencer_partnership",
        "social media manager": "content_creation",
    }
    _problem_map = {
        "low_social_engagement": "content_creation",
        "no_website":            "website_redesign",
        "no_booking_path":       "booking_conversion",
        "weak_seo":              "seo",
        "reputation_gap":        "reputation_management",
        "no_events":             "events_performance",
        "content_gap":           "content_creation",
    }
    return (
        _role_map.get(user_role or "")  or
        _problem_map.get(problem or "")
    )


def _snake(text: str) -> str:
    """Convert to lowercase snake_case, strip punctuation."""
    if not text:
        return ""
    return re.sub(r"[^a-z0-9_]", "_", text.lower().strip()).strip("_")


def _build_plain_english(
    user_role: str | None,
    category:  str | None,
    problem:   str | None,
    angle:     str | None,
) -> str:
    role_str    = user_role or "a service provider"
    cat_str     = category  or "local businesses"
    prob_str    = (problem  or "a digital gap").replace("_", " ")
    angle_str   = (angle    or "general outreach").replace("_", " ")
    return (
        f"{role_str.title()} targeting {cat_str} with {prob_str}, "
        f"using a {angle_str} approach."
    )

import datetime as _dt


# Re-use _clamp from signal_service if this file is appended to it.
# If tested standalone, define a local fallback:
try:
    _clamp  # already defined if appended to signal_service.py
except NameError:
    def _clamp(val: float, lo: float = 0.0, hi: float = 100.0) -> int:
        return int(max(lo, min(hi, val)))


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1 — classify_fetch_result
# ─────────────────────────────────────────────────────────────────────────────

def classify_fetch_result(
    status_code=None,
    error_message: str | None = None,
    url: str | None = None,
) -> dict:
    """
    Classify a fetch attempt into source_status / asset_state.

    Rule: blocked != missing.
    A 403/401/429/timeout result reduces confidence, NOT opportunity score.

    Returns:
        {
          "source_status":     "ok|blocked|not_found|timeout|error|unknown",
          "asset_state":       "found|missing|unknown|unverified",
          "confidence_impact": "none|low|medium|high",
          "plain_english":     str,
        }
    """
    err = (error_message or "").lower()

    if status_code == 200:
        return {
            "source_status":     "ok",
            "asset_state":       "found",
            "confidence_impact": "none",
            "plain_english":     "Source fetched successfully.",
        }
    if status_code == 404:
        return {
            "source_status":     "not_found",
            "asset_state":       "missing",
            "confidence_impact": "medium",
            "plain_english":     "The page or resource was not found (404).",
        }
    if status_code in (401, 403):
        return {
            "source_status":     "blocked",
            "asset_state":       "unknown",
            "confidence_impact": "low",
            "plain_english":     "This source could not be verified because the platform blocked direct access.",
        }
    if status_code == 429:
        return {
            "source_status":     "blocked",
            "asset_state":       "unknown",
            "confidence_impact": "low",
            "plain_english":     "This source could not be verified because the platform rate-limited direct access.",
        }
    if status_code and status_code >= 500:
        return {
            "source_status":     "error",
            "asset_state":       "unknown",
            "confidence_impact": "medium",
            "plain_english":     "The server returned an error. The asset may exist but could not be confirmed.",
        }
    if any(w in err for w in ("timeout", "timed out", "read timeout", "connect timeout")):
        return {
            "source_status":     "timeout",
            "asset_state":       "unknown",
            "confidence_impact": "medium",
            "plain_english":     "The request timed out. The asset may exist but could not be confirmed.",
        }
    if any(w in err for w in ("captcha", "forbidden", "access denied", "blocked", "403", "401")):
        return {
            "source_status":     "blocked",
            "asset_state":       "unknown",
            "confidence_impact": "low",
            "plain_english":     "This source could not be verified because the platform blocked direct access.",
        }
    if error_message:
        return {
            "source_status":     "error",
            "asset_state":       "unknown",
            "confidence_impact": "medium",
            "plain_english":     f"Fetch failed: {str(error_message)[:100]}",
        }
    return {
        "source_status":     "unknown",
        "asset_state":       "unknown",
        "confidence_impact": "medium",
        "plain_english":     "Could not determine fetch result.",
    }


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 2 — build_discovery_verification_signals
# ─────────────────────────────────────────────────────────────────────────────

def build_discovery_verification_signals(
    scout_data:   dict,
    analyze_data: dict,
    business:     dict | None = None,
) -> dict:
    """
    Compare Scout (first-pass) vs Analyze (deep-pass) findings.

    Decision matrix per asset:
      Scout found  + Analyze blocked → found (unverified)
      Scout found  + Analyze found   → found (confirmed)
      Scout miss   + Analyze blocked → unknown / unverified
      Scout miss   + Analyze found   → discovery_gap  ← intelligence opportunity
      Scout miss   + Analyze miss    → true_missing_asset
    """
    scout_data   = scout_data   or {}
    analyze_data = analyze_data or {}
    business     = business     or {}

    biz_label = (
        business.get("name") or business.get("business_name") or
        scout_data.get("business_name") or "unknown"
    )

    mismatches:      list[dict] = []
    verified_assets: dict[str, str] = {}

    # ── Helper: is an asset "blocked" according to analyze_data? ────────────
    def _blocked(key: str) -> bool:
        blocked_list = analyze_data.get("_blocked_sources") or []
        if isinstance(blocked_list, list):
            return any(key in str(b) for b in blocked_list)
        return False

    # ── 1. Website ────────────────────────────────────────────────────────────
    scout_website   = bool(scout_data.get("website") or scout_data.get("website_url"))
    analyze_website = bool(
        analyze_data.get("website") or analyze_data.get("website_url") or
        analyze_data.get("website_found") or
        (analyze_data.get("pages_scraped") or 0) > 0
    )
    website_blocked = _blocked("website")

    if not scout_website and analyze_website:
        mismatches.append({
            "type":             "website_discovery_gap",
            "scout_claim":      "no_website_detected",
            "analyze_finding":  "website_found",
            "severity":         "medium",
            "plain_english":    (
                "The business appears to have a website, but it was not surfaced "
                "clearly in the initial local discovery source."
            ),
            "business_meaning": (
                "Customers relying on Google or local search may have a harder "
                "time reaching the official website."
            ),
            "opportunity_angle": "local_search_cleanup",
        })
        verified_assets["website"] = "found"
        print(f"[Verify] {biz_label}: website_discovery_gap detected", flush=True)
    elif not scout_website and website_blocked:
        verified_assets["website"] = "unverified"
    elif scout_website:
        verified_assets["website"] = "found"
    else:
        verified_assets["website"] = "missing"

    # ── 2. Social ─────────────────────────────────────────────────────────────
    scout_social = bool(
        scout_data.get("instagram_url") or scout_data.get("instagram") or
        scout_data.get("facebook_url")  or scout_data.get("facebook") or
        scout_data.get("tiktok")
    )
    analyze_social = bool(
        analyze_data.get("instagram")     or analyze_data.get("instagram_url") or
        analyze_data.get("tiktok")        or analyze_data.get("facebook_url") or
        analyze_data.get("facebook")      or analyze_data.get("linkedin")
    )
    social_blocked = _blocked("instagram") or _blocked("social")

    if not scout_social and analyze_social:
        mismatches.append({
            "type":             "social_discovery_gap",
            "scout_claim":      "no_social_detected",
            "analyze_finding":  "social_found",
            "severity":         "medium",
            "plain_english":    (
                "The business has a social presence, but it was not connected "
                "to their local listing or Google profile."
            ),
            "business_meaning": (
                "Customers who find the business on Google may not find their "
                "Instagram or Facebook easily."
            ),
            "opportunity_angle": "social_link_cleanup",
        })
        verified_assets["social"] = "found"
        print(f"[Verify] {biz_label}: social_discovery_gap detected", flush=True)
    elif not scout_social and social_blocked:
        verified_assets["social"] = "unverified"
    elif scout_social:
        verified_assets["social"] = "found"
    else:
        verified_assets["social"] = "missing"

    # ── 3. Booking ────────────────────────────────────────────────────────────
    _booking_kws = ("booksy", "fresha", "vagaro", "mindbody", "calendly", "square")
    scout_booking = bool(
        scout_data.get("booking_url") or
        any(kw in (scout_data.get("website") or "").lower() for kw in _booking_kws)
    )
    analyze_booking = bool(
        analyze_data.get("booking_url") or analyze_data.get("booking_platform") or
        analyze_data.get("has_booking") or
        any(kw in (analyze_data.get("website") or "").lower() for kw in _booking_kws)
    )
    booking_blocked = _blocked("booking") or _blocked("booksy") or _blocked("fresha")

    if not scout_booking and analyze_booking:
        mismatches.append({
            "type":             "booking_discovery_gap",
            "scout_claim":      "no_booking_detected",
            "analyze_finding":  "booking_found",
            "severity":         "low",
            "plain_english":    (
                "A booking platform was found during deep analysis but not "
                "surfaced in the initial scan."
            ),
            "business_meaning": (
                "The business may have some booking infrastructure that "
                "wasn't immediately visible in local search."
            ),
            "opportunity_angle": "booking_optimization",
        })
        verified_assets["booking"] = "found"
        print(f"[Verify] {biz_label}: booking_discovery_gap detected", flush=True)
    elif not scout_booking and booking_blocked:
        verified_assets["booking"] = "unverified"
    elif scout_booking:
        verified_assets["booking"] = "found"
    else:
        verified_assets["booking"] = "missing"

    # ── 4. Contact ────────────────────────────────────────────────────────────
    scout_contact   = bool(scout_data.get("phone") or scout_data.get("email"))
    analyze_contact = bool(
        analyze_data.get("phone") or analyze_data.get("email") or
        analyze_data.get("contact_email")
    )
    contact_blocked = _blocked("phone") or _blocked("email")

    if not scout_contact and analyze_contact:
        mismatches.append({
            "type":             "contact_discovery_gap",
            "scout_claim":      "no_contact_detected",
            "analyze_finding":  "contact_found",
            "severity":         "low",
            "plain_english":    (
                "Contact information was found during deep analysis but was "
                "not visible in the initial local listing."
            ),
            "business_meaning": (
                "The business may be reachable, but their contact info isn't "
                "prominently displayed where customers look first."
            ),
            "opportunity_angle": "contact_clarity",
        })
        verified_assets["phone"] = "found"
    elif not scout_contact and contact_blocked:
        verified_assets["phone"] = "unverified"
    elif scout_contact:
        verified_assets["phone"] = "found"
    else:
        verified_assets["phone"] = "missing"

    # ── 5. True missing assets ────────────────────────────────────────────────
    # Only flag as truly missing if both Scout AND Analyze agree it's absent
    # and there's no block evidence (blocked = unknown, not missing).
    for asset in ("website", "social", "booking", "phone"):
        if verified_assets.get(asset) == "missing":
            mismatches.append({
                "type":             "true_missing_asset",
                "scout_claim":      f"no_{asset}_detected",
                "analyze_finding":  f"no_{asset}_confirmed",
                "severity":         "high",
                "plain_english":    (
                    f"No {asset} was found in either the initial scan or deep analysis."
                ),
                "business_meaning": (
                    f"This appears to be a genuine gap — the business likely has no active {asset}."
                ),
                "opportunity_angle": f"{asset}_creation",
            })

    has_mismatch = any(
        m["type"] != "true_missing_asset" for m in mismatches
    )

    gap_types = [m["type"] for m in mismatches if "discovery_gap" in m["type"]]
    miss_types = [m["type"] for m in mismatches if m["type"] == "true_missing_asset"]

    if gap_types:
        pattern_summary = (
            f"Scout vs Analyze mismatch on: {', '.join(gap_types)}. "
            "These are intelligence opportunities, not data errors."
        )
        recommended_action = (
            "Use the discovery gaps as conversation starters — the business likely has "
            "digital assets that aren't well-connected or surfaced in local search."
        )
    elif miss_types:
        pattern_summary = f"Confirmed missing: {', '.join(m['type'].replace('true_missing_asset','') for m in mismatches if m['type']=='true_missing_asset')}. Both sources agree."
        recommended_action = "Focus outreach on the confirmed missing assets — these are the highest-value gaps."
    else:
        pattern_summary = "Assets appear consistent between Scout and Analyze."
        recommended_action = "No significant discovery gaps found. Proceed with standard outreach."

    return {
        "has_mismatch":       has_mismatch,
        "mismatches":         mismatches,
        "verified_assets":    verified_assets,
        "pattern_summary":    pattern_summary,
        "recommended_action": recommended_action,
    }


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 5 — apply_saturation_rerank
# ─────────────────────────────────────────────────────────────────────────────

def apply_saturation_rerank(
    candidates:     list[dict],
    exposure_stats: dict,
    context:        dict | None = None,
) -> list[dict]:
    """
    Rerank candidates using token-scoped and global exposure history.

    Soft penalties applied for seen/saved/contacted candidates.
    Diversity boosts applied for novel sources and gap types.
    Minimum floor: 35 unless already saved/contacted.

    Returns candidates with added keys:
        original_rank, reranked_score, rerank_reasons, exposure
    """
    if not candidates:
        return []

    exposure_stats = exposure_stats or {}
    context        = context        or {}

    now = _dt.datetime.now(_dt.timezone.utc)

    def _key(c: dict) -> str:
        import re as _re
        name = _re.sub(r"[^a-z0-9]", "", (c.get("business_name") or c.get("name") or "").lower())[:20]
        loc  = (c.get("location") or "").lower().strip()
        return f"{name}|{loc}"

    def _parse_ts(ts) -> _dt.datetime | None:
        if not ts:
            return None
        try:
            if isinstance(ts, str):
                return _dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return ts
        except Exception:
            return None

    seen_sources:    set[str] = set()
    seen_gap_types:  set[str] = set()
    penalized_count: int      = 0

    annotated: list[dict] = []

    for i, c in enumerate(candidates):
        key    = _key(c)
        stats  = exposure_stats.get(key) or {}

        base_score   = float(c.get("opportunity_score") or c.get("reranked_score") or 50)
        reranked     = base_score
        reasons: list[str] = []

        seen_tok  = stats.get("seen_count_for_token",      0)
        seen_glob = stats.get("seen_count_global",         0)
        saved     = bool(stats.get("already_saved_by_token",     False))
        contacted = bool(stats.get("already_contacted_by_token", False))
        last_seen = _parse_ts(stats.get("last_seen_at_for_token"))

        # ── Penalties ─────────────────────────────────────────────────────────
        if contacted:
            reranked -= 50
            reasons.append("already_contacted")
            penalized_count += 1
        elif saved:
            reranked -= 40
            reasons.append("already_saved")
            penalized_count += 1
        elif seen_tok >= 3:
            reranked -= 25
            reasons.append(f"seen_{seen_tok}x_by_token")
            penalized_count += 1
        elif seen_tok >= 2:
            reranked -= 15
            reasons.append(f"seen_{seen_tok}x_by_token")
            penalized_count += 1
        elif seen_tok >= 1:
            reranked -= 8
            reasons.append("seen_1x_by_token")

        if last_seen:
            try:
                diff_s = (now - last_seen.astimezone(_dt.timezone.utc)).total_seconds()
                if diff_s < 86_400:  # 24 h
                    reranked -= 20
                    reasons.append("seen_within_24h")
            except Exception:
                pass

        if seen_glob >= 20:
            reranked -= 15
        elif seen_glob >= 10:
            reranked -= 10
        elif seen_glob >= 5:
            reranked -= 5

        # ── Diversity boosts ──────────────────────────────────────────────────
        source = c.get("source") or c.get("platform") or ""
        if source and source not in seen_sources:
            reranked += 5
            seen_sources.add(source)
            reasons.append("diverse_source")

        gap_list = c.get("score_reasons") or []
        if isinstance(gap_list, list) and gap_list:
            first_gap = str(gap_list[0])
            if first_gap not in seen_gap_types:
                reranked += 5
                seen_gap_types.add(first_gap)
                reasons.append("unique_gap_type")

        if seen_glob < 3 and base_score >= 50:
            reranked += 8
            reasons.append("low_saturation_boost")

        # ── Floor ─────────────────────────────────────────────────────────────
        if not saved and not contacted:
            reranked = max(reranked, 35)

        annotated.append({
            **c,
            "original_rank":  i + 1,
            "reranked_score": int(_clamp(reranked, 0, 99)),
            "rerank_reasons": reasons,
            "exposure": {
                "seen_count_for_token":       seen_tok,
                "seen_count_global":          seen_glob,
                "already_saved_by_token":     saved,
                "already_contacted_by_token": contacted,
            },
        })

    print(
        f"[Rerank] exposure stats applied — "
        f"{penalized_count}/{len(annotated)} candidates penalized",
        flush=True,
    )

    # Sort by reranked_score descending
    annotated.sort(key=lambda x: -x["reranked_score"])

    n = len(annotated)
    if n >= 10:
        top_5      = annotated[:5]
        low_sat    = [c for c in annotated[5:] if c["exposure"]["seen_count_global"] < 3][:3]
        # exploration: distinct from top + low_sat, pull from lower positions
        used_names = {c.get("business_name") for c in top_5 + low_sat}
        explore    = [c for c in annotated if c.get("business_name") not in used_names][:2]
        mixed: list[dict] = []
        seen_mix: set[str] = set()
        for c in top_5 + low_sat + explore:
            k = c.get("business_name") or ""
            if k not in seen_mix:
                seen_mix.add(k)
                mixed.append(c)
        return mixed

    return annotated


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 6 — compute_opportunity_score_v2
# ─────────────────────────────────────────────────────────────────────────────

def compute_opportunity_score_v2(
    signals:    dict,
    saturation: dict | None = None,
    context:    dict | None = None,
) -> dict:
    """
    Phase 6 scoring model. Works with the FULL opportunity signals object
    (output of build_opportunity_signals) — not just social_conversion.

    Weighted combination of 6 dimensions:
        business_quality   — trust + intent (are they established?)
        opportunity_gap    — role-matched weakness = your value (high gap = high score)
        user_fit           — platform alignment with user's service angle
        contactability     — can we actually reach this business?
        saturation         — how many others have already tried?
        confidence         — how complete is the data?

    Missing data = lower confidence, NOT lower score.
    Blocked sources = confidence reduction, NOT score collapse.
    Floor: 35 when gap > 60 + contactability ≥ 60.
    """
    if not signals:
        return {
            "final_score":      0,
            "score_components": {},
            "score_label":      "low",
            "explanation":      "No signal data available.",
        }

    saturation = saturation or {}
    context    = context    or {}

    # Pull social_conversion sub-dict — handle both full-signals and bare sc dicts
    sc = signals.get("social_conversion") or signals

    service_angle = (
        signals.get("service_angle") or signals.get("primary_angle") or
        context.get("service_angle") or "content_creation"
    )
    user_role = (context.get("user_role") or "").lower()

    # ── 1. Business quality (0–1) ─────────────────────────────────────────────
    trust_s  = (sc.get("trust_signal")  or {}).get("score", 50)
    intent_s = (sc.get("intent_signal") or {}).get("score", 50)
    business_quality = (trust_s * 0.5 + intent_s * 0.5) / 100.0

    # ── 2. Opportunity gap (0–1) — GAP IS THE OPPORTUNITY ────────────────────
    attention_s  = (sc.get("attention_signal")       or {}).get("score", 50)
    funnel_s     = (sc.get("funnel_clarity_signal")  or {}).get("score", 50)
    conversion_s = (sc.get("conversion_path_signal") or {}).get("score", 50)
    content_s    = (sc.get("content_market_fit_signal") or {}).get("score", 50)
    retention_s  = (sc.get("retention_signal")       or {}).get("score", 50)

    _gap_map = {
        "content_creation":    max(0, 100 - attention_s),
        "short_form_video":    max(0, 100 - attention_s),
        "photography":         max(0, 100 - attention_s),
        "seo":                 max(0, 100 - intent_s),
        "website_redesign":    max(0, 100 - intent_s),
        "booking_conversion":  max(0, 100 - conversion_s),
        "crm_follow_up":       max(0, 100 - retention_s),
        "reputation_management": max(0, 100 - retention_s),
        "paid_ads":            max(0, 100 - funnel_s),
        "events_performance":  max(0, 100 - attention_s),
        "influencer_partnership": max(0, 100 - attention_s),
        "local_partnership":   max(0, 100 - funnel_s),
    }
    raw_gap        = _gap_map.get(service_angle, max(0, 100 - min(funnel_s, content_s)))
    opportunity_gap = _clamp(raw_gap, 0, 100) / 100.0

    # ── 3. User fit (0–1) ─────────────────────────────────────────────────────
    platform_s = (sc.get("platform_fit_signal") or {}).get("score", 40)
    user_fit   = _clamp(platform_s, 0, 100) / 100.0 if user_role else 0.5

    # ── 4. Contactability (0–1) ───────────────────────────────────────────────
    best_path = (
        signals.get("best_contact_path") or
        (signals.get("contact") or {}).get("best_contact_path") or
        "unknown"
    )
    _path_scores = {
        "phone_call":            0.90,
        "email":                 0.90,
        "instagram_dm":          0.70,
        "linkedin_message":      0.65,
        "website_contact_form":  0.60,
        "facebook_message":      0.40,
        "booking_page":          0.30,
        "unknown":               0.15,
    }
    contactability = _path_scores.get(best_path, 0.40)

    # ── 5. Saturation adjustment (0.5–1.0) ────────────────────────────────────
    total_saves     = saturation.get("total_saves",     0)
    total_contacted = saturation.get("total_contacted", 0)
    total_won       = saturation.get("total_won",       0)
    sat_raw = (
        min(total_saves,     6) * 0.05 +
        min(total_contacted, 5) * 0.06 +
        min(total_won,       3) * 0.10
    )
    sat_adj = 1.0 - min(sat_raw, 0.50)

    # ── 6. Confidence (0.4–1.0) ───────────────────────────────────────────────
    keys_present = sum(
        1 for k in (
            "trust_signal", "intent_signal",
            "attention_signal", "conversion_path_signal",
        )
        if sc.get(k)
    )
    confidence = min(1.0, 0.4 + keys_present * 0.15)

    # Blocked sources reduce confidence, not score
    blocked_count = signals.get("_blocked_sources", 0)
    if blocked_count:
        confidence = max(0.30, confidence - blocked_count * 0.10)

    # ── Weighted combination ──────────────────────────────────────────────────
    _w = {
        "business_quality": 0.15,
        "opportunity_gap":  0.30,
        "user_fit":         0.20,
        "contactability":   0.20,
        "saturation":       0.10,
        "confidence":       0.05,
    }

    raw_final = (
        business_quality * _w["business_quality"] +
        opportunity_gap  * _w["opportunity_gap"]  +
        user_fit         * _w["user_fit"]          +
        contactability   * _w["contactability"]    +
        sat_adj          * _w["saturation"]        +
        confidence       * _w["confidence"]
    ) * 100

    final_score = _clamp(raw_final, 1, 99)

    # Floor: strong gap + reachable contact should never score below 35
    if opportunity_gap >= 0.60 and contactability >= 0.60:
        final_score = max(final_score, 35)

    label = "high" if final_score >= 65 else "medium" if final_score >= 40 else "low"

    explanation = (
        f"Business quality: {int(business_quality * 100)}/100 | "
        f"Opportunity gap: {int(opportunity_gap * 100)}/100 | "
        f"User fit: {int(user_fit * 100)}/100 | "
        f"Contactability: {int(contactability * 100)}/100 | "
        f"Saturation adj: {int(sat_adj * 100)}% | "
        f"Confidence: {int(confidence * 100)}%"
    )

    score_components = {
        "business_quality": round(business_quality, 3),
        "opportunity_gap":  round(opportunity_gap,  3),
        "user_fit":         round(user_fit,         3),
        "contactability":   round(contactability,   3),
        "saturation":       round(sat_adj,          3),
        "confidence":       round(confidence,       3),
    }

    return {
        "final_score":      final_score,
        "score_components": score_components,
        "score_label":      label,
        "explanation":      explanation,
    }
