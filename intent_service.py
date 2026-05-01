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
