"""
contact_service.py — Contact inference and service angle detection.

No external API calls. Works entirely from data already available in the
business dict and the request context (user_role, mode, signal_preferences).

Public API
----------
infer_service_angle(context, business) -> str
extract_contact_candidates(business)   -> list[dict]
infer_best_contact_path(business, context, signals) -> dict
"""

from __future__ import annotations
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_ANGLES = {
    "content_creation",
    "short_form_video",
    "photography",
    "seo",
    "website_redesign",
    "booking_conversion",
    "crm_follow_up",
    "events_performance",
    "influencer_partnership",
    "local_partnership",
    "paid_ads",
    "reputation_management",
}

# (pattern_words, angle) — evaluated in order; first match wins
_ANGLE_RULES: list[tuple[list[str], str]] = [
    # Performers / events
    (["comedian", "perform", "dj", "musician", "singer", "entertainer"], "events_performance"),
    # Photography
    (["photographer", "photography", "photo shoot", "videographer"], "photography"),
    # Short-form / video
    (["tiktok", "reels", "short form", "short-form", "video content"], "short_form_video"),
    # Influencer / creator partnership
    (["influencer", "ugc", "brand deal", "collab", "collaboration", "sponsor"], "influencer_partnership"),
    # Content creation (generic creator)
    (["content creator", "content creation", "social media manager", "social manager"], "content_creation"),
    # SEO
    (["seo", "search engine", "weak seo", "no seo", "organic traffic", "keywords"], "seo"),
    # Website / web design
    (["web designer", "web design", "no website", "bad website", "website redesign", "website rebuild", "landing page"], "website_redesign"),
    # Booking / conversion
    (["no booking", "booking gap", "booking system", "booking conversion", "no online booking", "missing booking"], "booking_conversion"),
    # CRM / follow-up
    (["crm", "follow-up", "follow up", "email marketing", "sms marketing", "retention", "marketer", "marketing"], "crm_follow_up"),
    # Paid ads
    (["paid ads", "facebook ads", "google ads", "instagram ads", "tiktok ads", "ppc", "ad spend"], "paid_ads"),
    # Reputation
    (["reputation", "review management", "reviews", "few reviews", "low reviews", "negative reviews"], "reputation_management"),
    # Local partnership (fallback for partnership mode without creator signal)
    (["partnership", "local partner", "event"], "local_partnership"),
]

_SOCIAL_FIRST_ROLES = {
    "content creator", "photographer", "videographer", "influencer",
    "ugc creator", "social media manager",
}
_PERFORMER_ROLES = {"comedian", "performer", "dj", "musician", "singer", "entertainer", "artist"}
_TECH_ROLES = {"seo specialist", "web designer", "web developer", "marketer", "crm specialist"}

_VENUE_NICHES = {"bar", "lounge", "restaurant", "cafe", "event venue", "live music venue",
                 "nightclub", "club", "comedy club", "theater", "theatre"}


# ---------------------------------------------------------------------------
# infer_service_angle
# ---------------------------------------------------------------------------

def infer_service_angle(context: dict, business: dict | None = None) -> str:
    """
    Infer the most appropriate service angle from context signals.

    Context keys used:
        user_role, mode, signal_preferences, niche, goal, user_query

    Returns one of the VALID_ANGLES strings.
    Defaults to "content_creation" when no strong signal is found.
    """
    if not context:
        context = {}

    # Build a single lowercase text to match against
    parts = [
        str(context.get("user_role") or ""),
        str(context.get("mode") or ""),
        str(context.get("niche") or ""),
        str(context.get("goal") or ""),
        str(context.get("user_query") or ""),
    ]
    prefs = context.get("signal_preferences") or []
    if isinstance(prefs, list):
        parts += [str(p) for p in prefs]
    if business:
        parts.append(str(business.get("category") or ""))

    haystack = " ".join(parts).lower()

    for keywords, angle in _ANGLE_RULES:
        if any(kw in haystack for kw in keywords):
            return angle

    # Mode-based fallbacks
    mode = (context.get("mode") or "").lower()
    if mode == "partnership":
        return "influencer_partnership"
    if mode in ("outreach", "market"):
        return "content_creation"

    return "content_creation"


# ---------------------------------------------------------------------------
# extract_contact_candidates
# ---------------------------------------------------------------------------

def extract_contact_candidates(business: dict) -> list[dict]:
    """
    Extract all reachable contact paths from available business data.
    No external calls — uses only the data already in the business dict.

    Returns a list of candidate dicts, sorted best-first by confidence.
    """
    candidates: list[dict] = []

    def _add(method: str, value: str, confidence: str, reason: str,
             role: str = "unknown", name: str | None = None):
        if not value or not value.strip():
            return
        candidates.append({
            "contact_method": method,
            "contact_value":  value.strip(),
            "contact_role":   role,
            "contact_name":   name,  # None unless explicitly available
            "confidence":     confidence,
            "reason":         reason,
        })

    # Instagram
    ig = business.get("instagram_url") or business.get("instagram") or ""
    if ig:
        _add("instagram_dm", ig, "medium",
             "Instagram account detected — DM is a common first-touch for creators and local businesses.",
             role="owner_or_social_media_manager")

    # Facebook
    fb = business.get("facebook_url") or business.get("facebook") or ""
    if fb:
        _add("facebook_message", fb, "low",
             "Facebook page found — Messenger DM is a lower-response path but viable.",
             role="owner_or_page_admin")

    # LinkedIn
    li = business.get("linkedin") or ""
    if li:
        _add("linkedin_message", li, "medium",
             "LinkedIn profile found — good for B2B or professional service outreach.",
             role="owner_or_decision_maker")

    # Phone
    phone = business.get("phone") or ""
    if phone:
        _add("phone_call", phone, "high",
             "Phone number available — direct call reaches a decision-maker quickly for local businesses.",
             role="owner_or_manager")

    # Website contact form / email
    website = business.get("website") or ""
    if website:
        parsed = urlparse(website if website.startswith("http") else "https://" + website)
        if parsed.netloc:
            _add("website_contact_form", website, "medium",
                 "Website detected — contact form or email link is a professional first touch for web/SEO services.",
                 role="owner_or_marketing_contact")

    # Email directly
    email = business.get("email") or ""
    if email and "@" in email:
        _add("email", email, "high",
             "Email address available — direct email is professional and documented.",
             role="owner_or_contact")

    # Booking URL
    booking = business.get("booking_url") or (business.get("raw_data") or {}).get("booking_url", "")
    if booking:
        _add("booking_page", booking, "low",
             "Booking page found — not ideal for cold outreach but confirms digital presence.",
             role="unknown")

    # Confidence order: high → medium → low
    _order = {"high": 0, "medium": 1, "low": 2}
    candidates.sort(key=lambda c: _order.get(c["confidence"], 3))

    return candidates


# ---------------------------------------------------------------------------
# infer_best_contact_path
# ---------------------------------------------------------------------------

def infer_best_contact_path(
    business: dict,
    context:  dict | None = None,
    signals:  dict | None = None,
) -> dict:
    """
    Infer the single best contact path plus alternatives.

    Rules (applied in order):
    1. Social-first roles (creator, photographer) + Instagram → instagram_dm
    2. Performers + venue-type niche → phone_call or instagram_dm
    3. SEO / web / marketing → website_contact_form or email
    4. No website but phone → phone_call
    5. LinkedIn + B2B angle → linkedin_message
    6. Fallback: best of what's available or unknown

    Never invents a person's name.
    Inferred roles are clearly labelled as inferred.
    """
    if not context:
        context = {}

    user_role = (context.get("user_role") or "").lower()
    mode      = (context.get("mode") or "outreach").lower()
    niche     = (context.get("niche") or business.get("category") or "").lower()

    candidates = extract_contact_candidates(business)
    service_angle = infer_service_angle(context, business)

    def _find(method: str):
        return next((c for c in candidates if c["contact_method"] == method), None)

    best: dict | None = None
    contact_reason = ""
    contact_target = "owner_or_manager"  # default — labelled as inferred

    # ── Rule 1: social-first creator/photographer ─────────────────────────
    if any(r in user_role for r in ("content creator", "photographer", "videographer",
                                    "influencer", "ugc")):
        ig = _find("instagram_dm")
        if ig:
            best = ig
            contact_target = "owner_or_social_media_manager (inferred)"
            contact_reason = (
                "Content creators typically get the best response via Instagram DM — "
                "it's the platform where social media decisions are made and the owner "
                "or social manager is most active."
            )

    # ── Rule 2: performers + venue niche ─────────────────────────────────
    if not best and any(r in user_role for r in ("comedian", "performer", "dj", "musician",
                                                  "singer", "entertainer", "artist")):
        if any(n in niche for n in _VENUE_NICHES):
            phone = _find("phone_call")
            ig    = _find("instagram_dm")
            if phone:
                best = phone
                contact_target = "events_manager_or_owner (inferred)"
                contact_reason = (
                    "For performers booking at venues, a direct phone call reaches the "
                    "bookings manager or owner fastest. Ask for whoever handles events "
                    "or entertainment bookings."
                )
            elif ig:
                best = ig
                contact_target = "venue_manager_or_social (inferred)"
                contact_reason = (
                    "No phone found — Instagram DM is the next-best path for venue "
                    "performance booking outreach."
                )

    # ── Rule 3: SEO / web / marketing ────────────────────────────────────
    if not best and service_angle in ("seo", "website_redesign", "crm_follow_up", "paid_ads"):
        web = _find("website_contact_form")
        em  = _find("email")
        if em:
            best = em
            contact_target = "owner_or_marketing_contact (inferred)"
            contact_reason = (
                "Email is the professional standard for web/SEO/marketing outreach — "
                "it's documentable, easy to follow up on, and expected by business owners."
            )
        elif web:
            best = web
            contact_target = "owner_or_marketing_contact (inferred)"
            contact_reason = (
                "Website contact form is ideal for web/SEO services — it demonstrates "
                "you can navigate their digital presence and positions you professionally."
            )

    # ── Rule 4: LinkedIn for B2B ─────────────────────────────────────────
    if not best and mode in ("outreach",) and "b2b" in niche:
        li = _find("linkedin_message")
        if li:
            best = li
            contact_target = "owner_or_decision_maker (inferred)"
            contact_reason = "LinkedIn is the standard channel for B2B professional outreach."

    # ── Rule 5: phone if no website ──────────────────────────────────────
    if not best and not business.get("website"):
        phone = _find("phone_call")
        if phone:
            best = phone
            contact_target = "owner_or_manager (inferred)"
            contact_reason = (
                "No website detected — phone is the most direct path to the owner "
                "for a business without an active digital presence."
            )

    # ── Rule 6: best available ────────────────────────────────────────────
    if not best and candidates:
        best = candidates[0]
        contact_reason = (
            f"{best['reason']} No stronger signal available based on provided data."
        )

    # ── Fallback: nothing available ───────────────────────────────────────
    if not best:
        return {
            "best_contact_path": "unknown",
            "contact_target":    "unknown",
            "contact_method":    "unknown",
            "contact_value":     None,
            "contact_confidence": "low",
            "contact_reason": (
                "No phone, email, website, or social profiles were found for this business. "
                "Consider searching manually or visiting in person."
            ),
            "alternatives": [],
        }

    alternatives = [
        c for c in candidates
        if c["contact_method"] != best["contact_method"]
    ][:3]

    confidence = best.get("confidence", "low")
    if not candidates or len(candidates) <= 1:
        confidence = "low"

    return {
        "best_contact_path": best["contact_method"],
        "contact_target":    contact_target,
        "contact_method":    best["contact_method"],
        "contact_value":     best.get("contact_value"),
        "contact_confidence": confidence,
        "contact_reason":    contact_reason,
        "alternatives": [
            {
                "contact_method": a["contact_method"],
                "contact_value":  a.get("contact_value"),
                "contact_role":   a.get("contact_role"),
                "confidence":     a.get("confidence"),
            }
            for a in alternatives
        ],
    }
