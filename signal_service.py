"""
signal_service.py — Opportunity scoring and signal analysis for Yelhao.

No external API calls. All scoring is derived from data already present
in the business dict, context, and saturation record.

Public API
----------
build_social_conversion_signals(business, context)              -> dict
build_business_value_insights(business, context, signals)       -> dict
build_opportunity_signals(business, context, saturation, contact) -> dict
compute_opportunity_score(signals, saturation, context)         -> dict
"""

from __future__ import annotations
import math


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _present(val) -> bool:
    """True if val is a non-empty string, non-zero number, or non-empty list."""
    if val is None:
        return False
    if isinstance(val, str):
        return bool(val.strip())
    if isinstance(val, (list, dict)):
        return bool(val)
    if isinstance(val, (int, float)):
        return val > 0
    return bool(val)


def _clamp(val: float, lo: float = 0.0, hi: float = 100.0) -> int:
    return int(max(lo, min(hi, val)))


def _has_social(biz: dict) -> bool:
    return any(_present(biz.get(k)) for k in (
        "instagram_url", "instagram", "facebook_url", "facebook",
        "tiktok", "twitter", "youtube",
    ))


def _has_booking(biz: dict) -> bool:
    raw = biz.get("raw_data") or {}
    return any(_present(raw.get(k)) for k in ("booking_url", "booking_platform")) or \
           any(kw in (biz.get("website") or "").lower() for kw in
               ("booksy", "fresha", "vagaro", "mindbody", "square", "calendly", "booking"))


def _has_website(biz: dict) -> bool:
    return _present(biz.get("website"))


def _review_count(biz: dict) -> int:
    return int(biz.get("google_review_count") or 0)


def _rating(biz: dict) -> float:
    try:
        return float(biz.get("google_rating") or 0)
    except (ValueError, TypeError):
        return 0.0


def _signal_prefs(context: dict) -> list[str]:
    prefs = (context or {}).get("signal_preferences") or []
    return [str(p).lower() for p in prefs]


# ---------------------------------------------------------------------------
# build_social_conversion_signals
# ---------------------------------------------------------------------------

def build_social_conversion_signals(
    business: dict,
    context:  dict | None = None,
) -> dict:
    """
    Map the business's public signals to a social-conversion funnel model.

    Attention   → top-of-funnel reach (social following, TikTok/Instagram)
    Intent      → search intent (Google, website, booking page)
    Trust       → social proof (reviews, UGC, tagged content, testimonials)
    Funnel      → clarity of next step (CTA, link in bio, contact button)
    Conversion  → can a visitor actually act? (booking, form, phone)
    Content fit → is the content strategy helping or hurting?
    Retention   → do customers come back? (loyalty, specials, email/SMS)
    Platform    → is the business on the right platform for its goal?
    """
    if not business:
        business = {}
    if not context:
        context = {}

    prefs = _signal_prefs(context)

    # ── Attention signal ──────────────────────────────────────────────────
    has_ig = _present(business.get("instagram_url") or business.get("instagram"))
    has_fb = _present(business.get("facebook_url")  or business.get("facebook"))
    has_tt = _present(business.get("tiktok"))
    social_count = sum([has_ig, has_fb, has_tt])

    attention_score = _clamp(social_count * 25 + (25 if social_count else 0))
    attention_evidence = ", ".join(filter(None, [
        "Instagram found"   if has_ig else "",
        "Facebook found"    if has_fb else "",
        "TikTok found"      if has_tt else "",
        "No social detected" if social_count == 0 else "",
    ]))
    low_social = any(p in ("low social engagement", "weak instagram", "no social") for p in prefs)
    if low_social:
        attention_score = max(10, attention_score - 20)
        attention_evidence += "; signal: low engagement reported"

    attention_pe = (
        "This business has social media presence but may not be converting attention into action."
        if social_count > 0 else
        "No social media detected — this business is invisible on the platforms where customers discover local services."
    )

    # ── Intent signal ─────────────────────────────────────────────────────
    has_web     = _has_website(business)
    has_booking = _has_booking(business)
    has_phone   = _present(business.get("phone"))
    rev_count   = _review_count(business)

    intent_score  = 0
    intent_score += 30 if has_web else 0
    intent_score += 25 if has_booking else 0
    intent_score += 20 if has_phone else 0
    intent_score += min(25, int(math.log1p(rev_count) * 5)) if rev_count > 0 else 0
    intent_score  = _clamp(intent_score)

    intent_evidence = ", ".join(filter(None, [
        "Website found"           if has_web     else "No website",
        "Booking platform found"  if has_booking else "No booking system",
        f"Phone available"        if has_phone   else "",
        f"{rev_count} Google reviews" if rev_count > 0 else "No reviews found",
    ]))

    intent_pe = (
        "Strong intent signals — website, booking, and phone all present."
        if (has_web and has_booking and has_phone) else
        "Weak intent signals — customers who find this business may not know how to act."
        if not (has_web or has_booking or has_phone) else
        "Partial intent signals — some paths exist but others are missing."
    )

    # ── Trust signal ──────────────────────────────────────────────────────
    rating = _rating(business)
    few_reviews = any("few reviews" in p for p in prefs)
    high_reviews = any("high reviews" in p for p in prefs)

    trust_score = 0
    if rev_count > 0:
        trust_score += min(35, int(math.log1p(rev_count) * 7))
    if rating >= 4.5:
        trust_score += 35
    elif rating >= 4.0:
        trust_score += 25
    elif rating >= 3.5:
        trust_score += 10
    elif rating > 0:
        trust_score += 5
    if few_reviews:
        trust_score = max(5, trust_score - 15)
    trust_score = _clamp(trust_score)

    trust_pe = (
        f"Strong trust signals: {rating}★ across {rev_count} reviews."
        if (rating >= 4.0 and rev_count >= 20) else
        "Limited social proof — few or no visible reviews or testimonials."
        if rev_count < 5 else
        f"Moderate trust: {rating}★ with {rev_count} reviews."
    )

    # ── Funnel clarity signal ─────────────────────────────────────────────
    no_booking = any("no booking" in p for p in prefs)
    poor_presence = any(p in ("poor digital presence", "bad website", "no website") for p in prefs)

    funnel_score = 50  # neutral start
    if has_booking:
        funnel_score += 25
    if has_web:
        funnel_score += 15
    if no_booking:
        funnel_score -= 25
    if poor_presence:
        funnel_score -= 20
    funnel_score = _clamp(funnel_score)

    funnel_pe = (
        "Clear funnel: the business has a website and a booking system — customers know what to do."
        if (has_web and has_booking) else
        "Broken funnel: people may discover this business on social but have no clear next step."
        if (not has_web and not has_booking) else
        "Partial funnel: some path exists but the conversion step is unclear or missing."
    )

    # ── Conversion path signal ────────────────────────────────────────────
    conversion_score = 0
    if has_booking:
        conversion_score += 45
    if has_phone:
        conversion_score += 25
    if has_web:
        conversion_score += 20
    if not (has_booking or has_phone or has_web):
        conversion_score = 5
    conversion_score = _clamp(conversion_score)

    conversion_pe = (
        "Direct conversion path exists via booking system and/or phone."
        if (has_booking or has_phone) else
        "No clear conversion path — there's no easy way for an interested customer to take action."
    )

    # ── Content market fit signal ─────────────────────────────────────────
    needs_content = any("content gap" in p or "needs content" in p for p in prefs)
    content_score = 40
    if social_count > 0:
        content_score += 25
    if needs_content:
        content_score = max(10, content_score - 25)
    content_score = _clamp(content_score)

    content_pe = (
        "Content gap detected — the business has a platform but isn't making the most of it."
        if needs_content else
        "No specific content gap flagged — check engagement signals for clarity."
    )

    # ── Retention signal ──────────────────────────────────────────────────
    raw = business.get("raw_data") or {}
    has_loyalty = _present(raw.get("loyalty_program")) or _present(raw.get("email_capture"))
    retention_score = 20  # base assumption
    if has_loyalty:
        retention_score += 40
    if rev_count >= 50:
        retention_score += 20  # high review count suggests repeat customers
    retention_score = _clamp(retention_score)

    retention_pe = (
        "Retention signals present — loyalty, email capture, or strong review volume."
        if (has_loyalty or rev_count >= 50) else
        "Limited retention signals — the business may have a leaky customer bucket."
    )

    # ── Platform fit signal ───────────────────────────────────────────────
    user_role = (context.get("user_role") or "").lower()
    niche = (context.get("niche") or business.get("category") or "").lower()

    platform_score = 40
    if "tiktok" in user_role or "content" in user_role:
        if has_tt or has_ig:
            platform_score += 40
        else:
            platform_score -= 20
    if "seo" in user_role:
        if has_web:
            platform_score += 30
        else:
            platform_score += 10  # gap = opportunity
    platform_score = _clamp(platform_score)

    platform_pe = (
        "Platform alignment is good — the business is on channels relevant to this opportunity."
        if platform_score >= 60 else
        "Platform mismatch — the business may not be on the right platforms for this service angle."
    )

    # ── Main conversion leak + recommended fix ────────────────────────────
    scores_map = {
        "attention":   attention_score,
        "intent":      intent_score,
        "trust":       trust_score,
        "funnel":      funnel_score,
        "conversion":  conversion_score,
        "content_fit": content_score,
        "retention":   retention_score,
        "platform":    platform_score,
    }
    main_leak = min(scores_map, key=scores_map.get)

    fix_map = {
        "attention":   "Start a consistent Instagram or TikTok presence to build top-of-funnel awareness.",
        "intent":      "Add a website with clear contact info and a booking or inquiry system.",
        "trust":       "Generate and respond to reviews — ask happy customers to share their experience.",
        "funnel":      "Add a clear call-to-action: booking link, contact button, or link in bio.",
        "conversion":  "Make it easy for customers to act — add a booking page, contact form, or phone number.",
        "content_fit": "Create consistent, value-driven content that shows the business's expertise.",
        "retention":   "Introduce a loyalty program, email capture, or recurring seasonal campaigns.",
        "platform":    "Expand to platforms where the target audience actually discovers this type of business.",
    }

    # ── Creator angle + business value ───────────────────────────────────
    creator_angle = _infer_creator_angle(business, context)
    business_value = _short_business_value(business, context, scores_map)

    return {
        "attention_signal":       {"score": attention_score,  "evidence": attention_evidence,  "plain_english": attention_pe},
        "intent_signal":          {"score": intent_score,     "evidence": intent_evidence,      "plain_english": intent_pe},
        "trust_signal":           {"score": trust_score,       "evidence": f"{rating}★ · {rev_count} reviews", "plain_english": trust_pe},
        "funnel_clarity_signal":  {"score": funnel_score,     "evidence": f"Booking: {'yes' if has_booking else 'no'} · Website: {'yes' if has_web else 'no'}", "plain_english": funnel_pe},
        "conversion_path_signal": {"score": conversion_score, "evidence": f"Phone: {'yes' if has_phone else 'no'} · Booking: {'yes' if has_booking else 'no'}", "plain_english": conversion_pe},
        "content_market_fit_signal": {"score": content_score, "evidence": "content gap" if needs_content else "no gap flagged", "plain_english": content_pe},
        "retention_signal":       {"score": retention_score,  "evidence": "loyalty/email detected" if has_loyalty else "no retention system detected", "plain_english": retention_pe},
        "platform_fit_signal":    {"score": platform_score,   "evidence": f"Social platforms: {social_count}", "plain_english": platform_pe},
        "main_conversion_leak":   main_leak,
        "recommended_fix":        fix_map[main_leak],
        "creator_angle":          creator_angle,
        "business_value":         business_value,
    }


def _infer_creator_angle(business: dict, context: dict) -> str:
    user_role = (context.get("user_role") or "").lower()
    niche     = (context.get("niche") or business.get("category") or "").lower()

    if "photographer" in user_role:
        return f"Product and atmosphere photography for a {niche or 'local business'} — before/after or transformation content."
    if "content creator" in user_role or "videographer" in user_role:
        return f"Short-form video or day-in-the-life content for a {niche or 'local business'} that could drive local discovery."
    if "comedian" in user_role or "performer" in user_role:
        return f"Live performance or event hosting at a {niche or 'local venue'} — adds entertainment value for their audience."
    if "dj" in user_role:
        return f"DJ set or music activation for a {niche or 'local venue'} — drives foot traffic and social moments."
    if "seo" in user_role:
        return f"Search engine visibility for a {niche or 'local business'} that may be invisible to nearby customers."
    if "web designer" in user_role:
        return f"Website redesign or landing page for a {niche or 'local business'} — converts more of their existing traffic."
    return f"Service offering that fills a visible gap in the {niche or 'local business'}'s digital presence."


def _short_business_value(business: dict, context: dict, scores: dict) -> str:
    niche = (context.get("niche") or business.get("category") or "this business").title()
    weak  = min(scores, key=scores.get)
    weak_label = {
        "attention": "low visibility",
        "intent": "missing purchase intent",
        "trust": "limited social proof",
        "funnel": "no clear next step for customers",
        "conversion": "no conversion path",
        "content_fit": "underperforming content",
        "retention": "weak customer retention",
        "platform": "wrong platform mix",
    }.get(weak, "an untapped gap")
    return (
        f"{niche} has {weak_label}. Addressing this could help the business attract more customers, "
        f"convert interest into bookings or visits, and build a stronger local reputation."
    )


# ---------------------------------------------------------------------------
# build_business_value_insights
# ---------------------------------------------------------------------------

def build_business_value_insights(
    business: dict,
    context:  dict | None = None,
    signals:  dict | None = None,
) -> dict:
    """
    Plain-English explanation of how the user creates value for this business.
    Does NOT invent financial numbers — uses directional impact only.
    """
    if not business:
        business = {}
    if not context:
        context = {}
    if not signals:
        signals = {}

    user_role = (context.get("user_role") or "service provider").lower()
    niche = (context.get("niche") or business.get("category") or "local business").title()
    service_angle = context.get("service_angle") or "content_creation"

    # CAC, LTV, Churn, Conversion impact — directional only
    _impact_map = {
        "content_creation":        {"cac": "medium", "ltv": "medium", "churn": "low",    "conversion": "medium"},
        "short_form_video":        {"cac": "high",   "ltv": "medium", "churn": "low",    "conversion": "medium"},
        "photography":             {"cac": "medium", "ltv": "medium", "churn": "low",    "conversion": "medium"},
        "seo":                     {"cac": "high",   "ltv": "high",   "churn": "medium", "conversion": "high"},
        "website_redesign":        {"cac": "medium", "ltv": "medium", "churn": "medium", "conversion": "high"},
        "booking_conversion":      {"cac": "low",    "ltv": "high",   "churn": "medium", "conversion": "high"},
        "crm_follow_up":           {"cac": "low",    "ltv": "high",   "churn": "high",   "conversion": "medium"},
        "events_performance":      {"cac": "high",   "ltv": "medium", "churn": "low",    "conversion": "medium"},
        "influencer_partnership":  {"cac": "high",   "ltv": "medium", "churn": "low",    "conversion": "medium"},
        "local_partnership":       {"cac": "medium", "ltv": "medium", "churn": "low",    "conversion": "low"},
        "paid_ads":                {"cac": "medium", "ltv": "medium", "churn": "low",    "conversion": "high"},
        "reputation_management":   {"cac": "medium", "ltv": "high",   "churn": "high",   "conversion": "medium"},
    }
    impact = _impact_map.get(service_angle, {"cac": "unknown", "ltv": "unknown", "churn": "unknown", "conversion": "unknown"})

    # How user contributes
    contributions = _build_contributions(user_role, service_angle, niche)

    # Relationship pitch
    pitch = (
        f"You can help {niche} solve a specific, visible problem in their business — "
        f"not just as a vendor, but as a growth partner who understands their customers."
    )

    return {
        "value_summary": f"{user_role.title()} + {niche}: bridging a {service_angle.replace('_', ' ')} gap.",
        "plain_english_value": _short_business_value(business, context, {k: 50 for k in ["attention","intent","trust","funnel","conversion","content_fit","retention","platform"]}),
        "customer_acquisition_angle": f"Reaching new customers who are searching or scrolling but not finding {niche} yet.",
        "customer_retention_angle":   f"Giving existing customers a reason to come back and bring others.",
        "repeat_visit_angle":         f"Creating content or systems that remind past customers about {niche}.",
        "trust_angle":                f"Building credibility through reviews, social proof, and consistent brand presence.",
        "revenue_angle":              f"More discovered customers + clearer conversion path = more revenue without raising prices.",
        "relationship_angle":         f"Positioning as a strategic partner, not a cold vendor, by solving a visible real-world problem.",
        "simple_metrics": {
            "likely_cac_impact":        impact["cac"],
            "likely_ltv_impact":        impact["ltv"],
            "likely_churn_impact":      impact["churn"],
            "likely_conversion_impact": impact["conversion"],
        },
        "metric_explanations": {
            "cac":        "CAC = cost (time or money) to get a new customer. Lower is better. Your work can reduce this by bringing customers in more organically.",
            "ltv":        "LTV = how much a customer is worth over time if they return. Higher is better. You help increase this by making the business more memorable and easier to re-engage.",
            "churn":      "Churn = how many customers stop returning. Lower is better. You help reduce churn by keeping the brand top-of-mind and improving the experience.",
            "conversion": "Conversion = how many interested people actually book, buy, call, or visit. Higher is better. You help by removing friction and adding a clear next step.",
        },
        "how_user_contributes": contributions,
        "relationship_pitch": pitch,
    }


def _build_contributions(user_role: str, service_angle: str, niche: str) -> list[dict]:
    base = [
        {
            "contribution": f"Identify the biggest gap in {niche}'s digital presence",
            "business_value": "Saves the owner from guessing — gives them a clear priority",
            "example": f"A {niche} with no booking system may be losing 30%+ of interested visitors.",
        },
    ]

    angle_contribs = {
        "content_creation": {
            "contribution": "Create content that shows the business's personality and expertise",
            "business_value": "Attracts the right customers and builds trust before the first visit",
            "example": f"A before/after post for a {niche} can outperform a paid ad by 5x organically.",
        },
        "short_form_video": {
            "contribution": "Produce short-form video that drives local discovery on TikTok and Reels",
            "business_value": "Gets the business in front of people who didn't know it existed",
            "example": f"A single viral Reel for a {niche} can drive hundreds of new profile visits in days.",
        },
        "seo": {
            "contribution": "Improve visibility in local search results",
            "business_value": "Puts the business in front of high-intent searchers ready to act",
            "example": f'Ranking for "{niche} near me" can bring in customers with intent to book immediately.',
        },
        "website_redesign": {
            "contribution": "Redesign or rebuild the website to convert more visitors",
            "business_value": "Turns existing traffic into actual customers",
            "example": f"A clear CTA and booking button on a {niche}'s homepage can double conversions.",
        },
        "booking_conversion": {
            "contribution": "Add or improve a booking system",
            "business_value": "Makes it easy for interested customers to commit right now",
            "example": f"Adding a Calendly or Booksy link to a {niche}'s Instagram bio can instantly improve bookings.",
        },
        "events_performance": {
            "contribution": "Provide live entertainment that draws a crowd",
            "business_value": "Increases foot traffic and social-shareable moments on event nights",
            "example": f"A comedy night or DJ set at a {niche} can fill the venue and generate organic social content.",
        },
        "reputation_management": {
            "contribution": "Help the business generate and respond to more reviews",
            "business_value": "Builds trust before a customer even steps through the door",
            "example": f"Going from 10 to 50 reviews for a {niche} can meaningfully increase Google Maps ranking.",
        },
    }

    contrib = angle_contribs.get(service_angle, {
        "contribution": f"Apply {service_angle.replace('_', ' ')} expertise to fill a real gap",
        "business_value": "Creates tangible improvement in a measurable area",
        "example": f"Focused {service_angle.replace('_', ' ')} work for a {niche} can shift engagement or conversions within 30–60 days.",
    })

    return base + [contrib]


# ---------------------------------------------------------------------------
# build_opportunity_signals
# ---------------------------------------------------------------------------

def build_opportunity_signals(
    business:   dict,
    context:    dict | None = None,
    saturation: dict | None = None,
    contact:    dict | None = None,
) -> dict:
    """
    Synthesize all available signals into a unified opportunity assessment.
    """
    if not business:
        business = {}
    if not context:
        context = {}

    try:
        from contact_service import infer_service_angle, infer_best_contact_path
        service_angle = infer_service_angle(context, business)
        if not contact:
            contact = infer_best_contact_path(business, context)
    except Exception:
        service_angle = "content_creation"
        if not contact:
            contact = {"best_contact_path": "unknown", "contact_confidence": "low"}

    social_conv = build_social_conversion_signals(business, context)
    biz_value   = build_business_value_insights(business, {**context, "service_angle": service_angle}, social_conv)
    score_dict  = compute_opportunity_score(social_conv, saturation, context)

    # Identify primary problem
    signals_list = _extract_signal_list(social_conv)
    primary_problem = social_conv.get("recommended_fix", "No specific problem identified.")

    # Why now
    why_now = _infer_why_now(business, context, social_conv)

    # Recommended pitch
    pitch = biz_value.get("relationship_pitch", "")

    # Risk flags
    risk_flags = _collect_risk_flags(business, saturation, social_conv)

    # Next best action
    next_action = _next_best_action(contact, service_angle, social_conv)

    biz_name = business.get("name") or business.get("business_name") or "this business"
    print(
        f"[Signals] {biz_name}: score={score_dict['final_score']}, "
        f"angle={service_angle}, contact={contact.get('best_contact_path', 'unknown')}",
        flush=True,
    )

    return {
        "overall_signal_score":  score_dict["final_score"],
        "confidence":            score_dict["score_label"],
        "primary_angle":         service_angle,
        "service_angle":         service_angle,
        "primary_problem":       primary_problem,
        "why_now":               why_now,
        "recommended_pitch":     pitch,
        "best_contact_path":     contact.get("best_contact_path", "unknown"),
        "contact_target":        contact.get("contact_target", "unknown"),
        "signals":               signals_list,
        "social_conversion":     social_conv,
        "business_value":        biz_value,
        "risk_flags":            risk_flags,
        "next_best_action":      next_action,
    }


def _extract_signal_list(social_conv: dict) -> list[dict]:
    """Pull the individual signals into a flat list for easy rendering."""
    signal_keys = [
        ("attention_signal",          "attention",   "Attention"),
        ("intent_signal",             "intent",      "Purchase Intent"),
        ("trust_signal",              "trust",       "Trust / Social Proof"),
        ("funnel_clarity_signal",     "funnel",      "Funnel Clarity"),
        ("conversion_path_signal",    "conversion",  "Conversion Path"),
        ("content_market_fit_signal", "content_fit", "Content Market Fit"),
        ("retention_signal",          "retention",   "Retention"),
        ("platform_fit_signal",       "platform",    "Platform Fit"),
    ]
    result = []
    for key, slug, label in signal_keys:
        sig = social_conv.get(key, {})
        score = sig.get("score", 0)
        result.append({
            "signal":      label,
            "slug":        slug,
            "score":       score,
            "evidence":    sig.get("evidence", ""),
            "plain_english": sig.get("plain_english", ""),
            "impact":      "positive" if score >= 55 else "negative",
            "weight":      "high" if score < 30 or score >= 75 else "medium",
        })
    return result


def _infer_why_now(business: dict, context: dict, social_conv: dict) -> str:
    prefs = _signal_prefs(context)
    parts = []
    if any("low social engagement" in p or "weak instagram" in p for p in prefs):
        parts.append("the business's social engagement is currently weak, creating an opening before a competitor fills it")
    if any("no website" in p or "bad website" in p for p in prefs):
        parts.append("they have no effective web presence, so any professional website work will have outsized impact now")
    if any("no booking" in p for p in prefs):
        parts.append("they're losing bookings daily by having no online reservation system")
    if not parts:
        parts.append("local businesses that don't invest in digital presence now will fall further behind competitors who do")
    return "This is a good time to reach out because " + " and ".join(parts) + "."


def _collect_risk_flags(business: dict, saturation: dict | None, social_conv: dict) -> list[str]:
    flags = []
    if (saturation or {}).get("total_contacted", 0) >= 5:
        flags.append("High outreach volume — this business has been contacted frequently and may be fatigued.")
    if (saturation or {}).get("total_won", 0) >= 1:
        flags.append("This angle has already converted once — a service provider may already be in place.")
    rating = _rating(business)
    if 0 < rating < 3.5:
        flags.append("Low review rating — approach with care and avoid leading with reputation management angle without context.")
    if not _has_social(business) and not _has_website(business):
        flags.append("No online presence detected — data quality is low. Verify the business is still active before outreach.")
    return flags


def _next_best_action(contact: dict, service_angle: str, social_conv: dict) -> str:
    path = contact.get("best_contact_path", "unknown")
    angle_phrase = service_angle.replace("_", " ")
    if path == "instagram_dm":
        return f"Send a personalised Instagram DM referencing a specific post. Lead with the {angle_phrase} value you can offer."
    if path == "phone_call":
        return f"Call during business hours. Ask for the owner or manager. Lead with a specific {angle_phrase} observation."
    if path in ("website_contact_form", "email"):
        return f"Email with a specific subject line. Reference a real gap you noticed. Propose one concrete {angle_phrase} deliverable."
    if path == "linkedin_message":
        return f"Connect on LinkedIn with a brief note. Mention one specific {angle_phrase} opportunity you spotted."
    return f"Research the business to find the best contact path, then lead with a specific {angle_phrase} angle."


# ---------------------------------------------------------------------------
# compute_opportunity_score
# ---------------------------------------------------------------------------

def compute_opportunity_score(
    signals:    dict,
    saturation: dict | None = None,
    context:    dict | None = None,
) -> dict:
    """
    Score = signal_strength × reliability × freshness × (1 – saturation_penalty)
    Scaled 0–100. No single signal contributes > 40% of signal_strength.
    Missing data is 'unknown', not automatically bad.
    Google reviews do not dominate social-first candidates.
    """
    if not signals:
        return {"final_score": 0, "score_components": {}, "score_label": "low", "explanation": "No signal data available."}
    if not saturation:
        saturation = {}
    if not context:
        context = {}

    user_role = (context.get("user_role") or "").lower()
    _social_first = any(r in user_role for r in ("content creator", "photographer", "videographer", "influencer"))

    # Signal strength — weighted average with 40% cap per signal
    raw_scores = {
        "attention":   (signals.get("attention_signal") or {}).get("score", 50),
        "intent":      (signals.get("intent_signal") or {}).get("score", 50),
        "trust":       (signals.get("trust_signal") or {}).get("score", 50),
        "funnel":      (signals.get("funnel_clarity_signal") or {}).get("score", 50),
        "conversion":  (signals.get("conversion_path_signal") or {}).get("score", 50),
        "content_fit": (signals.get("content_market_fit_signal") or {}).get("score", 50),
        "platform":    (signals.get("platform_fit_signal") or {}).get("score", 50),
    }

    # Base weights
    weights = {
        "attention":   0.15,
        "intent":      0.20,
        "trust":       0.20,
        "funnel":      0.15,
        "conversion":  0.15,
        "content_fit": 0.10,
        "platform":    0.05,
    }

    # For social-first roles, shift weight away from intent (Google-heavy)
    if _social_first:
        weights["intent"]      = 0.10
        weights["attention"]   = 0.25
        weights["platform"]    = 0.15
        weights["content_fit"] = 0.15
        weights["conversion"]  = 0.10
        weights["trust"]       = 0.15
        weights["funnel"]      = 0.10

    # Normalise weights
    total_w = sum(weights.values())
    weights = {k: v / total_w for k, v in weights.items()}

    # 40% cap: scale any weight above 0.40 down
    for k in weights:
        if weights[k] > 0.40:
            excess = weights[k] - 0.40
            weights[k] = 0.40
            # distribute excess equally across the others
            others = [o for o in weights if o != k]
            for o in others:
                weights[o] += excess / len(others)

    signal_strength = sum(raw_scores[k] * weights[k] for k in raw_scores) / 100.0  # 0–1

    # Reliability — based on data availability
    has_web    = raw_scores["intent"] > 40
    has_social = raw_scores["attention"] > 30
    data_points = sum([has_web, has_social, raw_scores["trust"] > 0])
    reliability = min(1.0, 0.4 + data_points * 0.2)

    # Freshness — assume fresh unless explicitly told otherwise
    freshness = 0.90

    # Saturation penalty
    total_saves    = saturation.get("total_saves", 0)
    total_contacted = saturation.get("total_contacted", 0)
    total_won      = saturation.get("total_won", 0)

    sat_raw = (
        min(total_saves, 6) * 0.05 +
        min(total_contacted, 5) * 0.06 +
        min(total_won, 3) * 0.10
    )
    sat_penalty = min(sat_raw, 0.50)  # cap at 50% reduction

    raw_final = signal_strength * reliability * freshness * (1.0 - sat_penalty) * 100
    final_score = _clamp(raw_final, 1, 99)

    label = "high" if final_score >= 65 else "medium" if final_score >= 40 else "low"

    # Plain English explanation
    sat_note = ""
    if sat_penalty > 0.20:
        sat_note = f" Saturation is elevated ({total_saves} saves, {total_contacted} contacted) — consider a differentiated angle."
    elif sat_penalty > 0.05:
        sat_note = f" Some prior outreach detected ({total_saves} saves) — verify this angle hasn't already been taken."

    explanation = (
        f"Score reflects signal strength ({int(signal_strength * 100)}/100), "
        f"data reliability ({int(reliability * 100)}%), "
        f"and a {int(sat_penalty * 100)}% saturation adjustment.{sat_note}"
    )

    return {
        "final_score": final_score,
        "score_components": {
            "signal_strength": round(signal_strength, 3),
            "reliability":     round(reliability, 3),
            "freshness":       round(freshness, 3),
            "saturation":      round(sat_penalty, 3),
        },
        "score_label":  label,
        "explanation":  explanation,
    }
