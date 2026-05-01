"""
PHASE 1–6 ADDITIONS - append these to the bottom of signal_service.py.

Functions added:
  classify_fetch_result(status_code, error_message, url) -> dict
  build_discovery_verification_signals(scout_data, analyze_data, business) -> dict
  apply_saturation_rerank(candidates, exposure_stats, context) -> list[dict]
  compute_opportunity_score_v2(signals, saturation, context) -> dict

Do NOT remove any existing functions. These are additive only.
"""

import datetime as _dt


# Re-use _clamp from signal_service if this file is appended to it.
# If tested standalone, define a local fallback:
try:
    _clamp  # already defined if appended to signal_service.py
except NameError:
    def _clamp(val: float, lo: float = 0.0, hi: float = 100.0) -> int:
        return int(max(lo, min(hi, val)))


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1 - classify_fetch_result
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
# PHASE 2 - build_discovery_verification_signals
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
                    f"This appears to be a genuine gap - the business likely has no active {asset}."
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
            "Use the discovery gaps as conversation starters - the business likely has "
            "digital assets that aren't well-connected or surfaced in local search."
        )
    elif miss_types:
        pattern_summary = f"Confirmed missing: {', '.join(m['type'].replace('true_missing_asset','') for m in mismatches if m['type']=='true_missing_asset')}. Both sources agree."
        recommended_action = "Focus outreach on the confirmed missing assets - these are the highest-value gaps."
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
# PHASE 5 - apply_saturation_rerank
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
        f"[Rerank] exposure stats applied - "
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
# PHASE 6 - compute_opportunity_score_v2
# ─────────────────────────────────────────────────────────────────────────────

def compute_opportunity_score_v2(
    signals:    dict,
    saturation: dict | None = None,
    context:    dict | None = None,
) -> dict:
    """
    Phase 6 scoring model. Works with the FULL opportunity signals object
    (output of build_opportunity_signals) - not just social_conversion.

    Weighted combination of 6 dimensions:
        business_quality   - trust + intent (are they established?)
        opportunity_gap    - role-matched weakness = your value (high gap = high score)
        user_fit           - platform alignment with user's service angle
        contactability     - can we actually reach this business?
        saturation         - how many others have already tried?
        confidence         - how complete is the data?

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

    # Pull social_conversion sub-dict - handle both full-signals and bare sc dicts
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

    # ── 2. Opportunity gap (0–1) - GAP IS THE OPPORTUNITY ────────────────────
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
