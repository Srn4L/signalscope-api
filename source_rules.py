"""
Which provider content Yelhao may keep in durable records, per source.

One place for the rules, used by every durable write path: Scout journey
records (journey_service), the businesses table (upsert_business, which
every Scout run and every Networks save goes through) and Networks
opportunities saved from Scout cards.

Until a source's permitted use is established, durable records keep only
identifiers and Yelhao's own data. Google Maps Platform terms allow keeping
place IDs; other Places content (names, addresses, phone numbers,
websites, ratings, review counts, prices, hours, coordinates) and text
built from it (score reasons quoting ratings) is not kept. Geoapify
(OpenStreetMap data) is not approved yet: its terms and attribution need
checking. Booking sources come from web search results and are not
approved. A source not listed here keeps nothing.

Identity: a durable business record needs a permitted identifier (today
only a Google place ID). Records without one are not written; existing
rows keep their Yelhao IDs and relationships. Matching them automatically
waits until a permitted identifier exists.
"""
SOURCE_RULES = {
    "google_text":       {"ids": ("google_place_id",), "content": ()},
    "google_nearby":     {"ids": ("google_place_id",), "content": ()},
    "google_places":     {"ids": ("google_place_id",), "content": ()},
    "adjacent_category": {"ids": ("google_place_id",), "content": ()},
    "geoapify":          {"ids": (), "content": ()},
    "booking_platform":  {"ids": (), "content": ()},
}
NO_RULE = {"ids": (), "content": ()}
# Content fields a rule may ever allow (none are allowed today)
CONTENT_FIELDS = ("business_name", "address", "phone", "website")

GOOGLE_SOURCES = ("google_text", "google_nearby", "google_places", "adjacent_category")


def rule_for(source) -> dict:
    return SOURCE_RULES.get(source or "", NO_RULE)


def business_fields(data: dict) -> dict:
    """
    The businesses-table values a write stores, per the source rules.
    Provider content a source may not keep is returned as empty (name "",
    others None) so writes actively clear it on existing rows. Keeps Yelhao's
    own fields (category and location from the search, source, platform,
    website_checked) and allowed identifiers.
    """
    src = data.get("source") or (data.get("raw_data") or {}).get("platform") or ""
    rule = rule_for(src)
    raw_in = data.get("raw_data") or {}
    allowed = set(rule["content"])
    return {
        "name":                (data.get("name") or "") if "business_name" in allowed else "",
        "category":            data.get("category"),
        "location":            data.get("location"),
        "address":             data.get("address") if "address" in allowed else None,
        "phone":               data.get("phone") if "phone" in allowed else None,
        "website":             data.get("website") if "website" in allowed else None,
        "google_place_id":     data.get("google_place_id") if "google_place_id" in rule["ids"] else None,
        "google_rating":       None,
        "google_review_count": None,
        "instagram_url":       None,
        "facebook_url":        None,
        "source":              src or None,
        "raw_data": {k: v for k, v in {
            "platform":        raw_in.get("platform") or src or None,
            "website_checked": raw_in.get("website_checked"),
        }.items() if v not in (None, "")},
    }


def permitted_identifier(fields: dict) -> str:
    """The identifier a record may be stored under, or "" when none."""
    return (fields.get("google_place_id") or "").strip()


# Opportunity fields that hold only Yelhao's or the user's own data. Every
# other opportunity field (problem, why it fits, fit score, signals, contact
# path, target, reason, suggested message and follow-up, offers) is built
# from provider data and is not kept for sources without content rights.
OPPORTUNITY_ALLOWED = ("business_id", "search_id", "opportunity_type", "service_angle", "user_role")


def opportunity_fields(payload: dict, source) -> dict:
    """Final opportunity payload per the source rules (apply after inference)."""
    if rule_for(source)["content"]:
        return payload
    return {k: (payload.get(k) if k in OPPORTUNITY_ALLOWED else None) for k in payload}


def reason_keys(score_reasons) -> list:
    """
    Which reasons applied, by key only. Text, impact and weight are derived
    from provider data and are not kept (same decision as journey records).
    """
    return [{"key": r.get("key")} for r in score_reasons or [] if isinstance(r, dict) and r.get("key")]
