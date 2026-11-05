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

Identity without a place ID: records keep identity_key, a one-way hash of
the normalized name and location, so the same business can be matched
again without storing its name. (Whether a hash of a Google-provided name
is acceptable is part of the open review.)
"""
import hashlib
import re

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


def identity_key(name, location) -> str:
    """One-way key for matching a business without keeping its name."""
    norm = lambda s: re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()
    n, loc = norm(name), norm(location)
    if not n:
        return ""
    return "ik1:" + hashlib.sha256(f"{n}|{loc}".encode("utf-8")).hexdigest()[:32]


def business_fields(data: dict) -> dict:
    """
    The businesses-table fields a write may keep, per the source rules.
    Keeps Yelhao's own fields (category, location from the search, source,
    platform) and allowed identifiers; drops provider content and text built
    from it. Name becomes "" (the column is NOT NULL) unless the source may
    keep names. raw_data keeps platform, website_checked and identity_key.
    """
    src = data.get("source") or (data.get("raw_data") or {}).get("platform") or ""
    rule = rule_for(src)
    raw_in = data.get("raw_data") or {}
    allowed = set(rule["content"])
    out = {
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
            "identity_key":    identity_key(data.get("name"), data.get("location")),
        }.items() if v not in (None, "")},
    }
    return out


def reason_keys(score_reasons) -> list:
    """Score reasons without their text (keys, impact, weight only)."""
    out = []
    for r in score_reasons or []:
        if isinstance(r, dict):
            out.append({k: r.get(k) for k in ("key", "impact", "weight") if r.get(k) is not None})
    return out
