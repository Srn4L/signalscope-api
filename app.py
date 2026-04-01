"""
SignalScope Backend — /agent endpoint
=====================================
Add this to your existing Flask app on signalscope-api.onrender.com

If you're starting fresh, this is a complete Flask app.

Install:
  pip install flask flask-cors requests beautifulsoup4 openai anthropic duckduckgo-search

Env vars on Render:
  OPENAI_API_KEY
  ANTHROPIC_API_KEY
"""

import os
import re
import json
import time
import uuid
import textwrap
from threading import Thread
from collections import defaultdict
from flask import Flask, request, jsonify
from flask_cors import CORS

# ─── RATE LIMITING ────────────────────────────────────────────────────────────
# Simple in-memory log — resets on dyno restart (fine for now)
REQUEST_LOG  = defaultdict(list)  # ip → [timestamps]
VALIDATE_LOG = defaultdict(list)  # ip → [timestamps]

RATE_LIMIT_VALIDATE = 20   # max /validate attempts per IP per hour

# ─── RESPONSE CACHE ───────────────────────────────────────────────────────────
# Caches full agent responses by business name + location for 24 hours.
# Repeat analyses on the same business return instantly and cost nothing.
CACHE     = {}          # key → (response_data, timestamp)
CACHE_TTL = 86400       # 24 hours in seconds

def cache_key(business_name, website_url, location):
    """Build a consistent cache key."""
    base = f"{business_name.lower().strip()}:{location.lower().strip()}:{website_url.lower().strip()}"
    return re.sub(r'[^a-z0-9:.]', '_', base)[:120]

def get_cached(key):
    """Return cached response if still fresh, else None."""
    if key in CACHE:
        data, ts = CACHE[key]
        if time.time() - ts < CACHE_TTL:
            return data
        del CACHE[key]
    return None

def set_cache(key, data):
    """Store response in cache."""
    CACHE[key] = (data, time.time())

# ─── BACKGROUND JOBS ──────────────────────────────────────────────────────────
# Stores deep pipeline results keyed by job_id
# Resets on restart — fine for now
JOBS = {}  # job_id → {"status": "running|done|error", "result": ..., "error": ...}
JOBS_TTL = 600  # clean up finished jobs after 10 minutes

def cleanup_jobs():
    """Remove finished/errored jobs older than JOBS_TTL to prevent memory drift."""
    now = time.time()
    stale = [k for k, v in JOBS.items()
             if v.get("status") in ("done","error") and now - v.get("ts", now) > JOBS_TTL]
    for k in stale:
        del JOBS[k]

RATE_LIMIT_AGENT = 10  # requests per hour per user (master is exempt)

def check_rate_limit(log, key, limit, window=3600):
    """Return True if under limit, False if exceeded. key = ip or ip:token."""
    now = time.time()
    log[key] = [t for t in log[key] if now - t < window]
    if len(log[key]) >= limit:
        return False
    log[key].append(now)
    return True

# Import your existing agent logic
# If adding to existing app, just paste the /agent route below
import requests as req
from bs4 import BeautifulSoup
from openai import OpenAI
from anthropic import Anthropic
from ddgs import DDGS
from urllib.parse import urlparse

app = Flask(__name__)
CORS(app)

OPENAI_KEY    = os.environ.get("OPENAI_API_KEY",    "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ─── ACCESS CONTROL ──────────────────────────────────────────────────────────
#
# Two env vars on Render:
#
#   MASTER_CODE   — YOUR permanent code. Never expires. Set it once, never share it.
#                   You will never be asked to re-enter it (stored in your browser).
#                   e.g. "stanley_master_2024"
#
#   GUEST_CODES   — Comma-separated codes you give to specific people.
#                   They must enter it every session. Revoke anytime by removing
#                   it from this list and saving the env var (no redeploy needed).
#                   e.g. "recruiter_abc,client_xyz,demo_user1"
#
# Anyone without a valid code gets a 401.

_master = os.environ.get("MASTER_CODE", "stanley_master").strip().lower()
_guests = os.environ.get("GUEST_CODES", "").strip()

MASTER_CODE  = _master
GUEST_CODES  = {c.strip().lower() for c in _guests.split(",") if c.strip()}
ALL_CODES    = GUEST_CODES | {MASTER_CODE}

# Guest session expiry — how long a guest code stays valid after first use
GUEST_SESSION_HOURS = int(os.environ.get("GUEST_SESSION_HOURS", "4"))

# Tracks when each guest code was first validated: code → timestamp
# Resets on Render restart (which is fine — forces re-entry)
GUEST_SESSIONS = {}  # code → first_validated_timestamp


def get_code_type(req) -> str | None:
    """
    Read token ONLY from X-Agent-Token header.
    Returns "master", "guest", or None if invalid/missing/expired.
    """
    token = req.headers.get("X-Agent-Token", "").strip().lower()
    if not token:
        return None
    if token == MASTER_CODE:
        return "master"
    if token in GUEST_CODES:
        # Check if guest session has expired
        now = time.time()
        first_used = GUEST_SESSIONS.get(token)
        if first_used is None:
            # First time using this code — start the clock
            GUEST_SESSIONS[token] = now
            return "guest"
        if now - first_used > GUEST_SESSION_HOURS * 3600:
            # Session expired — remove so they need a fresh code from you
            del GUEST_SESSIONS[token]
            return None
        return "guest"
    return None

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

# ─── CLIENTS ──────────────────────────────────────────────────────────────────

gpt_client    = None
claude_client = None

def init_clients():
    global gpt_client, claude_client
    if OPENAI_KEY and not gpt_client:
        gpt_client = OpenAI(api_key=OPENAI_KEY)
    if ANTHROPIC_KEY and not claude_client:
        claude_client = Anthropic(api_key=ANTHROPIC_KEY)

# ─── MINIMAL INLINE VERSIONS (replace with full versions from agent_v2.py) ──

SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "")

def safe_get(url, timeout=10):
    """
    Fetch a URL safely. Uses ScraperAPI if key is configured (handles
    blocked sites, Cloudflare, JS-rendered pages). Falls back to direct
    requests if no key is set or ScraperAPI fails.
    """
    # ── ScraperAPI (handles blocked sites) ───────────────────────────────────
    if SCRAPER_API_KEY:
        try:
            proxy_url = (
                f"http://api.scraperapi.com"
                f"?api_key={SCRAPER_API_KEY}"
                f"&url={req.utils.quote(url, safe='')}"
                f"&render=false"  # set True for JS-heavy sites (costs 5 credits)
            )
            r = req.get(proxy_url, timeout=timeout)
            r.raise_for_status()
            return r.text, r.status_code
        except Exception as e:
            # Fall through to direct request if ScraperAPI fails
            pass

    # ── Direct request fallback ───────────────────────────────────────────────
    try:
        r = req.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        return r.text, r.status_code
    except Exception as e:
        return None, str(e)

def extract_text(html, max_chars=4000):
    if not html: return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script","style","nav","footer","noscript"]): tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    return re.sub(r"\s+", " ", text)[:max_chars]

def find_social_links(html):
    if not html: return {}
    soup = BeautifulSoup(html, "html.parser")
    found = {}
    PATTERNS = {
        "instagram": re.compile(r"instagram\.com/(?!p/|explore|reel)[A-Za-z0-9_.]+", re.I),
        "tiktok":    re.compile(r"tiktok\.com/@[A-Za-z0-9_.]+", re.I),
        "twitter":   re.compile(r"(?:twitter|x)\.com/(?!share|intent)[A-Za-z0-9_]+", re.I),
        "linkedin":  re.compile(r"linkedin\.com/(?:company|in)/[A-Za-z0-9_-]+", re.I),
        "facebook":  re.compile(r"facebook\.com/(?!sharer|share)[A-Za-z0-9_.]+", re.I),
        "youtube":   re.compile(r"youtube\.com/(?:channel|@|c/)[A-Za-z0-9_-]+", re.I),
    }
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        for platform, pat in PATTERNS.items():
            if platform not in found and pat.search(href):
                found[platform] = href if href.startswith("http") else "https://" + pat.search(href).group(0)
    return found

def search_web(query, max_results=6):
    snippets = []
    try:
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=max_results):
                snippets.append({"title": r.get("title",""), "url": r.get("href",""), "body": r.get("body","")})
    except: pass
    return snippets


# ── BOOKING PLATFORM INTELLIGENCE ────────────────────────────────────────────

BOOKING_PLATFORMS = {
    "booksy":   {"domain": "booksy.com",    "search": "booksy.com/en-US/s/"},
    "fresha":   {"domain": "fresha.com",    "search": "fresha.com"},
    "styleseat":{"domain": "styleseat.com", "search": "styleseat.com"},
    "vagaro":   {"domain": "vagaro.com",    "search": "vagaro.com"},
    "mindbody": {"domain": "mindbody.io",   "search": "mindbodyonline.com"},
    "yelp":     {"domain": "yelp.com",      "search": "yelp.com/biz"},
}

SERVICE_KEYWORDS = [
    "haircut","blowout","color","highlights","balayage","keratin","extensions",
    "braids","locs","twists","box braids","knotless","cornrows","natural hair",
    "lashes","eyelash extensions","microblading","brow lamination","wax",
    "facial","hydrafacial","microneedling","chemical peel","dermaplaning",
    "massage","swedish","deep tissue","hot stone","prenatal",
    "manicure","pedicure","nail art","gel","acrylic","dip powder",
    "barber","fade","lineup","beard trim","shape up",
    "botox","filler","lip flip","prp","iv therapy","weight loss",
]

def parse_booking_card(html, platform, url):
    """Extract pricing, rating, services from a booking platform page."""
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True).lower()
    raw_text = re.sub(r"\s+", " ", text)

    # Business name — try title tag first
    name = ""
    title_tag = soup.find("title")
    if title_tag:
        name = title_tag.get_text().strip().split("|")[0].split("-")[0].strip()

    # Pricing — find dollar amounts with context
    price_pattern = re.compile(r'\$\s*(\d+(?:\.\d{2})?)\s*(?:[-–]\s*\$\s*(\d+(?:\.\d{2})?))?')
    prices_found = []
    for m in price_pattern.finditer(raw_text):
        lo = float(m.group(1))
        hi = float(m.group(2)) if m.group(2) else lo
        if 5 <= lo <= 2000:
            prices_found.append((lo, hi, m.start()))

    # Get surrounding context for top prices
    pricing_signals = []
    for lo, hi, pos in prices_found[:8]:
        context = raw_text[max(0, pos-40):pos+40].strip()
        label = f"${lo:.0f}" if lo == hi else f"${lo:.0f}–${hi:.0f}"
        pricing_signals.append({"price": label, "context": context})

    # Average price
    avg_price = None
    if prices_found:
        all_vals = [(lo+hi)/2 for lo,hi,_ in prices_found]
        avg_price = round(sum(all_vals) / len(all_vals))

    # Rating
    rating = None
    rating_patterns = [
        re.compile(r'(\d\.\d)\s*(?:out of\s*5|stars?|\()', re.I),
        re.compile(r'rated\s+(\d\.\d)', re.I),
        re.compile(r'(\d\.\d)\s*/\s*5', re.I),
    ]
    for pat in rating_patterns:
        m = pat.search(raw_text)
        if m:
            val = float(m.group(1))
            if 1.0 <= val <= 5.0:
                rating = val
                break

    # Review count
    review_count = None
    rev_m = re.search(r'(\d+(?:,\d+)?)\s*(?:reviews?|ratings?)', raw_text)
    if rev_m:
        review_count = rev_m.group(1).replace(",", "")

    # Services detected
    services = list(set(kw for kw in SERVICE_KEYWORDS if kw in raw_text))[:10]

    # Pricing position
    if avg_price:
        if avg_price < 50:   pricing_pos = "budget"
        elif avg_price < 150: pricing_pos = "mid"
        else:                 pricing_pos = "premium"
    else:
        pricing_pos = "unknown"

    return {
        "name":            name,
        "platform":        platform,
        "url":             url,
        "rating":          rating,
        "review_count":    review_count,
        "avg_price":       avg_price,
        "pricing_position": pricing_pos,
        "pricing_signals": pricing_signals[:5],
        "services":        services,
    }


def scrape_booking_competitors(business_name, industry, location, limit=5):
    """
    Search all 6 booking platforms for local competitors.
    Returns a list of competitor cards with pricing, ratings, and services.
    """
    print(f"  → Booking platforms: searching {industry or business_name} in {location}...")

    service_hint = industry or business_name
    cards = []
    seen_names = set()

    for platform, cfg in BOOKING_PLATFORMS.items():
        domain = cfg["domain"]

        # Search for this service type + location on this platform
        queries = [
            f'site:{domain} "{service_hint}" "{location}"',
            f'site:{domain} {service_hint} {location}',
        ]

        for query in queries:
            results = search_web(query, max_results=4)
            for r in results:
                url = r.get("url", "")
                if domain not in url:
                    continue

                # Skip if it looks like the business itself
                biz_slug = re.sub(r'[^a-z0-9]', '', business_name.lower())
                url_lower = url.lower()
                if biz_slug[:6] in url_lower:
                    continue

                # Fetch the page
                html, status = safe_get(url)
                if not html or not isinstance(status, int) or status >= 400:
                    # Use snippet as fallback
                    snippet = r.get("body", "")
                    if snippet and len(snippet) > 50:
                        # Extract what we can from the snippet
                        prices = re.findall(r'\$[\d,]+', snippet)
                        rating_m = re.search(r'(\d\.\d)\s*(?:stars?|rating)', snippet, re.I)
                        cards.append({
                            "name":             r.get("title","").split("|")[0].split("-")[0].strip(),
                            "platform":         platform,
                            "url":              url,
                            "rating":           float(rating_m.group(1)) if rating_m else None,
                            "review_count":     None,
                            "avg_price":        None,
                            "pricing_position": "unknown",
                            "pricing_signals":  [{"price": p, "context": ""} for p in prices[:3]],
                            "services":         [],
                            "source":           "snippet",
                        })
                    continue

                card = parse_booking_card(html, platform, url)
                if not card:
                    continue

                # Deduplicate by name
                name_key = re.sub(r'[^a-z0-9]', '', card["name"].lower())[:12]
                if name_key and name_key in seen_names:
                    continue
                if name_key:
                    seen_names.add(name_key)

                card["source"] = "scraped"
                cards.append(card)

                if len(cards) >= limit:
                    break

            if len(cards) >= limit:
                break

        time.sleep(0.3)  # be polite between platforms

    print(f"  ✓ Booking intelligence: {len(cards)} competitor cards from {len(set(c['platform'] for c in cards))} platforms")
    return cards[:limit]


def format_booking_intelligence(cards):
    """Format booking cards into a rich text block for the AI pipeline."""
    if not cards:
        return ""

    lines = ["=== BOOKING PLATFORM COMPETITOR INTELLIGENCE ===\n"]
    for i, c in enumerate(cards, 1):
        lines.append(f"Competitor {i}: {c['name']} [{c['platform'].upper()}]")
        lines.append(f"  URL: {c['url']}")
        if c.get("rating"):
            rev = f" ({c['review_count']} reviews)" if c.get("review_count") else ""
            lines.append(f"  Rating: {c['rating']}★{rev}")
        if c.get("avg_price"):
            lines.append(f"  Avg Price: ${c['avg_price']} ({c['pricing_position']})")
        if c.get("pricing_signals"):
            for ps in c["pricing_signals"][:3]:
                lines.append(f"  Price Signal: {ps['price']} — {ps['context'][:60]}")
        if c.get("services"):
            lines.append(f"  Services: {', '.join(c['services'][:6])}")
        lines.append("")

    # Market summary
    rated = [c for c in cards if c.get("rating")]
    priced = [c for c in cards if c.get("avg_price")]
    if rated:
        avg_rating = round(sum(c["rating"] for c in rated) / len(rated), 1)
        lines.append(f"Market Avg Rating: {avg_rating}★ across {len(rated)} competitors")
    if priced:
        avg_price = round(sum(c["avg_price"] for c in priced) / len(priced))
        lines.append(f"Market Avg Price: ${avg_price}")
        positions = [c["pricing_position"] for c in priced]
        dominant = max(set(positions), key=positions.count)
        lines.append(f"Dominant Pricing Tier: {dominant}")

    return "\n".join(lines)


# ── TREND INTELLIGENCE ────────────────────────────────────────────────────────

NICHE_KEYWORDS = {
    "hair":       ["hairstyle trend", "hair color trend", "haircut trend", "hair tutorial"],
    "braids":     ["box braids", "knotless braids", "braids trend", "protective styles"],
    "lash":       ["lash extensions trend", "lash styles", "eyelash trend", "lash tech"],
    "nails":      ["nail art trend", "nail design", "manicure trend", "nail tutorial"],
    "barber":     ["barber fade trend", "haircut trend", "taper fade", "lineup"],
    "skincare":   ["skincare trend", "facial treatment trend", "glow skin routine"],
    "spa":        ["spa treatment trend", "massage trend", "wellness trend"],
    "medspa":     ["botox trend", "filler trend", "aesthetic treatment trend"],
    "brow":       ["brow lamination trend", "microblading trend", "eyebrow trend"],
    "makeup":     ["makeup trend", "beauty trend", "makeup tutorial"],
    "fitness":    ["workout trend", "fitness routine trend", "gym trend"],
    "tax":        ["tax tips small business", "tax strategy", "small business tax"],
    "restaurant": ["food trend", "restaurant marketing", "menu trend"],
    "retail":     ["retail trend", "product trend", "ecommerce trend"],
}

def detect_niche_keywords(industry, business_name, website_pages):
    combined = (industry + " " + business_name + " " + " ".join(list(website_pages.values())[:2])).lower()
    matched = []
    for niche, keywords in NICHE_KEYWORDS.items():
        if niche in combined:
            matched.extend(keywords)
    if not matched:
        words = re.findall(r'\b\w+\b', industry.lower()) if industry else []
        matched = [f"{w} trend" for w in words[:2]] + [f"{business_name} trend"]
    return list(set(matched))[:5]


def scrape_tiktok_trends(keyword):
    trends = []
    for q in [f'site:tiktok.com "{keyword}" trending', f'"{keyword}" tiktok viral 2025 2026']:
        results = search_web(q, max_results=4)
        for r in results:
            body = r.get("body","")
            title = r.get("title","")
            if not body: continue
            views_m = re.search(r'([\d.]+[KkMmBb])\s*(?:views?|likes?|plays?)', body)
            hashtags = re.findall(r'#\w+', body + title)
            trends.append({
                "title": title[:80], "snippet": body[:200],
                "views": views_m.group(0) if views_m else None,
                "hashtags": hashtags[:4], "source": "tiktok", "keyword": keyword,
            })
        if len(trends) >= 3: break
    return trends[:4]


def scrape_instagram_trends(keyword):
    trends = []
    results = search_web(f'site:instagram.com "{keyword}" trending', 4)
    for r in results:
        body = r.get("body","")
        if not body: continue
        hashtags = re.findall(r'#\w+', body + r.get("title",""))
        trends.append({
            "title": r.get("title","")[:80], "snippet": body[:200],
            "hashtags": hashtags[:4], "source": "instagram", "keyword": keyword,
        })
    return trends[:3]


def scrape_reddit_trends(keyword):
    trends = []
    results = search_web(f'site:reddit.com {keyword} 2025 OR 2026', 4)
    for r in results:
        body = r.get("body","")
        if body and len(body) > 50:
            trends.append({"title": r.get("title","")[:80], "snippet": body[:200], "source": "reddit"})
    return trends[:3]


def fetch_trend_intelligence(business_name, industry, location, website_pages):
    print(f"  → Trend intelligence for niche: {industry or business_name}")
    keywords = detect_niche_keywords(industry, business_name, website_pages)
    if not keywords:
        return None
    print(f"  → Trend keywords: {keywords[:3]}")
    all_trends = []
    for keyword in keywords[:2]:
        all_trends.extend(scrape_tiktok_trends(keyword))
        all_trends.extend(scrape_instagram_trends(keyword))
        time.sleep(0.3)
    reddit = scrape_reddit_trends(keywords[0] if keywords else industry)
    result = {
        "keywords_analyzed": keywords[:3],
        "tiktok_trends":     [t for t in all_trends if t.get("source") == "tiktok"][:5],
        "instagram_trends":  [t for t in all_trends if t.get("source") == "instagram"][:4],
        "reddit_signals":    reddit,
        "top_hashtags":      list(set(h for t in all_trends for h in t.get("hashtags",[]) if len(h) > 3))[:10],
    }
    print(f"  ✓ Trends: {len(result['tiktok_trends'])} TikTok, {len(result['instagram_trends'])} Instagram, {len(result['reddit_signals'])} Reddit")
    return result


def format_trend_intelligence(trends):
    if not trends: return ""
    lines = ["=== TREND INTELLIGENCE — WHAT'S HOT RIGHT NOW ===\n"]
    lines.append(f"Keywords: {', '.join(trends.get('keywords_analyzed',[]))}\n")
    if trends.get("tiktok_trends"):
        lines.append("TIKTOK TRENDING:")
        for t in trends["tiktok_trends"][:4]:
            views = f" [{t['views']}]" if t.get("views") else ""
            lines.append(f"  • {t['title']}{views}")
            if t.get("snippet"): lines.append(f"    {t['snippet'][:120]}")
            tags = " ".join(t.get("hashtags",[])[:3])
            if tags: lines.append(f"    Tags: {tags}")
    if trends.get("instagram_trends"):
        lines.append("\nINSTAGRAM TRENDING:")
        for t in trends["instagram_trends"][:3]:
            lines.append(f"  • {t['title']}")
            if t.get("snippet"): lines.append(f"    {t['snippet'][:120]}")
    if trends.get("reddit_signals"):
        lines.append("\nCONSUMER DEMAND (Reddit):")
        for t in trends["reddit_signals"][:3]:
            lines.append(f"  • {t['title']}: {t['snippet'][:100]}")
    if trends.get("top_hashtags"):
        lines.append(f"\nTOP HASHTAGS: {' '.join(trends['top_hashtags'][:8])}")
    return "\n".join(lines)


def compute_complexity(website_pages, social_text, review_text, competitors):
    score = 0
    total = sum(len(v) for v in website_pages.values())
    if total > 3000: score += 2
    elif total > 1000: score += 1
    if len(social_text) >= 2: score += 2
    elif len(social_text) == 1: score += 1
    if len(review_text) > 500: score += 2
    elif len(review_text) > 100: score += 1
    if len(competitors) >= 3: score += 2
    elif len(competitors) >= 1: score += 1
    if len(website_pages) >= 3: score += 1
    return min(score, 10)

def route_task(complexity):
    if complexity >= 5: return "multi_model"
    elif complexity >= 3: return "claude_only"
    return "gpt_only"

def build_routing_reasons(website_pages, social_text, review_text, competitors, data_quality):
    reasons = []
    reasons.append(f"{len(website_pages)} website page{'s' if len(website_pages) != 1 else ''} scraped")
    if social_text:
        reasons.append(f"{len(social_text)} social platform{'s' if len(social_text) != 1 else ''} detected: {', '.join(social_text.keys())}")
    else:
        reasons.append("No accessible social pages found")
    if competitors:
        reasons.append(f"{len(competitors)} competitors identified")
    review_chars = len(review_text)
    if review_chars > 500:
        reasons.append(f"Strong review data ({review_chars} chars)")
    elif review_chars > 0:
        reasons.append(f"Limited review data ({review_chars} chars)")
    prices = re.findall(r"\$[\d,]+", " ".join(website_pages.values()))
    if prices:
        reasons.append(f"Pricing detected: {', '.join(set(prices[:4]))}")
    return reasons

def build_data_quality(website_pages, social_text, review_text, competitors):
    web_pct = min(100, len(website_pages) * 25)
    web_level = "high" if web_pct >= 75 else "medium" if web_pct >= 40 else "low"

    soc_pct = min(100, len(social_text) * 35)
    soc_level = "high" if soc_pct >= 70 else "medium" if soc_pct >= 35 else "low"

    rev_len = len(review_text)
    rev_pct = min(100, int(rev_len / 30))
    rev_level = "high" if rev_pct >= 70 else "medium" if rev_pct >= 35 else "low"

    prices = re.findall(r"\$[\d,]+", " ".join(website_pages.values()))
    pr_pct = min(100, len(prices) * 20 + 20)
    pr_level = "high" if pr_pct >= 70 else "medium" if pr_pct >= 40 else "low"

    comp_pct = min(100, len(competitors) * 20)
    comp_level = "high" if comp_pct >= 60 else "medium" if comp_pct >= 30 else "low"

    return {
        "website":     {"pct": web_pct,  "level": web_level,  "detail": f"{len(website_pages)} pages scraped"},
        "social":      {"pct": soc_pct,  "level": soc_level,  "detail": f"{', '.join(social_text.keys()) or 'none detected'}"},
        "reviews":     {"pct": rev_pct,  "level": rev_level,  "detail": f"{rev_len} chars"},
        "pricing":     {"pct": pr_pct,   "level": pr_level,   "detail": f"{len(prices)} price signals"},
        "competitors": {"pct": comp_pct, "level": comp_level, "detail": f"{len(competitors)} found"},
    }

def run_fast_pipeline(business_name, location, website_pages, social_text):
    """
    Fast GPT-only pass using just homepage + social signals.
    Returns a basic report in 3-8 seconds.
    Used as the immediate response while deep analysis runs in background.
    """
    import textwrap
    raw = {
        "website_pages": {k: v[:1200] for k, v in list(website_pages.items())[:2]},
        "social_text":   {k: v[:800]  for k, v in list(social_text.items())[:2]},
    }
    prompt = textwrap.dedent(f"""
    You are a marketing analyst. Generate a SignalScope report for {business_name} ({location}).
    Use the data provided. Extract every signal you can — brand voice, pricing, services, platforms.
    Be specific. Even if data is limited, give real observations not generic advice.

    DATA: {json.dumps(raw)[:4000]}

    Return ONLY valid JSON — fill every field with real analysis:
    {{
      "company": "{business_name}",
      "industry": "string",
      "overall_score": 0,
      "key_insight": "string",
      "scores": {{
        "content_consistency": {{"score": 0, "confidence": "low"}},
        "engagement_quality":  {{"score": 0, "confidence": "low"}},
        "content_diversity":   {{"score": 0, "confidence": "low"}},
        "brand_voice_clarity": {{"score": 0, "confidence": "low"}},
        "platform_coverage":   {{"score": 0, "confidence": "low"}}
      }},
      "score_reasoning": {{"content_consistency":"","engagement_quality":"","content_diversity":"","brand_voice_clarity":"","platform_coverage":""}},
      "score_evidence":  {{"content_consistency":"","engagement_quality":"","content_diversity":"","brand_voice_clarity":"","platform_coverage":""}},
      "signal_map_data": {{"platforms_present":[],"platforms_absent":[],"content_types":[],"tone_signals":[],"video_presence":false,"ugc_signals":false,"posting_frequency":"unknown"}},
      "overview": "string",
      "strengths": ["","",""],
      "weaknesses": ["","",""],
      "content_patterns": {{"themes":[],"caption_structure":""}},
      "social_strategy": "string",
      "experiments": [
        {{"experiment":"","signal":"","hypothesis":"","metric":"","timeframe":"14 days","success_threshold":"","affects":"","impact":5,"effort":5,"confidence":4}},
        {{"experiment":"","signal":"","hypothesis":"","metric":"","timeframe":"21 days","success_threshold":"","affects":"","impact":5,"effort":5,"confidence":4}},
        {{"experiment":"","signal":"","hypothesis":"","metric":"","timeframe":"14 days","success_threshold":"","affects":"","impact":5,"effort":5,"confidence":4}},
        {{"experiment":"","signal":"","hypothesis":"","metric":"","timeframe":"30 days","success_threshold":"","affects":"","impact":5,"effort":5,"confidence":4}}
      ],
      "strategy_blueprint": {{"pillars":["","","",""],"posting_mix":[{{"type":"Content","pct":35}},{{"type":"Educational","pct":25}},{{"type":"Engagement","pct":25}},{{"type":"Promotional","pct":15}}],"channel_recommendations":["","",""]}},
      "platform_scores": {{}},
      "competitive_signals": {{"pricing_position":"unknown","pricing_notes":"Competitor analysis running...","main_competitors":[],"competitive_advantage":"","market_gaps":""}},
      "review_signals": {{"sentiment":"unknown","review_themes":[],"reputation_notes":""}},
      "signal_confidence": "Low",
      "coverage_notes": "Initial fast analysis — full intelligence report loading in background.",
      "cover_letter_snippet": "string",
      "ai_methodology_note": "Fast GPT pass — full multi-model analysis with competitor + trend intelligence loading."
    }}
    Fill ALL fields with real analysis based on data provided.
    """)
    r = gpt_client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.2,
        max_tokens=2000,
    )
    return json.loads(r.choices[0].message.content.strip())


def run_deep_job(job_id, business_name, location, website_pages, social_text,
                 review_text, pricing_text, competitors, mode,
                 booking_cards, trend_data, ck):
    """
    Runs in a background thread. Executes the full pipeline and stores result in JOBS.
    Also updates the cache when done.
    """
    try:
        JOBS[job_id] = {"status": "running"}
        report = run_full_pipeline(
            business_name, location,
            website_pages, social_text,
            review_text, pricing_text,
            competitors, mode,
        )
        report["company"] = business_name
        report["_pipeline_mode"]    = mode
        report["_complexity_score"] = compute_complexity(website_pages, social_text, review_text, competitors)

        data_quality    = build_data_quality(website_pages, social_text, review_text, competitors)
        routing_reasons = build_routing_reasons(website_pages, social_text, review_text, competitors, data_quality)

        agent_brain = {
            "complexity_score":    compute_complexity(website_pages, social_text, review_text, competitors),
            "pipeline_mode":       mode,
            "routing_reasons":     routing_reasons,
            "data_quality":        data_quality,
            "booking_competitors": booking_cards,
            "trend_intelligence":  trend_data,
        }

        response_data = {"report": report, "agent_brain": agent_brain}
        set_cache(ck, response_data)
        if booking_cards: set_cache(ck + '_booking', booking_cards)
        if trend_data:    set_cache(ck + '_trends',  trend_data)
        JOBS[job_id] = {"status": "done", "result": response_data, "ts": time.time()}
        print(f"  ✓ Deep job {job_id} complete")

    except Exception as e:
        print(f"  ✗ Deep job {job_id} failed: {e}")
        JOBS[job_id] = {"status": "error", "error": str(e)}


def run_full_pipeline(business_name, location, website_pages, social_text,
                      review_text, pricing_text, competitors, mode):
    """
    Three-stage multi-model pipeline:
      multi_model  → GPT extract → Claude analyze → GPT validate+format
      claude_only  → Claude direct analysis
      gpt_only     → GPT single pass (thin data)
    """
    import textwrap

    raw_data = {
        "website_pages": website_pages,
        "social_text":   social_text,
        "review_text":   review_text,
        "pricing_text":  pricing_text,
        "competitors":   competitors,
    }

    # ── MULTI-MODEL: GPT extract → Claude analyze → GPT validate ─────────────
    if mode == "multi_model":

        # Stage 1 — GPT: structured signal extraction
        extract_prompt = textwrap.dedent(f"""
        You are a data extraction specialist. Extract structured marketing signals
        from the raw scraped data below. Return ONLY valid JSON — no markdown, no explanation.

        RAW DATA:
        Website: {json.dumps(website_pages)[:3000]}
        Social: {json.dumps(social_text)[:2000]}
        Reviews: {review_text[:1500]}
        Pricing: {pricing_text[:1000]}
        Competitors: {json.dumps(competitors)[:1500]}

        Extract and return:
        {{
          "value_proposition": "Core offer in 1-2 sentences",
          "pricing": {{
            "detected_prices": ["$X", "$Y"],
            "pricing_model": "hourly|flat-rate|subscription|unknown",
            "position": "budget|mid|premium|unknown"
          }},
          "social_presence": {{
            "platforms_found": ["list"],
            "platforms_missing": ["list"],
            "estimated_activity": "low|moderate|high|unknown",
            "video_detected": false,
            "ugc_detected": false
          }},
          "brand_voice": {{
            "tone": ["descriptor1", "descriptor2"],
            "key_phrases": ["phrase from site"],
            "personality": "1-sentence description"
          }},
          "content_signals": {{
            "themes": ["theme1", "theme2"],
            "content_types_found": ["product", "educational", "UGC"],
            "content_gaps": ["missing type1", "missing type2"]
          }},
          "review_signals": {{
            "overall_sentiment": "positive|mixed|negative|unknown",
            "recurring_positives": ["what customers love"],
            "recurring_negatives": ["what customers complain about"],
            "star_rating_hint": "X stars or unknown"
          }},
          "competitive_landscape": {{
            "main_competitors": ["domain1", "domain2"],
            "apparent_differentiators": ["what sets this business apart"],
            "competitor_strengths": ["what competitors do better"],
            "market_gaps": ["opportunity not being captured"]
          }},
          "data_quality": {{
            "website_richness": "low|medium|high",
            "social_richness": "low|medium|high",
            "review_richness": "low|medium|high",
            "overall_confidence": "low|medium|high"
          }}
        }}
        """)

        r1 = gpt_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": extract_prompt}],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=1500,
        )
        structured = json.loads(r1.choices[0].message.content.strip())

        # Stage 2 — Claude: deep strategic analysis
        claude_prompt = textwrap.dedent(f"""
        You are a senior growth strategist and brand intelligence analyst with deep expertise
        in digital marketing for small businesses.

        You have been given structured marketing signals for: **{business_name}** ({location}).

        STRUCTURED DATA:
        {json.dumps(structured, indent=2)}

        Produce a comprehensive strategic analysis covering:

        1. OVERALL SIGNAL SCORE (0-100) — be honest, not generous
        2. SCORES (0-100 each, with confidence: low/medium/high and specific evidence):
           - content_consistency
           - engagement_quality
           - content_diversity
           - brand_voice_clarity
           - platform_coverage
        3. KEY INSIGHT — single most actionable finding from this data
        4. BRAND OVERVIEW — 2-3 sentences, data-driven, no fluff
        5. TOP 3 STRENGTHS — specific, evidence-backed
        6. TOP 3 WEAKNESSES — specific, evidence-backed, honest
        7. CONTENT PATTERNS — themes, caption structure, posting frequency
        8. SOCIAL STRATEGY ASSESSMENT — 3-4 sentences on current strategy and gaps
        9. COMPETITIVE POSITION — pricing position, key gaps vs competitors
        10. GROWTH EXPERIMENTS (exactly 4):
            - What to test (specific)
            - Which signal triggered it
            - Falsifiable hypothesis with numeric prediction
            - Metric, timeframe (days), Impact/Effort/Confidence (1-10 each)
        11. STRATEGY BLUEPRINT — 4 content pillars, posting mix (%), channel priorities
        12. PLATFORM SCORES — 0-100 for each detected and relevant missing platform
        13. REPUTATION ANALYSIS — sentiment, themes, risks
        14. SIGNAL COVERAGE NOTES — transparent about thin or missing data

        Be specific. Ground everything in actual data. Do NOT give generic advice.
        Every insight must trace back to a specific signal.
        """)

        r2 = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=3000,
            messages=[{"role": "user", "content": claude_prompt}],
        )
        claude_output = r2.content[0].text

        # Stage 3 — GPT: fact-check Claude + format final JSON
        schema = {
            "company": business_name,
            "industry": "string",
            "overall_score": "int 0-100",
            "key_insight": "string",
            "scores": {
                "content_consistency": {"score": "int", "confidence": "low|medium|high"},
                "engagement_quality":  {"score": "int", "confidence": "low|medium|high"},
                "content_diversity":   {"score": "int", "confidence": "low|medium|high"},
                "brand_voice_clarity": {"score": "int", "confidence": "low|medium|high"},
                "platform_coverage":   {"score": "int", "confidence": "low|medium|high"},
            },
            "score_reasoning":    {"content_consistency":"str","engagement_quality":"str","content_diversity":"str","brand_voice_clarity":"str","platform_coverage":"str"},
            "score_evidence":     {"content_consistency":"str","engagement_quality":"str","content_diversity":"str","brand_voice_clarity":"str","platform_coverage":"str"},
            "signal_map_data":    {"platforms_present":["list"],"platforms_absent":["list"],"content_types":["list"],"tone_signals":["list"],"video_presence":"bool","ugc_signals":"bool","posting_frequency":"str"},
            "overview": "str",
            "strengths": ["str","str","str"],
            "weaknesses": ["str","str","str"],
            "content_patterns":   {"themes":["list"],"caption_structure":"str"},
            "social_strategy":    "str",
            "experiments": [{"experiment":"str","signal":"str","hypothesis":"str","metric":"str","timeframe":"str","success_threshold":"str","affects":"str","impact":"int","effort":"int","confidence":"int"}],
            "strategy_blueprint": {"pillars":["str","str","str","str"],"posting_mix":[{"type":"str","pct":"int"}],"channel_recommendations":["str","str","str"]},
            "platform_scores":    {"PlatformName": "int 0-100"},
            "competitive_signals":{"pricing_position":"budget|mid|premium|unknown","pricing_notes":"str","main_competitors":["list"],"competitive_advantage":"str","market_gaps":"str"},
            "review_signals":     {"sentiment":"positive|mixed|negative|unknown","review_themes":["list"],"reputation_notes":"str"},
            "signal_confidence":  "Low|Medium|High",
            "coverage_notes":     "str",
            "cover_letter_snippet": "str",
            "ai_methodology_note": "str — mention 3-stage pipeline: GPT extraction + Claude analysis + GPT validation",
        }

        format_prompt = textwrap.dedent(f"""
        You are a JSON formatter and fact-checker for a marketing intelligence platform.

        TASK:
        1. Read Claude's strategic analysis below
        2. Cross-check every claim against the structured signal data
        3. Correct any claims not supported by the data
        4. If Claude was too generous on scores, adjust them down
        5. Convert everything into the exact SignalScope JSON schema

        CLAUDE'S ANALYSIS:
        {claude_output}

        STRUCTURED SIGNAL DATA (ground truth):
        {json.dumps(structured, indent=2)[:2000]}

        REQUIRED JSON SCHEMA:
        {json.dumps(schema, indent=2)}

        Rules:
        - Return ONLY valid JSON — no markdown, no backticks, no explanation
        - company field must be exactly: "{business_name}"
        - Generate exactly 4 experiments
        - posting_mix percentages must sum to 100
        - ai_methodology_note must mention the 3-stage pipeline
        - Replace unsupported claims with "Signal not detected — insufficient data"
        - Be honest about data gaps in coverage_notes
        """)

        r3 = gpt_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": format_prompt}],
            response_format={"type": "json_object"},
            temperature=0.15,
            max_tokens=3500,
        )
        return json.loads(r3.choices[0].message.content.strip())

    # ── CLAUDE ONLY: direct analysis pass ────────────────────────────────────
    elif mode == "claude_only":
        prompt = textwrap.dedent(f"""
        You are a senior marketing strategist. Analyze {business_name} ({location}).
        Return ONLY valid JSON — no markdown, no backticks.

        DATA:
        {json.dumps(raw_data)[:5000]}

        Return JSON with these exact keys:
        company ("{business_name}"), industry, overall_score, key_insight,
        scores (5 dims each with score+confidence), score_reasoning, score_evidence,
        signal_map_data (platforms_present, platforms_absent, content_types, tone_signals,
        video_presence, ugc_signals, posting_frequency), overview, strengths[3], weaknesses[3],
        content_patterns (themes, caption_structure), social_strategy,
        experiments[4] (experiment, signal, hypothesis, metric, timeframe, success_threshold,
        affects, impact, effort, confidence), strategy_blueprint (pillars[4], posting_mix[4],
        channel_recommendations[3]), platform_scores, competitive_signals
        (pricing_position, pricing_notes, main_competitors, competitive_advantage, market_gaps),
        review_signals (sentiment, review_themes, reputation_notes),
        signal_confidence, coverage_notes, cover_letter_snippet, ai_methodology_note.

        Fill ALL fields with real analysis. Generate exactly 4 specific experiments.
        """)

        r = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=3500,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = r.content[0].text.strip()
        raw = re.sub(r"^```json\s*", "", raw)
        raw = re.sub(r"```$", "", raw).strip()
        return json.loads(raw)

    # ── GPT ONLY: single fast pass (thin data) ────────────────────────────────
    else:
        prompt = textwrap.dedent(f"""
        You are a marketing analyst. Generate a SignalScope report for {business_name} ({location}).
        Be honest when signals are weak. Use whatever data is available.

        DATA: {json.dumps(raw_data)[:4000]}

        Return ONLY valid JSON with these keys:
        company ("{business_name}"), industry, overall_score, key_insight,
        scores (5 dims each with score+confidence), score_reasoning, score_evidence,
        signal_map_data, overview, strengths[3], weaknesses[3], content_patterns,
        social_strategy, experiments[4], strategy_blueprint, platform_scores,
        competitive_signals, review_signals, signal_confidence, coverage_notes,
        cover_letter_snippet, ai_methodology_note.

        Generate all 4 experiments specific to this business. Set signal_confidence to
        "Low" if data was thin. Be honest in coverage_notes.
        """)

        r = gpt_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.2,
            max_tokens=3000,
        )
        return json.loads(r.choices[0].message.content.strip())


@app.route('/job/<job_id>', methods=['GET'])
def job_status(job_id):
    """Poll for deep analysis completion."""
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"status": "not_found"}), 404
    return jsonify(job)


# ─── /agent ENDPOINT ──────────────────────────────────────────────────────────

@app.route('/agent', methods=['POST'])
def agent():
    if len(JOBS) > 100: cleanup_jobs()  # cap size + remove stale jobs
    # ── Auth ──────────────────────────────────────────────────────────────────
    code_type = get_code_type(request)
    if code_type is None:
        return jsonify({"error": "Access denied. Valid access code required."}), 401

    if code_type == "guest":
        ip    = request.remote_addr
        token = request.headers.get("X-Agent-Token", "").strip().lower()
        if not check_rate_limit(REQUEST_LOG, f"{ip}:{token}", RATE_LIMIT_AGENT):
            return jsonify({"error": "Rate limit exceeded — 10 analyses per hour."}), 429

    init_clients()
    if not gpt_client or not claude_client:
        return jsonify({"error": "API keys not configured on backend"}), 500

    body = request.get_json(force=True, silent=True) or {}
    if isinstance(body, str):
        try: body = json.loads(body)
        except: body = {}

    business_name = body.get('business_name', '')
    website_url   = body.get('website', '')
    location      = body.get('location', '')
    industry      = body.get('industry', '')
    user_content  = body.get('user_content', '')

    if not business_name:
        return jsonify({"error": "business_name is required"}), 400

    manual_socials = {
        k: body.get(k) for k in ['instagram','tiktok','facebook','twitter','linkedin']
        if body.get(k)
    }

    if not website_url and not manual_socials:
        return jsonify({"error": "Provide at least a website URL or one social profile"}), 400

    # ── Cache check ───────────────────────────────────────────────────────────
    ck = cache_key(business_name, website_url, location)
    cached = get_cached(ck)
    if cached and not user_content:
        cached["_from_cache"] = True
        return jsonify(cached)

    # ── Scrape website ────────────────────────────────────────────────────────
    website_pages = {}
    social_links  = manual_socials.copy()

    if website_url:
        if not website_url.startswith("http"):
            website_url = "https://" + website_url
        html, _ = safe_get(website_url)
        if html:
            website_pages["homepage"] = extract_text(html, 1500)
            detected = find_social_links(html)
            for k, v in detected.items():
                if k not in social_links:
                    social_links[k] = v
        # Only scrape 2 subpages to keep it fast
        for slug in ["/about"]:
            h, code = safe_get(website_url.rstrip("/") + slug)
            if h and isinstance(code, int) and code < 400:
                text = extract_text(h, 1500)
                if text: website_pages[slug.lstrip("/")] = text
    else:
        results = search_web(f'"{business_name}" {location} official website', 3)
        for r in results:
            u = r.get("url", "")
            skip = ["yelp","google","facebook","instagram","tiktok","twitter","linkedin","bing"]
            if u and not any(s in u for s in skip):
                html, _ = safe_get(u)
                if html:
                    website_pages["homepage"] = extract_text(html, 1500)
                    detected = find_social_links(html)
                    for k, v in detected.items():
                        if k not in social_links:
                            social_links[k] = v
                break

    # ── Scrape social — parallel fetches ─────────────────────────────────────
    social_text = {}
    if social_links:
        from concurrent.futures import ThreadPoolExecutor as _SPE
        def _fetch_social(item):
            platform, url = item
            h, _ = safe_get(url)
            if h:
                text = extract_text(h, 1500)
                if len(text) > 200:
                    return platform, text
            return platform, None
        with _SPE(max_workers=4) as ex:
            for platform, text in ex.map(_fetch_social, social_links.items()):
                if text:
                    social_text[platform] = text

    # User content
    if user_content:
        if user_content.startswith("http") and any(p in user_content for p in ["instagram","tiktok","twitter","linkedin","facebook"]):
            h, _ = safe_get(user_content)
            if h:
                fetched = extract_text(h, 2000)
                if len(fetched) > 200:
                    platform_hint = next((p for p in ["instagram","tiktok","twitter","linkedin","facebook"] if p in user_content), "social")
                    social_text[platform_hint] = f"[USER-PROVIDED URL]:\n{fetched}"
        else:
            social_text["user_provided"] = f"[USER-PROVIDED CONTENT — highest confidence]:\n{user_content}"

    # ── FAST RESPONSE — run GPT immediately, return to user ───────────────────
    try:
        fast_report = run_fast_pipeline(business_name, location, website_pages, social_text)
        fast_report["company"] = business_name
    except Exception as e:
        fast_report = {"company": business_name, "error": str(e), "overall_score": 0}

    # ── DEEP ANALYSIS — run in background thread ──────────────────────────────
    job_id = str(uuid.uuid4())[:8]
    JOBS[job_id] = {"status": "running"}

    def deep_work():
        try:
            pages        = dict(website_pages)
            local_social = dict(social_text)  # clone — never mutate shared state across threads

            # Collect trends + booking — use cache if available
            cached_trends  = get_cached(ck + '_trends')
            cached_booking = get_cached(ck + '_booking')
            try:
                trend_data = cached_trends or fetch_trend_intelligence(business_name, industry, location, pages)
            except: trend_data = None
            try:
                booking_cards = cached_booking or scrape_booking_competitors(business_name, industry, location, 2)
            except: booking_cards = []

            # Layer 2+3 social
            for platform, url in social_links.items():
                if platform in social_text and len(local_social.get(platform,"")) > 500:
                    continue
                handle = url.rstrip("/").split("/")[-1].lstrip("@")
                site_map = {
                    "instagram": f"site:instagram.com/{handle}",
                    "tiktok":    f"site:tiktok.com/@{handle}",
                    "twitter":   f"site:x.com/{handle}",
                }
                q = site_map.get(platform)
                if q:
                    results = search_web(q, 3)
                    snippets = [r["body"] for r in results if r.get("body") and len(r["body"]) > 50]
                    if snippets:
                        local_social[platform] = local_social.get(platform,"") + "\n" + "\n".join(snippets[:3])

            # Parallelize press + reviews + pricing searches
            from concurrent.futures import ThreadPoolExecutor as _TPE
            def _search(q): return search_web(q, 3)
            with _TPE(max_workers=3) as ex:
                f_press   = ex.submit(_search, f'"{business_name}" {location}')
                f_reviews = ex.submit(_search, f'"{business_name}" {location} reviews')
                f_pricing = ex.submit(_search, f'{business_name} pricing cost {industry}')
                press_results   = f_press.result()
                review_snippets = f_reviews.result()
                pricing_results = f_pricing.result()

            # Press mentions
            SKIP_PRESS = ["yelp.com","yellowpages.com","bbb.org","facebook.com"]
            snippets = [f"[{r.get('title','')}]: {r['body']}" for r in press_results if r.get('body') and not any(s in r.get('url','') for s in SKIP_PRESS)]
            if snippets: pages["press_mentions"] = "\n".join(snippets[:3])

            # Reviews
            rev_texts = [f"[{r['title']}]: {r['body']}" for r in review_snippets if r.get('body')]
            yelp_r = search_web(f"{business_name} {location} yelp", 2)
            for r in yelp_r:
                if "yelp.com/biz/" in r.get("url",""):
                    h, _ = safe_get(r["url"])
                    if h: rev_texts.append("[Yelp]: " + extract_text(h, 2000))
                    break
            review_text = "\n".join(rev_texts)

            # Pricing
            flat = " ".join(pages.values())
            pr = pricing_results
            pricing_text = "\n".join([f"[{r['title']}]: {r['body']}" for r in pr if r.get('body')])
            prices = re.findall(r"\$[\d,]+(?:\.\d{2})?", flat)
            if prices: pricing_text += "\n[Website prices]: " + ", ".join(set(prices[:8]))

            # Add pre-collected trend + booking intelligence
            trend_text = format_trend_intelligence(trend_data)
            if trend_text: pricing_text += "\n\n" + trend_text
            booking_intel = format_booking_intelligence(booking_cards)
            if booking_intel: pricing_text += "\n\n" + booking_intel

            # Competitors
            comp_results = search_web(f"{industry or business_name} {location}", 5)
            seen = set(urlparse(c.get("url","")).netloc.replace("www.","") for c in booking_cards if c.get("url"))
            competitors = [{"domain": urlparse(c["url"]).netloc.replace("www.",""), "title": c["name"],
                           "body": f"Platform: {c['platform']} | Rating: {c.get('rating','N/A')}★ | ${c.get('avg_price','N/A')}"} for c in booking_cards]
            SKIP = ["yelp.com","google.com","facebook.com","yellowpages.com","bbb.org","tripadvisor.com",
                   "reddit.com","booksy.com","fresha.com","styleseat.com","vagaro.com","mindbody.io"]
            for r in comp_results:
                if len(competitors) >= 6: break
                url = r.get("url","")
                try: domain = urlparse(url).netloc.replace("www.","")
                except: continue
                if not domain or domain in seen or any(s in domain for s in SKIP): continue
                seen.add(domain)
                competitors.append({"domain": domain, "title": r.get("title",""), "body": r.get("body","")[:200]})

            complexity = compute_complexity(pages, social_text, review_text, competitors)
            mode = route_task(complexity)

            run_deep_job(job_id, business_name, location, pages, local_social,
                        review_text, pricing_text, competitors, mode,
                        booking_cards, trend_data, ck)
        except Exception as e:
            JOBS[job_id] = {"status": "error", "error": str(e), "ts": time.time()}
            print(f"  ✗ Deep work failed: {e}")

    Thread(target=deep_work, daemon=True).start()

    # ── Return fast report immediately ────────────────────────────────────────
    fast_brain = {
        "complexity_score": 0,
        "pipeline_mode":    "fast",
        "routing_reasons":  [f"Fast analysis complete — enhancing with full intelligence (job: {job_id})"],
        "data_quality":     build_data_quality(website_pages, social_text, "", []),
        "booking_competitors": [],
        "trend_intelligence":  None,
    }

    return jsonify({
        "report":      fast_report,
        "agent_brain": fast_brain,
        "job_id":      job_id,
        "status":      "processing",
    })


@app.route('/validate', methods=['POST'])
def validate():
    """Check if an access code is valid. Frontend calls this before unlocking."""
    ip = request.remote_addr
    if not check_rate_limit(VALIDATE_LOG, ip, RATE_LIMIT_VALIDATE):
        return jsonify({"valid": False, "code_type": None, "error": "Too many attempts"}), 429

    time.sleep(0.5)  # Slow down brute-force attempts

    body = request.get_json() or {}
    code = body.get('code', '').strip().lower()
    code_type = "master" if code == MASTER_CODE else ("guest" if code in GUEST_CODES else None)
    if code_type:
        return jsonify({"valid": True, "code_type": code_type})
    return jsonify({"valid": False, "code_type": None})




@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "agent_version": "2.0"})


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
