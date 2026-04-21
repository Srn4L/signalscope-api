"""
SignalScope Backend - /agent endpoint
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
import random
import uuid
import textwrap
import sys
from threading import Thread
from collections import defaultdict
from flask import Flask, request, jsonify
from flask_cors import CORS

# --- RATE LIMITING ------------------------------------------------------------
# Simple in-memory log - resets on dyno restart (fine for now)
REQUEST_LOG  = defaultdict(list)  # ip -> [timestamps]
VALIDATE_LOG = defaultdict(list)  # ip -> [timestamps]

RATE_LIMIT_VALIDATE = 20   # max /validate attempts per IP per hour

# --- RESPONSE CACHE -----------------------------------------------------------
# Caches full agent responses by business name + location for 24 hours.
# Repeat analyses on the same business return instantly and cost nothing.
CACHE     = {}          # key -> (response_data, timestamp)
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

# --- BACKGROUND JOBS ----------------------------------------------------------
# Stores deep pipeline results keyed by job_id
# Resets on restart  fine for now
JOBS = {}  # job_id -> {"status": "running|done|error", "result": ..., "error": ...}
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
from urllib.parse import urlparse, quote

app = Flask(__name__)
CORS(app)

OPENAI_KEY    = os.environ.get("OPENAI_API_KEY",    "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# --- ACCESS CONTROL ----------------------------------------------------------
#
# Two env vars on Render:
#
#   MASTER_CODE    YOUR permanent code. Never expires. Set it once, never share it.
#                   You will never be asked to re-enter it (stored in your browser).
#                   e.g. "stanley_master_2024"
#
#   GUEST_CODES    Comma-separated codes you give to specific people.
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

# Guest session expiry  how long a guest code stays valid after first use
GUEST_SESSION_HOURS = int(os.environ.get("GUEST_SESSION_HOURS", "4"))

# Tracks when each guest code was first validated: code -> timestamp
# Resets on Render restart (which is fine  forces re-entry)
GUEST_SESSIONS = {}  # code -> first_validated_timestamp


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
    return None

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

# --- CLIENTS ------------------------------------------------------------------

gpt_client    = None
claude_client = None

def init_clients():
    global gpt_client, claude_client
    if OPENAI_KEY and not gpt_client:
        gpt_client = OpenAI(api_key=OPENAI_KEY)
    if ANTHROPIC_KEY and not claude_client:
        claude_client = Anthropic(api_key=ANTHROPIC_KEY)

# --- MINIMAL INLINE VERSIONS (replace with full versions from agent_v2.py) --

SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "")

def safe_get(url, timeout=10):
    """
    Fetch a URL safely. Uses ScraperAPI if key is configured (handles
    blocked sites, Cloudflare, JS-rendered pages). Falls back to direct
    requests if no key is set or ScraperAPI fails.
    """
    # -- ScraperAPI (handles blocked sites) -----------------------------------
    if SCRAPER_API_KEY:
        try:
            proxy_url = (
                f"http://api.scraperapi.com"
                f"?api_key={SCRAPER_API_KEY}"
                f"&url={quote(url, safe='')}"
                f"&render=false"  # set True for JS-heavy sites (costs 5 credits)
            )
            r = req.get(proxy_url, timeout=timeout)
            r.raise_for_status()
            return r.text, r.status_code
        except Exception as e:
            # Fall through to direct request if ScraperAPI fails
            pass

    # -- Direct request fallback -----------------------------------------------
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

def extract_social_metadata(html):
    """Extract OG metadata from social pages - works even when full text is blocked."""
    if not html: return {}
    soup = BeautifulSoup(html, "html.parser")
    data = {}
    for prop in ["og:title","og:description","og:image","twitter:title","twitter:description"]:
        tag = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
        if tag and tag.get("content"):
            data[prop.split(":")[-1]] = tag["content"].strip()
    # Also grab the page title
    title = soup.find("title")
    if title and not data.get("title"):
        data["title"] = title.get_text().strip()[:100]
    return data

def find_social_links(html):
    """
    Aggressively find social links from:
    1. <a href> attributes (primary)
    2. Raw HTML text (catches JS-rendered or data-* embedded links)
    3. <script> tag content (catches social embeds)
    4. Meta tags (og:url, etc.)
    """
    if not html: return {}
    found = {}

    PATTERNS = {
        "instagram": re.compile(r"instagram\.com/(?!p/|explore/|reel/|stories/|accounts|legal|about|help)([A-Za-z0-9_.]{2,})", re.I),
        "tiktok":    re.compile(r"tiktok\.com/@([A-Za-z0-9_.]+)", re.I),
        "twitter":   re.compile(r"(?:twitter|x)\.com/(?!share|intent|i/|home|search|hashtag|explore)([A-Za-z0-9_]{2,})", re.I),
        "linkedin":  re.compile(r"linkedin\.com/(?:company|in)/([A-Za-z0-9_-]+)", re.I),
        "facebook":  re.compile(r"facebook\.com/(?!sharer|share|dialog|plugins|groups/discover|pages/category|events|watch|gaming|marketplace|help|policies|legal|about|ads|business)([A-Za-z0-9_.]{3,})", re.I),
        "youtube":   re.compile(r"youtube\.com/(?:channel/|@|c/)([A-Za-z0-9_-]+)", re.I),
        "yelp":      re.compile(r"yelp\.com/biz/([A-Za-z0-9_-]+)", re.I),
    }

    def _normalize(url, platform, pat):
        """Return clean full URL for a matched social link."""
        if url.startswith("http"):
            # Clean tracking params
            return url.split("?")[0].rstrip("/")
        m = pat.search(url)
        if m:
            base = {
                "instagram": "https://instagram.com/",
                "tiktok":    "https://tiktok.com/@",
                "twitter":   "https://x.com/",
                "linkedin":  "https://linkedin.com/",
                "facebook":  "https://facebook.com/",
                "youtube":   "https://youtube.com/",
                "yelp":      "https://yelp.com/biz/",
            }.get(platform, "https://")
            return base + m.group(1)
        return None

    NOISE = {
        "instagram": {"instagram", "instagrammer", "instagram.com"},
        "facebook":  {"facebook", "fb", "facebook.com"},
        "twitter":   {"twitter", "tweet", "x.com", "twitter.com"},
        "tiktok":    {"tiktok", "tiktok.com"},
        "youtube":   {"youtube", "youtube.com"},
        "linkedin":  {"linkedin", "linkedin.com"},
        "yelp":      {"yelp", "yelp.com"},
    }

    soup = BeautifulSoup(html, "html.parser")

    # Pass 1: <a href> — most reliable
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        for platform, pat in PATTERNS.items():
            if platform not in found and pat.search(href):
                url = _normalize(href, platform, pat)
                if url:
                    handle = url.rstrip("/").split("/")[-1].lstrip("@").lower()
                    if handle and handle not in NOISE.get(platform, set()) and len(handle) > 2:
                        found[platform] = url

    # Pass 2: all tag attributes (data-href, data-url, content, src)
    if len(found) < 4:
        for tag in soup.find_all(True):
            for attr in ["data-href", "data-url", "data-link", "content", "value"]:
                val = tag.get(attr, "")
                if not val: continue
                for platform, pat in PATTERNS.items():
                    if platform not in found and pat.search(val):
                        url = _normalize(val, platform, pat)
                        if url:
                            handle = url.rstrip("/").split("/")[-1].lstrip("@").lower()
                            if handle and handle not in NOISE.get(platform, set()) and len(handle) > 2:
                                found[platform] = url

    # Pass 3: raw HTML text scan (catches embedded JS social links)
    if len(found) < 3:
        raw_text = html
        for platform, pat in PATTERNS.items():
            if platform not in found:
                for m in pat.finditer(raw_text):
                    full = m.group(0)
                    handle = m.group(1) if m.lastindex else ""
                    if not handle or handle.lower() in NOISE.get(platform, set()) or len(handle) < 3:
                        continue
                    # Skip obvious false positives
                    if any(x in handle.lower() for x in ["cdn", "static", "asset", "img", "image", "logo", "icon"]):
                        continue
                    found[platform] = "https://www." + full
                    break

    return found

SERP_API_KEY = os.environ.get("SERP_API_KEY", "")

def search_web(query, max_results=6):
    """
    Primary: SerpAPI (Google results - better quality, fresher, more accurate)
    Fallback: DuckDuckGo (free, no key required)
    """
    # -- SerpAPI (Google) ------------------------------------------------------
    if SERP_API_KEY:
        try:
            import requests as _req
            r = _req.get(
                "https://serpapi.com/search",
                params={
                    "q":       query,
                    "api_key": SERP_API_KEY,
                    "num":     max_results,
                    "hl":      "en",
                    "gl":      "us",
                },
                timeout=8
            )
            data = r.json()
            snippets = []
            for result in data.get("organic_results", [])[:max_results]:
                snippets.append({
                    "title": result.get("title", ""),
                    "url":   result.get("link", ""),
                    "body":  result.get("snippet", ""),
                })
            if snippets:
                return snippets
        except Exception as e:
            print(f"  [!] SerpAPI failed, falling back to DuckDuckGo: {e}")

    # -- DuckDuckGo fallback ---------------------------------------------------
    snippets = []
    try:
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=max_results):
                snippets.append({"title": r.get("title",""), "url": r.get("href",""), "body": r.get("body","")})
    except: pass
    return snippets


# -- BOOKING PLATFORM INTELLIGENCE --------------------------------------------

BOOKING_PLATFORMS = {
    "booksy":    {"domain": "booksy.com",    "search": "booksy.com/en-US/s/"},
    "fresha":    {"domain": "fresha.com",    "search": "fresha.com"},
    "vagaro":    {"domain": "vagaro.com",    "search": "vagaro.com"},
    "yelp":      {"domain": "yelp.com",      "search": "yelp.com/biz"},
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

    # Business name - try title tag first
    name = ""
    title_tag = soup.find("title")
    if title_tag:
        name = title_tag.get_text().strip().split("|")[0].split("-")[0].strip()

    # Pricing - find dollar amounts with context
    price_pattern = re.compile(r'\$\s*(\d+(?:\.\d{2})?)\s*(?:[--]\s*\$\s*(\d+(?:\.\d{2})?))?')
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
        label = f"${lo:.0f}" if lo == hi else f"${lo:.0f}-${hi:.0f}"
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
    print(f"  -> Booking platforms: searching {industry or business_name} in {location}...")

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
            results = search_web(query, max_results=3)
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

        time.sleep(0.2)  # be polite between platforms

    print(f"  [OK] Booking intelligence: {len(cards)} competitor cards from {len(set(c['platform'] for c in cards))} platforms")
    return cards[:limit]


def format_booking_intelligence(cards):
    """Format booking cards into a rich text block for the AI pipeline."""
    if not cards:
        return ""

    lines = ["=== BOOKING PLATFORM COMPETITOR INTELLIGENCE ===\n"]
    for i, c in enumerate(cards, 1):
        lines.append(f"Competitor {i}: {c.get('name', c.get('title','Unknown'))} [{c.get('platform','').upper()}]")
        lines.append(f"  URL: {c.get('url','')}")
        if c.get("rating"):
            rev = f" ({c['review_count']} reviews)" if c.get("review_count") else ""
            lines.append(f"  Rating: {c['rating']}*{rev}")
        if c.get("avg_price"):
            lines.append(f"  Avg Price: ${c['avg_price']} ({c['pricing_position']})")
        if c.get("pricing_signals"):
            for ps in c["pricing_signals"][:3]:
                lines.append(f"  Price Signal: {ps['price']} - {ps['context'][:60]}")
        if c.get("services"):
            lines.append(f"  Services: {', '.join(c['services'][:6])}")
        lines.append("")

    # Market summary
    rated = [c for c in cards if c.get("rating")]
    priced = [c for c in cards if c.get("avg_price")]
    if rated:
        avg_rating = round(sum(c["rating"] for c in rated) / len(rated), 1)
        lines.append(f"Market Avg Rating: {avg_rating}* across {len(rated)} competitors")
    if priced:
        avg_price = round(sum(c["avg_price"] for c in priced) / len(priced))
        lines.append(f"Market Avg Price: ${avg_price}")
        positions = [c["pricing_position"] for c in priced]
        dominant = max(set(positions), key=positions.count)
        lines.append(f"Dominant Pricing Tier: {dominant}")

    return "\n".join(lines)


# -- TREND INTELLIGENCE --------------------------------------------------------

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
                "url": r.get("url", ""),
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
            "url": r.get("url", ""),
        })
    return trends[:3]


def scrape_reddit_trends(keyword):
    trends = []
    results = search_web(f'site:reddit.com {keyword} 2025 OR 2026', 4)
    for r in results:
        body = r.get("body","")
        if body and len(body) > 50:
            trends.append({
                "title": r.get("title","")[:80], "snippet": body[:200],
                "source": "reddit", "url": r.get("url", ""),
            })
    return trends[:3]


def fetch_trend_intelligence(business_name, industry, location, website_pages):
    print(f"  -> Trend intelligence for niche: {industry or business_name}")
    keywords = detect_niche_keywords(industry, business_name, website_pages)
    if not keywords:
        return None
    print(f"  -> Trend keywords: {keywords[:3]}")
    all_trends = []
    for keyword in keywords[:1]:
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
    print(f"  [OK] Trends: {len(result['tiktok_trends'])} TikTok, {len(result['instagram_trends'])} Instagram, {len(result['reddit_signals'])} Reddit")
    return result


def format_trend_intelligence(trends):
    if not trends: return ""
    lines = ["=== TREND INTELLIGENCE - WHAT'S HOT RIGHT NOW ===\n"]
    lines.append(f"Keywords: {', '.join(trends.get('keywords_analyzed',[]))}\n")
    if trends.get("tiktok_trends"):
        lines.append("TIKTOK TRENDING:")
        for t in trends["tiktok_trends"][:4]:
            views = f" [{t['views']}]" if t.get("views") else ""
            lines.append(f"  * {t['title']}{views}")
            if t.get("snippet"): lines.append(f"    {t['snippet'][:120]}")
            tags = " ".join(t.get("hashtags",[])[:3])
            if tags: lines.append(f"    Tags: {tags}")
    if trends.get("instagram_trends"):
        lines.append("\nINSTAGRAM TRENDING:")
        for t in trends["instagram_trends"][:3]:
            lines.append(f"  * {t['title']}")
            if t.get("snippet"): lines.append(f"    {t['snippet'][:120]}")
    if trends.get("reddit_signals"):
        lines.append("\nCONSUMER DEMAND (Reddit):")
        for t in trends["reddit_signals"][:3]:
            lines.append(f"  * {t['title']}: {t['snippet'][:100]}")
    if trends.get("top_hashtags"):
        lines.append(f"\nTOP HASHTAGS: {' '.join(trends['top_hashtags'][:8])}")
    return "\n".join(lines)


def compute_complexity(website_pages, social_text, review_text, competitors, social_links=None):
    score = 0
    total = sum(len(v) for v in website_pages.values())
    if total > 3000: score += 2
    elif total > 1000: score += 1
    _soc = social_links if social_links else social_text
    if len(_soc) >= 2: score += 2
    elif len(_soc) >= 1: score += 1
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

def run_fast_pipeline(business_name, location, website_pages, social_text, social_links=None):
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
    _links = social_links or {}
    _known = list(_links.keys()) or list(social_text.keys())
    _with_content = [p for p in social_text if len(social_text[p]) > 100]
    _blocked      = [p for p in _known if p not in _with_content]

    platform_hint = f"Social platforms detected: {_known}" if _known else "No social profiles provided"
    social_presence_note = ""
    if _blocked:
        social_presence_note = f"\nNOTE: Content scraping blocked for {_blocked} - treat as ACTIVE but unverified. Do NOT say 'no social presence'. State they have {_known} presence with limited visible content."
    if _with_content:
        social_presence_note += f"\nContent retrieved from: {_with_content}"
    prompt = textwrap.dedent(f"""
    You are a marketing analyst for small businesses. Be specific - never generic.
    IMPORTANT: Return ONLY valid JSON. Your entire response must be JSON.

    BUSINESS: {business_name} ({location})
    {platform_hint}{social_presence_note}

    DATA: {json.dumps(raw)[:4000]}

    STRICT RULES:
    - Reference actual data found - prices, platforms, services, review sentiment
    - Never say "post more consistently" or "expand platform presence" without specifics
    - If pricing data exists, compare it to market norms
    - If social platforms are missing, name which ones and why they matter for this niche
    - Set signal_confidence to "Low" if data was thin - be honest
    {{
      "company": "{business_name}",
      "industry": "detected industry from data",
      "overall_score": 0,
      "key_insight": "single most important finding from available data",
      "scores": {{
        "content_consistency": {{"score": 0, "confidence": "low"}},
        "engagement_quality":  {{"score": 0, "confidence": "low"}},
        "content_diversity":   {{"score": 0, "confidence": "low"}},
        "brand_voice_clarity": {{"score": 0, "confidence": "low"}},
        "platform_coverage":   {{"score": 0, "confidence": "low"}}
      }},
      "score_reasoning": {{"content_consistency":"reason","engagement_quality":"reason","content_diversity":"reason","brand_voice_clarity":"reason","platform_coverage":"reason"}},
      "score_evidence":  {{"content_consistency":"evidence","engagement_quality":"evidence","content_diversity":"evidence","brand_voice_clarity":"evidence","platform_coverage":"evidence"}},
      "signal_map_data": {{"platforms_present":[],"platforms_absent":[],"content_types":[],"tone_signals":[],"video_presence":false,"ugc_signals":false,"posting_frequency":"unknown"}},
      "overview": "2-3 sentence data-driven overview of this business",
      "strengths": ["specific strength 1","specific strength 2","specific strength 3"],
      "weaknesses": ["specific weakness 1","specific weakness 2","specific weakness 3"],
      "content_patterns": {{"themes":[],"caption_structure":"observed pattern"}},
      "social_strategy": "specific assessment of their current social approach and gaps",
      "experiments": [
        {{"experiment":"specific test","signal":"trigger","hypothesis":"if X then Y by Z%","metric":"metric","timeframe":"14 days","success_threshold":"threshold","affects":"outcome","impact":5,"effort":5,"confidence":4}},
        {{"experiment":"specific test","signal":"trigger","hypothesis":"if X then Y by Z%","metric":"metric","timeframe":"21 days","success_threshold":"threshold","affects":"outcome","impact":5,"effort":5,"confidence":4}},
        {{"experiment":"specific test","signal":"trigger","hypothesis":"if X then Y by Z%","metric":"metric","timeframe":"14 days","success_threshold":"threshold","affects":"outcome","impact":5,"effort":5,"confidence":4}},
        {{"experiment":"specific test","signal":"trigger","hypothesis":"if X then Y by Z%","metric":"metric","timeframe":"30 days","success_threshold":"threshold","affects":"outcome","impact":5,"effort":5,"confidence":4}}
      ],
      "strategy_blueprint": {{"pillars":["pillar1","pillar2","pillar3","pillar4"],"posting_mix":[{{"type":"Content","pct":35}},{{"type":"Educational","pct":25}},{{"type":"Engagement","pct":25}},{{"type":"Promotional","pct":15}}],"channel_recommendations":["rec1","rec2","rec3"]}},
      "platform_scores": {{}},
      "competitive_signals": {{"pricing_position":"unknown","pricing_notes":"Full competitor analysis loading...","main_competitors":[],"competitive_advantage":"observed advantage","market_gaps":"observed gap"}},
      "review_signals": {{"sentiment":"unknown","review_themes":[],"reputation_notes":""}},
      "signal_confidence": "Low",
      "coverage_notes": "Fast analysis from homepage data - full intelligence loading in background.",
      "cover_letter_snippet": "brief snippet about this business",
      "ai_methodology_note": "Fast GPT pass - full multi-model analysis with competitor + trend intelligence loading."
    }}
    Replace ALL placeholder text with real analysis from the data above.
    """)
    r = gpt_client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.2,
        max_tokens=2500,
    )
    return json.loads(r.choices[0].message.content.strip())


def extract_enrichment(website_pages, social_links, website_url=""):
    """
    Extract contact enrichment signals from scraped website content.
    Returns email, phone, booking platform, services found.
    """
    combined = " ".join(website_pages.values())
    enrichment = {}

    # Email extraction
    emails = re.findall(
        r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
        combined
    )
    # Filter out common non-contact emails
    skip_domains = ["sentry.io","example.com","gmail.com","wix.com","squarespace.com","wordpress.com"]
    real_emails = [e for e in emails if not any(s in e for s in skip_domains)]
    if real_emails:
        enrichment["email"] = real_emails[0]

    # Phone extraction
    phones = re.findall(
        r'(?:\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
        combined
    )
    if phones:
        enrichment["phone"] = phones[0]

    # Booking platform detection
    BOOKING_SIGNALS = {
        "acuity":     ["as.me", "acuityscheduling.com"],
        "square":     ["squareup.com", "square.site"],
        "calendly":   ["calendly.com"],
        "booksy":     ["booksy.com"],
        "fresha":     ["fresha.com"],
        "vagaro":     ["vagaro.com"],
        "mindbody":   ["mindbodyonline.com"],
        "styleseat":  ["styleseat.com"],
        "gloss_genius":["glossgenius.com"],
    }
    for platform, signals in BOOKING_SIGNALS.items():
        if any(s in combined for s in signals):
            enrichment["booking_platform"] = platform
            # Try to extract the booking URL
            for s in signals:
                match = re.search(rf'https?://[^\s"\']*{re.escape(s)}[^\s"\']*', combined)
                if match:
                    enrichment["booking_url"] = match.group(0)[:200]
            break

    # Business address
    address_m = re.search(
        r'\d+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,?\s+(?:[A-Z][a-z]+\s+)*(?:NY|NYC|New York|Bronx|Brooklyn|Manhattan|Queens)',
        combined
    )
    if address_m:
        enrichment["address"] = address_m.group(0)

    # Extract all social platforms from social_links param if passed
    # (these come from find_social_links called before extract_enrichment)
    for plat in ['instagram', 'tiktok', 'facebook', 'youtube', 'yelp', 'linkedin', 'twitter']:
        links = social_links if isinstance(social_links, dict) else {}
        if plat in links:
            enrichment[plat] = links[plat]

    return enrichment


# --- HUBSPOT INTEGRATION ------------------------------------------------------

HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
HS_BASE       = "https://api.hubapi.com"

def hs_headers():
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type":  "application/json",
    }

def hs_find_contact(email=None, domain=None):
    """Find contact by email first, fall back to website domain."""
    if not HUBSPOT_TOKEN:
        return None
    try:
        if email:
            r = req.post(f"{HS_BASE}/crm/v3/objects/contacts/search",
                headers=hs_headers(), json={
                    "filterGroups": [{"filters": [
                        {"propertyName": "email", "operator": "EQ", "value": email}
                    ]}]
                }, timeout=10)
            results = r.json().get("results", [])
            if results:
                return results[0]["id"]
        if domain:
            # Search by website domain
            clean = domain.replace("https://","").replace("http://","").replace("www.","").rstrip("/").split("/")[0]
            r = req.post(f"{HS_BASE}/crm/v3/objects/contacts/search",
                headers=hs_headers(), json={
                    "filterGroups": [{"filters": [
                        {"propertyName": "website", "operator": "CONTAINS_TOKEN", "value": clean}
                    ]}]
                }, timeout=10)
            results = r.json().get("results", [])
            if results:
                return results[0]["id"]
    except Exception:
        pass
    return None

def hs_create_contact(business_name, email, website, instagram, location, niche, phone="", booking_platform=""):
    """Create a new HubSpot contact for the business."""
    props = {
        "company":   business_name,
        "firstname": business_name,  # shows name instead of -- in HubSpot
        "website":   website or "",
        "city":      location or "",
    }
    if email:            props["email"]        = email
    if phone:            props["phone"]         = phone
    if instagram:        props["twitterhandle"] = instagram
    if booking_platform: props["jobtitle"]      = f"Uses {booking_platform.title()}"
    r = req.post(f"{HS_BASE}/crm/v3/objects/contacts",
        headers=hs_headers(), json={"properties": props}, timeout=10)
    return r.json().get("id")

def hs_update_contact(contact_id, website, instagram, location, phone="", booking_platform=""):
    """Update existing contact with latest signals."""
    props = {}
    if website:          props["website"]       = website
    if location:         props["city"]          = location
    if instagram:        props["twitterhandle"] = instagram
    if phone:            props["phone"]         = phone
    if booking_platform: props["jobtitle"]      = f"Uses {booking_platform.title()}"
    if props:
        req.patch(f"{HS_BASE}/crm/v3/objects/contacts/{contact_id}",
            headers=hs_headers(), json={"properties": props}, timeout=10)

def hs_create_deal(contact_id, business_name, priority_score):
    """Create a deal linked to the contact."""
    r = req.post(f"{HS_BASE}/crm/v3/objects/deals",
        headers=hs_headers(), json={
            "properties": {
                "dealname":  f"{business_name} - Yelhao Lead",
                "pipeline":  "default",
                "dealstage": "appointmentscheduled",
                "amount":    "",
            },
            "associations": [{
                "to":    {"id": contact_id},
                "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 3}]
            }]
        }, timeout=10)
    return r.json().get("id")

def hs_create_note(contact_id, body):
    """Attach a note to a contact using the modern CRM v3 Notes API."""
    req.post(f"{HS_BASE}/crm/v3/objects/notes",
        headers=hs_headers(), json={
            "properties": {"hs_note_body": body},
            "associations": [{
                "to":    {"id": contact_id},
                "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 202}]
            }]
        }, timeout=10)


# --- SALESFORCE INTEGRATION --------------------------------------------------

SALESFORCE_CLIENT_ID     = os.environ.get("SALESFORCE_CLIENT_ID",     "")
SALESFORCE_CLIENT_SECRET = os.environ.get("SALESFORCE_CLIENT_SECRET", "")
SALESFORCE_INSTANCE_URL  = os.environ.get("SALESFORCE_INSTANCE_URL",  "").rstrip("/")

_SF_TOKEN_CACHE = {"access_token": None, "expires_at": 0}

def sf_get_token():
    """Client Credentials Flow - get access token, cached for ~55 min."""
    if not (SALESFORCE_CLIENT_ID and SALESFORCE_CLIENT_SECRET and SALESFORCE_INSTANCE_URL):
        return None
    now = time.time()
    if _SF_TOKEN_CACHE["access_token"] and _SF_TOKEN_CACHE["expires_at"] > now:
        return _SF_TOKEN_CACHE["access_token"]
    try:
        import requests as _req
        r = _req.post(
            f"{SALESFORCE_INSTANCE_URL}/services/oauth2/token",
            data={
                "grant_type":    "client_credentials",
                "client_id":     SALESFORCE_CLIENT_ID,
                "client_secret": SALESFORCE_CLIENT_SECRET,
            },
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            _SF_TOKEN_CACHE["access_token"] = data["access_token"]
            _SF_TOKEN_CACHE["expires_at"]   = now + 3300
            return data["access_token"]
        print(f"  [!] Salesforce auth failed {r.status_code} - {r.text[:200]}", flush=True)
    except Exception as e:
        print(f"  [!] Salesforce auth exception: {e}", flush=True)
    return None


def sf_create_lead(business_name, email, website, phone, industry, location,
                   score, primary_problem, key_insight, priority_score):
    """Create a Salesforce Lead record with standard fields."""
    token = sf_get_token()
    if not token:
        return None

    last_name = business_name[:80] or "Unknown"
    city, state = "", ""
    if location and "," in location:
        parts = [p.strip() for p in location.split(",")]
        city  = parts[0][:40]
        state = parts[1][:40] if len(parts) > 1 else ""
    elif location:
        city = location[:40]

    if score and score < 40:
        rating = "Hot"
    elif score and score < 60:
        rating = "Warm"
    else:
        rating = "Cold"

    payload = {
        "FirstName":   "Contact",
        "LastName":    last_name,
        "Company":     business_name[:255],
        "Email":       email or None,
        "Phone":       phone or None,
        "Website":     website or None,
        "Industry":    (industry or "")[:40] or None,
        "City":        city or None,
        "State":       state or None,
        "Status":      "Open - Not Contacted",
        "Rating":      rating,
        "Description": f"Signal Score: {score}/100 | Priority: {priority_score:.1f}\n\nKey Insight: {key_insight}\n\nPrimary Problem: {primary_problem}"[:31000],
        "LeadSource":  "Yelhao",
    }
    payload = {k: v for k, v in payload.items() if v is not None and v != ""}

    try:
        import requests as _req
        r = _req.post(
            f"{SALESFORCE_INSTANCE_URL}/services/data/v61.0/sobjects/Lead/",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload, timeout=8,
        )
        if r.status_code in (200, 201):
            lead_id = r.json().get("id")
            print(f"  [OK] Salesforce: created Lead {lead_id} for {business_name}", flush=True)
            return lead_id
        if r.status_code == 400 and "INVALID_FIELD" in r.text:
            import re as _re
            bad = _re.search(r"No such column '([^']+)'", r.text)
            if bad and bad.group(1) in payload:
                print(f"  [!] Salesforce: dropping invalid field {bad.group(1)} and retrying", flush=True)
                payload.pop(bad.group(1))
                r = _req.post(
                    f"{SALESFORCE_INSTANCE_URL}/services/data/v61.0/sobjects/Lead/",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json=payload, timeout=8
                )
                if r.status_code in (200, 201):
                    lead_id = r.json().get("id")
                    print(f"  [OK] Salesforce: created Lead {lead_id} (fallback) for {business_name}", flush=True)
                    return lead_id
        print(f"  [!] Salesforce Lead create failed {r.status_code} - {r.text[:300]}", flush=True)
    except Exception as e:
        print(f"  [!] Salesforce Lead create exception: {e}", flush=True)
    return None


def sf_create_task(lead_id, subject, body):
    token = sf_get_token()
    if not token or not lead_id:
        return None
    try:
        import requests as _req
        r = _req.post(
            f"{SALESFORCE_INSTANCE_URL}/services/data/v61.0/sobjects/Task/",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={
                "WhoId":       lead_id,
                "Subject":     subject[:255],
                "Description": body[:32000],
                "Status":      "Not Started",
                "Priority":    "Normal",
                "ActivityDate": time.strftime("%Y-%m-%d"),
            },
            timeout=6,
        )
        if r.status_code in (200, 201):
            print(f"  [OK] Salesforce: task logged on Lead {lead_id}", flush=True)
            return r.json().get("id")
        print(f"  [!] Salesforce Task create failed {r.status_code} - {r.text[:200]}", flush=True)
    except Exception as e:
        print(f"  [!] Salesforce Task create exception: {e}", flush=True)
    return None



def sf_lead_exists(email, company_name):
    """Check if a lead already exists in Salesforce by email or company name."""
    token = sf_get_token()
    if not token or not (email or company_name):
        return None
    try:
        import urllib.parse as _up, requests as _req
        # Search by email first (most reliable)
        if email:
            query = f"SELECT Id FROM Lead WHERE Email='{email}' LIMIT 1"
            r = _req.get(
                f"{SALESFORCE_INSTANCE_URL}/services/data/v61.0/query?q={_up.quote(query)}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=5
            )
            if r.status_code == 200 and r.json().get("records"):
                lead_id = r.json()["records"][0]["Id"]
                print(f"  [!] Salesforce: lead already exists (ID: {lead_id})", flush=True)
                return lead_id
        # Fallback to company name search
        if company_name:
            query = f"SELECT Id FROM Lead WHERE Company='{company_name}' LIMIT 1"
            r = _req.get(
                f"{SALESFORCE_INSTANCE_URL}/services/data/v61.0/query?q={_up.quote(query)}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=5
            )
            if r.status_code == 200 and r.json().get("records"):
                lead_id = r.json()["records"][0]["Id"]
                print(f"  [!] Salesforce: lead exists for {company_name} (ID: {lead_id})", flush=True)
                return lead_id
    except Exception as e:
        print(f"  [!] Salesforce dedup check failed: {e}", flush=True)
    return None


def push_to_salesforce(report, website_pages, social_links, website_url=""):
    """
    Pushes analyzed lead into Salesforce as a Lead record + outreach Task.
    Silent fail - never blocks the report. Returns lead_id.
    """
    if not (SALESFORCE_CLIENT_ID and SALESFORCE_CLIENT_SECRET and SALESFORCE_INSTANCE_URL):
        return None

    business_name = report.get("company", "")
    if not business_name:
        return None

    enrichment = extract_enrichment(website_pages, social_links or {})
    email      = enrichment.get("email", "")
    
    # Check for existing lead - prevents duplicates
    existing_lead_id = sf_lead_exists(email, business_name)
    if existing_lead_id:
        print(f"  [!] Salesforce: skipping duplicate - returning existing lead {existing_lead_id}", flush=True)
        return existing_lead_id
    phone      = enrichment.get("phone", "")
    website    = website_url or report.get("website", "") or ""

    industry        = report.get("industry", "")
    location        = report.get("location", "")
    score           = report.get("overall_score", 50)
    key_insight     = report.get("key_insight", "")
    primary_problem = report.get("primary_problem", "")
    priority_score  = 100 - score if score else 50

    lead_id = sf_create_lead(
        business_name=business_name, email=email, website=website, phone=phone,
        industry=industry, location=location, score=score,
        primary_problem=primary_problem, key_insight=key_insight,
        priority_score=priority_score,
    )

    if lead_id:
        weaknesses   = report.get("weaknesses", [])
        top_weakness = weaknesses[0] if weaknesses else "growth opportunity"
        subject      = f"Yelhao analysis: {business_name} - Signal Score {score}"
        body         = f"Auto-generated from Yelhao analysis.\n\nKey Insight:\n{key_insight}\n\nTop Weakness:\n{top_weakness}\n\nPriority Score: {priority_score:.1f}"
        sf_create_task(lead_id, subject, body)
    
    return lead_id



def hs_contact_exists(email, domain):
    """Check if a contact already exists in HubSpot by email or domain."""
    if not HUBSPOT_TOKEN:
        return None
    if not email and not domain:
        return None
    try:
        # Search by email first (most reliable)
        if email:
            r = req.post(
                "https://api.hubapi.com/crm/v3/objects/contacts/search",
                headers={"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"},
                json={"filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}]},
                timeout=5
            )
            if r.status_code == 200 and r.json().get("results"):
                contact_id = r.json()["results"][0]["id"]
                print(f"  [!] HubSpot: contact already exists (ID: {contact_id})", flush=True)
                return contact_id
        # Fallback to domain search
        if domain:
            r = req.post(
                "https://api.hubapi.com/crm/v3/objects/contacts/search",
                headers={"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"},
                json={"filterGroups": [{"filters": [{"propertyName": "hs_email_domain", "operator": "EQ", "value": domain}]}]},
                timeout=5
            )
            if r.status_code == 200 and r.json().get("results"):
                contact_id = r.json()["results"][0]["id"]
                print(f"  [!] HubSpot: contact exists for domain {domain} (ID: {contact_id})", flush=True)
                return contact_id
    except:
        pass
    return None


def push_to_hubspot(report, website_pages, social_links):
    """
    Event-driven CRM sync - triggered automatically on deep job completion.
    Creates or updates contact, creates deal, logs AI-generated note.
    Silent fails - never blocks the report from returning to the user.
    """
    if not HUBSPOT_TOKEN:
        return

    business_name = report.get("company", "")
    if not business_name:
        return

    # Run enrichment extraction
    enrichment = extract_enrichment(website_pages, social_links)
    
    # Check for existing contact - prevents duplicates
    email = enrichment.get("email", report.get("email", ""))
    website = report.get("website", "")
    domain = website.replace("http://", "").replace("https://", "").replace("www.", "").split("/")[0] if website else ""
    existing_contact_id = hs_contact_exists(email, domain)
    if existing_contact_id:
        print(f"  [!] HubSpot: skipping duplicate - returning existing contact {existing_contact_id}", flush=True)
        return existing_contact_id

    website    = report.get("website", "")
    instagram  = social_links.get("instagram", "")
    location   = report.get("location", "")
    niche      = report.get("industry", "")
    email      = enrichment.get("email", report.get("email", ""))
    phone      = enrichment.get("phone", "")
    booking_platform = enrichment.get("booking_platform", "")
    booking_url      = enrichment.get("booking_url", "")
    address          = enrichment.get("address", "")

    overall  = report.get("overall_score", 50)
    priority = max(0, 100 - overall)

    weaknesses = report.get("weaknesses", [])
    comp_sigs  = report.get("competitive_signals", {})

    note_body = f"""[Yelhao] Intelligence Report

Business: {business_name}
Location: {location}{f' | {address}' if address else ''}
Niche: {niche}
Signal Score: {overall}/100
Priority Score: {priority}/100
{f'Booking Platform: {booking_platform.title()}' if booking_platform else ''}
{f'Booking URL: {booking_url}' if booking_url else ''}
{f'Phone: {phone}' if phone else ''}

--------------------------
KEY INSIGHT:
{report.get("key_insight", "N/A")}

TOP WEAKNESS:
{weaknesses[0] if weaknesses else "N/A"}

COMPETITOR GAP:
{comp_sigs.get("market_gaps", "N/A")}

PRICING POSITION:
{comp_sigs.get("pricing_position", "unknown")} - {comp_sigs.get("pricing_notes", "")}

OUTREACH ANGLE:
{report.get("cover_letter_snippet", "N/A")}

PLATFORMS DETECTED:
{", ".join(social_links.keys()) or "None detected"}
--------------------------
Generated by Yelhao AI . yelhoa.netlify.app"""

    # Dedup: find by email -> fall back to domain
    contact_id = hs_find_contact(email=email or None, domain=website or None)

    if contact_id:
        hs_update_contact(contact_id, website, instagram, location, phone, booking_platform)
        print(f"  [OK] HubSpot: updated contact {contact_id} for {business_name}")
    else:
        contact_id = hs_create_contact(
            business_name, email, website, instagram,
            location, niche, phone, booking_platform
        )
        if not contact_id:
            print(f"  [!] HubSpot: failed to create contact for {business_name}")
            return
        print(f"  [OK] HubSpot: created contact {contact_id} for {business_name}")

    deal_id = hs_create_deal(contact_id, business_name, priority)
    print(f"  [OK] HubSpot: deal {deal_id} created (priority {priority})")

    hs_create_note(contact_id, note_body)
    print(f"  [OK] HubSpot: note logged for {business_name}")
    
    return contact_id
# --- AIRTABLE INTEGRATION -----------------------------------------------------

AIRTABLE_TOKEN    = os.environ.get("AIRTABLE_TOKEN", "")
AIRTABLE_BASE_ID  = os.environ.get("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE_ID = os.environ.get("AIRTABLE_TABLE_ID", "")

def airtable_business_exists(business_name):
    """Check if a business already exists in Airtable by name."""
    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID or not AIRTABLE_TABLE_ID:
        return False
    try:
        import urllib.parse
        formula = urllib.parse.quote(f'{{Business Name}}="{business_name}"')
        r = req.get(
            f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/Leads?filterByFormula={formula}&maxRecords=1",
            headers={"Authorization": f"Bearer {AIRTABLE_TOKEN}"},
            timeout=5,
        )
        records = r.json().get("records", [])
        return len(records) > 0
    except:
        return False


def airtable_get_record_id(business_name):
    """Get the record ID of an existing business in Airtable, or None if not found."""
    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID or not AIRTABLE_TABLE_ID:
        return None
    try:
        import urllib.parse
        formula = urllib.parse.quote(f'{{Business Name}}="{business_name}"')
        r = req.get(
            f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/Leads?filterByFormula={formula}&maxRecords=1",
            headers={"Authorization": f"Bearer {AIRTABLE_TOKEN}"},
            timeout=5,
        )
        records = r.json().get("records", [])
        if records:
            return records[0].get("id")
    except:
        pass
    return None


def airtable_update_record(record_id, fields):
    """Update an existing Airtable record with new field values."""
    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID or not AIRTABLE_TABLE_ID or not record_id:
        return False
    # Remove empty values so we don't overwrite existing data with blanks
    fields = {k: v for k, v in fields.items() if v != "" and v is not None}
    if not fields:
        return False
    try:
        r = req.patch(
            f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/Leads/{record_id}",
            headers={"Authorization": f"Bearer {AIRTABLE_TOKEN}", "Content-Type": "application/json"},
            json={"fields": fields},
            timeout=8,
        )
        if r.status_code == 200:
            print(f"  [OK] Airtable: updated record {record_id} ({len(fields)} fields)", flush=True)
            return True
        # Retry without unknown fields
        if r.status_code == 422:
            import re as _re
            bad = _re.search(r'Unknown field name: "([^"]+)"', r.text)
            if bad and bad.group(1) in fields:
                print(f"  [!] Airtable: dropping unknown field {bad.group(1)} and retrying update", flush=True)
                fields.pop(bad.group(1))
                if fields:
                    return airtable_update_record(record_id, fields)
        print(f"  [!] Airtable update failed {r.status_code} - {r.text[:200]}", flush=True)
    except Exception as e:
        print(f"  [!] Airtable update exception: {e}", flush=True)
    return False


# --- STATIC SAAS RECOMMENDATION MAPPING ---------------------------------------
WEAKNESS_TO_SAAS = {
    # Social / content
    "social": ["Later", "Buffer", "Hootsuite"],
    "instagram": ["Later", "Planoly", "Preview"],
    "tiktok": ["CapCut", "InShot", "Later"],
    "content": ["Canva", "Later", "Buffer"],
    "posting": ["Buffer", "Later", "Hootsuite"],
    
    # Booking / scheduling
    "booking": ["Calendly", "Acuity", "Square Appointments"],
    "scheduling": ["Calendly", "Acuity", "Square Appointments"],
    "appointment": ["Calendly", "Acuity", "Fresha"],
    
    # Reviews / reputation
    "review": ["Birdeye", "Podium", "NiceJob"],
    "reputation": ["Birdeye", "Podium", "Trustpilot"],
    "feedback": ["Birdeye", "Podium", "NiceJob"],
    
    # Email / marketing
    "email": ["Klaviyo", "Mailchimp", "Constant Contact"],
    "newsletter": ["Klaviyo", "Mailchimp", "Beehiiv"],
    "marketing": ["Klaviyo", "Mailchimp", "HubSpot"],
    
    # Website / SEO
    "website": ["Webflow", "Squarespace", "Wix"],
    "seo": ["Semrush", "Ahrefs", "Yoast"],
    "design": ["Canva", "Figma", "Webflow"],
    
    # CRM / sales
    "crm": ["HubSpot", "Pipedrive", "Salesforce"],
    "lead": ["HubSpot", "Pipedrive", "ActiveCampaign"],
    "customer": ["HubSpot", "Pipedrive", "Zoho"],
    
    # Analytics / tracking
    "analytics": ["Google Analytics", "Plausible", "Fathom"],
    "tracking": ["Google Analytics", "Hotjar", "Mixpanel"],
    
    # Ads / paid
    "ads": ["Google Ads", "Meta Ads", "AdRoll"],
    "paid": ["Google Ads", "Meta Ads", "TikTok Ads"],
    
    # Loyalty / retention
    "loyalty": ["Smile.io", "Yotpo", "LoyaltyLion"],
    "retention": ["Klaviyo", "Smile.io", "Yotpo"],
}

def recommend_saas(weaknesses, top_experiment=""):
    """Maps detected weaknesses to 2-3 relevant SaaS tools. Returns comma-separated string."""
    if not weaknesses:
        return ""
    
    combined_text = " ".join([str(w) for w in weaknesses]).lower() + " " + str(top_experiment).lower()
    
    # Find all matching keywords, collect unique tools
    recommended = []
    seen = set()
    for keyword, tools in WEAKNESS_TO_SAAS.items():
        if keyword in combined_text:
            for tool in tools:
                if tool not in seen:
                    recommended.append(tool)
                    seen.add(tool)
                    if len(recommended) >= 3:
                        break
            if len(recommended) >= 3:
                break
    
    return ", ".join(recommended[:3])


def push_to_airtable(report, enrichment, website_url="", location="", hubspot_contact_id=None, salesforce_lead_id=None):
    """
    Creates a new row in Airtable Leads table after deep analysis completes.
    Skips if business already exists (dedup by name).
    Silent fail - never blocks the report from returning to the user.
    """
    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID or not AIRTABLE_TABLE_ID:
        print("  [!] Airtable: env vars not set, skipping")
        return

    business_name = report.get('company', '')
    
    # Check if business already exists - if so, UPDATE instead of skip
    # This handles the case where prospect creates the row first, then deep analysis adds CRM IDs
    existing_record_id = airtable_get_record_id(business_name)

    weaknesses  = report.get("weaknesses", [])
    experiments = report.get("experiments", [])

    # Pick highest impact experiment as the top one
    top_experiment = ""
    if experiments:
        best = max(experiments, key=lambda e: e.get("impact", 0))
        top_experiment = best.get("experiment", "")

    # Consolidate all social profiles into one field for Airtable's LinkedIn URL column
    # (repurposed to hold all socials - cleaner than having one column per platform)
    social_parts = []
    for plat in ["linkedin", "instagram", "tiktok", "facebook", "youtube", "yelp", "twitter"]:
        url = enrichment.get(plat, "")
        if url:
            social_parts.append(f"{plat.capitalize()}: {url}")
    socials_combined = "\n".join(social_parts)

    fields = {
        "Business Name":    report.get("company", ""),
        "Website":          website_url,
        "Industry":         report.get("industry", ""),
        "Location":         location,
        "Score":            report.get("overall_score", 0),
        "Owner Email":      enrichment.get("email", ""),
        "Phone":            enrichment.get("phone", ""),
        "Social Profiles":  socials_combined,
        "Key Insight":      report.get("key_insight", ""),
        "Key Weakness":     weaknesses[0] if weaknesses else "",
        "Top Experiment":   top_experiment,
        "Outreach Status":  "Not Started",
        "Pipeline Stage":  "New Lead",
        "Recommended SaaS": recommend_saas(weaknesses, top_experiment),
        "Hubspot Contact ID": hubspot_contact_id or "",
        "Salesforce Lead ID": salesforce_lead_id or "",
    }
    # Remove empty string fields so Airtable doesn't reject them
    fields = {k: v for k, v in fields.items() if v != "" and v is not None}

    # If record already exists, UPDATE it with the new fields (especially CRM IDs)
    if existing_record_id:
        print(f"  -> Airtable: updating existing record for {business_name} with CRM IDs", flush=True)
        airtable_update_record(existing_record_id, fields)
        return

    # Otherwise, create a new row
    # Resilient push: retry dropping fields Airtable doesn't recognize
    def _airtable_post(payload_fields):
        return req.post(
            f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/Leads",
            headers={
                "Authorization": f"Bearer {AIRTABLE_TOKEN}",
                "Content-Type":  "application/json",
            },
            json={"fields": payload_fields},
            timeout=5,
        )

    try:
        r = _airtable_post(fields)
        # If Airtable rejects an unknown field, drop it and retry
        attempts = 0
        while r.status_code == 422 and "UNKNOWN_FIELD_NAME" in r.text and attempts < 5:
            import re as _re
            m = _re.search(r'Unknown field name:\s*"([^"]+)"', r.text)
            if not m: break
            bad_field = m.group(1)
            if bad_field not in fields: break
            print(f"  [!] Airtable: dropping unknown field '{bad_field}' and retrying", flush=True)
            fields.pop(bad_field, None)
            attempts += 1
            r = _airtable_post(fields)

        if r.status_code == 200:
            print(f"  [OK] Airtable: row created for {report.get('company')} ({len(fields)} fields)", flush=True)
        else:
            print(f"  [!] Airtable: failed {r.status_code} - {r.text[:200]}", flush=True)
    except Exception as e:
        print(f"  [!] Airtable: exception - {e}")
def run_deep_job(job_id, business_name, location, website_pages, social_text,
                 review_text, pricing_text, competitors, mode,
                 booking_cards, trend_data, ck, social_links=None, manual_email=''):
    """
    Runs in a background thread. Executes the full pipeline and stores result in JOBS.
    Also updates the cache when done.
    """
    try:
        JOBS[job_id]["step"] = "Analyzing signals with AI..."
        report = run_full_pipeline(
            business_name, location,
            website_pages, social_text,
            review_text, pricing_text,
            competitors, mode,
            booking_cards=booking_cards,
            trend_data=trend_data,
            social_links=social_links,
        )
        report["company"] = business_name
        report["_pipeline_mode"]    = mode
        report["_complexity_score"] = compute_complexity(website_pages, social_text, review_text, competitors, social_links)

        data_quality    = build_data_quality(website_pages, social_text, review_text, competitors)
        routing_reasons = build_routing_reasons(website_pages, social_text, review_text, competitors, data_quality)

        agent_brain = {
            "complexity_score":    compute_complexity(website_pages, social_text, review_text, competitors, social_links),
            "pipeline_mode":       mode,
            "routing_reasons":     routing_reasons,
            "data_quality":        data_quality,
            "booking_competitors": booking_cards,
            "trend_intelligence":  trend_data,
        }

        # Attach metrics_meta to report for confidence layer
        dq = agent_brain.get("data_quality", {})
        conf = confidence_from_data_quality(dq)
        report["metrics_confidence"] = conf
        report["metrics_source"]     = "industry_estimate"

        response_data = {"report": report, "agent_brain": agent_brain}
        set_cache(ck, response_data)
        if booking_cards: set_cache(ck + '_booking', booking_cards)
        if trend_data:    set_cache(ck + '_trends',  trend_data)
        JOBS[job_id] = {"status": "done", "result": response_data, "ts": time.time()}
        print(f"  [OK] Deep job {job_id} complete")

        # -- Push to HubSpot (silent fail - never block the report) ------------
        hubspot_contact_id = None
        try:
            hubspot_contact_id = push_to_hubspot(report, website_pages, social_links or {})
        except Exception as hs_err:
            print(f"  [!] HubSpot sync failed (non-critical): {hs_err}")

        # -- Push to Salesforce (silent fail - never block the report) ---------
        salesforce_lead_id = None
        try:
            salesforce_lead_id = push_to_salesforce(report, website_pages, social_links or {}, website_url=report.get("website", ""))
        except Exception as sf_err:
            print(f"  [!] Salesforce sync failed (non-critical): {sf_err}")

        # -- Push to Airtable ----------------------------------------------
        print("  -> Attempting Airtable push...")
        try:
            enrichment = extract_enrichment(website_pages, social_links or {})
            # Manual email entered by user takes priority over auto-scraped email
            if manual_email and not enrichment.get('email'):
                enrichment['email'] = manual_email
            push_to_airtable(report, enrichment, website_url=report.get("website",""), location=report.get("location",""),
                           hubspot_contact_id=hubspot_contact_id, salesforce_lead_id=salesforce_lead_id)
        except Exception as at_err:
            print(f"  [!] Airtable sync failed (non-critical): {at_err}")
    except Exception as e:
        print(f"  [X] Deep job {job_id} failed: {e}")
        JOBS[job_id] = {"status": "error", "error": str(e), "ts": time.time()}


def run_full_pipeline(business_name, location, website_pages, social_text,
                      review_text, pricing_text, competitors, mode,
                      booking_cards=None, trend_data=None, social_links=None):
    """
    Three-stage multi-model pipeline:
      multi_model  -> GPT extract -> Claude analyze -> GPT validate+format
      claude_only  -> Claude direct analysis
      gpt_only     -> GPT single pass (thin data)
    """
    import textwrap

    raw_data = {
        "website_pages": website_pages,
        "social_text":   social_text,
        "social_platforms_detected": list((social_links or {}).keys()),
        "social_content_available":  [p for p in social_text if len(social_text.get(p,"")) > 100],
        "review_text":   review_text,
        "pricing_text":  pricing_text,
        "competitors":   competitors,
    }

    # -- MULTI-MODEL: GPT extract -> Claude analyze -> GPT validate -------------
    if mode == "multi_model":

        # Stage 1 - GPT: structured signal extraction
        extract_prompt = textwrap.dedent(f"""
        You are a data extraction specialist. Extract structured marketing signals
        from the raw scraped data below. Return ONLY valid JSON - no markdown, no explanation.

        RAW DATA:
        Website: {json.dumps(website_pages)[:3000]}
        Social Platforms Detected: {list((social_links or {}).keys())}
        Social Content: {json.dumps(social_text)[:2000]}
        Reviews: {review_text[:1500]}
        Pricing: {pricing_text[:4000]}
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

        # Stage 2 - Claude: deep strategic analysis
        # pricing_text already contains formatted booking + trend intelligence
        # Build social state for Claude
        _s_detected = list((social_links or {}).keys())
        _s_content  = [p for p in social_text if len(social_text.get(p,"")) > 100]
        _s_blocked  = [p for p in _s_detected if p not in _s_content]

        claude_prompt = textwrap.dedent(f"""
        You are a senior growth strategist for small businesses. You have access to REAL market data.

        BUSINESS: {business_name} ({location})

        EXTRACTED SIGNALS:
        {json.dumps(structured, indent=2)}

        SOCIAL PLATFORM STATE (IMPORTANT - use this for all social reasoning):
        Platforms detected: {_s_detected}
        Platforms with scraped content: {_s_content}
        Platforms blocked (active but unverified): {_s_blocked}
        Rule: If a platform is "detected", treat it as ACTIVE even if content is limited.
        Never say "no social presence" if platforms are detected.

        STRUCTURED BOOKING COMPETITOR DATA (real pricing + ratings from Booksy/Fresha/Yelp/StyleSeat):
        {json.dumps(booking_cards[:3], indent=2)}

        STRUCTURED TREND DATA (live TikTok/Instagram/Reddit signals):
        {json.dumps(trend_data, indent=2)[:1500] if trend_data else "No trend data available"}

        FULL INTELLIGENCE CONTEXT (reviews, pricing signals, web mentions):
        {pricing_text[:6000]}

        RULES - STRICT:
        - NEVER give generic advice like "post more consistently" or "expand platform presence"
        - EVERY insight must reference specific data: competitor prices, ratings, trend names, or platform signals
        - If booking competitors have avg ratings, compare this business to them explicitly
        - If competitor pricing exists, state the market average and how this business compares
        - If a trend is detected, name it specifically and say how this business can capitalize on it
        - If review data exists, cite specific themes directly
        - Scores must reflect actual data quality - be honest, not generous
        - ALWAYS compute a market baseline if competitor data is available:
          state the avg price, avg rating, and how this business compares explicitly
          e.g. "Market avg: $92, 4.6* - this business shows no pricing -> conversion gap"

        Produce strategic analysis covering:
        1. OVERALL SIGNAL SCORE (0-100) - honest
        2. SCORES (0-100, confidence low/medium/high, with specific evidence from data above):
           content_consistency, engagement_quality, content_diversity, brand_voice_clarity, platform_coverage
        3. KEY INSIGHT - single most actionable finding backed by a specific data point
        4. BRAND OVERVIEW - 2-3 sentences citing actual signals found
        5. TOP 3 STRENGTHS - each must cite a specific signal or data point
        6. TOP 3 WEAKNESSES - each must cite a specific gap or competitor advantage
        7. CONTENT PATTERNS - what themes/formats are present or absent
        8. SOCIAL STRATEGY - what they're doing and what they're missing, with specific platform context
        9. COMPETITIVE POSITION - price vs market avg, rating vs competitors, specific gaps
        10. GROWTH EXPERIMENTS (exactly 4) - each triggered by a specific signal found above,
            with falsifiable hypothesis and numeric success threshold
        11. STRATEGY BLUEPRINT - 4 pillars, posting mix %, channel priorities
        12. PLATFORM SCORES - 0-100 per platform detected or relevant
        13. REPUTATION - sentiment, specific review themes if found
        14. COVERAGE NOTES - honest about what data was thin or missing
        """)

        r2 = claude_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=3000,
            messages=[{"role": "user", "content": claude_prompt}],
        )
        claude_output = r2.content[0].text

        # Stage 3 - GPT: fact-check Claude + format final JSON
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
            "ai_methodology_note": "str - mention 3-stage pipeline: GPT extraction + Claude analysis + GPT validation",
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
        - Return ONLY valid JSON - no markdown, no backticks, no explanation
        - company field must be exactly: "{business_name}"
        - Generate exactly 4 experiments
        - posting_mix percentages must sum to 100
        - ai_methodology_note must mention the 3-stage pipeline
        - Preserve all specific insights from Claude if plausibly supported by the data
        - Only remove claims that directly contradict extracted signal data
        - Keep all competitor pricing references, trend names, and specific numbers
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

    # -- CLAUDE ONLY: direct analysis pass ------------------------------------
    elif mode == "claude_only":
        prompt = textwrap.dedent(f"""
        You are a senior marketing strategist. Analyze {business_name} ({location}).
        Return ONLY valid JSON - no markdown, no backticks.

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

    # -- GPT ONLY: single fast pass (thin data) --------------------------------
    else:
        prompt = textwrap.dedent(f"""
        You are a marketing analyst for small businesses. Be specific - never generic.

        BUSINESS: {business_name} ({location})

        DATA: {json.dumps(raw_data)[:4000]}

        STRICT RULES:
        - Reference actual data found - prices, platforms, services, review sentiment
        - Never say "post more consistently" or "expand platform presence" without specifics
        - If pricing data exists, compare it to market norms
        - If social platforms are missing, name which ones and why they matter for this niche
        - Set signal_confidence to "Low" if data was thin - be honest

        Return ONLY valid JSON with these keys:
        company ("{business_name}"), industry, overall_score, key_insight,
        scores (5 dims each with score+confidence), score_reasoning, score_evidence,
        signal_map_data, overview, strengths[3], weaknesses[3], content_patterns,
        social_strategy, experiments[4], strategy_blueprint, platform_scores,
        competitive_signals, review_signals, signal_confidence, coverage_notes,
        cover_letter_snippet, ai_methodology_note.

        Generate 4 specific experiments triggered by actual signals in the data.
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


# --- /agent ENDPOINT ----------------------------------------------------------

@app.route('/agent', methods=['POST'])
def agent():
    if len(JOBS) > 100: cleanup_jobs()  # cap size + remove stale jobs
    # -- Auth ------------------------------------------------------------------
    code_type = get_code_type(request)
    if code_type is None:
        return jsonify({"error": "Access denied. Valid access code required."}), 401

    if code_type == "guest":
        ip    = request.remote_addr
        token = request.headers.get("X-Agent-Token", "").strip().lower()
        if not check_rate_limit(REQUEST_LOG, f"{ip}:{token}", RATE_LIMIT_AGENT):
            return jsonify({"error": "Rate limit exceeded - 10 analyses per hour."}), 429

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
    manual_email  = body.get('owner_email', '').strip()

    if not business_name:
        return jsonify({"error": "business_name is required"}), 400

    manual_socials = {
        k: body.get(k) for k in ['instagram','tiktok','facebook','twitter','linkedin']
        if body.get(k)
    }

    if not website_url and not manual_socials:
        return jsonify({"error": "Provide at least a website URL or one social profile"}), 400

    # -- Cache check -----------------------------------------------------------
    ck = cache_key(business_name, website_url, location)
    cached = get_cached(ck)
    if cached and not user_content:
        cached["_from_cache"] = True
        return jsonify(cached)

    # -- Scrape website --------------------------------------------------------
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

    # -- Scrape social - parallel fetches with metadata fallback --------------
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
                # Fallback: extract OG metadata even if full text is blocked
                meta = extract_social_metadata(h)
                if meta:
                    meta_str = " | ".join(f"{k}: {v}" for k,v in meta.items() if v)
                    return platform, f"[{platform.upper()} METADATA]: {meta_str}"
            # Never discard user-provided links - record presence even if scraping fails
            if platform in manual_socials:
                return platform, f"[{platform.upper()} PRESENCE DETECTED - content blocked by platform]"
            return platform, None
        with _SPE(max_workers=3) as ex:
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
            social_text["user_provided"] = f"[USER-PROVIDED CONTENT - highest confidence]:\n{user_content}"

    # -- FAST RESPONSE - run GPT immediately, return to user -------------------
    try:
        fast_report = run_fast_pipeline(business_name, location, website_pages, social_text, social_links)
        fast_report["company"] = business_name
    except Exception as e:
        fast_report = {"company": business_name, "error": str(e), "overall_score": 0}

    # -- DEEP ANALYSIS - run in background thread ------------------------------
    job_id = str(uuid.uuid4())[:8]
    JOBS[job_id] = {"status": "running"}

    def deep_work():
        try:
            pages        = dict(website_pages)
            local_social = dict(social_text)  # clone - never mutate shared state across threads
            _social_links = social_links      # capture explicitly for closure safety

            JOBS[job_id]["step"] = "Scanning social profiles and trends..."

            # Collect trends + booking - use cache if available
            cached_trends  = get_cached(ck + '_trends')
            cached_booking = get_cached(ck + '_booking')
            try:
                JOBS[job_id]["step"] = "Fetching trend intelligence..."
                trend_data = cached_trends or fetch_trend_intelligence(business_name, industry, location, pages)
            except: trend_data = None
            try:
                JOBS[job_id]["step"] = "Scraping competitor pricing..."
                booking_cards = cached_booking or scrape_booking_competitors(business_name, industry, location, 3)
            except: booking_cards = []

            # --- Nearby competitor intelligence via Google Places ---------------
            try:
                JOBS[job_id]["step"] = "Finding nearby competitors..."
                _address = pages.get("_meta", "")  # may have rating hint
                _site_address = enrichment_address if 'enrichment_address' in dir() else None
                _lat, _lng = geocode_business(business_name, _site_address, location)
                if _lat and _lng:
                    _radius = get_niche_radius(industry)
                    nearby_cards = nearby_competitors_google(_lat, _lng, industry, _radius, limit=8)
                    nearby_intel  = format_nearby_competitors(nearby_cards)
                else:
                    nearby_cards  = []
                    nearby_intel  = ""
            except Exception as _ne:
                print(f"  [!] Nearby search failed: {_ne}")
                nearby_cards = []
                nearby_intel = ""

            # Layer 2+3 social - DuckDuckGo cached posts + metadata
            for platform, url in _social_links.items():
                if platform in local_social and len(local_social.get(platform,"")) > 200:
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
                        existing = local_social.get(platform, "")
                        local_social[platform] = existing + "\n[Cached posts via search]:\n" + "\n".join(snippets[:3])
                    elif platform not in local_social:
                        # Still record presence - platform exists even if no cached content
                        local_social[platform] = f"[{platform.upper()} PRESENCE - {url}] No cached posts found. Platform blocks scraping."

            # Get Google Places rating for this specific business for review context
            try:
                _gp_results = search_google_places(business_name, location, limit=1)
                gp_rating  = _gp_results[0].get('rating', 0) if _gp_results else 0
                gp_reviews = _gp_results[0].get('review_count', 0) if _gp_results else 0
            except:
                gp_rating = 0
                gp_reviews = 0

            JOBS[job_id]["step"] = "Gathering reviews and press mentions..."
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

            # Reviews - multi-source with fallback
            rev_texts = [f"[{r['title']}]: {r['body']}" for r in review_snippets if r.get('body') and len(r.get('body','')) > 80]

            # Try Google reviews snippet search
            google_rev = search_web(f"{business_name} {location} google reviews rating", 2)
            for r in google_rev:
                if r.get('body') and len(r['body']) > 80:
                    rev_texts.append(f"[Google Reviews]: {r['body']}")

            # Try Yelp via search snippet (don't fetch page - Yelp blocks scrapers)
            yelp_snip = search_web(f"site:yelp.com {business_name} {location}", 2)
            for r in yelp_snip:
                if r.get('body') and 'yelp.com' in r.get('url',''):
                    rev_texts.append(f"[Yelp snippet]: {r['body']}")
                    break

            # Try Tripadvisor
            ta_snip = search_web(f"site:tripadvisor.com {business_name} {location}", 1)
            for r in ta_snip:
                if r.get('body'):
                    rev_texts.append(f"[Tripadvisor]: {r['body']}")
                    break

            # Try Facebook reviews
            fb_snip = search_web(f"{business_name} {location} facebook reviews", 1)
            for r in fb_snip:
                if r.get('body') and len(r['body']) > 60:
                    rev_texts.append(f"[Facebook]: {r['body']}")
                    break

            # Inject Google Places rating as a review signal if available
            if gp_rating and gp_reviews:
                rev_texts.insert(0, f"[Google Places Rating]: {gp_rating} stars from {gp_reviews} reviews")

            review_text = "\n".join(rev_texts)
            if review_text:
                print(f"  -> Reviews: {len(rev_texts)} sources, {len(review_text)} chars")
            else:
                print("  [!] Reviews: no data found")

            # Pricing
            flat = " ".join(pages.values())
            pr = pricing_results
            pricing_text = "\n".join([f"[{r['title']}]: {r['body']}" for r in pr if r.get('body')])
            prices = re.findall(r"\$[\d,]+(?:\.\d{2})?", flat)
            if prices: pricing_text += "\n[Website prices]: " + ", ".join(set(prices[:8]))

            # Add pre-collected trend + booking + nearby intelligence
            trend_text    = format_trend_intelligence(trend_data)
            booking_intel = format_booking_intelligence(booking_cards)
            # Nearby intel goes first - most location-specific signal
            pricing_text  = "\n\n".join(filter(None, [nearby_intel, booking_intel, trend_text, pricing_text]))

            # Merge nearby cards into booking_cards for competitor tab display
            if nearby_cards:
                seen_nearby = {nc['name'] for nc in nearby_cards}
                booking_cards = nearby_cards + [bc for bc in booking_cards if bc.get('name') not in seen_nearby]

                        # Competitors
            comp_results = search_web(f"{industry or business_name} {location}", 5)
            seen = set(urlparse(c.get("url","")).netloc.replace("www.","") for c in booking_cards if c.get("url"))
            competitors = [{"domain": urlparse(c.get("url","")).netloc.replace("www.",""), 
                            "title": c.get("name", c.get("title", "")),
                            "body": f"Platform: {c.get('platform','')} | Rating: {c.get('rating','N/A')}* | ${c.get('avg_price','N/A')}"} 
                           for c in booking_cards if c.get("url")]
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

            complexity = compute_complexity(pages, local_social, review_text, competitors, _social_links)
            mode = route_task(complexity)

            JOBS[job_id]["step"] = f"Running {mode} AI pipeline..."
            run_deep_job(job_id, business_name, location, pages, local_social,
                        review_text, pricing_text, competitors, mode,
                        booking_cards, trend_data, ck, _social_links,
                        manual_email=manual_email)
        except Exception as e:
            JOBS[job_id] = {"status": "error", "error": str(e), "ts": time.time()}
            print(f"  [X] Deep work failed: {e}")

    Thread(target=deep_work, daemon=True).start()

    # -- Return fast report immediately ----------------------------------------
    fast_brain = {
        "complexity_score": 0,
        "pipeline_mode":    "fast",
        "routing_reasons":  [f"Fast analysis complete - enhancing with full intelligence (job: {job_id})"],
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

# --- SAAS RECOMMENDER ---------------------------------------------------------

def calculate_score(report):
    score = 50
    text = " ".join([
        report.get("key_insight", ""),
        " ".join(report.get("weaknesses", [])),
        report.get("overview", ""),
        report.get("social_strategy", ""),
    ]).lower()

    # Negative signals
    if "no website" in text or "lacks a website" in text: score -= 15
    if "no booking" in text or "no online booking" in text: score -= 12
    if "no social" in text or "lacks social" in text: score -= 12
    if "no crm" in text or "manual" in text: score -= 8
    if "poor seo" in text or "not ranking" in text or "low visibility" in text: score -= 10
    if "low engagement" in text or "minimal engagement" in text: score -= 8
    if "limited" in text: score -= 6
    if "lacks" in text: score -= 5
    if "outdated" in text or "no content" in text: score -= 6
    if "inconsistent" in text: score -= 4

    # Positive signals
    if "strong brand" in text or "well-established" in text: score += 12
    if "good presence" in text or "strong presence" in text: score += 10
    if "high engagement" in text or "strong engagement" in text: score += 10
    if "active" in text and "social" in text: score += 6
    if "consistent" in text and "posting" in text: score += 5

    # Google Places signals (real data - weighted heavily)
    rating  = report.get("rating", 0)
    reviews = report.get("review_count", 0)

    if rating:
        if rating >= 4.8:   score += 18
        elif rating >= 4.5: score += 12
        elif rating >= 4.2: score += 6
        elif rating >= 4.0: score += 2
        elif rating < 3.5:  score -= 18
        elif rating < 4.0:  score -= 10

    if reviews:
        if reviews >= 500:  score += 15
        elif reviews >= 200: score += 10
        elif reviews >= 100: score += 6
        elif reviews >= 50:  score += 2
        elif reviews < 20:   score -= 10
        elif reviews < 50:   score -= 5

    # Deterministic scoring anchor using weighted GPT sub-scores
    sub = report.get("scores", {})
    if sub:
        weights = {
            "content_consistency": 0.25,
            "engagement_quality":  0.20,
            "content_diversity":   0.20,
            "brand_voice_clarity": 0.15,
            "platform_coverage":   0.20,
        }
        weighted_sum = 0
        weight_total = 0
        for key, w in weights.items():
            val = sub.get(key, {})
            s = val.get("score", 0) if isinstance(val, dict) else 0
            if s:
                weighted_sum += s * w
                weight_total += w
        if weight_total > 0:
            gpt_score = weighted_sum / weight_total
            # Blend: 60% signal-based score, 40% deterministic GPT weighted score
            score = round(score * 0.6 + gpt_score * 0.4)

    return max(10, min(score, 90))

def get_primary_problem(report):
    text = " ".join([
        report.get("key_insight", ""),
        " ".join(report.get("weaknesses", []))
    ]).lower()
    if "booking" in text or "appointment" in text:
        return "No or weak booking system"
    if "crm" in text or "follow up" in text or "follow-up" in text:
        return "No customer follow-up system"
    if "seo" in text or "google" in text or "search" in text:
        return "Not visible on Google search"
    if "website" in text:
        return "Weak or outdated website"
    if "social" in text or "instagram" in text:
        return "Weak social media presence"
    return "Low customer conversion"


# --- GROWTH METRICS (modeled estimates, industry benchmarks) -----------------


def confidence_from_data_quality(data_quality):
    """Derive confidence level from data quality scores."""
    if not data_quality:
        return "low"
    avg = sum(v.get("pct", 0) for v in data_quality.values()) / max(len(data_quality), 1)
    if avg >= 65:
        return "high"
    elif avg >= 35:
        return "medium"
    return "low"

def build_metrics_meta(growth_metrics, confidence, source="industry_estimate"):
    """Attach source and confidence to every metric."""
    return {
        "ltv":           {"value": growth_metrics["ltv"],          "source": source, "confidence": confidence},
        "cac":           {"value": growth_metrics["cac"],          "source": source, "confidence": confidence},
        "ltv_cac_ratio": {"value": growth_metrics["ltv_cac_ratio"],"source": source, "confidence": confidence},
        "payback_months":{"value": growth_metrics["payback_months"],"source": source, "confidence": confidence},
        "churn_proxy":   {"value": growth_metrics["churn_proxy"],  "source": source, "confidence": confidence},
        "benchmark_note": growth_metrics["benchmark_note"],
    }

def is_established_business(industry, location, rating, reviews, followers=0):
    """
    Returns True if the business is too established to be a Yelhao target.
    Thresholds vary by niche and location to avoid over-filtering NYC outer boroughs.
    """
    industry = (industry or '').lower()
    location = (location or '').lower()

    if followers >= 10000:
        return True

    if 'med spa' in industry or 'medspa' in industry or 'manhattan' in location:
        return rating >= 4.7 and reviews >= 400

    if any(x in industry for x in ['barber', 'hair salon', 'salon', 'hair']):
        if any(x in location for x in ['bronx', 'queens', 'brooklyn']):
            return rating >= 4.8 and reviews >= 600
        return rating >= 4.7 and reviews >= 500

    return rating >= 4.8 and reviews >= 500


NICHE_BENCHMARKS = {
    # Beauty & wellness
    'nail':        {'avg_ticket': 75,  'visits_per_year': 8,  'retention_years': 2.5, 'cac_base': 22, 'industry_label': 'US nail salon'},
    'barber':      {'avg_ticket': 35,  'visits_per_year': 18, 'retention_years': 3.0, 'cac_base': 18, 'industry_label': 'US barbershop'},
    'hair':        {'avg_ticket': 110, 'visits_per_year': 6,  'retention_years': 2.5, 'cac_base': 28, 'industry_label': 'US hair salon'},
    'spa':         {'avg_ticket': 140, 'visits_per_year': 5,  'retention_years': 2.0, 'cac_base': 35, 'industry_label': 'US day spa'},
    'med spa':     {'avg_ticket': 300, 'visits_per_year': 4,  'retention_years': 2.0, 'cac_base': 55, 'industry_label': 'US med spa'},
    'lash':        {'avg_ticket': 120, 'visits_per_year': 8,  'retention_years': 2.0, 'cac_base': 25, 'industry_label': 'US lash studio'},
    'brow':        {'avg_ticket': 65,  'visits_per_year': 10, 'retention_years': 2.0, 'cac_base': 22, 'industry_label': 'US brow studio'},
    'tattoo':      {'avg_ticket': 200, 'visits_per_year': 2,  'retention_years': 3.0, 'cac_base': 30, 'industry_label': 'US tattoo studio'},
    # Fitness
    'gym':         {'avg_ticket': 60,  'visits_per_year': 12, 'retention_years': 1.5, 'cac_base': 40, 'industry_label': 'US gym/fitness'},
    'yoga':        {'avg_ticket': 90,  'visits_per_year': 10, 'retention_years': 1.5, 'cac_base': 38, 'industry_label': 'US yoga studio'},
    'pilates':     {'avg_ticket': 100, 'visits_per_year': 10, 'retention_years': 1.5, 'cac_base': 42, 'industry_label': 'US pilates studio'},
    'personal training': {'avg_ticket': 80, 'visits_per_year': 24, 'retention_years': 1.5, 'cac_base': 45, 'industry_label': 'US personal training'},
    # Health & medical
    'dentist':     {'avg_ticket': 250, 'visits_per_year': 2,  'retention_years': 5.0, 'cac_base': 80, 'industry_label': 'US dental practice'},
    'dental':      {'avg_ticket': 250, 'visits_per_year': 2,  'retention_years': 5.0, 'cac_base': 80, 'industry_label': 'US dental practice'},
    'chiropractor':{'avg_ticket': 90,  'visits_per_year': 12, 'retention_years': 2.0, 'cac_base': 60, 'industry_label': 'US chiropractic'},
    'physical therapy': {'avg_ticket': 120, 'visits_per_year': 16, 'retention_years': 1.5, 'cac_base': 65, 'industry_label': 'US physical therapy'},
    'optometrist': {'avg_ticket': 200, 'visits_per_year': 1,  'retention_years': 6.0, 'cac_base': 75, 'industry_label': 'US optometry'},
    'vet':         {'avg_ticket': 180, 'visits_per_year': 3,  'retention_years': 5.0, 'cac_base': 70, 'industry_label': 'US veterinary'},
    # Food & beverage
    'restaurant':  {'avg_ticket': 45,  'visits_per_year': 12, 'retention_years': 2.0, 'cac_base': 20, 'industry_label': 'US restaurant'},
    'cafe':        {'avg_ticket': 12,  'visits_per_year': 40, 'retention_years': 2.0, 'cac_base': 15, 'industry_label': 'US cafe'},
    'coffee':      {'avg_ticket': 12,  'visits_per_year': 40, 'retention_years': 2.0, 'cac_base': 15, 'industry_label': 'US coffee shop'},
    'bakery':      {'avg_ticket': 20,  'visits_per_year': 20, 'retention_years': 2.0, 'cac_base': 18, 'industry_label': 'US bakery'},
    'food truck':  {'avg_ticket': 18,  'visits_per_year': 15, 'retention_years': 1.5, 'cac_base': 15, 'industry_label': 'US food truck'},
    # Auto
    'car wash':    {'avg_ticket': 25,  'visits_per_year': 12, 'retention_years': 3.0, 'cac_base': 20, 'industry_label': 'US car wash'},
    'auto repair': {'avg_ticket': 300, 'visits_per_year': 3,  'retention_years': 4.0, 'cac_base': 50, 'industry_label': 'US auto repair'},
    'detailing':   {'avg_ticket': 150, 'visits_per_year': 4,  'retention_years': 3.0, 'cac_base': 35, 'industry_label': 'US auto detailing'},
    # Home services
    'cleaning':    {'avg_ticket': 150, 'visits_per_year': 12, 'retention_years': 2.5, 'cac_base': 45, 'industry_label': 'US cleaning service'},
    'landscaping': {'avg_ticket': 200, 'visits_per_year': 8,  'retention_years': 3.0, 'cac_base': 50, 'industry_label': 'US landscaping'},
    'plumber':     {'avg_ticket': 250, 'visits_per_year': 2,  'retention_years': 5.0, 'cac_base': 60, 'industry_label': 'US plumbing'},
    'electrician': {'avg_ticket': 280, 'visits_per_year': 2,  'retention_years': 5.0, 'cac_base': 65, 'industry_label': 'US electrical'},
    # Retail & other
    'boutique':    {'avg_ticket': 80,  'visits_per_year': 6,  'retention_years': 2.0, 'cac_base': 30, 'industry_label': 'US retail boutique'},
    'pet grooming':{'avg_ticket': 70,  'visits_per_year': 8,  'retention_years': 3.0, 'cac_base': 25, 'industry_label': 'US pet grooming'},
    'photography': {'avg_ticket': 400, 'visits_per_year': 2,  'retention_years': 3.0, 'cac_base': 60, 'industry_label': 'US photography'},
    'tutor':       {'avg_ticket': 60,  'visits_per_year': 30, 'retention_years': 1.5, 'cac_base': 35, 'industry_label': 'US tutoring'},
    'law':         {'avg_ticket': 350, 'visits_per_year': 2,  'retention_years': 4.0, 'cac_base': 120,'industry_label': 'US law firm'},
    'accountant':  {'avg_ticket': 300, 'visits_per_year': 2,  'retention_years': 5.0, 'cac_base': 90, 'industry_label': 'US accounting'},
    # Default fallback
    'default':     {'avg_ticket': 90,  'visits_per_year': 7,  'retention_years': 2.0, 'cac_base': 30, 'industry_label': 'US small business'},
}

def get_niche_benchmarks(niche_str):
    niche_lower = (niche_str or '').lower()
    for key, vals in NICHE_BENCHMARKS.items():
        if key != 'default' and key in niche_lower:
            return vals
    return NICHE_BENCHMARKS['default']

def estimate_growth_metrics(lead, niche=''):
    """
    Returns modeled LTV, CAC, payback period, and churn proxy.
    All figures are industry-benchmark estimates, not real data.
    """
    b = get_niche_benchmarks(niche)

    avg_ticket       = b['avg_ticket']
    visits_per_year  = b['visits_per_year']
    retention_years  = b['retention_years']
    cac_base         = b['cac_base']

    # Churn proxy: signal-aware dynamic calculation
    score = lead.get('overall_score', 50)
    platform = (lead.get('platform') or '').lower()
    text = ' '.join([
        lead.get('key_insight', ''),
        ' '.join(lead.get('weaknesses', []))
    ]).lower()

    churn_pct = 30  # base %
    if score < 40:
        churn_pct += 15
    elif score < 55:
        churn_pct += 8

    if 'no social' in text or 'limited social' in text:
        churn_pct += 10
    if 'no website' in text:
        churn_pct += 10
    if 'low engagement' in text or 'limited engagement' in text:
        churn_pct += 8
    if 'poor seo' in text or 'low visibility' in text:
        churn_pct += 5
    if 'strong brand' in text or 'established' in text:
        churn_pct -= 10
    if 'high engagement' in text or 'strong social' in text:
        churn_pct -= 8

    # small noise for realism across similar leads
    churn_pct += random.uniform(-4, 4)
    churn_pct = max(15, min(round(churn_pct), 70))

    churn_rate = churn_pct / 100
    effective_retention = min(retention_years, 1 / churn_rate)

    # LTV = avg ticket x visits per year x effective retention
    ltv = round(avg_ticket * visits_per_year * effective_retention)

    # CAC adjustment by platform
    if 'instagram' in platform or 'tiktok' in platform:
        cac = cac_base + 12
    elif 'yelp' in platform:
        cac = cac_base + 8
    elif 'booksy' in platform or 'fresha' in platform or 'vagaro' in platform:
        cac = cac_base + 5
    else:
        cac = cac_base

    # Payback period in months = CAC / (monthly revenue per customer)
    monthly_rev = (avg_ticket * visits_per_year) / 12
    payback_months = round(cac / monthly_rev, 1) if monthly_rev > 0 else 0

    ltv_cac_ratio = round(ltv / cac, 1) if cac > 0 else 0

    # Per-metric confidence based on what signals were actually available
    has_pricing  = bool(lead.get('scraped_pricing'))
    has_reviews  = bool(lead.get('review_count') or lead.get('rating'))
    has_social   = bool(lead.get('instagram') or lead.get('tiktok') or lead.get('facebook'))
    has_platform = bool(platform and platform not in ['google_places', 'google_nearby'])

    ltv_conf     = 'medium' if has_pricing else 'low'
    cac_conf     = 'medium' if has_platform else 'low'
    ratio_conf   = 'medium' if (ltv_conf == 'medium' and cac_conf == 'medium') else 'low'
    payback_conf = cac_conf
    churn_conf   = 'medium' if (has_reviews or has_social) else 'low'

    # Overall confidence = most common across metrics
    conf_scores  = [ltv_conf, cac_conf, churn_conf]
    overall_conf = 'medium' if conf_scores.count('medium') >= 2 else 'low'

    return {
        'ltv':              ltv,
        'cac':              cac,
        'ltv_cac_ratio':    ltv_cac_ratio,
        'payback_months':   payback_months,
        'churn_proxy':      churn_pct,
        'benchmark_note':   f'Based on modeled estimates using {b.get("industry_label", "US small business")} industry averages (avg ticket size, visit frequency, and retention patterns).',
        'metrics_confidence': overall_conf,
        'metrics_source':     'industry_estimate',
        'per_metric_confidence': {
            'ltv':            {'confidence': ltv_conf,     'reason': 'pricing data scraped' if has_pricing else 'industry benchmark only'},
            'cac':            {'confidence': cac_conf,     'reason': 'platform-adjusted estimate' if has_platform else 'base industry estimate'},
            'ltv_cac_ratio':  {'confidence': ratio_conf,   'reason': 'derived from LTV and CAC'},
            'payback_months': {'confidence': payback_conf, 'reason': 'derived from CAC estimate'},
            'churn_proxy':    {'confidence': churn_conf,   'reason': 'review/social signals used' if (has_reviews or has_social) else 'score-based estimate only'},
        }
    }

def search_google_places(niche, location, limit=10):
    import requests as req
    key = os.environ.get("GOOGLE_PLACES_KEY", "")
    if not key:
        return []
    try:
        r = req.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": f"{niche} in {location}", "key": key, "type": "establishment"},
            timeout=8
        )
        results = []
        for p in r.json().get("results", [])[:limit]:
            name     = p.get("name", "").strip()
            place_id = p.get("place_id", "")
            if not name or len(name) < 3:
                continue
            website = ""
            if place_id and len(results) < 5:
                try:
                    det = req.get(
                        "https://maps.googleapis.com/maps/api/place/details/json",
                        params={"place_id": place_id, "fields": "website,formatted_phone_number", "key": key},
                        timeout=5
                    ).json()
                    website = det.get("result", {}).get("website", "")
                except:
                    pass
            results.append({
                "business_name": name,
                "website":       website or f"https://www.google.com/search?q={name.replace(' ', '+')}+{location.replace(' ', '+')}",
                "platform":      "google_places",
                "location":      location,
                "industry":      niche,
                "address":       p.get("formatted_address", ""),
                "rating":        p.get("rating", 0),
                "review_count":  p.get("user_ratings_total", 0),
            })
            time.sleep(1)
        print(f"  -> Google Places: {len(results)} results")
        return results
    except Exception as e:
        print(f"  [!] Google Places failed: {e}")
        return []


def search_geoapify(niche, location, limit=10):
    import requests as req
    key = os.environ.get("GEOAPIFY_KEY", "")
    if not key:
        return []
    try:
        geo = req.get(
            "https://api.geoapify.com/v1/geocode/search",
            params={"text": location, "apiKey": key, "limit": 1},
            timeout=5
        ).json()
        features = geo.get("features", [])
        if not features:
            return []
        lon, lat = features[0]["geometry"]["coordinates"]
        r = req.get(
            "https://api.geoapify.com/v2/places",
            params={
                "categories": "commercial.beauty_and_spa,commercial.health",
                "filter":     f"circle:{lon},{lat},3000",
                "limit":      limit,
                "apiKey":     key,
            },
            timeout=8
        ).json()
        results = []
        for f in r.get("features", []):
            props = f.get("properties", {})
            name  = props.get("name", "").strip()
            if not name or len(name) < 3:
                continue
            results.append({
                "business_name": name,
                "website":       props.get("website", f"https://www.google.com/search?q={name.replace(' ', '+')}"),
                "platform":      "geoapify",
                "location":      location,
                "industry":      niche,
                "address":       props.get("formatted", ""),
                "rating":        0,
                "review_count":  0,
            })
        print(f"  -> Geoapify: {len(results)} results")
        return results
    except Exception as e:
        print(f"  [!] Geoapify failed: {e}")
        return []




# --- LOCATION-AWARE COMPETITOR INTELLIGENCE -----------------------------------

NICHE_RADIUS_METERS = {
    'nail':        800,    # ~0.5 mile
    'barber':      800,
    'hair':        1200,
    'spa':         2000,
    'med spa':     3000,
    'medspa':      3000,
    'gym':         2500,
    'yoga':        2000,
    'pilates':     2000,
    'restaurant':  600,
    'cafe':        400,
    'coffee':      400,
    'dentist':     3000,
    'dental':      3000,
    'default':     1500,
}

def get_niche_radius(niche):
    niche_lower = (niche or '').lower()
    for key, radius in NICHE_RADIUS_METERS.items():
        if key != 'default' and key in niche_lower:
            return radius
    return NICHE_RADIUS_METERS['default']


def geocode_business(business_name, address=None, location=None):
    """
    Returns (lat, lng) for a business using Google Geocoding API.
    Tries address first, falls back to business name + location.
    """
    key = os.environ.get('GOOGLE_PLACES_KEY', '')
    if not key:
        return None, None
    try:
        import requests as req
        query = address or f'{business_name} {location or ""}'
        r = req.get(
            'https://maps.googleapis.com/maps/api/geocode/json',
            params={'address': query, 'key': key},
            timeout=5
        )
        results = r.json().get('results', [])
        if results:
            loc = results[0]['geometry']['location']
            return loc['lat'], loc['lng']
    except:
        pass
    return None, None


def nearby_competitors_google(lat, lng, niche, radius_m=1500, limit=8):
    """
    Uses Google Places Nearby Search to find actual nearby competitors.
    Returns structured competitor cards with real ratings, price level, and distance.
    """
    key = os.environ.get('GOOGLE_PLACES_KEY', '')
    if not key or not lat or not lng:
        return []
    try:
        import requests as req
        r = req.get(
            'https://maps.googleapis.com/maps/api/place/nearbysearch/json',
            params={
                'location': f'{lat},{lng}',
                'radius':   radius_m,
                'keyword':  niche,
                'key':      key,
            },
            timeout=8
        )
        results = r.json().get('results', [])
        cards = []
        price_map = {0: 'unknown', 1: '$', 2: '20', 3: '20$', 4: '2020'}
        for p in results[:limit]:
            name   = p.get('name', '').strip()
            rating = p.get('rating', 0)
            reviews= p.get('user_ratings_total', 0)
            price  = price_map.get(p.get('price_level', 0), 'unknown')
            open_now = p.get('opening_hours', {}).get('open_now')
            vicinity = p.get('vicinity', '')
            place_id = p.get('place_id', '')

            # Get website via Details call (only for top 4)
            website = ''
            if place_id and len(cards) < 4:
                try:
                    det = req.get(
                        'https://maps.googleapis.com/maps/api/place/details/json',
                        params={'place_id': place_id, 'fields': 'website,formatted_phone_number', 'key': key},
                        timeout=4
                    ).json()
                    website = det.get('result', {}).get('website', '')
                except:
                    pass

            cards.append({
                'name':         name,
                'rating':       rating,
                'review_count': reviews,
                'price_level':  price,
                'address':      vicinity,
                'website':      website,
                'open_now':     open_now,
                'platform':     'google_nearby',
                'url':          website or f'https://www.google.com/search?q={name.replace(" ", "+")}',
            })
        print(f'  -> Nearby competitors: {len(cards)} within {radius_m}m')
        return cards
    except Exception as e:
        print(f'  [!] Nearby search failed: {e}')
        return []


def format_nearby_competitors(cards, target_rating=None, target_niche=''):
    """Format nearby competitor cards for AI prompt injection."""
    if not cards:
        return ''
    lines = [f'NEARBY COMPETITOR INTELLIGENCE ({len(cards)} businesses within local radius):']
    rated  = [c for c in cards if c.get('rating')]
    if rated:
        avg_r = round(sum(c['rating'] for c in rated) / len(rated), 1)
        avg_rev = round(sum(c.get('review_count', 0) for c in rated) / len(rated))
        lines.append(f'Market avg rating: {avg_r}* | Avg review count: {avg_rev}')
        if target_rating:
            gap = round(target_rating - avg_r, 1)
            lines.append(f'Target business rating vs market: {target_rating}* ({"above" if gap >= 0 else "below"} avg by {abs(gap)})')
    prices = [c['price_level'] for c in cards if c.get('price_level') and c['price_level'] != 'unknown']
    if prices:
        lines.append(f'Pricing range in area: {min(prices, key=len)} to {max(prices, key=len)}')
    lines.append('')
    for i, c in enumerate(cards[:6], 1):
        rev = f"({c['review_count']} reviews)" if c.get('review_count') else ''
        price = f"| {c['price_level']}" if c.get('price_level') and c['price_level'] != 'unknown' else ''
        open_s = '| Open now' if c.get('open_now') else ''
        lines.append(f'{i}. {c["name"]} - {c["rating"]}* {rev} {price} {open_s}')
        if c.get('address'):
            lines.append(f'   Address: {c["address"]}')
    return '\n'.join(lines)

# --- /prospect ENDPOINT -------------------------------------------------------

# --- NICHE VALIDATION FOR PROSPECT FLOW -----------------------------------------
# High-quality niches = strong local presence + booking platforms + review signals

RECOMMENDED_NICHES = {
    "health_wellness": [
        "dentists", "dental practices", "chiropractors", "physical therapy",
        "medical spas", "dermatologists", "optometrists", "massage therapy"
    ],
    "beauty_personal_care": [
        "hair salons", "nail salons", "barbershops", "spas", "waxing studios",
        "eyebrow threading", "lash extensions", "tanning salons"
    ],
    "fitness": [
        "gyms", "yoga studios", "pilates studios", "crossfit", "crossfit boxes",
        "martial arts", "dance studios", "personal trainers"
    ],
    "food_beverage": [
        "restaurants", "cafes", "coffee shops", "bakeries", "juice bars",
        "food trucks", "catering services"
    ],
    "automotive": [
        "car washes", "auto detailing", "oil change", "tire shops",
        "auto repair", "mechanic shops"
    ],
    "home_services": [
        "cleaning services", "lawn care", "landscaping", "plumbing",
        "hvac", "pest control", "handyman services", "painting"
    ],
    "professional_services": [
        "lawyers", "accountants", "financial advisors", "real estate agents",
        "insurance agents", "tutoring centers"
    ],
    "pet_services": [
        "veterinarians", "pet grooming", "dog training", "pet boarding",
        "doggy daycare"
    ]
}

LOW_QUALITY_NICHES = [
    "ecommerce", "saas", "b2b software", "enterprise", "consulting",
    "remote", "online only", "virtual", "freelance", "digital agency",
    "crypto", "nft", "web3", "blockchain"
]


def validate_niche(niche_input):
    """
    Returns dict with: quality_score (0-100), category, suggestions, message
    Used to warn users when they enter niches outside Yelhao\'s sweet spot.
    """
    niche_lower = niche_input.lower().strip()

    # Check for low-quality patterns first
    for bad in LOW_QUALITY_NICHES:
        if bad in niche_lower:
            return {
                "quality_score": 0,
                "category": None,
                "suggestions": ["nail salons", "dentists", "gyms"],
                "message": f"\"{niche_input}\" is outside Yelhao\'s strength. Yelhao analyzes local service businesses with physical locations, booking platforms, and review signals."
            }

    # Check for exact or partial match against recommended niches
    best_score = 0
    best_category = None
    closest = []

    for category, niches in RECOMMENDED_NICHES.items():
        for recommended in niches:
            if niche_lower == recommended or niche_lower in recommended or recommended in niche_lower:
                return {"quality_score": 100, "category": category, "suggestions": [], "message": None}
            # Word-level partial match
            if any(word in recommended for word in niche_lower.split() if len(word) > 3):
                if len(closest) < 3 and recommended not in closest:
                    closest.append(recommended)
                if best_score < 60:
                    best_score = 60
                    best_category = category

    if best_score == 0:
        # Unknown niche - allow but warn
        return {
            "quality_score": 30,
            "category": None,
            "suggestions": ["nail salons", "dentists", "gyms", "restaurants", "cleaning services"],
            "message": f"\"{niche_input}\" is unfamiliar. Yelhao works best with local service businesses. Results may be limited."
        }

    return {"quality_score": best_score, "category": best_category, "suggestions": closest, "message": None}


@app.route('/prospect', methods=['POST'])
def prospect():
    body = request.get_json(force=True, silent=True) or {}
    is_demo = bool(body.get('demo'))

    if is_demo and not request.headers.get('X-Agent-Token'):
        code_type = 'demo'
    else:
        code_type = get_code_type(request)
        if code_type is None:
            return jsonify({"error": "Access denied."}), 401

    init_clients()
    if not gpt_client:
        return jsonify({"error": "API keys not configured"}), 500
    niche    = body.get('niche', '').strip()
    location = body.get('location', '').strip()
    maturity = body.get('maturity', 'all').strip().lower()  # all | new | growing
    try:
        limit = min(int(body.get('limit', 10)), 25)
    except:
        limit = 10
    if code_type == 'demo' and not request.headers.get('X-Agent-Token'):
        limit = min(limit, 3)

    if not niche or not location:
        return jsonify({"error": "niche and location are required"}), 400

    # Validate niche quality - warn if outside Yelhao's sweet spot
    niche_validation = validate_niche(niche)
    if niche_validation["quality_score"] < 50:
        print(f"  [!] Low-quality niche: {niche} (score: {niche_validation['quality_score']})")

    print(f"  -> Prospecting: {niche} in {location} (limit {limit})")
    sys.stdout.flush()

    results = []
    seen_names = set()

    raw = search_google_places(niche, location, limit=limit)

    if not raw:
        print("  -> Google Places empty, trying Geoapify...")
        raw = search_geoapify(niche, location, limit=limit)

    if not raw:
        print("  -> Geoapify empty, falling back to booking platforms...")
        for platform, cfg in BOOKING_PLATFORMS.items():
            domain = cfg["domain"]
            for query in [f'site:{domain} "{niche}" "{location}"', f'site:{domain} {niche} {location}']:
                for r in search_web(query, max_results=5):
                    url   = r.get("url", "")
                    title = r.get("title", "").split("|")[0].split("-")[0].strip()
                    if domain not in url:
                        continue
                    bad_title = title.lower()
                    if any(x in bad_title for x in ["near me", "top 20", "top 10", "top 30", "best", "directory", "list of", "search results", "guide"]):
                        continue
                    bad_url = url.lower()
                    if any(x in bad_url for x in ["/search", "near-me", "top-"]):
                        continue
                    raw.append({"business_name": title, "website": url, "platform": platform, "location": location, "industry": niche, "rating": 0, "review_count": 0})
                if len(raw) >= limit:
                    break
            if len(raw) >= limit:
                break

    for biz in raw:
        name_key = re.sub(r'[^a-z0-9]', '', biz["business_name"].lower())[:20]
        if not name_key or name_key in seen_names:
            continue
        # Apply maturity filter
        rv = biz.get('review_count', 0) or 0
        if maturity == 'new' and rv > 20:
            continue
        if maturity == 'growing' and (rv <= 20 or rv > 150):
            continue
        seen_names.add(name_key)
        results.append(biz)

    results = results[:limit]
    print(f"  -> Found {len(results)} businesses to analyze")
    sys.stdout.flush()
    analyzed = []
    for biz in results[:limit]:
        try:
            name = biz["business_name"]
            site = biz["website"]
            name = biz["business_name"]
            site = biz["website"]
            if len(name) < 3 or "/" in name:
                print(f"  [!] Skipping bad name: {name}")
                continue
            print(f"  -> Analyzing: {name}")
            sys.stdout.flush()

            # ── Established business filter ──────────────────────────────────
            gp_rating  = biz.get('rating', 0)
            gp_reviews = biz.get('review_count', 0)
            followers  = biz.get('followers', 0)
            if is_established_business(niche, location, gp_rating, gp_reviews, followers):
                print(f"  [!] Skipping {name} - established business (rating={gp_rating}, reviews={gp_reviews})")
                continue

            website_pages = {}
            social_links  = {}
            social_text   = {}

            html, _ = safe_get(site)
            if not html:
                print(f"  [!] Skipping {name} - no HTML")
                continue

            website_pages["homepage"] = extract_text(html, 2000)
            social_links = find_social_links(html)

            # Scrape one social profile for richer signal
            for plat in ["instagram", "facebook", "yelp", "tiktok"]:
                if plat in social_links:
                    try:
                        s_html, _ = safe_get(social_links[plat])
                        if s_html:
                            social_text[plat] = extract_text(s_html, 800)
                            break
                    except:
                        pass

            # Try to get one more page (booking or about)
            for path in ["/book", "/booking", "/about", "/services", "/menu"]:
                try:
                    from urllib.parse import urljoin
                    extra_url  = urljoin(site, path)
                    extra_html, _ = safe_get(extra_url)
                    if extra_html:
                        website_pages["extra"] = extract_text(extra_html, 800)
                        break
                except:
                    pass

            # Inject real rating/reviews from Google Places into pipeline
            if gp_rating or gp_reviews:
                rating_hint = f"Google rating: {gp_rating} stars ({gp_reviews} reviews)"
                website_pages["_meta"] = rating_hint

            try:
                report = run_fast_pipeline(name, location, website_pages, social_text, social_links)
            except Exception as gpt_err:
                print(f"  [!] GPT failed for {name}: {gpt_err}")
                continue

            # Insufficient data flag - if GPT had nothing to work with, skip
            homepage_text = website_pages.get('homepage', '')
            social_data   = ''.join(social_text.values())
            if len(homepage_text) < 200 and len(social_data) < 100:
                print(f"  [!] Skipping {name} - insufficient data for reliable diagnosis")
                continue

            # Inject Google Places signals into report for scoring
            report["company"]      = name
            report["website"]      = site
            report["location"]     = location
            report["industry"]     = niche
            report["rating"]       = gp_rating
            report["review_count"] = gp_reviews
            report["overall_score"]       = calculate_score(report)
            report["primary_problem"]     = get_primary_problem(report)
            report["is_high_opportunity"] = report["overall_score"] < 45
            weaknesses = report.get("weaknesses", [])
            experiments = report.get("experiments", [])

            top_experiment = ""
            if experiments:
                top_experiment = max(experiments, key=lambda e: e.get("impact", 0)).get("experiment", "")

            report["recommended_saas"] = recommend_saas(weaknesses, top_experiment)
            

            enrichment = extract_enrichment(website_pages, social_links)
            try:
                push_to_airtable(report, enrichment, website_url=site, location=location)
                print(f"  [OK] Airtable push for {name}", flush=True)
            except Exception as at_err:
                print(f"  [!] Airtable push failed for {name}: {at_err}")

            # Extract pricing signals from scraped data
            _prices = []
            for pg in website_pages.values():
                import re as _re
                _prices += _re.findall(r'\$[\d,]+(?:\.\d{2})?', pg)

            growth_metrics = estimate_growth_metrics(
                {
                    "overall_score":   report.get("overall_score", 50),
                    "platform":        biz["platform"],
                    "key_insight":     report.get("key_insight", ""),
                    "weaknesses":      report.get("weaknesses", []),
                    "rating":          biz.get("rating", 0),
                    "review_count":    biz.get("review_count", 0),
                    "instagram":       social_links.get("instagram", ""),
                    "tiktok":          social_links.get("tiktok", ""),
                    "facebook":        social_links.get("facebook", ""),
                    "scraped_pricing": _prices[:5],
                },
                niche=niche
            )

            analyzed.append({
                "business_name":        name,
                "overall_score":        report.get("overall_score", 0),
                "primary_problem":      report.get("primary_problem", ""),
                "is_high_opportunity":  report.get("is_high_opportunity", False),
                "key_insight":          report.get("key_insight", ""),
                "top_weakness":         (report.get("weaknesses") or [""])[0],
                "recommended_saas":     report.get("recommended_saas", []),
                "website":              site,
                "platform":             biz["platform"],
                "location":             location,
                "industry":             niche,
                "phone":                enrichment.get("phone", ""),
                "instagram":            social_links.get("instagram", ""),
                "tiktok":               social_links.get("tiktok", ""),
                "facebook":             social_links.get("facebook", ""),
                "sources_scanned":      list(social_links.keys()),
                "ltv":                  growth_metrics["ltv"],
                "cac":                  growth_metrics["cac"],
                "ltv_cac_ratio":        growth_metrics["ltv_cac_ratio"],
                "payback_months":       growth_metrics["payback_months"],
                "churn_proxy":          growth_metrics["churn_proxy"],
                "benchmark_note":       growth_metrics["benchmark_note"],
                "metrics_confidence":   growth_metrics.get("metrics_confidence", "low"),
                "metrics_source":       growth_metrics.get("metrics_source", "industry_estimate"),
                "per_metric_confidence": growth_metrics.get("per_metric_confidence", {}),
            })
            time.sleep(1)

        except Exception as e:
            print(f"  [X] Failed: {biz.get('business_name')}: {e}")
            continue

    analyzed.sort(key=lambda x: x["overall_score"])
    print(f"  [OK] Prospect run complete: {len(analyzed)} analyzed", flush=True)

    return jsonify({"niche": niche, "location": location, "count": len(analyzed), "leads": analyzed, "niche_warning": niche_validation if niche_validation["quality_score"] < 50 else None})

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
