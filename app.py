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
import textwrap
from collections import defaultdict
from flask import Flask, request, jsonify
from flask_cors import CORS

# ─── RATE LIMITING ────────────────────────────────────────────────────────────
# Simple in-memory log — resets on dyno restart (fine for now)
REQUEST_LOG  = defaultdict(list)  # ip → [timestamps]
VALIDATE_LOG = defaultdict(list)  # ip → [timestamps]

RATE_LIMIT_VALIDATE = 20   # max /validate attempts per IP per hour

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
from duckduckgo_search import DDGS
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


def get_code_type(req) -> str | None:
    """
    Read token ONLY from X-Agent-Token header.
    Returns "master", "guest", or None if invalid/missing.
    """
    token = req.headers.get("X-Agent-Token", "").strip().lower()
    if not token:
        return None
    if token == MASTER_CODE:
        return "master"
    if token in GUEST_CODES:
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

def safe_get(url, timeout=25):
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
        r = req.get(url, headers=HEADERS, timeout=15)
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


# ─── /agent ENDPOINT ──────────────────────────────────────────────────────────

@app.route('/agent', methods=['POST'])
def agent():
    # ── Auth check ────────────────────────────────────────────────────────────
    code_type = get_code_type(request)
    if code_type is None:
        return jsonify({"error": "Access denied. Valid access code required."}), 401

    # ── Rate limit — master code is exempt, guests get 10/hour ─────────────
    if code_type == "guest":
        ip    = request.remote_addr
        token = request.headers.get("X-Agent-Token", "").strip().lower()
        if not check_rate_limit(REQUEST_LOG, f"{ip}:{token}", RATE_LIMIT_AGENT):
            return jsonify({"error": "Rate limit exceeded — 10 analyses per hour."}), 429

    init_clients()
    if not gpt_client or not claude_client:
        return jsonify({"error": "API keys not configured on backend"}), 500

    body = request.get_json()
    business_name = body.get('business_name', '')
    website_url   = body.get('website', '')
    location      = body.get('location', '')
    industry      = body.get('industry', '')
    user_content  = body.get('user_content', '')  # pasted captions OR profile URL

    # Optional manual social hints
    manual_socials = {
        k: body.get(k) for k in ['instagram','tiktok','twitter','linkedin']
        if body.get(k)
    }

    if not business_name or not website_url:
        return jsonify({"error": "business_name and website are required"}), 400

    # ── Scrape ────────────────────────────────────────────────────────────────
    website_pages = {}
    homepage_html = ""
    social_links  = manual_socials.copy()

    if not website_url.startswith("http"):
        website_url = "https://" + website_url

    html, _ = safe_get(website_url)
    if html:
        website_pages["homepage"] = extract_text(html, 3000)
        homepage_html = html
        # Auto-detect social links via <a href>
        detected = find_social_links(html)
        for k, v in detected.items():
            if k not in social_links:
                social_links[k] = v

    for slug in ["/about", "/about-us", "/services", "/pricing", "/contact"]:
        h, code = safe_get(website_url.rstrip("/") + slug)
        if h and isinstance(code, int) and code < 400:
            text = extract_text(h, 1500)
            if text: website_pages[slug.lstrip("/")] = text

    # ── Social signals — 3-layer approach ───────────────────────────────────
    social_text = {}

    # Layer 1: Scrape detected/manual profile pages directly
    for platform, url in social_links.items():
        h, _ = safe_get(url)
        if h:
            text = extract_text(h, 1500)
            if len(text) > 200:
                social_text[platform] = text
        time.sleep(0.3)

    # Layer 2: DuckDuckGo site: search — pulls cached posts/captions
    # Works even when direct profile scraping is blocked
    for platform, url in social_links.items():
        if platform in social_text and len(social_text[platform]) > 500:
            continue  # already have good data
        handle = url.rstrip("/").split("/")[-1].lstrip("@")
        if not handle:
            continue
        site_map = {
            "instagram": f"site:instagram.com/{handle}",
            "tiktok":    f"site:tiktok.com/@{handle}",
            "twitter":   f"site:x.com/{handle} OR site:twitter.com/{handle}",
            "linkedin":  f"site:linkedin.com {handle}",
        }
        query = site_map.get(platform)
        if not query:
            continue
        results = search_web(query, 5)
        snippets = [r["body"] for r in results if r.get("body") and len(r["body"]) > 50]
        if snippets:
            combined = f"[{platform.upper()} cached content via search]:\n" + "\n".join(snippets[:4])
            if platform in social_text:
                social_text[platform] += "\n" + combined
            else:
                social_text[platform] = combined

    # Layer 3: If still no social data, search for profiles + content by name
    if not social_text:
        for platform in ["instagram", "tiktok", "twitter"]:
            results = search_web(f'"{business_name}" {platform} {location}', 4)
            snippets = []
            for r in results:
                url = r.get("url", "")
                body = r.get("body", "")
                if platform in url.lower() and body:
                    snippets.append(body)
                elif body and platform in body.lower():
                    snippets.append(body)
            if snippets:
                social_text[platform] = f"[{platform.upper()} signals via web search]:\n" + "\n".join(snippets[:3])

    # ── User-provided content (highest confidence — direct signal) ────────────
    # Accepts: pasted captions OR a profile URL the agent couldn't auto-detect
    user_content = body.get("user_content", "").strip()
    if user_content:
        # Check if it looks like a URL — try to fetch it
        if user_content.startswith("http") and any(p in user_content for p in ["instagram","tiktok","twitter","linkedin","facebook"]):
            h, _ = safe_get(user_content)
            if h:
                fetched = extract_text(h, 2000)
                if len(fetched) > 200:
                    platform_hint = next((p for p in ["instagram","tiktok","twitter","linkedin","facebook"] if p in user_content), "social")
                    social_text[platform_hint] = f"[USER-PROVIDED URL — {platform_hint}]:\n{fetched}"
                else:
                    # Fetch failed or thin — treat as pasted text
                    social_text["user_provided"] = f"[USER-PROVIDED CONTENT — direct signal]:\n{user_content}"
            # Also run DuckDuckGo on the profile URL for cached posts
            results = search_web(f"site:{user_content.split('//')[1].split('/')[0]} {user_content.rstrip('/').split('/')[-1]}", 4)
            cached = [r["body"] for r in results if r.get("body")]
            if cached:
                key = next((p for p in ["instagram","tiktok","twitter","linkedin","facebook"] if p in user_content), "social")
                social_text[key] = social_text.get(key, "") + "\n[Cached posts]:\n" + "\n".join(cached[:3])
        else:
            # Treat as pasted captions/text — highest confidence, use directly
            social_text["user_provided"] = f"[USER-PROVIDED POSTS & CAPTIONS — direct signal, highest confidence]:\n{user_content}"

    # ── Press & web mentions for thin sites ──────────────────────────────────
    # Finds news articles, interviews, features — useful when website is minimal
    press_results = search_web(f'"{business_name}" {location}', 5)
    press_snippets = []
    SKIP_PRESS = ["yelp.com", "yellowpages.com", "mapquest.com", "bbb.org", "facebook.com"]
    for r in press_results:
        url = r.get("url", "")
        body = r.get("body", "")
        if body and not any(s in url for s in SKIP_PRESS):
            press_snippets.append(f"[{r.get('title','')}]: {body}")
    if press_snippets:
        website_pages["press_mentions"] = "\n".join(press_snippets[:4])

    # Reviews
    q = f'"{business_name}" {location} reviews'
    review_snippets = search_web(q, 5)
    review_texts = [f"[{r['title']}]: {r['body']}" for r in review_snippets if r.get('body')]
    yelp_results = search_web(f"{business_name} {location} yelp", 3)
    for r in yelp_results:
        if "yelp.com/biz/" in r.get("url",""):
            h, _ = safe_get(r["url"])
            if h: review_texts.append("[Yelp]: " + extract_text(h, 2000))
            break
    review_text = "\n".join(review_texts)

    # Pricing
    website_flat = " ".join(website_pages.values())
    pricing_results = search_web(f"{business_name} pricing cost {industry}", 5)
    pricing_text = "\n".join([f"[{r['title']}]: {r['body']}" for r in pricing_results if r.get('body')])
    prices = re.findall(r"\$[\d,]+(?:\.\d{2})?", website_flat)
    if prices: pricing_text += "\n[Website prices]: " + ", ".join(set(prices[:12]))

    # Competitors
    comp_results = search_web(f"{business_name} competitors alternatives {industry} {location}", 8)
    comp_results += search_web(f"best {industry or business_name} {location}", 5)
    seen_domains = set()
    competitors = []
    SKIP = ["yelp.com","google.com","facebook.com","yellowpages.com","bbb.org","tripadvisor.com","reddit.com"]
    for r in comp_results:
        url = r.get("url","")
        try: domain = urlparse(url).netloc.replace("www.","")
        except: continue
        if not domain or domain in seen_domains or any(s in domain for s in SKIP): continue
        seen_domains.add(domain)
        competitors.append({"domain": domain, "title": r.get("title",""), "body": r.get("body","")[:300]})
        if len(competitors) >= 5: break

    # ── Route ─────────────────────────────────────────────────────────────────
    complexity = compute_complexity(website_pages, social_text, review_text, competitors)
    mode       = route_task(complexity)

    # ── Run pipeline ──────────────────────────────────────────────────────────
    try:
        report = run_full_pipeline(
            business_name, location,
            website_pages, social_text,
            review_text, pricing_text,
            competitors, mode,
        )
    except json.JSONDecodeError as e:
        return jsonify({"error": f"AI output parse error: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Guarantee fields
    report["company"] = business_name
    report["_pipeline_mode"]    = mode
    report["_complexity_score"] = complexity

    # ── Build agent_brain ─────────────────────────────────────────────────────
    data_quality = build_data_quality(website_pages, social_text, review_text, competitors)
    routing_reasons = build_routing_reasons(website_pages, social_text, review_text, competitors, data_quality)

    agent_brain = {
        "complexity_score":  complexity,
        "pipeline_mode":     mode,
        "routing_reasons":   routing_reasons,
        "data_quality":      data_quality,
        "access_type":       code_type,
    }

    return jsonify({
        "report":      report,
        "agent_brain": agent_brain,
    })


@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "agent_version": "2.0"})


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
