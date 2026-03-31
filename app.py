import os
import json
import re
import time
import logging
from functools import wraps
from flask import Flask, request, jsonify
from flask_cors import CORS
from bs4 import BeautifulSoup
import requests

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*")
CORS(app, origins=ALLOWED_ORIGINS)

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# ── Required fields in a valid AI response ────────────────────────────────────
REQUIRED_FIELDS = [
    "company", "overall_score", "scores", "key_insight",
    "strengths", "weaknesses", "opportunities", "strategy_blueprint"
]
REQUIRED_SCORE_KEYS = [
    "content_consistency", "engagement_quality",
    "content_diversity", "brand_voice_clarity", "platform_coverage"
]

# ── Rate limiter (per IP, 10 req/min) ─────────────────────────────────────────
_rate_store = {}

def rate_limit(max_per_minute=10):
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            ip = request.headers.get("X-Forwarded-For", request.remote_addr)
            now = time.time()
            history = _rate_store.get(ip, [])
            history = [t for t in history if now - t < 60]
            if len(history) >= max_per_minute:
                logger.warning(f"Rate limit hit for IP: {ip}")
                return jsonify({"error": "Rate limit exceeded. Try again in a minute."}), 429
            history.append(now)
            _rate_store[ip] = history
            return f(*args, **kwargs)
        return wrapped
    return decorator


# ── AI Response Validation (Upgrade: validation layer) ───────────────────────
def validate_ai_response(data: dict) -> tuple[bool, list[str]]:
    """
    Validates that the AI response contains all required fields and
    that scores are within the expected 0-100 range.
    Returns (is_valid, list_of_issues).
    """
    issues = []

    # Check top-level required fields
    for field in REQUIRED_FIELDS:
        if field not in data:
            issues.append(f"Missing required field: '{field}'")

    # Check score sub-fields
    scores = data.get("scores", {})
    if not isinstance(scores, dict):
        issues.append("'scores' must be an object")
    else:
        for key in REQUIRED_SCORE_KEYS:
            if key not in scores:
                issues.append(f"Missing score key: '{key}'")
            elif not isinstance(scores[key], (int, float)) or not (0 <= scores[key] <= 100):
                issues.append(f"Score '{key}' must be a number between 0 and 100")

    # Check overall_score range
    overall = data.get("overall_score")
    if overall is not None and not (0 <= overall <= 100):
        issues.append("'overall_score' must be between 0 and 100")

    return len(issues) == 0, issues


def call_openai(prompt: str, max_tokens: int, attempt: int = 1) -> dict:
    """
    Calls the OpenAI API and returns parsed JSON.
    Raises ValueError if the response fails validation after max retries.
    """
    MAX_RETRIES = 2

    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}",
        },
        json={
            "model": "gpt-4o-mini",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
            # Instruct the model to return valid JSON only
            "response_format": {"type": "json_object"},
        },
        timeout=60,
    )
    resp.raise_for_status()

    raw = resp.json()["choices"][0]["message"]["content"].strip()

    # Strip any accidental markdown fences
    raw = raw.replace("```json", "").replace("```", "").strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error on attempt {attempt}: {e}")
        if attempt < MAX_RETRIES:
            logger.info(f"Retrying AI call (attempt {attempt + 1})...")
            return call_openai(prompt, max_tokens, attempt + 1)
        raise ValueError(f"AI returned invalid JSON after {attempt} attempts: {e}")

    # Validate response structure
    is_valid, issues = validate_ai_response(data)
    if not is_valid:
        logger.warning(f"Validation issues on attempt {attempt}: {issues}")
        if attempt < MAX_RETRIES:
            logger.info(f"Retrying due to validation issues (attempt {attempt + 1})...")
            return call_openai(prompt, max_tokens, attempt + 1)
        # On final attempt, attach validation warnings but still return data
        data["_validation_warnings"] = issues
        logger.error(f"Returning response with validation warnings: {issues}")

    return data


# ── Health check ──────────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "uper-api", "version": "2.0"})


def fetch_additional_pages(base_url):
    """Fetch about, blog, and product pages to enrich the analysis context."""
    paths = ["/about", "/pages/about", "/about-us", "/blog", "/products", "/our-story"]
    results = []
    for path in paths:
        try:
            url = base_url.rstrip("/") + path
            res = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
            if res.status_code == 200 and len(res.text) > 500:
                # Strip basic HTML tags for token efficiency
                import re
                text = re.sub(r'<[^>]+>', ' ', res.text)
                text = re.sub(r'\s+', ' ', text).strip()
                results.append(f"[{path}]\n{text[:1000]}")
                logger.info(f"Enriched with {path} ({len(text)} chars)")
        except Exception as e:
            logger.debug(f"Enrichment skip {path}: {e}")
    return results


MODE_INSTRUCTIONS = {
    "growth":     "Focus on acquisition opportunities, funnel efficiency, audience expansion, and growth levers. Identify what's driving or limiting top-of-funnel reach.",
    "content":    "Focus on content quality, consistency, storytelling, format diversity, and editorial voice. Identify content gaps and what types of content are missing.",
    "social":     "Focus on social media presence, platform-specific performance, engagement patterns, community signals, and posting strategy across channels.",
    "conversion": "Focus on UX signals, call-to-action clarity, messaging effectiveness, trust signals, and conversion optimization opportunities on the website.",
}


# ── Main analysis endpoint ────────────────────────────────────────────────────
@app.route("/analyze", methods=["POST"])
@rate_limit(max_per_minute=10)
def analyze():
    if not OPENAI_API_KEY:
        return jsonify({"error": "Server is not configured with an API key."}), 500

    body = request.get_json(force=True)
    prompt = body.get("prompt")
    max_tokens = body.get("max_tokens", 2500)
    website = body.get("website", "")
    mode = body.get("mode", "social")

    if not prompt:
        return jsonify({"error": "Missing prompt."}), 400

    # Multi-source enrichment — fetch additional pages and inject into prompt
    enriched_context = ""
    if website:
        try:
            if not website.startswith("http"):
                website = "https://" + website
            extra_pages = fetch_additional_pages(website)[:3]  # cap at 3 pages
            if extra_pages:
                enriched_context = "\n\n--- ADDITIONAL PAGES (about, blog, products) ---\n" + "\n\n".join(extra_pages)
                logger.info(f"Enriched with {len(extra_pages)} additional pages")
        except Exception as e:
            logger.warning(f"Enrichment failed: {e}")

    # Inject enriched context and mode instruction into prompt
    mode_instruction = MODE_INSTRUCTIONS.get(mode, MODE_INSTRUCTIONS["social"])
    if enriched_context:
        prompt = prompt + enriched_context
    mode_prefix = f"ANALYSIS MODE: {mode.upper()}\n{mode_instruction}\n\n"
    prompt = mode_prefix + prompt

    logger.info(f"Analysis request — mode: {mode}, prompt length: {len(prompt)} chars")

    try:
        data = call_openai(prompt, max_tokens)

        data["_disclaimer"] = (
            "This analysis provides AI-generated marketing insights based on publicly "
            "visible content signals. Scores are estimates, not platform-verified metrics. "
            "Final strategic decisions should be made by marketing professionals."
        )
        data["_mode"] = mode

        logger.info(f"Analysis complete — company: {data.get('company', 'unknown')}, score: {data.get('overall_score', 'n/a')}")

        return jsonify({
            "choices": [{
                "message": {
                    "content": json.dumps(data)
                }
            }]
        })

    except requests.exceptions.Timeout:
        logger.error("OpenAI request timed out")
        return jsonify({"error": "Request to AI service timed out."}), 504

    except requests.exceptions.HTTPError as e:
        try:
            detail = e.response.json()
        except Exception:
            detail = str(e)
        logger.error(f"OpenAI HTTP error: {detail}")
        return jsonify({"error": detail}), e.response.status_code

    except ValueError as e:
        logger.error(f"Validation/parse error: {e}")
        return jsonify({"error": str(e)}), 422

    except Exception as e:
        logger.exception("Unexpected error in /analyze")
        return jsonify({"error": str(e)}), 500


# ── Disclaimer endpoint (human-in-the-loop) ───────────────────────────────────
@app.route("/disclaimer")
def disclaimer():
    return jsonify({
        "disclaimer": (
            "Uper provides AI-generated marketing insights based on publicly visible "
            "web content. Analysis is derived from structured signal extraction and "
            "AI reasoning — not platform API integrations or private analytics. "
            "Scores are estimates. Final strategic decisions should be made by "
            "qualified marketing professionals."
        ),
        "data_sources": "Public web content only. No OAuth, no private account data.",
        "ai_model": "OpenAI GPT-4o mini",
    })



# ── Competitor Intelligence endpoint ─────────────────────────────────────────
@app.route("/competitors", methods=["POST"])
@rate_limit(max_per_minute=10)
def competitors():
    if not OPENAI_API_KEY:
        return jsonify({"error": "Server not configured with API key."}), 500

    body = request.get_json(force=True)
    competitor_urls = body.get("competitors", [])
    client_name     = body.get("client_name", "Client")
    uper_analysis   = body.get("uper_analysis")  # optional — passed from frontend
    mode_param      = body.get("mode", "social")

    if not competitor_urls:
        return jsonify({"error": "No competitor URLs provided."}), 400

    logger.info(f"Competitor analysis — client: {client_name}, urls: {len(competitor_urls)}, has_uper: {bool(uper_analysis)}")

    # ── Scrape each competitor ────────────────────────────────────────────────
    PROMO_KEYWORDS = [
        "free consultation","discount","off","sale","limited time","special offer",
        "starting at","flat fee","new client","first visit","membership","package",
        "bundle","complimentary","refer a friend","introductory"
    ]
    SERVICE_KEYWORDS = [
        "tax preparation","tax filing","bookkeeping","accounting","payroll","cpa",
        "botox","filler","laser","facial","hydrafacial","microneedling","coolsculpting",
        "iv therapy","weight loss","semaglutide","massage","acupuncture","prp",
        "seo","social media","marketing","branding","web design","analytics",
        "consulting","strategy","email marketing","paid ads","content"
    ]

    def scrape_competitor(url):
        try:
            resp = requests.get(url, timeout=10, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })
            soup = BeautifulSoup(resp.text, "html.parser") if resp.ok else None
            if not soup:
                return {"url": url, "error": "Could not fetch page"}

            # Strip noise
            for tag in soup(["script","style","nav","footer","head"]):
                tag.decompose()
            text = re.sub(r'\s+', ' ', soup.get_text(separator=" ")).strip().lower()

            # Prices
            prices_raw = re.findall(r'\$[\d,]+(?:\.\d{2})?', text)
            prices = []
            for p in prices_raw:
                try:
                    v = float(p.replace("$","").replace(",",""))
                    if 10 <= v <= 10000:
                        prices.append(p)
                except:
                    pass
            prices = list(dict.fromkeys(prices))[:12]

            title     = soup.find("title")
            headlines = [h.get_text().strip() for h in soup.find_all(["h1","h2"])[:6] if len(h.get_text().strip()) > 5]
            promos    = [kw for kw in PROMO_KEYWORDS if kw in text]
            services  = [kw for kw in SERVICE_KEYWORDS if kw in text]

            # Insight score
            price_vals = []
            for p in prices:
                try: price_vals.append(float(p.replace("$","").replace(",","")))
                except: pass

            if price_vals:
                avg = sum(price_vals) / len(price_vals)
                pricing_agg = "High" if avg < 100 else ("Medium" if avg < 300 else "Low")
                price_range = f"${min(price_vals):.0f}–${max(price_vals):.0f} (avg ${avg:.0f})"
            else:
                pricing_agg = "Unknown"
                price_range = "Not detected"

            def band(val, lo, hi):
                return "High" if val >= hi else ("Low" if val <= lo else "Medium")

            sm = {"High":3,"Medium":2,"Low":1,"Unknown":1}
            threat = round((sm[pricing_agg] + sm[band(len(promos),1,4)] + sm[band(len(services),2,6)]) / 9 * 100)

            # Also try /pricing or /services subpage
            extra_prices, extra_services = [], []
            for path in ["/services", "/pricing", "/treatments"]:
                try:
                    r2 = requests.get(url.rstrip("/") + path, timeout=6, headers={"User-Agent": "Mozilla/5.0"})
                    if r2.ok and len(r2.text) > 500:
                        s2 = BeautifulSoup(r2.text, "html.parser")
                        for t in s2(["script","style","nav","footer"]): t.decompose()
                        t2 = s2.get_text(separator=" ").lower()
                        ep = [p for p in re.findall(r'\$[\d,]+(?:\.\d{2})?', t2)
                              if 10 <= float(p.replace("$","").replace(",","")) <= 10000]
                        extra_prices += ep[:6]
                        extra_services += [kw for kw in SERVICE_KEYWORDS if kw in t2]
                except:
                    pass

            all_prices   = list(dict.fromkeys(prices + extra_prices))[:15]
            all_services = list(set(services + extra_services))

            return {
                "url": url,
                "title": title.get_text().strip() if title else url,
                "headlines": headlines,
                "prices_found": all_prices,
                "promotions_detected": promos,
                "services_detected": all_services,
                "insight_score": {
                    "threat_score": threat,
                    "pricing_aggressiveness": pricing_agg,
                    "promo_intensity": band(len(promos),1,4),
                    "service_breadth": band(len(all_services),2,6),
                    "price_range": price_range
                }
            }
        except Exception as e:
            logger.warning(f"Scrape failed for {url}: {e}")
            return {"url": url, "error": str(e)}

    competitor_data = []
    for url in competitor_urls[:5]:  # cap at 5
        data = scrape_competitor(url)
        competitor_data.append(data)

    # ── Build GPT prompt ──────────────────────────────────────────────────────
    comp_blocks = []
    for i, c in enumerate(competitor_data, 1):
        if "error" in c:
            comp_blocks.append(f"Competitor {i}: {c['url']} — Could not scrape")
            continue
        sc = c.get("insight_score", {})
        comp_blocks.append(f"""Competitor {i}: {c.get('title', c['url'])}
URL: {c['url']}
Headlines: {' | '.join(c['headlines'][:4]) or 'N/A'}
Prices: {', '.join(c['prices_found']) or 'None detected'}
Promotions: {', '.join(c['promotions_detected']) or 'None'}
Services: {', '.join(c['services_detected']) or 'None'}
Threat Score: {sc.get('threat_score','?')}/100 | Pricing: {sc.get('pricing_aggressiveness','?')} | Promos: {sc.get('promo_intensity','?')} | Services: {sc.get('service_breadth','?')}
Price Range: {sc.get('price_range','N/A')}""")

    comp_text = "\n---\n".join(comp_blocks)

    if uper_analysis:
        # Full mode — has Uper brand analysis to compare against
        system_prompt = """You are a senior growth strategist who specializes in market positioning.
You have access to both a brand's own external signal analysis (from Uper) and competitor intelligence.
Your job: identify exactly where the brand stands relative to its market, and recommend specific moves.
Be direct, specific, and actionable. No generic advice."""

        user_prompt = f"""Client: {client_name}

YOUR BRAND ANALYSIS (from Uper):
{uper_analysis}

COMPETITOR INTELLIGENCE:
{comp_text}

Output in EXACTLY this format:

## Executive Summary
2-3 sentences. Where does {client_name} stand vs the market right now? What is the single most important takeaway?

## Your Position vs Market
- How your brand signals compare to competitors on voice, consistency, engagement, and coverage
- Where you are stronger and where competitors have the edge

## Competitor Snapshot
For each competitor:
**[Name]** — Threat: X/100
- Pricing: [range and tactic]
- Promotions: [what they're running]
- Positioning: [their angle in one line]

## Key Gaps
- Specific gaps between your brand and the market that represent risk or opportunity

## Opportunities
- 3-4 concrete moves {client_name} can make to improve market position

## Recommended Actions
1. [Most urgent — within 7 days]
2. [Second priority]
3. [Third priority]
4. [Fourth priority]

## Threat Summary
High: [competitors and why]
Medium: [competitors and why]
Low: [competitors and why]"""

    else:
        # Standalone mode — competitor intelligence only
        system_prompt = """You are a senior market intelligence analyst.
Analyze competitor data and produce a clear, actionable competitive intelligence report.
Be specific and direct. No generic advice. Focus on what the client should do next."""

        user_prompt = f"""Client: {client_name}

COMPETITOR INTELLIGENCE:
{comp_text}

Output in EXACTLY this format:

## Market Overview
2-3 sentences on the competitive landscape.

## Competitor Snapshot
For each competitor:
**[Name]** — Threat: X/100
- Pricing: [range and tactic]
- Promotions: [what they're running]
- Positioning: [their angle in one line]

## Key Patterns
- 3-4 observations about pricing, promotions, and positioning trends

## Opportunities
- 3-4 specific gaps {client_name} could act on

## Recommended Actions
1. [Most urgent]
2. [Second]
3. [Third]
4. [Fourth]

## Threat Summary
High: [competitors and why]
Medium: [competitors and why]
Low: [competitors and why]"""

    # Direct OpenAI call — competitor report returns markdown, not JSON
    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt}
                ],
                "max_tokens": 1600,
                "temperature": 0.4
            },
            timeout=90
        )
        resp.raise_for_status()
        report_text = resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"Competitor GPT call failed: {e}")
        return jsonify({"error": str(e)}), 500

    logger.info(f"Competitor analysis complete — {len(competitor_data)} competitors analyzed")
    return jsonify({
        "mode": "full_positioning" if uper_analysis else "competitor_only",
        "client": client_name,
        "competitor_data": competitor_data,
        "report": report_text
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
