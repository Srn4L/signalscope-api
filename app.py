import os
import json
import time
import logging
from functools import wraps
from flask import Flask, request, jsonify
from flask_cors import CORS
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
