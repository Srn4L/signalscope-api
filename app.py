import os
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from functools import wraps
import time

app = Flask(__name__)

# Allow requests from your Netlify domain + localhost for dev
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*")
CORS(app, origins=ALLOWED_ORIGINS)

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# ── Simple in-memory rate limiter (per IP, 10 req/min) ──────────────────────
_rate_store = {}

def rate_limit(max_per_minute=10):
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            ip = request.headers.get("X-Forwarded-For", request.remote_addr)
            now = time.time()
            window = 60
            history = _rate_store.get(ip, [])
            history = [t for t in history if now - t < window]
            if len(history) >= max_per_minute:
                return jsonify({"error": "Rate limit exceeded. Try again in a minute."}), 429
            history.append(now)
            _rate_store[ip] = history
            return f(*args, **kwargs)
        return wrapped
    return decorator


# ── Health check ─────────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# ── Main proxy endpoint ───────────────────────────────────────────────────────
@app.route("/analyze", methods=["POST"])
@rate_limit(max_per_minute=10)
def analyze():
    if not OPENAI_API_KEY:
        return jsonify({"error": "Server is not configured with an API key."}), 500

    data = request.get_json(force=True)
    prompt = data.get("prompt")
    max_tokens = data.get("max_tokens", 1400)

    if not prompt:
        return jsonify({"error": "Missing prompt."}), 400

    try:
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
            },
            timeout=60,
        )
        resp.raise_for_status()
        return jsonify(resp.json())

    except requests.exceptions.Timeout:
        return jsonify({"error": "Request to AI service timed out."}), 504
    except requests.exceptions.HTTPError as e:
        try:
            detail = e.response.json()
        except Exception:
            detail = str(e)
        return jsonify({"error": detail}), e.response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
