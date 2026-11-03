"""
Fast analysis must not overstate social presence or confidence.
Reproduces the live-test case: profiles linked but blocked, AI claimed a strong
Instagram presence, scored TikTok, and reported high confidence.
"""
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import app  # noqa: E402

BLOCKED_IG = "[INSTAGRAM PRESENCE DETECTED - content blocked by platform]"
FB_META = "[FACEBOOK METADATA]: title: Angel of Your Nails"
REAL_TEXT = "x" * 400


def test_social_coverage_counts_content_not_links():
    dq = app.build_data_quality({"home": "hi"}, {"instagram": BLOCKED_IG, "facebook": FB_META, "tiktok": BLOCKED_IG}, "", [])
    social = dq["social"]
    assert social["pct"] == 10, social
    assert social["level"] == "low"
    assert social["blocked"] == ["instagram", "tiktok"]
    assert social["metadata_only"] == ["facebook"]
    assert "blocked" in social["detail"]


def test_verified_social_still_scores_high():
    dq = app.build_data_quality({}, {"instagram": REAL_TEXT, "tiktok": REAL_TEXT}, "", [])
    assert dq["social"]["pct"] == 70
    assert dq["social"]["level"] == "high"


def test_verify_fast_report_strips_unverified_claims():
    social_text = {"instagram": BLOCKED_IG, "facebook": FB_META}
    dq = app.build_data_quality({"home": "hi"}, social_text, "", [])
    report = {
        "overall_score": 60,
        "signal_confidence": "High",
        "platform_scores": {"Instagram": 70, "TikTok": 28},
        "signal_map_data": {"platforms_present": ["Instagram", "Facebook", "TikTok"]},
        "scores": {"platform_coverage": {"score": 80, "confidence": "high"}},
    }
    out = app.verify_fast_report(report, social_text, dq)
    assert out["platform_scores"] == {}
    assert out["signal_confidence"] == "Low"
    assert out["scores"]["platform_coverage"]["confidence"] == "low"
    assert out["signal_map_data"]["platforms_present"] == ["Instagram", "Facebook"]
    assert out["overall_score"] == 60
    assert set(out["verification"]["platform_scores_removed"]) == {"Instagram", "TikTok"}


def test_verify_keeps_verified_platform_scores():
    social_text = {"instagram": REAL_TEXT}
    report = {"platform_scores": {"Instagram": 72}, "signal_confidence": "Low"}
    out = app.verify_fast_report(report, social_text, {"social": {"pct": 35}})
    assert out["platform_scores"] == {"Instagram": 72}
    assert out["signal_confidence"] == "Low"


def test_verify_never_raises_on_bad_input():
    assert app.verify_fast_report(None, {}, {}) is None
    assert "verification" in app.verify_fast_report({"platform_scores": "oops"}, None, None)


def test_linked_but_unfetched_platform_stays_present():
    # LinkedIn was linked but returned nothing: blocked is not missing.
    report = {"platform_scores": {"LinkedIn": 60}, "signal_map_data": {"platforms_present": ["LinkedIn"]}}
    out = app.verify_fast_report(report, {}, {}, {"linkedin": "https://linkedin.com/x"})
    assert out["signal_map_data"]["platforms_present"] == ["LinkedIn"]
    assert out["platform_scores"] == {}


def test_deep_live_case_linkedin_blocked_score_removed():
    social_text = {"facebook": "x" * 400, "instagram": "[INSTAGRAM METADATA]: title: Angel",
                   "linkedin": "[LINKEDIN PRESENCE DETECTED - content blocked by platform]"}
    report = {"platform_scores": {"Facebook": 65, "Instagram": 70, "LinkedIn": 60}, "signal_confidence": "High"}
    out = app.verify_fast_report(report, social_text, {"reviews": {"pct": 100}, "website": {"pct": 75}})
    assert out["platform_scores"] == {"Facebook": 65}
