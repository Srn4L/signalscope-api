"""Feature intent: rank under-discovered quality above popularity."""
import os

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import app  # noqa: E402


def biz(name, rating, reviews, **extra):
    d = {"business_name": name, "rating": rating, "review_count": reviews,
         "industry": "restaurant", "website": "https://example-place.com"}
    d.update(extra)
    return d


def score(c, mode="feature"):
    return app.score_and_explain(c, mode)


def test_hidden_gem_beats_popular_spot():
    gem = score(biz("Mama Ade's Kitchen", 4.8, 18))
    famous = score(biz("Famous Diner", 4.5, 2400, instagram="x", tiktok="y"))
    assert gem["opportunity_score"] > famous["opportunity_score"]
    assert "under-discovered" in gem["top_signal"]
    assert "well known" in famous["top_signal"]


def test_zero_reviews_is_unknown_not_negative():
    new = score(biz("Just Opened Patties", 0, 0))
    impacts = {r["key"]: r["impact"] for r in new["score_reasons"]}
    assert impacts.get("unreviewed") == "neutral"
    assert "be the first" in new["top_signal"]
    # Not punished below a mediocre, established place
    meh = score(biz("Average Spot", 3.6, 900))
    assert new["opportunity_score"] > meh["opportunity_score"]


def test_small_sample_not_fully_trusted():
    two_reviews = score(biz("Two Reviews", 5.0, 2))
    twenty_reviews = score(biz("Twenty Reviews", 5.0, 20))
    assert twenty_reviews["signal_scores"]["demand"] > two_reviews["signal_scores"]["demand"]


def test_other_modes_unchanged_shape():
    out = score(biz("Any", 4.5, 120), "outreach")
    for k in ("opportunity_score", "score_reasons", "signal_scores", "top_signal", "inferred_mode"):
        assert k in out
    assert out["inferred_mode"] == "outreach"


def test_intent_inference():
    assert app.infer_objective_mode("hidden food gems in the Bronx") == "feature"
    assert app.infer_objective_mode("underrated restaurants for my food reviews") == "feature"
    assert app.infer_objective_mode("brands to collab with on a pop-up") == "partnership"
    assert app.infer_objective_mode("salons that need better branding for my agency") == "outreach"
    assert app.infer_objective_mode("competitor landscape for my spa") == "market"
    assert "feature" in app.MODE_WEIGHTS
