"""Shared test setup: no test may reach OpenAI or the embeddings table."""
import pytest


@pytest.fixture(autouse=True)
def _offline_embeddings(monkeypatch):
    try:
        import embedding_service as es
    except Exception:
        yield
        return
    monkeypatch.setattr(es, "_live_embed", lambda texts, budget_ok=None: None)
    monkeypatch.setattr(es, "_db_get", lambda hashes: {})
    monkeypatch.setattr(es, "_db_put", lambda rows: None)
    monkeypatch.setattr(es, "record_match", lambda q, m: None)
    es._MEM.clear()
    yield
    es._MEM.clear()
