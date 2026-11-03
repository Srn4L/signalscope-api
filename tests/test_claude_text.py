"""Deep analysis must read the answer even when a thinking block comes first."""
import os
from types import SimpleNamespace as NS

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import app  # noqa: E402


def test_skips_thinking_block():
    resp = NS(content=[NS(type="thinking", thinking="..."), NS(type="text", text='{"ok": 1}')])
    assert app.claude_text(resp) == '{"ok": 1}'


def test_plain_text_response():
    assert app.claude_text(NS(content=[NS(type="text", text="hi")])) == "hi"


def test_empty_or_missing_content():
    assert app.claude_text(NS(content=[])) == ""
    assert app.claude_text(NS()) == ""
