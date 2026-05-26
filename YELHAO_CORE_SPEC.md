# Yelhao Claude Code Instructions

Yelhao used to be called SignalScope.

Some backend URLs, repo names, and legacy comments may still reference SignalScope. Treat Yelhao as the current product identity.

## Current Product

Yelhao is a live opportunity intelligence layer for local-business discovery.

It is not just:
- a scraper
- a static lead database
- a CRM wrapper
- an Apollo/Openmart clone

Yelhao enriches opportunities, not just contacts.

Core flow:

User input
→ Scout
→ Scout Hypothesis
→ Analyze Verification
→ Evidence-Based Scorecard
→ Role-Based Opportunity Translation
→ Network / CRM / Follow-up

## File Safety Rules

Do not rewrite full files unless explicitly asked.

Do not remove working routes, auth behavior, CRM sync, Scout, Analyze, Network, database persistence, or token scoping.

Do not refactor unrelated code while implementing a small task.

Before editing:
1. Identify exact files needed.
2. Identify exact insertion/replacement points.
3. State what will not be touched.

After editing:
1. Summarize changed files.
2. Provide compile/test commands.
3. Provide exact PowerShell/API tests.
4. Mention risks or assumptions.

If local files appear stale compared with known deployed behavior, ask for the latest file before large edits.

## Core Product Rules

Scout = discovery and opportunity hypothesis.

Analyze = verification and evidence-based explanation.

Network = saved opportunity workflow and follow-up memory.

AI explains evidence. Backend logic verifies and scores.

Blocked is not missing.

Unknown is not negative.

Inferred must be labeled as inferred.

Verified signals should be weighted higher.

If Scout and Analyze disagree, create a discovery gap, correction, or data-quality warning.

Intent Evidence is a boost, not a hard filter.

User role is inferred when possible, not required.

Better input creates better output, but weak input should still return useful results.

Approach fit guides the user; it should not exclude them from better opportunities.

A beginner user can still see high-quality businesses, but the result should be labeled as stretch, premium, or relationship-first when appropriate.

Saturation should track the opportunity angle, not only the business ID.

Asset problems should be translated differently depending on user role.

## Evidence Labels

Use these evidence states when possible:

- verified
- inferred
- blocked
- unknown
- conflicting
- mismatch

Never collapse all source failures into “missing.”

## Coding Style Rules

Avoid em dashes in Python strings and comments because they previously caused issues in Python 3.14/deploy contexts.

Use safe import guards when importing optional modules. Catch Exception where module syntax/import failures could crash Gunicorn before Flask starts.

Preserve old response fields for frontend compatibility when adding new fields.

When changing backend Python files, provide:

```powershell
python -c "import py_compile; py_compile.compile('app.py', doraise=True)"
python -c "import py_compile; py_compile.compile('database_service.py', doraise=True)"
python -c "import py_compile; py_compile.compile('signal_service.py', doraise=True)"
python -c "import py_compile; py_compile.compile('intent_service.py', doraise=True)"
python -c "import py_compile; py_compile.compile('discovery_service.py', doraise=True)"
