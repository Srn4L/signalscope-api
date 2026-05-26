# Yelhao Roadmap

## Current Stage

Yelhao is a working but unstable MVP.

It has:
- Scout
- Analyze
- Network
- CRM sync
- Postgres persistence
- token-scoped saved opportunities
- discovery tracking
- early Scout Refresh / source rotation
- scoring and signal logic

But several intelligence layers are incomplete, inconsistent, or need validation.

The current goal is to stabilize the core loop:

Scout → Analyze → Network → Outcome tracking

Do not add major new feature branches until the immediate stability issues are handled.

---

## Immediate Priorities

These are must-fix before more feature expansion.

### 1. Fix Network Save Schema Mismatch

Problem:
`/opportunities/save` can fail because live Postgres is missing `service_angle` or related columns expected by database_service.py.

Goal:
Live DB schema and schema.sql must match the code.

Likely actions:
- Add missing columns to live Postgres.
- Update schema.sql to include those columns.
- Verify `/opportunities/save` works.
- Verify `+ Network` works from Scout cards.

Validation:
- Save a Scout result to Network.
- Confirm opportunity row is created.
- Confirm opportunity_state row is created.
- Confirm token_hash is scoped correctly.
- Confirm duplicate saves return existing row or already_saved behavior.

### 2. Verify Analyze Source Contamination Fix

Problem:
Analyze previously analyzed one business using scraped website content from a previous business.

Goal:
Each Analyze job must use fresh local variables and correct source context.

Validation:
- Run Analyze on two different businesses back to back.
- Confirm website/domain/content belongs to the requested business.
- Confirm wrong-domain results create warning and lower confidence.
- Confirm cached response key includes business name, website, and location.

### 3. Reduce DDG Dependence

Problem:
DuckDuckGo supplemental search has rate-limited and degraded signal quality.

Goal:
Route discovery/context searches through better providers and use DDG only as a last resort.

Preferred stack:
- Google Places for local discovery
- SerpAPI for operator/discovery search
- Tavily for context/research
- Firecrawl for clean website text
- ScraperAPI only for blocked raw HTML fallback

Validation:
- Search logs show provider used.
- DDG failure does not collapse social/pricing/competitor scores to zero.
- Blocked/unknown is shown as not verified, not missing.

### 4. Stabilize Analyze Scoring

Problem:
Scout and Analyze can contradict each other. Scout can overweight ratings/reviews. Analyze can underweight verified demand if supplemental sources fail.

Goal:
Analyze should use structured evidence and deterministic scorecard before AI explanation.

Validation:
- Strong reviews/demand should not produce near-zero opportunity score unless there is a clear disqualifier.
- Unknown social/pricing/competitor data should reduce confidence, not become a zero.
- Old frontend score field should map to scorecard value for compatibility.

### 5. Fix Frontend Wording for Unknown / Not Verified

Problem:
UI can show `0` or “missing” when the real state is unknown, blocked, or not verified.

Goal:
Show:
- not verified
- blocked
- unknown
- possible mismatch
- verified
- inferred

Validation:
- Blocked source does not show as confirmed missing.
- Unknown social does not show as 0 percent weakness.
- Website issue states are specific.

### 6. Sync Working Files

Problem:
Some project snapshots are stale compared with deployed code.

Goal:
Make sure repo files, local files, and deployed files align before large Claude Code edits.

Validation:
- app.py, index.html, database_service.py, signal_service.py, intent_service.py, discovery_service.py, schema.sql match intended deployed version.
- Do not patch stale copies.

---

## Next Build Tasks

These are important after immediate stability is handled.

### Task 1: Analyze Evidence Scorecard

Goal:
Make Analyze evidence-based instead of AI-opinion-based.

Add or confirm:
- analyze_evidence
- analyze_scorecard
- source_status_summary
- data_quality_warnings
- scout_vs_analyze
- role_based_opportunity

Rules:
- AI explains evidence.
- Backend verifies and scores.
- Unknown lowers confidence, not score.
- Blocked is not missing.
- Wrong source returns warning.

Likely files:
- app.py
- signal_service.py
- maybe intent_service.py
- maybe frontend index.html

Do not break:
- /agent
- /job/<id>
- old response fields
- CRM sync

### Task 2: Flexible User Context + Approach Fit

Goal:
Infer user role and experience from natural language without forcing onboarding.

Outputs:
- user_context
- approach_fit
- recommended_first_offer

Behavior:
- Strong user input creates sharper recommendations.
- Weak input still returns useful results.
- User role is inferred, not required.
- Beginner users can still see premium businesses, but with stretch or relationship-first labels.
- Approach fit guides, not excludes.

Likely files:
- intent_service.py
- signal_service.py
- app.py
- index.html

### Task 3: Asset Health + Role Translation

Goal:
Detect and explain asset states such as:
- verified_working
- not_found_404
- expired_domain
- parked_domain
- domain_for_sale
- wrong_business
- redirect_mismatch
- booking_platform_only
- social_only
- blocked
- timeout
- unknown

Then translate the asset issue by user role.

Examples:
- content creator: social-first trust and CTA content
- web designer: landing page or booking flow repair
- photographer: visual proof assets
- SEO/local marketer: GBP/citation cleanup
- CRM marketer: lead capture and rebooking
- partnership/event user: offline/community conversion

Likely files:
- signal_service.py
- app.py
- index.html

### Task 4: Intent Evidence Boost

Goal:
Detect public signs of possible readiness without making them required.

Signals:
- DM for collab
- looking for photographer
- website coming soon
- booking link broken
- grand opening
- new location
- new menu
- hiring
- vendor wanted
- pop-up
- sponsor
- reviews appreciated
- looking for content creator
- need website
- social media help

Rules:
- Boost, not filter.
- No intent evidence should not remove the candidate.
- Do not overclaim.
- Label source and confidence.

Likely files:
- intent_service.py
- discovery_service.py
- signal_service.py
- app.py

### Task 5: Scout Refresh Quality

Goal:
Make Refresh Opportunities return meaningfully different businesses under the same user intent.

Improve:
- source rotation
- social-indexed discovery
- booking-indexed discovery
- adjacent category expansion
- repeat suppression
- saturation use
- refresh metadata

Validation:
- Refresh uses same query context.
- seen_names are suppressed.
- next_cursor advances.
- source_strategy appears.
- results differ from initial Scout.

Likely files:
- discovery_service.py
- app.py
- index.html

### Task 6: First-Class Need Score

Goal:
Clarify whether Need Score is separate from gap score.

Need Score should estimate how much help the business may need, not how good the business is.

Validation:
- Need Score appears as its own field or is clearly mapped.
- Strong demand plus visible friction can produce high opportunity.
- Weak demand plus many gaps should not automatically be high opportunity.

Likely files:
- signal_service.py
- app.py
- index.html

### Task 7: Freshness Badge

Goal:
Surface whether a signal is fresh, recent, stale, or unknown.

Validation:
- Current Scout discovery = fresh.
- Recently analyzed/verified = fresh/recent.
- Old DB-only result = stale/unknown.
- Blocked source = unknown freshness.

Likely files:
- signal_service.py
- app.py
- index.html

### Task 8: Saturation Downranking

Goal:
Use existing discovery_events and opportunity_states to downrank repeated opportunity angles.

Track:
- business
- user role
- service angle
- problem signal
- contact path
- source
- timing
- outcome

Rules:
- Same business plus same angle plus same contact path = strong penalty.
- Same business plus new role/angle/fresh event can remain valid.
- Do not permanently blacklist businesses just because they were shown once.

Likely files:
- database_service.py
- signal_service.py
- app.py

### Task 9: Outcome Tracking

Goal:
Track whether opportunities turn into replies, meetings, wins, losses, ignored, or hidden.

Why:
Outcome data is needed to make scoring less opinion-based over time.

Add or support:
- contacted
- replied
- meeting_booked
- won
- lost
- ignored
- hidden
- no_reply
- converted

Use this later to improve scoring.

Likely files:
- database_service.py
- schema.sql
- app.py
- index.html

---

## Later Expansion

Do not build these until the core loop is stable.

### Paid Ads Readiness

Goal:
Detect whether a business is ready for paid growth or would waste ad spend until the funnel is fixed.

Signals:
- strong demand
- clear offer
- conversion path
- website/landing page
- tracking pixels
- competitor ads
- high-intent category
- seasonality
- creative assets

Output:
- paid_ads_readiness
- recommended channel
- warnings
- prerequisite fixes

### Watchlists / Autopilot

Goal:
User can save a Scout intent and Yelhao can keep watching for matching opportunities.

Rules:
- Do not contact businesses automatically.
- Detect → rank → suggest → user approves.
- No automatic outreach.

### Contact Finding

Goal:
Find best contact path or decision maker.

Caution:
Do not compete directly with Openmart/Apollo on pure data volume.

Focus:
- best contact path
- confidence
- reason
- role-specific approach

### Outreach Drafting

Goal:
Draft messages tied to verified evidence, user role, and opportunity angle.

Rules:
- Do not invent facts.
- Do not send automatically.
- User approval required.

### Credit System

Goal:
Protect API costs.

Potential credit model:
- Scout = cheap/free
- Refresh = low credits
- Analyze = more credits
- Deep Analyze = highest credits
- Save to Network = free
- CRM sync = free or low credits

### Public Beta

Goal:
Test with real users.

Target wedges:
- content creators
- freelancers
- web designers
- photographers
- local marketers
- small agencies
- service providers
- later SDR/BDR/growth teams

Validation metrics:
- businesses scouted
- saved opportunities
- contacted
- replies
- meetings
- won clients
- ignored/hidden
- score-to-outcome correlation

---

## Known Risks

1. Stale files used as base for patches.
2. Manual web editor diffs causing syntax/indentation bugs.
3. Frontend is a large single-file artifact.
4. Schema.sql can drift from live DB.
5. Module-level imports can crash Gunicorn.
6. DDG is unreliable as primary supplemental search.
7. AI-generated scoring can feel random without deterministic evidence.
8. Unknown data can accidentally display as zero.
9. Source contamination can destroy trust.
10. Feature expansion can outrun product clarity.

---

## Priority Rule

Do not chase every new feature.

Current priority is:

1. Make Scout useful.
2. Make Analyze trustworthy.
3. Make Network reliable.
4. Track outcomes.
5. Then expand intelligence layers.
