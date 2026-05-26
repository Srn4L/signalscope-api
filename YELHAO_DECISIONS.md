# Yelhao Product Decisions

This file records durable product decisions so future AI sessions do not restart or reverse prior thinking.

## Naming

Decision:
Yelhao used to be called SignalScope.

Decision:
SignalScope was the original analysis/reporting concept. Yelhao is the broader opportunity intelligence product.

Decision:
Some URLs, repo names, and backend references may still say SignalScope. Do not rename infrastructure casually without a migration plan.

## Core Direction

Decision:
Yelhao is a live opportunity intelligence layer, not a static lead database.

Decision:
Yelhao enriches opportunities, not just contacts.

Decision:
The core object is not only a business record. The core object is a business plus role, signal, angle, evidence, timing, confidence, saturation, and next action.

Decision:
Yelhao should not compete mainly on having the biggest local business database.

Decision:
Yelhao should compete on opportunity interpretation, verification, freshness, saturation, role fit, and actionability.

## Product Flow

Decision:
Scout is discovery and hypothesis.

Decision:
Analyze is verification and evidence-based explanation.

Decision:
Network is saved opportunity workflow and follow-up memory.

Decision:
CRM sync is an action layer, not the core product.

Decision:
Scout should not auto-create Network rows. User saves should create Network rows.

Decision:
Analyze should consume Scout context when available instead of starting from scratch.

## AI and Scoring

Decision:
AI should explain evidence. Backend logic should verify and score.

Decision:
AI should not invent raw opportunity scores.

Decision:
Scores should be deterministic, explainable, and based on structured evidence.

Decision:
If AI output contradicts structured evidence, preserve the backend scorecard and soften or sanitize the AI wording.

Decision:
Failed supplemental search should lower confidence, not create fake zero scores.

Decision:
Scout and Analyze scoring should be calibrated together so they do not contradict each other.

Decision:
Scout can overweight Google rating/review prominence, and Analyze can underweight verified demand when supplemental sources fail. Both need evidence-based calibration.

## Evidence Rules

Decision:
Blocked is not missing.

Decision:
Unknown is not negative.

Decision:
Inferred must be labeled as inferred.

Decision:
Verified signals should be weighted higher.

Decision:
Wrong website or source mismatch should create a data-quality warning and lower confidence.

Decision:
If Scout and Analyze disagree, create a discovery gap, correction, or warning.

Decision:
Do not collapse website failures into “no website.” Distinguish 404, expired, parked, wrong business, redirect mismatch, blocked, timeout, social-only, booking-only, and unknown.

## User Input and Context

Decision:
User role is inferred when possible, not required.

Decision:
Do not force users to provide work history.

Decision:
Do not force mandatory onboarding before Scout.

Decision:
Better input creates better output.

Decision:
Weak input should still produce meaningful results.

Decision:
If role is unknown, return broader opportunity angles and suggest a better query.

Decision:
If role is inferred, label it inferred.

Decision:
If role is explicit, use sharper role-based recommendations.

## Approach Fit

Decision:
Approach fit guides, not excludes.

Decision:
Beginner users should not be excluded from high-quality businesses.

Decision:
Successful or premium businesses can appear for beginner users, but should be labeled as stretch, premium, or relationship-first when appropriate.

Decision:
For harder targets, recommend safer first offers such as sample work, audits, low-risk content, small experiments, or relationship-first outreach.

Decision:
Lower-demand businesses should not be shown only because they are easier. They still need some demand, friction, timing, or contactability signal.

## Business Quality

Decision:
Yelhao should not only focus on successful businesses.

Decision:
Yelhao should not only focus on struggling businesses.

Decision:
The best opportunities usually combine demand, friction, timing, contactability, low saturation, and role fit.

Decision:
A high-demand business with clear friction can be a strong opportunity.

Decision:
A low-demand business with many gaps may still be low actionability if it lacks budget, activity, or contact path.

Decision:
Demand and friction should be evaluated together.

## Intent Evidence

Decision:
Intent Evidence is a boost, not a hard filter.

Decision:
No intent evidence should not remove a candidate.

Decision:
Intent Evidence should explain why a business may be warmer, more timely, or more ready.

Decision:
Intent Evidence includes signals like collab, hiring, launch, new location, grand opening, booking broken, website coming soon, looking for photographer, looking for content creator, vendor wanted, sponsor, and social media help.

Decision:
Do not overclaim intent. Say “publicly signaled possible interest” or “suggests a possible outreach angle” unless the evidence directly says they need help.

## Asset Health and Role Translation

Decision:
Website problems should not only create web-designer opportunities.

Decision:
Asset problems should be translated differently by user role.

Decision:
A broken website can mean:
- content creator: social-first trust and CTA content
- web designer: landing page or booking flow repair
- photographer/videographer: visual proof assets
- SEO/local marketer: search/citation cleanup
- CRM/retention marketer: lead capture and follow-up
- partnership/event person: offline/community conversion
- SDR/BDR: outreach hook and account prioritization

Decision:
Asset health is a business-friction signal, not only a technical signal.

## Saturation

Decision:
Saturation should track opportunity angle, not only business ID.

Decision:
A business can be saturated for one role, angle, or contact path but still fresh for another.

Decision:
Saturation should include:
- business
- user role
- service angle
- problem signal
- contact path
- source
- timing
- outcome

Decision:
Do not permanently kill a business just because it was shown once.

Decision:
Same business plus same angle plus same contact path should be penalized heavily.

Decision:
Same business plus new signal, different angle, or different role may still be valid.

Decision:
Outcome tracking should eventually improve saturation and scoring.

## Network and Persistence

Decision:
Token scoping should use hashed tokens only. Never store raw tokens.

Decision:
Missing token should return None, not a hash of an empty string.

Decision:
Network save should be explicit user action.

Decision:
Discovery events should be logged from Scout previews so saturation can be calculated later.

Decision:
Opportunity states should track workflow progress, follow-ups, statuses, contact method, and notes.

## Competitive Positioning

Decision:
Openmart validates the market but should not define Yelhao.

Decision:
Openmart is similar on local business discovery, owner/contact enrichment, and AI smart prospecting.

Decision:
Yelhao should not try to beat Openmart first on data scale.

Decision:
Yelhao should differentiate through live opportunity intelligence, role-based translation, evidence verification, freshness, saturation, and Scout to Analyze validation.

Decision:
Clay, Apollo, ZoomInfo, Sales Navigator, RedShip, GummySearch, 6sense, and Warmly are useful reference categories, but Yelhao’s wedge is opportunity enrichment.

## Roadmap Philosophy

Decision:
Do not panic-build every feature.

Decision:
Prioritize the core loop:
Scout → Analyze → Network → Outcome tracking.

Decision:
Stability and trust matter more than adding more intelligence layers.

Decision:
The next business proof should come from users, replies, meetings, saved opportunities, and conversions.

Decision:
Yelhao must prove that its opportunities lead to real action, not just impressive reports.
