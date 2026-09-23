# Behavioral health intake and scheduling: first vertical slice

## Goal

Turn the existing Yelhao backend into a learning project for healthcare workflow systems and implementation. Model a **fictional outpatient behavioral health clinic** and follow one referral from inquiry through attendance and follow-up. This is a prototype with synthetic data, not an EHR or a system for real patient information.

This first slice teaches process mapping, relational design, SQL, state transitions, permissions, audit history, and acceptance testing. The current business discovery product can keep running while the healthcare module is built separately.

## Existing system and boundary

`app.py` serves discovery, analysis, Networks, and CRM routes. `schema.sql` and `database_service.py` persist businesses, searches, opportunities, opportunity states, and discovery events in PostgreSQL. Those records describe **business leads** and outreach. A patient's referral or appointment is a different entity: do not reuse `businesses`, `opportunities`, `opportunity_states`, or token-hashed discovery events for patient workflow. Start with separate `clinic_*` tables and a separate route namespace.

Before any live patient data, replace the current shared master/guest codes and permissive CORS with individual identities, role-based authorization, a documented retention policy, and an appropriate deployment/security review. Until then, use invented names and invented details only; never paste actual records, insurance identifiers, notes, or patient contact information into this repo, demo, logs, or third-party AI services.

## Workflow and responsibilities

| Stage | Entry | Required work | Exit |
| --- | --- | --- | --- |
| Referral / inquiry | Staff records a synthetic source and request | Check for duplicate open cases; record contact preference | Intake pending |
| Intake | Intake coordinator gathers required administrative fields and consent status | Flag missing fields; no clinical diagnosis or treatment recommendation | Eligibility pending |
| Eligibility | Authorized coordinator records verification outcome and timestamp | If unknown or failed, route to manual resolution; no automatic coverage promise | Provider match pending, or blocked |
| Provider match | Scheduler selects an available provider based on configured service, location, and availability | Human confirms suitability; no automated clinical triage | Scheduling pending |
| Scheduling | Scheduler offers and confirms a slot | Prevent double booking; record confirmation and timezone | Appointment scheduled |
| Appointment | Staff updates arrival and result | Distinguish arrived, no-show, cancelled, and rescheduled | Attendance recorded |
| Follow-up / outcome | Coordinator records next administrative action and due date | Assign owner; track completion separately from attendance | Closed or active follow-up |

A blocked eligibility result remains visible and assignable. A cancellation returns to scheduling when the case remains open. A no-show may create a follow-up task; it never implies treatment failure. Every state change has an actor, timestamp, and reason. Staff may correct a mistaken state through an explicit logged correction, not by rewriting history.

## Proposed relational model

| Table | Essential fields | Purpose |
| --- | --- | --- |
| `clinic_cases` | id, synthetic_patient_ref, source, service_requested, status, assigned_owner_id, opened_at, closed_at, version | One intake journey; reference is a made-up demo identifier |
| `clinic_intakes` | id, case_id, completeness_status, consent_status, captured_at, captured_by | Administrative intake checklist |
| `clinic_eligibility_checks` | id, case_id, result (unknown/verified/blocked), checked_at, checked_by, reason_code | Historical verification attempts; no policy number in the demo |
| `clinic_providers` | id, display_name, service_type, location, active | Synthetic provider directory |
| `clinic_slots` | id, provider_id, starts_at, ends_at, status | Capacity with a single reservation per slot |
| `clinic_appointments` | id, case_id, slot_id, status, booked_by, booked_at | Appointment history; reschedules create/link a new booking |
| `clinic_tasks` | id, case_id, kind, assigned_to, due_at, status, completed_at | Follow-up queue |
| `clinic_users` | id, display_name, role, active | Demo actor identities; production auth remains future work |
| `clinic_events` | id, case_id, actor_id, event_type, prior_state, new_state, reason, occurred_at | Append-only audit trail |

Use foreign keys, constrained status values, UTC timestamps, indexes on case/status/due date, and a database uniqueness constraint so a slot cannot have two active bookings. Keep case status separate from appointment and task status. No names, dates of birth, addresses, clinical notes, payer IDs, or real contact details are needed to demonstrate this workflow.

## First implementation slice

1. Add an isolated, repeatable PostgreSQL migration for `clinic_cases`, `clinic_events`, `clinic_users`, and `clinic_tasks`; leave the existing schema and routes alone. Seed only synthetic actors and cases.
2. Implement case creation, case detail, an allowed transition endpoint, and an overdue follow-up list under `/clinic/demo/`. Each transition writes the case update and event in one transaction, with an expected `version` check to reject stale edits.
3. Validate the path with one synthetic case, an invalid transition, a stale concurrent edit, a missing actor, and an overdue task. Record the expected responses and SQL checks.
4. Add eligibility, provider availability, and appointment tables/endpoints only after the first case lifecycle is reliable. Add a simple dashboard for stage counts and overdue tasks.
5. Document a fictional clinic's requirements, a workflow diagram, and UAT scenarios. Then map the learned concepts to healthcare systems or implementation job descriptions. Explore interoperability (HL7/FHIR) after this local workflow is understood.

## Initial acceptance criteria

- A synthetic referral can enter `intake_pending`, advance to `eligibility_pending`, and produce a dated audit event for each change.
- A case cannot jump from `intake_pending` directly to `appointment_scheduled`.
- An update with an old version fails without changing either the case or its event history.
- A task due in the past and still open appears in the overdue list; a completed task does not.
- Case queries are scoped to a demo actor and role; an unknown or inactive actor cannot mutate cases.
- No existing Scout/Networks route or table changes as part of this first slice.

## Learning checkpoints

For each stage, explain the business rule in plain English, write the SQL query that supports the screen or report, and run a UAT scenario. The artifact should make it possible to discuss workflow discovery, implementation, data quality, SQL, access control, and troubleshooting in an interview. The product direction is healthcare workflow systems and implementation, with a possible service offering for small practices after a secure, real-world deployment is designed.
