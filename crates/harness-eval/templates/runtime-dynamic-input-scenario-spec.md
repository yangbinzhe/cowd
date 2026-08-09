# Runtime Dynamic Input Scenario Spec

This specification evaluates inputs that arrive while a Turn is running. It
requires durable business evidence; a UI screenshot or a successful build is
not proof that a Task, Team, Session dispatch, or graph was materialized.

## Required Evidence Package

```text
dynamic-input-run/
  report.md
  summary.json
  requests/
  input-projections/
  disposition-receipts/
  task-team-graphs/
  runtime-events/
  recovery/
  token-usage/
  latency/
```

Every report records input count, model rounds, input/output tokens, first
receipt latency, materialization latency, terminal latency, Task/Team/Agent/tool
counts, retry count, recovery count, and the final correctness judgment.

## Invariants

- Every checkpoint slot is covered exactly once by one typed disposition.
- The model chooses semantic work only; Runtime binds physical identities.
- A disposition group has one durable ID and one lowest-sequence leader.
- `Prepared -> Materializing -> Applied|Failed` is atomic for the whole group.
- Replaying or recovering an Applied disposition creates no second entity.
- Structural actions cannot execute stale ordinary tool calls from the same
  model response.
- SQLite and PostgreSQL produce the same decision, status, receipt, and refs.
- WebUI and TUI project the same receipt revision and load referenced details
  only when requested.

## Scenario Matrix

| Scenario | Expected disposition | Required proof |
|---|---|---|
| current_constraint | amend_current_turn | Goal revision changes; no Task is created |
| current_path_correction | replan_current_graph | Fresh graph step uses the correction; stale tools do not run |
| add_three_researchers | add_team_lane | One Team run, three Agent instances, delegated Tasks and parent lineage |
| manufacturing_what_if_background | add_background_task | Additional Task and admitted background graph; current Turn remains responsive |
| task_team_synthesis | add_task_with_team | One additional Task, Team, child graph, and synthesis evidence |
| reusable_team_definition | add_required_task | Governance Task and definition artifact, not an accidental active Team run |
| replace_unrelated_work | replace_current_task | Old Goal is cancelled; input is reclassified once as successor Turn |
| cross_session_dispatch | dispatch_session | Target Session authorization and one durable handoff without transcript copying |
| progress_or_approval | progress_or_control | No graph mutation; control status is durably completed |
| clarification | clarify | No speculative entity; clarification remains attached until terminal commit |
| grouped_updates | one grouped structural action | Three related inputs share one disposition and one entity set |
| independent_updates | three decisions | Three inputs create three independent, traceable dispositions |
| crash_after_materializing | recovered same disposition | Restart resumes the receipt and reuses deterministic entity IDs |
| stale_or_invalid_contract | one repair then blocked | Exactly one contract repair; durable failure evidence and no unbounded retry |

## Reverse Audit

The evaluator fails the run when production source still contains the old
`Additional Mission work` completion path, a second Gateway action classifier,
a generic Gateway handler for `route_input`, mutable receipt data inside
classification JSON, raw execution payloads inside receipts, or model-owned
mutation/execution/revision fields in the disposition graph contract.
