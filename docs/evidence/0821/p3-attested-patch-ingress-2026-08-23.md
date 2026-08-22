# P3 attested dynamic-patch ingress increment — 2026-08-23

This record covers the Coordinator ingress boundary for two previously added
atomic commands. It is not evidence that P3, the 0821 program, or any B/A
release gate is complete.

## Durable/production route

- The ordinary `submit_collaboration_intent_patch` route now rejects
  `ChangeEdge` and `RetireTeam`; a caller-supplied `source_attempt` string is
  not treated as an authorization fact.
- `submit_attested_collaboration_intent_patch` compares that patch field with
  the Runtime-derived expected attempt before it loads the Program and submits
  `ApplyCrossTeamEdgePatch` or `ApplyCollaborationTeamRetirement`.
- The result is rebuilt from the canonical graph projection after command
  commit. It carries the canonical digest request id and an explicit
  `runtime_attested_source_attempt` authorization marker.
- Managed-Agent escalation now uses the attested route. Add/Review retain the
  existing semantic compiler route; unsupported patch operations still return
  typed rejections rather than falling back to arbitrary graph JSON.

## Verification

```text
cargo test -p runtime --lib orchestration::tests::add_team_patch_compiles_to_an_exact_active_program_revision -- --nocapture
# 1 passed

cargo test -p runtime --lib execution_core::graph::commit_service::tests::retirement_cancels_only_a_confirmed_unstarted_team_and_revises_program_atomically -- --nocapture
# 1 passed

cargo test -p runtime --lib -q
# 1797 passed, 0 failed, 2 ignored

git diff --check
```

## Still open

- The ingress needs an end-to-end Gateway test using a managed-Agent binding,
  plus an actual user-approval receipt verifier for user-initiated retirement.
- `NarrowObjective`, `SetParallelismHint`, split/merge and dispute operations
  still need their own atomic graph mutations and policy outcomes.
- P4--P6, all B gates and the real-Qwen A12 gate remain open.
