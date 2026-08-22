# P3 `RetireTeam` atomic increment — 2026-08-23

This record covers one bounded P3 operation. It is not evidence that P3, the
0821 program, or any B/A release gate is complete.

## Durable chain added

- `ApplyCollaborationTeamRetirement` accepts only a validated `RetireTeam`
  patch that matches the exact Program id and revision.
- Retirement is allowed only while the physical Team node is `Planned`; a
  ready/running/admitted Team cannot be silently removed.
- Removing a required Team requires a non-empty user confirmation reference.
- One graph revision cancels the physical Team node and removes the matching
  Program instance, obligation, semantic mapping, incident typed handoffs and
  completion requirement. The Program and active resource-obligation fences
  advance together, then the full graph validator runs before append.

## Verification

```text
cargo test -p harness-contract --lib -q
# 185 passed

cargo test -p runtime --lib execution_core::graph::commit_service::tests::retirement_cancels_only_a_confirmed_unstarted_team_and_revises_program_atomically -- --nocapture
# 1 passed

cargo test -p runtime --lib -q
# 1797 passed, 0 failed, 2 ignored

git diff --check
```

The retirement test covers missing user confirmation, successful atomic
retirement, removal from every Program/graph contract surface, and rejection
after the Team leaves `Planned`.

## Still open

- This command is not yet exposed through the validated production
  Coordinator ingress; `NarrowObjective` and `SetParallelismHint` likewise
  still need their own atomic graph mutations.
- P3's ephemeral-template and escalation-source-of-truth work remains open.
- P4--P6, all B gates and the real-Qwen A12 gate remain open.
