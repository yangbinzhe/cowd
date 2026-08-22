# P3 `NarrowObjective` atomic increment — 2026-08-23

This record covers one bounded P3 operation. It is not evidence that P3, the
0821 program, or any B/A release gate is complete.

## Durable chain added

- `ApplyCollaborationObjectiveNarrowing` accepts only a validated
  `NarrowObjective` patch at the exact Program revision.
- It resolves the Program's semantic-to-physical Team mapping and requires
  every affected Team node to remain `Planned`.
- The command decodes and replaces only the corresponding durable
  `TeamInstantiationRequest.objective`; Team identity, template selector,
  permission/evidence scopes, acceptance, edge topology and historical
  receipts are preserved.
- Program revision and active control fences advance in the same graph commit,
  followed by full graph validation. The operation is available only through
  the Runtime-attested dynamic-patch ingress.

## Verification

```text
cargo test -p runtime --lib execution_core::graph::commit_service::tests::objective_narrowing_rewrites_only_a_planned_team_request_atomically -- --nocapture
# 1 passed

cargo test -p runtime --lib -q
# 1799 passed, 0 failed, 2 ignored

git diff --check
```

## Still open

- Runtime still needs policy evidence that a proposed objective change is a
  true narrowing versus an expansion that introduces new obligations.
- `SetParallelismHint`, split/merge and dispute operations still need atomic
  graph mutations and policy outcomes.
- P4--P6, all B gates and the real-Qwen A12 gate remain open.
