# P4 startup Program recovery increment evidence

## Capability boundary

Startup recovery now reconciles every durable Collaboration Program's live
wait/admission control state before terminal reconciliation and before the
ordinary graph recovery pump runs. A graph-persisted `WaitingApproval` node is
therefore reprojected as Program `AwaitingApproval` after a restart; Runtime
does not infer a resource or provider wait from volatile state.

The resource-pressure path is also non-terminal: when a ready graph cannot
obtain a temporary permit, the completion pump waits for the
`ExecutionResourceManager` change notification and resumes the same graph
after a lease release. It no longer returns `Completed` merely because the
temporary queue left no active node. Program resource ledgers now reject a
mix of aggregate-only and exact Team reservations, and reject an aggregate
that does not equal the lossless sum of its exact Team obligations. This
prevents a replan/restart from recovering a Program with a silently drifted
capacity claim.

## Checks

```text
cargo test -p runtime startup_reconciliation_restores_live_program_approval_wait_state --lib
cargo test -p runtime startup_recovery_rehydrates_and_advances_persistent_execution_graphs --lib
cargo test -p runtime resource_pressure_keeps_a_ready_graph_pump_alive_until_a_lease_releases --lib
cargo test -p runtime continuation_retry_returns_the_existing_root_to_the_execution_runner --lib
cargo test -p runtime collaboration_program_revision_keeps_prior_obligations_and_adds_new_teams --lib
cargo test -p harness-contract active_program_control_requires_exact_obligations_and_technical_ledger --lib
cargo check -p runtime --all-targets
cargo fmt --all -- --check
git diff --check
```

All listed checks passed before commit.

## Residuals

- Resource-wait projection remains gated on a durable ResourceManager lease
  receipt; the pump resumes correctly after a live lease release, but this
  increment does not manufacture an operator-visible durable queue receipt
  from transient resource state.
- The full continuation/admission/effect crash matrix, ambiguity-to-
  `WaitingInput`, and evidence-progress/model-hint soft-demand reconciliation
  remain P4 work; these tests close only the listed recovery and ledger
  invariants.
- P5 projection/audit completion and P6 real-Qwen E2E remain open.
