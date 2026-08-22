# P4 startup Program recovery increment evidence

## Capability boundary

Startup recovery now reconciles every durable Collaboration Program's live
wait/admission control state before terminal reconciliation and before the
ordinary graph recovery pump runs. A graph-persisted `WaitingApproval` node is
therefore reprojected as Program `AwaitingApproval` after a restart; Runtime
does not infer a resource or provider wait from volatile state.

## Checks

```text
cargo test -p runtime startup_reconciliation_restores_live_program_approval_wait_state --lib
cargo test -p runtime startup_recovery_rehydrates_and_advances_persistent_execution_graphs --lib
cargo check -p runtime --all-targets
cargo fmt --all -- --check
git diff --check
```

All listed checks passed before commit.

## Residuals

- Resource-wait projection remains gated on a durable ResourceManager lease
  receipt; this increment does not manufacture it from transient queue state.
- P5 projection/audit completion and P6 real-Qwen E2E remain open.
