# P5 public projection boundary increment evidence

## Capability boundary

`ExecutionGraphProjection` and the event-sourced graph-store projection now
expose `execution-payload:<node-id>` as a stable opaque identity instead of
copying a node's Runtime-owned payload. This prevents a public inspection
surface from receiving private prompts, Agent bindings, or ephemeral Team
template snapshots. Runtime's completed-Team assessment reloads the exact
payload from the governed graph store, preserving internal verification.

## Checks

```text
cargo test -p harness-contract public_work_projection_omits_private_context_and_model_bindings --lib
cargo test -p runtime state_store_projection_keeps_node_payloads_opaque --lib
cargo test -p runtime add_team_patch_compiles_to_an_exact_active_program_revision --lib
cargo test -p runtime startup_reconciliation_restores_live_program_approval_wait_state --lib
cargo check -p harness-contract --all-targets
cargo check -p runtime --all-targets
cargo check -p gateway --all-targets
cargo fmt --all -- --check
git diff --check
```

All listed checks passed before commit.

## Residuals

- Operator-facing Program timeline/audit aggregation beyond graph projection
  remains P5 follow-up work.
- P6 real-Qwen E2E remains open.
