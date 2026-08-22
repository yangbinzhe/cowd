# P5 public projection boundary increment evidence

## Capability boundary

`ExecutionGraphProjection` and the event-sourced graph-store projection now
expose `execution-payload:<node-id>` as a stable opaque identity instead of
copying a node's Runtime-owned payload. This prevents a public inspection
surface from receiving private prompts, Agent bindings, or ephemeral Team
template snapshots. Runtime's completed-Team assessment reloads the exact
payload from the governed graph store, preserving internal verification.

The runtime projection lane has also passed its release-mode foreground
latency guard both without projector catch-up and while processing a 512-item
catch-up backlog. The measurements exercise 2,000 and 10,000 foreground
samples respectively; they establish that projection work does not silently
serialize foreground progress in those bounded scenarios.

The signed reference Surface performance suite also passed: a 100-bundle
catalog remains activation-free, Supervisor fairness was measured at active
limits 1/4/16 with no orphan or duplicate spawn, 256 callers shared one
activation, and the cold/hot Gateway/UDS plus stream-cancel/TTFB contract
completed successfully. Its report was written under
`/tmp/cowd-reference-performance-1787437989.json`.

## Checks

```text
cargo test -p harness-contract public_work_projection_omits_private_context_and_model_bindings --lib
cargo test -p runtime state_store_projection_keeps_node_payloads_opaque --lib
cargo test -p runtime add_team_patch_compiles_to_an_exact_active_program_revision --lib
cargo test -p runtime startup_reconciliation_restores_live_program_approval_wait_state --lib
cargo check -p harness-contract --all-targets
cargo check -p runtime --all-targets
cargo check -p gateway --all-targets
cargo test --release -p runtime --lib paired_foreground_probe_with_and_without_projector_is_bounded -- --ignored --nocapture --test-threads=1
cargo test --release -p runtime --lib paired_foreground_probe_during_projector_catchup_is_bounded -- --ignored --nocapture --test-threads=1
scripts/test/reference-app-performance.sh
cargo fmt --all -- --check
git diff --check
```

All listed checks passed before commit.

## Residuals

- Operator-facing Program timeline/audit aggregation beyond graph projection
  remains P5 follow-up work.
- The required 100x10 database gate, mobile batch/session-switch and stream
  saturation Surface scenarios remain open; this runtime-only performance
  proof does not cover them.
- P6 real-Qwen E2E remains open.
