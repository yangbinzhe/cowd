# P3 collaboration snapshot increment evidence

This record covers the fenced, session/turn-scoped custom-Team and AddTeam
increment only. It does not claim P4--P6 closure or the full non-additive
live-Program operation set.

## Capability boundary

- A normal `runtime_orchestrate(operation=propose)` may pair exactly one Team
  semantic node (without a catalog selector) with `template_proposal`.
  Runtime compiles it to an immutable ephemeral Team revision bound to the
  authenticated session, turn, policy reference, expiry and terminal fence.
- The model-visible schema cannot deserialize Runtime-owned ephemeral snapshot
  data. Team instantiation verifies the exact revision and rejects expiry,
  lineage mismatch and terminal parents.
- A managed Agent may request one fenced AddTeam escalation. Its custom
  template content is compiled by the parent Program Runtime; the Agent never
  supplies a snapshot, definition revision, executor, graph identity or
  permission ceiling.
- An ephemeral parent Team can add an ephemeral child Team without falling
  back to a catalog lookup. Program revisions retain exact obligations and
  advance every obligation fence atomically.

## Changed dependency cone

- `harness-contract`: Program patch/escalation and Team selector contracts.
- `runtime`: snapshot compilation/resolution, Program patch admission,
  Coordinator ingress, semantic compiler, Host guidance and regressions.
- `gateway`: managed-Agent escalation tool contract and runtime bootstrap
  schema.

## Checks

```text
cargo test -p harness-contract --lib
cargo test -p runtime propose_with_custom_template_materializes_a_turn_bound_team_snapshot --lib
cargo test -p runtime add_team_patch_compiles_to_an_exact_active_program_revision --lib
cargo test -p runtime ephemeral_team_snapshot_compiles_without_catalog_publication --lib
cargo test -p gateway runtime_capability_tool_is_always_registered_as_readonly --lib
cargo check -p runtime --all-targets
cargo check -p gateway --all-targets
cargo fmt --all -- --check
git diff --check
```

All listed checks passed for this increment before commit.

## Known residuals

- Retire, edge mutation, narrowing and parallelism Program operations remain
  deliberately unimplemented rather than being silently compiled as generic
  additive revisions.
- Durable recovery/reconciliation, projection completeness and real-Qwen E2E
  remain P4--P6 work.
