# Collaboration Semantic Harness Baseline — v0.9.704

## Commit gate

- Branch: `integration/0821-terminal`
- Version: `0.9.704` from `[workspace.package]` in `Cargo.toml`
- Phase: freeze the accumulated collaboration correctness baseline before the
  three-version Semantic Contract Harness program
- Previous HEAD: `d04963d99fde71185826ceb419c5e24210a00d4f`
- Previous tree: `478327385854064646dba6d6fed9424c3aa0e309`
- Intended tag: `v0.9.704`
- Existing tag check: absent locally, on `origin`, and on `github`
- Indexed changes before the evidence record: none
- Mixed states (`MM`, `AM`, `AD`, conflicts): none
- Untracked input before the evidence record:
  `docs/architecture/collaboration-program-hardening.md`

## Frozen pre-evidence snapshot

- Status manifest SHA-256:
  `e3d2c036474da0988c333835e6df7f041b1c133460b324cca007e40eaa9ec0df`
- Complete tracked diff from HEAD SHA-256:
  `1560d8420afc423c31f834086e931e41839624480102313bd29624c5f9d34949`
- Index diff SHA-256 (empty):
  `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- Worktree diff SHA-256:
  `1560d8420afc423c31f834086e931e41839624480102313bd29624c5f9d34949`
- Architecture document SHA-256:
  `8de431c05b4221a37c1dd98f551e1115e35bd231790b5da5d95d99e838e8f888`
- Snapshot note: this evidence file is the only deterministic metadata addition
  after the frozen snapshot and before staging. The final commit tree and commit
  hash supersede this pre-evidence identifier after the commit gate closes.

## Changed source manifest

```text
crates/gateway/src/runtime/gateway_tool_executor.rs
crates/gateway/src/runtime/runtime_bootstrap.rs
crates/harness-contract/src/execution_graph/contract.rs
crates/harness-contract/src/orchestration.rs
crates/harness-contract/src/strategy/mod.rs
crates/harness-contract/src/team/binding.rs
crates/harness-contract/src/team/definition.rs
crates/harness-contract/src/team/mod.rs
crates/provider/src/providers/openai_compat.rs
crates/runtime/src/agent/collaboration_template.rs
crates/runtime/src/agent/definition_registry.rs
crates/runtime/src/agent/in_process_worker.rs
crates/runtime/src/agent/result_validator.rs
crates/runtime/src/approval/approval_queue.rs
crates/runtime/src/conversation/host.rs
crates/runtime/src/execution_core/graph/commit_service.rs
crates/runtime/src/execution_core/graph/executors/subgraph.rs
crates/runtime/src/execution_core/graph/executors/verify.rs
crates/runtime/src/execution_core/model_affordance.rs
crates/runtime/src/execution_core/services.rs
crates/runtime/src/infrastructure/capability_manifest.rs
crates/runtime/src/orchestration/collaboration_coordinator.rs
crates/runtime/src/orchestration/compiler.rs
crates/runtime/src/orchestration/mod.rs
crates/runtime/src/orchestration/result.rs
crates/runtime/src/orchestration/team_authority.rs
crates/runtime/src/orchestration/validator.rs
crates/runtime/src/team/definition/bootstrap.rs
crates/runtime/src/team/definition/store.rs
crates/runtime/src/team/instantiation.rs
crates/runtime/src/team/result_reducer.rs
crates/runtime/src/team/team_binding.rs
crates/runtime/src/team/template_candidate.rs
crates/runtime/tests/team_definition_store.rs
crates/runtime/tests/team_instantiation.rs
docs/architecture/collaboration-program-hardening.md
docs/evidence/collaboration-semantic-harness-baseline-v0.9.704.md
```

## Checks

- `git diff --check`: passed
- `git diff --cached --check`: passed with an empty index
- `cargo fmt --all -- --check`: passed
- `cargo check -p harness-contract -p provider -p runtime -p gateway --all-targets`:
  passed in the complete changed dependency cone
- Workspace metadata: resolves workspace packages at `0.9.704` except the
  independently versioned `cowd-app-protocol` at `1.0.0`
- Stale `0.9.703` scan across `Cargo.toml`, `Cargo.lock`, `crates`, `scripts`
  and `README.md`: no matches
- Changed-file secret-shape scan for long `sk-*` values: no matches

## Baseline completion claim and residuals

This commit preserves the current accumulated collaboration correctness work as
the `v0.9.704` implementation baseline. It does not claim that the new Semantic
Contract Harness, unified capacity policy, governed experience reuse, final
performance suite, or final real-provider/WebUI acceptance is implemented.
Those residuals are deliberately owned by the next three version boundaries in
the reviewed architecture authority.
