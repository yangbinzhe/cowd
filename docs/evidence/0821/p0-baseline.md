# 0821 / P0 基线与实施板

日期：2026-08-22

## Snapshot

| Repository | Ref | Worktree | Role |
|---|---|---|---|
| Cowd | `b3d381ddd3c0c3d591f72f4d1f8fa9ede0b3a9e8` | `/media/yi/Datas/workspace/cowd-0821-terminal` | clean integration branch `integration/0821-terminal` |
| Cowd Edge | `be5cebe4810120fc85038ee786eed781f97de746` | `/media/yi/Datas/workspace/cowd-edge` | clean `master` |
| Cowd App MFG | `3d47526a37154ad58f3ecf9e174f229f10090a7d` | `/media/yi/Datas/workspace/cowd-app-mfg` | clean `master` |

Plan authority is `0821-自主编排与跨Agent跨团队协同-版本审计与框架升级方案.md`,
SHA-256 `dab9b51479e88899afb04a549a5a23ec407564b2a309018e18c40d4471efc8fd`.
The separate plan worktree contains unrelated historical report changes, so it is not a
source or commit input for this implementation branch.

## P0 terminal goal

Freeze a reproducible source/dependency/ownership boundary and add only the contract seed
needed by the Program coordinator. No production caller may claim Program completion in P0;
P1 owns the first live command path.

## Current facts that constrain the implementation

* `CollaborationProgram` is graph metadata, while no `CollaborationCoordinator`,
  `TeamAdmissionObligation`, `CollaborationEscalationRequest`,
  `EphemeralTeamTemplateSnapshot`, or `ProgramResourceLedger` exists.
* `conversation/host.rs::start_selected_strategy` is still a live Team start path.
* `builtin_team_template_summaries` is still a hand-maintained selection source.
* `AcceptanceEvaluator`, typed Team binding/facets, `EvidenceReady`, child-lineage recovery,
  typed approval decisions, and generated approval groups are existing foundations, not P0
  replacement targets.

## Generator boundary

The Edge generator writes five coupled outputs:

```text
surfaces/webui/src/generated/gateway-api.ts
surfaces/webui/src/generated/live-contract-meta.ts
surfaces/webui/src/generated/projection-v2-golden.ts
surfaces/webui/src/generated/projection-contract-meta.ts
surfaces/webui/src/generated/app-protocol-meta.ts
```

P0/P1 contracts are generated first to an isolated output directory and hash-compared. They
must not run an Edge check that silently rewrites those source files while Cowd contracts are
being edited.

## P0 allowlist

* `docs/evidence/0821/p0-baseline.md`
* `docs/evidence/0821/0821-file-ownership.tsv`
* `harness-contract/src/execution_graph/contract.rs`
* `harness-contract/src/execution_graph/projection.rs`
* `harness-contract/src/execution_graph/validation.rs`
* `harness-contract/src/strategy/mod.rs`
* `harness-contract/src/turn.rs`
* `harness-contract/src/team/instantiation.rs`
* contract tests directly covering the new types and serde migration
* Mechanical literal migrations only: `runtime/orchestration/compiler.rs`,
  `runtime/orchestration/mod.rs`, `execution_core/graph/commit_service.rs` tests, and
  `conversation/host.rs` tests. They may
  initialize the new control/edge fields to the documented legacy `Planning` defaults; they
  must not add a Coordinator, change a production lifecycle, or consume the new authority.

No runtime semantic behavior, Gateway, Edge, MFG, Host production path, TeamRuntime, Runner, or
generated-file modification is allowed until the P0 contract API and the ownership TSV are closed.

## P0 exit evidence

1. Every future production consumer is classified in the TSV with one semantic owner.
2. Contract types distinguish Program lifecycle from node lifecycle and acceptance/effect facts.
3. Serde migration is read-compatible but does not introduce a production `None` authority
   placeholder.
4. Contract-only tests cover digest/authority/display separation and state validity.
5. `git diff --check`, `cargo fmt --all -- --check`, and the contract dependency-cone check pass.

## P0 execution evidence

Implemented contract seed:

* `CollaborationProgramLifecycle`, `TeamAdmissionState`, `TeamAdmissionObligation`,
  `CrossTeamEdgeState`, `CrossTeamInputContract`, `CollaborationEscalationRequest`,
  `EphemeralTeamTemplateSnapshot`, `ProgramResourceLedger`, and
  `CollaborationProgramControlState`.
* `CollaborationProgram` now carries defaultable control state and validates that every
  non-`Planning` program has the exact current-revision Team obligation set plus a non-zero
  technical parallel demand and deadline. Legacy graph metadata decodes only as `Planning`.
* `CollaborationProgramEdge` now carries typed input contract and delivery state. Existing
  callers were migrated mechanically to default `Pending` edges and `Planning` control; no
  runtime owner or execution behavior changed in P0.

Executed on the integration worktree:

```text
cargo test -p harness-contract active_program_control_requires_exact_obligations_and_technical_ledger --lib -- --nocapture  PASS
cargo test -p harness-contract collaboration_program_requires_exact_team_obligations_and_bound_edges --lib -- --nocapture  PASS
cargo check -p harness-contract --all-targets  PASS
cargo check -p runtime --lib  PASS
cargo fmt --all -- --check  PASS
git diff --check  PASS
```

P0 is provisionally closed. The contract carriers are intentionally not yet a live execution
path; P1 must make `Admitting` and later states transactionally live, remove Host direct
orchestration, and add the Coordinator/recovery owner before any capability completion claim.
