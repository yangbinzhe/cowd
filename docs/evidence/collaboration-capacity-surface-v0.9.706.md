# v0.9.706 Capacity, Approval and Live-Surface Evidence

## Phase board

| Gate | State | Evidence |
| --- | --- | --- |
| Approval wait ownership | Passed | Orchestration delegates confirmation waits to `ApprovalCoordinator`; deadline wake notifies both coordinator and graph supervisor; the wait registers before its durable status read, so veto/timeout/cancel cannot lose a wake or leak a waiter |
| Frozen capacity profile | Passed | Runtime control resolves a deterministic `ExecutionCapacityProfile`; Team requests and Program ledgers freeze its id/revision/digest/numeric limits; Gateway composition passes the profile to ResourceManager admission; bounded overload and fairness are exercised at the manager boundary |
| Projection v3 | Passed | Contract/reducer and Gateway/OpenAPI metadata use v3; `ReplaceGraphOrchestration` and complete `SetDeliveryTruth` apply atomically in Rust and WebUI, while version mismatch remains fail-closed |
| Load/race/browser evidence | Passed for deterministic release gates | Resource/approval races, isolated source Gateway OpenAPI generation, full WebUI contracts, Chromium browser suite and production build passed. Real-provider terminal acceptance remains owned by v0.9.707. |

## Implemented finding

The prior user-directed Team confirmation loop read `ApprovalQueue` every
100ms and owned an independent timeout check. This made a durable approval
queue behave as a polling API and could leave a coordinator waiter unwoken by
a deadline. The replacement calls the coordinator's generic execution wait;
the existing single deadline scheduler now wakes both the coordinator and the
graph supervisor. No graph lock, database transaction or resource permit is
held across that wait.

The first capacity pass also exposed an error in the planning prose: the locked
defaults permit 32 roles and a per-role maximum of 32 while retaining 32 total
Agent nodes. Those are valid independent maxima only when the concrete Team
validator checks the declared cardinality sum. The profile therefore rejects
invalid queue/guard values but does not multiply independent maxima and reject
its own defaults. The former Team-local operational `32` is now solely a 1024
representability guard; Runtime admission remains the capacity owner.

## Allowlist amendment: immutable capacity snapshot carriers

The initial P9 pass correctly made `RuntimeControlPolicy` the configuration
owner and injected its policy into `RuntimeServices`, but source inspection
found that it still stopped before the two durable admission carriers required
by the version contract: `TeamInstantiationRequest` and
`ProgramResourceLedger`. A mutable service policy cannot prove which numeric
limits admitted an already-running Program after a reload or restart.

This amendment authorizes only additive capacity-profile/snapshot fields,
their deterministic digest validation, compiler/coordinator freezing before
Team planning, and directly affected fixture constructors:

- `crates/harness-contract/src/team/instantiation.rs`;
- `crates/harness-contract/src/execution_graph/contract.rs`;
- `crates/runtime/src/infrastructure/{runtime_control,config,config_validate}.rs`;
- `crates/runtime/src/{execution_core/{services.rs,graph/{commit_service.rs,executors/subgraph.rs}},orchestration/{compiler,collaboration_coordinator}.rs,team/{instantiation.rs,team_binding.rs}}`;
- `crates/harness-eval/src/runner.rs` for the existing direct Runtime Team-plan
  evaluation path, which must freeze the same service-owned snapshot;
- direct Team-request / Program-ledger test fixtures reported by the compiler.

No lifecycle, scheduler, queue, Gateway, or projection ownership moves through
this amendment. `ExecutionResourceManager` remains the sole admission queue;
the added carriers are immutable evidence for its resolved policy, not another
reservation or scheduler.

## Allowlist amendment: generated projection v3 contract identity

The canonical projection fixture now declares schema and reducer version 3,
but the Gateway OpenAPI extension and WebUI generated-artifact name still
described it as v2. That mismatch would permit a browser to consume v3 data
while presenting v2 as the contract identity. This amendment authorizes the
mechanical identity update only:

- Gateway OpenAPI extension `x-cowd-projection-v3-golden` and its contract
  assertion in `crates/gateway/src/api_routes/capability_contract.rs`;
- the canonical fixture path/name and direct Rust callers, so source identity
  cannot continue to describe the v3 corpus as v2;
- the source-owned isolated OpenAPI generation scenario;
- WebUI generator, generated golden corpus filename/export, and its two direct
  consumer tests;
- directly affected WebUI projection fixtures and offline API defaults, whose
  schema values must match the generated v3 contract for reducer and resync
  tests to exercise their intended branch;
- the WebUI canonical projection reducer and its direct adapter test, to apply
  the new `set_delivery_truth` v3 operation atomically;
- the existing controlled Playwright projection fixtures and v3 assertion, so
  browser coverage continues to exercise rendering rather than an obsolete
  deliberate schema mismatch.

No projection reducer, wire shape, route, or ownership boundary changes in
this amendment; the generated corpus remains byte-for-byte derived from the
same Gateway response.

## Allowlist amendment: WebUI release manifest

`surfaces/webui/surface.json` is the checked-in surface version consumed by
the Gateway catalog. The prior v0.9.705 generated-contract-only Edge commit
left it at `0.9.702`; v0.9.706 therefore authorizes updating this existing
release metadata field only. No package dependency or client runtime ownership
changes accompany the version update.

## Allowlist amendment: versioned task handoff

`docs/architecture/collaboration-program-handoff-2026-08-27.md` was the
pre-existing untracked continuation record supplied for this implementation.
It is committed unchanged with the v0.9.706 evidence so the release boundary
does not depend on an unversioned architecture instruction. It creates no
runtime behavior or ownership change.

## Commit gate record

- Branch: `integration/0821-terminal` (core) and `master` (WebUI).
- Version: core workspace `0.9.706`; WebUI surface manifest `0.9.706`.
- Phase: P9 capacity/approval/projection-v3/live-surface closure.
- Changed dependency cone: Runtime control, Team/Program immutable capacity
  carriers, orchestration/approval/projection reducers, Gateway OpenAPI/live
  routes, TUI fixture consumers, and WebUI generated contract/reducer/browser
  consumers.
- Evidence: this document plus the isolated source-Gateway generation check.
- Known residuals: governed experience reuse and real-provider terminal
  acceptance are explicitly v0.9.707 work; no v0.9.707 source is included in
  this commit.

## Executed evidence

| Command | Result |
| --- | --- |
| `cargo test -p runtime approval --lib --quiet` | 57 passed, 0 failed |
| `cargo test -p runtime team_instantiation --lib --quiet` | 22 passed, 0 failed |
| `cargo test -p runtime orchestration --lib --quiet` | 103 passed, 0 failed |
| `cargo test -p harness-contract projection --lib --quiet` | 8 passed, 0 failed |
| `cargo test -p runtime projection --lib --quiet` | 76 passed, 0 failed |
| `cargo test -p runtime orchestration --lib --quiet` after profile-window wiring | 103 passed, 0 failed |
| `cargo test -p runtime runtime_control --lib --quiet` | 8 passed, 0 failed |
| `cargo test -p runtime config_runtime_control_merges_scenario_and_policy_overrides --lib --quiet` | 1 passed, 0 failed |
| `cargo test -p runtime approval --lib --quiet` after waiter race coverage | 59 passed, 0 failed |
| `cargo test -p runtime projection --lib --quiet` after `SetDeliveryTruth` | 76 passed, 0 failed |
| `cargo check -p runtime -p harness-contract -p gateway --all-targets` | Passed |
| `cargo test -p gateway runtime_bootstrap::tests --lib --quiet` | 3 passed, 0 failed |
| `cargo check -p tui --all-targets` | Passed |
| `cargo test -p runtime execution_core::graph::resources::manager --lib --quiet` | 30 passed, 0 failed; covers bounded instance/class/key queues, typed overload and interactive reserve fairness |
| `npm --prefix cowd-edge/surfaces/webui test` | Passed: 439 unit tests plus i18n, governance, API matrix, presentation, capability, raw-payload, secondary-section and acceptance gates |
| `npm --prefix cowd-edge/surfaces/webui run test:e2e` | Passed: Chromium 20 passed, 1 real-Gateway-only case skipped by its explicit environment guard |
| `npm --prefix cowd-edge/surfaces/webui run build` | Passed |
| `bash scripts/scenarios/openapi-generation.sh check` | Passed against an isolated source-built Gateway and isolated configuration/storage root |
| `cargo check -p runtime -p harness-contract -p gateway -p tui --all-targets` | Passed after the `0.9.706` version update |
