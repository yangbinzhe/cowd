# Framework Core Hardening v0.9.708

Status: implementation authority for `v0.9.708`.

This version closes five high-impact framework defects in one release. It supersedes the
execution status in `collaboration-program-handoff-2026-08-27.md`; that document remains a
historical handoff and is not a current completion source.

## Release authority

- Baseline commit: `4fa74bc45b6f81e3a9f9fb74f0e9779038530bff` (`v0.9.707`).
- Baseline tree: `8d3ee7bb2f7ed8feadcfa3f89529ea8152680233`.
- Target version and annotated tag: `0.9.708` / `v0.9.708`.
- Branch closure: `master` and `dev` must point to the same release commit.
- Remote closure: `origin` and `github` must contain both branches and the annotated tag.
- Evidence authority: `docs/evidence/framework-core-hardening-v0.9.708.md`.

## Five defect closures

| ID | Root defect | New owner and contract | Old path to remove | Acceptance |
| --- | --- | --- | --- | --- |
| H1 | Gateway session ingress imports Runtime resource scheduler internals and records scheduler observations itself. | Runtime owns a public `SessionTurnAdmissionPort` and lease outcome contract; Gateway only requests and completes a session-turn lease. | Direct `ExecutionResourceManager`, `ExecutionResourceLease`, `ExecutionResourceKind`, and `ResourceObservation` use in the session bridge. | Static residual scan, lease outcome tests, session/runtime scenarios. |
| H2 | Gateway background task admission and completion notification are unbounded. | `GatewayRuntimeTaskSet` owns a hard active-task capacity, bounded completion queue, typed overload rejection, and health counters. | Unlimited `spawn_owned` admission and `mpsc::unbounded_channel`. | Capacity/recovery/burst tests and health assertions. |
| H3 | Harness live health treats transport success as semantic success. | Harness Eval owns endpoint-specific, fail-closed health contracts for Gateway, Runtime, outbox, control plane, evolution, and Surface Host. | Wrapper-only `status=passed` for every successful HTTP JSON response. | HTTP-200/degraded fixtures fail; fully ready fixtures pass; real DeepSeek Flash run. |
| H4 | Gateway health and CLI argument tests share mutable process-global configuration, while a Runtime orchestration test assumes repository files exist in an isolated workspace. | Gateway process discovery accepts an explicit config-home scope; isolated tests create their own evidence fixture and env-mutating CLI tests use the canonical global-state guard. | Health lookup through ambient `COWD_CONFIG_HOME`; implicit repository fixture dependency; unguarded env parsing tests. | Default parallel full regression passes repeatedly and serial-global lane passes. |
| H5 | Release governance, historical handoff, evidence status, and workspace version disagree. | This authority plus the v0.9.708 evidence file are current; governance inventory and README must exactly match the workspace package version; historical documents are explicitly marked superseded without rewriting historical results. | Active-looking 0.9.704/0.9.706/0.9.707 status claims. | Governance gate, evidence/version residual scans, commit/version/tag gate. |

## Implementation board

| Phase | Work | Exit gate |
| --- | --- | --- |
| P0 | Freeze baseline, authority, allowlist, and acceptance matrix. | Clean baseline recorded; no implementation outside the declared cone. |
| P1 | Introduce Runtime session-turn port, rewire Gateway callers, delete private scheduler coupling. | Targeted Runtime/Gateway tests and architecture residual scan. |
| P2 | Bound Gateway task admission and completion queue; expose overload health. | Saturation, release-after-reap, completion burst, shutdown tests. |
| P3 | Add semantic live-health contracts and diagnostics. | Healthy/degraded fixture matrix and live provider scenario. |
| P4 | Remove process-global and repository-fixture test coupling. | Parallel Gateway and Runtime regression, plus serial-global lane. |
| P5 | Align governance, docs, evidence, and version. | Governance and architecture gates. |
| P6 | Full validation and external evaluation. | fmt/check, full regression, scenario, surface, performance, DeepSeek Flash. |
| P7 | Release closure. | One release commit, annotated tag, synchronized branches/remotes, clean tree. |

Implementation result: P0-P6 are complete with passing evidence. P7 is the
post-commit ref operation governed by the release gate; its branch, tag, remote,
and clean-tree assertions are verified after the release commit exists.

## Write allowlist

The implementation cone is limited to:

- `Cargo.toml`, `Cargo.lock`;
- `crates/runtime/src/lib.rs`, `crates/runtime/src/execution_core/services.rs`, and the new
  Runtime session-turn admission module;
- `crates/runtime/src/recovery/runtime_event_reactor.rs` for H2 foreground protection while
  maintenance projections drain a durable backlog;
- `crates/runtime/src/execution_core/model_work/estimator.rs` and
  `crates/runtime/src/execution_core/model_work/mod.rs`, plus
  `crates/runtime/src/orchestration/compiler.rs`, so observed-cost optimization may veto
  automatic parallelism but cannot silently override an explicit user Team-cardinality contract;
- `crates/gateway/src/runtime/session_runtime_bridge.rs`;
- `crates/gateway/src/runtime_host/task_set.rs`;
- `crates/gateway/src/runtime_host/mod.rs` for socket-safe isolated test roots when the caller's
  temporary directory exceeds the Unix-domain path budget;
- `crates/gateway/src/server/mod.rs`, `crates/gateway/src/infrastructure/gateway_health.rs`;
- the ignored release-performance contract in
  `crates/gateway/src/api_routes/app_routes.rs`, so direct and Gateway hot-path
  samples are paired in alternating rounds instead of accepting order-biased
  one-shot throughput evidence;
- the directly affected Gateway CLI tests in `crates/gateway/src/main.rs`;
- the deterministic orchestration test fixture in `crates/runtime/src/orchestration/mod.rs`;
- `crates/runtime/src/orchestration/intent_compiler.rs`,
  `crates/runtime/src/team/instantiation.rs`,
  `crates/runtime/src/agent/in_process_worker.rs`, and
  `crates/runtime/src/agent/evaluation.rs`,
  `crates/runtime/src/agent/result_validator.rs` for receipt-grounded custom-artifact terminal
  validation,
  `crates/runtime/src/execution_core/graph/executors/verify.rs` so Agent admission and Team
  delivery use one evidence-eligibility policy (failed audit artifacts never count as successful
  evidence),
  `crates/runtime/src/conversation/host.rs` for contract-aware terminal parsing before bounded
  recovery, and
  `crates/harness-contract/src/team/instantiation.rs` for the generic typed
  custom-artifact repair exposed by the live multi-Team evaluation;
- `crates/harness-eval/src/live_scenario_runner.rs` and its direct tests;
- `scripts/scenarios/harness-eval-real-qwen.sh` so the isolated deep-real
  environment satisfies the same required control-plane contract it evaluates;
- `scripts/architecture/check-boundaries.sh`, `scripts/test/governance-gate.sh`;
- `tests/test-governance/test-inventory.yaml`, `tests/test-governance/README.md`;
- the historical handoff/evidence headers and the v0.9.708 authority/evidence documents.

Any additional production file requires an amendment here before editing.

## Acceptance matrix

| Risk | Required evidence |
| --- | --- |
| Boundary ownership | Gateway session bridge has no Runtime resource-manager type; Runtime port unit tests; architecture gate. |
| Concurrency/backpressure | Admission limit is deterministic; excess work never starts; capacity returns after reaping; shutdown remains bounded. |
| Semantic health | Each endpoint checks its domain invariants; failed checks name their reason; control-plane non-readiness fails the report. |
| Isolation/recovery | Default parallel regression, Gateway serial-global regression, session recovery and shutdown tests. |
| Functional depth | Golden Session/Memory/Tool/Skill scenarios, Surface/reference app, and a real multi-Team DeepSeek Flash research/analysis/simulation evaluation. |
| Performance | Runtime projection and reference application performance gates remain within their declared budgets. |
| Release integrity | Clean diff, version consistency, evidence report, annotated tag, identical `master`/`dev`, two remotes verified. |

No phase is complete merely because code compiles. A defect closes only after the new owner is
wired, the old path is absent, and its acceptance evidence passes.
