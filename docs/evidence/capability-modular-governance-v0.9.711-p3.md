# v0.9.711 P3 Gateway Active Session Aggregate Evidence

Date: 2026-08-30

Plan: `plan/0830-cowd-v0.9.711-capability-conserving-modular-governance/plan.md`

Approved plan SHA-256: `225b4286d5504bce28259302328d849384663064448b3a419fde4f8e1c4399a1`

## Authority and lifecycle result

- `ActiveSessionDirectory` atomically publishes one aggregate containing the Runtime carrier,
  input stream, event bus, selected model, canonical Runtime policy-control handle, per-Session
  policy transition lock, relay lease, and monotonically increasing generation.
- The five parallel maps (`session_inputs`, `session_event_buses`, `session_models`,
  `session_execution_policies`, and `session_policy_update_locks`) are removed.
- The former `HotSessionPool` and its private `SessionLifecycle` observer are removed.
- Same-key materialize/publish/drain is serialized by a reclaimable weak lock; different keys use
  distinct locks and materialize in parallel. New-key capacity fails closed while replacement of
  an existing key remains legal.
- Removal unpublishes the complete aggregate first, then drains relay/tasks outside the directory
  guard. Runtime's `SessionExecutionPolicyControl` remains the policy authority; Gateway stores
  only that canonical handle.
- `GatewayCompositionRoot` creates the process task authority and active-Session directory once.

The governed duplicate candidate `gateway.active_session.parallel_carriers` was deleted from the
allowlist after its five source symbols reached zero.

## Module and source-size result

| Source | Before | P3 |
|---|---:|---:|
| `crates/gateway/src/runtime/runtime_service.rs` | 8,575 | 4,974 |
| `crates/gateway/src/runtime/session_runtime_bridge.rs` | 5,016 | 2,480 |
| `crates/gateway/src/main.rs` | 7,583 | 4,129 |

Extracted production owners: `active_session/{aggregate,directory,transition}.rs`,
`session_materializer.rs`, `session_worker_supervisor.rs`, `terminal_codec.rs`, and
`core/composition_root.rs`. Large inline tests moved to dedicated Core/Runtime test modules.
All three P3 transitional source-size exceptions were removed.

## Correctness and recovery gates

- Atomic lifecycle tests: 1, 16, and 256 parallel publish/read/remove operations; zero half-state,
  orphan aggregate, or duplicate generation.
- Capacity/replacement: same-key generation advances at full capacity; new key is rejected.
- Same-key lock identity is shared; cross-key lock identity is distinct.
- Full Gateway: 807 passed, 13 explicitly ignored external/environment tests, 0 failed.
- Gateway integration suites: 16 passed across route parity, authorization, agent/team templates,
  evolution, surface triggers, sidecar isolation, and sandbox startup; 0 failed.
- Workspace `cargo check --workspace --all-targets --all-features`: passed.
- Architecture inventory: Runtime 112, routes 482, tools 53, Edge 115, legacy owners 0.
- Authority registry: 43 unique authorities; duplicate candidates reduced from 5 to 4.
- Source-size and structural-limit gates: passed.

The source-size gate was also hardened to ignore a tracked path deleted in the current worktree;
this permits an architecture phase to delete legacy oversized sources before commit without
mistaking the index entry for a live file.

## Performance gates

Atomic publication microbenchmark (7 rounds, 20,000 register/remove cycles per round):

- legacy six-map median: 48.545 ms
- aggregate median: 23.635 ms
- register/remove throughput improvement: 51.31% (required: 25%)
- legacy activation-publication p95: 55.786 ms
- aggregate activation-publication p95: 26.848 ms
- activation-publication p95 improvement: 51.87% (required: 15%)

Frozen six-workload candidate report:
`test-reports/performance-v0.9.711/p3-candidate.json`, SHA-256
`94d57e8448ffdbc37e2c74755213163618785b972a662f76929f7f3bd8ce141e`.
The report binds worktree digest
`30fcb055028ca01443d1f6aae807e01f3dfac8cfc42d6524ce06dca4937209ae`.

Median change against P0:

- active-session contract: +0.80%
- persisted Session activation: +15.81%
- deterministic 6-Team collaboration: +17.17%
- TUI refresh: -0.56%
- route generation: +5.20%
- backend page: -2.27%

Median change against P2 is within 3% for five workloads; persisted activation improves a further
15.34%. The active-session workload selector was corrected after the type/module rename to run an
equivalent lightweight directory contract, rather than incorrectly including the new 256-thread
correctness stress test. The algorithmic throughput target is enforced separately by the explicit
microbenchmark above.
