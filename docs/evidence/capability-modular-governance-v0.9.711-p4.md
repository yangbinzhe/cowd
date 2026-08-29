# v0.9.711 P4 Runtime Pipeline Modularization Evidence

Date: 2026-08-30
Plan: `plan/0830-cowd-v0.9.711-capability-conserving-modular-governance/plan.md`
Plan SHA-256: `225b4286d5504bce28259302328d849384663064448b3a419fde4f8e1c4399a1`

## Capability-preserving structure

- Conversation production logic is separated into turn, provider, context,
  tool, and evidence/terminal planes. The existing `ConversationRuntime`
  remains the only execution facade and no second provider/tool/terminal
  authority was introduced.
- Host construction, graph-owned provider/tool backends, and terminal
  presentation policy are separate implementation modules.
- Runtime composition and lifecycle services, graph commit pipeline, Agent
  preparation/terminal policy, semantic orchestration facade, configuration
  schema/load/validation, and Tool dispatch/builtin adapters carry the moved
  implementation rather than pass-through copies.
- Embedded tests were split by behavior without deleting test names or
  negative paths.
- The stale source self-audit URL-literal checks now verify typed Surface route
  catalog constants, matching the already-migrated Gateway route authority.

## Source governance

All P4 transitional source-size exceptions were removed. Representative final
line counts:

| Source | Lines |
| --- | ---: |
| `conversation/conversation.rs` | 4,946 |
| `conversation/host.rs` | 4,352 |
| `execution_core/services.rs` | 4,545 |
| `agent/in_process_worker.rs` | below 5,000 |
| `execution_core/graph/commit_service.rs` | 2,066 |
| `orchestration/mod.rs` | 2,569 |
| `infrastructure/config.rs` | below 5,000 |
| `tools/execution/executor.rs` | 3,154 |

`cargo xtask architecture audit --check` passed with:

- runtime bindings: 112
- typed routes: 482
- tools: 53
- Edge bindings: 115
- legacy owners: 0
- state authorities: 43
- classified duplicate candidates: 4
- remaining oversized sources: 10 (9 later-phase transitional, 1 generated)

## Functional verification

- `cargo test -p runtime --all-features`: 1,910 library tests passed, 2
  explicitly ignored; every Runtime integration and doc-test target passed.
- `cargo test -p tools --all-features`: 198 library tests passed, 1 explicitly
  ignored; 4 integration tests and doc tests passed.
- `cargo test -p runtime --test runtime_capability_authority --test runtime_module_architecture`:
  3/3 passed.
- `cargo xtask architecture source-size --check`: passed.
- `cargo xtask architecture structural-limits --check`: passed.
- `git diff --check`: passed.

## Frozen performance comparison

Candidate report (ignored generated evidence):
`test-reports/performance-v0.9.711/p4-candidate.json`
SHA-256: `360c10d2bcd1656963bdbb0a17baa6608415387de3df00c9d53a2321566eda83`

Relative to the frozen P0 baseline:

| Workload | Median change |
| --- | ---: |
| active-session | -2.18% |
| session-activation | +0.18% |
| six-team | +15.39% |
| tui-refresh | -0.02% |
| route-generation | +3.66% |
| backend-page | -2.92% |

The P4 six-Team target (at least 15% median improvement, or 10% p95
improvement) is met. Every other frozen workload remains inside the 5%
regression ceiling.

## Phase decision

P4 is closed: the planned production and test boundaries exist, all P4
oversize exceptions are retired, canonical execution/state authorities remain
unique, full Runtime and Tool behavior is green, and the frozen performance
gate passes without capability or quality relaxation.
