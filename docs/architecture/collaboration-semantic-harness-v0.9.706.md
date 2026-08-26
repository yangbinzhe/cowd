# v0.9.706 — Capacity, Veto Approval and Live Surface Truth

## Contract status

This is the subordinate execution contract for `v0.9.706`. It starts only
after `v0.9.705` is committed, tagged and audited in both repositories. The
cross-version authority is
`docs/architecture/collaboration-program-hardening.md`.

## Closed outcome

Every collaboration admission resolves one immutable execution-capacity and
approval snapshot. The existing ResourceManager remains the only execution
backpressure queue. A user-directed turn-scoped Team:

- executes immediately with an audit receipt in Autonomous/Trust-All modes;
- opens a bounded confirmation/veto window in human-confirming modes;
- executes once when that window expires without a decision;
- stops on an explicit veto;
- exposes admission, wait, role/Team progress, Program control, capacity and
  terminal truth through contiguous live projection deltas to WebUI and TUI.

The version is incomplete if orchestration still uses a local `5_000`, a
compiler default `32`, the Team static `32`, a schema `100`, or a private busy
poll as an independent policy decision. It is also incomplete if a live
Surface must refresh the full snapshot to learn that Program control changed.

## Explicit non-goals

- Gateway HTTP/SSE connection semaphores remain owned by
  `GatewayCapacityController`; they are ingress transport capacity, not a
  second execution scheduler.
- Provider/account/token and Tool quotas remain resource kinds in the existing
  ResourceManager. This version wires policy; it does not replace adaptive
  quota logic.
- No automatic experience promotion; `v0.9.707` owns that lifecycle.
- No provider call or Team execution is moved into the projection layer.

## Current source facts

| Current owner | Fact observed after `v0.9.705` | Defect to close |
| --- | --- | --- |
| `crates/runtime/src/infrastructure/runtime_control.rs` | `AgentControlPolicy.max_parallel_agents` already supplies an operator-configured ceiling and defaults to 42. `EffectiveExecutionCapacity` exposes only Agent/Tool widths. | This existing owner must be extended, not shadowed by a new collaboration config. |
| `crates/runtime/src/execution_core/graph/resources/manager.rs` | `ExecutionAdmissionPolicy` is versioned and bounded, with one fair queue and typed observations. `ExecutionResourceManager::with_admission_policy` already exists. | `RuntimeServices` always calls `new`, so deployment configuration never reaches it. |
| `crates/runtime/src/execution_core/services.rs` | Builder injects quotas and approval config, creates one ResourceManager and one ApprovalCoordinator. | It lacks an immutable execution-capacity profile input. |
| `crates/runtime/src/orchestration/compiler.rs` | Missing `max_parallel_agents` becomes 32. | Compiler owns an undocumented capacity default. |
| `crates/runtime/src/team/instantiation.rs` | `MAX_TEAM_GRAPH_AGENT_NODES = 32` clamps budget and rejects larger static graphs. | Operational topology is confused with a kernel representability guard. |
| `crates/harness-contract/src/orchestration.rs` | One schema range admits up to 100 semantic instances. | Model schema embeds another operational ceiling. |
| `crates/runtime/src/orchestration/mod.rs` | `ORCHESTRATION_VETO_WINDOW_MS = 5_000`; custom-Team confirmation polls ApprovalQueue every 100 ms. | Policy and waiting are duplicated outside the canonical ApprovalCoordinator. |
| `crates/runtime/src/approval/coordinator.rs` | One wait registry already uses `Notify`, cancellation/control selection and canonical queue state. | Orchestration bypasses it. |
| `crates/runtime/src/approval/approval_queue.rs` | One supervised deadline set resolves scoped timeout policy durably. | Its deadline wake currently notifies graph approvals but not Coordinator waiters. |
| `crates/harness-contract/src/projection/{snapshot,delta}.rs` | Projection v2 carries `graph.orchestration` in snapshots, but delta operations cannot replace it; terminal delivery/presentation also lack a complete update operation. | WebUI/TUI can show stale collaboration control until a resync. |
| `cowd-edge/surfaces/webui/src/components/runtime/CollaborationProgramSummary.vue` | A real Program card already renders obligations, edges and the resource ledger. | It cannot show semantic role provenance and receives stale live Program metadata. |
| `crates/tui/src/components/agent_team_panel.rs` | TUI already reads the canonical execution projection. | It needs the same v3 live truth and compact provenance, not a separate poller. |

## One resolved execution-capacity profile

`RuntimeControlPolicy` remains the configuration owner. It gains a nested
`capacity` policy while retaining `agent.max_parallel_agents` as the one
backward-compatible operator field for Agent width. Runtime resolves both once
at process composition into `ExecutionCapacityProfile`:

```text
ExecutionCapacityProfile
  schema_version
  profile_id
  revision
  digest
  max_program_teams
  max_team_roles
  max_role_instances_per_team
  max_agent_nodes_per_team
  max_parallel_agents              # sourced from control.agent
  max_pending_instance
  max_pending_per_class
  max_pending_per_key
  admission_aging_interval_ms
  user_team_veto_window_ms
  max_semantic_revisions_per_turn
```

All values are positive, bounded and relationally validated. Configuration
parsing fails early if per-key exceeds per-class, per-class exceeds instance,
preferred cardinality exceeds role max, total Team instances exceed the Team
node maximum, or a profile exceeds the kernel representability bound.

The locked default profile is:

| Field | Default |
| --- | ---: |
| `schema_version` / `revision` | `1` / `1` |
| `profile_id` | `default-balanced` |
| `max_program_teams` | `32` |
| `max_team_roles` | `32` |
| `max_role_instances_per_team` | `32` |
| `max_agent_nodes_per_team` | `32` |
| `max_parallel_agents` | existing configured default `42` |
| `max_pending_instance` | `4096` |
| `max_pending_per_class` | `2048` |
| `max_pending_per_key` | `512` |
| `admission_aging_interval_ms` | `5000` |
| `user_team_veto_window_ms` | `5000` |
| `max_semantic_revisions_per_turn` | `2` (initial decision plus one repair) |

These values preserve the currently effective Team ceiling, Agent setting,
queue policy and veto duration while removing their independent ownership.
They live in one `Default` implementation and are projected with a digest. A
failed performance gate requires a documented plan amendment; implementation
may not silently tune the defaults or duplicate them in compiler, Team,
Gateway schema or approval code.

`MAX_REPRESENTABLE_TEAM_AGENT_NODES = 1024` may remain as the one clearly named
kernel allocation-safety maximum. It is not advertised as deployable
throughput, does not select normal topology and is greater than every accepted
profile ceiling. Boundary tests must reject profile values above it before
allocation.

## Composition and ownership wiring

```text
merged config
  -> RuntimeControlPolicy validation
  -> ExecutionCapacityProfile::resolve
  -> RuntimeServicesBuilder::execution_capacity_profile
     -> ExecutionResourceManager::with_admission_policy
     -> orchestration compiler/validator
     -> TeamInstantiationRequest capacity snapshot
     -> ProgramResourceLedger profile ref
     -> approval policy snapshot
     -> projection/metrics
```

`RuntimeServices` stores one `Arc<ExecutionCapacityProfile>` and exposes an
immutable accessor. Conversations and Gateway capability manifests read that
same value. Hot reload may create a new revision for new admissions; already
admitted Programs keep their frozen profile ref and numeric snapshot.

Gateway `GatewayCapacityConfig` remains a separate named transport profile.
Diagnostics expose both refs so an operator can distinguish “HTTP lane full”
from “execution queue full”, but neither controller calls or bypasses the
other.

## Compiler, Team and Program rules

- Model `max_parallel_agents`, when present, is a hint and cannot exceed the
  resolved profile. Omission selects the smaller of runnable role demand and
  the profile ceiling.
- Total Team count, roles and role instances validate against the same profile
  before expensive context hydration or graph allocation.
- `TeamInstantiationRequest` carries a frozen `TeamExecutionCapacitySnapshot`
  with profile ref, maximum Agent nodes and runnable width.
- Team compilation uses the request snapshot; it does not read mutable global
  config and does not clamp silently.
- Work beyond active capacity remains in the existing fair ResourceManager
  queue. Offered load beyond pending limits returns typed overload/backpressure
  evidence.
- `ProgramResourceLedger` gains additive `capacity_profile_ref`,
  `capacity_profile_digest` and resolved parallel ceiling fields. Existing
  serialized Programs remain readable.
- Each exact Team obligation retains its resource reservation so revision and
  cancellation release only owned capacity.

## Approval and confirmation policy

### Trusted classification

The Runtime-only ingress provenance introduced in `v0.9.705`, not template
presence or a model field, determines whether the request is a user-directed
turn-scoped Team.

`CollaborationApprovalPolicySnapshot` freezes:

- Session execution-policy revision/digest;
- autonomy and approval profiles;
- decision class (`audit_auto`, `steward_auto`, `confirmation_veto`,
  `human_required`, `deny`);
- timeout action;
- veto deadline;
- source intent and Program refs.

### Required behavior matrix

| Context | Decision | Wait behavior |
| --- | --- | --- |
| Autonomous or Trust-All/Yolo session, user-directed Team | auto-approve once with policy audit receipt | no human wait |
| Steward policy eligible for bounded approval | steward approve with receipt | no human wait |
| Cautious/Supervised/other confirming profile, user-directed Team | confirmation/veto | bounded window; timeout auto-approves once |
| User explicitly vetoes in the window | deny | Program becomes typed blocked/cancelled; no execution |
| Non-user-directed high-risk orchestration | canonical router result | may remain human-required/pending; no custom-Team timeout privilege |
| Authorization or policy forbids the action | deny | timeout cannot widen authorization |

The original collaboration request is authorization to attempt the Team, not
proof that all effects are safe. Tool- and effect-specific approvals inside
Agents remain governed independently by the same Session policy.

### One wait path

Add a generic `ApprovalCoordinator::resolve_execution` (or equivalent shared
private primitive) and route orchestration through it. It submits one durable
request, resolves the global router, and waits through the existing `Notify`
registry with cancellation/control/deadline selection.

The ApprovalQueue deadline scheduler wakes both:

- the Coordinator waiter for root orchestration confirmation;
- the graph supervisor for graph-owned approval nodes.

Delete the orchestration 100 ms polling loop and local veto constant. No lock,
database transaction, graph CAS guard or resource permit is held across the
confirmation await.

## Projection v3 and live truth

`EXECUTION_PROJECTION_SCHEMA_VERSION` and reducer version advance together to
3. Add complete operations rather than relying on a later snapshot:

```text
ReplaceGraphOrchestration { orchestration }
SetDeliveryTruth {
  delivery_envelope,
  terminal_presentation,
  cancellation_receipt
}
```

The Runtime delta reducer emits `ReplaceGraphOrchestration` for every graph
revision that changes Program control, semantic provenance, escalation or
cross-Team receipts. It emits `SetDeliveryTruth` whenever any terminal
delivery field changes. Rust, TypeScript and TUI reducers implement identical
semantics and reject schema/reducer mismatches with directed resync.

The delta is derived from the graph projection at the frozen source cursor.
If that coherent materialization cannot be proven, Runtime returns an explicit
resync reason; it never combines a new Program control state with an old graph
revision.

## WebUI and TUI behavior

The existing Program summary becomes the no-template collaboration card. It
renders canonical fields, not transcript text:

- badge: “AI composed · turn scoped · not published” or exact catalog ref;
- original Team and role display names;
- responsibility, capability/Skill/Tool requirements;
- resolved Definition revision, effective grant and binding provenance;
- Program/Team/role lifecycle and cross-Team handoff status;
- capacity profile, queue age, wait reason and blocker;
- confirmation countdown, approve and veto actions using the existing global
  Approval API;
- typed compiler/admission/terminal diagnostics and allowed next action;
- evidence/receipt links and terminal coverage.

There is no “matched local template” label for a turn-scoped Team and no
template publication prompt. If no approved Agent Definition satisfies a
role, the card shows the typed capability gap rather than a fake Agent.

TUI renders the same provenance and state in
`agent_team_panel.rs`. It does not add another API, approval queue or polling
timer.

## Exact source allowlist

### `cowd-0821-terminal`

- version surfaces selected by the version gate;
- `config-default.yaml`;
- `crates/runtime/src/infrastructure/{runtime_control,config,config_validate,capability_manifest}.rs`;
- `crates/runtime/src/execution_core/services.rs`;
- `crates/runtime/src/execution_core/graph/resources/manager.rs`;
- `crates/runtime/src/orchestration/{mod,request,validator,compiler,collaboration_coordinator}.rs`;
- `crates/runtime/src/team/instantiation.rs`;
- `crates/runtime/src/approval/{coordinator,approval_queue,router}.rs` only for
  shared execution resolution and wake wiring;
- `crates/harness-contract/src/execution_graph/contract.rs`;
- `crates/harness-contract/src/projection/{snapshot,delta}.rs` and projection
  exports;
- `crates/runtime/src/projection/{snapshot,delta,reducer_support,mod}.rs`;
- `crates/gateway/src/api_routes/{runtime_routes,live_routes,capability_contract}.rs` and route
  schema tests;
- `crates/gateway/src/runtime/runtime_bootstrap.rs`;
- `crates/tui/src/app_core/protocol.rs`;
- `crates/tui/src/components/agent_team_panel.rs`;
- focused scenario/performance tests owned by these modules;
- `docs/evidence/collaboration-capacity-surface-v0.9.706.md`.

### `cowd-edge`

- `surfaces/webui/src/generated/{gateway-api,projection-contract-meta,projection-v2-golden}.ts`
  (rename the golden when required by the generator);
- `surfaces/webui/src/types.ts`;
- `surfaces/webui/src/adapters/executionProjection.ts` and tests;
- `surfaces/webui/src/components/runtime/{CollaborationProgramSummary,ExecutionTruthSummary}.vue`
  and tests;
- existing approval presentation/inbox files only when required to expose the
  same approve/veto receipt;
- `surfaces/webui/src/i18n/messages/{zh-CN,en-US}.ts` and typed keys;
- browser/live E2E fixtures and generated OpenAPI inputs;
- built `dist` only through the repository's normal build/release workflow.

Any source outside this allowlist requires a documented amendment before it is
edited.

## Deletion and reconnection table

| Retired decision/path | Replacement | Required proof |
| --- | --- | --- |
| compiler `unwrap_or(32)` | frozen profile width | scan and boundary tests |
| operational `MAX_TEAM_GRAPH_AGENT_NODES=32` | request snapshot + one kernel representability maximum | configured widths below/above old 32 behave consistently |
| schema/team count `100` as policy | profile-driven validation/schema description | no independent schema max controls Runtime admission |
| `ORCHESTRATION_VETO_WINDOW_MS` | approval snapshot value | scan and config tests |
| orchestration approval polling loop | ApprovalCoordinator `Notify` wait | waiter count returns to zero after approve/veto/timeout/cancel |
| template-presence classification | trusted ingress provenance | generic catalog and user custom paths cannot impersonate each other |
| snapshot-only Program control updates | projection v3 operation | WebUI/TUI update at each revision without full refresh |
| UI template fallback label | lifecycle/provenance badge | browser assertion on no-template custom Team |

## Queue, lock and resource map

| Await/resource | Owner | Bound | Lock/permit rule | Recovery |
| --- | --- | --- | --- | --- |
| collaboration compile | IntentCompiler | profile shape limits | no graph lock or permit | idempotent digest retry |
| approval confirmation | ApprovalCoordinator + Queue | veto deadline | no graph lock/resource lease | durable timeout/decision; waiter re-registers |
| execution pending | ResourceManager | instance/class/key limits | manager mutex held only for state transition | event-replayed observations; request retries by id |
| active Team/Agent | graph runner + resource lease | profile/quotas | permit held only for owned execution | supervisor recovery releases/reacquires by lease contract |
| projection subscriber | Gateway live controller | configured queue capacity | never blocks graph commit | cursor replay or explicit resync |
| WebUI/TUI reducer | Surface process | one projection state per selected execution | no Runtime lock | schema mismatch triggers snapshot fetch |

## Failure and recovery gates

- approval before timeout: one decision, one grant, one admission;
- veto racing timeout: exactly one terminal receipt wins; execution never
  starts after a winning veto;
- timeout racing cancellation/new input: fence winner is durable and stale
  work cannot resume;
- process restart during veto: queue rebuilds deadline, Program remains
  awaiting approval, then follows the frozen timeout policy;
- ResourceManager overload: typed overload with policy revision and queue
  counts, no unbounded waiter growth;
- graph CAS conflict: recompute from the latest Program revision without
  double reservation;
- slow/disconnected Surface: graph continues, bounded subscriber policy
  applies, reconnect replays or resyncs from cursor;
- stale v2 client: version mismatch, explicit resync/upgrade; no partial v3
  reduction.

## Performance and concurrency evidence

### Required workloads

1. compile-only matrix from one role through the maximum configured semantic
   shape;
2. 100 Programs × 10 Teams deterministic admission/reconciliation workload;
3. configured ceiling steady-state and 2× offered-load saturation;
4. mixed interactive/foreground/background fairness with one hot Session;
5. approve/veto/timeout races under concurrent graph commits;
6. live subscribers at normal capacity plus one intentionally slow consumer;
7. restart at planning, awaiting approval, queued, active and terminal states.

### Gates

- no deadlock, leaked waiter, leaked permit, duplicate terminal or lost
  obligation;
- pending queues never exceed the profile;
- an interactive request is not starved by a background flood;
- p95/p99 queue and non-provider control-plane latency are recorded;
- against the `v0.9.704` evidence workload, p95 non-provider latency and peak
  memory regress by no more than 5%, or the phase remains open with an owned
  optimization;
- at 2× offered load, throughput remains bounded and excess work receives
  typed backpressure;
- projection delta payload, reducer time, lag and resync count are recorded;
- no database/graph mutex is held across provider, Team, user or subscriber
  waits (source review plus contention test).

## Build, browser and audit gates

- focused Rust tests for config/profile, ResourceManager, compiler, approval,
  projection reducer and TUI;
- full workspace Debug build/test gate for affected crates;
- generated OpenAPI diff reviewed before `cowd-edge` regeneration;
- all WebUI unit/contract/i18n/governance tests;
- production WebUI build;
- Playwright scenario that submits a fresh arbitrary-role Team request,
  observes the confirmation countdown or auto receipt, watches at least three
  Program revisions arrive through live deltas, and verifies terminal card
  agreement without page refresh;
- source scans for retired constants, polling loop and v2-only reducer logic;
- reverse audit from each displayed field to a Program/approval/resource
  record.

## Version close

After evidence is complete and worktrees are clean:

1. bump terminal version surfaces to `0.9.706`;
2. commit core and WebUI repositories independently;
3. annotate `v0.9.706` in both repositories;
4. record commit/tree/tag ids and test artifact hashes;
5. do not push without explicit user authorization.

## Reverse audit

The terminal Surface claim must traverse both directions:

```text
displayed role/status/capacity/approval
  -> projection v3 revision/cursor
  -> Program semantic snapshot/control/resource ledger
  -> approval/resource receipt and exact Team obligation
  -> compiled binding and v2 semantic decision
  -> authenticated user Turn and Session policy
```

and:

```text
user Turn + policy + capacity profile
  -> decision/compiler/binding
  -> approval or immediate audit grant
  -> resource admission and graph execution
  -> Program terminal
  -> contiguous Gateway delta
  -> WebUI/TUI card
```

Any display state inferred from assistant prose, any hidden full-snapshot
dependency, or any independent capacity/approval default keeps this version
open.
