# Collaboration Program Hardening

## Status and execution authority

This is the sole implementation authority for hardening user-directed Team
collaboration. It supersedes ad-hoc fixes that interpret a model tool receipt
inside the conversation host. The terminal architecture extends the existing
graph-owned `CollaborationProgram`; it does not introduce a second Team
scheduler or a second collaboration lifecycle registry.

Historical incident baseline captured 2026-08-25:

- Repository HEAD: `d04963d99fde71185826ceb419c5e24210a00d4f`.
- Worktree is intentionally dirty in the collaboration dependency cone. The
  16 changed files must be treated as unplanned repair candidates until each
  is assigned to a phase below; unrelated work must not be overwritten.
- Real WebUI/DeepSeek runs established four independent symptoms: cardinality
  shape mismatch, builtin-role contamination of an ephemeral Team, reversed
  role dependency direction, and an opaque required-node failure. A fourth
  run completed the Team but then the parent model reopened orchestration.

Frozen release baseline captured 2026-08-26:

- `cowd-0821-terminal`: annotated tag `v0.9.704`, commit
  `31f578078727c59035a2a2c47a219e50ae429676`, tree
  `913ebfd10622781c22a458f9e0d0abdc24830efd`.
- `cowd-edge`: annotated tag `v0.9.704`, commit
  `04b63861e9e332576d08a2f81326942b22c92e9a`, tree
  `e78d575c4821a210e6f762edbd8afa785d463f7a`.
- Core formatting and affected-crate checks passed; WebUI passed 438 unit tests
  across 53 files and its production build. The exact commands and tag objects
  are recorded in
  `docs/evidence/collaboration-semantic-harness-baseline-v0.9.704.md`.
- No remote push is authorized by this plan. Each implementation version must
  close independently in both repositories before the next version starts.

Review amendment captured 2026-08-26:

- The semantic-contract, concurrency and experience-reuse design below is
  review-ready. It is deliberately recorded in this sole authority rather
  than in a competing plan. P7-P11 must not begin until the decisions in
  **Proposed locked decisions awaiting user confirmation** are accepted; the
  earlier correctness repairs remain
  prerequisites and are not reopened by this amendment.

Implementation confirmation gate captured 2026-08-26:

- Planning and plan auditing are authorized before implementation.
- No business-source implementation for P7-P11 may start until the user
  explicitly confirms the audited three-version plan.
- The release packaging below is the executable interpretation of P7-P11. Its
  subordinate version contracts add source allowlists and acceptance evidence;
  they do not create competing architecture authorities.

## Three-version release packaging

| Version | Architecture phases | Closed boundary | Subordinate contract |
| --- | --- | --- | --- |
| `v0.9.705` | P7 + P8 | One semantic model contract, deterministic intent compiler, exact capability/Skill/Tool resolution, immutable Program provenance, no builtin or name-based fallback. | `docs/architecture/collaboration-semantic-harness-v0.9.705.md` |
| `v0.9.706` | P9 + approval and live-Surface portion of P11 | One execution-capacity profile, one approval/veto wait path, projection v3, truthful live WebUI/TUI collaboration card. | `docs/architecture/collaboration-semantic-harness-v0.9.706.md` |
| `v0.9.707` | P10 + terminal integrated portion of P11 | Typed experience episodes, advisory semantic patterns, governed owner-specific promotion, real-provider/browser/concurrency/restart/fault acceptance. | `docs/architecture/collaboration-semantic-harness-v0.9.707.md` |

The pre-implementation contradiction, ownership, concurrency, recovery,
performance and source-overlap audit is
`docs/architecture/collaboration-semantic-harness-plan-audit.md`. A version may
not borrow a later version's completion claim: v0.9.705 proves semantic
correctness without the new Surface; v0.9.706 proves live execution truth
without reusable promotion; v0.9.707 proves governed reuse and the terminal
business chain.

## User-visible terminal outcome

A user can name any one or more Teams and any role names. The model proposes
only semantic topology; Runtime compiles immutable bindings, executes it under
the active authorization policy, and presents one explainable terminal result.
No catalog template is selected or published unless the user explicitly asks.
In trust-all mode admission proceeds automatically; in confirmable modes an
approval is a veto window, not a permanently blocking workflow.

## Code facts and ownership

| Boundary | Current source facts | Current defect | Terminal owner |
| --- | --- | --- | --- |
| Model intent | `harness-contract/src/orchestration.rs` exposes both `runtime_orchestrate` and `submit_collaboration_decision`; custom Team details cross JSON/template forms. | Equivalent Team intent has more than one entry shape. | Harness Contract defines one semantic collaboration decision; Gateway only transports it. |
| Admission | `runtime/src/orchestration/{mod,collaboration_coordinator,compiler,team_authority}.rs` creates `CollaborationProgram`, graph nodes, immutable snapshots and resource scopes. | Program records admission, while later layers re-derive execution truth from tool text. | Runtime `CollaborationProgram` and graph revision are canonical. |
| Team execution | `runtime/src/team/{team_runtime,instantiation,result_reducer}.rs` owns Team binding, child graph, delivery envelope and role acceptance. | Parent completion receives a lossy, completed-node-only assessment. | TeamRuntime owns role/Team result; Program owns cross-Team aggregation. |
| Parent turn | `runtime/src/conversation/host.rs` stores `verified_team_ids`, root-control phases and parses tool receipts. | In-memory and transcript-derived state can diverge from Program state; an already-completed Team can reopen orchestration. | Conversation host consumes a typed Program terminal projection only. |
| Surface | Gateway routes and projections expose events, but the model-facing failure is often `required_node_not_completed:<id>`. | UI cannot identify role, acceptance gap, retryability or policy cause. | Runtime projection defines a typed diagnostic; Gateway/Surface renders it unchanged. |

## Whole business and reverse-evidence chain

```text
User/WebUI request
  -> Gateway authenticated session input
  -> Runtime root admission decision
  -> CollaborationProgram revision + immutable Team snapshots
  -> ExecutionGraph scheduler/resource admission
  -> TeamRuntime child graphs + Agent role results
  -> Program reconciliation + typed collaboration terminal
  -> graph event/projection
  -> Gateway/WebUI result card and parent text presentation

WebUI terminal card
  -> collaboration terminal diagnostic/event
  -> Program revision and Team obligation outcome
  -> child Team delivery envelope and role acceptance receipts
  -> immutable binding/snapshot
  -> admitted user intent and authorization policy
```

Runtime remains the only owner of durable state, scheduling, recovery,
permissions, approvals, resource admission and terminal commit. The model may
choose names, responsibilities, topology, dependencies, confidence and a
bounded replan; it never chooses a runtime identity, lease, mutable catalog
revision, permission grant or terminal status.

## Root causes to eliminate

1. **Dual semantic codecs.** The narrow root tool and the generic orchestration
   tool can both express Team work, with different template/focus conventions.
   This caused cardinality and snapshot mapping drift.
2. **Lifecycle truth is duplicated.** `CollaborationProgram` has durable
   lifecycle state, but `host.rs` also infers success from tool transcripts and
   keeps an in-memory `verified_team_ids` cache.
3. **Topology meaning is implicit.** `RoleBehaviorFacet` and a generic DAG are
   individually valid yet do not explicitly carry producer/consumer evidence
   semantics. Prompt wording and a role-specific guard cannot be the source of
   dependency correctness.
4. **Template lifecycle is overloaded.** A turn-scoped Team snapshot and a
   publishable catalog template share proposal machinery despite distinct
   authority/lifecycle rules.
5. **Terminal aggregation is lossy.** `assess_team_subgraphs` skips failed or
   blocked nodes and later reduces them to `required_node_not_completed`,
   discarding role-level acceptance and retry classification.
6. **Approval and completion policy are dispersed.** Program admission,
   session policy and parent conversation controls each make partial decisions.

## 2026-08-26 incident analysis and approved execution amendment

The eighth real three-Team run is a decisive counterexample to the current
claim that typed evidence is carried end-to-end. `signal_team` made four
durable `read_file` receipts, yet its Team verifier blocked with two
`team_contract:evidence_scope:read:cowd-0821-terminal/...` findings. The
downstream Teams were then correctly stopped by their handoff dependency.

The failure is not a missing permission and is not specific to either role
name used in the scenario:

1. `TeamInstantiationCompiler::compile_required_acceptance` compiles role
   scopes to `EvidenceObligation` through `WorkspacePathIdentityResolver`.
   That gives the role-side acceptance evaluator canonical, repository-aware
   identities and it accepted the tool receipts.
2. `VerifyNodeExecutor` then separately evaluates the Team-level criteria.
   It propagates the role's original criterion strings into
   `satisfied_team_criteria`, but compares them to the Team node's criterion
   strings. The Team node has already passed through workspace normalization,
   so an equivalent project alias is not textually equal.
3. `is_runtime_evidence_backed_team_criterion` explicitly excludes
   `evidence_scope:*`; consequently the verifier does not use the already
   satisfied typed role acceptance to satisfy the Team's equivalent typed
   scope contract. This is the direct source of the false negative.
4. The root model's later prose stated that no Team had run. The durable
   Program and tool receipts contradict it. Model prose is therefore not a
   valid status source and must not be presented as an execution report.

### Feasibility evidence before implementation

The replacement requires no new scheduler, provider behavior, template, or
role-name branch. The exact evidence already exists in the code:

| Claim | Existing proof | Consequence |
| --- | --- | --- |
| A role's bounded read is already canonical and receipt-backed. | `WorkspacePathIdentityResolver::compile_required_acceptance_with_root_alias` and `AcceptanceEvaluator` consume typed `EvidenceObligation`, not display strings. | Reuse that result; never re-parse a path at Team verification. |
| The failure happens only in the Team verifier's second semantic pass. | `VerifyNodeExecutor` adds every completed role's `packet.acceptance` string, then requires a direct match to `node.acceptance.criteria`; `evidence_scope:*` is excluded from the Runtime-backed branch. | Treat every typed `evidence_scope:*` as Runtime-backed at the Team boundary after all role slots have accepted. |
| This preserves safety. | A Team slot cannot be completed without `AcceptanceEvaluator` satisfying its typed obligation; the verifier also requires durable Team evidence and zero invalid slots. | The change cannot turn an unread path into accepted evidence. |
| The Surface already has a durable diagnostic carrier. | `CollaborationDiagnostic` is projected from Program obligations in `orchestration/mod.rs`. | Add a canonical terminal report derived exclusively from that projection; model prose becomes supplemental. |

### Closed implementation phases

| Phase | Goal / canonical owner | Write allowlist | Required wiring and deletion | Acceptance gate |
| --- | --- | --- | --- | --- |
| P3A | Make typed Team evidence identity-preserving. Runtime verifier owns Team-level evidence satisfaction. | `crates/runtime/src/execution_core/graph/executors/verify.rs`; its tests. | Remove the raw-string-only exclusion for `evidence_scope:*`; retain structured-field checks and role-level receipt validation. No role display/id branch is permitted. | Regression test with equivalent relative/canonical scopes; negative test with an unsatisfied role; existing verifier tests. |
| P4A | Make Program projection the sole user-visible execution truth. Runtime orchestration owns terminal report data; Gateway only transports it. | `crates/runtime/src/orchestration/{mod,result}.rs`, `crates/gateway/src/runtime/gateway_tool_executor.rs`, focused tests. | Replace any model-text-derived completion/failure summary at the tool boundary with a typed Program terminal report; retain model prose only as an attributed narrative. | Completed/blocked Program projection tests; source scan for transcript-based collaboration completion inference. |
| P5A | Enforce truthful upstream-only terminal synthesis. Agent result validator owns semantic anti-fabrication guard. | `crates/runtime/src/agent/result_validator.rs`; its tests; `crates/runtime/src/team/instantiation.rs` prompt contract. | Reject an upstream-only completed result that contains a tool-call payload or claims future capability/tool retrieval; require a non-empty bounded conclusion. | Positive synthesis and rejected simulated-tool payload tests; no role-name rule. |
| P6 | Real observable acceptance. Gateway/Runtime execute, Surface observes. | No production change unless a failed gate identifies its owner. | Fresh session; exact custom Team names/roles; three Teams, five roles, two handoffs, read-only policy; poll Program projection and event stream while running. | Every required Team completed, all handoffs delivered, no approval in trust-all, no extra root tool calls, report agrees with Program state and evidence receipts. |

The phases are dependency-ordered: P3A produces the only evidence truth P4A
may present; P5A constrains the final Team whose result P6 accepts. Existing
dirty collaboration changes remain preserved and are prerequisites, not
silently claimed as complete by this amendment.

## Target contracts

### 1. Canonical semantic decision

Keep the generic `runtime_orchestrate` for non-Team graph operations. Route all
model-authored Team admission, including its generic-tool representation,
through one normalized `CollaborationDecision` adapter before validation. A
decision has semantic Team nodes, roles, directed data dependencies, required
outputs, and an explicit lifecycle intent: `turn_scoped` or `publish_catalog`.
Only `turn_scoped` is legal for user-named Teams unless the user requests
publication.

### 2. Program-owned execution terminal

Extend `CollaborationProgramControlState`/obligations with a typed outcome for
every Team instance: admitted child graph reference, execution status, delivery
status, failed role/acceptance obligations, retry disposition, evidence refs and
terminal timestamp. The Program transitions `Running -> Reconciling ->`
`Completed|Partial|Blocked|Failed` atomically with graph revision fencing.

The conversation host must remove receipt parsing as a source of truth. It may
read one typed Program terminal projection and force a text-only presentation
only when that terminal is `Completed` or an intentional `Partial` result.

### 3. Generic role dataflow validation

Compile each role into declared inputs, outputs and acceptance obligations.
Dependencies are validated as producer-output -> consumer-input edges, with DAG
and reachability checks. A final aggregation role must declare the inputs it
consumes; no code path may infer it from display names or a fixed role id. The
current terminal/upstream directional guard is transitional and must be deleted
once this typed dataflow contract is live.

### 4. Typed observable diagnostics

Create a stable `CollaborationDiagnostic` projection containing Program state,
Team instance, child graph/node, role binding, failed acceptance fields,
evidence references, failure class, retryability and recommended Runtime action.
`required_node_not_completed` becomes a compatibility rendering of this object,
not the only failure data.

### 5. Central approval policy

Program admission consumes one resolved authorization decision. `trust_all`
auto-admits; confirmation profiles create an expiring veto record and may admit
on timeout according to the policy. Neither model nor Team template decides
approval handling.

## Terminal architecture amendment: Semantic Contract Harness

### Decision in one sentence

**AI owns meaning; the compiler owns lowering; the Runtime kernel owns truth;
evidence owns completion.**

The current boundary asks a model to author too much execution IR. A model can
reliably state why a role exists, what it consumes, what it must produce and
how success should be judged. It should not have to know `RoleBehaviorFacet`,
exact Agent revisions, permission ceilings, leases, physical graph ids or
scheduler recipes. Those are deterministic compiler and kernel concerns.

The terminal framework has four planes with one-way authority:

| Plane | May decide | Must not decide |
| --- | --- | --- |
| User directive | Goal, explicit Team/role names, constraints, authorization intent, selected catalog assets and desired outcome. | Runtime ids, leases, terminal status or evidence facts. |
| Semantic AI | Decomposition, Team/role count when omitted, role purpose and responsibility, required capability/Skill predicates, dataflow, semantic acceptance and bounded replan proposals. | Effective grants, concrete instances, mutable catalog publication, resource admission or success claims without evidence. |
| Intent compiler | Normalize semantic aliases, derive execution facets, resolve approved Definitions/Skills/Tools, compile dataflow, crop permissions, estimate resources and emit immutable bindings. | Invent a user goal, silently change explicit names/constraints or relax an acceptance contract. |
| Runtime kernel | Authentication, authorization, effects, revisions, idempotency, graph state, scheduling, quotas, backpressure, leases, recovery, evidence verification, terminal commit and audit. | Guess business meaning from a role name or substitute an unrelated template. |

This is the flexibility/rigidity membrane. Flexibility is unrestricted on the
semantic side until it reaches an explicit policy or resource limit. Rigidity
begins exactly where an untrusted proposal could mutate state, consume bounded
resources, claim evidence or become durable truth.

### The small canonical contract

The end-to-end chain uses four different artifacts instead of treating one
large JSON object as prompt, program and receipt at the same time:

```text
AuthenticatedUserDirective
  -> FrozenCollaborationIntent
  -> CompiledCollaborationProgram
  -> CollaborationOutcome
```

`AuthenticatedUserDirective` preserves the exact user-authored goal, names,
constraints, selected assets and authorization context. The model cannot
rewrite it. `FrozenCollaborationIntent` is the only model-authored semantic
artifact and contains:

```text
schema_version, intent_id, revision, goal
teams[]: semantic_key, display_name, objective, depends_on[]
roles[]: semantic_key, display_name, purpose, responsibilities[]
         required_capabilities[], required_skill_predicates[]
         inputs[], outputs[], acceptance[], assumptions[], confidence
completion: required_outputs[], evidence_policy, partial_result_policy
constraints: effects, budget_hint, latency_class, user_fixed_fields[]
provenance: user_preserved_fields[], model_decisions[], retrieved_pattern_refs[]
```

`CompiledCollaborationProgram` is Runtime-only and adds exact Definition and
Skill revisions, typed behavior facets, permission/effect grants, resource
scopes, instance identities, budgets, deadlines, idempotency keys, graph nodes,
leases, approval decision and recovery fences. `CollaborationOutcome` contains
only durable Team/role terminals, evidence references, diagnostics, retry
disposition and the final verified result.

JSON remains suitable as a transport serialization and durable debug format;
it is not the semantic/programming interface. Native tool calling,
structured output or a constrained text codec may all feed one decoder. The
decoder must immediately produce the same versioned typed intent. Tolerant
transport repair may fix wrappers, aliases and representation shape, but it
must never reverse a dependency, add a role, select a template or weaken an
acceptance rule. Any semantic ambiguity becomes a typed correction request.

### Forward execution and reverse compensation loop

```text
Understand -> Freeze -> Compile -> Admit -> Execute -> Observe -> Verify -> Deliver
     ^           |          |         |         |          |         |
     |           +----------+---------+---------+----------+---------+
     |             typed diagnosis / bounded semantic patch
     +---------------------------------------------------------------
```

1. **Understand.** The model converts the current directive into a semantic
   intent, preserving all user-fixed fields and recording assumptions.
2. **Freeze.** Runtime checks ownership, schema and ambiguity, then persists an
   immutable intent revision before any execution identity is allocated.
3. **Compile.** A deterministic compiler derives mechanics and resolves only
   approved registry assets. No provider/network call is legal here.
4. **Admit.** One policy decision applies authorization, veto semantics,
   capacity reservation and backpressure.
5. **Execute.** Dependency-ready work runs concurrently through the existing
   graph scheduler and `ResourceManager`.
6. **Observe.** Program, Team, role, provider and tool facts stream from
   durable state; monitoring describes actual work rather than merely polling.
7. **Verify.** Typed obligations and evidence decide completion; prose cannot.
8. **Deliver.** Surface renders the Program terminal and attributes optional
   model narrative separately.

Reverse compensation is deliberately asymmetric:

- Mechanical representation errors are repaired once by the decoder/compiler
  and recorded in a repair receipt.
- Capacity pressure is handled by queueing, backpressure or a policy-owned
  reduction; the model is not asked to guess provider capacity.
- A local semantic gap returns a minimal diagnostic with immutable context and
  an allowed patch surface. The model revises only that intent revision.
- A material change to user-fixed scope, write effects, cost/risk ceiling or
  completion semantics returns to the user. No retry loop may infer consent.
- Repeated identical semantic failures terminate with a root-cause diagnostic;
  they never consume an unbounded number of model rounds.

### Role and Team creation without hard-coded business logic

When the user supplies a Team or role name but no template, the correct flow is
not catalog matching. Runtime asks the model to create a turn-scoped semantic
definition:

1. Preserve every explicit Team/role display name and relationship.
2. Let the model generate missing purpose, responsibilities, capability/Skill
   requirements, inputs, outputs, acceptance, assumptions and confidence.
3. Let the compiler derive generic reducer/verification/upstream-consumption
   facets from dataflow and the completion contract.
4. Resolve required capabilities against the approved Agent/Skill/Tool
   registries. Select exact revisions only after authorization cropping.
5. If resolution is impossible, emit `capability_gap`, `skill_gap` or
   `ambiguous_semantics`; never fall back to a builtin role or template.
6. Freeze the result for this turn. It is not a reusable catalog object until
   it passes the separate evolution pipeline.

The model therefore defines **what competence is needed**, while Runtime
defines **which approved executable asset may supply it**. An exact Definition
or template reference remains legal only when the user explicitly selects it
or a policy-authorized planner deliberately pins it with visible provenance.

The UI must render this state explicitly:

```text
AI-composed Team · turn scoped · not published
  Team name / objective
  Role name / generated responsibility / required capabilities
  resolved Agent + Skill revisions / effective grants
  dependencies / current work / evidence / terminal diagnostic
```

In `trust_all`, this card appears as execution starts and retains Cancel. In a
confirmation profile it shows the bounded veto countdown and automatically
continues on timeout only when the resolved policy permits. A missing template
never becomes an approval or publication wait.

### What is code, configuration, registry data and AI judgment

| Kind | Required examples | Boundary rule |
| --- | --- | --- |
| Hard-coded kernel mechanism | Intent/program state machines; schema and revision fences; DAG/cycle and producer-consumer validation; idempotency; atomic terminal commit; lease ownership; cancellation propagation; evidence types; effect classes; permission intersection; queue bounds; stale-write rejection; diagnostic categories. | Finite, deterministic, exhaustively tested and independent of business role names. |
| Versioned policy/configuration | Provider/session/program/team capacity; pending queue limits; timeouts; retry ceilings; veto windows; cost/token budgets; fairness weights; deployment topology limits. | One resolved snapshot per run; no scattered magic numbers or model-authored limits. |
| Registry/evolution data | Agent Definitions, Skills, Tool contracts, semantic patterns, Team templates, evaluation scenarios and release pointers. | Versioned, provenance-bearing, evaluated, promotable and rollbackable. |
| AI semantic judgment | Roles, responsibilities, skills needed, topology, decomposition, evidence relevance, assumptions, confidence, synthesis and bounded replans. | Free to vary for every request, but frozen per revision and unable to grant itself authority. |
| Presentation only | Localized Team/role names, descriptions, progress wording and layout. | Never used for behavior, dependency, authorization or acceptance decisions. |

Some mechanical vocabulary must be coded because an executor cannot safely run
an unbounded invented opcode. The stable vocabulary should be minimal:
`spawn`, `join`, `verify`, `dispatch`, `commit`, plus typed effects and resource
kinds. Business concepts such as researcher, architect, reviewer, CTO, legal
advisor or arbitrator must never be opcodes. `review` and `synthesis` can remain
reusable semantic patterns, while the compiler lowers them to generic
dataflow/join/verify mechanics.

Capability extensibility uses a registry, not an ever-growing prompt enum. A
registry capability has a stable id, a hard-coded effect class, required
permission, Tool/Skill adapters and evaluation contract. The model requests a
capability predicate; only Runtime can resolve it to a registered id and grant.
Unknown capability ids fail closed but return a typed gap that AI can explain.

### Experience, template and Skill sedimentation

Execution history, reusable guidance and executable assets are distinct
lifecycles:

| Layer | Contents | May execute directly? | Reuse rule |
| --- | --- | --- | --- |
| Execution episode | Frozen intent, actual bindings, events, receipts, outcome, cost/latency and failure diagnosis. | No. | Audit and candidate evidence only; never injected wholesale into a later turn. |
| Semantic pattern | Parameterized topology skeleton, capability slots, invariants, applicability/exclusion conditions and quality statistics. | No. | Retrieved as advisory context; must be re-instantiated against the current directive. |
| Executable revision | Approved Agent Definition, Skill, Tool contract or Team template with exact version and evaluation contract. | Yes, after normal authorization/admission. | Selected explicitly or by an authorized resolver; immutable during a run. |

The promotion chain is:

```text
successful/failed episodes
  -> evidence-backed ExperienceCandidate
  -> deduplicate + isolate variables
  -> baseline/candidate evaluation
  -> policy review and optional Canary
  -> versioned promotion
  -> monitored reuse
  -> rollback/deprecation pointer
```

One successful run never publishes a template. Promotion requires the minimum
sample/confidence/evidence thresholds in a versioned evaluation policy and
must prove non-inferiority on safety, correctness, latency and cost. Failed
runs may improve diagnostics or an eval scenario but cannot become positive
workflow guidance merely because they are frequent.

Reusable patterns are parameterized by capability slots and dataflow, not by
the original display names, source paths or session ids. Current-turn authority
always has this precedence:

```text
kernel safety and organization policy
  > authenticated explicit user directive
  > frozen current intent
  > approved executable revision
  > retrieved semantic pattern
  > unverified episode or model prior
```

This prevents yesterday's Team/template from contaminating a new explicit
request. Existing `EvolutionCandidateKind` already names Agent Definition,
Skill package and Team template targets, while the current governed
`EvolutionCandidateSubject` accepts only Agent Definition and Team template
revisions. P10 must close that ownership gap rather than treating Memory L4 as
an executable template registry.

## Performance and concurrency assurance

### What the architecture can and cannot guarantee

The design can guarantee correctness invariants, bounded queues, deterministic
admission, cancellation/recovery behavior and absence of unbounded spawning.
It cannot promise an arbitrary throughput or latency independent of hardware,
provider rate limits, token volume and external tools. Those are guaranteed
only for a named capacity profile after benchmark and soak gates pass.

The semantic layer does not need another model round in the normal path. Its
normalization, graph validation and compilation cost must be
`O(teams + roles + dependencies + requirements)`; registry lookup must use a
revisioned index. It performs no network/provider call and holds no Program
mutation lock. A content-addressed cache may reuse only the normalized semantic
skeleton keyed by schema version, semantic digest and registry/policy
revisions. Per-session grants, ids, leases, evidence scopes and bindings are
always compiled anew and never shared from the cache.

### Concurrency model

- One Program revision is sequenced by optimistic revision/CAS; compilation
  happens outside the mutation lock and only the fenced commit is serialized.
- Different Programs, Sessions and Missions remain concurrent. There is no
  global orchestration mutex and no permanent task per durable entity.
- Within a Program, dependency-ready Teams run in parallel. Within a Team,
  roles without producer-consumer edges run in parallel; joins wait only for
  their declared policy (`all`, `any` or quorum).
- `ResourceManager` remains the single capacity/admission queue owner for
  Session Turn, provider/account/model/token pool, Agent, parent execution and
  Tool resources. The intent compiler must not create a second semaphore.
- Admission reserves capacity before expensive context hydration or provider
  dispatch. Slow providers, tools and subscribers propagate bounded
  backpressure; they never cause unbounded task/event accumulation.
- Same-key mutations are ordered, cross-key work is fair, and cancellation,
  deadline and lease loss flow from Program to Team to Agent/Tool.

Current source has three inconsistent apparent ceilings: model multiplicity
allows 100, orchestration falls back to 32, and one Team graph hard-stops at
32 Agent nodes. P9 replaces those independent decisions with one versioned
`ExecutionCapacityProfile`; a documented kernel safety maximum may remain as a
last-resort allocation guard, but it must not silently conflict with the model
schema or deployment policy.

### Required performance gates

Numeric thresholds are initial acceptance targets for the CI reference host;
release evidence must record the hardware/provider profile and may tighten
them. They are not claims about the current dirty worktree.

| Gate | Required proof |
| --- | --- |
| Compiler latency | At the configured maximum Program shape, freeze + normalize + validate + compile p95 <= 50 ms and p99 <= 100 ms, with zero provider/network calls. |
| Relative control-plane cost | Against the pre-P7 baseline, p95 non-provider control-plane latency and peak memory regress by no more than 5%. |
| Durable admission scale | Preserve the existing 100 Programs x 10 Teams stress case, record per-stage p50/p95/p99, and replace its total-only 60-second bound with calibrated p95/p99 assertions. |
| Capacity saturation | At the configured ceiling, throughput remains stable; at 2x offered load, pending queues stay within policy and excess work receives typed overload/backpressure instead of unbounded growth. |
| Fairness | No continuously eligible fairness key starves behind another key; maximum queue age is bounded by the capacity policy and observable. |
| Parallelism | Independent Programs and dependency-ready branches overlap in traces; same-Program revision commits never overlap. |
| Lock/wait safety | Automated instrumentation proves no provider, tool, network, subscriber or storage wait occurs while a global/Program mutation guard is held. |
| Recovery | Crash/restart, lease loss, provider stream loss, timeout and terminal/delete races preserve at-most-one terminal commit and reject stale results. |
| End-to-end effect | Real-provider/WebUI scenario throughput and terminal latency are non-inferior to baseline while correctness and evidence gates remain hard. |

### Whole-chain state, queue and failure ownership

| State | Canonical durable owner | In-memory role | Revision/fence | Recovery rule |
| --- | --- | --- | --- | --- |
| User directive | Session/Turn input log | Projection cache only | turn id + input revision | Replay exact authenticated input. |
| Frozen intent | CollaborationProgram metadata | Parsed immutable snapshot | intent id + revision + digest | Reload; never ask model to reconstruct accepted semantics. |
| Compiled program/bindings | Execution graph + Program/Team snapshots | Scheduler working set | graph revision + binding digests | Resume exact revisions; never resolve latest defaults. |
| Approval | ApprovalQueue | Deadline worker/index | approval id + policy revision | Reconcile deadline; honor veto/auto-timeout policy once. |
| Resource admission | ResourceManager observations plus graph node state | One bounded queue | request id + policy revision | Re-submit idempotently from durable ready state. |
| Evidence/results | Team/role packets and Runtime evidence store | Reducer cache | node/attempt/lease fence | Reject late or wrong-scope receipts. |
| Terminal outcome | CollaborationProgram control state | Surface projection cache | atomic graph revision | Re-project; model prose cannot reopen it. |
| Reusable candidate | Evolution governance ledger | Ranking/index cache | candidate/revision/digest | Continue evaluation/release or roll back exact pointer. |

| Queue/event | Producer | Single claim/consumer owner | Ordering/idempotency/backpressure |
| --- | --- | --- | --- |
| Session input | Gateway | Runtime Turn inbox | Per Session order; input idempotency; bounded pending policy. |
| Program mutation | Semantic model/Runtime recovery | Collaboration coordinator | Per Program CAS; mutation id; conflict retry ceiling. |
| Resource admission | Graph runner | ResourceManager | Service class + fairness key; request id; bounded per-instance/class/key queues. |
| Team/Agent work | Graph scheduler | Execution supervisor | Dependency order; node attempt/lease; configured capacity. |
| Tool/provider stream | Agent executor | Tool host/provider runtime | Invocation/stream sequence; cancellation and bounded buffers. |
| Program events | Runtime commit path | Projection/outbox consumer | Commit-before-publish; event revision; slow-consumer policy. |
| Evolution candidate | Outcome evaluator | Evolution governance service | Candidate id/digest; bounded evaluation budget; no self-promotion. |

| Path | Lock/gate scope | Await permitted while held? | Parallel scope and bound |
| --- | --- | --- | --- |
| Intent freeze/Program commit | One Program revision guard/CAS | No model/tool/network await. | Other Programs concurrent; bounded CAS retries. |
| Registry snapshot resolution | Revisioned read snapshot | No external await. | Shared reads; immutable snapshot. |
| Resource admission | ResourceManager queue mutex/notify | Queue wait occurs after releasing mutex. | Bounded by capacity and pending policy. |
| Team execution | Parent-execution capacity lease | Provider/tool awaits only after grant. | Bounded by resolved profile and role DAG. |
| Projection stream | Per subscriber bounded buffer | Slow write never holds Program lock. | Disconnect/coalesce according to policy. |
| Evolution evaluation | Candidate-scoped lease | Provider/tool awaits allowed outside governance commit lock. | Bounded paired runs and Canary policy. |

| Failure | Owner and terminal response | Recovery/stale-result rule |
| --- | --- | --- |
| Invalid/ambiguous semantic intent | Intent decoder/compiler returns typed patch surface. | No graph is admitted; at most one bounded semantic repair per revision class. |
| No approved Agent/Skill/Tool match | Capability resolver emits a gap. | AI may revise role requirements or user may authorize/install; no builtin substitution. |
| Queue overload/deadline | ResourceManager emits typed overload/expiry observation. | No hidden retry storm; policy may requeue once or Program records partial/blocked. |
| Provider/tool interruption | Owning executor classifies retry safety. | Retry only with invocation idempotency/effect receipt; partial side effects remain visible. |
| Process crash/restart | Execution supervisor + Program coordinator. | Reload frozen intent/bindings and reconcile non-terminal graphs/approvals. |
| Lease loss/stale completion | Graph commit fence. | Reject stale result and requeue/recover current attempt only. |
| Cancel/delete/terminal race | Program commit service. | One atomic terminal wins; descendants receive cancellation and cannot overwrite it. |
| Subscriber lag | Projection owner. | Coalesce or disconnect; execution is unaffected and can be re-projected from durable state. |
| Bad reusable candidate | Evolution governance. | Mark ineligible/rollback exact release pointer; baseline remains available. |

## Source fact map for the amendment

| Symbol/current path | Current responsibility and classification | Terminal decision |
| --- | --- | --- |
| `ModelCollaborationControlDecision`, `ModelTemplateProposal`, `ModelProposedRole` in `crates/harness-contract/src/orchestration.rs` | Active model-facing carrier. It currently exposes low-level behavior, grants, cardinality, exact Agent references and several dependency codecs. | Keep one versioned semantic carrier but reduce it to intent fields. Transport compatibility decodes to that carrier and is not a second internal path. |
| `RoleBehaviorFacet` in `crates/harness-contract/src/team/binding.rs` | Active compiled behavior carrier; generic, not a role-name branch. | Keep in compiled snapshots; remove it from required model authoring and derive it in `IntentCompiler`. |
| `TeamRoleDefinition` / `TeamTemplateRevision` | Active durable executable Definition carrier. | Keep exact immutable revisions. Turn-scoped semantic roles compile to snapshots; promotion creates a separate evaluated revision. |
| `AgentCapabilityContract` / `AgentBindingSnapshot` | Active enforcement and immutable binding carriers. | Keep exact grants/Skill refs in Runtime; add a distinct model capability-requirement contract. |
| `AgentCatalog::discover` and `AgentBindingCompiler` | Active registry lookup and permission-intersection services. | Extend discovery to capability/Skill predicates with explainable ranking; binding compiler remains final grant owner. |
| `CapabilityRecipeId::{Review,Synthesis}` and model-authored `RoleBehaviorFacet` | Active semantic conveniences that currently leak compiler mechanics into the model contract. | Demote to compiler/pattern vocabulary; model describes dataflow and completion meaning. Generic compiled opcodes remain finite. |
| `ResourceManager` in `crates/runtime/src/execution_core/graph/resources/manager.rs` | Active single capacity queue with service classes, fairness keys and bounded pending policy. | Keep as sole owner; extend capacity profile/metrics, do not add orchestration semaphores. |
| `ORCHESTRATION_VETO_WINDOW_MS`, default `32`, `MAX_TEAM_GRAPH_AGENT_NODES=32`, schema max `100` | Active policy values scattered across contract/compiler/Team code. | Move operational values to one versioned capacity/approval policy; retain only a documented absolute allocation safety bound. |
| `EvolutionCandidateKind` versus `EvolutionCandidateSubject` | Active governance: kind taxonomy includes Skills and templates, but governed subjects currently admit only Agent/Team revisions. | Add explicit governed adapters for semantic pattern and Skill revisions or narrow advertised kinds; Memory promotion is not a substitute. |
| `CollaborationProgramControlState`, `CollaborationDiagnostic` | Active durable truth/projection carriers. | Extend with frozen intent/outcome refs and semantic repair diagnostics; keep Program terminal authoritative. |

### Mission/Session/Task/Team/Agent conversion map

| Conversion | Canonical operation | Identity/authority rule |
| --- | --- | --- |
| User input -> Session Turn | Gateway authenticates and Runtime appends the input envelope/Turn checkpoint. | The Session owns conversational order; a model response cannot create a second Turn identity. |
| Session Turn -> root Task/Program | Runtime freezes the intent and creates or links the bounded Task and `CollaborationProgram`. | User directive and policy revisions are carried forward; Program ids are Runtime-issued. |
| Mission -> Task graph | Mission governance links the required bounded work and progress. | A Team is not a Mission scheduler and cannot create a parallel mission registry. |
| Program semantic Team -> `TeamInstantiationRequest` | `IntentCompiler` emits one immutable request and Program obligation per Team instance. | The model's semantic key is provenance, not the physical Team/graph identity. |
| Team request -> role Tasks | `TeamRuntime`/instantiation emits `TaskCreateCommand`, child graph and frozen role snapshots. | Team topology is declarative; ExecutionGraph remains the only scheduler. |
| Role requirement -> Agent Definition/Binding | Capability resolver ranks approved Definitions; `AgentBindingCompiler` intersects grants and pins exact revisions. | Definition is reusable identity; `AgentInstanceRef` is one run and never updates the Definition. |
| Agent result -> Team/Task/Program terminal | Acceptance evaluator, Team reducer and Program reconciliation commit typed results/evidence. | A result packet or parent prose cannot independently mark any enclosing lifecycle complete. |

### Resource and backpressure map

| Resource | Capacity owner | Admission/reservation | Queue/fairness/backpressure | Required metrics |
| --- | --- | --- | --- | --- |
| Session Turn | `ResourceManager` + Turn inbox | Reserve before starting a provider turn. | Bounded per Session/fairness key; reject or defer typed excess input. | pending, queue age, active, terminal latency. |
| Provider/account/model/token pool | `ResourceManager` and provider adapter | Atomic bundle grant before dispatch/token reservation. | Provider-specific quota and fairness key; retry-after becomes capacity evidence. | utilization, 429/rate limit, queue p50/p95/p99, tokens. |
| Program/Team/Agent | `ResourceManager` parent-execution and Agent kinds | Resolve one capacity profile and reserve before context hydration. | Dependency-ready queue; per-parent ceiling; no unbounded spawn. | runnable/active/waves, saturation, starvation, deadline. |
| Tool | `ResourceManager` + Tool host | Grant before invocation; effect/idempotency contract captured. | Per Tool/resource key limits; slow Tool propagates cancellation/backpressure. | queue/run latency, errors, side-effect receipts. |
| DB read/write/background | DB pool and commit service | Connection/transaction admission before graph commit. | Pool bounds; short transactions; background work cannot drain interactive capacity. | pool wait, transaction duration, conflicts, write latency. |
| Context/Memory/Reality hydration | Owning store plus parent budget reservation | Estimate/reserve bytes/tokens before hydration. | Bounded recall/result size; truncate by policy with evidence, never unbounded load. | bytes/tokens hydrated, cache hit, eviction, latency. |
| Events/projections/subscribers | Commit/outbox and Surface subscriber buffers | Publish only after durable commit. | Per-subscriber bounded buffer; coalesce/disconnect slow consumers. | lag, dropped/coalesced count, reconnect/replay latency. |
| Files/attachments/connectors | Tool host/connector adapter | Scope and size checked before open/upload/action. | Bounded stream chunks and concurrency; cancel closes handles. | bytes, open handles, transfer latency, partial effects. |

### Capability preservation matrix

| Capability | Current path | Terminal path | Parity/performance proof | Delete proof |
| --- | --- | --- | --- | --- |
| Arbitrary user Team/role names | `ModelTemplateProposal` -> `template_candidate` -> Team snapshot | Small semantic role intent -> compiler -> same immutable snapshot | Generative multilingual names and unknown-domain roles; no extra model round on valid input. | Production scan for display-name/id behavior branches and builtin fallback. |
| Explicit catalog selection | Model exact template/Agent references | User-preserved explicit selection -> registry validation -> exact revision binding | Exact revision, revoke and missing revision tests. | No `LatestStable` or fallback when an exact user pin is present. |
| Permission and Skill safety | Grant ceilings + `AgentBindingCompiler` intersection | Semantic requirements -> resolver -> unchanged binding intersection | Existing ceiling tests plus unknown capability/Skill fail-closed tests. | Model-facing intent contains no effective grant/lease field. |
| Team dataflow and synthesis | Model dependencies/behavior facets -> graph | Semantic inputs/outputs/completion -> compiler-derived facets/graph | DAG property tests, join/quorum, direction and evidence acceptance tests. | No production inference from display name, graph position or result label. |
| Approval/veto/autonomy | Orchestration approval router/queue | One resolved versioned policy snapshot at Program admission | trust-all, human veto, timeout-auto and deny tests with audit receipts. | No template or model-controlled approval branch; no scattered veto constant. |
| Durable recovery/terminal truth | Graph/Program plus remaining host inference | Frozen intent/bindings and Program outcome only | Crash at every state, stale completion and UI reverse-evidence tests. | Host/transcript success inference scans. |
| Bounded concurrency | `ResourceManager` plus scattered topology hints | One capacity profile feeding existing manager and scheduler | Scale, saturation, fairness, cancellation and memory tests. | Scans for independent 100/32/default admission decisions. |
| Reusable Agent/Team knowledge | Definition/evolution stores and L4 knowledge promotion | Episodes -> semantic candidate -> governed versioned asset | Paired baseline, Canary, contamination, rollback and lookup latency tests. | No turn-scoped success directly publishes or becomes executable Memory. |

### Deletion preflight

| Delete/demote target | Current compile dependents/state carried | Replacement and caller rewiring | Tests and final scan |
| --- | --- | --- | --- |
| Model-authored `ModelProposedRole.behavior` | Harness schema/fixtures, Gateway bootstrap schema text, `template_candidate` normalization/compile, orchestration fixtures. It currently carries required compiled mechanics. | P7 adds semantic input/output/completion fields; P8 derives `RoleBehaviorFacet` before `TeamRoleDefinition`. Persisted compiled snapshots retain facets. | Migrate schema/fixtures; scan model contracts/Gateway text for required `behavior`; compiled binding tests still require non-empty derived facets. |
| Optional/unknown `agent_definition_ref` safe builtin fallback | `normalize_agent_definition_ref`, candidate compilation and fallback-positive tests in `template_candidate.rs`; model schema promises fallback. | Capability resolver consumes semantic predicates; explicit exact pins validate exactly; unresolved requirements return typed gaps. | Replace fallback-positive tests with resolver/gap tests; scan production for `default_agent_ref_for_ceiling` and safe-builtin fallback messages. |
| Model-facing review/synthesis mechanics | `CapabilityRecipeId`, runtime request/compiler matches, model affordance and schemas. They carry both semantic convenience and executable lowering. | Keep compatibility only at the transport decoder for persisted/old input; normalize to generic semantic work/dataflow/completion and compile to generic nodes. | Equal-digest compatibility fixtures; production internal-IR scan proving Review/Synthesis are not required model decisions. |
| Multi-shape internal dependency handling | `ModelTemplateDependencies` and tolerant candidate parser/tests. | Boundary decoder accepts versioned legacy shapes and immediately emits canonical producer/consumer pairs; all validators/compiler callers consume only pairs. | Codec fixtures; scan Runtime compiler/Team instantiation for alternate dependency variants. |
| Scattered concurrency/veto values | Contract multiplicity max, compiler `unwrap_or(32)`, Team graph `32`, orchestration `5_000` and their tests. | Versioned approval/capacity snapshot plus one documented allocation representability guard. | Config boundary tests and scan for retired literals/constant names in decision paths. |
| Conversation/transcript terminal inference | Conversation host progress/receipt helpers and presentation callers. It may carry useful projection formatting but not durable truth. | Program terminal projection and diagnostic; move any pure rendering to projection/Surface before deletion. | Caller-by-caller migration, restart tests and production scan for collaboration success derived from messages/tool text. |

No carrier is deleted before its replacement is production-wired. Legacy
durable data decoders may remain explicitly scoped to recovery/ingress, but no
legacy type may remain an active internal writer or scheduler input.

## Terminal amendment implementation DAG

```text
P0-P6 correctness/reality closure
          |
          v
P7 semantic contract simplification
          |
          v
P8 deterministic intent compiler and role resolver
       /                           \
      v                             v
P9 capacity/concurrency closure   P10 experience/evolution closure
       \                           /
        +------------+------------+
                     v
P11 Surface + real-provider integrated closure
```

Release grouping is dependency-preserving:

```text
v0.9.704 frozen baseline
  -> v0.9.705 [P7 + P8]
  -> v0.9.706 [P9 + approval/live projection P11]
  -> v0.9.707 [P10 + terminal integrated P11]
```

P11 is deliberately split by ownership, not partially claimed: v0.9.706 owns
the live projection protocol and Surface wiring needed to observe later tests;
v0.9.707 owns the complete real-provider, browser, failure, recovery and reuse
acceptance matrix.

| Phase | Closed goal and source allowlist | Required deletion/rewire | Acceptance evidence |
| --- | --- | --- | --- |
| P7 | One small semantic intent: `crates/harness-contract/src/orchestration.rs`, `crates/harness-contract/src/team/{definition,binding}.rs`, `crates/runtime/src/orchestration/{request,validator}.rs`, `crates/runtime/src/team/template_candidate.rs`, `crates/gateway/src/runtime/{runtime_bootstrap,gateway_tool_executor}.rs`. | Remove model obligation to author behavior facets, physical Agent refs and mechanical recipes. Collapse dependency variants after boundary decoding; version persisted legacy reads without retaining two active internal contracts. | Native tool, structured-output and tolerant legacy fixtures normalize to one intent digest; property tests with arbitrary names; no semantic auto-repair. |
| P8 | `IntentCompiler` and capability resolver: `crates/runtime/src/orchestration/{request,validator,compiler,team_authority}.rs`, `crates/runtime/src/team/{template_candidate,instantiation,team_binding}.rs`, `crates/runtime/src/agent/{catalog,binding}.rs`. | Delete default builtin role/template substitution and behavior-from-name/position heuristics. Derive facets/dataflow mechanics; resolve exact Definitions/Skills/Tools and emit typed gaps. | Goal-to-binding reverse audit; arbitrary-role generative tests; missing-capability negative tests; restart with exact binding digests. |
| P9 | One capacity profile: `crates/harness-contract/src/{orchestration,execution_graph/contract}.rs`, `crates/runtime/src/orchestration/{mod,request,compiler}.rs`, `crates/runtime/src/team/instantiation.rs`, `crates/runtime/src/execution_core/graph/{runner,resources/manager}.rs`, `crates/runtime/src/projection/{mod,snapshot,activity}.rs`. | Remove scattered 100/32/default ceilings and duplicate orchestration capacity decisions; move veto/timeout values to policy snapshots. | Compiler/scale/saturation/fairness/lock/recovery gates above; configuration and projection tests; source scan for retired magic values. |
| P10 | Governed experience/pattern/Skill promotion: `crates/runtime/src/evolution/{candidate_kind,governance}.rs`, `crates/runtime/src/team/{template_candidate,l4_promotion}.rs`, `crates/runtime/src/skill/{mod,governance}.rs`, `crates/runtime/src/agent/definition_registry.rs`, `crates/harness-contract/src/{agent/definition,team/definition,skill/mod}.rs`. | Do not auto-publish successful turn templates; do not use Memory L4 as executable authority; close or remove advertised subject kinds lacking a promotion adapter. | Repeated-evidence threshold, paired baseline, Canary, rollback, contamination and precedence tests. |
| P11 | Gateway/Surface/eval: `crates/gateway/src/runtime/{runtime_bootstrap,gateway_tool_executor}.rs`, `crates/gateway/src/api_routes/{runtime_routes,live_routes,evolution_routes,audit_routes}.rs`, `crates/runtime/src/projection/*`, `crates/harness-contract/src/projection/*`, `crates/tui/src/{app_core/protocol.rs,components/agent_team_panel.rs}`, `crates/harness-eval/src/{runner,live_scenario_runner}.rs`, and sibling `cowd-edge/surfaces/webui`. | Remove obsolete huge low-level model schema and unlabelled template fallback from the production WebUI; no model prose as status. Reference-app surfaces remain test fixtures, not the acceptance UI. | Fresh production-WebUI Session with custom names and no template, multi-Team dependencies, trust-all and veto timeout, capability gap/replan, restart, overload and reusable-pattern runs; observe every stage and prove Program/evidence/UI agreement. |

| Phase | Allowed residuals outside its closed boundary | Evidence file |
| --- | --- | --- |
| P7 | Versioned legacy decoding only at durable-read/transport ingress; it cannot write internal execution state. | `docs/evidence/collaboration-semantic-compiler-v0.9.705.md` |
| P8 | Historical compiled snapshots remain readable; no new snapshot is authored by the retired path. | `docs/evidence/collaboration-semantic-compiler-v0.9.705.md` |
| P9 | One named representability safety maximum may remain in the kernel; every operational limit is policy data. | `docs/evidence/collaboration-capacity-surface-v0.9.706.md` |
| P10 | Historical episodes and ineligible candidates remain auditable but are never executable defaults. | `docs/evidence/collaboration-semantic-harness-v0.9.707.md` |
| P11 | No residual is allowed in the user-visible semantic/execution/evidence chain. | v0.9.706 live-Surface evidence plus `docs/evidence/collaboration-semantic-harness-v0.9.707.md` terminal evidence |

Every phase must publish the state-truth, producer-consumer,
concurrency/wait, failure/recovery and resource/backpressure tables from its
actual implementation evidence. P11 is incomplete until the user-visible
capability reaches level 5: production wired, durable/recoverable,
failure/concurrency proven and operationally observable.

## Proposed locked decisions awaiting user confirmation

The audited plan uses these decisions as fixed implementation constraints. The
user confirmation gate accepts or rejects them as one coherent design; any
requested change reopens the plan audit before source implementation.

1. **AI-generated role semantics: accept.** AI generates missing duties,
   capability and Skill requirements; explicit user fields stay immutable.
2. **Compiler-derived mechanics: accept.** Model-facing behavior facets,
   exact bindings and scheduler recipes move behind the semantic membrane.
3. **Experience is advisory before promotion: accept.** A run becomes an
   episode first; only evaluated/versioned assets become executable defaults.
4. **Capacity is a named deployment contract: accept.** Kernel guarantees
   bounds and correctness universally; throughput/latency guarantees attach
   to measured capacity profiles rather than magic numbers.
5. **No-template UX: accept.** Show an AI-composed, turn-scoped Team and its
   resolved capabilities/provenance; never silently match a local template.

## Historical correctness phases and acceptance gates

The P0-P6 work below records the correctness path that produced the frozen
v0.9.704 baseline. It is retained for traceability and does not compete with
the three-version P7-P11 release packaging above.

| Phase | Closed boundary and write allowlist | Required deletion/rewire | Acceptance evidence |
| --- | --- | --- | --- |
| P0 | Contract and baseline audit: this document; `harness-contract/src/orchestration.rs`, `execution_graph/contract.rs`. | Classify all existing dirty changes; no new behavior. | Contract schema tests; source scan proving no user role-name branch is added. |
| P1 | Canonical Team decision normalization: `harness-contract/src/orchestration.rs`, `runtime/src/orchestration/{mod,validator,collaboration_coordinator}.rs`, Gateway transport tests. | Remove independent narrow/generic Team lowering semantics; both use one adapter. | Equal normalized command/snapshot for both ingress forms; arbitrary role-name/property tests. |
| P2 | Program terminal and recovery: `execution_graph/contract.rs`, `runtime/src/orchestration/{collaboration_coordinator,mod,result}.rs`, graph commit/recovery tests. | Remove conversation-owned success inference and `verified_team_ids` as authority. | Restart at every Program state; exact typed diagnostic for failed child/role; no terminal ambiguity. |
| P3 | Role dataflow and template lifecycle: `harness-contract/src/team/*`, `runtime/src/team/{template_candidate,instantiation,result_reducer}.rs`. | Delete name/behavior heuristic validation and conflated catalog/ephemeral paths. | Producer/consumer DAG property tests, invalid topology diagnostics, snapshot immutability and catalog-isolation tests. |
| P4 | Authorization and Surface: Runtime approval policy/projection plus Gateway/Surface read models. | Remove approval waits that duplicate Program policy; remove opaque node-only UI errors. | trust-all, deny, timeout-veto, recovery and UI diagnostic contract tests. |
| P5 | Integrated evaluation: real WebUI + provider scenarios and failure injection. | No compatibility bypass may remain in production paths. | Multi-round arbitrary-role matrix, provider/tool failure, cancellation, restart and concurrency tests; final reverse-evidence audit. |

## Non-negotiable gates

- No production branch may test a user role display name or role id to choose
  behavior. Runtime may inspect only typed behavior/dataflow contracts.
- A Team execution terminal must be persisted in Program/graph state before it
  is exposed to the parent model or Surface.
- Every required Team obligation has either a typed satisfied terminal or a
  typed diagnostic; opaque node-id-only failure is forbidden in the model/UI
  projection.
- No long-running Team/provider await occurs while a Program graph revision
  mutation lock is held.
- A user-named turn-scoped Team never creates a catalog revision and never
  waits for template publication approval.
- The model-facing intent never requires compiled behavior facets, effective
  grants, exact runtime instances, leases or scheduler opcodes.
- Capacity, retry, timeout and veto behavior resolves from one versioned policy
  snapshot; production decision paths contain no independent magic defaults.
- Execution episodes and Memory facts never become executable Agent, Skill or
  Team defaults without isolated evaluation, versioned promotion and rollback.
- Final validation must include real-provider WebUI runs in addition to unit
  and deterministic integration tests.
