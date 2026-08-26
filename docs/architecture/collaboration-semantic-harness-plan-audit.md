# Collaboration Semantic Harness — Pre-Implementation Plan Audit

## Audit result

**Verdict: executable after explicit user confirmation.**

The three-version plan is dependency-complete, has one owner for every durable
state and wait path, and contains no known unresolved architecture decision.
Implementation has not started. The remaining gate is the user's acceptance of
the proposed locked decisions and version boundaries.

Audited contracts:

- `docs/architecture/collaboration-program-hardening.md` — sole global authority;
- `docs/architecture/collaboration-semantic-harness-v0.9.705.md` — semantic
  intent and deterministic compilation;
- `docs/architecture/collaboration-semantic-harness-v0.9.706.md` — capacity,
  veto approval and live Surface truth;
- `docs/architecture/collaboration-semantic-harness-v0.9.707.md` — governed
  experience reuse and terminal acceptance.

Frozen code baselines:

| Repository | Branch | Tag | Commit | Tree |
| --- | --- | --- | --- | --- |
| `cowd-0821-terminal` | `integration/0821-terminal` | `v0.9.704` | `31f578078727c59035a2a2c47a219e50ae429676` | `913ebfd10622781c22a458f9e0d0abdc24830efd` |
| `cowd-edge` | `master` | `v0.9.704` | `04b63861e9e332576d08a2f81326942b22c92e9a` | `e78d575c4821a210e6f762edbd8afa785d463f7a` |

No push is included in this authorization. Version tags remain local until the
user separately authorizes a push.

## Problem statement audit

The plan addresses the observed failures as manifestations of one architectural
defect, not independent prompt bugs:

```text
natural-language user intent
  -> model currently authors Runtime mechanics
  -> compatibility/template code guesses missing mechanics
  -> several owners re-derive lifecycle, approval and capacity
  -> projections omit some live state transitions
  -> model/UI report can disagree with durable execution
```

The terminal design inserts a semantic membrane and removes downstream guessing:

```text
user intent
  -> AI-authored semantic collaboration decision
  -> typed validation + bounded compensation
  -> deterministic Runtime compiler/resolver
  -> immutable Program/Team/Agent bindings
  -> one approval and capacity kernel
  -> durable execution/evidence terminal
  -> contiguous projection
  -> truthful WebUI/TUI
  -> advisory experience -> governed promotion
```

This explains why repeated prompt tuning was fragile: the model was asked to
emit low-level behavior facets, exact Agent references, grant ceilings and
several dependency shapes, while Runtime later guessed defaults and the host
still inferred status from prose. The AI capability was not the only limiting
factor; the machine boundary was ambiguous and duplicated.

## JSON/semantic representation decision

JSON is suitable as a versioned transport encoding, but it is not by itself a
reliable bridge from semantics to execution. The audited bridge has five parts:

1. a small JSON Schema containing business semantics only;
2. strict typed decoding and canonical normalization;
3. a deterministic compiler that owns executable mechanics;
4. immutable provenance, policy and binding digests;
5. typed diagnostics that let the model repair only allowed semantic fields.

This separates positive program drive from reverse AI compensation:

- valid semantics compile and execute without another model round;
- incomplete or inconsistent semantics return field-level gaps, allowed repairs
  and authority boundaries;
- one bounded revision may correct the semantic decision;
- repeated identical gaps terminate visibly instead of prompting forever;
- Runtime never repairs a gap by selecting a builtin role or parsing a display
  name.

No alternative free-form DSL is required. A new syntax would move the ambiguity
rather than remove it.

## Flexible and rigid boundary audit

| Flexible, AI/user-authored semantics | Rigid, code-enforced mechanics |
| --- | --- |
| arbitrary Team and role display names in any language | schema version, identity normalization and digest rules |
| objective, responsibility and division of work | Program/graph/Team/Agent lifecycle state machines |
| number of semantic roles within policy bounds | resource ceilings, fair admission, cancellation and deadlines |
| capability, Skill and Tool requirements using registry identifiers | exact approved Definition resolution and permission intersection |
| producer/consumer dataflow and evidence meaning | DAG validation, behavior-facet derivation and immutable bindings |
| result fields and semantic acceptance | receipt validation, terminal commit and idempotency fences |
| one bounded semantic replan | retry count, allowed repair set and duplicate-diagnostic stop |
| advisory reuse suggestions | evaluation, approval, Canary, Stable and rollback authority |

Defining a role therefore does not require a production enum or a hardcoded
prompt branch. The model formulates duties, requirements and evidence semantics
from the user's current goal; Runtime compiles those semantics using a finite set
of mechanical opcodes. Finite code is required for safety and interoperability,
not for enumerating business roles.

The following finite concepts are intentionally hardcoded and versioned:

- state transitions and terminal invariants;
- stable dependency/acceptance relation kinds;
- capability and Tool permission enforcement;
- capacity/profile validation and queue fairness;
- approval/veto/timeout resolution;
- event schemas, idempotency keys, CAS fences and recovery;
- projection reducer operations;
- evaluation, release and rollback lifecycle.

Display names, role taxonomies, domain responsibilities, Team topologies and
test-specific wording are explicitly forbidden from production decision logic.

## Version dependency and completeness audit

| Version | Input precondition | Produces | Explicitly does not claim | Next-version dependency |
| --- | --- | --- | --- | --- |
| `v0.9.705` | clean `v0.9.704` baselines | semantic v2, compiler, resolver, Program provenance, typed compensation | capacity policy, live UI completeness, experience reuse | v0.9.706 consumes provenance and compiled binding facts |
| `v0.9.706` | both repos tagged/audited `v0.9.705` | one capacity profile, one approval wait, projection v3, WebUI/TUI live card | automatic or governed experience reuse | v0.9.707 consumes reliable terminal/projection/capacity facts |
| `v0.9.707` | both repos tagged/audited `v0.9.706` | episodes, advisory patterns, governed promotion and terminal E2E evidence | no later deferred programme requirement | closes the full chain |

There is no circular dependency. P11 is split by ownership: its observation
protocol must exist in v0.9.706 before v0.9.707 can honestly monitor and accept
the full scenario matrix.

## Contradiction and root-cause resolution matrix

| Discovered contradiction/risk | Evidence in current source | Locked resolution | Version/evidence |
| --- | --- | --- | --- |
| User asks for unseen Team/roles but Runtime falls back to local builtins. | `runtime/team/template_candidate.rs` normalizes missing/unknown Agent refs to explore/execute builtins. | Capability/Skill/Tool resolver returns exact approved binding or a typed gap; no template is selected. | v0.9.705 resolver and source scans |
| Model-facing schema asks AI to emit compiled behavior and grants. | `ModelProposedRole` and Gateway affordance/schema expose exact refs, ceilings and behavior facets. | Model emits semantic responsibility/dataflow/acceptance; Runtime derives facets and grants. | v0.9.705 contract/compiler tests |
| Equivalent dependency intent has multiple codecs and guessed group matching. | orchestration dependency variants and suffix/substring matching in candidate code. | Versioned ingress decoder normalizes to exact producer-to-consumer pairs; active IR has one form. | v0.9.705 codec/property tests |
| “JSON valid” is treated as “executable semantics valid.” | current conversion lowers in the contract/Gateway before registry and policy checks. | Transport decoding, semantic validation, resolution and lowering are separate typed phases. | v0.9.705 diagnostics/reverse audit |
| Capacity values are 42, 32, 32 and 100 in different owners. | Runtime control, compiler, Team constant and schema. | Extend existing Runtime control into one immutable profile; keep one non-operational kernel representability maximum. | v0.9.706 config/scale/scans |
| Adding a collaboration semaphore would duplicate scheduling. | existing `ExecutionResourceManager` already owns bounded/fair execution admission. | Inject the resolved profile through `RuntimeServicesBuilder`; no second scheduler or queue. | v0.9.706 contention/resource tests |
| Explicit user Team still waits on approval in full-auto mode. | orchestration has custom polling and a local veto window. | Trusted Runtime ingress + one policy snapshot: auto receipt in full-auto, confirmation/veto in confirming modes, timeout-auto, explicit veto stops. | v0.9.706 approval race matrix |
| Approval timeout polling can leak or race. | custom loop polls Queue every 100 ms while Coordinator already has `Notify`. | Reuse one ApprovalCoordinator wait path; deadline scheduler wakes Coordinator and graph waiters. | v0.9.706 waiter/restart tests |
| Effect approval could be accidentally bypassed by Team authorization. | root Team admission and Agent Tool approvals are distinct domains. | User Team confirmation affects orchestration admission only; Tool/effect authorization remains independent. | v0.9.706 negative tests |
| UI is a sibling repository, not Gateway-owned static HTML. | Gateway serves `cowd-edge/surfaces/webui/dist`; real components and reducer live in `cowd-edge`. | Synchronized version gates, allowlists and annotated tags across both repositories. | every version gate |
| Snapshot contains Program truth but live UI can stay stale. | projection v2 delta has no operation for `graph.orchestration` or complete delivery truth. | Projection/reducer v3 adds complete orchestration and delivery operations in Rust/TS/TUI. | v0.9.706 live browser/reconnect tests |
| Assistant prose can contradict durable Team execution. | historical host/tool-report behavior re-derived completion; current Program already has durable truth. | Surface and parent presentation consume Program terminal projection only; prose is attributed narrative. | v0.9.706/v0.9.707 reverse audit |
| Successful runs do not produce reusable execution experience. | evolution projector focuses on failure/tool signals; no typed successful collaboration episode. | Program terminal emits an idempotent privacy-safe episode; pattern projector aggregates durable facts. | v0.9.707 replay/restart tests |
| One success could silently become a template. | template proposal and L4 paths are easy to conflate with catalog authority. | Minimum three distinct eligible Turns; advisory only; explicit candidate and owner governance before executable reuse. | v0.9.707 threshold/authority tests |
| Memory L4 is mistaken for an executable asset registry. | L4 promotes `KnowledgeCandidate`; it is knowledge governance. | Keep knowledge contextual and non-executable; Team/Agent/Skill use their own registries and pointers. | v0.9.707 type/API scans |
| Evolution advertises kinds without matching subjects/adapters. | 14 `EvolutionCandidateKind`s; subject enum currently only Agent/Team. | Exhaustive owner router: real adapter or typed unavailable; never advertise execution by string alone. | v0.9.707 exhaustive routing tests |
| A brand-new Definition has no prior revision for paired evaluation. | governance currently requires scalar `baseline_revision`. | Versioned baseline: exact published revision or immutable qualified episode set. | v0.9.707 legacy/new baseline tests |
| Tests can pass by injecting APIs without exercising real UX. | live runner and WebUI Playwright exist but are separable. | Required fresh scenarios originate through real browser, use real Gateway/provider, and observe every durable stage. | v0.9.707 E2E artifacts |
| Test role names could leak into production behavior. | prior fixes risk scenario-specific prompt/code rules. | Fixed names/prompts live only in fixtures; production scans and renaming property tests forbid name-based behavior. | v0.9.705 and final scan |

All listed contradictions have one selected resolution. The default capacity
profile is also numerically locked in the v0.9.706 contract (32 Teams/roles/
Team-Agent nodes, configured Agent width 42, existing 4096/2048/512 pending
bounds, five-second aging/veto and one repair revision). None requires a user
choice beyond acceptance or rejection of the complete design.

## State-truth audit

| State | Sole owner | Prohibited duplicate authority |
| --- | --- | --- |
| Session/Turn order and user directive | Runtime conversation/session store | provider text or UI-local inferred turn |
| semantic collaboration decision | admitted Turn + canonical intent snapshot | template candidate JSON after compile |
| compiled Team topology and bindings | `CollaborationProgram`/immutable Team snapshots | role display names or latest catalog pointers |
| graph scheduling and terminal status | ExecutionGraph + Program coordinator | conversation receipt parsing |
| capacity admission and reservations | existing ResourceManager under frozen profile | Gateway ingress semaphore or Team-local semaphore |
| root collaboration confirmation | ApprovalCoordinator/Queue + policy snapshot | orchestration polling state |
| Tool/effect authorization | existing effect approval policy | root Team confirmation |
| delivery and user-visible execution truth | Program terminal/projection | assistant prose |
| experience and pattern history | Runtime event store/projectors | prompt memory |
| executable default revision | Definition/Skill release pointer owner | episode, pattern, L4 or candidate existence |

The forward and reverse chains in each version document cover every state
conversion. No model-owned or Surface-owned durable execution state remains.

## Source ownership and overlap audit

File overlap between versions is intentional only where a later closed boundary
extends an already-versioned carrier:

| Overlap | v0.9.705 purpose | v0.9.706/v0.9.707 purpose | Drift guard |
| --- | --- | --- | --- |
| `execution_graph/contract.rs` | semantic provenance in Program | capacity/approval refs, then terminal episode source compatibility | each version starts at prior clean tag; additive schema and legacy read tests |
| orchestration coordinator/module | compile/admit semantic Program | approval/capacity control, then terminal event emission | no lifecycle re-owner; Program remains canonical |
| projection contracts/runtime | expose v0.9.705 provenance-compatible state | v3 live truth, then additive experience/governance visibility | reducer schema/golden and reconnect tests |
| Gateway/generated WebUI types | transport new contract | v3 live delta and later Audit fields | generated diff reviewed in core then edge |
| Team instantiation | consume deterministic binding/facets | capacity snapshot, then governed Definition selection tests | exact snapshot digest/restart gates |

No two active agents or repositories may edit the same file concurrently during
implementation. Work proceeds one version at a time, with a clean status and
evidence review before the next. Any required file outside a version allowlist
forces a documented allowlist amendment before editing.

## Concurrency and lock audit

The design does not add an independent orchestration queue. Its wait graph is:

```text
semantic compile (bounded, no external await)
  -> graph CAS commit
  -> optional ApprovalCoordinator Notify wait (no graph lock/permit)
  -> ResourceManager fair admission
  -> provider/Agent/Tool work under owned leases
  -> short revision-fenced commits
  -> outbox/projection/experience background consumers
```

Required invariants are complete:

- no graph/store/config lock spans provider, Tool, user, subscriber or
  background-evaluation waits;
- no resource permit is reserved during the confirmation window;
- approval timeout, veto, cancellation and restart converge through one durable
  decision fence;
- queue sizes exist at instance, service-class and fairness-key levels;
- projection and Audit subscriber buffers are bounded and recover by cursor;
- experience aggregation is partitioned by signature and fenced by event-stream
  revision;
- evaluation uses background capacity and cannot starve interactive Turns;
- terminal replay, duplicate Tool calls and projector restart are idempotent.

The v0.9.706 saturation/race tests establish kernel correctness before v0.9.707
adds background experience load.

## Failure and recovery coverage audit

| Boundary | Before durable commit | After durable commit | Recovery evidence |
| --- | --- | --- | --- |
| semantic admission | typed gap, no Program | same decision id/digest returns receipt | duplicate/restart fixtures |
| binding | no graph and exact diagnostic | immutable binding replays; no re-resolution | exact digest recovery |
| approval | no Agent resource held | frozen deadline/decision reconstructed | timeout/veto/cancel restart matrix |
| execution | no terminal claim | graph/Program terminal fence rejects stale completion | crash at every non-terminal state |
| projection | execution unaffected | replay contiguous cursor or explicit resync | Rust/TS/TUI reducer tests |
| episode | Program already terminal | deterministic episode id prevents duplicates | projector replay/restart |
| candidate/release | current Stable unchanged | exact generation and rollback target recover | candidate/Canary/pointer restart tests |

Partial effects remain governed by existing Tool idempotency/effect receipts;
root Team timeout-auto approval never widens those permissions.

## Performance and capacity feasibility

The plan is feasible without sacrificing concurrency because it reuses existing
bounded kernels:

- semantic compile is CPU/local-registry work, linear in roles and edges plus
  deterministic candidate ordering; valid intent adds no provider round;
- one existing ResourceManager already implements service classes, fairness
  keys, pending bounds and admission observations;
- one existing ApprovalCoordinator already has event-driven waiters, avoiding
  100 ms polling and reducing wakeups under concurrency;
- projection v3 sends complete changed substructures rather than forcing full
  snapshots or client polling;
- experience and pattern projectors run after terminal commit in a background
  class, outside interactive latency;
- advisory lookup is served by a read model and has a 20 ms p95 target;
- all queues, subscriber buffers, episode payloads and caches receive explicit
  bounds and leak/soak tests.

The promise is profile-bound, not hardware-independent. v0.9.706 records a
named hardware/configuration/provider baseline and gates non-provider p95 plus
peak-memory regression at 5%. v0.9.707 gates interactive admission/fairness at
10% under the additional background load and runs a 30-minute saturation soak.
If a threshold fails, the responsible version remains open; the plan does not
lower a threshold after seeing the result without an audited amendment.

## Surface and user-flow audit

No-template behavior is unambiguous:

1. the user asks for any Team and role names, or only an objective;
2. the model supplies semantic duties/requirements/topology for missing fields;
3. the card says **AI composed · turn scoped · not published**;
4. it displays original names, semantic responsibilities, resolved exact
   Definitions, grants, evidence requirements and provenance;
5. no approved match produces a capability gap with allowed next actions, not
   a silent builtin/template substitution;
6. approval UI shows auto receipt or a bounded confirmation/veto countdown;
7. timeout with no user action starts exactly once; explicit veto stops it;
8. Program/Team/role, queue, evidence and terminal changes arrive through live
   deltas without refresh;
9. later advisory reuse is labelled and cannot override current explicit intent.

The parent model receives the same typed diagnostics and Program terminal facts
as the Surface, so conversational compensation cannot invent a different state.

## Test-depth and evidence audit

Completion level for every required capability is level 5: production wired,
durable/recoverable, concurrency/failure proven and operationally observable.

| Capability | Unit/property | Integration/restart | Real provider/browser | Load/fault |
| --- | --- | --- | --- | --- |
| arbitrary roles/no template | yes | yes | yes | shape bounds |
| multi-Team dataflow | DAG/property | Program/Team recovery | 3 Teams/5 roles/handoffs | saturation/cancel |
| approval/autonomy | policy/race | restart at deadline | trust-all/confirm/veto/timeout | waiter/permit leak |
| truthful Surface | reducer/golden | cursor resync | request originates in browser | slow subscriber |
| capability compensation | diagnostics | duplicate/replan fence | real missing predicate scenario | bounded revision |
| governed reuse | signature/threshold/router | projector/candidate restart | three successes through promotion/rollback | background fairness/soak |

Every real run records progress events, last durable state, wait owner, queue age,
outstanding obligation, deadlines, evidence receipts and terminal agreement. A
process exit code or final assistant answer alone is not acceptance evidence.

## Security and privacy audit

- credentials, raw prompts, hidden reasoning and raw Tool output are excluded
  from semantic snapshots and experience signatures;
- user display names remain presentation data and are not reusable identity;
- Session/Turn references in episodes are salted hashes;
- resolver grants remain the intersection of semantic need and authenticated
  ceiling;
- model/Gateway cannot forge trusted ingress, policy snapshots, effective
  grants, release baselines or pointer changes;
- explicit catalog selection validates the exact user-pinned revision and never
  falls back to latest;
- timeout-auto applies only to user-directed Team admission under an authorized
  confirming policy and never approves otherwise forbidden effects;
- reusable content has size/count/privacy classification gates.

## Version, commit, tag and rollback gates

For each of v0.9.705, v0.9.706 and v0.9.707:

1. verify the prior annotated tag, branch relation, clean worktree and baseline
   test evidence in both repositories;
2. implement only the version allowlist and record any amendment before editing
   a newly discovered owner;
3. run focused tests continuously, then full version gates and reverse audit;
4. inspect the complete diff, generated-contract diff and secret scan;
5. write the evidence document with commands, results and artifact hashes;
6. bump version surfaces, commit core and edge independently, annotate matching
   tags and verify tag commit/tree identities;
7. roll back by the exact previous tag/pointer if a release gate fails; never
   hide failure by editing a Stable pointer or compatibility writer;
8. do not push without separate explicit authorization.

## Ambiguity and omission checklist

| Question | Audit answer |
| --- | --- |
| Who turns semantics into mechanics? | Runtime `IntentCompiler`, exactly once. |
| Who chooses Agent/Skill/Tool binding? | deterministic resolver plus existing binding compiler and authorization ceiling. |
| What happens without a template? | turn-scoped AI-composed snapshot; no publication or fake match. |
| Can future roles be unseen? | yes; mechanics use typed semantics, never display-name enums. |
| Who schedules and limits work? | existing ResourceManager under one frozen profile. |
| Is user Team approval blocking? | not in full-auto; in confirming modes it is a veto window with timeout-auto. |
| Can timeout bypass a denial/effect approval? | no. |
| What is execution truth? | durable Program/graph terminal and typed evidence. |
| How does UI stay current? | projection v3 complete deltas plus cursor resync. |
| What becomes reusable? | privacy-safe episodes and advisory patterns first. |
| When is reuse executable? | only through owner-specific evaluated/versioned release pointers. |
| Can Memory L4 publish a Team/Skill? | no. |
| How are new assets evaluated without a prior revision? | immutable qualified episode-set baseline. |
| Are real browser/provider tests mandatory? | yes, with stage-by-stage monitoring. |
| Are performance and concurrent load covered? | yes, named profiles, saturation, fairness, leak and soak gates. |
| Is implementation authorized now? | no; explicit user confirmation remains the final gate. |

## Final audit decision

No contradiction, missing state owner, unbounded wait, silent fallback, hidden
template authority or deferred terminal requirement remains in the plan. All
implementation choices needed to begin v0.9.705 are fixed. The plan is therefore
**conditionally approved for immersive implementation after the user confirms
the complete three-version package**.

If the user changes any locked boundary—especially model/runtime semantics,
timeout-auto scope, capacity ownership, promotion authority or the three-version
cut—the affected contracts and this audit must be revised before implementation.
