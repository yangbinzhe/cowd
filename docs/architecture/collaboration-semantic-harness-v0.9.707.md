# v0.9.707 — Governed Experience Reuse and Terminal Acceptance

## Contract status

This is the third and final subordinate implementation contract under
`collaboration-program-hardening.md`. It may start only after both repositories
are clean and carry the accepted `v0.9.706` tag. It closes P10 and the remaining
P11 integrated-acceptance work.

Implementation is forbidden until the user accepts the audited three-version
plan. A successful v0.9.707 release closes the semantic harness programme; an
episode, candidate, unit test, provider response, or attractive UI screenshot
alone does not.

## Closed outcome

The release is complete only when all of the following are simultaneously true:

1. every terminal `CollaborationProgram` emits one immutable, idempotent
   `CollaborationExperienceEpisode` derived from durable execution truth;
2. experience signatures preserve semantic topology, capability requirements,
   evidence quality and outcome, but exclude display names, raw prompts, raw
   chain-of-thought and secrets;
3. repeated successful episodes may produce an advisory semantic pattern, but
   never silently publish or select an Agent, Skill or Team Definition;
4. executable reuse enters the existing owner-specific evaluation, approval,
   Canary, Stable and rollback lifecycle;
5. explicit user intent always outranks a remembered or promoted pattern;
6. WebUI and TUI expose provenance, advisory reuse and governed promotion state
   without presenting Memory as executable authority;
7. fresh-session, real-provider, real-browser, restart, overload, cancellation,
   approval and multi-Team scenarios are observed from admission through the
   final projection and meet the terminal acceptance matrix below.

## Explicit non-goals

- v0.9.707 does not create a second learning engine, scheduler, template store,
  Skill store, approval queue or release pointer.
- It does not train or fine-tune a model from user content.
- It does not persist raw hidden reasoning, raw provider payloads, credentials,
  unrestricted tool output or full prompts as reusable experience.
- It does not make one successful execution a default.
- It does not treat `KnowledgeCandidate`, Memory L4, a model recommendation or
  an evolution evaluation report as permission to execute an asset.
- It does not add role-name, Team-name, language or scenario-string branches to
  production code. Named scenarios and fixed prompts are test fixtures only.

## Current source facts

| Existing source | Fact | Gap closed here |
| --- | --- | --- |
| `crates/runtime/src/evolution/candidate_kind.rs` | Fourteen candidate kinds are advertised, including `SkillPackage` and `TeamTemplate`. | Advertised executable kinds must route to a real owner or be reported as unsupported; names cannot imply a nonexistent promotion path. |
| `crates/runtime/src/evolution/governance.rs` | `EvolutionCandidateSubject` currently admits only exact Agent Definition and Team Template revisions and already has evaluation, Canary, Stable and rollback controls. | Reuse those controls; add a trustworthy baseline for a newly synthesized immutable revision. |
| `crates/runtime/src/skill/governance.rs` | Skill revision activation and rollback have a separate human-governed pointer lifecycle. | Skill promotion stays with this owner and cannot be disguised as a generic evolution subject. |
| `crates/runtime/src/evolution/projector.rs` | Evolution signals are currently projected primarily from failures and repeated tool behavior. | Successful collaboration needs a typed terminal source, not transcript mining. |
| `crates/runtime/src/team/l4_promotion.rs` | `KnowledgeCandidate` promotion governs factual/knowledge memory. | Knowledge may inform context, but cannot become an executable Team/Agent/Skill authority. |
| `crates/harness-contract/src/execution_graph/contract.rs` | `CollaborationProgram` is the durable multi-Team lifecycle and evidence truth. | Terminal Program commit becomes the only episode trigger. |
| `crates/runtime/src/team/result_reducer.rs` | `DeliveryEnvelope` and branch terminals already carry durable Team outcomes. | Episode quality derives from these typed receipts and Program obligations. |

## Canonical experience contracts

The public contract is versioned and stored as a Runtime event payload:

```rust
pub struct CollaborationExperienceEpisode {
    pub schema_version: u16,
    pub episode_id: String,
    pub session_ref_hash: String,
    pub turn_ref_hash: String,
    pub program_id: String,
    pub program_revision: u64,
    pub intent_digest: String,
    pub binding_digest: String,
    pub capacity_profile_digest: String,
    pub approval_policy_digest: String,
    pub semantic_signature: CollaborationSemanticSignature,
    pub outcome: CollaborationExperienceOutcome,
    pub evidence_refs: Vec<EvidenceRef>,
    pub coverage: CollaborationEvidenceCoverage,
    pub latency_ms: u64,
    pub resource_summary: CollaborationResourceSummary,
    pub completed_at_ms: u64,
}

pub struct CollaborationSemanticSignature {
    pub workstream_shapes: Vec<SemanticWorkstreamShape>,
    pub dependency_shapes: Vec<SemanticDependencyShape>,
    pub required_capability_ids: Vec<String>,
    pub required_skill_ids: Vec<String>,
    pub required_tool_capabilities: Vec<String>,
    pub acceptance_kinds: Vec<String>,
    pub result_field_shapes: Vec<String>,
}

pub enum CollaborationExperienceOutcome {
    Completed,
    IntentGap,
    BindingGap,
    Denied,
    Cancelled,
    Partial,
    Failed,
}
```

`episode_id` is deterministically derived from Program id, terminal revision and
episode schema version. Replaying a terminal event therefore produces the same
event identity and no duplicate episode.

The signature normalizer must:

- sort set-valued identifiers and use stable producer-to-consumer edge order;
- preserve multiplicity ranges, typed acceptance and required result shapes;
- retain registry identifiers but not effective grants or runtime leases;
- replace Team/role display names with stable within-signature ordinals;
- hash Session and Turn identities with a Runtime-held salt;
- omit objectives, prompts and reports except for bounded approved semantic
  labels that are explicitly classified safe;
- record the normalizer revision in the signature digest.

## Episode eligibility and pattern aggregation

All terminal outcomes are auditable episodes. Only episodes satisfying every
condition below are eligible to support a reusable pattern:

1. outcome is `Completed`;
2. every required Program and Team obligation is satisfied;
3. delivery and presentation projections agree with the committed Program;
4. no unresolved approval, cancellation, stale lease or missing evidence exists;
5. evidence coverage meets the policy floor;
6. no secret, personal-data or unbounded-content classifier blocks reuse;
7. the episode was not produced by an explicitly non-reusable Session policy.

A pattern needs at least three eligible episodes from three distinct Turn ids.
The policy may require more; it may never require fewer. Duplicate retries,
replayed events and repeated projections of one Program count once.

```rust
pub struct CollaborationSemanticPattern {
    pub pattern_id: String,
    pub pattern_revision: u64,
    pub signature_digest: String,
    pub source_episode_ids: Vec<String>,
    pub semantic_suggestion: SemanticCollaborationSuggestion,
    pub evidence_summary: PatternEvidenceSummary,
    pub lifecycle: SemanticPatternLifecycle,
    pub created_at_ms: u64,
}

pub enum SemanticPatternLifecycle {
    Advisory,
    CandidateCreated,
    Superseded,
    Ineligible,
    Withdrawn,
}
```

Pattern aggregation is an idempotent projector over terminal episode events.
It cannot update a Definition pointer, compile a Team or submit graph work.

## Retrieval precedence and compensation

The effective semantic decision uses this fixed precedence:

```text
explicit current user fields
  > current Turn's validated model semantic intent
  > Session/Runtime authorization and capacity policy
  > explicitly selected promoted Definition revisions
  > advisory semantic pattern suggestions
  > generic model inference
```

An advisory pattern may propose missing capability requirements, role count
ranges, dependency shapes or acceptance kinds. It cannot replace an explicit
name, remove a user requirement, widen permissions, change approval behavior,
select a catalog template, or suppress a compiler diagnostic.

The compiler reports whether each accepted suggestion came from the current
intent, an explicitly selected Definition, or an advisory pattern. If a pattern
conflicts with current intent, Runtime discards only the conflicting suggestion,
records the reason and continues with the current intent. It never enters a
hidden retry loop.

## Governed promotion routes

| Proposed reusable asset | Construction owner | Evaluation/release owner | Activation rule |
| --- | --- | --- | --- |
| Team Definition revision | Team Definition registry builds an immutable Draft from an advisory pattern. | Existing `EvolutionGovernanceService` with exact Team subject. | Paired evaluation, approved Canary, observation floor, approved Stable pointer. |
| Agent Definition revision | Agent Definition registry builds an immutable Draft. | Existing `EvolutionGovernanceService` with exact Agent subject. | Same evaluation/Canary/Stable fences; exact revision remains rollback target. |
| Skill package revision | Skill maintenance/packaging builds and fingerprints the immutable package. | Existing `SkillRevisionGovernanceService`. | Eligible maintenance Draft plus explicit human activation review; no generic evolution pointer. |
| Knowledge fact | Knowledge candidate projector. | Existing L4 knowledge promotion policy. | May enter Memory context only; never an executable Definition or Skill. |
| Unsupported advertised kind | Evolution candidate router. | No synthetic adapter. | Return a typed `promotion_adapter_unavailable` and expose it in Audit; do not claim support. |

The proposal router must be exhaustive over `EvolutionCandidateKind`. A kind
either has a tested owner adapter or produces the typed unsupported result.
The string returned by `promotion_adapter()` is descriptive metadata, not proof
that the adapter exists.

## Evaluation baseline for new revisions

The current `baseline_revision: u64` cannot honestly compare a first synthesized
Team or Agent revision with a nonexistent previous revision. Replace the trusted
registration field with a versioned baseline contract:

```rust
pub enum EvolutionEvaluationBaseline {
    PublishedRevision {
        subject_ref: String,
        revision: u64,
        content_digest: String,
    },
    EpisodeSet {
        semantic_signature_digest: String,
        episode_ids: Vec<String>,
        aggregate_digest: String,
    },
}
```

Compatibility reads of old candidates map their exact `baseline_revision` to
`PublishedRevision` after Runtime resolves and verifies the immutable content.
New candidates use `EpisodeSet` only when no published baseline exists. The
scenario bundle compares the candidate against the frozen episode-set outcomes
under identical semantic tasks and policy floors. A candidate cannot supply its
own baseline, scenario digest or evaluation floor.

This schema migration is complete only when recovery tests load both shapes and
new writers emit only the versioned baseline.

## State, writer and recovery map

| State | Canonical owner/writer | Readers | Recovery/idempotency |
| --- | --- | --- | --- |
| Program terminal | Graph commit + collaboration coordinator | episode projector, projections | revision-fenced; one terminal wins |
| Experience episode | Runtime event store + episode projector | pattern aggregator, Audit API | deterministic id; duplicate terminal delivery is a no-op |
| Semantic pattern | pattern event stream | intent advisory resolver, Audit API | rebuilt from episodes; revision CAS prevents double aggregation |
| Definition candidate | owning registry + evolution governance | evaluator, release resolver, Audit | immutable digest and exact baseline; no mutable Draft execution |
| Skill candidate | Skill maintenance/governance | Skill activation and Audit | package fingerprint plus approved pointer generation |
| Knowledge candidate | L4 promotion owner | Memory recall | remains non-executable and independently governed |
| Canary/Stable pointer | existing release owner | binding resolver | generation CAS, exact rollback target, restart projection |

## Concurrency, resources and backpressure

- Episode creation runs after terminal commit through the durable event/outbox
  path; it never extends the Program mutation transaction with model or Tool I/O.
- Pattern aggregation is partitioned by semantic-signature digest and fenced by
  stream revision. Concurrent episodes cannot create duplicate pattern revisions.
- Evaluation and Canary work enters the v0.9.706 `ExecutionCapacityProfile` and
  uses a background service class. It cannot starve interactive Session Turns.
- Episode/pattern payloads have explicit byte, identifier-count and evidence-ref
  limits. Oversize reusable material is rejected from reuse while the terminal
  execution remains successful and auditable.
- Audit subscribers have bounded buffers and replay from durable cursors.
- Retention may compact derived pattern projections only after source event and
  release evidence retention obligations are met.

Required metrics are episode projection lag, eligible/ineligible counts, pattern
cardinality, aggregation conflicts, advisory hit/accept/reject rates, candidate
evaluation latency, Canary health, rollback count, payload bytes and Audit lag.

## Failure and rollback behavior

| Failure | Required behavior |
| --- | --- |
| Terminal event delivered twice | Same episode id; no duplicate support count. |
| Episode projector crashes | Restart from durable cursor; Program result remains terminal and visible. |
| Secret/privacy classifier blocks reuse | Store only the auditable ineligibility reason and safe refs; do not store prohibited content. |
| Pattern conflicts with current intent | Ignore conflicting advisory fields and expose the reason; current intent proceeds. |
| Candidate fails evaluation | Mark exact candidate ineligible; advisory pattern and current Stable pointer are unchanged. |
| Canary violates floor | Stop assignment and roll back the exact pointer generation. |
| Skill promotion lacks human review | Remain inert; never route it through Team/Agent approval semantics. |
| Unsupported candidate kind | Typed unavailable result; no fabricated adapter or partial release. |
| Process restarts during promotion | Recover candidate, review, pointer and observation states from durable events; do not repeat external effects. |
| Old baseline payload is corrupt/unresolvable | Quarantine candidate and keep current Stable pointer; fail closed. |

## Exact source allowlist

### `cowd-0821-terminal`

- `crates/harness-contract/src/evolution.rs`
- `crates/harness-contract/src/execution_graph/contract.rs`
- `crates/harness-contract/src/agent/definition.rs`
- `crates/harness-contract/src/team/definition.rs`
- `crates/harness-contract/src/skill/mod.rs`
- `crates/runtime/src/evolution/{candidate_kind,governance,projector,mod}.rs`
- `crates/runtime/src/orchestration/{collaboration_coordinator,mod}.rs`
- `crates/runtime/src/agent/definition_registry.rs`
- `crates/runtime/src/team/{definition,instantiation,l4_promotion}.rs`
- `crates/runtime/src/skill/{maintenance,governance,mod}.rs`
- Runtime event/outbox and recovery files required to register the new projector
- `crates/runtime/src/projection/*` and `crates/harness-contract/src/projection/*`
- `crates/gateway/src/api_routes/{runtime_routes,evolution_routes,audit_routes}.rs`
- `crates/gateway/src/services/{evolution_service,growth_projection_lane,runtime_event_service}.rs`
- `crates/gateway/src/runtime/runtime_bootstrap.rs` and generated-contract inputs
- `crates/harness-eval/src/{runner,live_scenario_runner}.rs`
- focused unit, integration, restart, load and fault tests
- `docs/evidence/collaboration-semantic-harness-v0.9.707.md`

Changes outside this list require an amendment in the evidence file before the
change. The amendment must identify the missed caller/state and prove that it
does not create a new owner.

### `cowd-edge`

- `surfaces/webui/src/adapters/graph/evolution.ts`
- `surfaces/webui/src/adapters/executionProjection.ts` and focused tests
- `surfaces/webui/src/components/runtime/CollaborationProgramSummary.vue`
- `surfaces/webui/src/pages/AuditPage.vue`
- generated Runtime/Gateway API and projection metadata
- unit/component tests, `surfaces/webui/reference-app.live.e2e.spec.js`,
  `surfaces/webui/webui-next.e2e.spec.js` and `surfaces/webui/e2e-release-contract.js`
- build metadata needed for version `0.9.707`

The UI may request candidate creation or an authorized review action through
existing APIs. It cannot compute eligibility, change a release pointer or treat
an advisory pattern as active.

## Deletion and reconnection table

| Retired/incorrect path | Reconnection | Delete proof |
| --- | --- | --- |
| Success reuse inferred from assistant text or fixture phrases | Program terminal event -> episode projector | production scan for transcript/prompt parsing in experience writer |
| Display names in reusable identity | normalized semantic signature ordinals | generative renaming tests produce equal signatures |
| Direct turn-scoped template publication | advisory pattern -> explicit candidate -> owner governance | scans and negative tests prove no terminal event moves a Definition pointer |
| L4 knowledge as Team/Skill authority | keep L4 only in contextual recall | type/API scan and activation negative test |
| Advertised but nonexistent promotion adapter | exhaustive router returns real owner or typed unavailable | exhaustive enum test and Audit rendering |
| Raw `baseline_revision` for every new candidate | versioned `EvolutionEvaluationBaseline` | legacy recovery fixture and scan for new-writer scalar baseline |

## Test and evidence matrix

### Deterministic and property gates

- episode idempotency across replay, restart and duplicate terminal delivery;
- semantic-signature equality under arbitrary multilingual Team/role renaming;
- signature inequality when capability, dataflow, acceptance or result semantics
  materially differ;
- secret/raw-prompt/hidden-reasoning exclusion and payload-bound tests;
- three-distinct-Turn threshold, duplicate retry exclusion and failed-run
  ineligibility;
- advisory precedence/conflict tests;
- exhaustive promotion routing, first-revision baseline, legacy baseline recovery,
  paired evaluation, Canary, Stable, rollback and pointer-CAS tests;
- absence of direct executable authority in pattern and Knowledge contracts.

### Fresh real scenarios

Every scenario starts in a new Session and records request, Program id, graph
revision/cursor, model/tool events, Team/role transitions, approval and capacity
receipts, delivery envelope, terminal projection and browser-visible result.

1. one arbitrary user-named Team with unseen multilingual role names and no
   template;
2. three Teams, at least five roles, fan-out, cross-Team handoffs and a final
   reducer;
3. two collaborating Teams created from a vague goal, proving typed compensation
   without guessing executable identities;
4. explicit exact catalog selection, proving user precedence and no advisory
   substitution;
5. trust-all mode with zero blocking approval and an audit grant receipt;
6. confirmable mode with positive confirmation, explicit veto, and no-response
   timeout-auto execution;
7. capability/Skill gap followed by one bounded semantic replan;
8. provider failure, Tool failure, cancellation and process restart at each
   non-terminal Program state;
9. capacity saturation with multiple Sessions, proving bounded queues, fairness,
   no lock-held await and interactive priority;
10. three qualifying similar successes followed by advisory retrieval, explicit
    candidate creation, evaluation, Canary, Stable selection and rollback;
11. a superficially similar but semantically different task, proving that the
    pattern is not misapplied;
12. WebUI and TUI reconnect from stale cursors and converge on the same terminal
    Program, experience and governance state.

At least the arbitrary-name, three-Team, approval matrix and governed-reuse
scenarios run against the configured real provider and real Gateway. Browser
automation sends the user request through the production UI; direct API fixture
injection cannot substitute for those gates.

### Observation rule

The harness must monitor progress rather than merely wait for a process exit.
For each run it samples or subscribes to:

```text
Turn accepted
  -> semantic decision received/compensated
  -> compiler diagnostics and immutable bindings
  -> approval resolution
  -> capacity queue/admission
  -> Program and Team transitions
  -> Agent/Tool evidence receipts
  -> delivery and terminal commit
  -> contiguous Gateway delta
  -> WebUI/TUI rendering
  -> episode/pattern/candidate events when applicable
```

A timeout report must name the last durable state, current wait owner, queue age,
deadline, outstanding obligation and recovery action. “Timed out” or “model did
not finish” without these facts fails the gate.

### Performance gates

Measured on a named hardware/provider/configuration profile:

- no measurable extra provider round for a valid semantic decision;
- p95 episode projection lag at or below 250 ms under the baseline load profile;
- p95 advisory lookup at or below 20 ms from the read model;
- pattern aggregation remains outside the interactive critical path;
- interactive admission p95 and fairness do not regress more than 10% from the
  v0.9.706 recorded baseline at equal load;
- queues, caches, subscriber buffers and episode payloads remain within their
  declared bounds during a 30-minute saturation/reconnect soak;
- no leaked permits, waiters, tasks, file handles or monotonically growing
  in-memory maps after cancellation/restart cycles.

Thresholds must be recorded with raw artifacts and environment identity. A
faster unit fixture is not evidence for a real-provider or browser gate.

## Final reverse audit

The programme is closed only if both chains are reproducible:

```text
browser-visible terminal result and reuse provenance
  -> projection revision/cursor
  -> Program terminal + delivery/evidence receipts
  -> exact bindings + semantic intent provenance
  -> authenticated Session Turn + policy/capacity snapshots
```

```text
promoted executable revision/pointer
  -> approved Stable/Skill review
  -> Canary/evaluation evidence and immutable baseline
  -> candidate artifact digest
  -> advisory pattern revision
  -> three or more distinct eligible episodes
  -> durable Program terminals and original evidence refs
```

Any missing reverse link, UI inference from prose, unresolved compatibility
writer, unbounded queue, direct turn-success publication or hidden role-name
branch keeps v0.9.707 open.

## Version close

After all evidence is complete:

1. run full Rust formatting, workspace checks, focused/all relevant tests,
   source scans, WebUI unit/component tests, production build and Playwright;
2. record real-provider/browser, restart, fault and performance artifacts in
   `docs/evidence/collaboration-semantic-harness-v0.9.707.md`;
3. bump both repositories to `0.9.707` where their own version surfaces require;
4. commit the core and WebUI repositories independently from clean allowlists;
5. annotate `v0.9.707` in both repositories and record commit/tree/tag ids;
6. run the global reverse audit and verify both worktrees are clean;
7. do not push either repository without explicit user authorization.
