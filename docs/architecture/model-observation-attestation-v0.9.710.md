# Provider model-observation attestation hardening (v0.9.710)

Status: implementation, deterministic verification and immutable
`qwen3.8-max` release verification complete.

This amendment closes the remaining evidence-truth defect discovered by the
real `qwen3.8-max` six-Team evaluation. It is subordinate to
`collaboration-program-hardening.md` and
`high-quality-collaboration-terminal-v0.9.710.md`; where the earlier
v0.9.710 evidence document describes a terminal `ContextTurnReport` join as
the final repair, this document supersedes that repair before release.

## 1. Outcome and non-negotiable invariant

An exact semantic obligation is satisfied only when one Runtime-owned chain
proves all of the following for the same Provider tool invocation:

1. the Provider requested the tool call;
2. ToolHost executed or safely replayed it under the frozen Agent lease;
3. ToolHost produced typed evidence matching the exact obligation;
4. Conversation generated a non-omitting model receipt for that invocation;
5. the exact receipt was present, byte-identical, in a concrete subsequent
   Provider request;
6. that Provider request produced a valid response which Runtime committed to
   the same turn;
7. the resulting attestation was attached to the evidence before the single
   `AcceptanceEvaluator` minted the Agent verdict.

Acquisition without steps 4-6 remains valid ToolHost audit evidence but is not
semantic model observation. A request projection, hash coincidence, matching
byte count, role name, source path count, or final prose can never substitute
for the chain.

## 2. Why the current candidates are rejected

The two post-`b7e2c6d4` candidates correctly stopped the earlier false pass but
used invalid substitutes for the missing identity:

- cross-joining ToolHost and Conversation artifacts by hash/bytes conflated
  independently owned evidence namespaces;
- joining terminal `ToolObservation` and `EvidenceAuditProjection` remained a
  presentation/projection join rather than an execution-to-wire join;
- the current uncommitted draft writes Conversation delivery facts backward
  into `ToolExecutor`, reversing ownership;
- counting exact observations against all successful `read_file` deliveries
  lets an unrelated read mask a missing exact call, treats one `read_many`
  invocation with several observations incorrectly, and has no exact mapping
  for identical-content calls;
- recording when a model receipt is created proves construction, not that the
  receipt survived request preflight/compaction and entered the next Provider
  request;
- an Agent-wide exact-delivery flag expands every tool result, wasting context
  and reducing useful concurrency even when only one invocation owns an exact
  obligation.

These are one root defect: the framework has no canonical invocation-level
semantic-delivery contract across ToolHost, Conversation and Provider.

## 3. Canonical forward and reverse chain

```text
Provider ToolUse(provider_invocation_id)
  -> invocation-aware ToolExecutor entry
  -> ScopedToolExecutionReceipt(provider_invocation_id, observations)
  -> ToolModelDeliveryRequirement::Exact(obligation_ids)
  -> Conversation ModelReceipt(provider_invocation_id, body digest, omission)
  -> actual packed ApiRequest contains matching ToolResult id + body digest
  -> valid Provider response committed
  -> ProviderModelObservationAttestation
  -> exact ObservedEvidence + attestation
  -> AcceptanceEvaluator revision 2
  -> AgentReturnPacket / graph terminal
  -> Team reducer / Program outcome / evaluator

Agent exact acceptance
  -> exact ObservedEvidence
  -> model-observation attestation
  -> committed Provider request sequence + attempt + model-receipt digest
  -> generated receipt for the same provider invocation
  -> scoped ToolHost receipt for the same provider invocation
  -> typed obligation and frozen Agent lease
```

## 4. State and single ownership

| State | Single owner | Lifetime / durability | Rule |
| --- | --- | --- | --- |
| Provider invocation identity | Conversation | turn, then terminal evidence | Passed into ToolExecutor; never regenerated from content |
| Effect/acquisition evidence | ToolHost / scoped executor | durable effect receipt | Proves execution/replay and typed observed target only |
| Exact delivery requirement | frozen Agent acceptance + scoped executor | Agent attempt | Read-only per-invocation policy query; never an Agent-wide semantic verdict |
| Generated model receipt | Conversation | current turn metadata | Body digest and omission facts; construction is not delivery |
| Packed request membership | Conversation request boundary | Provider attempt | Derived from the actual immutable `ApiRequest.messages` |
| Valid Provider consumption | Conversation | current turn, promoted on valid response commit | Failed/protocol-invalid attempts do not promote delivery |
| Semantic observation attestation | Conversation, attached by Agent terminal producer | durable inside Agent terminal | Only bridge from acquisition to semantic acceptance |
| Acceptance verdict | `AcceptanceEvaluator` | durable digest/revision | Exact matching requires the attestation; callers cannot reimplement it |
| External evaluation | harness-eval | report only | Reads canonical Agent acceptance; never mints or repairs proof |

`ContextTurnReport` remains a durable diagnostic projection. It is not an
acceptance input. `ToolExecutor` never stores Conversation-owned Provider
delivery state.

## 5. Contract changes

### 5.1 Invocation-aware execution

Add object-safe invocation-aware ToolExecutor entry points with compatibility
defaults delegating to the existing methods. Conversation always calls the new
entry points for Provider ToolUse work. `ScopedRuntimeToolExecutor` carries the
Provider invocation id into its transient scoped receipt; its independent
durable effect/idempotency identity remains unchanged.

This separates:

- Provider invocation identity: correlation across model request/response;
- ToolHost effect identity: idempotency and replay fencing;
- raw evidence identity: content-addressed Conversation storage;
- semantic evidence identity: ToolHost-attested target and digest.

No pair is joined by incidental equality.

### 5.2 Per-invocation exact delivery policy

Add a read-only `ToolExecutor::model_delivery_requirement(tool_name, input)`
query. Its default is bounded. The scoped executor resolves the normalized
request against exact Provider-model observation obligations frozen in that
Agent packet and returns sorted matching obligation ids. This is a policy fact
owned by the execution binding, not a claim that delivery occurred.

Keeping the requirement separate from `ToolOutputDraft` is necessary for safe
effect replay: the outer Conversation effect fence currently stores output
text, not draft variants. A replayed output must receive the same policy from
the frozen packet without fabricating a new ToolHost execution.

Conversation applies the expanded exact budget only to a successful invocation
whose requirement is exact. Ordinary, unrelated and error outputs stay
bounded. Provider context preflight remains authoritative: if the complete
receipt cannot fit, the attempt fails explicitly. No compaction or
staged-artifact summary may silently satisfy exact semantics.

### 5.3 Model-observation attestation

Add an explicit observation authority to `EvidenceObligation`:
`RuntimeAcquisition` (the backward-compatible default) or `ProviderModel`.
Agent-packet compilation marks exact semantic reads and upstream verification
reads as `ProviderModel`; deterministic effect verification can remain
`RuntimeAcquisition`. This prevents a global exact-content rule from breaking
mechanical Runtime verification while making the semantic boundary explicit.

Add a typed optional attestation to `ObservedEvidence` containing at least:

- Provider invocation id;
- covered obligation ids;
- Conversation raw evidence ref;
- exact model-receipt SHA-256;
- raw/receipt/omitted token counts and completion flag;
- Provider request sequence and attempt;
- effective model.

The field is optional for backward decoding and non-exact evidence. New
obligations whose authority is `ProviderModel` require a complete attestation
under evaluator revision 2. Exact Runtime-only obligations retain their
existing typed ToolHost semantics.

### 5.4 Actual-request confirmation

Conversation keeps only bounded metadata for generated exact receipts. Before
moving a concrete `ApiRequest` into the client, it scans the immutable history
for matching ToolResult invocation ids and body digests. Candidates are
promoted only after the returned Provider step passes transport, protocol and
transcript commit validation. A failed attempt, invalid tool frame, preflight
compaction that removed the body, or a generated-but-never-dispatched receipt
cannot produce an attestation.

Production Provider wire artifacts remain the independent audit copy of the
exact protocol body. The semantic decision does not parse that artifact or
block on a second storage read.

### 5.5 Per-receipt acceptance join

At the Agent terminal, each scoped execution receipt is evaluated once:

- non-exact observations retain existing behavior;
- a receipt containing one or more exact observations requires one confirmed
  delivery attestation with the same Provider invocation id and all relevant
  obligation ids;
- one `read_many` delivery may therefore attest several exact observations;
- two calls over identical bytes remain distinct because invocation ids differ;
- an unrelated non-exact read cannot mask a missing exact delivery;
- exact observations are cloned with the attestation before entering the
  canonical acceptance snapshot.

## 6. Concurrency, recovery and resource rules

- Parallel ToolUse items write generated-receipt metadata under one short
  turn-local mutex; no Provider/tool/storage await occurs while it is held.
- Delivery confirmation runs after the tool batch and before/after one Provider
  attempt; it is linear in request history plus the bounded number of pending
  exact invocations, with no quadratic content comparison.
- Duplicate invocation id with a different body digest is framework-invalid.
  An identical replay is idempotent. A later failed/compacted duplicate cannot
  erase an already confirmed delivery.
- Crash after ToolHost commit but before Provider confirmation retains
  acquisition only. Recovery must actually redeliver the exact body and obtain
  a valid response, otherwise the obligation stays unresolved.
- Replayed ToolHost output under a fresh Provider invocation can be attested
  normally after redelivery; effect replay never fabricates model observation.
- Provider fallback/retry records the attempt and effective model that actually
  returned the valid continuation. Failed candidate attempts do not count.
- Metadata is bounded; complete bytes remain in existing Session/artifact
  storage and Provider history. No second body cache or scheduler is added.

## 7. Compatibility and migration

- Existing ToolExecutor implementations compile through default invocation
  methods; only the production scoped executor opts into correlation.
- Existing `ToolOutputDraft` serialization is unchanged. The new delivery
  policy query defaults to bounded for compatibility executors.
- `ObservedEvidence.model_observation` uses serde default/skip-empty. Historical
  terminal outcomes remain replayable and are never retroactively rejudged.
- A new attempt evaluated under revision 2 cannot reuse a legacy exact
  acquisition as semantic observation without real redelivery.
- External/process Agent adapters must clear model-observation claims; they
  cannot self-attest Provider delivery.
- Runtime and evaluator consume the same canonical Agent terminal. Harness
  fixtures gain attestations where they claim exact semantic coverage.

## 8. Implementation phases and write allowlist

| Phase | Change cone | Exit gate |
| --- | --- | --- |
| P0 | This document and current diff classification | Plan and audit approved; no business implementation claimed |
| P1 | `harness-contract/src/context/mod.rs`; Runtime context/evidence types | Additive observation-authority and attestation schemas; round-trip and legacy decode tests |
| P2 | `conversation/conversation.rs`, `context/ledger/mod.rs`, request compiler/provider conversion tests | Per-invocation ledger expansion inside the existing subsystem ceiling; generated -> packed -> valid-response confirmation; failed/compacted/protocol-invalid negatives |
| P3 | `agent/in_process_worker.rs`, `context/acceptance_evaluator.rs`, `context/path_identity.rs`, process adapter | Invocation propagation, per-receipt join, evaluator rev 2, external-claim stripping |
| P4 | harness-eval large-scale source gates and fixtures | Evaluator requires attested exact Agent observations; no raw/projection fallback |
| P5 | architecture/evidence docs | Supersede rejected joins and record final ownership/recovery truth |
| P6 | unified verification only | Static/full/fault/real-provider gates; no design changes during this phase |

Any additional production file requires an explicit allowlist amendment before
editing. The current uncommitted reverse-callback draft must be removed during
P2/P3; it cannot survive as a compatibility path.

## 9. Mandatory adversarial acceptance matrix

| Case | Required result |
| --- | --- |
| Complete exact call, present in valid next Provider request | accepted with attestation |
| Receipt generated but never packed | unresolved |
| Packed request whose Provider attempt fails | unresolved |
| Packed request with protocol-invalid response, then no valid retry | unresolved |
| Preflight compaction replaces/removes exact body | unresolved |
| Model receipt has any omitted tokens | unresolved |
| ToolHost exact acquisition exists with no delivery | unresolved |
| Unrelated successful read plus missing exact read delivery | unresolved |
| One `read_many` call covers several exact obligations | all covered observations accepted |
| Two distinct calls return identical bytes | both correlated independently |
| Duplicate invocation id changes body digest | framework-invalid |
| Crash after effect commit before delivery | acquisition retained, semantic acceptance unresolved |
| Recovered effect is actually redelivered on a new call | accepted after valid response |
| Parallel exact calls return out of order | identity-correct, race-free acceptance |
| External adapter submits a forged attestation | stripped/rejected |
| Evaluator sees raw exact receipts outside Agent acceptance | rejected |
| Terminal prose claims coverage but typed proof is absent | rejected |

## 10. Release gates

The implementation is not complete until all of these pass in one unchanged
candidate:

1. format, diff, workspace check, architecture and governance gates;
2. full workspace all-target regression;
3. the adversarial matrix above, including recovery and parallelism;
4. managed-Agent compatibility and ordinary bounded-tool budget regression;
5. real isolated `qwen3.8-max` six-Team/twelve-Agent/five-edge execution;
6. 24 independently attested exact observations, twelve paths each from two
   distinct Agent execution identities;
7. complete E/F semantic handoffs and a contradiction-free final answer;
8. immutable candidate commit and binary SHA bound into evidence;
9. annotated `v0.9.710`, identical `master`/`dev`, both remotes verified, and
   clean worktrees.

No character/token target may truncate canonical semantics. Provider capacity
remains finite and explicit: work that cannot fit is repartitioned or fails
closed, never reported as complete.

## 11. Pre-implementation design audit (2026-08-29)

Audit status: **approved after amendments below**. Business-source execution
may start only from this amended design; the earlier uncommitted callback/count
draft is not approved.

### 11.1 Whole-framework boundary audit

| Plane | Current authority check | Audit result |
| --- | --- | --- |
| User/semantic intent | frozen semantic intent and typed `review_of`; display names do not mint authority | no new defect in this repair cone |
| Compilation/binding | exact Agent revisions, typed behavior, scoped obligations and one capacity profile | retain; add explicit observation authority while freezing the packet |
| Scheduling/concurrency | ExecutionGraph and ResourceManager remain the only scheduling/admission owners | retain; proposed metadata ledger creates no queue or semaphore |
| Tool acquisition/effects | ToolHost and effect receipts own execution/idempotency | retain; never use their content hash as Provider delivery identity |
| Provider context | Conversation builds request; provider adapter persists exact wire body | missing canonical semantic-delivery attestation; this plan closes it |
| Agent acceptance | single `AcceptanceEvaluator` owns the verdict | retain and bump semantics revision; no worker-local verdict |
| Team/Program delivery | lossless Agent/Team results and typed delivery envelopes | retain; consume only attested Agent acceptance |
| Surface/evaluator | projections and harness are read-only consumers | retain; remove every raw-receipt/projection fallback from the exact gate |
| Recovery/evolution | graph terminal is durable; episodes do not become executable truth | retain; unconfirmed acquisition cannot become learned success |

The repository architecture boundary script passes at the pre-implementation
baseline. No new reverse dependency, lifecycle registry, scheduler, cache of
canonical bytes, or role-specific branch is needed.

### 11.2 Rejected alternatives and required amendments

| Finding | Rejected design | Why it fails | Approved correction |
| --- | --- | --- | --- |
| A1 ownership | Conversation calls `ToolExecutor::record_model_delivery` | writes Provider truth into the acquisition owner and makes terminal state depend on a reverse callback | Conversation owns generated/confirmed delivery metadata and returns it in `TurnSummary` |
| A2 replay | encode exactness only in `ToolOutputDraft::ExactInline` | the outer effect fence persists text and loses the variant on replay | read-only per-invocation delivery-requirement query from the frozen scoped binding |
| A3 authority | every `ExactContent` observation globally requires a model receipt | deterministic Runtime verification also uses exact content without a model | explicit `EvidenceObservationRequirement`, default Runtime, ProviderModel only where compiled |
| A4 correlation | compare counts, hashes, bytes or raw evidence refs | unrelated calls, identical content and independent namespaces create false matches | join scoped acquisition and confirmed delivery only by Provider invocation id |
| A5 cardinality | require one delivery per `ObservedEvidence` | one `read_many` call legitimately creates several observations | evaluate one scoped execution receipt; one delivery covers its declared obligation ids |
| A6 timing | attest when `ModelReceipt` is created | preflight compaction or a never-dispatched result can still remove it | match the actual packed request and promote only after a valid response is committed |
| A7 recovery | restore model observation from durable ToolHost receipt | acquisition survived, but model context did not | retain acquisition and fail closed until actual redelivery succeeds |
| A8 evaluator | let harness infer observation from timeline receipts | recreates a second verdict and previously allowed false passes | harness consumes only Agent `observed_acceptance` plus its typed attestation |

## 12. Implementation conformance record (2026-08-29)

The implementation was completed against the frozen design without adding a
second scheduler, verdict path, byte cache, reverse Conversation-to-ToolHost
callback, or content-hash correlation fallback.

| Frozen requirement | Implemented authority |
| --- | --- |
| explicit observation authority | `EvidenceObservationRequirement` on each obligation |
| typed semantic delivery proof | `ProviderModelObservationAttestation` on observed evidence |
| invocation-level correlation | Provider invocation id is passed through invocation-aware `ToolExecutor` calls |
| per-invocation exact policy | immutable scoped executor returns exact obligation ids for the matching request only |
| actual packed-request proof | Conversation scans the concrete `ApiRequest` ToolResult membership and digest |
| valid-response promotion | confirmation occurs only after provider success and assistant transcript commit |
| recovery fail-closed | durable acquisition without real redelivery has no provider invocation attestation |
| single verdict | `AcceptanceEvaluator` revision 2 validates ProviderModel attestations |
| external boundary | process adapters clear externally asserted observation truth |
| bounded resources | exact budget expands monotonically only inside the existing subsystem ceiling |

One implementation audit found and closed a same-target deduplication hazard:
the former novelty key could collapse an attested observation into an earlier
unattested duplicate. Revision 2 deduplicates the complete observed-evidence
fingerprint, so evidence authority can never be lost by target-only equality.
This is a conformance correction to the frozen single-verdict design, not a new
acceptance path.

## 13. Deterministic verification record

The unchanged implementation candidate passed:

- `cargo fmt --all -- --check` and `git diff --check`;
- `cargo check --workspace --all-targets`;
- `scripts/architecture/check-boundaries.sh`;
- Runtime library concurrency suite: 1,901 passed, 0 failed, 2 ignored;
- `scripts/test/full-regression.sh`: every workspace target passed, all ten
  isolated Gateway process-global tests passed, and the standalone signed
  reference APP Bundle/proxy end-to-end test passed.

The first full-regression attempt exposed a test-only observation-authority
fixture that still emitted legacy acquisition evidence for a ProviderModel
obligation. The fixture was corrected to use the typed attestation contract;
production remained fail-closed. A later single environment assertion failed
once and then passed in the focused Runtime suite and unchanged full rerun. No
production change was made without a reproducible framework cause.

Real-provider evidence, immutable commit/binary identity, release tag and
branch synchronization remain release gates and are recorded only after the
isolated `qwen3.8-max` execution completes.

## 14. Real-provider failure amendment and re-audit (2026-08-29)

Candidate `c6ef018c1f7382fae91a62da1de3abafa5569892`, release binary
SHA-256 `1c0ab7b3e8a438543750d8dd6398d1be981c1bb5488cc777b8d6cb558b1d40a5`,
was executed unchanged against only `qwen3.8-max` in an isolated Gateway. The
run correctly failed closed and is negative evidence, not a release pass:

- report:
  `/tmp/cowd-qwen38-v0910-attested-final/runs/v0.9.710-1788001968-mission-harness-deep/report.json`;
- isolated Gateway: `/tmp/cowd-real-qwen-gateway.niK82A`;
- group-theory research scenario: failed before Team admission, 2 model rounds,
  2 tool calls, 25,923 tokens;
- six-Team scenario: failed after one bounded replan, 19 model rounds, 66 tool
  calls, 3,240,049 tokens; the first four investigators and their v2
  replacements all failed acceptance while their reviewers were correctly
  blocked;
- Provider wire artifacts contain complete non-omitted ToolResult bodies and
  the model returned source-grounded findings, so this is neither a provider,
  read execution, context-capacity, nor model-quality failure.

### 14.1 Root cause

Delegated exact evidence is intentionally prefetched by Runtime before the
first Provider model node. `runtime-focus-verify-*` calls execute under the
frozen Agent packet and are later represented as a valid assistant ToolUse +
ToolResult pair in the Provider wire request. The generated exact-receipt
ledger was populated correctly.

However, `ConversationRuntime::execute_model_step(..., first_step = true)`
called `clear_turn_tool_observations()` immediately before packing that first
Provider request. `first_step` means "first Provider model node", not "start of
the Runtime turn". It therefore erased every pre-model generated exact receipt.
The subsequent request demonstrably carried the full ToolResults, but there
was no generated-receipt candidate left to promote. Agent terminal evidence
retained acquisition with `model_observation: None`, and evaluator revision 2
correctly left the ProviderModel obligations unresolved.

The differing obligation ids visible in the terminal trace are expected:
ToolHost acquisition evidence has its own evidence identity, while matching to
the required obligation is directional by typed target. They were initially
suspected during triage but are not the cause.

### 14.2 Approved lifecycle correction

The turn ledger must be owned by the turn admission boundary, not by a model
node:

1. rename the reset operation to express a complete turn-observation epoch;
2. invoke it exactly once in
   `submit_owned_conversation_turn_with_ingress`, before any graph planning,
   Runtime prefetch, early tool lane, Provider call or recovery node;
3. remove the reset from `execute_model_step(first_step)`;
4. retain all generated and confirmed receipt metadata across every model and
   tool node in the same turn;
5. a later top-level turn resets all observation, audit and metric ledgers
   before it can execute, so no prior-turn attestation can leak;
6. graph continuation/recovery inside the same turn never resets the epoch;
   process recovery without the in-memory generated ledger remains fail-closed
   until a real redelivery creates a new candidate.

The correlation field called `provider_invocation_id` is the tool invocation
identity carried in the Provider protocol transcript. It may originate from a
Provider ToolUse or from the existing Runtime-authored, contract-bounded exact
prefetch. Runtime-authored prefetch does not pretend that the model selected the
tool: its graph event and reserved invocation namespace retain that origin.
Both origins require the same non-omitted ToolResult membership, valid Provider
response and assistant commit before semantic observation is attested.

### 14.3 Strict amendment audit

Audit result: **approved**. This is a lifecycle-owner correction inside the
already frozen architecture, not a weakened acceptance rule.

| Audit dimension | Result |
| --- | --- |
| authority | turn admission is the earliest boundary that owns both Runtime prefetch and Provider nodes |
| concurrency | one reset occurs before workers exist; no new mutex, await, queue or scheduler is added |
| recovery | same-turn graph retries preserve candidates; process loss still cannot fabricate confirmation |
| isolation | a mandatory two-turn test proves old attestations are removed before new work |
| semantics | complete acquisition alone still fails; only a packed request followed by valid commit promotes |
| compatibility | Provider-driven ToolUse remains unchanged; ordinary bounded tools remain bounded |
| resource use | metadata bounds and exact-context ceilings are unchanged |
| evaluator | revision 2 and the single Agent verdict remain unchanged |

Mandatory new tests before another live run:

- Runtime prefetch before the first model step survives turn initialization and
  is confirmed from the actual packed request;
- first Provider failure does not promote, while a later valid same-turn
  redelivery does;
- a second top-level turn begins with empty generated/confirmed ledgers;
- a Runtime-prefetched exact Agent read reaches a satisfied revision-2
  terminal only with the attestation;
- the same acquisition with no valid Provider continuation remains unresolved.

No release evidence, tag or branch synchronization is allowed from candidate
`c6ef018c`.

### 14.4 Identity and replay audit

- Provider invocation id is the only cross-plane correlation key. Content
  digest remains an integrity check inside one correlated call, never a join.
- Tool effect idempotency keys remain unchanged and independent. A completed
  effect replayed for a new Provider invocation receives a new semantic
  delivery identity without re-executing the effect.
- Same invocation plus same receipt digest is idempotent. Same invocation plus
  a different digest is `FrameworkInvalid`; last-writer-wins is forbidden.
- Identical bytes from two calls remain two calls. A single `read_many` call
  remains one call with several covered obligation ids.
- The attestation is monotonic after valid response commit. Later failed or
  compacted attempts cannot erase it, but cannot widen its obligation set.

### 14.5 Concurrency and lock audit

- Tool execution remains parallel under the existing graph/ToolExecutionPlane.
- Generated metadata writes hold a turn-local mutex for bounded insertion only.
- Packed-request matching uses a local map and immutable HistoryView; no mutex
  is held across Provider dispatch or response streaming.
- Confirmation is a short commit after protocol validation and Session message
  append. Provider/tool/storage awaits do not occur under the metadata lock.
- Complexity is `O(history blocks + pending exact invocations)` per Provider
  attempt. Reverse iteration may stop when every pending id is found; there is
  no body-to-body quadratic comparison.

### 14.6 Failure and recovery audit

| Boundary failure | Acquisition | Model observation | Terminal behavior |
| --- | --- | --- | --- |
| Tool failure/truncated source | absent or non-exact | absent | obligation unresolved |
| Model receipt compaction | retained | absent | obligation unresolved / repartition |
| Provider preflight rejection | retained | absent | explicit context-capacity failure |
| Transport/provider failure | retained | absent for that attempt | bounded retry/fallback; no promotion |
| Protocol-invalid response | retained | absent for that attempt | existing bounded protocol recovery |
| Crash before response commit | durable acquisition may remain | absent | new attempt must redeliver |
| Crash after Agent terminal commit | included in terminal digest | included | replay terminal; do not re-evaluate history |
| Stale/foreign invocation | retained for audit only | join rejected | FrameworkInvalid or unresolved |

### 14.7 Compatibility audit

- Trait compatibility defaults keep lightweight and non-Agent executors
  bounded and invocation-agnostic.
- New contract fields are serde-defaulted; old durable objects decode.
- Historical committed acceptance remains historical truth at its evaluator
  revision. Only a newly evaluated attempt must satisfy revision 2.
- Runtime-only exact verification explicitly retains its former behavior.
- External Agent adapters cannot become trusted merely by serializing the new
  field; adapter sanitization and in-process provenance remain mandatory.
- The real-provider gate binds the final result to a clean candidate commit and
  binary hash, so tests cannot accidentally validate a prior build.

### 14.8 Audit conclusion

The amended plan has one truth owner at every stage, a complete forward and
reverse evidence path, bounded concurrency, explicit recovery semantics and an
additive compatibility boundary. All identified counterexamples have a
deterministic fail-closed result. Implementation is authorized in P1-P5 as one
coherent change set; P6 is verification-only. If P6 exposes a design change
rather than an implementation defect, execution returns to this audit instead
of patching during the test phase.

## 15. Lifecycle correction conformance record (2026-08-29)

Implementation matches the approved amendment without changing the evidence
contract or evaluator:

- `ConversationRuntime::begin_turn_runtime_epoch` is the single reset operation
  for turn observations, audits, generated and confirmed model receipts, tool
  exposure/stable-prefix metrics, governed plans, preflight compaction and the
  turn context ledger;
- `submit_owned_conversation_turn_with_ingress` calls it before evaluation
  parsing, session projection, graph state construction or worker admission;
- `execute_model_step_with_early_dispatch(first_step)` retains only
  first-Provider-node work such as user transcript insertion and Skill
  activation. It no longer owns any turn reset;
- same-turn Provider failure and redelivery keep the candidate ledger, while a
  later top-level turn clears both generated and confirmed attestations.

The lifecycle regression recreates the original ordering with a reserved
`runtime-focus-verify-*` invocation. It proves in one executable chain that:

1. the prefetched complete receipt is present in both packed Provider requests;
2. a failed first response leaves model observations empty;
3. a valid same-turn redelivery commits exactly one complete attestation;
4. the next turn epoch clears generated, confirmed, acquisition and audit
   ledgers.

Deterministic verification passed after the correction:

- focused lifecycle regression: 1 passed;
- Runtime library: 1,902 passed, 0 failed, 2 ignored;
- the one unrelated graph callback timing failure from the first parallel run
  passed five isolated repetitions and the complete rerun; no production
  change was made for a non-reproducible failure;
- `cargo check --workspace --all-targets`: passed without warnings;
- architecture boundary gate: passed;
- full workspace all-target regression, ten isolated Gateway global-environment
  tests, standalone reference Bundle and generic APP proxy: passed.

This corrected candidate is eligible for a new immutable real-provider run.
It is not release-eligible until both required `qwen3.8-max` scenarios pass and
their terminal evidence is audited.

## 16. Cross-Team synthesis contract amendment (2026-08-29)

Status: **implemented; deterministic and immutable live gates passed**.
This amendment was produced only after candidate
`290da57c2319ff4980ec6fb1d707eab904ee311b` completed both isolated live
scenarios. It supersedes any scenario-local workaround.

### 16.1 Immutable live evidence and root cause

The six-Team pressure scenario passed unchanged with 6/6 Teams, 12/12 Agents,
five cross-Team edges, 97 tool calls and only `qwen3.8-max`. This proves the
turn-epoch/provider-observation repair under a much larger execution.

The group-theory scenario failed closed with 3/4 Teams completed. A, B and C
each committed a verified artifact and D received all three complete Team
delivery bundles. D then produced the requested synthesis fields after one
presentation-format recovery, but its Agent terminal was rejected with
`missing_acceptance=[]`, `runtime_change_receipts=0` and
`observed_evidence_count=0`.

The failure is a three-boundary framework defect:

1. **Cross-Team dataflow was not lowered into role behavior.** The semantic
   Program had three workstream `depends_on` edges and D declared all three
   `input_artifacts`, but `derive_behavior` considered only dependencies inside
   D's one-role Team. D therefore lacked the typed `UpstreamConsumption`
   facet even though Runtime later attached the durable upstream results.
2. **Output materialization was conflated with source acquisition.**
   `team_evidence_policy` treated any custom `StructuredArtifact` as requiring
   a new tool execution unless an explicit `UpstreamEvidence` check happened
   to coexist. A synthesis artifact is an output schema, not a source lease.
   Runtime consequently rejected a valid zero-tool reduction of authenticated
   predecessor evidence.
3. **A failed Program terminal was collapsed into a Tool transport error.**
   Gateway discarded `RuntimeOrchestrationResult::model_receipt()` for
   `failed`/`blocked` business terminals and returned only a small error. The
   root goal policy therefore saw no checked evidence and generated the false
   claim that no Team or source receipt existed, despite durable A/B/C
   terminals in the Program projection.

The submitted semantic intent also placed D's three input artifact names in
its role `acceptance`. The compiler accepted them as if D must republish those
inputs. This did not cause the final rejection after format recovery, but it
is a contract-normalization defect that inflated D's output and token cost.

### 16.2 Frozen target model

The framework must keep four concepts orthogonal:

```text
workstream dependency + role input_artifacts
  -> typed upstream-consumer binding
  -> authenticated predecessor evidence attached by Runtime
  -> zero-tool UpstreamEvidence acceptance
  -> role-owned output_artifacts only
  -> Team delivery

Program business terminal (completed | partial | failed | blocked)
  -> successful tool-protocol receipt containing truthful typed projection
Tool execution/transport failure or semantic-plan rejection
  -> ToolError eligible for bounded plan repair
```

The compiler, not role names or Provider prose, owns the first chain. Gateway,
not the model, owns the second distinction.

### 16.3 Approved implementation phases

| Phase | Change | Mandatory proof |
| --- | --- | --- |
| S1 semantic dataflow | Validate every cross-workstream `input_artifacts` item against the declared result artifacts of its `depends_on` workstreams; derive `UpstreamConsumption` for the exact consuming role | missing producer, undeclared dependency and unrelated input all fail before Program admission; valid A/B/C -> D compiles |
| S2 acceptance separation | Remove input-only artifact labels from role output acceptance; for an upstream-only consumer append one Runtime-owned `UpstreamEvidence` requirement; custom structured outputs no longer imply fresh acquisition by themselves | D requires only `final_recommendation` plus typed upstream evidence; source-producing A/B/C still require their scoped Provider-model observations |
| S3 terminal truth | Return typed model receipts for executed `failed`/`blocked` Program business terminals; retain ToolError only for rejection/unavailability/transport failure | a 3-complete/1-failed Program exposes the three verified Team terminals and exact diagnostic; root cannot claim zero Teams/evidence |
| S4 deterministic closure | Run focused compiler, instantiation, Agent admission, Team verifier, Gateway and terminal-recovery tests, then Runtime/full workspace gates | all adversarial cases pass in one unchanged candidate |
| S5 immutable live closure | Commit/build once, record binary hash, rerun group-theory plus six-Team scenarios with only `qwen3.8-max` | 2/2 scenarios pass; no fallback, dangling work, false completion or contradictory terminal |
| S6 release | Update evidence, commit/tag/sync/push/clean | `master == dev == v0.9.710`, both remotes verified, no surplus branch/worktree retained without audit |

### 16.4 Strict architecture audit

Audit result: **approved** with the following invariants. Implementation may
now begin and may not widen this cone during verification.

| Dimension | Audited decision |
| --- | --- |
| authority | Cross-Team behavior is derived only from typed `depends_on` plus exact `input_artifacts`; never from `synthesizer`, display text, responsibility prose or graph position alone |
| input/output | Input artifacts are prerequisites and never become output fields. Output artifacts remain the only Provider-materialized Team result fields |
| evidence | Upstream evidence is accepted only when Runtime attached a durable predecessor reference and the Agent returns the same reference; model prose cannot mint or replace it |
| reacquisition | Explicit `ReacquireEvidence` or independent verification still wins and keeps tools/scoped observations; upstream consumption never downgrades a verifier |
| scheduling | A consumer remains blocked on the existing evidence-ready graph join; no new scheduler, queue, lock or polling loop is introduced |
| concurrency | Independent upstream Teams remain parallel. Dataflow validation is a bounded pre-admission pass over workstreams/artifact names |
| recovery | Invalid semantic contracts fail before admission with a typed diagnostic. Admitted partial Programs preserve completed child truth and exact failed-child diagnostics |
| compatibility | Local intra-Team reducers keep their current behavior. Workstreams with no typed inputs are unchanged. Historical durable results are not reinterpreted |
| security | A model cannot smuggle a foreign artifact through `acceptance`; the producer must be in the declared dependency set and the reference is Runtime-attached |
| resources | Removing duplicate input fields reduces Provider output/context. The bounded partial receipt reuses `model_receipt`; the recursive raw graph remains artifact-only |
| verdict | `AcceptanceEvaluator` remains the single Agent verdict; Gateway transports terminal truth but never changes Team/Program status |

### 16.5 Adversarial acceptance matrix

- cross-workstream input with no `depends_on`: reject before admission;
- input absent from every declared predecessor result: reject before admission;
- input-only artifact repeated in model acceptance: normalize to input
  prerequisite, never require it as an output field;
- pure source-producing custom artifact with a scoped evidence contract:
  require successful exact Provider-observed acquisition;
- pure structured reasoning artifact with no source or upstream contract:
  reject as ungrounded under the Team evidence gate;
- upstream-only synthesis with durable attached results: zero tools, output
  materialized, upstream references retained, accepted;
- upstream-only synthesis missing one required predecessor: never scheduled;
- upstream consumer with explicit independent reacquisition: tools remain
  enabled and fresh evidence remains mandatory;
- completed Program: normal typed success receipt;
- admitted partial/failed Program: typed non-transport receipt preserving all
  completed Team terminals and the failed Team diagnostic;
- decode/compile/rejected/unavailable operation: ToolError and bounded semantic
  repair, never a fabricated Program receipt;
- partial receipt followed by root narration: claims must be a subset of the
  typed child terminals and diagnostics.

Any live failure that requires a new authority, scheduling or acceptance
design returns to this section for re-audit before source changes. Scenario
prompts, role-name heuristics, relaxed pass thresholds and harness-only
exceptions are explicitly forbidden.

### 16.6 Parallel verification amendment: evaluation lease isolation

The unchanged full Runtime regression exposed two different Conversation
tests failing only under parallel execution. In each failure an unrelated
request was charged against the `eval-small` or `eval-rollback` lease installed
by a budget unit test. Both tests passed repeatedly in isolation. Source audit
confirmed that the evaluation lease was a process-wide `OnceLock<Mutex<_>>`,
so every Conversation in the process implicitly consumed whichever lease was
temporarily installed. This is not a test-threshold problem: a multi-session
Gateway could apply one evaluation's paid budget to unrelated foreground work.

The following correction is approved before implementation:

```text
RuntimeServices evaluation-lease registry
  session_id -> Arc<EvaluationProviderTokenLease>
root evaluation Turn installs one guarded entry and binds the same Arc
delegated Agent host resolves the entry by its canonical parent session_id
ConversationRuntime charges only its explicit optional Arc
guard removes only its own session entry after the root Turn terminates
```

Strict audit invariants:

- the lease remains Runtime-owned and the model/provider cannot select it;
- root and delegated provider calls share one atomic counter by `Arc`, so
  moving the scope does not double-count or weaken the hard total;
- different Session IDs may run concurrently without observing or consuming
  one another's lease;
- duplicate installation for the same Session fails closed; guard teardown
  removes only the exact registered lease and cannot erase a replacement;
- a Conversation with no explicit binding never consults ambient process
  state, eliminating parallel-test and foreground-session contamination;
- the existing delegated parent budget remains an independent record-only
  admission ledger; reservation rollback order is unchanged;
- no scheduler, model policy, scenario prompt, token limit or pass threshold
  changes are authorized.

Mandatory proof is a focused two-session isolation test, the existing global
failure/rollback tests, parallel Conversation tests, and then the unchanged
full Runtime/workspace regression. Only after those pass may the immutable
real-provider build begin.

### 16.7 Implementation and deterministic conformance

The implementation stayed inside the audited cone:

- semantic compilation validates producer/dependency/input triples before
  admission, derives `UpstreamConsumption` only for exact consuming roles and
  removes input-only artifacts from output acceptance;
- Team instantiation emits typed `UpstreamEvidence` for upstream consumers and
  typed `ScopedEvidence` only for bounded source scopes; structured output
  shape alone no longer fabricates a fresh-acquisition requirement;
- Agent and Team verification retain one `AcceptanceEvaluator` verdict while
  distinguishing authenticated upstream consumption from fresh Provider-tool
  observation;
- Gateway returns a typed receipt for admitted `failed`/`blocked` Programs and
  keeps ToolError for rejected, unavailable and pre-admission failures;
- evaluation Token leases are RuntimeServices-owned, keyed by canonical
  Session ID and explicitly bound to every root/delegated Conversation.

Deterministic evidence on the unchanged source candidate:

- focused compiler, instantiation, Agent validator, Team verifier and Gateway
  terminal tests: passed;
- Session lease isolation, duplicate-registration rejection, delegated Host
  propagation and rollback tests: passed;
- Runtime library parallel regression: 1,907 passed, 0 failed, 2 ignored;
- `cargo check --workspace --all-targets`: passed without warnings;
- architecture boundary gate: passed;
- full workspace all-target regression, ten isolated Gateway global-environment
  tests, standalone reference Bundle and generic APP proxy: passed in 677 s.

The quick governance lane also passed compilation and every architecture
boundary, then correctly remained red because the release authority/evidence
inventory still identifies v0.9.708. Those release records must not be advanced
until the immutable Qwen run succeeds; they are an S6 gate, not a production
or test defect.

## 17. Live acceptance and lifecycle authority amendment (2026-08-29)

Status: **implemented; deterministic and immutable live gates passed**. This
amendment follows the first immutable paid-route rerun of candidate
`ecaf4bc4dd1e0a3860c91d2d9c27537e41afdf42`, release binary SHA-256
`467ca9d5b765d3774abe497ed04adcc3273b642253314374e728b44e8f53d1b7`.
The source candidate and scenario inputs remained unchanged during the run.

### 17.1 Immutable evidence and framework root causes

The group-theory scenario passed with 4/4 Teams, 4/4 Agents, three cross-Team
edges, 26 tool calls, 11 Provider rounds and 1,717,397 tokens. The six-Team
pressure scenario completed 6/6 Teams and 12/12 Agents over five cross-Team
edges, 97 tool calls, 25 Provider rounds and 8,560,712 tokens. Every one of the
12 exact source paths was independently attested, only `qwen3.8-max` ran, and
all architecture, handoff, transport, coverage, fact/inference/simulation,
concurrency, bottleneck, failure-mode and capacity checks passed.

The pressure report nevertheless failed one presentation check. Its terminal
explicitly states that larger collaboration is suitable inside the current
single-node boundary, lists the prerequisites for horizontal expansion and
gives actionable scale-up/scale-out advice. The checker accepted only the
literal fragments `扩大规模` or `scale recommendation`; it did not recognize
`扩容`, `扩大协作规模` or `横向扩展`. Its positive unit test contained only the
heading `扩大规模结论` followed by `结论完整`, proving both a false negative and
a heading-only false positive. This is a semantic evidence-shape defect, not a
model-quality failure.

The same run emitted three rejected `Complete -> Finalizing` transitions.
Canonical event inspection proves the sequence: a child
`execution_node.transitioned(status=completed)` was replayed into the Agent's
live record, the generic durable reducer mapped any `completed` status to the
whole execution's `Complete`, and the normal terminal synthesizer then entered
`Finalizing`. A child-node business status therefore stole lifecycle authority
from the owning execution. The temporarily stale mission display is a derived
projection symptom; no independent cache or polling defect is established.

### 17.2 Frozen target model

```text
presentation concept evidence
  = one bounded semantic block
    containing a scale subject
    AND an explicit decision, recommendation or prerequisite

durable child/business event status
  -> metrics, references and progress only
explicit execution-terminal carrier
  -> Complete | Error | Cancelled
live Cowd lifecycle owner
  -> Queued ... Finalizing -> terminal (monotonic)
```

The checker remains deterministic and local. It does not ask a second model to
judge prose, inject an expected answer into the scenario or accept a keyword
from an unrelated heading. The live reducer recognizes terminal authority by
event kind, not by a generic `status` string shared by graph nodes, tools and
business projections.

### 17.3 Approved implementation phases

| Phase | Change | Mandatory proof |
| --- | --- | --- |
| Q1 semantic concept predicate | Split terminal prose into bounded blocks and require the scale subject and a decision/action/prerequisite in the same block | the observed high-quality Qwen terminal passes; heading-only, subject-only and unrelated-block examples fail; affirmative and conditional/negative recommendations pass |
| Q2 terminal event authority | Remove generic durable-status lifecycle mutation; recover terminal state only from explicit execution-terminal carriers while retaining graph id, tool metrics and cursor progress from all events | completed child node cannot pre-complete an Agent; its later `Finalizing` transition succeeds; canonical terminal recovery remains idempotent |
| Q3 projection consistency | Exercise the real child-complete -> finalizing -> terminal sequence through replay and hot live state; treat mission lag as resolved only when the authoritative live sequence is correct | no `Complete -> Finalizing` warning, no false terminal, and the final terminal remains discoverable after restart |
| Q4 deterministic closure | Run focused adversarial tests, Runtime library, all-target check, architecture boundary and unchanged full regression | all gates green on one unchanged candidate |
| Q5 immutable live closure | Commit and build once, record commit/binary hash, then rerun both paid-route scenarios without source edits | 2/2 pass, only `qwen3.8-max`, no fallback, dangling work, false terminal or lifecycle warning |
| Q6 release closure | Advance evidence and release authority only after Q5, then version/tag/sync/push/clean | `master == dev == v0.9.710`, both remotes and clean worktrees verified |

### 17.4 Strict architecture audit

Audit result: **approved**. Implementation is authorized only inside Q1-Q6.

| Dimension | Audited invariant |
| --- | --- |
| semantic authority | A scale recommendation requires co-located subject plus decision/action semantics; a heading, isolated keyword or `结论完整` cannot satisfy it |
| determinism | Matching is bounded string classification over one terminal; no Provider call, score threshold relaxation or scenario-specific terminal exception |
| lifecycle authority | `Complete`, `Error` and `Cancelled` may be recovered only from explicit carriers that terminalize the same execution; child node, graph, tool and acceptance statuses never own the parent lifecycle |
| state machine | `Complete -> Finalizing` remains forbidden. Allowing it would hide corruption and make terminal state non-monotonic |
| recovery | Canonical terminal replay and checkpoint idempotence remain intact. Non-terminal durable events still restore metrics/references and advance the replay cursor |
| concurrency | No new locks, scheduler, timer or polling loop. Classification is linear in bounded terminal text; replay remains linear in canonical events |
| projection | Mission and Gateway consume Runtime-owned lifecycle truth; no cache TTL or forced-refresh patch is permitted without independent evidence |
| compatibility | Historical explicit terminal carriers remain readable. Ambiguous historical `status=completed` events become non-terminal instead of fabricating completion |
| security | Provider prose cannot mint Runtime terminal state, and a nested business event cannot terminate its parent through a shared status vocabulary |
| resources | No second transcript, event ledger or model judge is added; existing bounded text and durable events are reused |

### 17.5 Adversarial acceptance matrix

- heading `扩大规模结论` plus `结论完整`: reject;
- scale subject without a decision or prerequisite: reject;
- decision text in a different paragraph from the scale subject: reject;
- `适合继续扩大协作规模，但横向扩展必须先完成分片`: accept;
- `暂不建议扩容，需先消除恢复串行瓶颈`: accept as a complete negative
  recommendation;
- English scale-up/scale-out recommendation with an explicit should/must/not
  suitable decision: accept;
- completed execution node followed by Agent finalization: Agent remains
  non-terminal until its explicit terminal carrier;
- completed graph checkpoint, tool call or acceptance event: never terminalizes
  the Agent;
- explicit `runtime.session.terminal_requested`: recover `Complete` and its
  terminal reference idempotently;
- restart before a terminal carrier: replay remains non-terminal and continues
  from subsequent lifecycle events;
- restart after a terminal carrier: final state remains terminal and monotonic.

If implementation or Q4/Q5 reveals a new authority, scheduler, persistence or
acceptance design requirement, work returns to this audit before further source
changes. Adding one observed synonym, changing the prompt, weakening the pass
bar, permitting terminal reversal or adding refresh timers is explicitly
forbidden.

### 17.6 Implementation and deterministic conformance

Implementation remained inside the audited authority boundaries:

- the scale check now classifies bounded non-heading semantic blocks and
  requires a scale subject together with an explicit decision, action or
  prerequisite;
- heading-only, label-only and cross-paragraph keyword combinations fail;
- the observed Qwen conclusion, complete positive/conditional guidance and a
  complete negative recommendation pass without changing the scenario;
- durable replay still restores graph identity, tool metrics and cursor
  progress, but generic business status vocabulary no longer mutates live
  lifecycle;
- only `runtime.session.terminal_requested` with a durable payload reference
  recovers completion, through the existing monotonic transition reducer;
- malformed and duplicate terminal carriers are fail-closed/idempotent.

Deterministic evidence on the unchanged implementation candidate:

- Harness Eval library: 117 passed, 0 failed;
- Runtime library: 1,910 passed, 0 failed, 2 ignored;
- `cargo check --workspace --all-targets`: passed without warnings;
- architecture boundary gate: passed;
- full workspace all-target regression: passed;
- ten isolated Gateway process-global environment tests: passed;
- standalone reference Bundle, deterministic signing/tamper checks, generic
  APP worker lifecycle and Gateway/TUI proxy: passed.

The candidate may now be committed and built once for Q5. Release authority
and version evidence remain deliberately unchanged until both immutable live
scenarios pass and the Gateway log contains no lifecycle-authority warning.

### 17.7 Immutable Qwen closure

The clean source candidate was committed once as
`b4e40454576e5e2ec92e21428e372b5073d28d6c` and built as the release Cowd
binary before either scenario started:

- source archive SHA-256:
  `66b7346b81bf702f7ec86ee7d3d4abe28b8b5325361f1806578261268b3bec94`;
- Gateway binary SHA-256:
  `bd1b6f0bad0ed60c59e79e14cb6873ac57b9206bf2944990f42f676abed09540`;
- binary size: 122,149,752 bytes;
- report:
  `/tmp/cowd-qwen38-v0910-lifecycle-authority-final/runs/v0.9.710-1788014810-mission-harness-deep/report.json`;
- isolated Gateway: `/tmp/cowd-real-qwen-gateway.5oUqXW`.

The unchanged paid DashScope route executed only `qwen3.8-max`, with no model
fallback. Both required scenarios passed:

| Scenario | Runtime result | Work | Acceptance |
| --- | --- | --- | --- |
| group-theory research/evaluation/simulation | 4/4 Teams, 4/4 Agents, 3/3 cross-Team claims | 9 model rounds, 14 tool calls, 871,976 tokens | canonical Program, lineage, source receipts, C4/method/application/evaluation synthesis all passed |
| six-Team collaboration pressure | 6/6 Teams, 12/12 Agents, 5/5 cross-Team claims | 27 model rounds, 97 tool calls, 8,420,052 tokens | 12/12 exact paths independently observed by two Agent identities; every presentation and architecture check passed |

Aggregate live evidence is 36 model rounds, 111 tool calls and 9,292,028
tokens. The report is `passed`, its live suite is 2/2, and all 19 required
report gates passed. The six-Team terminal is transport-clean and complete; it
separates verified facts, source-grounded inference and unexecuted simulation,
corrects three inherited path-label mistakes, and provides an actionable
conditional scale recommendation in the same semantic block as its scale
subject.

The retained Gateway log has SHA-256
`8a086bbb3257fd010b2629668ab677a101de40244a6867dbce2dcfa3e0180fd8`.
It contains zero warning/error lines and zero matches for invalid live status
transition, `Complete -> Finalizing`, fallback, 429 or quota failure. Mission
projection reached every expected terminal without approval or recovery work.
The earlier display lag is therefore closed as a consequence of lifecycle
authority pollution; no cache timer or refresh workaround was needed.

Q5 is complete and Q6 release closure is authorized. The subsequent release
evidence/governance commit is documentation-only and must remain a descendant
of this tested source candidate; it does not alter the verified binary.
