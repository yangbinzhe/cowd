# Provider model-observation attestation hardening (v0.9.710)

Status: design frozen after strict audit; implementation complete; unified
verification in progress.

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

### 11.3 Identity and replay audit

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

### 11.4 Concurrency and lock audit

- Tool execution remains parallel under the existing graph/ToolExecutionPlane.
- Generated metadata writes hold a turn-local mutex for bounded insertion only.
- Packed-request matching uses a local map and immutable HistoryView; no mutex
  is held across Provider dispatch or response streaming.
- Confirmation is a short commit after protocol validation and Session message
  append. Provider/tool/storage awaits do not occur under the metadata lock.
- Complexity is `O(history blocks + pending exact invocations)` per Provider
  attempt. Reverse iteration may stop when every pending id is found; there is
  no body-to-body quadratic comparison.

### 11.5 Failure and recovery audit

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

### 11.6 Compatibility audit

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

### 11.7 Audit conclusion

The amended plan has one truth owner at every stage, a complete forward and
reverse evidence path, bounded concurrency, explicit recovery semantics and an
additive compatibility boundary. All identified counterexamples have a
deterministic fail-closed result. Implementation is authorized in P1-P5 as one
coherent change set; P6 is verification-only. If P6 exposes a design change
rather than an implementation defect, execution returns to this audit instead
of patching during the test phase.
