# High-quality collaboration terminal architecture (v0.9.710)

Status: implemented authority for collaboration result transport and root presentation.

## Problem statement

The pre-v0.9.710 path treated bounded presentation strings as if they were canonical result data. A large Agent result was replaced with `[truncated]`, Team summaries were sliced again according to Team count, and the Host mechanically joined those fragments. Because the root answer gate checked only that the string was non-empty and contained no tool protocol, this transport bundle could be committed as a satisfied user answer without root synthesis.

The failure was architectural, not a Qwen-specific quality defect. Qwen 3.8-max made the defect observable by producing sufficiently broad results to cross both lossy boundaries.

The first v0.9.710 real-provider rerun exposed a second, subtler false positive. The Program topology, durable delivery envelopes and root presentation all passed, but downstream Team E reported that it had not received the complete A/B terminal semantics and Team F reported the same for C/D/E. Three independent presentation limits were still acting as hidden semantic limits: predecessor Agent context was capped at 16,384 characters, cross-Team summaries were capped at 4,000 characters, and root receipt reconstruction kept at most 32 receipts with 4,000 characters per receipt and 48,000 characters overall. The dependency gate therefore proved scheduling order but not semantic handoff consumption.

The next candidate proved complete semantic handoffs and exact EOF reads, yet exposed a third role-semantics defect. A model-derived reviewer carried both `Verification` and `UpstreamConsumption`, but Team instantiation classified every upstream consumer without an explicit `ReacquireEvidence` facet as a zero-tool reducer. The investigator produced valid whole-file receipts while the reviewer could only inspect the investigator's result and receipt summaries. A machine gate that deduplicated only by source path therefore accepted one producer receipt per file as “reviewed”. v0.9.710 now treats semantic verification over an independently observable bounded resource as an evidence-producing role at the final Agent-slot lease, not as an upstream-only reducer.

The typed-review candidate exposed a fourth and deeper distinction. ToolHost had acquired all 24 whole-file reads and persisted correct exact-content receipts, but the generic model-facing tool budget compacted several large JSON bodies into head/tail summaries. Agent acceptance and the live evaluator still treated the durable acquisition receipt as proof that the role had semantically observed the omitted body. The final design separates **exact acquisition** from **exact model observation**: an exact obligation expands the bounded Provider delivery budget, and only a non-omitting model receipt may enter that Agent's Runtime-owned `observed_acceptance`. If complete delivery still cannot fit, the Agent fails closed and orchestration must repartition it.

## Invariants

1. Agent semantic results are lossless canonical data. Character limits may apply to raw tool previews, never to the authored result contract.
2. Team and Program transport must preserve complete semantic values. It may reference durable bytes, but must never replace them with a truncation sentinel or slice them mid-field.
3. A multi-Team carrier is evidence, not an answer candidate. Only a dedicated root synthesis step may turn it into the user-facing answer.
4. The final answer and the complete source carrier are committed atomically in the terminal artifact.
5. Objective, structural and transport quality failures close fail-closed. One model repair is allowed; continued failure produces `Partial` plus an invalid presentation receipt, never a false `Satisfied` result.
6. Runtime resource ceilings remain safety contracts. When a complete evidence packet cannot fit a provider context, preflight rejects the attempt explicitly; no hidden content deletion is permitted.
7. Dependency satisfaction has two parts: the predecessor must be terminal and its complete semantic result must be materialized into the successor input. A topology-only wait is not a valid cross-Team handoff.
8. Semantic verification has two independent inputs: the complete upstream result and fresh Runtime evidence from the verifier's own bounded lease. Reviewing a receipt summary is not equivalent to independently observing its source. Session-only synthesis remains a zero-tool upstream consumer.
9. Independent review is a typed semantic relation. Model-authored Teams must encode it as `review_of`; `handoff` and `aggregate` deliberately compile to upstream consumption/synthesis and cannot silently acquire verifier authority. An optional `independent_review` acceptance must name the same local predecessor or compilation fails with a repairable diagnostic.
10. Acquisition and semantic observation are different Runtime facts. Durable ToolHost bytes prove that a read occurred; they satisfy an exact semantic obligation only when the complete body was also delivered to that role's Provider context. Omitted model-facing content is never promoted into Agent acceptance.

## Canonical chain

| Stage | Producer | Canonical value | Consumer | Failure behavior |
|---|---|---|---|---|
| Agent terminal | `AgentTaskExecutor` | Complete normalized structured result | Team reducer | Invalid Agent terminal fails its node; no sentinel substitution |
| Team terminal | `TeamResultReducer` | Complete verified evidence bundle plus delivery envelope | Program assessment | Unsatisfied delivery remains typed partial/unavailable |
| Program receipt | `assess_team_subgraphs` | Complete per-Team terminal summaries and compact delivery identities | Host terminal gate | Missing materialized Team state rejects carrier |
| Root evidence carrier | `verified_team_terminal_summary` | Typed `cowd.runtime.collaboration_evidence.v1` JSON | Root synthesizer only | Carrier cannot qualify as a direct Markdown answer |
| Root synthesis | `synthesize_collaboration_answer` | Coherent user-language answer | Deterministic quality gate | One evidence-preserving repair attempt |
| Terminal commit | `SynthesizeNodeExecutor` | Final answer, carrier, envelope, validation and transcript | Session outbox/artifact readers | Quality exhaustion commits `Partial`, not false success |

## State and ownership

| State | Single owner | Durable authority | Rebuild source |
|---|---|---|---|
| Agent semantic result | Agent graph node terminal | execution graph / outcome event | node result or canonical outcome event |
| Team delivery truth | Team reducer | child graph `DeliveryEnvelope` | child graph replay |
| Program topology and completion | collaboration coordinator | Program projection | Program events and graph projection |
| Root evidence carrier | Host reducer | orchestration receipt, then terminal artifact | verified child terminal projections |
| User presentation | root synthesize node | terminal presentation + terminal artifact | terminal outbox replay |

No second lifecycle cache is introduced. The carrier is immutable synthesis input; it does not own Team or Program status.

## Concurrency and recovery

- Independent Teams and Agents retain existing graph concurrency. Root synthesis starts only after the Program terminal receipt proves all required dependency claims and Team terminals.
- A rejected draft emits `TerminalPresentationSuperseded`; clients must not merge it with the repaired attempt.
- New durable user input crossing the final-answer fence supersedes the entire presentation attempt and replans from the current Program facts.
- Provider failure, context preflight failure, malformed output, transport leakage and objective incompleteness share the same bounded terminal repair authority. None may re-run completed Teams.
- The terminal artifact pins both the presentation and complete carrier under one durable owner, so crash recovery cannot restore one without the other.

## Quality contract

The deterministic gate checks facts that can be decided without a second subjective judge:

- no `[truncated]` or Runtime evidence-bundle/transport markers;
- balanced code fences and a non-truncated sentence ending;
- requested minimum count of concrete `crates/.../*.rs` paths;
- requested fact/inference/unexecuted-simulation separation;
- requested concurrency waves, bottlenecks, failure modes, capacity boundaries and C4 discussion;
- no direct multi-Team carrier bypass.

The live six-Team Qwen scenario independently repeats the presentation checks in the evaluator. A structurally successful Program therefore cannot pass the release evidence gate with an empty, concatenated, truncated or incomplete answer.

For objectives that require actual structured-handoff consumption, the Runtime and evaluator additionally reject missing-upstream admissions and require a positive conclusion that successor Teams consumed the complete upstream semantics. This turns semantic delivery into a tested acceptance property instead of inferring it from graph topology.

For objectives that require independent source review, the evaluator groups exact-content observations from each Agent terminal's Runtime-owned `observed_acceptance` by canonical source path and stable Agent execution identity. Raw ToolHost receipt collections elsewhere in the timeline are intentionally ignored. Every target must have model-observed evidence from at least two distinct Agents with a valid digest; repeated reads or duplicated projections from one Agent count once. This is deliberately independent of localized display names and canonical `role-<hash>` identifiers. The typed two-role dependency and Runtime `Verification` contract determine which successor is the reviewer. Presentation must agree with those Runtime facts; a positive coverage phrase followed by a receipt-only, omitted-body or incomplete-content-review caveat is rejected.

## Resource model

“No character budget may reduce quality” does not mean unbounded memory allocation. The framework separates:

- canonical result bytes: complete and durable;
- provider context capacity: explicitly preflighted;
- presentation: intelligently synthesized and validated;
- ordinary raw tool bodies: referenced through the artifact/evidence plane;
- exact-obligation tool bodies: delivered completely within the explicit Provider context ceiling or rejected before semantic acceptance.

When a Program carrier exceeds the single-request routing target, Runtime partitions only between complete Team results and performs evidence-preserving synthesis layers. Every intermediate layer is checked for transport leakage, complete sentence closure and preservation of every concrete source path before it can feed the next layer. An oversized individual Team remains intact and is sent through explicit provider preflight; it is never sliced. The hierarchy is bounded to four merge levels and fails closed if it cannot converge, while the original complete carrier remains in the terminal artifact.

## Deletions and compatibility

The following old behavior is removed rather than retained behind a flag:

- the 2,000-character Agent structured result compactor;
- `[truncated]` replacement for known and unknown result fields;
- the 12,000-character Program-wide Team-summary slice;
- direct commitment of mechanically joined Team evidence bundles;
- large-scale live acceptance based only on topology and non-empty response.
- semantic `Verification` roles silently downgraded to zero-tool upstream reducers despite owning bounded readable scopes;
- source-coverage acceptance based on one deduplicated receipt per path when independent review was required.
- exact semantic acceptance based on durable acquisition receipts whose model-facing body was compacted or omitted.

Single-Team, explicitly validated `TeamSynthesizer` answer candidates remain eligible for direct reuse because they are already user-presentation objects, not multi-Team transport bundles.
