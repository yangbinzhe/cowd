# High-quality collaboration terminal architecture (v0.9.710)

Status: implemented authority for collaboration result transport and root presentation.

## Problem statement

The pre-v0.9.710 path treated bounded presentation strings as if they were canonical result data. A large Agent result was replaced with `[truncated]`, Team summaries were sliced again according to Team count, and the Host mechanically joined those fragments. Because the root answer gate checked only that the string was non-empty and contained no tool protocol, this transport bundle could be committed as a satisfied user answer without root synthesis.

The failure was architectural, not a Qwen-specific quality defect. Qwen 3.8-max made the defect observable by producing sufficiently broad results to cross both lossy boundaries.

The first v0.9.710 real-provider rerun exposed a second, subtler false positive. The Program topology, durable delivery envelopes and root presentation all passed, but downstream Team E reported that it had not received the complete A/B terminal semantics and Team F reported the same for C/D/E. Three independent presentation limits were still acting as hidden semantic limits: predecessor Agent context was capped at 16,384 characters, cross-Team summaries were capped at 4,000 characters, and root receipt reconstruction kept at most 32 receipts with 4,000 characters per receipt and 48,000 characters overall. The dependency gate therefore proved scheduling order but not semantic handoff consumption.

## Invariants

1. Agent semantic results are lossless canonical data. Character limits may apply to raw tool previews, never to the authored result contract.
2. Team and Program transport must preserve complete semantic values. It may reference durable bytes, but must never replace them with a truncation sentinel or slice them mid-field.
3. A multi-Team carrier is evidence, not an answer candidate. Only a dedicated root synthesis step may turn it into the user-facing answer.
4. The final answer and the complete source carrier are committed atomically in the terminal artifact.
5. Objective, structural and transport quality failures close fail-closed. One model repair is allowed; continued failure produces `Partial` plus an invalid presentation receipt, never a false `Satisfied` result.
6. Runtime resource ceilings remain safety contracts. When a complete evidence packet cannot fit a provider context, preflight rejects the attempt explicitly; no hidden content deletion is permitted.
7. Dependency satisfaction has two parts: the predecessor must be terminal and its complete semantic result must be materialized into the successor input. A topology-only wait is not a valid cross-Team handoff.

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

## Resource model

“No character budget may reduce quality” does not mean unbounded memory allocation. The framework separates:

- canonical result bytes: complete and durable;
- provider context capacity: explicitly preflighted;
- presentation: intelligently synthesized and validated;
- raw tool bodies: referenced through the artifact/evidence plane.

When a Program carrier exceeds the single-request routing target, Runtime partitions only between complete Team results and performs evidence-preserving synthesis layers. Every intermediate layer is checked for transport leakage, complete sentence closure and preservation of every concrete source path before it can feed the next layer. An oversized individual Team remains intact and is sent through explicit provider preflight; it is never sliced. The hierarchy is bounded to four merge levels and fails closed if it cannot converge, while the original complete carrier remains in the terminal artifact.

## Deletions and compatibility

The following old behavior is removed rather than retained behind a flag:

- the 2,000-character Agent structured result compactor;
- `[truncated]` replacement for known and unknown result fields;
- the 12,000-character Program-wide Team-summary slice;
- direct commitment of mechanically joined Team evidence bundles;
- large-scale live acceptance based only on topology and non-empty response.

Single-Team, explicitly validated `TeamSynthesizer` answer candidates remain eligible for direct reuse because they are already user-presentation objects, not multi-Team transport bundles.
