# High-quality collaboration terminal evidence (v0.9.710)

Date: 2026-08-29 (Asia/Shanghai)

## Candidate and scope

This version repairs the false-positive terminal observed in the v0.9.709 real `qwen3.8-max` six-Team run. The Program execution was structurally correct, but the final response was a mechanically concatenated, truncated transport bundle. The governing architecture is `docs/architecture/high-quality-collaboration-terminal-v0.9.710.md`.

Changed dependency cone: `harness-contract` → `runtime` → `gateway` / `harness-eval`, plus the workspace version and lockfile.

## Root-cause evidence

The v0.9.709 terminal ended mid-token (`MaintenanceLifecycle enum (Op`), contained 28 literal `[truncated]` sentinels, exposed 17 internal `runtime-team:` identifiers and concatenated six raw Team bundles. It did not distinguish verified facts, source inference and unexecuted simulation, and it omitted the requested concurrency/bottleneck/scale synthesis.

Source audit found four cooperating defects:

1. `AgentTaskExecutor` replaced large structured field values with truncation sentinels after 2,000 characters.
2. `assess_team_subgraphs` sliced all Team summaries into a 12,000-character Program-wide allowance.
3. `verified_team_terminal_summary` joined those fragments and promoted the join to `terminal_override`.
4. `qualified_root_answer` treated any non-empty Markdown as a valid root answer, bypassing the narrator.

The first v0.9.710 candidate rerun then found a second dependency-cone defect that the initial presentation gates did not cover:

1. `AgentRuntime::attach_predecessor_context` silently limited investigator-to-reviewer semantics to at most 16,384 characters.
2. `TeamSubgraphExecutor` and `decode_team_terminal_summary` silently limited each predecessor Team result to 4,000 characters.
3. `terminal_evidence_digest` retained at most 32 receipts, 4,000 characters per receipt and 48,000 characters total, followed by another 48,000-character outer slice.
4. The evaluator proved that the five dependency edges existed and waited correctly, but did not prove that E/F actually consumed complete predecessor semantics.

The exact-content candidate then found a third defect during human audit: all twelve investigator reads were complete and Runtime-attested, but every reviewer was still an upstream-only zero-tool consumer. `Verification` did not imply independent evidence reacquisition unless the role also happened to contain an explicit `ReacquireEvidence` facet. The evaluator deduplicated receipts by path, so one investigator receipt per target was sufficient to pass its former 12/12 check.

That run is deliberately retained as rejected evidence even though its machine status was `passed`:

| Observation | Value |
|---|---|
| report | `/tmp/cowd-qwen38-quality-v0910-rerun/runs/v0.9.710-1787977112-mission-harness-deep/report.json` |
| scenario | `/tmp/cowd-qwen38-quality-v0910-rerun/runs/v0.9.710-1787977112-mission-harness-deep/live-scenarios/001-live_qwen38_large_scale_collaboration.json` |
| model | only `qwen3.8-max`; no fallback |
| realized topology | 6 Teams, 12 Agents, 5 cross-Team edges |
| work | 19 model rounds, 49 tool calls, 535,515 total tokens |
| wall time | 784,092 ms |
| old evaluator | all checks passed; 13 concrete source paths; clean complete presentation |
| human semantic audit | rejected: answer said `有条件通过 / 部分通过` and admitted missing A/B→E and C/D/E→F semantic handoffs |

The later exact-content run is also retained as rejected evidence even though every machine check passed:

| Observation | Value |
|---|---|
| report | `/tmp/cowd-qwen38-quality-v0910-exact/runs/v0.9.710-1787985114-mission-harness-deep/report.json` |
| scenario | `/tmp/cowd-qwen38-quality-v0910-exact/runs/v0.9.710-1787985114-mission-harness-deep/live-scenarios/001-live_qwen38_large_scale_collaboration.json` |
| candidate | `9d165afc` |
| model | only `qwen3.8-max`; no fallback |
| realized topology | 6 Teams, 12 Agents, 5 cross-Team edges; all terminal |
| exact source evidence | 12/12 unique target paths, all `read_file` calls used `complete:true` |
| work | 21 model rounds, 49 tool calls, 845,494 scenario tokens |
| wall time | 896,278 ms |
| human semantic audit | rejected: reviewers consumed investigator results but had no independent local source receipts and the terminal explicitly disclosed receipt-level rather than source-level review |

The first independent-review run proved the Runtime repair but intentionally remains a failed evaluator record:

| Observation | Value |
|---|---|
| report | `/tmp/cowd-qwen38-quality-v0910-independent/runs/v0.9.710-1787987102-mission-harness-deep/report.json` |
| scenario | `/tmp/cowd-qwen38-quality-v0910-independent/runs/v0.9.710-1787987102-mission-harness-deep/live-scenarios/001-live_qwen38_large_scale_collaboration.json` |
| candidate | `52027413` |
| realized execution | 6/6 Teams and 12/12 Agents completed; 5 edges; 97 tool calls; no failure/recovery |
| durable exact receipts | 24 distinct role receipts: investigator and reviewer each read all 12 paths to EOF |
| failed gate | evaluator observed 0/12 independent paths because it matched only a role segment exactly equal to `investigator` or `reviewer`, while canonical role ids were `team-a-investigator`, `team-a-reviewer`, etc. |
| disposition | the first parser repair handled prefixed English ids but a subsequent candidate used localized role names compiled to opaque `role-<hash>` ids, proving name-based matching was still unsound. The final gate derives stable Agent execution identity from the durable receipt and requires two distinct identities per path; the failed report is not manually promoted |

A subsequent run under `/tmp/cowd-qwen38-quality-v0910-independent-final` was intentionally interrupted after the first four Teams entered: its opaque canonical role ids proved the name-based evaluator repair would inevitably produce another false negative. No completed report from that interrupted run is accepted as evidence.

The next full candidate run correctly rejected another semantic variation rather than producing a false pass:

| Observation | Value |
|---|---|
| report | `/tmp/cowd-qwen38-quality-v0910-independent-final2/runs/v0.9.710-1787989226-mission-harness-deep/report.json` |
| scenario | `/tmp/cowd-qwen38-quality-v0910-independent-final2/runs/v0.9.710-1787989226-mission-harness-deep/live-scenarios/001-live_qwen38_large_scale_collaboration.json` |
| candidate | `ec488d72` |
| realized execution | 6/6 Teams and 12/12 Agents completed; 5 cross-Team edges; all presentation concepts except coverage declarations were present |
| semantic cause | Qwen encoded each local investigator→reviewer dependency as ordinary `handoff`, so Runtime correctly compiled reviewers as zero-tool upstream consumers; 12 investigator reads occurred and the independent gate observed 0/12 |
| disposition | general model guidance and the live contract now state that independent verification must use `review_of`; optional `independent_review` acceptance is compiler-validated against the same predecessor. `handoff` is not widened implicitly |

The first typed-`review_of` run then exposed a fourth false positive during human audit:

| Observation | Value |
|---|---|
| report | `/tmp/cowd-qwen38-quality-v0910-reviewof-final/runs/v0.9.710-1787991759-mission-harness-deep/report.json` |
| scenario | sibling `live-scenarios/001-live_qwen38_large_scale_collaboration.json` |
| candidate | `144a80b3` |
| realized execution | 6/6 Teams, 12/12 Agents, 5 claimed cross-Team edges; 29 model rounds; 97 tool calls |
| model and usage | only `qwen3.8-max`; 1,269,333 scenario tokens; 1,283,420 ms wall time |
| durable acquisition | 24 distinct exact read receipts, investigator and reviewer for every target |
| human semantic audit | rejected: the full terminal simultaneously claimed 12/12 independent review and disclosed that several bodies were not retained, reviewers confirmed only at receipt level, and content-level review remained incomplete |
| root cause | generic tool-result limits compacted large complete-read JSON before the next Provider request; Agent acceptance and the evaluator counted the durable raw receipt rather than model-visible content |
| disposition | exact obligations now receive bounded full-delivery budget; omitted exact bodies are filtered from Agent `observed_acceptance`; the evaluator reads only Agent-owned observed acceptance and rejects the newly observed contradiction language |

The first fail-closed candidate correctly stopped the false pass, but exposed an invalid evidence join and therefore remains rejected:

| Observation | Value |
|---|---|
| report | `/tmp/cowd-qwen38-quality-v0910-model-observed-final/runs/v0.9.710-1787994705-mission-harness-deep/report.json` |
| isolated Gateway | `/tmp/cowd-real-qwen-gateway.Mahx7D` |
| candidate / binary | `b7e2c6d4`; SHA-256 `f0a631983e15c54a90a0c4016245f4d0264553cf6418921fbec13c68bd06ffdd` |
| result | failed as designed instead of falsely passing; exact observations were not promoted, first-wave investigators failed and downstream Teams were blocked |
| forensic evidence | Provider trace contained no omission marker and the artifact store held both ToolHost and Conversation copies of complete outputs; the failure was not a read or context-budget failure |
| association defect | the filter joined ToolHost semantic evidence access to Conversation delivery audit by content access metadata. These are independently owned evidence namespaces and must not rely on incidental digest/byte equality |
| final repair | correlate each model delivery through the Conversation-owned stable join `ToolObservation.raw_ref == EvidenceAuditProjection.evidence_ref`; require a non-error, zero-omission delivery audit for every exact read receipt in the Agent |

## Implemented gates

- Agent semantic results are lossless and retain unknown structured fields.
- Program receipts retain complete Team terminal results.
- Multi-Team receipts become typed synthesis-only carriers.
- Dedicated zero-tool root synthesis reconciles evidence and runs one repair draft when required.
- Oversized Programs use up to four evidence-preserving hierarchical merge levels; partitioning occurs only between complete results.
- Intermediate layers must preserve every concrete source path.
- Final quality rejects truncation, Runtime transport leakage, unclosed fences, incomplete endings and objective-required omissions.
- The terminal artifact schema v3 atomically stores the answer and complete collaboration carrier.
- Exhausted quality recovery changes the envelope and goal completion to `Partial` and records an invalid presentation.
- The six-Team live evaluator independently enforces content/presentation quality in addition to topology.
- Investigator-to-reviewer, Team-to-Team and receipt-to-root synthesis paths no longer apply silent character/receipt caps to semantic results.
- A new semantic-handoff gate rejects topology-only success and requires the final result to confirm complete upstream consumption by E/F.
- Whole-file source evidence is accepted only from structured `read_file` output proving start line 1, EOF coverage, no truncation and a valid SHA-256 digest.
- Semantic verifiers with a bounded observable Agent-slot lease retain upstream context but receive their own tools, scoped evidence obligations and an explicit independent-reacquisition constraint.
- Every model-facing collaboration surface now explains the typed `review_of` requirement; `independent_review` criteria with an unknown subject or without a matching `review_of` edge fail compilation with an explicit repair.
- The large-scale gate now requires exact-content receipts from two distinct stable Agent execution identities for every one of the twelve target paths; duplicated projections and repeated reads by one Agent do not count twice, and localized/opaque role ids require no special case.
- The terminal presentation must positively state independent reviewer coverage and is rejected if it also contains a source-visibility or receipt-only caveat.
- Exact-content acquisition and exact model observation are now separate facts. The Agent runtime expands its tool-delivery budget only for an explicit exact obligation, preserves the Provider preflight ceiling, and refuses to promote an omitted body into semantic acceptance.
- Model-delivery proof is joined inside the Conversation evidence namespace through the raw evidence identity; ToolHost and Conversation artifacts are never cross-joined by coincidental hashes.
- The live source gates consume only Agent terminal `observed_acceptance`; raw durable tool receipts elsewhere in the timeline can prove acquisition but cannot make source coverage pass.

## Deterministic verification

| Gate | Result |
|---|---|
| `cargo check --workspace --all-targets` | passed |
| `cargo test -p runtime --all-targets --quiet` | passed: library 1,897; ignored: 2; all integrations passed |
| `cargo test -p tools --all-targets --quiet` | passed: 198 + 4; ignored: 1; failed: 0 |
| `cargo test -p harness-contract --all-targets --quiet` | passed: 193 + 1; failed: 0 |
| `cargo test -p harness-eval --all-targets --quiet` | passed: 115 + 2 + 2; failed: 0 |
| `cargo test -p gateway --all-targets --quiet` | passed: 801 + 1 + 1; ignored: 12; failed: 0 |
| six-Team concurrent Program admission, 20 consecutive executions | passed: 20/20 |
| adversarial old concatenated/truncated presentation | rejected |
| complete synthesized presentation fixture with independent-review declaration | accepted |
| lossless 8,000-character structured Agent fields | passed |
| hierarchical partition and intermediate source-path preservation | passed |
| complete Agent predecessor result regression | passed |
| complete Team predecessor result regression | passed |
| all distinct root receipts and tails regression | passed |
| topology-only semantic-handoff presentation | rejected by Runtime and evaluator gates |
| investigator-only exact receipts for all twelve paths | rejected by independent-source-review gate |
| positive independent-review phrase followed by a reviewer visibility contradiction | rejected |
| raw exact acquisition receipt outside Agent observed acceptance | rejected |
| exact model receipt with omitted tokens | excluded from Agent acceptance; task fails closed |
| large exact result above the ordinary per-tool budget | delivered without omission under the exact-obligation budget; Provider ceiling retained |
| semantic reviewer AgentTask packet | retains upstream input and independently receives `read_file`, scoped evidence and one exact obligation |
| `cargo fmt --all -- --check` / `git diff --check` | passed |

## Real-provider verification

Pending immutable-candidate execution after the semantic-handoff repair. The final evidence revision must record the clean candidate commit, Gateway binary digest, report path, exact model provenance, Program/Team/Agent/edge counts, the new semantic-consumption gate, final-answer audit and any hierarchical synthesis rounds before release tagging.
