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
- The large-scale gate now requires distinct durable investigator and reviewer exact-content receipts for every one of the twelve target paths; duplicated projections of one receipt do not count twice.
- The terminal presentation must positively state independent reviewer coverage and is rejected if it also contains a source-visibility or receipt-only caveat.

## Deterministic verification

| Gate | Result |
|---|---|
| `cargo check --workspace --all-targets` | passed |
| `cargo test -p runtime --tests --quiet` | passed: library 1,893; ignored: 2; all integrations passed |
| `cargo test -p tools --all-targets --quiet` | passed: 198 + 4; ignored: 1; failed: 0 |
| `cargo test -p harness-contract --all-targets --quiet` | passed: 193 + 1; failed: 0 |
| `cargo test -p harness-eval --all-targets --quiet` | passed on clean rerun: 113 + 2 + 2; failed: 0 |
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
| semantic reviewer AgentTask packet | retains upstream input and independently receives `read_file`, scoped evidence and one exact obligation |
| `cargo fmt --all -- --check` / `git diff --check` | passed |

## Real-provider verification

Pending immutable-candidate execution after the semantic-handoff repair. The final evidence revision must record the clean candidate commit, Gateway binary digest, report path, exact model provenance, Program/Team/Agent/edge counts, the new semantic-consumption gate, final-answer audit and any hierarchical synthesis rounds before release tagging.
