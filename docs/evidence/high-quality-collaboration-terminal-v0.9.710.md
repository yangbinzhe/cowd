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

## Deterministic verification

| Gate | Result |
|---|---|
| `cargo check --workspace --all-targets` | passed |
| `cargo test -p runtime --lib` | passed: 1,885; ignored: 2; failed: 0 |
| `cargo test -p harness-eval --lib` | passed: 106; failed: 0 |
| six-Team concurrent Program admission, 20 consecutive executions | passed: 20/20 |
| adversarial old concatenated/truncated presentation | rejected |
| complete synthesized presentation fixture | accepted |
| lossless 8,000-character structured Agent fields | passed |
| hierarchical partition and intermediate source-path preservation | passed |
| `cargo fmt --all -- --check` / `git diff --check` | pending final candidate gate |

## Real-provider verification

Pending immutable-candidate execution. The final evidence revision must record the clean candidate commit, Gateway binary digest, report path, exact model provenance, Program/Team/Agent/edge counts, presentation gate results, final-answer audit and any hierarchical synthesis rounds before release tagging.

