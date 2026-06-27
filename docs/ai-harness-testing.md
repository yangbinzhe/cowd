# AI Harness Testing Manual

This document describes the Cowd AI Harness test system and the commands used
to prove core health, real provider behavior, tool safety, scenario coverage,
and repeat stability.

## Test Layers

| Layer | Purpose | Default |
| --- | --- | --- |
| L0 deterministic checks | Formatting, core crates, platform contracts | yes |
| L1 local deep scenarios | Strategy, workgraph, final gate, memory/matrix, agent merge | yes |
| L2 live provider light suite | Real model stream, structured output, routing | opt-in |
| L3 tool closure | Readonly tool execution, write denial, readonly batch isolation | yes |
| L4 scenario E2E | Gateway, runtime/session, memory, tool permission, skill/matrix | opt-in |
| L5 repeat stability | Repeated health runs and flaky lane evidence | opt-in |

## Main Health Command

```bash
scripts/ci/ai-harness-health-report.sh
```

Outputs:

- `target/ai-harness-health/latest.md`
- `target/ai-harness-health/latest.json`
- `target/ai-harness-health/lanes.tsv`
- one log file per lane under `target/ai-harness-health/`

Deep and scenario-style harness reports must follow
[`ai-harness-report-spec.md`](ai-harness-report-spec.md). A compact
`report.md` is only an index. The evaluator must produce the structured
evidence plus `analysis-context.json`, `full-analysis-report-template.md`, and
`full-analysis-report-prompt.md`; an AI reviewer then generates
`full-analysis-report.md` with the full analysis, local failures,
tool/provider evidence, runtime action interpretation, and evidence index.

Default lanes:

- `format-and-whitespace`
- `ai-harness-core`
- `ai-platform-contracts`
- `runtime-full-capability-eval`
- `deep-local-scenarios`
- `tool-closure`
- `provider-failure-classification`

## Full Local And Scenario Health

```bash
COWD_AI_HARNESS_FULL_WORKSPACE=1 \
COWD_AI_HARNESS_SCENARIO=1 \
scripts/ci/ai-harness-health-report.sh
```

This adds:

- `cargo test --workspace --exclude gateway --all-targets`
- `cargo test -p gateway --all-targets -- --test-threads=1`
- `scripts/ci/scenario.sh`

The scenario report is written under `test-reports/validation-*/report.md`.
The gateway crate is run serially in this lane because some gateway tests mutate
process-global configuration.

## Live Provider Validation

Live validation is always opt-in and must not be enabled in default CI without
an explicit quota decision.

Recommended light suite:

```bash
COWD_AI_HARNESS_LIVE=1 \
COWD_AI_HARNESS_LIVE_MODEL=deepseek-v4-flash \
COWD_AI_HARNESS_LIVE_MODE=all-light \
scripts/ci/ai-harness-live-provider.sh
```

Modes:

- `smoke`: provider config, simple direct answer, complex planning
- `stream`: real stream event ordering
- `drift`: 3 repeated structured JSON probes
- `routing`: simple/complex/high-risk route behavior
- `all-light`: all of the above

## Full Deep Verification

```bash
COWD_AI_HARNESS_LIVE=1 \
COWD_AI_HARNESS_LIVE_MODEL=deepseek-v4-flash \
COWD_AI_HARNESS_LIVE_MODE=all-light \
COWD_AI_HARNESS_FULL_WORKSPACE=1 \
COWD_AI_HARNESS_SCENARIO=1 \
scripts/ci/ai-harness-health-report.sh
```

This is the current strongest one-command proof for the AI Harness core.

## Repeat Stability

```bash
COWD_AI_HARNESS_REPEAT=3 scripts/ci/ai-harness-repeat.sh
```

Health report integration:

```bash
COWD_AI_HARNESS_REPEAT_ENABLED=1 \
COWD_AI_HARNESS_REPEAT=3 \
scripts/ci/ai-harness-health-report.sh
```

Outputs:

- `target/ai-harness-repeat/latest.json`
- `target/ai-harness-repeat/summary.tsv`
- `target/ai-harness-repeat/run-*/latest.json`

## Core Evidence By Lane

`deep-local-scenarios` proves:

- complex tasks route to `PlanExecute`
- workgraphs include DAG/review/synthesis
- empty final answers are blocked
- failures create repair hints and memory candidates
- multi-agent conflicts are surfaced instead of hidden
- low-value multi-agent history downgrades later routing

`tool-closure` proves:

- readonly file reads work under `ReadOnly`
- write attempts are denied under `ReadOnly`
- readonly batches reject write tools before execution

`provider-failure-classification` proves:

- auth failures are non-retryable provider auth
- 429 failures are retryable rate limit
- provider internal failures remain retryable provider errors
- context failures are classified as context window issues
- retry exhaustion preserves request-id and underlying class

`live-deep-validation` proves, when enabled:

- the configured real provider is reachable
- stream events arrive in a valid order
- structured output remains stable across repeated probes
- simple, complex, and high-risk prompts route differently

## Acceptance Rule

A capability is not considered complete unless its lane is represented in
`latest.json`, has a reproducible command, and records evidence logs. Live lanes
must also state the model, provider, probe count, and token usage in the log.
Deep harness result packages must also satisfy the report-completeness rules in
[`ai-harness-report-spec.md`](ai-harness-report-spec.md).

## Current Baseline

Latest local full verification:

- `target/ai-harness-health/latest.md`
- `target/ai-harness-health/latest.json`
- status: `PASS`
- command included full workspace, scenario, live, and two repeat health runs

Latest real provider light verification:

- model: `deepseek-v4-flash`
- mode: `all-light`
- result: 4 ignored live tests explicitly enabled and passed
