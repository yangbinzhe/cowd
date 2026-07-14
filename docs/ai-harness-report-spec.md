# AI Harness Report Specification

This document defines the mandatory report shape for Cowd AI Harness evaluation
runs. It is part of the test contract, not a planning note.

## Purpose

Every deep or scenario-style harness evaluation must produce both machine
evidence and a human-readable analysis report. A short summary is not enough:
the report must explain what was tested, what happened, what evidence was
created, which local failures were observed, and why the final verdict is valid.

Report generation is a two-step contract:

1. The evaluator writes structured evidence plus report-generation assets.
2. An AI reviewer consumes those assets and writes the final
   `full-analysis-report.md`.

Long-form report analysis must live in templates and prompts, not in Rust
business logic.

## Result Package Layout

Each run writes a self-contained package:

```text
runs/<stamp>-mission-harness-<level>/
  report.md
  full-analysis-report.md
  full-analysis-report-template.md
  full-analysis-report-prompt.md
  analysis-context.json
  report.json
  execution-trace.json
  provider-rounds/
    001-<round>.json
  tool-calls/
    001-<scenario>-<tool>.json
  requests/
  responses/
  events/
  run-evidence/
  model-speed/
  quality-rubric/
  evidence/
    evidence-manifest.json
    next-gen-harness-closure.json
    complex-scenarios.json
    reality-context-eval.json
    real-tool-scenarios.json
    stable-ai-health.json
```

`report.md` is an index and compact summary. `analysis-context.json`,
`full-analysis-report-template.md`, and `full-analysis-report-prompt.md` are the
AI report-generation contract. `full-analysis-report.md` is the canonical human
report after the AI reviewer fills it from evidence. JSON files are the full
audit trail.

## Mandatory Full Analysis Sections

`full-analysis-report.md` must include these sections in this order:

1. `执行结论`
   - final status
   - pass/fail counts
   - local failures or degraded behavior, even when final status is passed
   - whether follow-up work is required

2. `测试目标`
   - capabilities under test
   - why this run is meaningful

3. `测试环境`
   - workspace/worktree/target repo
   - gateway status
   - provider/model/budget
   - result package path

4. `执行统计`
   - elapsed time
   - provider rounds
   - token usage
   - runtime actions
   - tool calls
   - scenario pass rates

5. `能力项结果`
   - every capability row
   - status, evidence, and interpretation

6. `真实工具场景分析`
   - scenario goal
   - tool calls used
   - runtime/matrix/memory evidence
   - changed files or isolated artifacts
   - conclusion
   - acceptance basis: why this scenario passed or failed
   - evidence strength: strong / medium / weak, with justification
   - limitation: what this scenario still does not prove
   - next action: what should be improved next

7. `复杂场景分析`
   - generated scenario list
   - score and failed checks
   - whether the scenario is deterministic simulation or real execution

8. `Provider 回合分析`
   - round purpose
   - model
   - latency
   - token usage
   - request/response summary
   - detail file path

9. `Runtime Action 证据链`
   - ordered runtime action log grouped by domain

10. `工具调用分析`
    - total tool calls
    - success/failure counts
    - each failed tool call with error and detail path
    - clarify that provider tool-use events and local tool execution are different

11. `证据包结构`
    - list key files and directories

12. `代码原型变更边界`
    - target branch/worktree
    - whether the target repo was modified
    - isolated test artifacts

13. `问题与建议`
   - actionable follow-up issues
   - report any pass-with-warning behavior
   - classify issues as blocker / risk / improvement
   - explain whether the final verdict should be trusted despite local issues

14. `最终判断`
   - whether the run proves the target capability
   - what remains unproven
   - maturity assessment for each major harness domain
   - whether the run is suitable as a baseline

## Reporting Rules

- Do not hardcode long-form analysis prose in evaluator code. Keep report
  requirements in templates/specs and let an AI reviewer generate the final
  human report from evidence.
- Do not hide local failures behind an overall `passed` status.
- If a tool call fails, the full report must list the tool, scenario, error, and
  detail file path.
- If a scenario passes despite a failed supporting tool, explain why the
  acceptance criteria still passed.
- Provider `tool_use_count` and local `tool_calls` must be reported separately.
- Full request/response/tool output belongs in JSON detail files; the full
  analysis report should include summaries and file references.
- Any target repository used as an evaluation prototype must have its dirty
  state reported.
- Generated isolated artifacts must be listed so they can be inspected or
  cleaned.
- Analysis must not stop at "passed/failed"; it must explain the reason,
  evidence quality, residual risk, and follow-up action.
- A "passed" scenario with weak evidence must be called out as weak or partial
  proof.
- Deterministic simulations, real tool executions, and real provider calls must
  be distinguished explicitly.
- The final judgment must state which capabilities are proven, partially
  proven, and still unproven.
- Every scenario-style report must include `evidence/evidence-manifest.json`
  with repo, commit, version, command, real-model authorization, token usage,
  tool calls, and fixture health status.
- `next-gen-harness-closure` must cover simple direct handling, complex
  strategy selection, tool batch efficiency, team/agent execution,
  cross-session dispatch, memory/reality context governance, and
  conflict/recovery evidence.
- Report gates must reject unsupported claims: real-model claims with zero
  provider rounds, tool-validation claims with zero tool calls, orchestration
  claims with no runtime actions, memory/context claims with no Reality Context
  evidence, replay/recovery claims with no evidence refs, and external access
  claims with no connected/healthy fixture evidence.

## Template

```markdown
# Mission Harness <Level> Full Analysis Report

## 1. 执行结论

## 2. 测试目标

## 3. 测试环境

## 4. 执行统计

## 4.1 深度分析摘要

## 5. 能力项结果

## 6. 真实工具场景分析

## 7. 复杂场景分析

## 8. Provider 回合分析

## 9. Runtime Action 证据链

## 10. 工具调用分析

## 11. 证据包结构

## 12. 代码原型变更边界

## 13. 问题与建议

## 14. 最终判断
```

## Acceptance

A deep harness run is report-complete only if:

- `report.md` exists.
- `analysis-context.json` exists.
- `full-analysis-report-template.md` exists.
- `full-analysis-report-prompt.md` exists.
- `full-analysis-report.md` exists after the AI report-generation step.
- `report.json` exists.
- `execution-trace.json` exists.
- `evidence/evidence-manifest.json` exists.
- `evidence/next-gen-harness-closure.json` exists.
- provider detail files exist when provider rounds are present.
- tool detail files exist when tool calls are present.
- evidence files exist for scenario suites that ran.
- local failures are represented in `full-analysis-report.md`.
