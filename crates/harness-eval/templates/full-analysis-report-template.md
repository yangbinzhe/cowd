# Mission Harness {{level}} Full Analysis Report

> This template governs an optional AI `ReportReviewer` pass over the result
> package. It must never replace the independent scenario verdicts in
> `report.json`. When no reviewer is requested or available, the generated
> report must be clearly labelled as a structured evidence summary rather than
> an AI-authored quality judgment. Use `report.json`, `execution-trace.json`,
> `provider-rounds/`, `tool-calls/`, and `evidence/`.

## 1. 执行结论

Required analysis:

- State final status and whether it is trusted.
- Explain pass/fail counts.
- Explicitly list local failures or degraded behavior even if the final status is passed.
- State whether follow-up work is required.

## 2. 测试目标

Required analysis:

- Explain what this run intended to prove.
- Distinguish quick/full/deep scope.
- State which AI harness capabilities are in scope and out of scope.

## 3. 测试环境

Required analysis:

- List workspace, target repo/worktree, provider, model, budget, gateway state, and result package path.
- State whether the target repo was modified or only isolated artifacts were created.

## 4. 执行统计

Required analysis:

- Report elapsed time, provider rounds, token usage, runtime actions, tool calls, pass rates.
- Interpret whether the evidence volume is enough for this test level.

## 4.1 深度分析摘要

Required analysis:

- Give an evidence-strength rating: strong / medium / weak.
- Explain the main residual risks.
- List capabilities proven, partially proven, and unproven.
- Distinguish deterministic simulation, real tool execution, real provider calls, and runtime action evidence.

## 5. 能力项结果

Required analysis:

- For each capability row, explain the status and evidence.
- Highlight any pass-with-risk item.
- Identify shallow evidence or over-broad claims.

## 6. 真实工具场景分析

For each real tool scenario, include:

- Scenario goal.
- Tool calls used.
- Runtime/matrix/memory evidence.
- Changed files or isolated artifacts.
- Acceptance basis: why this scenario passed or failed.
- Evidence strength: strong / medium / weak, with justification.
- Limitation: what this scenario still does not prove.
- Next action.

## 7. 复杂场景分析

Required analysis:

- List generated scenarios and scores.
- Explain whether each scenario is deterministic simulation or real execution.
- State what complex capability is proven and what remains unproven.

## 8. Provider 回合分析

For each provider round, include:

- Round purpose.
- Model.
- Latency and token usage.
- Request summary and response summary.
- Detail file path.
- Quality judgment: whether the response proves the intended contract.

## 9. Runtime Action 证据链

Required analysis:

- Group runtime actions by domain: fact/memory/matrix, mission/session/team, governance/recovery, tools/gateway.
- Explain the causal chain instead of only listing actions.

## 10. 工具调用分析

Required analysis:

- Count successful and failed tool calls.
- For each failed tool call, list scenario, tool, error, detail path, and impact.
- Explain whether a scenario passed despite a failed support tool and why.
- Clearly distinguish provider tool-use events from local tool execution.

## 11. 证据包结构

Required analysis:

- List key files and directories.
- Explain what each file type is used for.
- State which files should be read for audit.

## 12. 代码原型变更边界

Required analysis:

- State target branch/worktree.
- State whether the target repo remained clean.
- List isolated artifacts and whether cleanup is required.

## 13. 问题与建议

Required analysis:

- Classify issues as blocker / risk / improvement.
- Explain whether the final verdict should be trusted despite local issues.
- Provide concrete next gates or fixes.

## 14. 最终判断

Required analysis:

- State whether this run proves the target capability.
- State maturity assessment for each major harness domain.
- State whether this run can be used as a baseline.
- State what remains unproven.
