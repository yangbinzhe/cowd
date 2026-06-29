# Runtime Execution Core Scenario Spec

This template defines the evidence package required for Runtime Execution Core
evaluation runs. It is intentionally a template, not hard-coded report prose.
The evaluator must generate scenario-specific analysis from collected evidence.

## Required Result Package

```text
scenario-run/
  report.md
  summary.json
  request-response/
  tool-calls/
  runtime-events/
  evidence/
  token-usage/
  traces/
```

## Required Report Sections

- Scenario objective and expected execution mode.
- Model-visible capability context.
- Whether `runtime_capabilities` was called.
- Whether `runtime_orchestrate` was called.
- Selected execution mode and template.
- Runtime decision: accepted, planned, rejected, needs approval, running, completed, or failed.
- Tool DAG / ReWOO / team / deliberation / reflexion evidence.
- Token usage by round and total.
- Latency by round and total.
- Tool count and agent count.
- Correctness judgment and residual risks.
- Comparison against naive ReAct when applicable.

## Required Scenario Matrix

| Scenario | Expected Mode | Required Evidence |
|---|---|---|
| simple_question | direct_answer | No over-orchestration |
| readme_code_audit | parallel_read_fanout / rewoo | Batch read-only evidence |
| code_refactor | plan_execute / implementation_review_fix | Plan, patch, review, verify |
| architecture_tradeoff | deliberation_search / debate_consensus | Alternatives, critique, merged decision |
| manufacturing_what_if | deliberation_search + matrix evidence | Structured facts and what-if reasoning |
| memory_conflict | reflexion / reality memory | Scope, conflict, latest-fact handling |
| failure_loop | reflexion_retry | Supervisor mode switch |
| multi_agent | supervisor_subagents | Team/agent progress and synthesis |
| cross_session | request_session_link | Session relation and boundary |
| high_risk | risk_gate / human_confirm | Approval or explicit rejection |

