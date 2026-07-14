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
    reality-context-eval.json
  token-usage/
  traces/
```

## Required Report Sections

- Scenario objective and expected execution pattern.
- Model-visible capability context.
- Whether `runtime_capabilities` was called.
- Whether `runtime_orchestrate` was called.
- Selected execution pattern, modifiers, gates, and template.
- Runtime decision: accepted, planned, rejected, needs approval, running, completed, or failed.
- Tool DAG / ReWOO / team / deliberation / reflexion evidence.
- Reality evidence: RecallReport, ContextEnvelope, selected context, omitted context, evidence refs.
- Token usage by round and total.
- Latency by round and total.
- Tool count and agent count.
- Correctness judgment and residual risks.
- Comparison against naive ReAct when applicable.

## Required Scenario Matrix

| Scenario | Expected Pattern | Required Evidence |
|---|---|---|
| simple_question | direct | No over-orchestration |
| readme_code_audit | explore + parallel | Batch read-only evidence |
| code_refactor | execute + with_verifier | Plan, patch, review, verify |
| architecture_tradeoff | deliberate | Alternatives, critique, merged decision |
| manufacturing_what_if | deliberate + with_matrix_evidence | Structured facts and what-if reasoning |
| memory_conflict | deliberate + with_trace | Scope, conflict, latest-fact handling |
| reality_recall | explore + with_trace | RecallReport selected/omitted/source evidence |
| context_envelope | explore | Stable head, runtime header, dynamic items, omissions |
| knowledge_default | direct + with_trace | Shared/default knowledge activation and namespace blocking |
| fact_matrix_trace | deliberate + with_matrix_evidence | Fact/Matrix evidence refs and boundary |
| failure_loop | execute + with_verifier | Retry and verification evidence |
| multi_agent | collaborate + parallel | Team/agent progress and synthesis |
| cross_session | supervise + background | Session relation and boundary |
| high_risk | execute + approval gate | Approval or explicit rejection |
