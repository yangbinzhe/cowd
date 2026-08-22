# 0821 P2 — durable facts and acceptance evaluator

## Boundary

This phase changes only the Runtime evaluator and Agent terminal boundaries.
It does not change Program admission, Team reduction, Host orchestration,
Gateway projection, schemas, versions, or external provider configuration.

## Authority chain

1. `AcceptanceReceiptSnapshot` accepts the frozen `RequiredAcceptance` and
   Runtime-attested criteria/evidence only. Model prose, workspace rereads and
   lifecycle status are excluded.
2. `AcceptanceEvaluator::evaluate_snapshot` is the sole verdict algorithm.
   Canonical observations and the evaluation's contract/receipt digests are the
   durable result carried by `AgentReturnPacket` and graph `ExecutionUsage`.
3. Both `AgentRuntime` and the graph `AgentTaskExecutor` canonicalize every
   backend return. A backend-provided evaluation is transport data and cannot
   mint a durable verdict.
4. Contract rejection calls `AcceptanceEvaluator::framework_invalid` with the
   same snapshot. It changes only the verdict; receipt/evidence facts are kept.
   Cancellation changes lifecycle status only and likewise retains the facts.
5. Validator and verification consumers read the evaluator revision/verdict;
   they do not implement a second obligation matcher.

## Checks

- `cargo fmt --all -- --check`
- `cargo test -p runtime acceptance_evaluator --lib`
- `cargo test -p runtime result_validator --lib`
- `cargo check -p runtime --lib`

All commands passed on the integration worktree during P2 implementation.

## Deliberate residuals

- `process_jsonl_adapter` is outside this phase's allowlist. Its legacy
  compatibility call remains API-compatible and produces an unresolved empty
  snapshot; it cannot produce satisfied receipt evidence.
- P2 does not make a Program/Team coordinator decision. P1/P3 own durable
  admission, cross-Team edge receipts, and escalation.
- This is unit/compile evidence only. Recovery, cross-Team and real-provider
  end-to-end gates remain release gates and are not claimed complete here.
