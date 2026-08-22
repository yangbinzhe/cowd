# P2/B3 fan-in receipt-claim fence — 2026-08-23

This record covers one cross-Team fan-in safety correction. It is not evidence
that P2, B3, the 0821 program, or any release gate is complete.

## Defect corrected

The consumer claim reconciler previously treated one matching prior `Claimed`
edge as success when no edge was currently `Delivered`. In a recovery-shaped
state with a second incoming edge still `Pending`, that could let the subgraph
executor call `admit_or_resume` before receiving all required handoffs.

The reconciler now evaluates the complete incoming edge set:

- `Blocked`/`Cancelled` rejects admission;
- a `Delivered` edge is claimed one at a time under CAS;
- `Pending`/`AwaitingProducer` rejects as `cross_team_claims_not_all_delivered`;
- success requires every incoming edge to be `Claimed` by the same consumer
  node and attempt.

## Verification

```text
cargo test -p runtime consumer_cannot_admit_after_only_one_of_multiple_incoming_claims --lib -q
# 1 passed

cargo test -p runtime cross_team_edge_delivery_and_claim_are_fenced_by_node_attempts --lib -q
# 1 passed

cargo test -p runtime terminal_producer_without_required_cross_team_facts_blocks_edge_durably --lib -q
# 1 passed

cargo check -p runtime --all-targets

git diff --check
```

The regression constructs a three-Team Program where A's receipt is claimed
and B's handoff remains pending; the consumer receives the typed rejection
instead of admission. It then starts consumer attempt 1, completes B through
the durable graph transition path, verifies that B's terminal transition
produces its delivery receipt, and has the Coordinator claim B without
rewriting A. Completion requires both receipt claims to name the same
consumer node and attempt.

## Still open

- A full deterministic A/B parallel + C merge run through actual Team child
  admission remains needed for B3/A3; this increment proves the durable
  root-graph delivery/claim recovery chain, not provider/child-Team execution.
- P3--P6, all remaining B gates and the real-Qwen A12 gate remain open.
