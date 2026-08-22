# P2 cross-Team edge increment — 2026-08-23

This record covers one P2 increment only. It is not evidence that P2, the
0821 program, or the release gates are complete.

## Durable chain added

- `CollaborationProgramEdge` now carries a producer delivery receipt and a
  consumer claim receipt, both fenced by the physical graph node attempt.
- A producer's terminal graph transition atomically derives every outgoing
  pending handoff: a contract-satisfying terminal result becomes `Delivered`;
  a missing result or missing typed fact becomes `Blocked` without breaking
  the producer's own terminal transition.
- The Team subgraph executor reconciles legacy/restarted pending deliveries,
  then claims all delivered incoming receipts **before** `admit_or_resume` can
  start the child Team. Multi-edge fan-in is not bounded by the CAS retry
  limit.
- Semantic Team dependencies compile to an `EvidenceReady` policy and the
  matching generated handoff contract requires observed evidence and an
  acceptance verdict. Independent Team workstreams retain no dependency.

## Verification

```text
cargo test -p harness-contract --lib
# 185 passed

cargo test -p runtime --lib -q
# 1794 passed, 0 failed, 2 ignored

cargo check -p runtime --all-targets
git diff --check
```

Targeted coverage includes producer-to-delivery-to-consumer-claim attempt
fencing and the terminal missing-fact `Blocked` path. The runtime-wide gate
also exercises evidence-ready waiting/blocking, failed-fact review and Team
admission/recovery regressions.

## Still open

- P2 still lacks the full A3/A9c deterministic multi-Team scenario and the
  wider acceptance-writer ownership audit.
- P3 dynamic edge operations, ephemeral templates and escalation remain open.
- P4--P6, all B gates and the real-Qwen A12 gate remain open.
