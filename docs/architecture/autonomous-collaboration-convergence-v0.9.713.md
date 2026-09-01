# Autonomous Collaboration Convergence v0.9.713

## Decision

Autonomous Team work is a Runtime state-machine obligation, not a prompt-only
convention. A valid execution must satisfy five coupled invariants:

1. A successor receives one causally terminal `runtime_change` receipt per
   workspace path. Historical writes remain on the predecessor result but do
   not become impossible successor digest obligations. Divergent terminal
   writes fail closed.
2. `write:.` maps to the checkpoint subsystem's canonical whole-workspace
   representation (an omitted path list). A model-supplied path list cannot
   narrow or redirect this Runtime-owned guard.
3. A proposal is admitted only when another eligible Agent is actionable now:
   Ready, Running, or Planned with every dependency completed. A dependency-
   blocked Planned Agent is not market capacity.
4. Claim leases cover at least the work contract's declared duration, bounded
   by a 5-second minimum and 1-hour maximum. Expired claims advertise reclaim,
   never heartbeat or submit. The agent checkpoint carries the same rule.
5. Required autonomous work may block terminal reducers only while a Team Agent
   can still act. If every Agent is terminal, the reducer commits a typed
   `autonomous_work_orphaned` failure instead of leaving a forever-running
   graph.

The live evaluator uses Summary projections for revision polling. Full
projections remain available for one-shot failure diagnostics, but are not a
progress transport. Acceptance still consumes the public graph, work states,
bounded activities, delivery envelope and terminal evidence references.

## Ownership

- `AgentTaskExecutor` owns predecessor evidence normalization and dynamic
  successor obligations.
- `ScopedRuntimeToolExecutor` owns checkpoint authority compilation.
- `TeamRuntime` owns actionable market capacity, proposal admission, lease
  duration and owner-scoped control actions.
- `ExecutionGraphRunner` owns fail-closed orphan convergence.
- Harness Eval owns observation cost and acceptance polling policy; it cannot
  change Runtime state to make a scenario pass.

No model response, Team-board entry, UI node or evaluator heuristic may replace
these owners.

## Acceptance gates

- repeated writes select only the causally terminal digest;
- divergent final writes are rejected;
- bounded and root checkpoint scopes compile to their canonical forms;
- serial dependency successors are not advertised as active peers;
- a requested short lease expands to the declared task duration;
- unresolved work is not orphaned while any Agent is active and is orphaned
  after all Agents become terminal;
- Runtime, Gateway, Harness Contract and Harness Eval suites pass;
- one isolated DeepSeek `deepseek-v4-flash` 4-Team/16-Agent run completes all
  required work, reviews, challenges, discussions and the single materialized
  output without fallback-provider evidence;
- observed projection traffic is bounded and no longer repeats full lineage
  projections on every graph revision.

## Release rule

The real-provider run occurs once, only after deterministic gates and a clean
candidate commit. Installation, cache cleanup and the local annotated tag occur
only after that immutable report passes. No remote push is part of this phase.
