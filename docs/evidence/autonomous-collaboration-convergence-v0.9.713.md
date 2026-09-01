# Autonomous Collaboration Convergence v0.9.713 Evidence

Release status: pending

## Failed-run evidence frozen before repair

- Report: `target/acceptance/real-qwen/runs/v0.9.713-1788226219-mission-harness-deep/report.json`
- Scenario: `live-scenarios/001-live_autonomous_collaboration_deepseek.json`
- Terminal: no durable execution progress for 480347 ms after 2659045 ms.
- Gate result: 16/19; the live scenario, provider-round and deep-live gates
  failed because the Team lineage never reached a terminal.
- Observation cost: 1,012,948,139 received bytes; 987,867,788 bytes came from
  repeatedly retrieving revision-changing root/lineage projections.

The trace proves five independent framework causes: stale intermediate write
digests were promoted as successor obligations; `write:.` became an unsafe
literal checkpoint path; dependency-blocked Planned Agents were counted as
active proposal capacity; the 300-second lease contradicted a 900-second work
contract and advertised invalid post-expiry actions; unresolved required work
had no fail-closed terminal transition after every Agent exited.

## Candidate implementation

Changed dependency cone:

- Runtime Agent executor and in-process worker;
- Runtime Team market and graph runner;
- Runtime orchestration/in-process regression tests;
- Harness Eval live lineage observer;
- test-governance inventory and the canonical Runtime execution performance
  runner.

Deterministic evidence completed before the candidate commit:

- `cargo fmt --all --check` and `git diff --check`: passed.
- Runtime: 1988 passed, 0 failed, 3 ignored; all package integration binaries
  passed.
- Gateway: 813 passed, 0 failed, 13 ignored.
- Harness Contract: 205 passed, 0 failed.
- Harness Eval: 143 passed, 0 failed.
- Release-mode Runtime execution saturation: 64 independent work items passed
  through the canonical single-test runner in 0.19 seconds.
- Focused causal-terminal receipt, conflict rejection, root checkpoint,
  actionable-peer, duration-aware lease, orphan detection and Team-board market
  tests passed.
- Architecture boundary scan passed. The first quick-gate execution correctly
  rejected stale `0.9.711` governance pointers and one unreachable ignored
  performance test; the ignored test now has the canonical
  `scripts/test/runtime-execution-performance.sh` runner. Version pointers are
  intentionally closed only with final evidence, not backfilled as a false
  release pass.

## Residual gates

- commit the clean deterministic candidate; the pre-commit backend version
  gate already passed for `0.9.713`;
- execute exactly one final DeepSeek 16-Agent isolated acceptance;
- audit provider identity, concurrency, Team/Agent/work/review counts,
  materialization and observation bytes;
- set this release status to passed only if every final gate succeeds;
- run final governance/version/install gates, clean local build/install caches,
  install `0.9.713`, and create the local annotated `v0.9.713` tag in both
  repositories without pushing.
