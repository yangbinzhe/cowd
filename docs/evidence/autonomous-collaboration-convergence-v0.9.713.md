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

A second immutable candidate run proved that those five repairs were necessary
but not sufficient:

- Report: `target/acceptance/real-qwen/runs/v0.9.713-1788230670-mission-harness-deep/report.json`
- Session: `6e3990fd-ff0d-43c9-a85f-3dac39291e53`
- Candidate: `029bbe2e5e39b65a8f8f73f806598062c5e3f509`
- Provider/model: only `deepseek/deepseek-v4-flash`; no fallback.
- Four Teams and sixteen Agents executed. A/B/C completed; D became partial
  only because `html-publisher` omitted terminal presentation fields.
- The publisher made 13 native tool calls, committed seven writes and left a
  56,262-byte final HTML on disk, yet presentation recovery converted that
  receipt-backed work to Blocked.
- Root final reread was rejected because the evaluator had not granted the
  exact output-file scope.
- Final gate was 17/19. Projection polling still received 914,338,588 bytes;
  884,349,891 bytes came from the root channel.
- Proposal/bid/review/challenge counts remained zero because Runtime had no
  market bootstrap obligation and rejected valid future roles in serial Teams.

## Candidate implementation

Changed dependency cone:

- Runtime Agent executor and in-process worker;
- Runtime Team market and graph runner;
- Runtime orchestration/in-process regression tests;
- Harness Eval live lineage observer;
- Harness Contract additive Agent change/read-back receipt fields;
- Runtime bounded Summary graph projection, receipt-backed terminal transport,
  topological market bootstrap, deterministic challenge policy and direct
  Agent materialization reduction;
- Harness Eval exact artifact lease and terminal Agent graph fallback;
- test-governance inventory and the canonical Runtime execution performance
  runner.

Deterministic evidence completed before the candidate commit:

- `cargo fmt --all --check` and `git diff --check`: passed.
- Runtime: 1993 passed, 0 failed, 3 ignored.
- Gateway: 813 passed, 0 failed, 13 ignored.
- Harness Contract: 205 passed, 0 failed.
- Harness Eval: 144 passed, 0 failed.
- Release-mode Runtime execution saturation: 64 independent work items passed
  through the canonical single-test runner in 0.21 seconds.
- Release-mode projection probes passed: steady paired mean 105,926 µs versus
  108,188 µs baseline, active catch-up mean 582,517 µs versus 571,695 µs
  baseline, with p95/p99 inside the declared bounds.
- Focused causal-terminal receipt, conflict rejection, root checkpoint,
  actionable-peer, duration-aware lease, orphan detection and Team-board market
  tests passed.
- Architecture boundary scan and backend `0.9.713` version gate passed.
- Strict workspace-wide Clippy remains blocked by pre-existing Provider and
  contract baseline lints (large error/enum variants and existing style
  lints). A no-dependency changed-file audit found and removed the new
  topological `expect` and recovery readability warning; no Clippy suppression
  or unrelated API rewrite was introduced.

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
