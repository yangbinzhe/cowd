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
- Runtime: 1997 passed, 0 failed, 3 ignored.
- Gateway: 813 passed, 0 failed, 13 ignored.
- Harness Contract: 205 passed, 0 failed.
- Harness Eval: 144 passed, 0 failed.
- Release-mode Runtime execution saturation: 64 independent work items passed
  through the canonical single-test runner in 0.21 seconds.
- Release-mode projection probes passed: steady paired mean 106,250 µs versus
  109,005 µs baseline (p95 66 versus 69, p99 171 versus 174), active catch-up
  mean 584,337 µs versus 573,985 µs baseline (p95 73 versus 70, p99 258 versus
  259), with every declared bound satisfied.
- Focused causal-terminal receipt, conflict rejection, root checkpoint,
  actionable-peer, duration-aware lease, orphan detection and Team-board market
  tests passed.
- Architecture boundary scan and backend `0.9.713` version gate passed.
- `cargo check -p runtime --all-targets` passed. `cargo clippy -p runtime --lib
  --no-deps` exited successfully with the repository's pre-existing warning
  baseline (large error/enum variants and existing style lints); no diagnostic
  was introduced by a changed line, no Clippy suppression was added, and no
  unrelated API rewrite was introduced.

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

## Candidate-3 failed-run evidence

- Report: `target/acceptance/real-qwen/runs/v0.9.713-1788236592-mission-harness-deep/report.json`
- Session: `da55b632-4c09-4e75-a6d9-6ce280ac4ac1`
- Candidate: `3976d58ed502b3fdd4cf30bf55f09390d7594378`
- Provider/model: only `deepseek-v4-flash`; no provider fallback was observed.
- Runtime cursor/revision at stall: `4875` / root Mission revision `66`.
- Team B completed 4/4 Agents. Team C completed 4/4 Agents and really executed
  proposal, bid, claim, workspace mutation, experiment and independent source
  review. Team A had one completed, one failed and two dependency-blocked
  Agents. Team D never became eligible because A did not close.
- Team A failure was exact and bounded: its designated Agent omitted the
  required proposal after three checkpoints, and Runtime converted that
  omission into a fatal Agent error rather than applying an identity-attested
  idempotent bootstrap default.
- Team C terminal facts prove two unresolved required market items: one was
  still `claimed`, one was still `offered`, while all four Agent nodes were
  terminal and Verify/Synthesize stayed Planned. The last reviewer explicitly
  reported that its continuation function-call contract was empty, so it could
  not bid or claim and did not simulate the mutation.
- The evaluator correctly declared inactivity after 480,169 ms without durable
  progress. It retained 422 actual progress observations and detected a stall;
  the test was not restarted.
- Observation cost remained unacceptable: 1,373,758,422 bytes received,
  including 1,347,169,507 root projection bytes across only 81 Summary probes.
  This proves activity reduction still consumed uncropped root/descendant graph
  summaries even though the final graph field itself was bounded.
- One malformed 21,559-byte OpenAI-compatible DeepSeek tool-call frame was
  rejected by Runtime and the remaining independent work continued. It is a
  recovered provider-transport incident, not the terminal cause.

The amended architecture and audited implementation sequence are recorded in
`docs/architecture/autonomous-collaboration-convergence-v0.9.713.md`. No further
paid run is permitted until those repairs have a clean deterministic commit.

## Candidate-4 failed-run evidence

- Report: `target/acceptance/real-qwen/runs/v0.9.713-1788241345-mission-harness-deep/report.json`
- Session: `69c4e0f4-a08a-4d95-8501-88f1c85fd302`
- Candidate: `fd42d60668c56ce6d8f126d6f0b11eeec8e71c64`, clean at launch.
- Provider/model: only `deepseek/deepseek-v4-flash`; 136 model rounds, 175
  native tool calls and 5,281,707 live canonical tokens; no fallback, auth,
  quota or overload failure was observed.
- The observer retained 747 changed records from 673,049,106 received bytes,
  kept a monotonic cursor, drained messages and timeline, detected no stall and
  passed observation integrity. The root payload remains the dominant traffic
  source and is retained as a separately measured optimization concern.
- Three Teams and twelve Agents materialized. Six first-wave Agents ran
  concurrently, dependency completion immediately activated successors, and
  the fourth Team correctly remained ineligible after its required handoff
  predecessors failed. Six Agents completed before the common failure chain.
- Exact root cause: both autonomous proposal producers embedded the full
  compositional graph id and Agent instance id in the idempotency key. Real
  identities exceeded the `TeamRuntime` 160-character contract, so a model
  omission followed by the governed Runtime default failed with
  `collaboration proposal idempotency_key must contain 1..160 characters`.
  Downstream Agents and Team D then blocked fail-closed as designed.
- Report gate: 17/19. Durable response, requested-model identity, typed output,
  persisted presentation, observer integrity and actor cleanup passed; terminal
  collaboration and the aggregate scenario capability gate failed.

The accepted repair is one domain-separated, length-delimited SHA-256 helper
shared by the model-visible mutation template and Runtime default request. It
produces a fixed-length key below the collaboration-control limit while
preserving stable replay identity and distinguishing every graph/Agent pair.
No second paid run is permitted until the long production-identity regression,
full Runtime/Gateway suites and a new clean candidate commit pass.

Candidate-4 correction deterministic evidence:

- Long production-identity regression: passed, including shared model/default
  key, bounded length, stable replay and graph/Agent identity separation.
- Runtime: 1997 passed, 0 failed, 3 ignored; all integration targets passed.
- Gateway: 813 passed, 0 failed, 13 ignored; both integration targets passed.
- Runtime all-target check, architecture boundary scan, formatting and diff
  checks: passed.
- Runtime library Clippy exited successfully with the recorded repository
  warning baseline; no changed line introduced a diagnostic or suppression.
- Backend version gate: passed for `0.9.713`.

The correction is now eligible for a clean candidate commit and one replacement
DeepSeek acceptance. The preceding failed run is retained as falsification
evidence and cannot be promoted as release success.

## Candidate-5 failed-run evidence

- Report: `target/acceptance/real-qwen/runs/v0.9.713-1788243369-mission-harness-deep/report.json`
- Session: `1e561a89-a8ed-46bf-b34f-eb26f47b4c85`
- Candidate: `a5f58fdb18eb7b2e77e2c4a5e2abd9c192e7f529`, clean at launch.
- Provider/model: only `deepseek/deepseek-v4-flash`; 108 model rounds,
  2,564,094 live canonical tokens and no provider fallback.
- Report gate: 17/19. All deterministic, evidence, observation, model-identity
  and packaging gates passed; only live collaboration completion and its
  aggregate capability gate failed.
- The fixed proposal identity was exercised successfully: A1/A2, B1/B2 and
  C1/C2 completed and dependency convergence activated the second wave. No
  idempotency boundary error occurred.
- A3, B3 and C3 then failed independently after provider admission waited
  30,001 ms and returned `DeadlineExpired`; their successors and Team D blocked
  fail-closed. Six completed Agents and persisted C1 artifacts were retained.
- At failure, provider global, DeepSeek account and Flash model adaptive limits
  had contracted to `8` with reserves `8`, `8` and `16`; the token pool was
  `256/256`. There were zero active leases, one queued waiter, 69,351 ms service
  p95 and 176,044 ms maximum. Thus the waiter had no non-interactive capacity
  and a deadline shorter than measured provider service.
- The root attempted two bounded recovery revisions and reported the terminal
  failure instead of claiming completion. Those revisions were rejected for a
  missing turn-scoped Team template and did not alter committed Team evidence.

The accepted provider-capacity correction is specified in the architecture
record. No replacement paid run is permitted until starvation-policy, token
floor and service-class deadline regressions plus full deterministic gates pass
on a new clean candidate commit.

Candidate-5 correction deterministic evidence:

- Provider starvation regression: default global, account, Flash model and
  token-pool resources were forced to their adaptive minima; four concurrent
  maximum-pressure Foreground bundles and a subsequent Interactive bundle were
  atomically granted.
- Invalid policies with `interactiveReserve >= minimum`, or with insufficient
  maximum headroom for one maximum token-pressure request, fail validation.
- Interactive admission remains 30 seconds; every delegated service class is
  covered by the bounded 300-second deadline regression.
- Runtime: 2000 passed, 0 failed, 3 ignored; all integration targets passed.
- Gateway: 813 passed, 0 failed, 13 ignored; both integration targets passed.
  The first run correctly rejected the installed legacy `8/8` and `4/8`
  provider policy; after migrating that active configuration to `12/8`, its
  three config consumers and the complete Gateway suite passed. One unrelated
  concurrent current-directory assertion also passed alone and in the full
  rerun.
- Harness Contract: 205 passed, 0 failed. Harness Eval: 144 passed, 0 failed.
- Runtime all-target check, architecture boundary scan, backend `0.9.713`
  version gate, formatting and diff checks: passed. Runtime Clippy exited zero
  with only its documented pre-existing warning baseline and no diagnostic on
  a changed line.
- Release execution saturation: 64/64 independent nodes overlapped and
  completed in 0.22 seconds.
- Release projection probes passed: steady paired mean 105,868 microseconds
  versus 109,602 baseline (p95 66 versus 70, p99 172 versus 176); active
  catch-up mean 587,813 microseconds versus 579,148 baseline (p95 74 versus 71,
  p99 262 versus 263). Every declared bound passed.

The correction is eligible for the integrated workspace compile gate, a clean
candidate commit and one replacement DeepSeek acceptance. The release status
remains pending until that immutable report passes all 19 gates.
