# Framework Core Hardening v0.9.708 Evidence

Release status: passed

Authority: `docs/architecture/framework-core-hardening-v0.9.708.md`.
Baseline: `4fa74bc45b6f81e3a9f9fb74f0e9779038530bff` (`v0.9.707`).

This document is the release evidence owner for the five v0.9.708 framework
closures. Every required implementation, performance, and regression gate has
passed. Release ref synchronization and remote verification are recorded by the
post-commit version gate because those refs cannot exist inside their own
release commit.

## Closure ledger

| ID | New authority | Residual deletion | Verification |
| --- | --- | --- | --- |
| H1 | Runtime `SessionTurnAdmissionPort` | Gateway Session bridge has no resource-manager/lease/observation types | Targeted tests and boundary gate passed |
| H2 | Capacity-bounded `GatewayRuntimeTaskSet` and bounded completion queue | No unbounded completion channel or unlimited admission | Saturation, reap, burst, and shutdown tests passed |
| H3 | Endpoint-specific semantic live-health contracts | No HTTP-success-only health verdict | Degraded fixtures rejected; real DeepSeek Flash health passed |
| H4 | Explicit config-home discovery and deterministic source fixture | No ambient health discovery or repository-dependent unit fixture | Parallel, serial-global, and long-path tests passed |
| H5 | Version-indexed authority/evidence and fail-closed governance gate | Historical documents explicitly marked non-current | Version/evidence/governance gates passed |

## Deterministic focused evidence

- Runtime Session-turn port: 2 passed, 0 failed.
- Gateway task set: 13 passed, 0 failed.
- Harness semantic health fixtures: 3 passed, 0 failed.
- Runtime deterministic semantic orchestration fixture: 1 passed, 0 failed.
- Gateway scoped process discovery: 1 passed, 0 failed.
- Gateway readiness repeated three times: 3 passed, 0 failed.
- Gateway CLI global-state guards: 2 passed, 0 failed.
- Architecture boundary script: passed, including the new Session admission boundary.
- Workspace all-target check: passed.
- The final post-scheduler-fix full regression passed all workspace targets, 10
  isolated process-global Gateway tests, and the signed reference
  Bundle/Gateway proxy chain. Runtime's main library reported 1,881 passed, 0
  failed, and 2 explicitly classified ignored live-contract tests.
- Scenario lane: all six steps passed (AI Harness, Session, Memory, Tool,
  Permission, and Skill/Surface paths).
- Surface lane: all five steps passed (minimal CLI, full build, TUI attach,
  WebUI contract, and reference Bundle).
- Runtime projection performance passed three consecutive repetitions for both
  no-backlog and active catch-up. No-backlog mean overhead ranged from -0.69%
  to +1.80%; active catch-up overhead ranged from +0.35% to +1.05%. Projected
  p95 stayed within 66-70us and projected p99 within 169-242us.
- Reference application performance passed three consecutive paired runs.
  Catalog-100 p95 was 38.307-38.677ms; cold p95 15.866-16.109ms; Gateway/direct
  throughput ratio was 90.40%-101.77% against the 85% floor; direct/Gateway
  hot p95 ranges were 357-372us/388-436us; cancellation was 156-401us.

## DeepSeek Flash diagnosis and external evaluation

The diagnostic run first proved the ordinary collaborative path: three Teams,
seven of seven Agents, two cross-Team edges, 13 live model rounds, 50 task tool
calls, and 582,873 live tokens completed on `deepseek-v4-flash` only. The same
run exposed a framework policy conflict for the explicit four-Team group-theory
task before Team execution began.

Root cause was not provider quality or account balance. The observed-work
estimator compared total graph tokens with the largest single-node token count.
That ratio grows approximately with graph cardinality, so the default 3x
automatic-optimization threshold mechanically vetoed four similar Team nodes.
The strategy layer had already accepted the user's exact Team cardinality,
creating two authorities with contradictory semantics.

The generic repair adds `user_mandated_topology` to the model-work estimate.
Observed cost can still downgrade automatically selected parallelism, while an
explicit Team-count contract is retained with a cost warning. Provider, Agent,
tool, and other hard capacity limits remain fail-closed. A deterministic test
proves that an automatic four-lane proposal is downgraded but an explicit
four-lane proposal remains pipelined.

The live post-fix run
`v0.9.708-1787933568-mission-harness-deep` passed in an isolated Gateway on
`deepseek-v4-flash` without fallback. Its collaboration source candidate tree
was `9d37f9ffa93c712d79c693d0213684d06f50b2ea`; the subsequent release delta is
limited to the maintenance-pass scheduler protection, paired performance
contract, and release evidence described below:

- exactly four Teams and four Agents completed, with zero Team/Agent failures;
- three required cross-Team dependencies were observed;
- Team A covered mathematical/method review, Team B application survey, Team C
  experiment/evaluation, and dependent Team D synthesis/risk review;
- architecture quality scored 3/3: canonical program projection, durable
  projection lineage, and checked source receipts;
- 9 live model rounds, 25 task tool calls, 335,774 live tokens, and 343.367s
  live wall time were recorded;
- the complete deep report recorded 366,318 tokens and passed all 19 report
  gates, all five complex scenarios, and all seven next-generation closure
  scenarios;
- all Gateway, Runtime, outbox, control-plane, evolution, and Surface Host
  endpoint health contracts passed semantically; no approval backlog or
  recovery-required state remained.

The live evaluation also produced two additional generic repairs before the
final pass: Agent and Team delivery now share one custom-artifact parser and
evidence-eligibility policy, failed audited tool artifacts never count as
successful evidence, and the evaluation execution key includes the selected
scenario set plus the group-theory flag so cached evidence cannot cross
different task contracts.

The final-tree performance rerun also caught a 2.54% foreground throughput
regression while the durable evolution projector was actively catching up.
The projector was functionally bounded, but a post-pass-only 100ms yield still
let the first commit-triggered maintenance scan race the foreground burst. The
generic scheduler repair debounces every maintenance pass, including the first,
for 200ms. With the existing 128-commit batch it retains at least 640 commits/s
catch-up capacity while protecting foreground latency.

The Reference application gate then exposed order-sensitive one-shot evidence:
one 500-request direct block happened to run unusually fast, making a stable
Gateway wall time fail the relative throughput gate. The contract now measures
six alternating direct/Gateway rounds with 250 requests per round (1,500 per
path) and attributes wall time, latency, and CPU to each paired block. This
preserves every original threshold while removing fixed-order and small-sample
bias; all three strict repetitions passed.

## Final gate results

Passed. The release commit is the commit containing this evidence document;
annotated tag `v0.9.708` must resolve to it. The post-commit version gate verifies
identical `master`/`dev` refs, both configured remotes, and clean worktrees.
