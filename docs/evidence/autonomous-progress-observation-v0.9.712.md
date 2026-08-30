# Autonomous progress observation — v0.9.712 candidate evidence

Status: deterministic implementation closure passed

Date: 2026-08-31

Approved plan SHA-256:
`4a731c32826e1092320be71316abc6c03b51629aea3fca214003a36933a04f17`.

## Root cause

The binding DeepSeek six-Team trace contained 6,439 polling records and was
49.7 MB, but the small number of durable graph snapshots was not evidence that
the Runtime repeated or stopped working. The root execution projection had 297
distinct response states, 295 distinct live revisions and 265 distinct output
byte counts while model and tool work continued. The actual business execution
completed 28 provider rounds and 97 tool calls across six Teams without a
duplicate business dispatch.

The waste and observability gap were in Harness Eval:

- 2,558 of 2,559 message responses replayed the same original user message,
  serializing 12,727,739 bytes of repeated response bodies.
- Timeline polling repeatedly requested the first fixed 500 events. The final
  response declared 628 events, `has_more=true`, and a v2 continuation cursor,
  so 128 events were not collected by the evaluator.
- Full root projections were polled at the high-frequency loop cadence even
  when only the lightweight live state changed.
- Timeout evidence exposed a generic fingerprint, not the active model, tool,
  handoff or finalization phase.

The framework's execution authority was sound; the evaluator's observation
protocol was incomplete and needlessly expensive.

A first real-provider smoke then exposed a separate semantic-verdict defect.
Runtime correctly rejected an out-of-scope `Cargo.toml` read and durably
recorded a failed/partial business outcome with zero tool calls. Harness still
reported success because it treated graph lifecycle closure as business
success and recursively matched provider capability metadata
`tool_calls=supported/configured` as executed tool evidence. That run is
retained as a negative regression artifact, not accepted as proof.

## Implemented contract

`LiveScenarioObserver` is now the single scenario-local owner for evaluation
cursors and compact evidence. Runtime, Session and Gateway remain the canonical
business-state owners.

- Session messages are consumed through `from_seq`/`next_seq` deltas.
- Runtime Timeline is drained through the existing composite v2 cursor until
  `has_more=false`, with a bounded 64-page fail-closed guard.
- Cursor values must advance semantically and monotonically. Cycles, regressions,
  malformed cursors, empty `has_more` pages and non-advancing message sequences
  fail the scenario.
- Stable message and Timeline identities are retained once; identity reuse with
  a different payload fails instead of silently hiding a changed fact.
- Every changed root or lightweight live revision is retained. Consecutive
  unchanged and transient-error observations are coalesced into spans carrying
  first/last elapsed time, poll count and received bytes.
- A transient observation error remains retryable, but the final report cannot
  pass until both durable streams have a later successful drain.
- Terminal completion performs a final message and Timeline drain before the
  observation window closes.
- Progress is classified as bootstrapping, preparing, calling model, calling
  tool, waiting handoff, finalizing, terminal pending, terminal, quiet or
  stalled. Timeouts report the current phase, last active phase and both
  cursors.
- Deep live report gates require monotonic cursors, fully drained messages and
  Timeline, zero omitted changes and no detected stall.
- Every live scenario verdict is bound to the root
  `runtime.outcome.recorded.v1` event. Both its event status and terminal class
  must be `succeeded`; failed, partial and missing outcomes fail closed.
- A tool scenario passes only with a complete successful
  `tool.invocation.completed` Runtime receipt bound to the expected operation
  and exact resource target. Provider capability metadata, unrelated tools,
  wrong-target reads, prose claims and usage counters cannot substitute for
  the required executed effect.
- The tool fixture receives the exact `read:Cargo.toml` resource lease it
  requires; no workspace-wide read or weakened Runtime policy was introduced.

No Runtime scheduling, Provider routing, Session persistence, Gateway API or
Edge production authority changed.

## Deterministic acceptance

- Harness Eval: 143 tests passed, including 10 observer adversarial/replay tests
  and semantic-verdict cases for success, failure, partial outcome, missing
  outcome, failed tool, zero count and provider-metadata false evidence.
- Runtime: 1,932 passed, zero failed, two existing ignored tests.
- Provider: 172 passed, zero failed.
- Gateway message pagination and composite Timeline cursor contract tests passed.
- `cargo check --workspace --all-targets` passed.
- `cargo xtask architecture audit` passed: 112 Runtime modules, 482 routes,
  53 tools, 115 Edge capabilities, 43 state authorities, zero legacy owners,
  zero duplicate authorities and zero duplicate-capability candidates.
- The repository `full-regression` lane passed deterministically with one test
  thread. Two earlier parallel attempts exposed unrelated pre-existing timing
  flakes in a Runtime heartbeat lease test and a TUI microbenchmark; each passed
  targeted repetition and neither touches the changed package.
- The repository `all` lane and the changed-crate lane passed.

The live provider report is intentionally external to the repository and must
bind a clean immutable candidate, the selected DeepSeek route, the exact paid
scenario and its explicit token lease.
