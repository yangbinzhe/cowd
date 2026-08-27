# v0.9.707 Governed Experience Reuse Evidence

## Phase board

| Gate | State | Evidence |
| --- | --- | --- |
| Episode and signature contract | Passed (deterministic) | Starts only after both v0.9.706 tags and clean worktrees; core tag `7202ae1b`, Edge tag `eb60d5f`; terminal event uses a deterministic Runtime transaction id, salted opaque Session/Turn hashes, delivery/presentation agreement and durable evidence gating |
| Durable projector and pattern aggregation | Passed (deterministic) | Replays terminal episode events only; three distinct eligible turns produce an advisory-only pattern event, fenced by signature stream revision |
| Advisory precedence and governed promotion | Passed (deterministic) | Pattern contains no executable subject; versioned `PublishedRevision` / verified `EpisodeSet` baseline has legacy recovery mapping and fails closed when the frozen set is not backed by a durable advisory pattern |
| Real-provider, real-browser and multi-Team acceptance | In progress | Two isolated real-provider runs completed; direct/tool/single-architecture checks passed, while explicit Team admission was not submitted by the provider. A source-level control-plane repair is being revalidated on a new immutable candidate. |

## Version boundary

The v0.9.706 worktrees were clean after independent commits and annotated local
tags `v0.9.706` before this phase began. No v0.9.706 source is modified by this
record. This phase has not claimed provider or browser terminal acceptance.

## Implemented, not yet closed

- Episode event kind: `evolution.collaboration_experience.recorded.v1`, keyed by
  deterministic Program id/revision and committed through the Runtime event
  transaction API. Startup reconciliation invokes the same writer.
- Pattern event kind: `evolution.collaboration_pattern.projected.v1`; it is
  built exclusively from eligible episode events and carries only bounded,
  name-free structural advice and opaque evidence identifiers.
- Gateway's existing Evolution overview now exposes patterns as
  `advisory_only: true`; it does not advertise them as an active Definition.
- Focused contract test passed: `cargo test -p harness-contract evolution --lib
  --quiet` (6 passed). Runtime governance test passed: `cargo test -p runtime
  evolution::governance --lib --quiet` (17 passed).
- Runtime replay tests passed: terminal Program duplicate reconciliation plus
  startup replay emits one episode, and three distinct eligible turns project
  one advisory pattern without duplicate revision. These are focused unit
  proofs, not a claim of real Provider or browser acceptance.
- `cargo check --workspace --all-targets` passed. Full relevant Rust libraries
  passed: Gateway 808 tests (2 intentionally ignored), TUI 1080 tests and
  harness-eval 95 tests. The Gateway capability-contract count was updated for
  the new WebUI route, and `scripts/scenarios/openapi-generation.sh check`
  passed against its isolated source Gateway.
- Runtime/Gateway all-target checks and WebUI production build passed. WebUI
  full suite passed: 439 unit tests plus i18n, governance, API-matrix,
  presentation, capability-parity, raw-payload, secondary-section and
  acceptance gates. OpenAPI isolation and broader end-to-end gates remain in
  progress.

## Allowlist amendment

- `crates/runtime/src/execution_core/services.rs` is added solely as the
  existing Runtime-only registration caller for `EvolutionCandidateIntent` and
  startup reconciliation. This reconnects the versioned evaluation baseline
  and terminal episode recovery to their current owner; it creates no new
  registry, scheduler, release pointer, or Gateway writer.
- `crates/runtime/src/lib.rs` is added only to re-export the existing public
  Runtime governance contract to Gateway. It does not add a writer or state.
- `crates/gateway/src/api_routes/{capability_contract,route_manifest,mod}.rs`
  are added solely for the existing Gateway route declaration, WebUI consumer
  manifest, and route-level read-only endpoint test. They introduce no Gateway
  state or mutation owner.
- `crates/tui/src/components/gateway_panel.rs` is added only to project the
  existing read-only Evolution overview's advisory-pattern count in the TUI;
  it cannot select or activate a pattern.
- `crates/runtime/src/conversation/host.rs` is added after the real-provider
  evidence exposed a generic admission gap: an explicit Team requirement now
  forces the already-active native `submit_collaboration_decision` schema on
  its first provider request, and its Runtime-owned micro-instruction includes
  the mandatory `team.team_key` field discovered by the rejected real request.
  This preserves user-authored Team semantics and creates no Runtime-synthesized
  Team, scheduler, template, or release owner.
