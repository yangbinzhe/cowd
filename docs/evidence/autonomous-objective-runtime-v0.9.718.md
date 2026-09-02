# v0.9.718 Integrated acceptance evidence

This is the final integration phase. Source-level gates from v0.9.716 and
v0.9.717 are prerequisites. The isolated full-product smoke validates Gateway,
Runtime, provider transport, session projection and frontend repository wiring;
real paid-model scenarios are only run when an explicit provider route and
scenario authorization are present.

Acceptance requires: clean build, mock-provider full-product smoke, replay and
terminal-fence regressions, then (when credentials are available) one isolated
DeepSeek scenario with live session/projection evidence and no unresolved
Team/Objective obligations.

Observed real DeepSeek run: 3 Teams were admitted concurrently; two completed
and the synthesis Team remained in `calling_tool` until the harness was stopped
after the configured observation window. Root cause was an unbounded
OpenAI-compatible HTTP client request combined with a local Mission graph being
sent to the Objective projector. The former is fixed with a 180-second client
timeout; the latter now returns a typed no-op for graphs without a GoalContract.
Mock full-product smoke and 61 provider protocol tests pass. A paid rerun after
the timeout fix is intentionally not repeated in this source-only closure to
avoid duplicate spend; it is the next controlled acceptance action.
