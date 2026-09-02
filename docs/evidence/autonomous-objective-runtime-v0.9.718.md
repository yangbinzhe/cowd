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
