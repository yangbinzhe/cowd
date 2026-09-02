# v0.9.718 Integrated acceptance historical evidence

> Status: **superseded by `autonomous-objective-runtime-closure-repair-v0.9.719.md`.**
> This record describes the first integrated candidate and is retained for incident history. The
> subsequent real DeepSeek rerun is authoritative for the current acceptance result.

This is the final integration phase. Source-level gates from v0.9.716 and
v0.9.717 are prerequisites. The isolated full-product smoke validates Gateway,
Runtime, provider transport, session projection and frontend repository wiring;
real paid-model scenarios are only run when an explicit provider route and
scenario authorization are present.

Acceptance requires: clean build, mock-provider full-product smoke, replay and
terminal-fence regressions, then (when credentials are available) one isolated
DeepSeek scenario with live session/projection evidence and no unresolved
Team/Objective obligations.

Observed first real DeepSeek run: 3 Teams were admitted concurrently; two completed
and the synthesis Team remained in `calling_tool` until the harness was stopped. The root causes
were an unbounded OpenAI-compatible HTTP request and incorrect Objective graph-id projection.
Both were repaired in the candidate. The subsequent rerun completed 3 Teams/6 Agents, but the
overall report still failed its cache gates (cold 60.34%, structural 54.26%, warm 69.16%/64.22%).
Therefore this phase is not a final business-closure proof. The authoritative failed report is
`target/acceptance/real-qwen/runs/v0.9.718-1788346316-mission-harness-deep/report.md`.
