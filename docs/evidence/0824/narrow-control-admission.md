# 0824 narrow collaboration-control admission evidence

Phase: P1 — semantic root-admission codec

## Scope

- Added `submit_collaboration_decision`, a narrow root-only native tool.
- Added provider-neutral typed workstream decision contracts.
- Kept `runtime_orchestrate` for inspect/revise/control; it is no longer the root admission protocol.
- Enforced the exact user-declared Team cardinality against the bound turn strategy.

## Deterministic evidence

- `cargo fmt --check`
- `cargo check -p runtime --all-targets`
- `cargo check -p gateway --all-targets`
- `cargo test -p harness-contract orchestration::tests::narrow_collaboration_decision_converts_without_runtime_owned_fields --no-fail-fast`
- `cargo test -p runtime capability_receipt_advances_root_control_plane_to_proposal_only --lib --no-fail-fast`
- `cargo test -p runtime only_a_successful_team_proposal_satisfies_root_control_plane_action --lib --no-fail-fast`
- `cargo test -p runtime explicit_team_requirement_rejects_collapsed_or_extra_workstreams --lib --no-fail-fast`
- `cargo test -p gateway runtime_capability_tool_is_always_registered_as_readonly --lib --no-fail-fast`

## Controlled provider probe

One non-streaming, non-thinking request was sent to the configured Token Plan
route with the exact narrow native-function schema and named function choice.
The response selected `submit_collaboration_decision` and carried two valid
workstreams. No full collaboration scenario was run for this probe.

This proves the configured route can return the narrow control transport. It
does not by itself prove Program admission, Team execution, recovery, or UI
projection. Those are separately gated by the isolated Gateway end-to-end
scenario after this immutable candidate is committed.

## Residuals deliberately not hidden

- P2 capability-probe caching / codec ladder and P3 SQLite single-writer
  remediation are not completed by this P1 change.
- No tag, push, version bump, or production deployment has been performed.
