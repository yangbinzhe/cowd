# P3 Team-template selection increment — 2026-08-23

This record covers the removal of one legacy selection path only. It is not
evidence that P3, the 0821 program, or any B/A release gate is complete.

## Change

`TeamInstantiation::resolve_template` no longer maps `Automatic` to a builtin
template by inspecting network scopes or workspace-write permission. That was
static runtime selection truth: the same request could silently acquire a
different Team family based on hard-coded branches.

The path now fails closed. A Team request must carry either a Coordinator-bound
published catalog selector (`Exact`, `LatestStable`, or `Default`) or a
validated `EphemeralTeamTemplateSnapshot`. The normal semantic compiler already
emits the former and the custom/escalation path emits the latter.

`builtin_team_template_summaries` remains an informational capability manifest
for builtin initialization/advertising; it is not called by Team instantiation
or used to make a production selection.

## Verification

```text
cargo test -p runtime --lib -q
# 1797 passed, 0 failed, 2 ignored

git diff --check
```

## Still open

- Catalog conformance must still prove published and ephemeral candidates
  end-to-end, including random non-builtin roles and display names.
- Dynamic patch ingress and the remaining P3 operations are still incomplete.
- P4--P6, all B gates and the real-Qwen A12 gate remain open.
