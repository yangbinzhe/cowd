# Capability-conserving modular governance (v0.9.711)

This release is governed by the approved and audited plan at
`../plan/0830-cowd-v0.9.711-capability-conserving-modular-governance` and the
phase evidence `docs/evidence/capability-modular-governance-v0.9.711-p0.md`
through `p6.md`, plus the final release evidence
`docs/evidence/capability-modular-governance-v0.9.711.md`.

The architectural outcome is capability conservation with fewer authorities:

- one generated Gateway route/OpenAPI authority and immutable cached projections;
- one atomic active-Session aggregate instead of parallel lifecycle maps;
- separated Runtime turn/provider/context/tool/evidence/terminal planes without
  replacing the `ConversationRuntime` execution authority;
- shared persistence-domain semantics with SQLite and PostgreSQL retained as
  independent storage adapters;
- explicit TUI state slices and typed Gateway effects instead of implicit state
  penetration;
- typed Provider failure scope, turn-local account fencing, independent-account
  fallback, deterministic account/configuration terminals, and a shared paid-eval
  token lease.

The release does not add a native Bailian generation route. Qwen generation is
accepted only through the configured `qwen-tokenplan` provider; DeepSeek is
accepted only through the configured `deepseek` route. Embedding configuration
is outside this generation restriction.

Release invariants are executable: architecture inventory, source and structural
limits, duplicate authority/capability scans, capability reverse-diff, full
workspace regression, dual-backend conformance, Edge acceptance, deterministic
TUI PTY, paired performance, candidate provenance, and bounded live scenarios.
The external plan reports are the authority for live-provider pass/fail and must
match the final clean candidate commit, source archive, route and binary.
