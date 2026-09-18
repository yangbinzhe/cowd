# Agent-first PG-only framework v0.9.725

Cowd v0.9.725 keeps PostgreSQL as the sole production durable-store topology.
The composition root requires a PostgreSQL secret reference, performs an
explicit schema upgrade, and fails closed when the configured store is absent
or unavailable. Process-local stores remain test adapters only.

Runtime remains the sole owner of Goal, Program, Task, Agent run, evidence,
and terminal decisions. Gateway, TUI, Edge, MFG, and connector surfaces consume
the Runtime projection and do not infer collaboration topology independently.

The release validation scenarios use an isolated PostgreSQL namespace and
launch their temporary Gateway process directly. This makes their process
environment explicit and avoids coupling product validation to tmux server
state or a host-specific temporary-directory policy.

The implementation plan and gate matrix remain under
`/media/yi/Datas/workspace/plan/2026-09/cowd-pg-agentic-terminal-2026-09-07/`.
Release evidence is recorded in
`docs/evidence/agent-first-model-framework-v0.9.725.md`.
