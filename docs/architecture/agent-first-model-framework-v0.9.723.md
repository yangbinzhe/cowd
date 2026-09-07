# Agent-first PG-only framework v0.9.723

This release makes PostgreSQL the only durable database selected by Cowd's
production composition root and removes the remaining SQLite implementations,
dependencies, migration bridges, and runtime fallbacks from the core workspace.
Process-local stores remain explicit test adapters; missing production ports
fail closed.

The agentic control plane keeps one owner for Goal, Program, Task, Agent run,
evidence, and terminal decisions. Models receive the stable objective and
scoped evidence, choose organization and actions through compact typed
contracts, and may continue the same deterministic execution graph as new
facts arrive. The framework owns validation, resource admission, effects,
durability, recovery, idempotency, cancellation, and projection; it does not
replace model judgment with role-name rules or fixed team templates.

The canonical collaboration projection is schema v5. Team, membership, Agent,
Task, active run, history, wait reason, terminal result, and display identity
are projected from Runtime authority and consumed by Gateway, TUI, Edge, and
MFG without topology inference in the clients.

Prompt construction preserves a stable cacheable head and puts private and
turn-varying material after it. Cache reports distinguish cold, eligible warm,
and unknown tokens; cache economics never become a business-execution gate.

The external implementation authority and gate matrix are maintained at
`/media/yi/Datas/workspace/plan/cowd-pg-agentic-terminal-2026-09-07/`.
Release evidence is recorded in
`docs/evidence/agent-first-model-framework-v0.9.723.md` and the external
authority's `evidence/` tree.
