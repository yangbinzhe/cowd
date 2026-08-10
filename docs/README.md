# Cowd Docs Boundary

This directory is reserved for active product documentation that is referenced by
runtime behavior, operator workflows, or release-facing user guidance.

Do not store version plans, implementation notes, agent work state, validation
transcripts, historical planning archives, or local runtime artifacts here.

Current retained sections:

- `api/` - generated API references and stable HTTP contract documentation.
- `architecture/` - active architecture, boundary, and module relationship documents.
- `operator/` - active operator runbooks and runtime readiness evidence.

Application architecture:

- `architecture/application-development-and-product-composition.md` - multi-App ownership, source locking, development/release modes, product composition, and acceptance rules.
- `architecture/app-activation-and-build.md` - current unified runtime enablement and build behavior for compiled Apps.
- `architecture/session-task-mission-governance.md` - canonical Session/Turn/Task/Mission ownership, routing, permission, persistence, and projection contracts.
- `architecture/session-execution-policy-and-authorization.md` - canonical Session policy, Agent capability ceiling, approval, grant, writer, and live revision boundaries.
- `architecture/evidence/task-mission-v652/` - v0.9.652 Task/Mission terminal implementation, storage, removal-scan, validation, and release-gate evidence.

Storage operations:

- `architecture/storage-governance.md` - V581 process-wide SQLite/PostgreSQL selection, shared pool and stable port boundaries, App-owned schemas and migration hooks, and the fail-closed `plan → migrate → verify → cutover` procedure.
- `architecture/runtime-performance-and-cache.md` - V626 Runtime hot path, Provider admission, PostgreSQL workload lanes, bounded Skill/Tool caches, MCP lifecycle, and verification boundaries.

Gateway operations:

- `operator/gateway-lifecycle.md` - safe Gateway start/stop/restart, binary replacement, authorization-state migration, and single-instance verification.
- `operator/session-permissions-and-approvals.md` - configure, inspect, change, and troubleshoot Session execution policies and approvals.

Gateway API references:

- `api/gateway-api-reference.md` - full Gateway API route inventory generated from `crates/gateway/src/api_routes/**/*.rs`.
- `architecture/gateway-api-framework.md` - Gateway API architecture, route family relationships, and major execution chains.
- `architecture/gateway-capability-contract-terminal-plan.md` - terminal plan for Gateway-owned capability contract.
- `architecture/gateway-capability-contract-terminal-evidence.md` - implementation evidence for Gateway capability contract closure.
- Gateway runtime source of truth: `GET /api/gateway/capability-contract`.
- Gateway derived machine-readable outputs: `GET /api/gateway/openapi.json` and `GET /api/gateway/openai-tools`.
- WebUI and TUI consume the Gateway capability contract for capability discovery; business APIs remain execution endpoints, not duplicated capability inventories.
