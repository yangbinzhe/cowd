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

Gateway API references:

- `api/gateway-api-reference.md` - full Gateway API route inventory generated from `crates/gateway/src/api_routes/**/*.rs`.
- `architecture/gateway-api-framework.md` - Gateway API architecture, route family relationships, and major execution chains.
- `architecture/gateway-capability-contract-terminal-plan.md` - terminal plan for Gateway-owned capability contract.
- `architecture/gateway-capability-contract-terminal-evidence.md` - implementation evidence for Gateway capability contract closure.
- Gateway runtime source of truth: `GET /api/gateway/capability-contract`.
- Gateway derived machine-readable outputs: `GET /api/gateway/openapi.json` and `GET /api/gateway/openai-tools`.
- WebUI and TUI consume the Gateway capability contract for capability discovery; business APIs remain execution endpoints, not duplicated capability inventories.
