# v0.9.717 Agent autonomy / task-market evidence

Status: source implementation in progress; paid provider and browser E2E are
reserved for v0.9.718.

This phase removes production assumptions that a fixed fraction of Agents must
propose work. `CollaborationWorkProposal::initiative` lets an Agent express
objective-scoped dependencies, collaboration references, and requested Team
roles. Runtime authority still validates identity, capability, resource and
revision fences; it does not manufacture proposals or constrain their count.

Gates: workspace format/check, task-market regression, serialization roundtrip,
negative authority tests, and production scan for mandatory proposal fallback.
