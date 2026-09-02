# v0.9.717 Agent autonomy / task-market historical evidence

Status: **superseded by `autonomous-objective-runtime-closure-repair-v0.9.719.md`.**
The original phase note was not a final acceptance record; its implementation and evidence state
must be interpreted together with the closure-repair candidate.

This phase removes production assumptions that a fixed fraction of Agents must
propose work. `CollaborationWorkProposal::initiative` lets an Agent express
objective-scoped dependencies, collaboration references, and requested Team
roles. Runtime authority still validates identity, capability, resource and
revision fences; it does not manufacture proposals or constrain their count.

Gates: workspace format/check, task-market regression, serialization roundtrip,
negative authority tests, and production scan for mandatory proposal fallback.
