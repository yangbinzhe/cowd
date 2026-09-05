include!("tool.rs");
include!("attestation.rs");
include!("agentic_content_bridge.rs");
include!("agentic_protocol_state.rs");
// Retired one-shot collaboration admission tests intentionally are not
// included. Agent-first coverage lives with `agentic::{action_service,
// execution}` and exercises incremental actions and durable projections.
