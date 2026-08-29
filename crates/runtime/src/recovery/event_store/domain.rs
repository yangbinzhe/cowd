//! Backend-independent Runtime event validation and hashing semantics.

use super::*;

pub fn validate_decision_lease_claims(
    lease_id: &str,
    principal_id: &str,
    review_id: &str,
    action: &str,
    scope: &str,
    evidence_digest: &str,
) -> RuntimeEventStoreResult<()> {
    if lease_id.trim().is_empty()
        || principal_id.trim().is_empty()
        || review_id.trim().is_empty()
        || action.trim().is_empty()
        || scope.trim().is_empty()
        || evidence_digest.trim().is_empty()
    {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "decision lease consumption requires non-empty bound claims".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_fenced_terminal(terminal: &SessionTerminalInput) -> RuntimeEventStoreResult<()> {
    let required = [
        terminal.terminal_id.as_str(),
        terminal.message_id.as_str(),
        terminal.session_id.as_str(),
        terminal.payload_ref.as_str(),
        terminal.execution_id.as_deref().unwrap_or_default(),
        terminal.turn_id.as_deref().unwrap_or_default(),
        terminal.request_id.as_deref().unwrap_or_default(),
        terminal.input_claim_owner.as_deref().unwrap_or_default(),
        terminal.input_claim_token.as_deref().unwrap_or_default(),
    ];
    if required.iter().any(|value| value.trim().is_empty())
        || terminal
            .session_generation
            .is_none_or(|generation| generation == 0)
        || terminal.input_sequence.is_none()
        || terminal
            .input_claim_revision
            .is_none_or(|revision| revision == 0)
    {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "terminal transaction requires complete execution, turn and Session claim fences"
                .to_string(),
        ));
    }
    Ok(())
}

pub fn validate_transaction(request: &AppendTransactionRequest) -> RuntimeEventStoreResult<()> {
    if request.transaction_id.trim().is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "transaction_id must not be empty".to_string(),
        ));
    }
    if request.events.is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "events must not be empty".to_string(),
        ));
    }
    if request.events.len() > MAX_TRANSACTION_EVENTS {
        return Err(RuntimeEventStoreError::InvalidTransaction(format!(
            "event count exceeds hard limit {MAX_TRANSACTION_EVENTS}"
        )));
    }
    let bytes = serde_json::to_vec(request)?.len();
    if bytes > MAX_TRANSACTION_BYTES {
        return Err(RuntimeEventStoreError::InvalidTransaction(format!(
            "serialized transaction exceeds hard limit {MAX_TRANSACTION_BYTES} bytes"
        )));
    }
    let mut expected = BTreeSet::new();
    for stream in &request.expected_streams {
        if stream.stream_id.trim().is_empty() || !expected.insert(stream.stream_id.as_str()) {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "expected streams must be non-empty and unique".to_string(),
            ));
        }
    }
    for event in &request.events {
        validate_event(&event.event)?;
        if event.schema_version == 0 {
            return Err(RuntimeEventStoreError::InvalidTransaction(
                "event schema_version must be positive".to_string(),
            ));
        }
        if !expected.contains(event.event.stream_id.as_str()) {
            return Err(RuntimeEventStoreError::InvalidTransaction(format!(
                "event stream `{}` has no expected revision",
                event.event.stream_id
            )));
        }
    }
    Ok(())
}

pub fn validate_event(input: &RuntimeEventInput) -> RuntimeEventStoreResult<()> {
    if input.stream_id.trim().is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "event stream_id must not be empty".to_string(),
        ));
    }
    if input.kind.trim().is_empty() {
        return Err(RuntimeEventStoreError::InvalidTransaction(
            "event kind must not be empty".to_string(),
        ));
    }
    if requires_activity_binding(input) {
        let binding = input.activity_binding().ok_or_else(|| {
            RuntimeEventStoreError::InvalidTransaction(format!(
                "business lifecycle event `{}` requires RuntimeActivityBinding",
                input.kind
            ))
        })?;
        validate_required_activity_identity(input.scope, &input.kind, &binding)?;
    }
    Ok(())
}

fn requires_activity_binding(input: &RuntimeEventInput) -> bool {
    match input.scope {
        RuntimeEventScope::Tool => input.kind.starts_with("tool.invocation."),
        RuntimeEventScope::Skill => input.kind == "skill.activation.selected",
        RuntimeEventScope::Agent => is_agent_activity_event(&input.kind),
        RuntimeEventScope::Team => {
            input.kind.starts_with("team.lifecycle.") || input.kind.starts_with("team.execution.")
        }
        RuntimeEventScope::Session => {
            input.kind == "model.item_completed"
                && input
                    .payload
                    .get("kind")
                    .and_then(serde_json::Value::as_str)
                    == Some("public_reasoning")
        }
        _ => false,
    }
}

fn is_agent_activity_event(kind: &str) -> bool {
    matches!(
        kind,
        "agent.prepared"
            | "agent.running"
            | "agent.terminal"
            | "agent.cancelled"
            | "agent.blocked"
            | "agent.blocked_recovery"
            | "agent.command"
            | "agent.command_rejected"
            | "agent.recovered"
            | "agent.execution.started"
            | "agent.provider.first_output"
            | "agent.acceptance.evaluated"
    )
}

fn validate_required_activity_identity(
    scope: RuntimeEventScope,
    kind: &str,
    binding: &harness_contract::projection::RuntimeActivityBinding,
) -> RuntimeEventStoreResult<()> {
    let mut missing = Vec::new();
    let mut require = |name: &'static str, value: Option<&str>| {
        if value.is_none_or(str::is_empty) {
            missing.push(name);
        }
    };
    match scope {
        RuntimeEventScope::Team => {
            require("node_id", binding.node_id.as_deref());
            require("parent_activity_id", binding.parent_activity_id.as_deref());
            require("team_run_id", binding.team_run_id.as_deref());
        }
        RuntimeEventScope::Agent => {
            require("node_id", binding.node_id.as_deref());
            require("parent_activity_id", binding.parent_activity_id.as_deref());
            require("agent_instance_id", binding.agent_instance_id.as_deref());
            require("agent_run_id", binding.agent_run_id.as_deref());
        }
        RuntimeEventScope::Skill => {
            require("parent_activity_id", binding.parent_activity_id.as_deref());
            require("skill_id", binding.skill_id.as_deref());
            require(
                "skill_activation_id",
                binding.skill_activation_id.as_deref(),
            );
        }
        RuntimeEventScope::Tool => {
            require("parent_activity_id", binding.parent_activity_id.as_deref());
            require("tool_contract_id", binding.tool_contract_id.as_deref());
            require("tool_call_id", binding.tool_call_id.as_deref());
        }
        _ => {}
    }
    if missing.is_empty() {
        return Ok(());
    }
    Err(RuntimeEventStoreError::InvalidTransaction(format!(
        "business lifecycle event `{kind}` has incomplete RuntimeActivityBinding; missing {}",
        missing.join(", ")
    )))
}

pub fn request_hash(request: &AppendTransactionRequest) -> RuntimeEventStoreResult<String> {
    Ok(hash_bytes(&serde_json::to_vec(request)?))
}

pub fn request_hash_with_terminal(
    request: &AppendTransactionRequest,
    terminal: Option<&SessionTerminalInput>,
) -> RuntimeEventStoreResult<String> {
    terminal.map_or_else(
        || request_hash(request),
        |terminal| Ok(hash_bytes(&serde_json::to_vec(&(request, terminal))?)),
    )
}

pub(super) fn hash_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
