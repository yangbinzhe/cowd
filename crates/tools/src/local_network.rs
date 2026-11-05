use super::*;
use harness_contract::context::{
    LocalNetworkFact, LocalNetworkOperation, LocalNetworkTerminal, ToolExecutionBinding,
};
use sha2::{Digest, Sha256};

const MAX_LOCAL_NETWORK_OPERATIONS: usize = 400;

/// Whether the joined local-network path implements this tool.
///
/// The joined path mints one `LocalNetworkFact` for a whole call, so it is only
/// correct for the tools it actually implements. The router and the executor
/// both read this one function, so a tool can never be routed into a path that
/// does not implement it: before it existed the router decided on the effect
/// property alone (`uses_network && !spawns_process`) while the executor decided
/// on the tool name, so every network read tool other than `web_fetch` was
/// routed here and could only fail.
///
/// `uses_network && !spawns_process` stays a necessary condition on top of
/// this; it is not a substitute for it.
pub fn joined_local_network_tool(tool: &str) -> bool {
    matches!(tool, "web_fetch" | "tool_orchestrate")
}

#[derive(Debug, Default)]
pub(super) struct LocalNetworkTrace {
    operations: Vec<LocalNetworkOperation>,
    unsupported: bool,
}

impl ToolHostLease {
    pub(crate) fn observe_local_network_dispatch(
        &self,
        tool: &str,
        input: &Value,
    ) -> Result<(), String> {
        let Some(trace) = &self.local_network_trace else {
            return Ok(());
        };
        let effect = self.describe_effect(tool, input);
        if effect.spawns_process
            || (effect.uses_network && !joined_local_network_tool(tool))
        {
            trace
                .lock()
                .map_err(|_| "local network trace poisoned")?
                .unsupported = true;
        }
        Ok(())
    }

    pub(crate) fn begin_local_network_operation(
        &self,
        input: &Value,
        endpoint: &str,
    ) -> Result<Option<usize>, String> {
        let Some(trace) = &self.local_network_trace else {
            return Ok(None);
        };
        let mut trace = trace.lock().map_err(|_| "local network trace poisoned")?;
        if trace.operations.len() >= MAX_LOCAL_NETWORK_OPERATIONS {
            trace.unsupported = true;
            return Err("local network operation limit".into());
        }
        let position = trace.operations.len();
        trace.operations.push(LocalNetworkOperation {
            ordinal: position as u32 + 1,
            input_sha256: format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(input).map_err(|error| error.to_string())?)
            ),
            endpoint: endpoint.into(),
            terminal: LocalNetworkTerminal::Unknown {
                reason_code: "operation_not_body_done".into(),
            },
        });
        Ok(Some(position))
    }

    pub(crate) fn finish_local_network_operation(
        &self,
        position: Option<usize>,
        terminal: LocalNetworkTerminal,
    ) -> Result<(), String> {
        let Some(position) = position else {
            return Ok(());
        };
        let trace = self
            .local_network_trace
            .as_ref()
            .ok_or("local network trace missing")?;
        let mut trace = trace.lock().map_err(|_| "local network trace poisoned")?;
        let operation = trace
            .operations
            .get_mut(position)
            .ok_or("local network operation missing")?;
        if !matches!(operation.terminal, LocalNetworkTerminal::Unknown { .. }) {
            trace.unsupported = true;
            return Err("local network operation finalized twice".into());
        }
        operation.terminal = terminal;
        Ok(())
    }

    pub async fn execute_joined_local_network(
        &self,
        authorization: &ToolExecutionAuthorization,
        tool_id: &str,
        input: &str,
        binding: ToolExecutionBinding,
        session_id: &str,
        origin: &harness_contract::step::CallOrigin,
    ) -> Result<(String, LocalNetworkFact), ToolHostError> {
        let failure = |message: &str| ToolHostError::Execution(message.into());
        if self.local_network_trace.is_some() {
            return Err(failure("local network owner cannot be nested"));
        }
        let value = harness_contract::step::parse_strict_json(input)
            .map_err(|error| failure(&error.to_string()))?;
        let canonical = self.validate_authorization(authorization, tool_id, &value)?;
        let effect = self.describe_effect(&canonical, &value);
        let input_digest = harness_contract::step::canonical_digest("tool-effect-payload", &value)
            .map_err(|error| failure(&error.to_string()))?;
        let descriptor_digest = harness_contract::step::canonical_digest(
            harness_contract::context::TOOL_EXECUTION_DESCRIPTOR_DOMAIN,
            &serde_json::to_value(&effect).map_err(|error| failure(&error.to_string()))?,
        )
        .map_err(|error| failure(&error.to_string()))?;
        let origin_valid = match &origin.source {
            harness_contract::step::CallSource::Provider { attempt_id } => !attempt_id.is_empty(),
            harness_contract::step::CallSource::Process { worker_id, sequence } => !worker_id.is_empty() && *sequence > 0,
            harness_contract::step::CallSource::SystemDerived { parent_step_id } => !parent_step_id.is_empty(),
            harness_contract::step::CallSource::OfflineTest => false,
        };
        let effect_key = harness_contract::step::canonical_digest(
            "step-effect-identity", &serde_json::json!([session_id, origin.origin_step_id, origin.call_id]),
        ).map_err(|error| failure(&error.to_string()))?;
        // The tool name is checked on its own so an unimplemented tool is named
        // as such instead of being reported as a binding mismatch. Everything
        // below is the identity proof for a call this path does implement.
        // Both the name the router saw and the name this path canonicalized to
        // must be in the implemented set. The router's value is checked here as
        // well so a canonicalization that renames the tool cannot make the two
        // sides disagree about what is accepted: this side stays the stricter
        // one, and a mismatch fails closed with the named reason.
        if !joined_local_network_tool(tool_id) || !joined_local_network_tool(&canonical) {
            return Err(failure("local network joined path does not implement this tool"));
        }
        if !effect.uses_network
            || effect.spawns_process
            || input_digest.strip_prefix("sha256:") != Some(binding.input_sha256.as_str())
            || descriptor_digest.strip_prefix("sha256:") != Some(binding.descriptor_sha256.as_str())
            || session_id.is_empty()
            || origin.origin_step_id.is_empty()
            || origin.catalog_digest.is_empty()
            || !origin_valid
            || binding.call_key != effect_key
            || binding.tool_use_id != origin.call_id
            || authorization.request_id != origin.call_id
            || authorization.authorization_lease.idempotency_key != origin.call_id
            || authorization
                .idempotency_key
                .as_deref()
                .is_some_and(|key| key != origin.call_id)
        {
            return Err(failure("local network original binding mismatch"));
        }
        let trace = Arc::new(std::sync::Mutex::new(LocalNetworkTrace::default()));
        let mut lease = self.for_authorized_read_cache(authorization)?;
        lease.local_network_trace = Some(Arc::clone(&trace));
        let output = tokio::task::spawn_blocking(move || lease.dispatch_sync(&canonical, &value))
            .await
            .map_err(|error| failure(&format!("local network host join failed: {error}")))??;
        Ok((output, seal_trace(binding, trace)?))
    }
}

fn seal_trace(
    binding: ToolExecutionBinding,
    trace: Arc<std::sync::Mutex<LocalNetworkTrace>>,
) -> Result<LocalNetworkFact, ToolHostError> {
    let trace = Arc::try_unwrap(trace)
        .map_err(|_| {
            ToolHostError::Execution("local network clone owner remains after host join".into())
        })?
        .into_inner()
        .map_err(|_| ToolHostError::Execution("local network trace poisoned".into()))?;
    Ok(LocalNetworkFact {
        version: 1,
        binding,
        host_joined: true,
        coverage_complete: !trace.unsupported,
        operations: trace.operations,
    })
}

#[cfg(test)]
#[path = "local_network_tests.rs"]
mod tests;
