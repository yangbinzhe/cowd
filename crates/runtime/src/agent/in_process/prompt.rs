use super::*;

pub(super) fn permission_policy(
    live_control: Option<crate::permissions::SessionExecutionPolicyControl>,
    mode: PermissionMode,
    tools: &BTreeSet<String>,
) -> PermissionPolicy {
    let policy = live_control
        .map_or_else(
            || PermissionPolicy::new(mode),
            PermissionPolicy::with_execution_policy_control,
        )
        .with_immutable_ceiling(mode);
    tools.iter().fold(policy, |policy, tool| {
        policy.with_tool_requirement(tool, crate::agent_capability::agent_tool_permission(tool))
    })
}

pub(super) fn is_agent_action_tool(tool_name: &str) -> bool {
    harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
        .iter()
        .any(|action| tool_name.eq_ignore_ascii_case(action))
}

pub(super) fn system_prompt(
    packet: &AgentTaskPacket,
    workspace_root: &std::path::Path,
    tool_names: &[String],
) -> Vec<String> {
    let mut prompt = vec![
        "You are a delegated Cowd agent. Return an evidence-backed result for the assigned objective.".into(),
        "You are an active role inside an already-running protocol. Return findings and evidence to the reducer, and when the objective reveals a genuine missing workstream you may propose a new Team or session through the governed collaboration protocol.".into(),
        "Use only native tool calls exposed by this runtime. Never write simulated tool syntax such as <tool_call>, <function=...>, <parameter=...>, or JSON-shaped pseudo-calls in final text. If no native tool is authorized, answer directly from the supplied objective and upstream evidence.".into(),
        crate::prompt::stable_runtime_context_protocol(),
    ];
    if tool_names.iter().any(|tool| is_agent_action_tool(tool)) {
        prompt.push(
            "# Stable autonomous-collaboration protocol\nYou are an active collaborator. Inspect current Program state, claim eligible bounded work, execute it with real tools, commit durable artifacts, submit evidence, review peers independently, publish scoped messages when coordination adds value, and create or publish follow-up work when the objective reveals a genuine gap. Prose never changes collaboration state. Runtime owns identity, permission, revisions, leases, execution and terminal truth; never invent them. Keep long content in normal output or files and use compact action references.".to_string(),
        );
    }
    prompt.push(crate::SYSTEM_PROMPT_CACHE_COHORT_BOUNDARY.to_string());
    prompt.extend([
        format!("Objective: {}", packet.objective),
        format!("Workspace root: {}", workspace_root.display()),
        format!(
            "Authorized resource scopes: {}. Every native file-tool path must be relative to the displayed Workspace root and retain the complete authorized scope prefix. For example, scope read:project means project/Cargo.toml, never bare Cargo.toml. A missing path never means the whole workspace.",
            if packet.resource_scopes.is_empty() {
                "(none)".to_string()
            } else {
                packet.resource_scopes.join(", ")
            }
        ),
    ]);
    if let Some(binding) = &packet.binding {
        prompt.push(format!(
            "Agent Definition: {}@{} (binding {}).",
            binding.definition_ref.definition_id.as_str(),
            binding.definition_ref.revision,
            binding.binding_id,
        ));
        prompt.push(format!(
            "Definition instructions:\n{}",
            binding.instructions.trim()
        ));
        prompt.push(format!(
            "Cognitive data lease: read={:?}; write={:?}",
            binding.data_lease.read_scopes, binding.data_lease.write_mode,
        ));
    }
    if !packet.constraints.is_empty() {
        prompt.push(format!("Constraints: {}", packet.constraints.join("; ")));
    }
    if !packet.acceptance.is_empty() {
        prompt.push(format!("Acceptance: {}", packet.acceptance.join("; ")));
    }
    let mut required_write_scopes = Vec::new();
    let contract = packet_acceptance_contract(packet);
    if !contract.is_empty() {
        required_write_scopes.extend(contract.iter().flat_map(
            |requirement| match &requirement.check {
                harness_contract::agent::OutputAcceptanceCheck::WorkspaceChange {
                    scopes, ..
                } => scopes.clone(),
                _ => Vec::new(),
            },
        ));
        required_write_scopes.sort();
        required_write_scopes.dedup();
        let mut fields = contract
            .iter()
            .filter_map(|requirement| match &requirement.check {
                harness_contract::agent::OutputAcceptanceCheck::StructuredField { field }
                | harness_contract::agent::OutputAcceptanceCheck::WorkspaceChange {
                    field, ..
                } => Some(field.as_str().to_string()),
                harness_contract::agent::OutputAcceptanceCheck::StructuredArtifact { name } => {
                    Some(name.clone())
                }
                harness_contract::agent::OutputAcceptanceCheck::SourceVerification { .. } => {
                    Some("source_verification".to_string())
                }
                harness_contract::agent::OutputAcceptanceCheck::UpstreamReview => {
                    Some("review".to_string())
                }
                harness_contract::agent::OutputAcceptanceCheck::UpstreamEvidence => None,
                harness_contract::agent::OutputAcceptanceCheck::ScopedEvidence { .. } => None,
            })
            .collect::<Vec<_>>();
        fields.sort();
        fields.dedup();
        prompt.push(format!(
            "Give a concise terminal answer. When practical, label these presentation fields: {}. Native structured output, a JSON object, Markdown headings, and `Field: value` labels are all understood. Runtime derives acceptance from committed tool receipts, change paths, and upstream evidence bindings; prose never substitutes for those facts.",
            fields.join(", ")
        ));
    }
    if !tool_names.is_empty() {
        if required_write_scopes.is_empty() {
            prompt.push(format!(
                "Authorized tool contracts are available natively: {}. When the objective asks for source, workspace, file, or current-state evidence, use an authorized read-only tool and cite the resulting paths/receipts; do not substitute prior model knowledge.",
                tool_names.join(", ")
            ));
        } else {
            prompt.push(format!(
                "Authorized tool contracts are available natively: {}. This role has a Runtime-verified workspace-change obligation for: {}. Read each target at most once before mutation, invoke an authorized write tool for the required change, then perform a separate read-only verification. Repeated reads, prose claims, or simulated tool markup cannot replace the committed write receipt.",
                tool_names.join(", "),
                required_write_scopes.join(", ")
            ));
        }
    }
    // Every section above is derived from the immutable AgentTaskPacket,
    // Binding, resource lease and host inventory. Request-local clocks,
    // working-state deltas, memory and evidence capsules are appended after
    // this boundary by PromptAssembly and are never duplicated into the
    // stable Agent assignment.
    prompt.push(crate::SYSTEM_PROMPT_DYNAMIC_BOUNDARY.to_string());
    prompt
}
