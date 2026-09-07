use super::*;

pub(super) struct ScopedRuntimeToolExecutor {
    pub(super) host: Arc<dyn RuntimeExecutionHost>,
    pub(super) allowed_tools: BTreeSet<String>,
    pub(super) session_id: String,
    pub(super) sandbox_posture: harness_contract::policy::SandboxPosture,
    pub(super) policy_revision: u64,
    pub(super) memory_context: memory::MemoryTurnContext,
    pub(super) model_lease: String,
    pub(super) execution_id: String,
    pub(super) node_id: String,
    pub(super) attempt: u32,
    pub(super) workspace_root: std::path::PathBuf,
    pub(super) path_identity_resolver: Arc<crate::path_identity::WorkspacePathIdentityResolver>,
    pub(super) scope_locks: Arc<ScopeLockManager>,
    /// Production instances persist every canonical ToolHost receipt before
    /// exposing it to the child terminal evaluator. Unit fixtures may omit
    /// this ledger because they do not model a durable RuntimeServices host.
    pub(super) commit_service: Option<crate::execution_core::graph::ExecutionCommitService>,
    /// `Some` marks a Team child and is always enforced. An empty list means
    /// no workspace authority; it never expands to the whole repository.
    pub(super) resource_scopes: Option<Vec<String>>,
    pub(super) managed_invocation:
        Option<harness_contract::managed_agent::ManagedAgentInvocationFence>,
    pub(super) next_receipt_sequence: AtomicU64,
    pub(super) receipts: Mutex<Vec<ScopedToolExecutionReceipt>>,
    /// Frozen semantic observation policy compiled into the Agent packet.
    /// This describes required delivery; it never claims delivery occurred.
    pub(super) provider_model_obligations: Vec<harness_contract::context::EvidenceObligation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ScopedToolExecutionReceipt {
    pub(super) sequence: u64,
    /// Provider ToolUse identity for this concrete delivery attempt. Durable
    /// replay receipts deliberately leave it absent until actual redelivery.
    pub(super) provider_invocation_id: Option<String>,
    pub(super) tool_name: String,
    pub(super) effect_kind: harness_contract::tool::ToolEffectKind,
    pub(super) resource_scopes: Vec<String>,
    pub(super) paths: Vec<String>,
    pub(super) prior_states: BTreeMap<String, harness_contract::context::WorkspacePriorState>,
    pub(super) after_digests: BTreeMap<String, Option<String>>,
    pub(super) observed_bytes: BTreeMap<String, u64>,
    pub(super) observed_evidence: Vec<harness_contract::context::ObservedEvidence>,
}

pub(super) fn scoped_receipt_from_durable(
    receipt: crate::execution_core::graph::DurableAgentToolReceipt,
) -> ScopedToolExecutionReceipt {
    let mut paths = Vec::new();
    let mut prior_states = BTreeMap::new();
    let mut after_digests = BTreeMap::new();
    let output_bytes = receipt
        .outcome
        .output
        .as_deref()
        .and_then(tool_output_byte_length);
    for evidence in &receipt.outcome.observed_evidence {
        let harness_contract::context::EvidenceTargetIdentity::Workspace { scope } =
            &evidence.target
        else {
            continue;
        };
        let path = scope.path.workspace_relative_path.clone();
        if !paths.contains(&path) {
            paths.push(path.clone());
        }
        if let Some(state) = evidence.workspace_prior_state.clone() {
            prior_states.insert(path.clone(), state);
        }
        after_digests.insert(path, scope.path.observed_revision_or_digest.clone());
    }
    paths.sort();
    let observed_bytes = if paths.len() == 1 {
        output_bytes
            .map(|bytes| BTreeMap::from([(paths[0].clone(), bytes)]))
            .unwrap_or_default()
    } else {
        BTreeMap::new()
    };
    ScopedToolExecutionReceipt {
        sequence: receipt.sequence,
        provider_invocation_id: None,
        tool_name: receipt.outcome.tool_name.clone(),
        effect_kind: receipt.effect_kind,
        resource_scopes: receipt.authorized_scopes,
        paths,
        prior_states,
        after_digests,
        observed_bytes,
        observed_evidence: receipt.outcome.observed_evidence,
    }
}

pub(super) fn tool_output_byte_length(output: &str) -> Option<u64> {
    let start = output.find('{')?;
    let value = serde_json::from_str::<serde_json::Value>(&output[start..]).ok()?;
    fn find(value: &serde_json::Value) -> Option<u64> {
        match value {
            serde_json::Value::Object(object) => object
                .get("byteLength")
                .and_then(serde_json::Value::as_u64)
                .or_else(|| object.values().find_map(find)),
            serde_json::Value::Array(values) => values.iter().find_map(find),
            _ => None,
        }
    }
    find(&value)
}

/// Bound, Runtime-attested recovery context for a delegated attempt that
/// crashed after ToolHost committed effects. It deliberately contains only
/// the canonical receipt outputs and evidence the Agent already holds under
/// its role lease; it never reads the live workspace or asks the model to
/// reconstruct a side effect.
pub(super) fn recovered_agent_tool_receipt_prompt(
    receipts: &[crate::execution_core::graph::DurableAgentToolReceipt],
    agentic_protocol_pending: bool,
) -> Option<String> {
    if receipts.is_empty() {
        return None;
    }
    let evidence = receipts
        .iter()
        .map(|receipt| {
            serde_json::json!({
                "sequence": receipt.sequence,
                "effect_kind": receipt.effect_kind,
                "authorized_scopes": receipt.authorized_scopes,
                "outcome": receipt.outcome,
            })
        })
        .collect::<Vec<_>>();
    let serialized = serde_json::to_string(&evidence).ok()?;
    let bounded = serialized.chars().take(48_000).collect::<String>();
    let instruction = if agentic_protocol_pending {
        "A previous process already committed the following Runtime ToolHost receipts for this exact Agent attempt. They are authoritative. Do not replay any recorded action or infer new workspace state. The Agent-first Task protocol is still pending; Runtime will expose only the next missing compact artifact/submit/review action. Execute that action using the exact retained refs."
    } else {
        "A previous process already committed the following Runtime ToolHost receipts for this exact Agent attempt. They are authoritative. Do not call tools, retry an action, or infer new workspace state. Produce one concise terminal response grounded only in these retained receipts; state any unresolved requirement plainly."
    };
    Some(format!(
        "# Durable tool-receipt recovery\n{instruction}\n\n{bounded}"
    ))
}

pub(super) fn agentic_task_protocol_pending(
    services: &crate::RuntimeServices,
    packet: &AgentTaskPacket,
) -> Result<bool, String> {
    let Some(agentic) = packet.agentic_binding.as_ref() else {
        return Ok(false);
    };
    let (task_id, mode) = match &agentic.focus {
        harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref } => {
            (task_ref.as_str(), "execute")
        }
        harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref } => {
            (task_ref.as_str(), "review")
        }
        _ => return Ok(false),
    };
    let program_id = agentic.program_id.as_str();
    let agent_id = agentic.agent_id.as_str();
    let projection = services
        .agent_action_service()
        .project(program_id)
        .map_err(|error| format!("Agent-first recovery projection failed: {error}"))?;
    let task = projection.tasks.get(task_id).ok_or_else(|| {
        format!("Agent-first recovery Program `{program_id}` has no Task `{task_id}`")
    })?;
    match mode {
        "execute" => Ok((matches!(
            task.status,
            crate::AgenticTaskStatus::Published | crate::AgenticTaskStatus::Rework
        ) && task.claimant.is_none()
            && task.claim_execution_id.is_none())
            || (task.status == crate::AgenticTaskStatus::Claimed
                && task.claimant.as_deref() == Some(agent_id)
                && task.claim_execution_id.as_deref() == Some(packet.graph_id())
                && task.claim_generation == u64::from(packet.attempt))),
        "review" => Ok(task.status == crate::AgenticTaskStatus::Submitted
            && task.claimant.as_deref() != Some(agent_id)
            && task.review_generation.saturating_add(1) == u64::from(packet.attempt)),
        _ => Err(format!(
            "Agent-first recovery packet has unsupported attempt mode `{mode}`"
        )),
    }
}

pub(super) struct AgentAutonomyCheckpoint {
    pub(super) prompt: String,
    pub(super) tool_ids: Vec<String>,
    pub(super) requires_tool_action: bool,
    pub(super) agentic_topic_ack: Option<crate::agentic::AgenticTopicObservationAck>,
}

pub(super) fn autonomy_checkpoint_tool_plan(
    packet: &AgentTaskPacket,
    requires_tool_action: bool,
    requires_execution_tools: bool,
) -> Vec<String> {
    if !requires_tool_action {
        Vec::new()
    } else if requires_execution_tools {
        packet
            .allowed_tools
            .iter()
            .filter(|tool| tool.as_str() != "tool_search")
            .cloned()
            .collect()
    } else {
        ["task_claim", "message_publish"]
            .into_iter()
            .filter(|tool| packet.allowed_tools.iter().any(|allowed| allowed == tool))
            .map(str::to_string)
            .collect()
    }
}

pub(super) fn autonomy_continuation_may_advance(
    completion: harness_contract::goal::GoalCompletion,
) -> bool {
    completion == harness_contract::goal::GoalCompletion::Satisfied
}

/// Fingerprint only the durable checkpoint state that can justify another
/// continuation. Program revisions advance for ordinary topic/actions;
/// including that counter would let a model repeat the same required action
/// forever. Task state and unread entries remain part of the digest.
pub(super) fn autonomy_checkpoint_progress_digest(prompt: &str) -> String {
    let Some((_, payload)) = prompt.rsplit_once("\n\n") else {
        return format!("{:x}", Sha256::digest(prompt.as_bytes()));
    };
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(payload) else {
        return format!("{:x}", Sha256::digest(prompt.as_bytes()));
    };
    if let serde_json::Value::Object(object) = &mut value {
        object.remove("program_revision");
    }
    let canonical = serde_json::to_vec(&value).unwrap_or_else(|_| payload.as_bytes().to_vec());
    format!("{:x}", Sha256::digest(canonical))
}

/// Bind the liveness fuse to semantic execution progress as well as Program
/// state. Complex delegated work commonly needs several model turns to write,
/// run and repair an artifact before it can be committed. Program state stays
/// `Claimed` throughout that work, so a checkpoint-only digest incorrectly
/// treats genuine file/test progress as a loop. Volatile receipt identities
/// and sequence numbers are deliberately excluded: replaying the same read or
/// command still converges, while a new target or content digest earns another
/// bounded continuation.
pub(super) fn agent_autonomy_progress_digest(
    prompt: &str,
    receipts: &[ScopedToolExecutionReceipt],
) -> String {
    let mut semantic_receipts = BTreeSet::new();
    for receipt in receipts {
        let targets = receipt
            .observed_evidence
            .iter()
            .map(|evidence| format!("{:?}", evidence.target))
            .collect::<BTreeSet<_>>();
        semantic_receipts.insert(format!(
            "{:?}|{}|{:?}|{:?}|{:?}|{:?}",
            receipt.effect_kind,
            receipt.tool_name,
            receipt.resource_scopes,
            receipt.paths,
            receipt.after_digests,
            targets,
        ));
    }
    let stable_checkpoint = autonomy_checkpoint_progress_digest(prompt);
    let mut hasher = Sha256::new();
    hasher.update(stable_checkpoint.as_bytes());
    for receipt in semantic_receipts {
        hasher.update(b"\n");
        hasher.update(receipt.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

pub(super) fn agent_autonomy_checkpoint(
    services: &Arc<RuntimeServices>,
    packet: &AgentTaskPacket,
) -> Result<Option<AgentAutonomyCheckpoint>, String> {
    let Some(agentic) = packet.agentic_binding.as_ref() else {
        // Generic ExecutionGraph/Team packets have no autonomous market. The
        // Agent-first Program is the sole collaboration control plane.
        return Ok(None);
    };
    let (task_id, mode) = match &agentic.focus {
        harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref } => {
            (task_ref.as_str(), "execute")
        }
        harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref } => {
            (task_ref.as_str(), "review")
        }
        _ => return Ok(None),
    };
    let program_id = agentic.program_id.as_str();
    let agent_id = agentic.agent_id.as_str();
    let projection = services
        .agent_action_service()
        .project(program_id)
        .map_err(|error| format!("load Agent-first autonomy checkpoint: {error}"))?;
    let _member = projection
        .agents
        .get(agent_id)
        .ok_or_else(|| format!("Agent-first Program `{program_id}` has no member `{agent_id}`"))?;
    if projection
        .membership_for(agent_id, &agentic.team_id)
        .is_none_or(|membership| membership.membership_id != agentic.membership_id)
    {
        return Err("Agent-first packet membership no longer matches Program".to_string());
    }
    let task = projection
        .tasks
        .get(task_id)
        .ok_or_else(|| format!("Agent-first Program `{program_id}` has no Task `{task_id}`"))?;
    // Execution ownership is Team-scoped, while review independence is
    // deliberately allowed (and preferentially scheduled) across Teams.  Do
    // not reuse the execution ownership fence for review packets: doing so
    // lets the review action commit and then falsely fails the physical Agent
    // graph at the next checkpoint.
    if mode == "execute" && !projection.agent_is_active_in(agent_id, &task.team_id) {
        return Err("Agent-first packet member is outside the bound Task Team".to_string());
    }
    let topic_ref = projection
        .teams
        .get(&task.team_id)
        .map(|team| team.topic_ref.as_str())
        .ok_or_else(|| "Agent-first bound Task Team has no topic".to_string())?;
    let already_declined = projection.topics.get(topic_ref).is_some_and(|entries| {
        entries.iter().any(|entry| {
            entry.actor_id == agent_id
                && entry.refs.iter().any(|reference| reference == task_id)
                && entry.summary.as_deref().is_some_and(|summary| {
                    summary
                        .trim_start()
                        .to_ascii_lowercase()
                        .starts_with("decline:")
                })
        })
    });
    let mut actions = Vec::new();
    let mut requires_execution_tools = false;
    match (mode, task.status) {
        ("execute", crate::AgenticTaskStatus::Published | crate::AgenticTaskStatus::Rework)
            if task.claimant.is_none()
                && task.claim_execution_id.is_none()
                && !already_declined =>
        {
            actions.push(serde_json::json!({
                "action": "decide_claim_or_decline",
                "task_ref": task.task_id,
                "options": [
                    {
                        "tool": "task_claim",
                        "input": {
                            "task_ref": task.task_id,
                            "reason": "This bounded task fits my Runtime-attested role and capabilities"
                        }
                    },
                    {
                        "tool": "message_publish",
                        "input": {
                            "topic_ref": topic_ref,
                            "summary": "DECLINE: replace with a concise role/capability mismatch",
                            "content_ref": null,
                            "refs": [task.task_id]
                        }
                    }
                ]
            }));
        }
        ("execute", crate::AgenticTaskStatus::Claimed)
            if task.claimant.as_deref() == Some(agent_id)
                && task.claim_execution_id.as_deref() == Some(packet.graph_id()) =>
        {
            requires_execution_tools = true;
            actions.push(serde_json::json!({
                "action": "execute_commit_submit",
                "task_ref": task.task_id,
                "objective": task.objective,
                "acceptance": task.acceptance,
                "required_protocol": ["artifact_commit", "task_submit"],
                "claim_execution_id": packet.graph_id(),
            }));
        }
        ("review", crate::AgenticTaskStatus::Submitted)
            if task.claimant.as_deref() != Some(agent_id) =>
        {
            requires_execution_tools = true;
            actions.push(serde_json::json!({
                "action": "inspect_and_review",
                "task_ref": task.task_id,
                "acceptance": task.acceptance,
                "artifact_refs": task.artifact_refs,
                "evidence_refs": task.evidence_refs,
                "tool": "task_review",
            }));
        }
        ("execute" | "review", _) => {}
        _ => {
            return Err(format!(
                "Agent-first packet has unsupported attempt mode `{mode}`"
            ))
        }
    }

    let agentic_topic_page = services
        .agent_action_service()
        .topic_observations(program_id, agent_id, packet.graph_id(), 16, 48 * 1024)
        .map_err(|error| format!("load Agent-first topic checkpoint: {error}"))?;
    let (
        agentic_topic_entries,
        agentic_topic_from_revision,
        agentic_topic_to_revision,
        agentic_topic_ack,
    ) = agentic_topic_page.map_or_else(
        || (Vec::new(), 0, 0, None),
        |page| {
            let ack = crate::agentic::AgenticTopicObservationAck {
                program_id: program_id.to_string(),
                execution_id: packet.graph_id().to_string(),
                through_revision: page.to_revision,
                expected_cursor_revision: page.cursor_revision,
            };
            (
                page.entries,
                page.from_revision,
                page.to_revision,
                Some(ack),
            )
        },
    );
    if actions.is_empty() && agentic_topic_entries.is_empty() {
        return Ok(None);
    }
    let checkpoint = serde_json::to_string(&serde_json::json!({
        "kind": "runtime_agent_autonomy_checkpoint",
        "program_revision": projection.revision,
        "attested_agent_instance_id": agent_id,
        "attempt_mode": mode,
        "task_ref": task_id,
        "required_actions": actions,
        "unread_agentic_topic_entries": agentic_topic_entries,
        "agentic_topic_from_revision": agentic_topic_from_revision,
        "agentic_topic_to_revision": agentic_topic_to_revision,
    }))
    .map_err(|error| format!("serialize Agent autonomy checkpoint: {error}"))?;
    let requires_tool_action = !actions.is_empty();
    let tool_ids =
        autonomy_checkpoint_tool_plan(packet, requires_tool_action, requires_execution_tools);
    Ok(Some(AgentAutonomyCheckpoint {
        prompt: format!(
            "Runtime safe checkpoint committed after your prior model round. It contains only canonical Agent-first Program/Task state and unread public Team topic entries. Resolve the listed compact action with your native tools: if the bound Task fits, actively task_claim it before substantive execution; if it does not fit, do not claim and use the supplied message_publish option to explain the mismatch so the Team can reassign or replan. After a successful claim, do the work, commit a durable artifact, and submit exact evidence; in review mode inspect the submitted artifact and issue an independent task_review verdict. Do not invent identities, revisions, artifacts, sources, or completion. Preserve the substance and evidence of your earlier result and return a complete updated answer after the checkpoint is closed.\n\n{checkpoint}"
        ),
        tool_ids,
        requires_tool_action,
        agentic_topic_ack,
    }))
}

#[async_trait::async_trait]
impl ToolExecutor for ScopedRuntimeToolExecutor {
    async fn execute_output(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        if tool_name == "tool_search" {
            let query = serde_json::from_str::<serde_json::Value>(input)
                .ok()
                .and_then(|value| {
                    value
                        .get("query")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
                .unwrap_or_default();
            let mut receipt = self.tool_discovery_receipt();
            receipt.query = query;
            return serde_json::to_string(&receipt)
                .map(harness_contract::context::ToolOutputDraft::bounded_inline)
                .map_err(|error| {
                    ToolError::new(format!("serialize agent tool discovery: {error}"))
                });
        }
        if tool_name == "checkpoint_create" {
            return Err(ToolError::new(
                "checkpoint_create is a Runtime-internal mutation guard and cannot be invoked by the delegated model",
            ));
        }
        if !self.allowed_tools.contains(tool_name) {
            return Err(ToolError::new(format!(
                "tool `{tool_name}` is outside the AgentTaskPacket allow-list"
            )));
        }
        let normalized_input = normalize_delegated_resource_paths(
            tool_name,
            input,
            &self.workspace_root,
            &self.path_identity_resolver,
            self.resource_scopes.as_deref(),
        )?;
        self.enforce_resource_ceiling(tool_name, &normalized_input)?;
        self.execute_scoped(tool_name, &normalized_input, None, None)
            .await
            .map(harness_contract::context::ToolOutputDraft::bounded_inline)
    }

    fn tool_discovery_receipt(&self) -> harness_contract::tool::ToolDiscoveryReceipt {
        use harness_contract::tool::{
            ToolDescriptorHealth, ToolDescriptorRef, ToolDiscoveryReceipt, ToolPermissionMode,
        };

        let mut descriptors = Vec::with_capacity(self.allowed_tools.len().saturating_add(1));
        descriptors.push(ToolDescriptorRef {
            canonical_id: "tool_search".to_string(),
            display_name: "tool_search".to_string(),
            source: "delegated-agent".to_string(),
            schema_hash: "delegated-agent:tool-search:v1".to_string(),
            required_permission: ToolPermissionMode::ReadOnly,
            permission_source: "runtime bootstrap".to_string(),
            health: ToolDescriptorHealth::Healthy,
        });
        descriptors.extend(self.allowed_tools.iter().filter_map(|tool_name| {
            let descriptor = self
                .host
                .delegated_tool_effect_descriptor(tool_name, &serde_json::json!({}))?;
            Some(ToolDescriptorRef {
                canonical_id: tool_name.clone(),
                display_name: tool_name.clone(),
                source: "delegated-agent".to_string(),
                schema_hash: descriptor.descriptor_hash,
                required_permission: descriptor.required_permission,
                permission_source: "runtime binding plus host snapshot".to_string(),
                health: ToolDescriptorHealth::Healthy,
            })
        }));
        ToolDiscoveryReceipt {
            query: "delegated-agent".to_string(),
            catalog_revision: 0,
            descriptors,
            activation_candidates: self.allowed_tools.iter().cloned().collect(),
        }
    }

    fn registered_tool_effect(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        if tool_name == "checkpoint_create" {
            return self
                .internal_checkpoint_input(input.clone())
                .ok()
                .and_then(|input| {
                    self.host
                        .delegated_tool_effect_descriptor(tool_name, &input)
                });
        }
        self.allowed_tools.contains(tool_name).then(|| {
            let normalized = normalize_delegated_resource_value(
                tool_name,
                input.clone(),
                &self.workspace_root,
                &self.path_identity_resolver,
                self.resource_scopes.as_deref(),
            );
            self.host
                .delegated_tool_effect_descriptor(tool_name, &normalized)
        })?
    }

    async fn execute_authorized_output(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        if authorization.tool_id != tool_name {
            return Err(ToolError::new(
                "agent tool authorization does not match the allowed tool request",
            ));
        }
        if tool_name == "checkpoint_create" {
            return self
                .execute_internal_checkpoint(input, authorization.clone())
                .await
                .map(harness_contract::context::ToolOutputDraft::bounded_inline);
        }
        // Runtime-owned collaborative tools must be delegated back to the
        // Gateway RuntimeExecutionHost. They are not pure ToolHost adapters;
        // letting them fall through would fail every required Team node with
        // "has no ToolHost implementation adapter".
        if tool_name == "evidence_retrieve" || is_agent_action_tool(tool_name) {
            if !self.allowed_tools.contains(tool_name) {
                return Err(ToolError::new(
                    "agent tool authorization does not match the allowed tool request",
                ));
            }
            return self
                .execute_delegated_runtime_tool(tool_name, input, authorization.clone())
                .await
                .map(harness_contract::context::ToolOutputDraft::bounded_inline);
        }
        if !self.allowed_tools.contains(tool_name) {
            return Err(ToolError::new(
                "agent tool authorization does not match the allowed tool request",
            ));
        }
        let normalized_input = normalize_delegated_resource_paths(
            tool_name,
            input,
            &self.workspace_root,
            &self.path_identity_resolver,
            self.resource_scopes.as_deref(),
        )?;
        self.enforce_resource_ceiling(tool_name, &normalized_input)?;
        self.execute_scoped(
            tool_name,
            &normalized_input,
            Some(authorization.clone()),
            None,
        )
        .await
        .map(harness_contract::context::ToolOutputDraft::bounded_inline)
    }

    fn available_tool_names(&self) -> Vec<String> {
        std::iter::once("tool_search".to_string())
            .chain(self.allowed_tools.iter().cloned())
            .collect()
    }

    fn observed_evidence_snapshot(&self) -> Vec<harness_contract::context::ObservedEvidence> {
        self.receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .flat_map(|receipt| receipt.observed_evidence.iter().cloned())
            .collect()
    }

    fn owns_durable_tool_effect(&self, tool_name: &str) -> bool {
        self.commit_service.is_some()
            && self.allowed_tools.contains(tool_name)
            && !matches!(tool_name, "tool_search" | "evidence_retrieve")
            && !is_agent_action_tool(tool_name)
    }

    fn model_delivery_requirement(
        &self,
        tool_name: &str,
        input: &str,
    ) -> crate::ToolModelDeliveryRequirement {
        crate::ToolModelDeliveryRequirement::exact(
            self.provider_model_obligation_ids(tool_name, input),
        )
    }

    async fn execute_authorized_invocation_output(
        &self,
        provider_invocation_id: &str,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        if tool_name == "checkpoint_create"
            || tool_name == "evidence_retrieve"
            || is_agent_action_tool(tool_name)
        {
            return self
                .execute_authorized_output(authorization, tool_name, input)
                .await;
        }
        if authorization.tool_id != tool_name || !self.allowed_tools.contains(tool_name) {
            return Err(ToolError::new(
                "agent tool authorization does not match the allowed tool request",
            ));
        }
        let normalized_input = normalize_delegated_resource_paths(
            tool_name,
            input,
            &self.workspace_root,
            &self.path_identity_resolver,
            self.resource_scopes.as_deref(),
        )?;
        self.enforce_resource_ceiling(tool_name, &normalized_input)?;
        self.execute_scoped(
            tool_name,
            &normalized_input,
            Some(authorization.clone()),
            Some(provider_invocation_id),
        )
        .await
        .map(harness_contract::context::ToolOutputDraft::bounded_inline)
    }

    fn has_tool(&self, tool_name: &str) -> bool {
        tool_name == "tool_search"
            || self.allowed_tools.contains(tool_name)
            || (tool_name == "checkpoint_create"
                && self
                    .host
                    .delegated_tool_effect_descriptor(tool_name, &serde_json::json!({}))
                    .is_some())
    }

    fn classify_tool_safety(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Option<crate::ToolSafetyCategory> {
        if !self.allowed_tools.contains(tool_name) {
            return None;
        }
        let input = serde_json::from_str::<serde_json::Value>(input).ok()?;
        self.host
            .delegated_tool_effect_descriptor(tool_name, &input)
            .map(|effect| crate::ToolSafetyCategory::from_effect(&effect))
    }
}
