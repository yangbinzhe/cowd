use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use harness_contract::agent::{
    AgentCommandRequest, AgentInput, AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus,
};
use harness_contract::turn::{InputSourceKind, SessionInputEnvelope};
use sha2::{Digest, Sha256};

use crate::{
    ContextProfile, PermissionMode, PermissionPolicy, RuntimeExecutionHost, RuntimeServices,
    RuntimeToolExecutionRequest, RuntimeToolExecutionStatus, Session, SharedPrompter,
    StandardRuntimeHost, StandardRuntimeHostConfig, ToolError, ToolExecutor,
};

use crate::agent_model_selector::AgentModelSelection;
use crate::agent_run_handle::{AgentBackendCapabilities, AgentBackendKind, AgentRunHandle};
use crate::agent_runtime::AgentRuntimeBackend;
use crate::execution_core::graph::ScopeLockManager;

#[path = "in_process/stages.rs"]
mod stages;
use stages::*;
#[path = "in_process/terminal.rs"]
mod terminal;
use terminal::*;
#[path = "in_process/model_loop.rs"]
mod model_loop;
#[path = "in_process/tool_turn.rs"]
mod tool_turn;
use tool_turn::*;
#[path = "in_process/path_scope.rs"]
mod path_scope;
pub(crate) use path_scope::resource_path_is_authorized;
#[path = "in_process/tool_scope.rs"]
mod tool_scope;
use path_scope::*;
#[path = "in_process/prompt.rs"]
mod prompt;
use prompt::*;
#[path = "in_process/evidence_collector.rs"]
mod evidence_collector;
use evidence_collector::*;
pub(crate) use evidence_collector::{
    structured_agent_output, structured_agent_output_for_fields,
    structured_contract_field_materialized,
};

/// Runtime-owned ToolHost bridge for an approved ProcessJsonl worker.
///
/// The external process may decide which allowed tool to request, but every
/// effect, receipt, scope lock and acceptance fact remains in the exact same
/// `ScopedRuntimeToolExecutor` used by the native worker. The process never
/// receives a raw ToolHost capability and therefore cannot mint evidence or
/// change receipts in its terminal JSON.
pub(crate) struct ProcessJsonlToolSession {
    services: Arc<RuntimeServices>,
    executor: Arc<ScopedRuntimeToolExecutor>,
    artifact_store: Arc<crate::ArtifactStore>,
    external_model_profile: String,
    topic_actions: crate::AgentActionService,
    topic_packet: AgentTaskPacket,
    topic_transport: Mutex<crate::agentic::topic_delivery::TopicTransport>,
    permission_policy: PermissionPolicy,
    authorization_negotiator: crate::AuthorizationNegotiator,
    event_store: Arc<crate::RuntimeEventStore>,
    steps: Mutex<VecDeque<Arc<crate::step_snapshot::BoundStep>>>,
}

pub(crate) fn process_tool_approval_ceiling_allows(
    policy: &PermissionPolicy,
    packet_ceiling: PermissionMode,
    required: PermissionMode,
) -> bool {
    policy.active_mode().permits(required) && packet_ceiling.permits(required)
}

pub(crate) fn process_tool_approval_context(
    packet: &AgentTaskPacket,
    policy: &harness_contract::policy::SessionExecutionPolicy,
    effective: &crate::EffectiveToolAuthorizationDescriptor,
    workspace_key: &str,
    invocation_id: &str,
) -> harness_contract::policy::ApprovalContext {
    let source = harness_contract::policy::ApprovalSource {
        kind: harness_contract::policy::ApprovalSourceKind::Agent,
        session_id: Some(packet.session_id().into()),
        agent_id: Some(packet.agent_id().into()),
        team_id: packet.team_id().map(str::to_string),
        mission_id: None, resource_ref: None, review_ref: None, application: None,
    };
    let mut context = harness_contract::policy::ApprovalContext::owned(
        &source, &effective.descriptor.tool_id, workspace_key).with_execution_policy(policy);
    context.principal_id = format!("agent:{}", packet.agent_id());
    context.invocation_id = Some(invocation_id.into());
    context.execution_id = Some(packet.graph_id().into());
    context.task_id = Some(packet.task_id().into());
    context.effect = Some(effective.descriptor.clone());
    context.resource_targets = effective.descriptor.scopes.iter()
        .filter_map(|scope| scope.target.clone()).collect();
    context
}

impl ProcessJsonlToolSession {
    pub(crate) fn prepare(
        services: &Arc<RuntimeServices>,
        packet: &AgentTaskPacket,
        selection: &AgentModelSelection,
    ) -> Result<Self, String> {
        let binding = packet.binding.as_ref().ok_or_else(|| {
            "ProcessJsonl tool bridge requires a Runtime-compiled Binding".to_string()
        })?;
        binding.validate().map_err(|error| error.to_string())?;
        if packet
            .allowed_tools
            .iter()
            .any(|tool| !binding.tool_contract_refs.contains(tool))
        {
            return Err("AgentTaskPacket tool allow-list exceeds its Binding contract".to_string());
        }
        let execution_graph = services
            .graph_state_store()
            .load(packet.graph_id())
            .map_err(|error| {
                format!(
                    "ProcessJsonl Agent graph `{}` is unavailable: {error}",
                    packet.graph_id()
                )
            })?;
        let lineage = execution_graph.lineage.as_ref().ok_or_else(|| {
            format!(
                "ProcessJsonl Agent graph `{}` has no canonical Session/Turn/Task lineage",
                packet.graph_id()
            )
        })?;
        lineage.validate().map_err(str::to_string)?;
        if lineage.session_id != packet.session_id()
            || lineage.root_task_id != packet.assignment.root_task_id
        {
            return Err(format!(
                "ProcessJsonl AgentTaskPacket lineage does not match parent graph `{}`",
                packet.graph_id()
            ));
        }
        let host = services.tool_execution_host().cloned().ok_or_else(|| {
            "RuntimeServices has no ToolHost for the ProcessJsonl bridge".to_string()
        })?;
        let packet_allowed_tools = packet
            .allowed_tools
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let bounded_resource_lease = packet.team_id().is_some()
            || packet.agentic_binding.is_some()
            || binding.evaluation.is_some();
        let requested_tool_names = packet_allowed_tools.iter().cloned().collect::<Vec<_>>();
        let discovery_lease = host.pin_step_catalog();
        let discovery_metadata = discovery_lease
            .as_ref()
            .map(tools::ToolHostLease::catalog_receipt);
        let projection = discovery_lease
            .as_ref()
            .zip(discovery_metadata.as_ref())
            .map(|(lease, metadata)| {
                crate::tool_host::DelegatedCatalogProjection::new(
                    lease,
                    metadata,
                    &packet_allowed_tools,
                )
            });
        let definitions = projection
            .as_ref()
            .map(|projection| projection.definitions().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let exposed = host.delegated_tool_definitions(&requested_tool_names);
        if definitions.iter().any(|definition| {
            !exposed.iter().any(|actual| {
                actual.name == definition.name
                    && actual.input_schema == definition.input_schema
                    && actual.description == definition.description
            })
        }) {
            return Err("agent_tool_inventory_drift: host exposure differs from catalog".into());
        }
        let allowed_tools = definitions
            .into_iter()
            .filter(|definition| {
                projection.as_ref().is_some_and(|projection| {
                    projection
                        .discover(
                            definition,
                            &packet_allowed_tools,
                            packet.permission_ceiling,
                            bounded_resource_lease.then_some(packet.resource_scopes.as_slice()),
                        )
                        .is_some()
                })
            })
            .map(|definition| definition.name)
            .collect::<BTreeSet<_>>();
        let unavailable_tools = packet_allowed_tools
            .difference(&allowed_tools)
            .filter(|tool| !matches!(tool.as_str(), "context_retrieve" | "evidence_retrieve"))
            .cloned()
            .collect::<Vec<_>>();
        if !unavailable_tools.is_empty() {
            return Err(format!(
                "agent_tool_inventory_drift: admitted Tool contracts are unavailable or no longer bounded on the active host: {}",
                unavailable_tools.join(", ")
            ));
        }
        let memory_context = memory::MemoryTurnContext::new(
            packet.session_id(),
            binding.instance.instance_id.clone(),
        )
        .with_definition_lineage_id(Some(
            binding.definition_ref.definition_id.as_str().to_string(),
        ))
        .with_project_id(Some(crate::memory_project_id_for_workspace(
            services.workspace_root(),
        )))
        .with_task_id(Some(binding.data_lease.task_id.clone()))
        .with_team_id(binding.data_lease.team_id.clone())
        .with_cognitive_read_scopes(binding.data_lease.read_scopes.clone());
        let live_control = services.session_execution_policy_control(packet.session_id());
        let session_policy = live_control
            .as_ref()
            .map(crate::permissions::SessionExecutionPolicyControl::snapshot)
            .ok_or_else(|| {
                format!(
                    "agent_session_policy_missing: session `{}` has no executable policy snapshot",
                    packet.session_id()
                )
            })?;
        if packet.policy_revision != 0 && packet.policy_revision != session_policy.revision {
            return Err(format!(
                "agent_policy_revision_stale: packet rev {} current rev {}; replan before tool execution",
                packet.policy_revision, session_policy.revision
            ));
        }
        let commit_service = crate::execution_core::graph::ExecutionCommitService::new(Arc::clone(
            services.event_store(),
        ))
        .with_artifact_store(Arc::clone(services.artifact_store()));
        let durable_receipts = commit_service
            .load_delegated_agent_tool_receipts(packet.graph_id(), packet.node_id(), packet.attempt)
            .map_err(|error| {
                format!(
                    "ProcessJsonl tool receipt recovery is invalid for {}:{}:{}: {error}",
                    packet.graph_id(),
                    packet.node_id(),
                    packet.attempt
                )
            })?
            .into_iter()
            .map(scoped_receipt_from_durable)
            .collect::<Vec<_>>();
        let recovered_sequence = commit_service
            .delegated_observation_high_water(packet.graph_id(), packet.node_id(), packet.attempt)
            .map_err(|error| error.to_string())?;
        let provider_model_obligations = packet
            .required_acceptance
            .evidence_obligations
            .iter()
            .filter(|obligation| {
                obligation.observation_requirement
                    == harness_contract::context::EvidenceObservationRequirement::ProviderModel
            })
            .cloned()
            .collect();
        let process_permission_policy = permission_policy(
            live_control.clone(),
            packet.permission_ceiling,
            &allowed_tools,
        );
        Ok(Self {
            services: Arc::clone(services),
            executor: Arc::new(ScopedRuntimeToolExecutor {
                tool_batch: Some(crate::execution_core::graph::executors::agent_tool::AgentToolBatchDispatcher::new(services, packet)),
                host,
                allowed_tools,
                session_id: packet.session_id().to_string(),
                sandbox_posture: session_policy.sandbox_posture,
                policy_revision: session_policy.revision,
                memory_context,
                reality_context: Some(binding.data_lease.clone()),
                model_lease: selection.model.clone(),
                execution_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
                attempt: packet.attempt,
                workspace_root: services.workspace_root().to_path_buf(),
                path_identity_resolver: Arc::clone(services.path_identity_resolver()),
                scope_locks: Arc::clone(services.scope_locks()),
                commit_service: Some(commit_service),
                resource_scopes: bounded_resource_lease.then(|| packet.resource_scopes.clone()),
                managed_invocation: packet.managed_invocation.clone(),
                next_receipt_sequence: AtomicU64::new(recovered_sequence),
                receipts: Mutex::new(durable_receipts),
                provider_model_obligations,
            }),
            artifact_store: Arc::clone(services.artifact_store()),
            external_model_profile: selection.model.clone(),
            topic_actions: services.agent_action_service(),
            topic_packet: packet.clone(),
            topic_transport: Mutex::new(Default::default()),
            permission_policy: process_permission_policy,
            authorization_negotiator: crate::AuthorizationNegotiator::new(),
            event_store: Arc::clone(services.event_store()),
            steps: Mutex::new(VecDeque::new()),
        })
    }

    pub(crate) fn working_context_delta(
        &self,
        handle: &tokio::runtime::Handle,
    ) -> Result<Option<serde_json::Value>, String> {
        self.validate_current_policy()?;
        let window = handle.block_on(
            self.services
                .working_context_window(&self.executor.memory_context),
        )?;
        Ok((window["coverage"]["active_references"].as_u64() != Some(0)).then_some(window))
    }

    pub(crate) fn topic_delta(&self) -> Result<Option<serde_json::Value>, String> {
        self.topic_transport
            .lock()
            .map_err(|_| "Topic transport lock poisoned".to_string())?
            .issue(&self.topic_actions, &self.topic_packet)
    }

    pub(crate) fn acknowledge_topic_delta(&self, delivery_id: &str) -> Result<(), String> {
        self.topic_transport
            .lock()
            .map_err(|_| "Topic transport lock poisoned".to_string())?
            .acknowledge(&self.topic_actions, delivery_id)
    }

    pub(crate) fn validate_current_policy(&self) -> Result<(), String> {
        let snapshot = self.permission_policy.execution_policy_control().snapshot();
        if snapshot.revision != self.executor.policy_revision {
            return Err(
                "ProcessJsonl packet policy revision is stale; reauthorize before continuing"
                    .into(),
            );
        }
        Ok(())
    }

    pub(crate) fn begin_step(&self) -> Result<serde_json::Value, String> {
        let snapshot = self.permission_policy.execution_policy_control().snapshot();
        if snapshot.revision != self.executor.policy_revision {
            return Err(
                "ProcessJsonl packet policy revision is stale; reauthorize before continuing"
                    .into(),
            );
        }
        let ceiling = self
            .permission_policy
            .bound_to_snapshot(&snapshot)
            .active_mode();
        let lease = self
            .executor
            .prepare_step_catalog(ceiling)
            .map_err(|error| error.to_string())?
            .ok_or("Process step requires a pinned ToolHost")?;
        let discovery = self
            .executor
            .tool_discovery_receipt_for_step(&lease, ceiling);
        let bootstrap = discovery
            .descriptors
            .iter()
            .any(|descriptor| descriptor.canonical_id == "tool_search");
        let mut advertised_names = self.executor.allowed_tools.clone();
        if bootstrap {
            advertised_names.insert("tool_search".into());
        }
        let definitions = lease
            .snapshot()
            .catalog
            .definitions(Some(&advertised_names))
            .into_iter()
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        let expected_count = self.executor.allowed_tools.len()
            + usize::from(bootstrap && !self.executor.allowed_tools.contains("tool_search"));
        if definitions.len() != expected_count {
            return Err("Process advertised tools do not match the pinned catalog".into());
        }
        let binding = self.topic_packet.agentic_binding.as_ref();
        let identity = harness_contract::step::StepIdentity {
            step_id: uuid::Uuid::new_v4().to_string(),
            session_id: self.executor.session_id.clone(),
            turn_id: self
                .topic_packet
                .assignment
                .execution_identity
                .turn_id()
                .ok_or("Process step requires an admitted turn identity")?
                .to_string(),
            execution_id: Some(self.executor.execution_id.clone()),
            actor_ref: Some(self.topic_packet.agent_id().to_string()),
            run_ref: Some(self.topic_packet.run_id().to_string()),
            attempt: Some(self.executor.attempt),
            epoch: Some(self.topic_packet.expected_graph_revision.to_string()),
            goal_ref: None,
            program_ref: binding.map(|binding| binding.program_id.clone()),
            task_ref: binding.and_then(|binding| match &binding.focus {
                harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref } => {
                    Some(task_ref.clone())
                }
                _ => None,
            }),
            build_ref: option_env!("COWD_GIT_SHA")
                .unwrap_or("unverified")
                .to_string(),
        };
        let step = Arc::new(crate::step_snapshot::BoundStep::bind(
            identity, lease, &definitions,
            &serde_json::json!({"model":self.external_model_profile,"control":"external_configured"}),
            &serde_json::json!({"claim":"runtime_boundary_only","worker_run":self.topic_packet.run_id()}),
            BTreeMap::from([
                ("worker_run".to_string(), self.topic_packet.run_id().to_string()),
                ("policy".to_string(), snapshot.revision.to_string()),
                ("attempt".to_string(), self.executor.attempt.to_string()),
            ]), chrono::Utc::now().timestamp_millis().saturating_add(3_600_000),
        ).map_err(|error| error.to_string())?);
        self.event_store
            .append(crate::runtime_event_store::RuntimeEventInput {
                stream_id: format!("session:{}", self.executor.session_id),
                scope: crate::runtime_event_store::RuntimeEventScope::Session,
                kind: "process.step_bound".to_string(),
                status: Some("prepared".to_string()),
                actor: Some(self.topic_packet.agent_id().to_string()),
                refs: vec![],
                payload: serde_json::to_value(step.snapshot())
                    .map_err(|error| error.to_string())?,
            })
            .map_err(|error| error.to_string())?;
        let response = serde_json::json!({"step":step.snapshot(),"tools":definitions,"claim":"runtime_boundary_only"});
        let mut steps = self
            .steps
            .lock()
            .map_err(|_| "Process step registry poisoned")?;
        if steps.len() == 16 {
            steps.pop_front();
        }
        steps.push_back(step);
        Ok(response)
    }

    pub(crate) async fn execute_step_tool(
        &self,
        step_id: &str,
        sequence: u64,
        request_id: &str,
        tool_name: &str,
        input: &str,
    ) -> Result<String, String> {
        let step = self
            .steps
            .lock()
            .map_err(|_| "Process step registry poisoned")?
            .iter()
            .find(|step| step.snapshot().identity.step_id == step_id)
            .cloned()
            .ok_or("Process step is missing, foreign, expired or evicted")?;
        let binding = crate::step_snapshot::StepCallBinding::process(
            step.clone(),
            self.topic_packet.run_id(),
            sequence,
            request_id,
        );
        step.validate_call(
            &binding.origin,
            &self.executor.session_id,
            request_id,
            tool_name,
            input,
            chrono::Utc::now().timestamp_millis(),
        )
        .map_err(|error| error.to_string())?;
        self.execute_tool_bound(Some(&binding), request_id, tool_name, input)
            .await
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub(crate) async fn execute_tool(
        &self,
        request_id: &str,
        tool_name: &str,
        input: &str,
    ) -> Result<String, String> {
        self.execute_tool_bound(None, request_id, tool_name, input)
            .await
    }

    async fn execute_tool_bound(
        &self,
        binding: Option<&crate::step_snapshot::StepCallBinding>,
        request_id: &str,
        tool_name: &str,
        input: &str,
    ) -> Result<String, String> {
        self.validate_current_policy()?;
        if tool_name == "checkpoint" {
            return Err("checkpoint_create is Runtime-internal".into());
        }

        let value = serde_json::from_str(input)
            .map_err(|e| format!("invalid ProcessJsonl tool input: {e}"))?;
        let effect = self
            .executor
            .registered_tool_effect(tool_name, &value)
            .ok_or_else(|| {
                format!("tool `{tool_name}` is outside the admitted Agent tool contracts")
            })?;
        let snapshot = self.permission_policy.execution_policy_control().snapshot();
        let policy = self.permission_policy.bound_to_snapshot(&snapshot);
        let authorization_id = format!(
            "process-tool:{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    &self.executor.execution_id,
                    self.executor.attempt,
                    request_id
                ))
                .map_err(|e| e.to_string())?
            )
        );
        let authorization_request = crate::AuthorizationRequest {
            principal_id: format!("agent:{}", self.topic_packet.agent_id()),
            capability: tool_name.into(),
            input: input.into(),
            idempotency_key: authorization_id.clone(),
            effect,
            parent_ceiling: self.topic_packet.permission_ceiling,
            parent_lease_id: None,
            policy_revision: snapshot.revision,
            recovery_scope: format!("execution:{}", self.executor.execution_id),
            context: crate::PermissionContext::default(),
            safe_alternatives: vec![],
        };
        let evaluated = self
            .authorization_negotiator
            .assess_effective(&policy, &authorization_request);
        let (evaluated, approved_grant) = {
            let mut evaluated = evaluated;
            let mut approved_grant = None;
            if tool_name == "execute_code"
                && evaluated.assessment.lease.is_none()
                && evaluated.assessment.path
                    == harness_contract::policy::AuthorizationPath::HumanApproval
                && process_tool_approval_ceiling_allows(
                    &policy,
                    self.topic_packet.permission_ceiling,
                    evaluated.effective.descriptor.required_permission,
                )
            {
                let context = process_tool_approval_context(
                    &self.topic_packet,
                    &snapshot,
                    &evaluated.effective,
                    self.executor.path_identity_resolver.workspace_id(),
                    &authorization_id,
                );
                let risk = match evaluated.assessment.risk {
                    harness_contract::policy::RiskLevel::Low => {
                        harness_contract::core::TaskRisk::Low
                    }
                    harness_contract::policy::RiskLevel::Medium => {
                        harness_contract::core::TaskRisk::Medium
                    }
                    harness_contract::policy::RiskLevel::High => {
                        harness_contract::core::TaskRisk::High
                    }
                    harness_contract::policy::RiskLevel::Critical => {
                        harness_contract::core::TaskRisk::Critical
                    }
                };
                if let Some(grant) = self
                    .services
                    .approval_queue()
                    .matching_exact_once_grant(&context, risk)
                {
                    let approved = self.authorization_negotiator.approve_effective(
                        &policy,
                        &authorization_request,
                        &evaluated.effective,
                        &grant,
                    );
                    if approved.lease.is_some() {
                        evaluated.assessment = approved;
                        approved_grant = Some((grant, context, risk));
                    }
                }
            }
            (evaluated, approved_grant)
        };
        #[cfg(test)]
        if tool_name == "execute_code" {
            eprintln!(
                "Own848_process_authorization {}",
                serde_json::json!({
                    "packet_ceiling": self.topic_packet.permission_ceiling,
                    "active_mode": policy.active_mode(),
                    "registered_required": authorization_request.effect.required_permission,
                    "effective_required": evaluated.effective.descriptor.required_permission,
                    "path": evaluated.assessment.path,
                    "gap": evaluated.assessment.gap.as_ref().map(|gap| gap.kind),
                    "lease_ceiling": evaluated.assessment.lease.as_ref().map(|lease| lease.ceiling),
                    "lease_permits": evaluated.assessment.lease.as_ref().map(|lease|
                        lease.permits(&evaluated.assessment.capability, evaluated.assessment.required_mode)),
                    "capability_matches": evaluated.assessment.capability == evaluated.effective.descriptor.tool_id,
                    "effect_matches": evaluated.assessment.effect == evaluated.effective.descriptor.assessment,
                    "scopes_match": evaluated.assessment.requested_scopes == evaluated.effective.descriptor.scopes,
                    "required_matches": evaluated.assessment.required_mode == evaluated.effective.descriptor.required_permission,
                    "lease_policy_revision": evaluated.assessment.lease.as_ref().map(|lease| lease.policy_revision),
                    "lease_descriptor_matches": evaluated.assessment.lease.as_ref().map(|lease|
                        lease.effect_descriptor_hash == evaluated.effective.descriptor.descriptor_hash),
                })
            );
        }
        let persist_authorization = || -> Result<(), String> {
            let _ = self
                .authorization_negotiator
                .take_transitions_for_persistence();
            for transition in self
                .authorization_negotiator
                .transitions_awaiting_persistence()
            {
                crate::authorization_negotiator::persist_authorization_transition(
                    &self.event_store,
                    &format!("authorization-lease:{}", transition.lease.lease_id),
                    "runtime.process_jsonl",
                    &transition,
                )?;
                self.authorization_negotiator
                    .acknowledge_persisted_transitions(std::slice::from_ref(
                        &transition.transition_id,
                    ));
            }
            Ok(())
        };
        if evaluated.assessment.lease.is_none() {
            persist_authorization()?;
        }
        let lease = evaluated.assessment.lease.clone().ok_or_else(|| {
            evaluated.assessment.gap.as_ref().map_or_else(
                || "ProcessJsonl tool authorization unavailable".to_string(),
                |gap| format!("ProcessJsonl tool authorization denied: {}", gap.reason),
            )
        })?;
        let lease_id = lease.lease_id.clone();
        let decision = crate::ToolPolicy
            .authorize(
                &evaluated.effective,
                &evaluated.assessment,
                authorization_id.clone(),
                lease,
                60,
            )
            .map_err(|error| {
                self.authorization_negotiator.revoke(&lease_id);
                error.to_string()
            });
        let decision = match decision {
            Ok(decision) => decision,
            Err(error) => {
                persist_authorization()?;
                return Err(error);
            }
        };
        if tool_name == "execute_code" {
            let current = self.permission_policy.execution_policy_control().snapshot();
            if current.revision != snapshot.revision
                || !process_tool_approval_ceiling_allows(
                    &self.permission_policy.bound_to_snapshot(&current),
                    self.topic_packet.permission_ceiling,
                    evaluated.effective.descriptor.required_permission,
                )
            {
                self.authorization_negotiator.revoke(&lease_id);
                persist_authorization()?;
                return Err("ProcessJsonl policy changed before approved execution".into());
            }
        }
        if let Some((grant, context, risk)) = approved_grant {
            if let Err(error) = self.services.approval_queue().consume_exact_once_grant(
                &grant.grant_id,
                &context,
                risk,
            ) {
                self.authorization_negotiator
                    .revoke(&decision.authorization.authorization_lease.lease_id);
                persist_authorization()?;
                return Err(format!(
                    "ProcessJsonl exact approval consumption failed: {error}"
                ));
            }
        }
        if let Err(error) = persist_authorization() {
            self.authorization_negotiator.revoke(&lease_id);
            let revoke_result = persist_authorization();
            return Err(match revoke_result {
                Ok(()) => format!("ProcessJsonl authorization persistence failed; lease revoked: {error}"),
                Err(revoke_error) => format!("ProcessJsonl authorization persistence failed: {error}; lease disabled in memory, durable revoke unconfirmed: {revoke_error}"),
            });
        }
        self.executor
            .execute_step_authorized_invocation_output(
                binding,
                &authorization_id,
                &decision.authorization,
                tool_name,
                input,
            )
            .await
            .map(|output| output.model_text().to_string())
            .map_err(|error| error.to_string())
    }


    fn transport_request_identity(&self, request_id: &str) -> (String, String) {
        let packet = &self.topic_packet;
        let scope = serde_json::json!([
            packet.session_id(),
            packet.graph_id(),
            packet.node_id(),
            packet.run_id(),
            packet.agent_id(),
            self.executor.attempt,
            packet
                .binding
                .as_ref()
                .map(|binding| binding.binding_digest.as_str())
        ]);
        (
            format!(
                "agent-process-transport:{:x}",
                Sha256::digest(scope.to_string().as_bytes())
            ),
            format!("{:x}", Sha256::digest(request_id.as_bytes())),
        )
    }

    /// Protocol replay metadata only. Physical effect and acceptance authority
    /// remains with the original ToolHost invocation and Session receipts.
    pub(crate) fn replay_or_begin_transport_request(
        &self,
        request_id: &str,
        fingerprint: &str,
    ) -> Result<Option<serde_json::Value>, String> {
        self.validate_current_policy()?;
        if request_id.trim().is_empty() {
            return Err("ProcessJsonl request_id must not be empty".into());
        }
        let (stream, key) = self.transport_request_identity(request_id);
        for stage in ["completed", "started"] {
            if let Some(record) = self
                .event_store
                .event_by_idempotency_key(&stream, &format!("{stage}:{key}"))
                .map_err(|error| error.to_string())?
            {
                if record.payload["fingerprint"].as_str() != Some(fingerprint)
                    || record.payload["request_id"].as_str() != Some(request_id)
                {
                    return Err(
                        "ProcessJsonl request_id was reused for a different tool invocation".into(),
                    );
                }
                if stage == "completed" {
                    let response = record
                        .payload
                        .get("response")
                        .filter(|response| response.is_object())
                        .cloned()
                        .ok_or("ProcessJsonl durable transport response is corrupt")?;
                    return Ok(Some(response));
                }
                return Err("ProcessJsonl prior request has unresolved transport completion; reconcile its recorded effects before retrying, do not blindly repeat the tool".into());
            }
        }
        if self.record_transport_request("started", request_id, fingerprint, None)? {
            return Err("ProcessJsonl prior request has unresolved transport completion; another reader already owns this request, reconcile its recorded effects before retrying".into());
        }
        Ok(None)
    }

    pub(crate) fn complete_transport_request(
        &self,
        request_id: &str,
        fingerprint: &str,
        response: &serde_json::Value,
    ) -> Result<(), String> {
        self.record_transport_request("completed", request_id, fingerprint, Some(response))
            .map(|_| ())
    }

    pub(crate) async fn bounded_transport_response(
        &self,
        request_id: &str,
        tool_name: &str,
        mut response: serde_json::Value,
        frame_bytes: usize,
    ) -> Result<serde_json::Value, String> {
        use crate::context_evidence::raw::{
            RawEvidenceStore, RawEvidenceWrite, SessionPortRawEvidenceStore,
        };
        let encoded_bytes = serde_json::to_vec(&response)
            .map_err(|error| error.to_string())?
            .len();
        if encoded_bytes.saturating_add(256) <= frame_bytes {
            return Ok(response);
        }
        let Some(serde_json::Value::String(output)) = response
            .as_object_mut()
            .and_then(|object| object.remove("output"))
        else {
            return Err("Process response metadata exceeds the original frame budget".into());
        };
        if crate::context_evidence::read_body_seal(tool_name, &output).is_none() {
            return Err(
                "Process response exceeds its frame budget without a complete readable source"
                    .into(),
            );
        }
        let journal = self
            .services
            .session_journal_port()
            .ok_or("Process large read requires the original Session evidence journal")?;
        let output_sha256 = crate::context_evidence::read_sha256(output.as_bytes());
        let identity =
            serde_json::to_vec(&(self.topic_packet.run_id(), request_id, &output_sha256))
                .map_err(|error| error.to_string())?;
        let evidence_id = format!("process-read:{:x}", Sha256::digest(identity));
        let access = SessionPortRawEvidenceStore::new(journal, Arc::clone(&self.artifact_store))
            .persist(RawEvidenceWrite {
                evidence_ref: harness_contract::reality::EvidenceRef::observed("tool", evidence_id),
                session_id: self.topic_packet.session_id().into(),
                media_type: "application/json".into(),
                visibility_scope: format!("session:{}", self.topic_packet.session_id()),
                payload: output.clone(),
                metadata: serde_json::json!({
                    "source":"process_tool_output",
                    "request_id_sha256":crate::context_evidence::read_sha256(request_id.as_bytes()),
                    "run_id":self.topic_packet.run_id(), "tool_name":tool_name,
                }),
            })
            .await
            .map_err(|error| error.to_string())?;
        let recovery = if self
            .topic_packet
            .allowed_tools
            .iter()
            .any(|tool| tool == "evidence_retrieve")
        {
            serde_json::json!({
                "tool_name":"evidence_retrieve",
                "input":{"evidence_ref":access.retrieval_selector,"limit":8},
            })
        } else {
            serde_json::json!({"status":"unavailable_in_immutable_packet",
                "reason":"Ask the original owner for authorized bounded access to this immutable output"})
        };
        let mut preview_chars = (frame_bytes / 16).min(4_000);
        loop {
            let preview = output.chars().take(preview_chars).collect::<String>();
            let bounded = serde_json::json!({
                "kind":"process_tool_preview", "truncated":true,
                "original_output_ref":access.retrieval_selector,
                "original_output_sha256":output_sha256,
                "original_output_bytes":access.bytes,
                "content_preview":preview, "next_request":recovery,
            })
            .to_string();
            let mut candidate = response.clone();
            candidate["output"] = serde_json::Value::String(bounded);
            if serde_json::to_vec(&candidate)
                .map_err(|error| error.to_string())?
                .len()
                .saturating_add(256)
                <= frame_bytes
            {
                response = candidate;
                return Ok(response);
            }
            if preview_chars == 0 {
                return Err("Process response metadata exceeds the original frame budget".into());
            }
            preview_chars /= 2;
        }
    }

    pub(crate) fn process_read_confirmation_revision(
        &self,
        tool_name: &str,
        response: &serde_json::Value,
    ) -> Result<Option<u64>, String> {
        if !matches!(tool_name, "read_file" | "read_ref" | "evidence_retrieve")
            || response.get("error").is_some()
        {
            return Ok(None);
        }
        self.validate_current_policy()?;
        self.event_store
            .stream_revision(&format!("session:{}", self.topic_packet.session_id()))
            .map(Some)
            .map_err(|error| error.to_string())
    }

    pub(crate) fn record_process_read_delivery(
        &self,
        tool_name: &str,
        fingerprint: &str,
        sent: &serde_json::Value,
        frame_sha256: &str,
        frame_bytes: usize,
        confirmation_revision: u64,
    ) -> Result<(), String> {
        use crate::context_evidence::{
            ReadDeliveryGrade, ReadDeliveryMaterial, ReadDeliveryTransport,
        };
        if !matches!(tool_name, "read_file" | "read_ref" | "evidence_retrieve")
            || sent.get("error").is_some()
        {
            return Ok(());
        }
        self.validate_current_policy()?;
        let packet = &self.topic_packet;
        let request_id = sent["request_id"]
            .as_str()
            .ok_or("sent Process read lacks request identity")?;
        let step_id = sent["origin_step_id"]
            .as_str()
            .ok_or("sent Process read lacks bound step")?;
        let output = sent["output"]
            .as_str()
            .ok_or("sent Process read lacks output")?;
        let sequence = sent["sequence"]
            .as_u64()
            .ok_or("sent Process frame lacks sequence")?;
        let frame = serde_json::to_vec(sent).map_err(|error| error.to_string())?;
        if sent["run_id"] != packet.run_id()
            || sent["agent_id"] != packet.agent_id()
            || sent["protocol_version"] != 3
            || frame.len() != frame_bytes
            || crate::context_evidence::read_sha256(&frame) != frame_sha256
        {
            return Err("sent Process frame differs from its authenticated worker output".into());
        }
        let (transport_stream, transport_key) = self.transport_request_identity(request_id);
        let completed = self
            .event_store
            .event_by_idempotency_key(&transport_stream, &format!("completed:{transport_key}"))
            .map_err(|error| error.to_string())?
            .ok_or("sent Process response has no original completed transport record")?;
        if completed.kind != "agent.process_request_completed"
            || completed.payload["fingerprint"] != fingerprint
            || completed.payload["response"]["output"] != sent["output"]
            || completed.payload["response"]["origin_step_id"] != step_id
        {
            return Err("sent Process response does not match original transport identity".into());
        }
        let index = format!(
            "execution-agent-receipts:{}:{}:{}",
            packet.graph_id(),
            packet.node_id(),
            packet.attempt
        );
        let head = self
            .event_store
            .stream_revision(&index)
            .map_err(|error| error.to_string())?;
        let mut after = 0;
        let mut acquisition = None;
        while after < head {
            let events =
                self.event_store
                    .list_stream_after(&index, after, head, 128, 2 * 1024 * 1024)?;
            if events.is_empty() {
                return Err("Process read acquisition index is incomplete".into());
            }
            for event in events {
                if event.sequence <= after || event.sequence > head {
                    return Err("Process read acquisition cursor is invalid".into());
                }
                after = event.sequence;
                if event.kind != "execution.agent_tool.receipt"
                    || event.actor.as_deref() != Some("governed_tool")
                {
                    continue;
                }
                let Some(value) = event
                    .payload
                    .get(crate::context_evidence::READ_ACQUISITION_FIELD)
                else {
                    continue;
                };
                let Ok(seal) = crate::context_evidence::decode_read_acquisition(value) else {
                    continue;
                };
                if seal.invocation_id != request_id
                    || seal.tool_name != tool_name
                    || seal.step.step_id != step_id
                    || seal.index_stream != index
                    || seal.attempt != packet.attempt
                    || seal.graph_id != packet.graph_id()
                    || seal.node_id != packet.node_id()
                    || seal.session_id != packet.session_id()
                    || seal.step.run_ref.as_deref() != Some(packet.run_id())
                    || event.idempotency_key.as_deref() != Some(seal.index_key.as_str())
                {
                    continue;
                }
                let origin: harness_contract::step::CallOrigin =
                    serde_json::from_value(seal.origin.clone())
                        .map_err(|error| error.to_string())?;
                if !matches!(origin.source, harness_contract::step::CallSource::Process { worker_id, .. }
                    if worker_id == packet.run_id())
                {
                    continue;
                }
                let primary = self
                    .event_store
                    .event_by_idempotency_key(&seal.primary_stream, &seal.primary_key)
                    .map_err(|error| error.to_string())?
                    .ok_or("Process read primary receipt is missing")?;
                if primary.kind != "execution.read.receipt"
                    || primary.actor.as_deref() != Some("governed_tool")
                    || primary.transaction_id != event.transaction_id
                    || primary
                        .payload
                        .get(crate::context_evidence::READ_ACQUISITION_FIELD)
                        != Some(value)
                    || primary.payload["outcome"]["status"] != "executed"
                {
                    return Err(
                        "Process read primary and index do not authenticate one acquisition".into(),
                    );
                }
                if acquisition
                    .replace((seal, primary, event.event_id))
                    .is_some()
                {
                    return Err("Process read has ambiguous acquisition identity".into());
                }
            }
        }
        let Some((seal, primary, index_event_id)) = acquisition else {
            return Ok(());
        };
        let body_omitted =
            crate::context_evidence::read_body_seal(tool_name, output).as_ref() != Some(&seal.body);
        let material = ReadDeliveryMaterial {
            schema_version: 1,
            grade: ReadDeliveryGrade::AgentChannelSend,
            acquisition_event_id: primary.event_id,
            acquisition_cursor: primary.commit_cursor,
            index_event_id,
            session_id: packet.session_id().into(),
            execution_id: packet.graph_id().into(),
            turn_id: packet
                .assignment
                .execution_identity
                .turn_id()
                .ok_or("Process read recipient has no turn identity")?
                .into(),
            tool_invocation_id: request_id.into(),
            recipient: crate::context_evidence::ReadDeliveryRecipient::AgentTask {
                identity: packet.assignment.execution_identity.clone(),
            },
            presentation_sha256: crate::context_evidence::read_sha256(output.as_bytes()),
            presentation_bytes: output.len() as u64,
            delivered_extent: (!body_omitted).then(|| seal.body.extent.clone()),
            body_omitted,
            acquisition: seal,
            transport: ReadDeliveryTransport::ProcessChannelSend {
                worker_id: packet.run_id().into(),
                request_id: request_id.into(),
                sequence,
                step_id: step_id.into(),
                request_fingerprint: fingerprint.into(),
                response_sha256: frame_sha256.into(),
                response_bytes: frame_bytes as u64,
            },
        };
        let payload = serde_json::to_value(material).map_err(|error| error.to_string())?;
        crate::context_evidence::decode_read_delivery(&payload).map_err(str::to_string)?;
        let stream = format!("session:{}", packet.session_id());
        let key = format!("process-read:{}:{sequence}", packet.run_id());
        if let Some(existing) = self
            .event_store
            .event_by_idempotency_key(&stream, &key)
            .map_err(|error| error.to_string())?
        {
            return if existing.kind == crate::context_evidence::PROCESS_DELIVERY_KIND
                && existing.actor.as_deref() == Some("runtime.process_jsonl")
                && existing.status.as_deref() == Some("sent")
                && existing.schema_version == 1
                && existing.scope == crate::RuntimeEventScope::Session
                && existing.payload == payload
            {
                Ok(())
            } else {
                Err("conflicting Process read delivery material".into())
            };
        }
        self.event_store
            .append_transaction(crate::AppendTransactionRequest {
                transaction_id: format!(
                    "process-read:{}",
                    crate::context_evidence::read_sha256(key.as_bytes())
                ),
                expected_streams: vec![crate::ExpectedStreamRevision {
                    stream_id: stream.clone(),
                    expected_revision: confirmation_revision,
                }],
                events: vec![crate::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: stream,
                        scope: crate::RuntimeEventScope::Session,
                        kind: crate::context_evidence::PROCESS_DELIVERY_KIND.into(),
                        status: Some("sent".into()),
                        actor: Some("runtime.process_jsonl".into()),
                        refs: vec![
                            crate::RuntimeEventRef {
                                kind: "read_acquisition".into(),
                                id: payload["acquisition_event_id"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .into(),
                            },
                            crate::RuntimeEventRef {
                                kind: "tool_invocation".into(),
                                id: request_id.into(),
                            },
                        ],
                        payload,
                    },
                    idempotency_key: Some(key),
                    schema_version: 1,
                }],
            })
            .map_err(|error| {
                format!("Process delivery confirmation unknown after actual send: {error}")
            })?;
        Ok(())
    }

    fn record_transport_request(
        &self,
        stage: &str,
        request_id: &str,
        fingerprint: &str,
        response: Option<&serde_json::Value>,
    ) -> Result<bool, String> {
        let (stream, key) = self.transport_request_identity(request_id);
        let head = self
            .event_store
            .stream_revision(&stream)
            .map_err(|error| error.to_string())?;
        self.event_store.append_transaction(crate::AppendTransactionRequest {
            transaction_id: format!("{stream}:{stage}:{key}"),
            expected_streams: vec![crate::ExpectedStreamRevision {stream_id: stream.clone(), expected_revision: head}],
            events: vec![crate::RuntimeTransactionEventInput {
                event: crate::RuntimeEventInput {
                    stream_id: stream, scope: crate::RuntimeEventScope::Agent,
                    kind: format!("agent.process_request_{stage}"), status: Some(stage.into()),
                    actor: Some(self.topic_packet.agent_id().into()),
                    refs: vec![crate::RuntimeEventRef {kind:"agent_run".into(), id:self.topic_packet.run_id().into()},
                        crate::RuntimeEventRef {kind:"session".into(), id:self.topic_packet.session_id().into()},
                        crate::RuntimeEventRef {kind:"execution_graph".into(), id:self.topic_packet.graph_id().into()}],
                    payload: serde_json::json!({"request_id":request_id,"fingerprint":fingerprint,
                        "response":response,"authority":"transport_replay_only"}),
                }, idempotency_key: Some(format!("{stage}:{key}")), schema_version: 1,
            }],
        }).map(|receipt| receipt.duplicate).map_err(|error| error.to_string())
    }

    pub(crate) fn artifact_store(&self) -> &Arc<crate::ArtifactStore> {
        &self.artifact_store
    }

    /// Replace all child-authored receipt/evidence claims with facts derived
    /// from canonical ToolHost receipts. Provider-model observation is left
    /// unsatisfied for this bridge because an arbitrary child process cannot
    /// attest that it semantically consumed a source; the evaluator records
    /// that gap instead of pretending the external model saw it.
    pub(crate) fn apply_terminal(
        &self,
        packet: &AgentTaskPacket,
        terminal: &mut AgentReturnPacket,
    ) {
        let receipts = self
            .executor
            .receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let observed_evidence =
            model_observed_evidence(&packet.required_acceptance, &[], &receipts);
        let evidence_refs = agent_evidence_refs(packet, &[], &receipts);
        let (acceptance, runtime_change_receipts) = derive_receipt_backed_satisfied_criteria(
            packet,
            &terminal.outcome,
            &evidence_refs,
            &self.executor,
            &observed_evidence,
        );
        let required = crate::acceptance_evaluator::AcceptanceEvaluator::effective_required(
            &packet.required_acceptance,
            &packet.acceptance,
        );
        let (observed_acceptance, acceptance_evaluation) =
            crate::acceptance_evaluator::AcceptanceEvaluator::evaluate_snapshot(
                crate::acceptance_evaluator::AcceptanceReceiptSnapshot::from_terminal(
                    required,
                    acceptance.clone(),
                    observed_evidence,
                ),
            );
        let mut runtime_write_attempt_paths = receipts
            .iter()
            .filter(|receipt| receipt.effect_kind == harness_contract::tool::ToolEffectKind::Write)
            .flat_map(|receipt| receipt.paths.iter().cloned())
            .collect::<Vec<_>>();
        runtime_write_attempt_paths.sort();
        runtime_write_attempt_paths.dedup();
        terminal.observed_acceptance = observed_acceptance;
        terminal.acceptance_evaluation = Some(acceptance_evaluation);
        terminal.acceptance = acceptance;
        terminal.evidence_refs = evidence_refs;
        terminal.changes = runtime_change_receipts
            .iter()
            .map(|receipt| receipt.path.clone())
            .collect();
        terminal.runtime_change_receipts = runtime_change_receipts;
        terminal.tool_calls = u64::try_from(receipts.len()).unwrap_or(u64::MAX);
        terminal.duplicate_tool_calls = 0;
        terminal.max_tool_concurrency_observed = 1;
        terminal.parallel_tool_batches = 0;
        terminal.runtime_write_attempt_paths = runtime_write_attempt_paths;
        terminal.runtime_observed_resource_scopes = Vec::new();
        terminal.model.clone_from(&self.external_model_profile);
        terminal.provider = "external_process".to_string();
    }
}

/// Executes a delegated task through the same RuntimeServices/Runner/provider
/// path as a primary turn. It never calls `ConversationRuntime` directly.
pub struct InProcessAgentWorker {
    services: Weak<RuntimeServices>,
    active_runs: Mutex<BTreeMap<String, ActiveInProcessRun>>,
    pending_cancellations: Mutex<BTreeSet<String>>,
    completed_runs: Mutex<VecDeque<String>>,
}
#[cfg(test)]
#[path = "in_process/tests.rs"]
mod tests;

#[cfg(test)]
mod discovery_process_tests {
    use super::*;

    #[tokio::test]
    async fn r1_composite_discovery_process_prepare_and_steps_share_pinned_catalog() {
        struct NoPinHost;
        #[async_trait::async_trait]
        impl RuntimeExecutionHost for NoPinHost {
            async fn execute_runtime_tool(
                &self,
                _: &RuntimeToolExecutionRequest,
            ) -> crate::RuntimeToolExecutionOutcome {
                panic!("unregistered defaults must not execute")
            }
        }
        let root = tempfile::tempdir().unwrap();
        let (services, host, packet) = tests::discovery_test_services(root.path(), true);
        assert!(matches!(
            packet.binding.as_ref().unwrap().executor,
            harness_contract::agent::AgentExecutorPolicy::ProcessJsonl { .. }
        ));
        let selection = AgentModelSelection {
            model: packet.model_lease.clone(),
            provider: "local-real-toolhost".into(),
            registry_revision: 0,
        };
        let bridge = ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap();
        let no_pin_root = tempfile::tempdir().unwrap();
        let no_pin = RuntimeServices::test_builder(
            no_pin_root.path().join("home"),
            services.workspace_root(),
        )
        .tool_execution_host(Arc::new(NoPinHost))
        .build()
        .unwrap();
        no_pin
            .commit_service()
            .register_graph(
                services
                    .graph_state_store()
                    .load(packet.graph_id())
                    .unwrap(),
            )
            .unwrap();
        assert!(ProcessJsonlToolSession::prepare(&no_pin, &packet, &selection).is_err());
        let receipt = bridge.executor.tool_discovery_receipt();
        assert_eq!(
            receipt.catalog_revision,
            host.tools.pin_snapshot().revision()
        );
        assert_eq!(
            receipt
                .activation_candidates
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            packet.allowed_tools.iter().cloned().collect()
        );
        let step = bridge.begin_step().unwrap();
        let step_id = step["step"]["identity"]["step_id"].as_str().unwrap();
        assert_eq!(step["tools"].as_array().unwrap().len(), 3);
        assert_eq!(
            step["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|definition| definition["name"].as_str().unwrap())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["read_file", "tool_orchestrate", "tool_search"])
        );
        for (sequence, call_id, tool, input) in [
            (
                1,
                "process-read",
                "read_file",
                serde_json::json!({"path":"src/a.rs"}),
            ),
            (
                2,
                "process-composite",
                "tool_orchestrate",
                serde_json::json!({"steps":[
                    {"id":"read","op":"call","tool":"read_file","input":{"path":"src/a.rs"}}
                ]}),
            ),
        ] {
            let output = bridge
                .execute_step_tool(step_id, sequence, call_id, tool, &input.to_string())
                .await
                .unwrap();
            assert!(output.contains("narrow-read-proof"));
        }
        assert_eq!(host.calls.load(Ordering::SeqCst), 2);
        let receipts_before = bridge.executor.receipts.lock().unwrap().len();
        assert_eq!(receipts_before, 2);
        for (index, (tool, input)) in tests::discovery_denied_calls().into_iter().enumerate() {
            assert!(bridge
                .execute_step_tool(
                    step_id,
                    index as u64 + 3,
                    &format!("process-denied-{index}"),
                    tool,
                    &input.to_string()
                )
                .await
                .is_err());
            assert_eq!(host.calls.load(Ordering::SeqCst), 2);
            assert_eq!(
                std::fs::read_to_string(services.workspace_root().join("src/a.rs")).unwrap(),
                "narrow-read-proof"
            );
            assert_eq!(
                std::fs::read_to_string(services.workspace_root().join("src/sibling.rs")).unwrap(),
                "untouched-sibling"
            );
        }
        assert_eq!(
            bridge.executor.receipts.lock().unwrap().len(),
            receipts_before
        );
        assert!(bridge
            .execute_step_tool(
                "foreign-step",
                99,
                "foreign",
                "read_file",
                r#"{"path":"src/a.rs"}"#
            )
            .await
            .is_err());
        assert!(bridge
            .execute_step_tool(step_id, 100, "checkpoint", "checkpoint", "{}")
            .await
            .is_err());
        host.tools
            .replace_snapshot(host.tools.pin_snapshot().snapshot().clone());
        let reloaded = bridge.executor.tool_discovery_receipt();
        assert_ne!(reloaded.catalog_revision, receipt.catalog_revision);
        assert_eq!(reloaded.descriptors, receipt.descriptors);
        assert_eq!(
            reloaded.activation_candidates,
            receipt.activation_candidates
        );
        assert!(bridge.begin_step().is_ok());
        for scopes in [
            vec![],
            vec!["network:*".into()],
            vec!["read:src/../a.rs".into()],
        ] {
            let mut denied = packet.clone();
            denied.resource_scopes = scopes;
            assert!(ProcessJsonlToolSession::prepare(&services, &denied, &selection).is_err());
        }
        let mut edit = packet.clone();
        edit.allowed_tools = vec!["edit".into()];
        assert!(ProcessJsonlToolSession::prepare(&services, &edit, &selection).is_err());
        let bootstrap_step = bridge.begin_step().unwrap();
        let bootstrap_id = bootstrap_step["step"]["identity"]["step_id"]
            .as_str()
            .unwrap();
        let bootstrap_input = r#"{"query":"select:read_file,edit,web_fetch"}"#;
        let bootstrap_output = bridge
            .execute_step_tool(
                bootstrap_id,
                1,
                "process-real-bootstrap",
                "tool_search",
                bootstrap_input,
            )
            .await
            .unwrap();
        let bootstrap: harness_contract::tool::ToolDiscoveryReceipt =
            serde_json::from_str(&bootstrap_output).unwrap();
        assert_eq!(bootstrap.activation_candidates, vec!["read_file"]);
        assert_eq!(
            bootstrap
                .descriptors
                .iter()
                .map(|descriptor| descriptor.canonical_id.as_str())
                .collect::<Vec<_>>(),
            vec!["read_file"]
        );
        assert!(!packet.allowed_tools.contains(&"tool_search".into()));
        assert!(!packet
            .binding
            .as_ref()
            .unwrap()
            .effective_capabilities
            .contains(&harness_contract::agent::AgentCapability::Search));
        assert_eq!(host.calls.load(Ordering::SeqCst), 3);
        let control = services
            .session_execution_policy_control(packet.session_id())
            .unwrap();
        let mut snapshot = control.snapshot();
        snapshot.revision += 1;
        control.replace(snapshot).unwrap();
        assert!(bridge.begin_step().is_err());
        assert!(bridge
            .execute_step_tool(step_id, 101, "stale", "read_file", r#"{"path":"src/a.rs"}"#)
            .await
            .is_err());
        assert_eq!(host.calls.load(Ordering::SeqCst), 3);
        assert!(services
            .execution_supervisor()
            .shutdown()
            .await
            .errors
            .is_empty());
        assert!(no_pin
            .execution_supervisor()
            .shutdown()
            .await
            .errors
            .is_empty());
    }
}

const COMPLETED_RUN_TOMBSTONE_LIMIT: usize = 1_024;

#[derive(Clone)]
struct ActiveInProcessRun {
    cancellation: crate::CancellationToken,
    session_id: String,
    input_stream: crate::SessionInputStream,
}

struct ActiveRunCleanup<'a> {
    worker: &'a InProcessAgentWorker,
    run_id: String,
}

impl Drop for ActiveRunCleanup<'_> {
    fn drop(&mut self) {
        // The tombstone is visible before the active handle disappears; the
        // completion flag/notification are published only after all maps are
        // clean. This also runs when the execute future is aborted.
        self.worker.record_completed_run(&self.run_id);
        self.worker
            .active_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.run_id);
        self.worker
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.run_id);
    }
}

impl InProcessAgentWorker {
    #[must_use]
    pub fn new(services: Weak<RuntimeServices>) -> Self {
        Self {
            services,
            active_runs: Mutex::new(BTreeMap::new()),
            pending_cancellations: Mutex::new(BTreeSet::new()),
            completed_runs: Mutex::new(VecDeque::new()),
        }
    }

    fn record_completed_run(&self, run_id: &str) {
        let mut completed = self
            .completed_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !completed.iter().any(|candidate| candidate == run_id) {
            completed.push_back(run_id.to_string());
        }
        while completed.len() > COMPLETED_RUN_TOMBSTONE_LIMIT {
            completed.pop_front();
        }
    }

    fn run_completed(&self, run_id: &str) -> bool {
        self.completed_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|candidate| candidate == run_id)
    }
}
