//! Resolve opaque results through their existing Artifact/Program/Tool owners.
use crate::agentic::program::AgenticProgramProjection;
use crate::{DurableRuntimeEvent, RuntimeServices};
use harness_contract::agent::AgentTaskPacket;
use harness_contract::agent_action::AgentActorBinding;
use harness_contract::context::ArtifactRef;
use harness_contract::goal::GoalResultKind;
use harness_contract::policy::PermissionScope;
use harness_contract::tool::{ToolEffectDescriptor, ToolEffectKind};
use std::collections::BTreeSet;

pub(super) struct ResultProducer {
    pub actor: String,
    pub execution: String,
    pub packet: Option<AgentTaskPacket>,
    pub tool: Option<(String, ToolEffectKind)>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub(super) enum ResultOrigin {
    Strong,
    StrongRaw { anchors: BTreeSet<String> },
    LegacyWeak,
    LegacyIncomplete,
}

impl ResultOrigin {
    fn raw_owner(owner: &DurableRuntimeEvent) -> Self {
        Self::StrongRaw {
            anchors: BTreeSet::from([format!(
                "{}#{}:{}:{}",
                owner.stream_id,
                owner.sequence,
                owner.event_id,
                crate::context_evidence::read_payload_digest(&owner.payload),
            )]),
        }
    }

    pub(super) fn require_strong(&self) -> Result<(), String> {
        if !matches!(self, Self::Strong | Self::StrongRaw { .. }) {
            return Err("proof requires strong original provenance; legacy raw metadata remains weak/incomplete and requires governed recovery".into());
        }
        Ok(())
    }

    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::StrongRaw { mut anchors }, Self::StrongRaw { anchors: other }) => {
                anchors.extend(other);
                Self::StrongRaw { anchors }
            }
            (left, right) => left.max(right),
        }
    }
}

#[cfg(any(test, feature = "test-fixtures"))]
impl RuntimeServices {
    pub(crate) async fn raw_origin_content_for_test(
        &self,
        actor: &AgentActorBinding,
        program: &AgenticProgramProjection,
        reference: &str,
    ) -> Result<ArtifactRef, String> {
        let metadata = resolve_material(self, actor, program, reference).await?;
        metadata.origin.require_strong()?;
        Ok(metadata.content)
    }
}

#[cfg(test)]
impl RuntimeServices {
    pub(crate) async fn assert_program_publication_for_test(
        &self,
        actor: &AgentActorBinding,
        reference: &str,
        selector: &str,
        publisher: &AgentTaskPacket,
    ) {
        let program = self
            .agent_action_service()
            .project(&actor.program_id)
            .unwrap();
        let metadata = resolve_result(self, actor, &program, reference)
            .await
            .unwrap();
        metadata.origin.require_strong().unwrap();
        assert_eq!(metadata.content.selector, selector);
        assert!(
            matches!(metadata.origin, ResultOrigin::StrongRaw { ref anchors } if !anchors.is_empty())
        );
        assert!(metadata
            .producers
            .iter()
            .any(|producer| producer.actor == actor.actor_id
                && producer.packet.is_none()
                && producer.execution
                    == format!(
                        "root-execution:{}",
                        actor.root_execution_id.as_ref().unwrap()
                    )));
        let owner = publisher.agentic_binding.as_ref().unwrap();
        assert!(metadata
            .producers
            .iter()
            .any(|producer| producer.actor == owner.agent_id
                && producer.execution == format!("agent-run:{}", publisher.run_id())
                && producer
                    .packet
                    .as_ref()
                    .is_some_and(|packet| packet.assignment.execution_identity
                        == publisher.assignment.execution_identity)));
        assert!(
            !metadata.primary_inputs.is_empty(),
            "original publisher read inputs must survive"
        );
        assert!(metadata
            .result_sources
            .iter()
            .any(|source| source.stream_id == format!("session:{}", actor.session_id)));
    }

    pub(crate) async fn program_primary_for_test(
        &self,
        actor: &AgentActorBinding,
        reference: &str,
        selector: &str,
        source_publisher: &AgentTaskPacket,
    ) -> Result<(), String> {
        let program = self
            .agent_action_service()
            .project(&actor.program_id)
            .unwrap();
        let metadata = resolve_result(self, actor, &program, reference).await?;
        metadata
            .origin
            .require_strong()
            .expect("D must have strong original provenance before recursive S validation");
        assert!(
            metadata
                .primary_inputs
                .iter()
                .any(|input| input.content.selector == selector),
            "the derived publisher's actual read must enter recursive primary validation"
        );
        let source_content = self.artifact_store().resolve(selector).unwrap();
        assert!(
            metadata
                .primary_inputs
                .iter()
                .any(|input| input.content == source_content),
            "D must retain the exact original S content, not only its selector spelling"
        );
        let stream = format!("session:{}", actor.session_id);
        let events = self.event_store().list_stream(&stream).unwrap();
        let prehead = crate::content_publication::fresh_object_precreation_head(
            &events,
            &actor.session_id,
            &metadata.content,
        )
        .unwrap()
        .expect("D requires a real fresh-object fact");
        let fact = events
            .iter()
            .find(|event| {
                event.kind == crate::content_publication::FRESH_OBJECT_KIND
                    && event.payload["artifact"]["selector"] == metadata.content.selector
            })
            .unwrap();
        let source = events
            .iter()
            .find(|event| {
                event.kind == "tool.invocation.completed"
                    && event.payload["tool_call_id"] == "native-publication-small"
                    && event.activity_binding().is_some_and(|binding| {
                        binding.agent_run_id.as_deref() == Some(source_publisher.run_id())
                    })
            })
            .unwrap();
        let derived = events
            .iter()
            .find(|event| {
                event.kind == "tool.invocation.completed"
                    && event.payload["tool_call_id"] == "program-derived-publish"
            })
            .unwrap();
        assert!(
            source.sequence <= prehead
                && prehead < fact.sequence
                && fact.sequence < derived.sequence,
            "S.completed={} D.n={} D.fact={} D.completed={}",
            source.sequence,
            prehead,
            fact.sequence,
            derived.sequence
        );
        assert!(metadata
            .effect_sources
            .iter()
            .any(|cut| cut.stream_id == stream && cut.expected_revision >= derived.sequence));
        let owner = source_publisher.agentic_binding.as_ref().unwrap();
        assert!(
            !metadata
                .producers
                .iter()
                .any(|producer| producer.actor == owner.agent_id
                    && producer.execution == format!("agent-run:{}", source_publisher.run_id())),
            "D direct producers must not include the publisher that only produced S"
        );
        Ok(())
    }
}

struct OriginalToolBody {
    bytes: Vec<u8>,
    origin: ResultOrigin,
    content: Option<ArtifactRef>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeRawContent {
    version: u32,
    ref_id: String,
    tool_call_id: String,
    sha256: String,
    byte_count: u64,
    visibility_scope: String,
    selector: String,
    media_type: String,
}

#[derive(Clone, serde::Serialize)]
pub(super) struct AuthenticatedProcessExecution {
    pub(super) fact: harness_contract::context::ToolExecutionFact,
    pub(super) primary: harness_contract::goal::ProofInputEvent,
    pub(super) admission: harness_contract::goal::ProofInputEvent,
    pub(super) index: harness_contract::goal::ProofInputEvent,
}

#[cfg(test)]
std::thread_local! {
    static BOUNDED_PROCESS_SOURCE_FAULT: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn bounded_process_source_fault(event: &mut DurableRuntimeEvent, admission: bool) {
    BOUNDED_PROCESS_SOURCE_FAULT.with(|source| match (source.get(), admission) {
        (Some("admission-inputs"), true) => {
            event.payload["admission"]["inputs"] = serde_json::json!([])
        }
        (Some("admission-digest"), true) => {
            event.payload["admission_sha256"] = serde_json::json!("0".repeat(64))
        }
        (Some("admission-original-input"), true) => {
            event.payload["original_input"] = serde_json::json!("{\"code\":\"counterfeit\"}")
        }
        (Some("admission-workspace"), true) => {
            event.payload["source_workspace_root"] = serde_json::Value::Null
        }
        (Some("index-fact"), false) => {
            event.payload["outcome"]["observed_evidence"] = serde_json::json!([])
        }
        (Some("index-scope"), false) => {
            event.payload["resolved_effect_scope"] = serde_json::Value::Null
        }
        _ => {}
    });
}

fn valid_process_sha256(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn process_sha256(bytes: &[u8]) -> String {
    crate::context_evidence::read_sha256(bytes)
        .trim_start_matches("sha256:")
        .to_string()
}

fn process_event(event: &DurableRuntimeEvent) -> harness_contract::goal::ProofInputEvent {
    harness_contract::goal::ProofInputEvent {
        stream_id: event.stream_id.clone(),
        sequence: event.sequence,
        event_id: event.event_id.clone(),
        payload_digest: super::proof_value_digest(&event.payload),
    }
}

pub(super) fn has_process_fact(primary: &DurableRuntimeEvent) -> bool {
    primary.payload["outcome"]["observed_evidence"]
        .as_array()
        .is_some_and(|observations| {
            observations
                .iter()
                .any(|observation| observation["target"]["kind"] == "tool_execution")
        })
}

pub(super) fn process_has_no_external_effects(
    run: &AuthenticatedProcessExecution,
) -> Result<(), String> {
    use harness_contract::context::{ToolExecutionStopReason, ToolExecutionTerminal};
    match &run.fact.terminal {
        ToolExecutionTerminal::NotStarted { reason_code }
            if !reason_code.is_empty() && reason_code.len() <= 128 =>
        {
            Ok(())
        }
        ToolExecutionTerminal::Settled {
            exit_code,
            signal,
            closure,
            stop_reason,
        } => {
            if exit_code.is_some() == signal.is_some()
                || !closure.all_children_reaped
                || !closure.init_reaped
                || !closure.kernel_hardening_applied
                || !closure.network_disabled
                || !closure.project_read_only
                || !closure.status_channel_protected
                || closure.namespace_inode == 0
                || closure.init_host_pid == 0
                || closure.init_start_ticks == 0
                || closure.backend != "bubblewrap+landlock+seccomp+pid1"
                || !matches!(
                    stop_reason,
                    ToolExecutionStopReason::CommandExit
                        | ToolExecutionStopReason::Timeout
                        | ToolExecutionStopReason::Cancelled
                        | ToolExecutionStopReason::CaptureLimit
                )
            {
                return Err(
                    "effect_review_pending: bounded process has no authenticated tree closure"
                        .into(),
                );
            }
            Ok(())
        }
        _ => Err("effect_review_pending: bounded process is unknown or has no terminal".into()),
    }
}

pub(super) fn process_ticket_start(
    store: &crate::RuntimeEventStore,
    graph_id: &str,
    node_id: &str,
    attempt: u32,
    intent: &DurableRuntimeEvent,
) -> Result<Vec<DurableRuntimeEvent>, String> {
    use crate::execution_core::graph::ExecutionGraphEvent;
    use harness_contract::execution_graph::ExecutionNodeStatus;
    let mut cuts = std::collections::BTreeMap::new();
    let mut selected = None;
    for running in result_source_events(store, &format!("{graph_id}:node:{node_id}"), &mut cuts)? {
        if running.kind != "execution_node.transitioned"
            || running.payload["to"] != "running"
            || running.commit_cursor > intent.commit_cursor
        {
            continue;
        }
        let revision = running.payload["graph_revision"]
            .as_u64()
            .filter(|revision| *revision > 0)
            .ok_or("bounded process original ticket revision missing")?;
        let start = store
            .list_stream_after(graph_id, revision - 1, revision, 1, 4 * 1024 * 1024)?
            .into_iter()
            .next()
            .ok_or("bounded process original ticket graph start missing")?;
        if running.schema_version != 1
            || running.actor.as_deref() != Some("execution_commit_service")
            || running.scope != crate::RuntimeEventScope::ExecutionNode
            || running.payload["graph_id"] != graph_id
            || running.payload["node_id"] != node_id
            || start.schema_version != 2
            || start.actor.as_deref() != Some("execution_commit_service")
            || start.scope != crate::RuntimeEventScope::ExecutionGraph
            || start.transaction_id != running.transaction_id
            || start.commit_cursor != running.commit_cursor
        {
            return Err("bounded process original ticket start source conflict".into());
        }
        let event: ExecutionGraphEvent =
            serde_json::from_value(store.resolve_graph_record(&start)?)
                .map_err(|_| "bounded process original ticket graph content invalid")?;
        if event.kind() != start.kind {
            return Err("bounded process ticket graph kind conflict".into());
        }
        let binding = match &event {
            ExecutionGraphEvent::NodesStarted { bindings, .. } => bindings.get(node_id),
            ExecutionGraphEvent::NodeTransitioned {
                node_id: original,
                to: ExecutionNodeStatus::Running,
                binding,
                ..
            } if original == node_id => binding.as_ref(),
            ExecutionGraphEvent::Checkpoint {
                cause, bindings, ..
            } if matches!(
                cause.as_str(),
                "execution_graph.nodes_started" | "execution_graph.node_transitioned"
            ) =>
            {
                bindings.get(node_id)
            }
            _ => None,
        }
        .ok_or("bounded process original ticket start binding missing")?;
        if binding.attempt != attempt {
            continue;
        }
        if selected.replace(vec![running, start]).is_some() {
            return Err("bounded process original ticket attempt is ambiguous".into());
        }
    }
    selected.ok_or_else(|| "bounded process original ticket attempt was never started".into())
}

pub(super) fn authenticate_bounded_process(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    primary: &DurableRuntimeEvent,
) -> Result<AuthenticatedProcessExecution, String> {
    use harness_contract::context::EvidenceTargetIdentity;
    use harness_contract::policy::{PermissionOperation, PermissionResource};
    let key = primary.payload["idempotency_key"]
        .as_str()
        .filter(|key| !key.is_empty())
        .ok_or("bounded process call identity missing")?;
    let source = format!("execution-effect:{key}");
    let original = store
        .event_by_idempotency_key(&source, &format!("{key}:receipt"))
        .map_err(|error| error.to_string())?
        .ok_or("bounded process original primary missing")?;
    let intent = store
        .event_by_idempotency_key(&source, &format!("{key}:intent"))
        .map_err(|error| error.to_string())?
        .ok_or("bounded process original intent missing")?;
    let admitted = store
        .event_by_idempotency_key(&source, &format!("{key}:process-admission"))
        .map_err(|error| error.to_string())?
        .ok_or("bounded process original admission missing")?;
    #[cfg(test)]
    let admitted = {
        let mut original = admitted;
        bounded_process_source_fault(&mut original, true);
        original
    };
    let descriptor: ToolEffectDescriptor = serde_json::from_value(intent.payload["effect"].clone())
        .map_err(|_| "bounded process original descriptor missing")?;
    let scope =
        require_original_effect_scope(&intent.payload["resolved_effect_scope"], &descriptor)?;
    let source_root = admitted.payload["source_workspace_root"]
        .as_str()
        .filter(|root| !root.is_empty())
        .ok_or("bounded process protected source workspace missing")?;
    let source_root = std::path::Path::new(source_root);
    if !source_root.is_absolute()
        || source_root.components().any(|part| {
            matches!(
                part,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err(
            "bounded process protected source workspace is not canonical absolute identity".into(),
        );
    }
    let original_input = admitted.payload["original_input"]
        .as_str()
        .ok_or("bounded process protected original request input missing")?;
    if original_input.len() > 131072 {
        return Err(
            "bounded process original request input exceeds the protected metadata budget".into(),
        );
    }
    let source_input = harness_contract::step::parse_strict_json(original_input)
        .map_err(|error| error.to_string())?;
    let input_digest =
        crate::execution_core::graph::ExecutionCommitService::bounded_process_input_sha256(
            &source_input,
        )
        .map_err(|error| error.to_string())?;
    let descriptor_digest =
        crate::execution_core::graph::ExecutionCommitService::bounded_process_descriptor_sha256(
            &descriptor,
        )
        .map_err(|error| error.to_string())?;
    let original_payload =
        harness_contract::step::canonical_digest("tool-effect-payload", &source_input)
            .map_err(|error| error.to_string())?;
    if intent.payload["input_sha256"]
        != format!("sha256:{}", process_sha256(original_input.as_bytes()))
        || primary.payload["input_sha256"] != intent.payload["input_sha256"]
    {
        return Err("bounded process original raw input fingerprint mismatch".into());
    }
    if intent.payload["replay"]["payload_digest"] != original_payload {
        return Err(
            "bounded process protected input differs from original replay fingerprint".into(),
        );
    }
    let raw_observations = &primary.payload["outcome"]["observed_evidence"];
    if raw_observations
        .as_array()
        .is_none_or(|observations| observations.len() > 8)
        || serde_json::to_vec(raw_observations)
            .map_err(|error| error.to_string())?
            .len()
            > 131072
    {
        return Err("bounded process fact metadata budget exceeded before decode".into());
    }
    let outcome: crate::RuntimeToolExecutionOutcome =
        serde_json::from_value(primary.payload["outcome"].clone())
            .map_err(|_| "bounded process original outcome missing")?;
    if outcome.observed_evidence.len() > 8
        || serde_json::to_vec(&outcome.observed_evidence)
            .map_err(|error| error.to_string())?
            .len()
            > 131072
    {
        return Err("bounded process fact budget exceeded".into());
    }
    let facts = outcome
        .observed_evidence
        .iter()
        .filter_map(|observation| match &observation.target {
            EvidenceTargetIdentity::ToolExecution { execution } => Some(execution.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    if facts.len() != 1 {
        return Err("bounded process needs exactly one original execution fact".into());
    }
    let fact = facts
        .into_iter()
        .next()
        .ok_or("bounded process fact missing")?;
    let admission_value =
        serde_json::to_value(&fact.admission).map_err(|error| error.to_string())?;
    if original.payload != primary.payload
        || original.event_id != primary.event_id
        || original.stream_id != primary.stream_id
        || primary.stream_id != source
        || original.commit_cursor != primary.commit_cursor
        || [primary, &intent, &admitted].iter().any(|event| {
            event.actor.as_deref() != Some("governed_tool")
                || event.schema_version != 1
                || event.scope != crate::RuntimeEventScope::ExecutionNode
        })
        || primary.kind != "execution.effect.receipt"
        || primary.status.as_deref() != Some("completed")
        || intent.kind != "execution.effect.intent"
        || admitted.kind != "execution.process.admitted"
        || admitted.status.as_deref() != Some("inflight")
        || admitted.refs != intent.refs
        || admitted.stream_id != source
        || admitted.payload["version"] != 1
        || admitted.payload["admission"] != admission_value
        || admitted.payload["admission_sha256"]
            != process_sha256(
                &serde_json::to_vec(&fact.admission).map_err(|error| error.to_string())?,
            )
        || intent.commit_cursor >= admitted.commit_cursor
        || admitted.commit_cursor >= primary.commit_cursor
        || intent.payload["replay"] != primary.payload["replay"]
        || intent.payload["effect"] != primary.payload["replay"]["invocation"]["effect"]
        || intent.payload["resolved_effect_scope"] != primary.payload["resolved_effect_scope"]
        || primary.payload["descriptor_hash"] != intent.payload["effect"]["descriptor_hash"]
        || primary.payload["root_effect_owner"] != intent.payload["root_effect_owner"]
        || primary.payload["root_effect_context"] != intent.payload["root_effect_context"]
        || descriptor.effect_kind != ToolEffectKind::Process
        || !descriptor.spawns_process
        || descriptor.uses_network
        || descriptor.mutates_packages
        || descriptor.mutates_system
        || scope.complete
        || descriptor.scopes.iter().any(|scope| {
            !matches!(
                (&scope.resource, &scope.operation),
                (PermissionResource::File, PermissionOperation::Read)
                    | (PermissionResource::Shell, PermissionOperation::Execute)
            )
        })
        || primary.payload["tool_name"] != "execute_code"
        || outcome.tool_name != "execute_code"
        || outcome.tool_use_id != fact.admission.binding.tool_use_id
        || fact.version != 1
        || fact.admission.version != 1
        || fact.admission.binding.call_key != key
        || fact.admission.binding.tool_use_id != intent.payload["tool_use_id"]
        || fact.admission.binding.input_sha256 != input_digest
        || fact.admission.binding.descriptor_sha256 != descriptor_digest
        || !valid_process_sha256(&fact.admission.binding.authority_sha256)
        || !valid_process_sha256(&fact.admission.binding.input_sha256)
        || !valid_process_sha256(&fact.admission.binding.descriptor_sha256)
        || !valid_process_sha256(&fact.admission.code_sha256)
        || !valid_process_sha256(&fact.stdout_sha256)
        || !valid_process_sha256(&fact.stderr_sha256)
        || fact.admission.inputs.len() > 64
        || fact.admission.source_bundle.bytes > 17825792
        || !fact.body.is_durable()
        || !fact.admission.source_bundle.is_durable()
        || fact.admission.source_bundle.media_type != "application/octet-stream"
        || fact.body.visibility_scope
            != format!(
                "session:{}",
                primary.payload["replay"]["session_id"]
                    .as_str()
                    .ok_or("bounded process original Session missing")?
            )
        || fact.admission.source_bundle.visibility_scope != fact.body.visibility_scope
        || fact.body.bytes > 6291456
        || fact.body.media_type != "application/json"
        || fact.body.selector.len() > 2048
        || fact.admission.source_bundle.selector.len() > 2048
        || fact.stdout_bytes > 2097152
        || fact.stderr_bytes > 2097152
    {
        return Err("bounded process original admission/terminal/binding/scope conflict".into());
    }
    let mut previous = None;
    let mut bytes = 0u64;
    for input in &fact.admission.inputs {
        let path = std::path::Path::new(&input.relative_path);
        if input.relative_path.is_empty()
            || input.relative_path.len() > 1024
            || path.is_absolute()
            || path
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
            || previous
                .as_ref()
                .is_some_and(|last: &String| last >= &input.relative_path)
            || input.bytes > 2097152
            || input.inode == 0
            || !valid_process_sha256(&input.sha256)
        {
            return Err("bounded process original finite input manifest invalid".into());
        }
        bytes = bytes
            .checked_add(input.bytes)
            .ok_or("bounded process input budget overflow")?;
        previous = Some(input.relative_path.clone());
    }
    if bytes > 16777216 {
        return Err("bounded process original input budget exceeded".into());
    }
    let parent = &primary.payload["replay"]["parent_execution"];
    let mut cuts = std::collections::BTreeMap::new();
    let (graph, node, attempt) = if parent.is_null() {
        authenticate_root_receipt_owner(store, primary, &mut cuts)?;
        let (owner, attempt, _) = original_root_effect_owner(primary)?;
        (
            owner.root_execution_id,
            owner.node_id.ok_or("bounded root node missing")?,
            attempt,
        )
    } else {
        let graph_id = parent["execution_id"]
            .as_str()
            .ok_or("bounded process parent graph missing")?;
        let node_id = parent["node_id"]
            .as_str()
            .ok_or("bounded process parent node missing")?;
        let graph = crate::ExecutionGraphStateStore::new(std::sync::Arc::clone(store))
            .load(graph_id)
            .map_err(|error| error.to_string())?;
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .ok_or("bounded process original node missing")?;
        let attempt = primary
            .refs
            .iter()
            .find(|reference| reference.kind == "agent_attempt")
            .and_then(|reference| reference.id.parse::<u32>().ok())
            .filter(|attempt| *attempt > 0)
            .ok_or("bounded process original attempt missing")?;
        process_ticket_start(store, graph_id, node_id, attempt, &intent)?;
        if node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
            let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref)
                .map_err(|_| "bounded process original immutable packet missing")?;
            if packet.attempt > attempt
                || packet.graph_id() != graph_id
                || packet.node_id() != node_id
                || primary.payload["replay"]["session_id"] != packet.session_id()
            {
                return Err("bounded process original packet/attempt conflict".into());
            }
        } else if node.kind != harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
            || graph
                .lineage
                .as_ref()
                .is_none_or(|lineage| primary.payload["replay"]["session_id"] != lineage.session_id)
        {
            return Err("bounded process original Host ticket owner invalid".into());
        }
        (graph_id.to_string(), node_id.to_string(), attempt)
    };
    let index_source = format!("execution-agent-receipts:{graph}:{node}:{attempt}");
    let index = store
        .event_by_idempotency_key(&index_source, &format!("agent-tool-receipt:{key}"))
        .map_err(|error| error.to_string())?
        .ok_or("bounded process original index missing")?;
    #[cfg(test)]
    let index = {
        let mut original = index;
        bounded_process_source_fault(&mut original, false);
        original
    };
    if index.kind != "execution.agent_tool.receipt"
        || index.actor != primary.actor
        || index.scope != primary.scope
        || crate::ExecutionCommitService::receipt_observation_sequence(&index).is_none()
        || index.status != primary.status
        || index.commit_cursor != primary.commit_cursor
        || index.transaction_id != primary.transaction_id
        || index.transaction_index <= primary.transaction_index
        || index.payload["outcome"] != primary.payload["outcome"]
        || index.payload["resolved_effect_scope"] != primary.payload["resolved_effect_scope"]
        || index.payload["effect"] != intent.payload["effect"]
        || index.payload["effect_kind"] != "process"
        || index.payload["effect_scopes"] != intent.payload["effect"]["scopes"]
        || !index.refs.iter().any(|reference| {
            reference.kind == "agent_attempt" && reference.id == attempt.to_string()
        })
        || index.payload["idempotency_key"] != key
        || primary.payload["process_admission_ref"] != format!("{key}:process-admission")
        || index.payload["process_admission_ref"] != primary.payload["process_admission_ref"]
    {
        return Err("bounded process original terminal/index conflict".into());
    }
    Ok(AuthenticatedProcessExecution {
        fact,
        primary: process_event(primary),
        admission: process_event(&admitted),
        index: process_event(&index),
    })
}

pub(super) fn retain_process_execution(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    run: &AuthenticatedProcessExecution,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<Vec<harness_contract::goal::ProofInputEvent>, String> {
    let mut materials = vec![
        run.primary.clone(),
        run.admission.clone(),
        run.index.clone(),
    ];
    let source = &run.primary.stream_id;
    let intent = store
        .event_by_idempotency_key(
            source,
            &format!("{}:intent", run.fact.admission.binding.call_key),
        )
        .map_err(|error| error.to_string())?
        .ok_or("bounded process original intent missing")?;
    materials.push(process_event(&intent));
    if intent.payload["replay"]["parent_execution"].is_null() {
        materials.extend(
            root_owner_materials(store, &intent, cuts)?
                .iter()
                .map(process_event),
        );
    } else {
        let graph = intent.payload["replay"]["parent_execution"]["execution_id"]
            .as_str()
            .ok_or("bounded process parent source missing")?;
        pin_result_source(store, graph, cuts)?;
        let node = intent.payload["replay"]["parent_execution"]["node_id"]
            .as_str()
            .ok_or("bounded process parent node source missing")?;
        let primary = store
            .event_by_idempotency_key(
                &intent.stream_id,
                &format!("{}:receipt", run.fact.admission.binding.call_key),
            )
            .map_err(|error| error.to_string())?
            .ok_or("bounded process original terminal source missing")?;
        let attempt = primary
            .refs
            .iter()
            .find(|reference| reference.kind == "agent_attempt")
            .and_then(|reference| reference.id.parse::<u32>().ok())
            .ok_or("bounded process original ticket attempt missing")?;
        materials.extend(
            process_ticket_start(store, graph, node, attempt, &intent)?
                .iter()
                .map(process_event),
        );
    }
    for event in &materials {
        pin_result_source(store, &event.stream_id, cuts)?;
    }
    Ok(materials)
}

pub(super) fn validate_process_archives(
    run: &AuthenticatedProcessExecution,
    readers: super::ResultContentReaders<'_>,
) -> Result<(), String> {
    use base64::Engine;
    let artifacts = readers
        .artifacts
        .ok_or("bounded process needs original ArtifactStore")?;
    let read = |artifact: &ArtifactRef| -> Result<Vec<u8>, String> {
        let canonical = artifacts
            .resolve(&artifact.selector)
            .map_err(|error| error.to_string())?;
        if canonical != *artifact {
            return Err("bounded process original archive identity changed".into());
        }
        let limit = if artifact.media_type == "application/json" {
            6291456
        } else {
            17825792
        };
        if artifact.bytes > limit {
            return Err("bounded process archive budget exceeded".into());
        }
        let bytes = artifacts
            .read_blocking(artifact, &artifact.visibility_scope, None)
            .map_err(|error| error.to_string())?;
        checked_original_bytes(bytes, &artifact.sha256, artifact.bytes)
    };
    let body = read(&run.fact.body)?;
    let value = harness_contract::step::parse_strict_json(
        std::str::from_utf8(&body).map_err(|_| "bounded process original body is not JSON UTF8")?,
    )
    .map_err(|_| "bounded process original body invalid")?;
    let body_keys = [
        "version",
        "admission_sha256",
        "terminal",
        "stdout_base64",
        "stderr_base64",
        "stdout_sha256",
        "stderr_sha256",
        "stdout_bytes",
        "stderr_bytes",
        "capture_complete",
        "stdout_utf8",
        "stderr_utf8",
    ];
    if value.as_object().is_none_or(|body| {
        body.len() < 10
            || body.len() > 12
            || body.keys().any(|key| !body_keys.contains(&key.as_str()))
    }) || ["stdout_base64", "stderr_base64"].iter().any(|key| {
        value[*key]
            .as_str()
            .is_none_or(|encoded| encoded.len() > 2796204)
    }) {
        return Err("bounded process original body structure or capture budget invalid".into());
    }
    let stdout = base64::engine::general_purpose::STANDARD
        .decode(
            value["stdout_base64"]
                .as_str()
                .ok_or("bounded process stdout missing")?,
        )
        .map_err(|_| "bounded process stdout invalid")?;
    let stderr = base64::engine::general_purpose::STANDARD
        .decode(
            value["stderr_base64"]
                .as_str()
                .ok_or("bounded process stderr missing")?,
        )
        .map_err(|_| "bounded process stderr invalid")?;
    for (key, raw) in [("stdout_utf8", &stdout), ("stderr_utf8", &stderr)] {
        if let Some(text) = value.get(key) {
            if raw.len() > 131072 || std::str::from_utf8(raw).ok() != text.as_str() {
                return Err("bounded process optional UTF8 presentation differs from complete original bytes".into());
            }
        }
    }
    let fact = &run.fact;
    if value["version"] != 1
        || value["admission_sha256"]
            != process_sha256(
                &serde_json::to_vec(&fact.admission).map_err(|error| error.to_string())?,
            )
        || value["terminal"]
            != serde_json::to_value(&fact.terminal).map_err(|error| error.to_string())?
        || value["capture_complete"] != fact.capture_complete
        || value["stdout_sha256"] != fact.stdout_sha256
        || value["stderr_sha256"] != fact.stderr_sha256
        || value["stdout_bytes"] != fact.stdout_bytes
        || value["stderr_bytes"] != fact.stderr_bytes
        || stdout.len() as u64 != fact.stdout_bytes
        || stderr.len() as u64 != fact.stderr_bytes
        || process_sha256(&stdout) != fact.stdout_sha256
        || process_sha256(&stderr) != fact.stderr_sha256
    {
        return Err("bounded process body is not the original complete terminal projection".into());
    }
    let source = read(&fact.admission.source_bundle)?;
    let mut remaining = source.as_slice();
    let take = |remaining: &mut &[u8], count: usize| -> Result<Vec<u8>, String> {
        if count > remaining.len() {
            return Err("bounded process source archive truncated".into());
        }
        let (head, tail) = (*remaining).split_at(count);
        *remaining = tail;
        Ok(head.to_vec())
    };
    if take(&mut remaining, 8)? != b"COWD-BP1" {
        return Err("bounded process source archive version invalid".into());
    }
    let length = u32::from_be_bytes(
        take(&mut remaining, 4)?
            .try_into()
            .map_err(|_| "source manifest length invalid")?,
    ) as usize;
    if length > 131072 {
        return Err("bounded source manifest budget exceeded".into());
    }
    let manifest_bytes = take(&mut remaining, length)?;
    let manifest = harness_contract::step::parse_strict_json(
        std::str::from_utf8(&manifest_bytes)
            .map_err(|_| "bounded source manifest is not JSON UTF8")?,
    )
    .map_err(|_| "bounded source manifest invalid")?;
    let code_bytes = u64::from_be_bytes(
        take(&mut remaining, 8)?
            .try_into()
            .map_err(|_| "source code length invalid")?,
    );
    if code_bytes > 65536 {
        return Err("bounded source code budget exceeded".into());
    }
    let code = take(&mut remaining, code_bytes as usize)?;
    if process_sha256(&code) != fact.admission.code_sha256
        || manifest["code_sha256"] != fact.admission.code_sha256
    {
        return Err("bounded source original code digest conflict".into());
    }
    if manifest
        .as_object()
        .is_none_or(|manifest| manifest.len() != 2)
    {
        return Err("bounded source manifest fields invalid".into());
    }
    let inputs = manifest["inputs"]
        .as_array()
        .ok_or("bounded source manifest inputs missing")?;
    if inputs.len() != fact.admission.inputs.len() {
        return Err("bounded source input manifest omission".into());
    }
    for (expected, stored) in fact.admission.inputs.iter().zip(inputs) {
        let length = u64::from_be_bytes(
            take(&mut remaining, 8)?
                .try_into()
                .map_err(|_| "source input length invalid")?,
        );
        if stored.as_object().is_none_or(|input| input.len() != 3)
            || length != expected.bytes
            || stored["path"] != expected.relative_path
            || stored["sha256"] != expected.sha256
            || stored["bytes"] != expected.bytes
        {
            return Err("bounded source input manifest conflict".into());
        }
        let bytes = take(&mut remaining, length as usize)?;
        if process_sha256(&bytes) != expected.sha256 {
            return Err("bounded source actual copied bytes conflict".into());
        }
    }
    if !remaining.is_empty() {
        return Err("bounded source archive has unbound trailing bytes".into());
    }
    Ok(())
}

pub(super) fn runtime_control_descriptor(descriptor: &ToolEffectDescriptor) -> bool {
    use harness_contract::policy::{PermissionOperation, PermissionResource};
    use harness_contract::tool::{ToolApprovalClass, ToolIdempotency, ToolPermissionMode};
    harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&descriptor.tool_id.as_str())
        && descriptor.effect_kind == ToolEffectKind::Write
        && descriptor.idempotency == ToolIdempotency::IdempotentWithKey
        && descriptor.required_permission == ToolPermissionMode::ReadOnly
        && descriptor.approval_class == ToolApprovalClass::None
        && !descriptor.uses_network
        && !descriptor.spawns_process
        && !descriptor.mutates_packages
        && !descriptor.mutates_system
        && descriptor.scopes.len() == 1
        && descriptor.scopes[0].resource == PermissionResource::Tool
        && descriptor.scopes[0].operation == PermissionOperation::Control
        && descriptor.scopes[0].target.as_deref().is_none_or(|target| target == descriptor.tool_id)
}

fn project_result_effect(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    resolved: &mut ResolvedReviewResult,
    descriptor: &ToolEffectDescriptor,
    scope: &harness_contract::goal::ProofEffectScope,
    primary: &DurableRuntimeEvent,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<(), String> {
    if runtime_control_descriptor(descriptor) {
        let checked = require_original_effect_scope(
            &serde_json::to_value(scope).map_err(|error| error.to_string())?,
            descriptor,
        )?;
        if checked.complete || !checked.targets.is_empty() {
            return Err("Runtime control action has a foreign workspace effect scope".into());
        }
        return Ok(());
    }
    if descriptor.effect_kind == ToolEffectKind::Process || has_process_fact(primary) {
        let run = authenticate_bounded_process(store, primary)?;
        process_has_no_external_effects(&run)?;
        retain_process_execution(store, &run, cuts)?;
        if !resolved
            .process_runs
            .iter()
            .any(|previous| previous.primary == run.primary)
        {
            resolved.process_runs.push(run);
        }
        Ok(())
    } else {
        merge_authenticated_result_effect(resolved, descriptor, scope, primary.commit_cursor)
    }
}

pub(super) fn process_runs_for_boundaries(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    boundaries: &[ResultInputBoundary],
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<Vec<AuthenticatedProcessExecution>, String> {
    let mut sources = BTreeSet::new();
    for boundary in boundaries {
        if let Some(packet) = &boundary.packet {
            let graph = crate::ExecutionGraphStateStore::new(std::sync::Arc::clone(store))
                .load(packet.graph_id())
                .map_err(|error| error.to_string())?;
            pin_result_source(store, packet.graph_id(), cuts)?;
            let attempts = graph
                .recovery_cursor
                .node_attempts
                .get(packet.node_id())
                .copied()
                .unwrap_or(packet.attempt);
            for attempt in 1..=attempts {
                sources.insert(format!(
                    "execution-agent-receipts:{}:{}:{attempt}",
                    packet.graph_id(),
                    packet.node_id()
                ));
            }
        } else if let Some(root) = boundary.producer.strip_prefix("root-execution:") {
            let graph = crate::ExecutionGraphStateStore::new(std::sync::Arc::clone(store))
                .load(root)
                .map_err(|error| error.to_string())?;
            pin_result_source(store, root, cuts)?;
            for (node, attempts) in &graph.recovery_cursor.node_attempts {
                for attempt in 1..=*attempts {
                    sources.insert(format!("execution-agent-receipts:{root}:{node}:{attempt}"));
                }
            }
        }
    }
    let mut runs = std::collections::BTreeMap::new();
    let mut history_bytes = 0usize;
    for source in sources {
        let events = result_source_events(store, &source, cuts)?;
        for event in events {
            if event.kind != "execution.agent_tool.receipt" {
                continue;
            }
            let key = event.payload["idempotency_key"]
                .as_str()
                .ok_or("process history original call missing")?;
            let stream = format!("execution-effect:{key}");
            let primary = store
                .event_by_idempotency_key(&stream, &format!("{key}:receipt"))
                .map_err(|error| error.to_string())?;
            let claimed = event.payload["effect_kind"] == "process"
                || has_process_fact(&event)
                || !event.payload["process_admission_ref"].is_null();
            let Some(primary) = primary else {
                if claimed {
                    return Err("process history original terminal missing".into());
                }
                continue;
            };
            if !claimed
                && primary.payload["effect"]["effect_kind"] != "process"
                && !has_process_fact(&primary)
                && primary.payload["process_admission_ref"].is_null()
            {
                continue;
            }
            pin_result_source(store, &stream, cuts)?;
            let run = authenticate_bounded_process(store, &primary)?;
            process_has_no_external_effects(&run)?;
            retain_process_execution(store, &run, cuts)?;
            if !runs.contains_key(&run.primary.event_id) {
                history_bytes = history_bytes
                    .checked_add(
                        serde_json::to_vec(&run)
                            .map_err(|error| error.to_string())?
                            .len(),
                    )
                    .ok_or("HistoryBudgetExceeded: bounded process facts")?;
                if history_bytes > 33554432 {
                    return Err("HistoryBudgetExceeded: bounded process facts".into());
                }
                runs.insert(run.primary.event_id.clone(), run);
            }
        }
    }
    Ok(runs.into_values().collect())
}

pub(super) struct ResolvedReviewResult {
    pub origin: ResultOrigin,
    pub result_sources: Vec<crate::ExpectedStreamRevision>,
    pub reference: String,
    pub content: ArtifactRef,
    pub kind: GoalResultKind,
    pub artifact_kinds: BTreeSet<String>,
    pub producers: Vec<ResultProducer>,
    pub effect_cursor: Option<u64>,
    pub effect_sources: Vec<crate::ExpectedStreamRevision>,
    pub effect_scopes: Vec<PermissionScope>,
    pub effect_targets: Vec<harness_contract::goal::ProofEffectTarget>,
    pub unscoped_effect: bool,
    pub primary_inputs: Vec<super::policy::ActualImmutableInput>,
    pub input_boundaries: Vec<ResultInputBoundary>,
    pub process_runs: Vec<AuthenticatedProcessExecution>,
}

#[derive(Clone, serde::Serialize)]
pub(super) struct ResultInputBoundary {
    pub producer: String,
    pub packet: Option<AgentTaskPacket>,
    pub step: Option<crate::step_snapshot::StepCallBinding>,
    pub cursor: u64,
    pub call_key: String,
    pub source: harness_contract::goal::ProofInputEvent,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginalRootEffectOwner {
    binding: harness_contract::projection::RuntimeActivityBinding,
    attempt: u32,
    graph_revision: u64,
}

pub(super) fn original_root_effect_owner(
    event: &DurableRuntimeEvent,
) -> Result<
    (
        harness_contract::projection::RuntimeActivityBinding,
        u32,
        u64,
    ),
    String,
> {
    let owner: OriginalRootEffectOwner =
        serde_json::from_value(event.payload["root_effect_owner"].clone())
            .map_err(|_| "root effect lacks its original admitted owner")?;
    let context: crate::CowdExecutionContext =
        serde_json::from_value(event.payload["root_effect_context"].clone())
            .map_err(|_| "root effect lacks its original admitted context")?;
    if owner.binding.validate().is_err()
        || owner.binding.node_id.as_deref().is_none_or(str::is_empty)
        || owner.binding.agent_run_id.is_some()
        || owner.attempt == 0
        || owner.graph_revision == 0
        || context.execution_id != owner.binding.root_execution_id
        || context.session_id != owner.binding.session_id
        || context.turn_id != owner.binding.turn_id
        || !event.payload["replay"]["parent_execution"].is_null()
    {
        return Err("root effect original owner/context is invalid".into());
    }
    Ok((owner.binding, owner.attempt, owner.graph_revision))
}

#[cfg(test)]
thread_local! {
    static ROOT_HISTORY_SOURCE_FAULT: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn root_history_source_fault(record: &mut DurableRuntimeEvent, index: bool) {
    ROOT_HISTORY_SOURCE_FAULT.with(|fault| match (fault.get(), index) {
        (Some("graph-content"), false) => {
            record.payload["sha256"] = serde_json::json!("sha256:invalid-content")
        }
        (Some("graph-transaction"), false) => {
            record.transaction_id = "foreign-start-transaction".into()
        }
        (Some("index-effect"), true) => {
            record.payload["effect"]["descriptor_hash"] = serde_json::json!("foreign-effect")
        }
        (Some("index-replay"), true) => {
            record.payload["replay"] = serde_json::json!({"call_id":"foreign-call"})
        }
        _ => {}
    });
}

fn original_root_start_record(
    store: &crate::RuntimeEventStore,
    owner: &harness_contract::projection::RuntimeActivityBinding,
    attempt: u32,
    running: &DurableRuntimeEvent,
) -> Result<DurableRuntimeEvent, String> {
    use crate::execution_core::graph::ExecutionGraphEvent;
    use harness_contract::execution_graph::ExecutionNodeStatus;
    let node = owner
        .node_id
        .as_deref()
        .ok_or("original root start node missing")?;
    let mut sequence = owner.revision;
    let mut tail = Vec::new();
    let mut start = None;
    let mut bytes = 0usize;
    let mut graph = loop {
        if sequence == 0 {
            return Err("original root start lacks a preceding graph checkpoint".into());
        }
        let record = store
            .list_stream_after(
                &owner.root_execution_id,
                sequence - 1,
                sequence,
                1,
                4 * 1024 * 1024,
            )?
            .into_iter()
            .next()
            .ok_or("original root start graph record missing")?;
        #[cfg(test)]
        let record = {
            let mut original = record;
            root_history_source_fault(&mut original, false);
            original
        };
        if record.sequence != sequence
            || record.stream_id != owner.root_execution_id
            || record.scope != crate::RuntimeEventScope::ExecutionGraph
            || record.actor.as_deref() != Some("execution_commit_service")
            || record.schema_version != 2
        {
            return Err("original root start graph record identity mismatch".into());
        }
        let body = store.resolve_graph_record(&record)?;
        bytes = bytes.saturating_add(
            serde_json::to_vec(&body)
                .map_err(|error| error.to_string())?
                .len(),
        );
        if bytes > 4 * 1024 * 1024 {
            return Err("HistoryBudgetExceeded: original root start".into());
        }
        let value: ExecutionGraphEvent = serde_json::from_value(body)
            .map_err(|_| "original root start graph content is invalid")?;
        if value.kind() != record.kind {
            return Err("original root start graph content kind mismatch".into());
        }
        if start.is_none() {
            if record.transaction_id != running.transaction_id
                || record.commit_cursor != running.commit_cursor
            {
                return Err(
                    "original root Running transition is not atomic with its graph start".into(),
                );
            }
            start = Some((record.clone(), value.clone()));
        }
        if matches!(
            &value,
            ExecutionGraphEvent::Planned { .. } | ExecutionGraphEvent::Checkpoint { .. }
        ) {
            break value.project(None)?;
        }
        tail.push(value);
        sequence -= 1;
    };
    for value in tail.into_iter().rev() {
        graph = value.project(Some(graph))?;
    }
    let (record, start) = start.ok_or("original root start missing")?;
    let start_binding = match &start {
        ExecutionGraphEvent::NodesStarted { bindings, .. } => bindings.get(node),
        ExecutionGraphEvent::NodeTransitioned {
            node_id,
            to: ExecutionNodeStatus::Running,
            binding,
            ..
        } if node_id == node => binding.as_ref(),
        ExecutionGraphEvent::Checkpoint {
            cause, bindings, ..
        } if matches!(
            cause.as_str(),
            "execution_graph.nodes_started" | "execution_graph.node_transitioned"
        ) =>
        {
            bindings.get(node)
        }
        _ => None,
    }
    .ok_or("original root owner is not bound to a real graph start")?;
    let spec = graph
        .nodes
        .iter()
        .find(|spec| spec.id == node)
        .ok_or("original root start spec missing")?;
    start_binding.validate_identity(spec)?;
    if graph.id != owner.root_execution_id
        || graph.revision != owner.revision
        || graph.parent_execution.is_some()
        || start_binding.attempt != attempt
        || graph.recovery_cursor.node_attempts.get(node).copied() != Some(attempt)
        || graph.node_statuses.get(node) != Some(&ExecutionNodeStatus::Running)
        || graph.lineage.as_ref().is_none_or(|lineage| {
            lineage.session_id != owner.session_id
                || lineage.turn_id != owner.turn_id
                || lineage.root_task_id != owner.root_task_id
                || lineage.task_id != owner.task_id
                || lineage.generation != owner.generation
        })
    {
        return Err("root admitted attempt does not match its original graph start".into());
    }
    Ok(record)
}

pub(super) fn authenticate_original_root_owner(
    store: &crate::RuntimeEventStore,
    intent: &DurableRuntimeEvent,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<
    (
        harness_contract::projection::RuntimeActivityBinding,
        u32,
        u64,
    ),
    String,
> {
    let (binding, attempt, revision) = original_root_effect_owner(intent)?;
    let node = binding
        .node_id
        .as_deref()
        .ok_or("original root owner has no node")?;
    let source = format!("{}:node:{node}", binding.root_execution_id);
    pin_result_source(store, &source, cuts)?;
    pin_result_source(store, &binding.root_execution_id, cuts)?;
    let original = store
        .event_by_idempotency_key(
            &source,
            &format!("{}:{node}:{}", binding.root_execution_id, binding.revision),
        )
        .map_err(|error| error.to_string())?
        .ok_or("original root Running transition missing")?;
    let fence = store
        .transaction_stream_revision_exact(&intent.transaction_id, &binding.root_execution_id)
        .map_err(|error| error.to_string())?;
    if original.schema_version != 1
        || original.kind != "execution_node.transitioned"
        || original.actor.as_deref() != Some("execution_commit_service")
        || original.scope != crate::RuntimeEventScope::ExecutionNode
        || original.activity_binding().as_ref() != Some(&binding)
        || original.payload["graph_id"] != binding.root_execution_id
        || original.payload["node_id"] != node
        || original.payload["graph_revision"].as_u64() != Some(binding.revision)
        || original.payload["to"] != "running"
        || original.commit_cursor > intent.commit_cursor
        || binding.revision > revision
        || fence.is_none_or(|fence| fence.expected_revision != revision)
    {
        return Err(
            "root effect does not authenticate its original Running owner/admission fence".into(),
        );
    }
    original_root_start_record(store, &binding, attempt, &original)?;
    Ok((binding, attempt, revision))
}

pub(super) fn root_owner_materials(
    store: &crate::RuntimeEventStore,
    intent: &DurableRuntimeEvent,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<Vec<DurableRuntimeEvent>, String> {
    let (owner, attempt, _) = authenticate_original_root_owner(store, intent, cuts)?;
    let node = owner
        .node_id
        .as_deref()
        .ok_or("root original node missing")?;
    let running = store
        .event_by_idempotency_key(
            &format!("{}:node:{node}", owner.root_execution_id),
            &format!("{}:{node}:{}", owner.root_execution_id, owner.revision),
        )
        .map_err(|error| error.to_string())?
        .ok_or("root original Running source missing")?;
    let start = original_root_start_record(store, &owner, attempt, &running)?;
    Ok(vec![intent.clone(), running, start])
}

pub(super) fn authenticate_root_receipt_owner(
    store: &crate::RuntimeEventStore,
    primary: &DurableRuntimeEvent,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<(), String> {
    let key = primary.payload["idempotency_key"]
        .as_str()
        .ok_or("root receipt original identity missing")?;
    pin_result_source(store, &primary.stream_id, cuts)?;
    let read = primary.kind == "execution.read.receipt";
    let suffix = if read { "read-intent" } else { "intent" };
    let intent = store
        .event_by_idempotency_key(&primary.stream_id, &format!("{key}:{suffix}"))
        .map_err(|error| error.to_string())?
        .ok_or("root receipt original admission missing")?;
    let (owner, attempt, _) = authenticate_original_root_owner(store, &intent, cuts)?;
    let node = owner
        .node_id
        .as_deref()
        .ok_or("root receipt original node missing")?;
    let index_source = format!(
        "execution-agent-receipts:{}:{node}:{attempt}",
        owner.root_execution_id
    );
    pin_result_source(store, &index_source, cuts)?;
    let index = store
        .event_by_idempotency_key(&index_source, &format!("agent-tool-receipt:{key}"))
        .map_err(|error| error.to_string())?
        .ok_or("root receipt original real-node index missing")?;
    #[cfg(test)]
    let index = {
        let mut original = index;
        root_history_source_fault(&mut original, true);
        original
    };
    if intent.kind
        != if read {
            "execution.read.intent"
        } else {
            "execution.effect.intent"
        }
        || intent.actor.as_deref() != Some("governed_tool")
        || intent.schema_version != 1
        || intent.scope != primary.scope
        || intent.commit_cursor >= primary.commit_cursor
        || intent.payload["effect"] != primary.payload["replay"]["invocation"]["effect"]
        || intent.payload["replay"] != primary.payload["replay"]
        || intent.payload["resolved_effect_scope"] != primary.payload["resolved_effect_scope"]
        || index.kind != "execution.agent_tool.receipt"
        || index.actor != primary.actor
        || index.scope != primary.scope
        || crate::ExecutionCommitService::receipt_observation_sequence(&index).is_none()
        || index.transaction_id != primary.transaction_id
        || index.commit_cursor != primary.commit_cursor
        || index.transaction_index <= primary.transaction_index
        || primary.status.as_deref() != Some("completed")
        || index.status != primary.status
        || primary.payload["effect"] != intent.payload["effect"]
        || index.payload["effect"] != intent.payload["effect"]
        || !index.payload["replay"].is_null()
            && index.payload["replay"] != primary.payload["replay"]
        || primary.payload["replay"]["call_id"] != intent.payload["tool_use_id"]
        || primary.payload["outcome"]["tool_use_id"] != intent.payload["tool_use_id"]
        || !primary.payload["tool_use_id"].is_null()
            && primary.payload["tool_use_id"] != intent.payload["tool_use_id"]
        || primary.payload["tool_name"] != intent.payload["tool_name"]
        || primary.payload["idempotency_key"] != intent.payload["idempotency_key"]
        || primary.payload["input_sha256"] != intent.payload["input_sha256"]
        || index.payload["idempotency_key"] != primary.payload["idempotency_key"]
        || index.payload["outcome"]["tool_use_id"] != intent.payload["tool_use_id"]
        || index.payload["outcome"]["tool_name"] != primary.payload["tool_name"]
        || index.payload["effect_kind"] != intent.payload["effect"]["effect_kind"]
        || index.payload["outcome"] != primary.payload["outcome"]
        || index.payload["resolved_effect_scope"] != primary.payload["resolved_effect_scope"]
        || [&intent, &index].iter().any(|event| {
            event.payload["root_effect_owner"] != primary.payload["root_effect_owner"]
                || event.payload["root_effect_context"] != primary.payload["root_effect_context"]
        })
    {
        return Err(
            "root receipt differs from its original owner/admission/real-node index".into(),
        );
    }
    Ok(())
}

pub(super) fn require_original_effect_scope(
    value: &serde_json::Value,
    descriptor: &ToolEffectDescriptor,
) -> Result<harness_contract::goal::ProofEffectScope, String> {
    use harness_contract::context::{
        EvidenceCoverageKind, WorkspaceAccessMode, WorkspaceObjectKind,
    };
    use harness_contract::policy::{PermissionOperation, PermissionResource};
    let scope: harness_contract::goal::ProofEffectScope = serde_json::from_value(value.clone())
        .map_err(|_| "original effect requires valid scope v2 with complete purpose provenance")?;
    scope.validate_purpose_mapping(descriptor)?;
    if scope.targets.iter().any(|target| {
        !descriptor.scopes.contains(&target.declared_scope)
            || target.path.workspace_id.is_empty()
            || target.path.repository_id.is_empty()
            || target.path.workspace_relative_path.is_empty()
            || target.declared_scope.resource != PermissionResource::File
            || target.exact && target.path.object_kind != WorkspaceObjectKind::File
            || match target.declared_scope.operation {
                PermissionOperation::Read => {
                    target.access_mode != WorkspaceAccessMode::Read
                        || target.coverage != EvidenceCoverageKind::ScopedContent
                }
                PermissionOperation::Write => {
                    target.access_mode != WorkspaceAccessMode::Write
                        || target.coverage != EvidenceCoverageKind::WriteEffect
                }
                _ => true,
            }
    }) || scope.complete
        && (scope.targets.len() != descriptor.scopes.len()
            || descriptor.uses_network
            || descriptor.spawns_process
            || descriptor.mutates_packages
            || descriptor.mutates_system
            || !matches!(
                descriptor.effect_kind,
                ToolEffectKind::Read | ToolEffectKind::Write
            )
            || descriptor.scopes.iter().any(|declared| {
                scope
                    .targets
                    .iter()
                    .filter(|target| &target.declared_scope == declared)
                    .count()
                    != 1
            }))
    {
        return Err("original effect canonical target mapping is invalid".into());
    }
    Ok(scope)
}

pub(super) fn receipt_effect_descriptor(
    event: &DurableRuntimeEvent,
) -> Result<ToolEffectDescriptor, String> {
    serde_json::from_value(event.payload["replay"]["invocation"]["effect"].clone())
        .map_err(|_| "original receipt has no authenticated compiled effect descriptor".into())
}

fn result_input_boundary(
    event: &DurableRuntimeEvent,
    producer: String,
    packet: Option<AgentTaskPacket>,
    step: Option<crate::step_snapshot::StepCallBinding>,
) -> Result<ResultInputBoundary, String> {
    let call_key = match &step {
        Some(step) => step.effect_idempotency_key(),
        None => format!("publication:{}", event.event_id),
    };
    if event.commit_cursor == 0 {
        return Err("result input boundary has no durable cursor".into());
    }
    Ok(ResultInputBoundary {
        producer,
        packet,
        step,
        cursor: event.commit_cursor,
        call_key,
        source: harness_contract::goal::ProofInputEvent {
            stream_id: event.stream_id.clone(),
            sequence: event.sequence,
            event_id: event.event_id.clone(),
            payload_digest: super::proof_value_digest(&event.payload),
        },
    })
}

pub(super) fn tool_effect_from_store(
    store: &crate::RuntimeEventStore,
    event: &DurableRuntimeEvent,
) -> Result<Option<ToolEffectDescriptor>, String> {
    let Some(plan_id) = event.payload["governed_plan_id"].as_str() else {
        return Ok(None);
    };
    let Some(call_id) = event.payload["tool_call_id"].as_str() else {
        return Ok(None);
    };
    let head = store
        .stream_revision(&event.stream_id)
        .map_err(|error| error.to_string())?;
    if event.sequence == 0 || event.sequence > head {
        return Err("proof source is outside its current-schema stream".into());
    }
    let mut through = event.sequence;
    loop {
        let page = store.list_stream_before(&event.stream_id, through, 64, 1024 * 1024)?;
        if page.is_empty() {
            return Ok(None);
        }
        through = page
            .last()
            .ok_or("empty proof page")?
            .sequence
            .checked_sub(1)
            .ok_or("invalid proof sequence")?;
        for candidate in page {
            if candidate.kind != "tool.execution_plan.created"
                || candidate.payload["plan_id"] != plan_id
            {
                continue;
            }
            let task = candidate.payload["tasks"]
                .as_array()
                .ok_or("original tool plan has no task array")?
                .iter()
                .find(|task| {
                    task["tool_call_id"] == call_id
                        && task["tool_name"] == event.payload["tool_name"]
                })
                .ok_or("original tool plan does not contain its claimed call")?;
            return serde_json::from_value(task["effect"].clone())
                .map(Some)
                .map_err(|_| "original tool plan has a malformed effect descriptor".into());
        }
    }
}

/// The refusal the model sees when a cited receipt's selector does not resolve.
///
/// Pure so the acceptance can assert what the model sees without a store:
/// V27-M4 requires the reference, the selector and the next step in the text,
/// because one30 was told six words - "artifact does not exist" - and spent
/// its window writing conclusions about the blocker instead of fixing it.
pub(super) fn unresolvable_receipt_error(
    subject: &str,
    reference: &str,
    selector: &str,
    error: &str,
    guidance: &str,
) -> String {
    format!(
        "{subject} {reference} points to durable content Runtime cannot read: {error} \
         (selector {selector}); {guidance}"
    )
}

/// The refusal the model sees when a resolved receipt cannot be read back.
pub(super) fn unreadable_receipt_error(
    subject: &str,
    reference: &str,
    selector: &str,
    error: &str,
    guidance: &str,
) -> String {
    format!(
        "{subject} {reference} has a durable receipt Runtime cannot read: {error} \
         (selector {selector}); {guidance}"
    )
}

/// The refusal the model sees when a Session receipt for a cited `tool://`
/// reference cannot be authenticated.
pub(super) fn unauthenticated_receipt_error(reference: &str, error: &str) -> String {
    format!(
        "result Tool evidence {reference} has no readable authenticated Session receipt: {error}"
    )
}

async fn tool_content(
    services: &RuntimeServices,
    program: &AgenticProgramProjection,
    session: &str,
    reference: &str,
) -> Result<ArtifactRef, String> {
    let id = reference.strip_prefix("tool://").ok_or_else(|| {
        format!(
            "result {reference} is not a Tool evidence reference; cite a `tool://` reference a \
             tool call in this Session returned"
        )
    })?;
    let history = services.session_history_reader();
    let hint = ArtifactRef::durable(
        reference,
        "",
        0,
        "application/octet-stream",
        format!("session:{session}"),
    );
    let metadata = resolve_result_metadata(
        services.event_store(),
        program,
        reference,
        &hint,
        None,
        super::ResultContentReaders {
            artifacts: Some(services.artifact_store().as_ref()),
            history: history.as_deref(),
            process_inputs: None,
        },
    )?;
    if metadata.content.selector.starts_with("artifact://") {
        return Ok(metadata.content);
    }
    let access = services
        .session_evidence_access(session, id)
        .await
        .map_err(|error| unauthenticated_receipt_error(reference, &error.to_string()))?
        .ok_or_else(|| {
            // Name the reference here too: the model cited `tool://X` and was
            // told its receipt did not exist, with no X to act on.
            format!(
                "result Tool evidence {reference} has no authenticated Session receipt; cite a \
                 reference a tool call in this Session actually returned"
            )
        })?;
    let content = services
        .artifact_store()
        .resolve(&access.retrieval_selector)
        .map_err(|error| {
            unresolvable_receipt_error(
                "result Tool evidence",
                reference,
                &access.retrieval_selector,
                &error.to_string(),
                "read it with evidence_retrieve instead of re-issuing the call",
            )
        })?;
    if content.sha256 != access.sha256
        || content.bytes != access.bytes
        || content.media_type != access.media_type
        || content.visibility_scope != access.visibility_scope
    {
        return Err(format!(
            "result Tool evidence {reference} does not match its durable content; re-read it with \
             evidence_retrieve and cite the reference that call returns"
        ));
    }
    Ok(content)
}

pub(super) async fn resolve_result(
    services: &RuntimeServices,
    actor: &AgentActorBinding,
    program: &AgenticProgramProjection,
    reference: &str,
) -> Result<ResolvedReviewResult, String> {
    let reference = super::review_result_identity(program, reference)?;
    resolve_material(services, actor, program, &reference).await
}

pub(super) async fn resolve_material(
    services: &RuntimeServices,
    actor: &AgentActorBinding,
    program: &AgenticProgramProjection,
    reference: &str,
) -> Result<ResolvedReviewResult, String> {
    let identity = super::identity::auxiliary_identity(program, reference)?;
    let reference = identity.reference();
    let content = if reference.starts_with("approval:v1:") {
        services
            .approval_result_content(&actor.session_id, reference)
            .await?
            .0
    } else if let Some(artifact) = program.artifacts.get(reference) {
        services
            .artifact_store()
            .resolve(&artifact.content_ref)
            .map_err(|error| {
                unresolvable_receipt_error(
                    "objective_review result",
                    reference,
                    &artifact.content_ref,
                    &error.to_string(),
                    "cite the result ref the Task returned from task_submit, or read it with \
                     evidence_retrieve",
                )
            })?
    } else if reference.starts_with("tool://") {
        tool_content(services, program, &actor.session_id, reference).await?
    } else if let super::identity::MaterialIdentity::Content(selector) = &identity {
        services
            .artifact_store()
            .resolve(selector)
            .map_err(|error| {
                format!(
                    "objective_review result {reference} points to durable content Runtime cannot read: {error} \
                     (resolved selector {selector}; cite the result ref the Task returned from task_submit, \
                     or read it with evidence_retrieve)"
                )
            })?
    } else {
        return Err(format!(
            "review result has no Runtime-resolved producer:{reference}"
        ));
    };
    let history = services.session_history_reader();
    resolve_result_metadata(
        services.event_store(),
        program,
        reference,
        &content,
        None,
        super::ResultContentReaders {
            artifacts: Some(services.artifact_store().as_ref()),
            history: history.as_deref(),
            process_inputs: None,
        },
    )
}

pub(super) fn pin_result_source(
    store: &crate::RuntimeEventStore,
    source: &str,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<u64, String> {
    let revision = store
        .stream_revision(source)
        .map_err(|error| error.to_string())?;
    if let Some(previous) = cuts.insert(source.to_string(), revision) {
        if previous != revision {
            return Err(format!(
                "result metadata source changed during resolution; rebuild the review; source={source} expected={previous} actual={revision} phase=result_source_pin"
            ));
        }
    }
    Ok(revision)
}

pub(super) fn result_source_events(
    store: &crate::RuntimeEventStore,
    source: &str,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<Vec<DurableRuntimeEvent>, String> {
    let head = pin_result_source(store, source, cuts)?;
    let mut after = 0;
    let mut events = Vec::new();
    while after < head {
        let page = store.list_stream_after(source, after, head, 64, 4 * 1024 * 1024)?;
        if page.is_empty() {
            return Err("result metadata source has an incomplete durable page".into());
        }
        for event in page {
            if event.stream_id != source || event.sequence != after + 1 || event.sequence > head {
                return Err("result metadata source has an invalid causal cursor".into());
            }
            after = event.sequence;
            events.push(event);
        }
    }
    pin_result_source(store, source, cuts)?;
    Ok(events)
}

fn result_packet(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    execution: &str,
    program: &AgenticProgramProjection,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<AgentTaskPacket, String> {
    let graphs = crate::ExecutionGraphStateStore::new(std::sync::Arc::clone(store));
    let source = crate::execution_core::graph::graph_id_for_lookup(execution);
    pin_result_source(store, source, cuts)?;
    let graph = graphs.load(execution).map_err(|error| error.to_string())?;
    if cuts.get(&graph.id) != Some(&graph.revision)
        || graph
            .lineage
            .as_ref()
            .is_none_or(|lineage| lineage.session_id != program.session_id)
    {
        return Err("result producer graph has a stale or foreign durable origin".into());
    }
    let mut packets = graph.nodes.iter().filter_map(|node| {
        if node.kind != harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
            return None;
        }
        serde_json::from_str::<AgentTaskPacket>(&node.payload_ref)
            .ok()
            .filter(|packet| {
                super::packet_identity_matches(packet.graph_id(), packet.run_id(), execution)
                    && packet.graph_id() == graph.id
                    && packet.node_id() == node.id
            })
    });
    let packet = packets
        .next()
        .ok_or("result physical producer packet missing")?;
    if packets.next().is_some()
        || packet.session_id() != program.session_id
        || packet.binding.as_ref().is_none_or(|binding| {
            !crate::agent::binding::recompute_binding_digest(binding)
                .is_ok_and(|digest| digest == binding.binding_digest)
        })
    {
        return Err("result physical producer packet is ambiguous or unauthenticated".into());
    }
    pin_result_source(store, &graph.id, cuts)?;
    Ok(packet)
}

fn authenticate_callback_parent_chain(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    producer_graph: &str,
    binding: &harness_contract::projection::RuntimeActivityBinding,
    programs: &[AgenticProgramProjection],
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<(), String> {
    let graphs = crate::ExecutionGraphStateStore::new(std::sync::Arc::clone(store));
    let mut execution = producer_graph.to_string();
    let mut visited = BTreeSet::new();
    loop {
        if !visited.insert(execution.clone()) {
            return Err("result callback producer parent chain is cyclic".into());
        }
        pin_result_source(store, &execution, cuts)?;
        let graph = graphs.load(&execution).map_err(|error| error.to_string())?;
        if cuts.get(&execution) != Some(&graph.revision)
            || graph.lineage.as_ref().is_none_or(|lineage| {
                lineage.session_id != binding.session_id
                    || lineage.turn_id != binding.turn_id
                    || lineage.root_task_id != binding.root_task_id
            })
        {
            return Err("result callback producer has a foreign parent lineage".into());
        }
        let Some(parent) = graph.parent_execution else {
            if !programs.iter().any(|program| {
                program.root_execution_id.as_deref() == Some(execution.as_str())
                    && program.session_id == binding.session_id
                    && program.turn_id == binding.turn_id
            }) || !visited.contains(&binding.root_execution_id)
            {
                return Err(
                    "result callback producer does not reach its bound Program root".into(),
                );
            }
            return Ok(());
        };
        pin_result_source(
            store,
            &format!("execution-lineage:{}", parent.execution_id),
            cuts,
        )?;
        if !graphs
            .child_links(&parent.execution_id)
            .map_err(|error| error.to_string())?
            .iter()
            .any(|link| {
                link.child_execution_id == execution && link.parent_node_id == parent.node_id
            })
        {
            return Err("result callback producer lacks its original parent link".into());
        }
        pin_result_source(store, &parent.execution_id, cuts)?;
        let original_parent = graphs
            .load(&parent.execution_id)
            .map_err(|error| error.to_string())?;
        if !original_parent
            .nodes
            .iter()
            .any(|node| node.id == parent.node_id)
        {
            return Err("result callback producer parent node is missing".into());
        }
        execution = parent.execution_id;
    }
}

pub(super) fn result_tool_plan<'events>(
    events: &'events [DurableRuntimeEvent],
    event: &DurableRuntimeEvent,
) -> Result<Option<&'events serde_json::Value>, String> {
    let Some(plan_value) = event.payload.get("governed_plan_id").filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let plan_id = plan_value
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or("result tool has an invalid claimed plan identity")?;
    let call_id = event.payload["tool_call_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or("result tool has no claimed plan call identity")?;
    let mut selected = None;
    for plan in events.iter().filter(|plan| {
        plan.kind == "tool.execution_plan.created" && plan.payload["plan_id"] == plan_id
    }) {
        if plan.actor.as_deref() != Some("conversation_runtime")
            || plan.stream_id != event.stream_id
            || plan.commit_cursor > event.commit_cursor
        {
            return Err("result tool plan has a foreign owner or ordering".into());
        }
        for task in plan.payload["tasks"]
            .as_array()
            .ok_or("result tool plan tasks missing")?
        {
            if task["tool_call_id"] != call_id {
                continue;
            }
            if selected.is_some() || task["tool_name"] != event.payload["tool_name"] {
                return Err("result tool plan has ambiguous call ownership".into());
            }
            if !task["step_binding"].is_null() {
                let binding = event
                    .activity_binding()
                    .ok_or("result tool has no physical owner")?;
                let step: crate::step_snapshot::StepCallBinding =
                    serde_json::from_value(task["step_binding"].clone())
                        .map_err(|_| "result original plan has an invalid Step binding")?;
                let expected = step
                    .snapshot
                    .identity
                    .execution_id
                    .as_deref()
                    .ok_or("result original Step has no execution")?;
                if step.snapshot.identity.session_id != binding.session_id
                    || step.snapshot.identity.turn_id != binding.turn_id
                    || match binding.agent_run_id.as_deref() {
                        Some(run) => step.snapshot.identity.run_ref.as_deref() != Some(run),
                        None => expected != binding.root_execution_id,
                    }
                    || ![
                        ("session", binding.session_id.as_str()),
                        ("turn", binding.turn_id.as_str()),
                        ("execution", expected),
                    ]
                    .into_iter()
                    .all(|(kind, id)| {
                        plan.refs
                            .iter()
                            .any(|reference| reference.kind == kind && reference.id == id)
                    })
                {
                    return Err(
                        "result original plan crosses its exact Session/Turn/Step owner".into(),
                    );
                }
            }
            selected = Some(task);
        }
    }
    selected
        .map(Some)
        .ok_or_else(|| format!(
            "result original tool plan does not contain its claimed call: plan_id={plan_id} call_id={call_id} owner_event_id={}",
            event.event_id
        ))
}

pub(super) fn original_tool_primary(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    owner: &DurableRuntimeEvent,
    task: &serde_json::Value,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<DurableRuntimeEvent, String> {
    let step: crate::step_snapshot::StepCallBinding =
        serde_json::from_value(task["step_binding"].clone()).map_err(|_| {
            "result_origin_requires_original_immutable_content: publication Step missing"
        })?;
    let binding = owner
        .activity_binding()
        .ok_or("publication has no original tool owner")?;
    if step.snapshot.identity.session_id != binding.session_id
        || step.snapshot.identity.turn_id != binding.turn_id
        || owner.payload["tool_call_id"] != step.origin.call_id
        || task["tool_name"] != owner.payload["tool_name"]
    {
        return Err("publication original Step crosses its physical owner".into());
    }
    let key = step.effect_idempotency_key();
    let source = format!("execution-effect:{key}");
    pin_result_source(store, &source, cuts)?;
    let mut selected = None;
    for suffix in ["read-receipt", "receipt"] {
        if let Some(event) = store
            .event_by_idempotency_key(&source, &format!("{key}:{suffix}"))
            .map_err(|error| error.to_string())?
        {
            if selected.replace(event).is_some() {
                return Err("publication has ambiguous original primary receipts".into());
            }
        }
    }
    let primary = selected
        .ok_or("result_origin_requires_original_immutable_content: publication primary missing")?;
    let digest =
        harness_contract::step::canonical_digest("tool-effect-payload", &task["normalized_input"])
            .map_err(|error| error.to_string())?;
    if primary.schema_version != 1
        || primary.status.as_deref() != Some("completed")
        || !matches!(
            primary.kind.as_str(),
            "execution.read.receipt" | "execution.effect.receipt"
        )
        || primary.actor.as_deref() != Some("governed_tool")
        || primary.scope != crate::RuntimeEventScope::ExecutionNode
        || primary.payload["replay"]["origin"]
            != serde_json::to_value(&step.origin).map_err(|error| error.to_string())?
        || primary.payload["replay"]["call_id"] != step.origin.call_id
        || primary.payload["replay"]["payload_digest"] != digest
        || primary.payload["tool_name"] != task["tool_name"]
        || !has_process_fact(&primary)
            && (primary.payload["outcome"]["status"] != "executed"
                || primary.payload["outcome"]["error"].as_str().is_some())
        || primary.commit_cursor >= owner.commit_cursor
    {
        return Err("publication primary does not authenticate its actual output".into());
    }
    if primary.kind == "execution.effect.receipt" {
        let intent = store
            .event_by_idempotency_key(&source, &format!("{key}:intent"))
            .map_err(|error| error.to_string())?
            .ok_or("result producer effect has no original admitted intent")?;
        if intent.schema_version != 1
            || intent.kind != "execution.effect.intent"
            || intent.actor.as_deref() != Some("governed_tool")
            || intent.scope != primary.scope
            || intent.payload["effect"]["descriptor_hash"] != primary.payload["descriptor_hash"]
            || intent.payload["effect"]["descriptor_hash"] != task["effect"]["descriptor_hash"]
            || intent.payload["effect"] != task["effect"]
            || intent.payload["resolved_effect_scope"] != primary.payload["resolved_effect_scope"]
        {
            return Err(
                "result producer effect disagrees with its original admitted scope/classifier"
                    .into(),
            );
        }
    }
    let descriptor: ToolEffectDescriptor = serde_json::from_value(task["effect"].clone())
        .map_err(|_| "original tool plan has no typed effect descriptor")?;
    if primary.payload["replay"]["invocation"]["effect"] != task["effect"] {
        return Err("original tool receipt descriptor differs from original plan".into());
    }
    if primary.payload["resolved_effect_scope"].is_null()
        && descriptor.effect_kind == ToolEffectKind::Read
        && !descriptor
            .scopes
            .iter()
            .any(|scope| scope.resource == harness_contract::policy::PermissionResource::File)
        && !descriptor.spawns_process
        && !descriptor.uses_network
        && !descriptor.mutates_packages
        && !descriptor.mutates_system
    {
        if !primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null() {
            let seal = crate::context_evidence::decode_read_acquisition(
                &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
            )
            .map_err(str::to_string)?;
            super::policy::read_original_scope(&primary, &descriptor, &seal)?;
        }
    } else {
        require_original_effect_scope(&primary.payload["resolved_effect_scope"], &descriptor)?;
    }
    let root_owner = if primary.payload["replay"]["parent_execution"].is_null() {
        let suffix = if primary.kind == "execution.read.receipt" {
            "read-intent"
        } else {
            "intent"
        };
        let intent = store
            .event_by_idempotency_key(&source, &format!("{key}:{suffix}"))
            .map_err(|error| error.to_string())?
            .ok_or("root output lacks its original admitted intent")?;
        let (admitted, attempt, graph_revision) =
            authenticate_original_root_owner(store, &intent, cuts)?;
        if intent.kind
            != if suffix == "read-intent" {
                "execution.read.intent"
            } else {
                "execution.effect.intent"
            }
            || intent.schema_version != 1
            || intent.actor != primary.actor
            || intent.scope != primary.scope
            || intent.commit_cursor >= primary.commit_cursor
            || admitted.root_execution_id != binding.root_execution_id
            || admitted.session_id != binding.session_id
            || admitted.turn_id != binding.turn_id
            || step.snapshot.identity.execution_id.as_deref()
                != Some(admitted.root_execution_id.as_str())
            || !super::binding_is_owned_by_root(&binding, Some(&admitted.root_execution_id))
            || binding
                .node_id
                .as_ref()
                .is_some_and(|node| Some(node) != admitted.node_id.as_ref())
            || intent.payload["effect"] != task["effect"]
            || intent.payload["replay"] != primary.payload["replay"]
            || intent.payload["root_effect_context"] != primary.payload["root_effect_context"]
            || intent.payload["root_effect_owner"] != primary.payload["root_effect_owner"]
            || intent.payload["resolved_effect_scope"] != primary.payload["resolved_effect_scope"]
        {
            return Err("root read/write output disagrees with its original caller owner".into());
        }
        authenticate_root_receipt_owner(store, &primary, cuts)?;
        Some((admitted, attempt, graph_revision))
    } else {
        None
    };
    let graph_owned = match &root_owner {
        Some((admitted, _, _)) => admitted.root_execution_id.clone(),
        None => primary.payload["replay"]["parent_execution"]["execution_id"]
            .as_str()
            .ok_or("publication primary lacks its tool ticket graph")?
            .to_string(),
    };
    let node_owned = match &root_owner {
        Some((admitted, _, _)) => admitted
            .node_id
            .clone()
            .ok_or("root output lacks its original caller node")?,
        None => primary.payload["replay"]["parent_execution"]["node_id"]
            .as_str()
            .ok_or("publication primary lacks its tool ticket node")?
            .to_string(),
    };
    let graph = graph_owned.as_str();
    let node = node_owned.as_str();
    pin_result_source(store, graph, cuts)?;
    let graph_state = crate::ExecutionGraphStateStore::new(std::sync::Arc::clone(store))
        .load(graph)
        .map_err(|error| error.to_string())?;
    if cuts.get(graph) != Some(&graph_state.revision)
        || !graph_state
            .nodes
            .iter()
            .any(|candidate| candidate.id == node)
    {
        return Err("publication tool ticket graph changed during original receipt lookup".into());
    }
    let original_node = graph_state.nodes.iter().find(|candidate| candidate.id == node)
        .ok_or("publication original graph node missing")?;
    let packet_attempt = if root_owner.is_none()
        && original_node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask {
        let run = binding.agent_run_id.as_deref()
            .ok_or_else(|| format!("delegated tool output has no original run identity: graph={graph} node={node} kind={:?} step={}",
                original_node.kind, step.snapshot.identity.step_id))?;
        let packet = graph_state
            .nodes
            .iter()
            .find(|candidate| {
                candidate.id == node
                    && candidate.kind
                        == harness_contract::execution_graph::ExecutionNodeKind::AgentTask
            })
            .and_then(|candidate| {
                serde_json::from_str::<AgentTaskPacket>(&candidate.payload_ref).ok()
            })
            .ok_or("Agent tool output has no original parent packet")?;
        let identity = &step.snapshot.identity;
        let (expected_execution, expected_actor) = match &step.origin.source {
            harness_contract::step::CallSource::Provider { attempt_id }
                if !attempt_id.is_empty()
                    && attempt_id == &step.snapshot.provider_attempt.attempt_id =>
            {
                (
                    packet.run_id(),
                    packet.assignment.execution_identity.principal_id(),
                )
            }
            harness_contract::step::CallSource::Process {
                worker_id,
                sequence,
            } if worker_id == packet.run_id()
                && *sequence > 0
                && identity.attempt == Some(packet.attempt)
                && step.snapshot.related_revisions.get("worker_run") == Some(worker_id) =>
            {
                (packet.graph_id(), packet.agent_id())
            }
            _ => return Err("Agent tool output original Step producer source mismatch".into()),
        };
        if packet.run_id() != run
            || packet.graph_id() != graph
            || packet.node_id() != node
            || binding.agent_instance_id.as_deref() != Some(packet.agent_id())
            || packet.session_id() != binding.session_id
            || packet.assignment.execution_identity.turn_id() != Some(binding.turn_id.as_str())
            || identity.execution_id.as_deref() != Some(expected_execution)
            || identity.actor_ref.as_deref() != Some(expected_actor)
            || identity.run_ref.as_deref() != Some(packet.run_id())
            || identity
                .attempt
                .is_some_and(|attempt| attempt != packet.attempt)
            || step.origin.origin_step_id != identity.step_id
            || step.origin.catalog_digest != step.snapshot.catalog_digest
        {
            return Err("Agent tool output Step does not match its original packet".into());
        }
        packet.attempt
    } else if root_owner.is_none() {
        let identity = &step.snapshot.identity;
        if !matches!((original_node.kind, original_node.executor_kind.as_str()),
                (harness_contract::execution_graph::ExecutionNodeKind::InlineModel, "inline_model")
                | (harness_contract::execution_graph::ExecutionNodeKind::ToolBatch, "tool_batch"))
            || graph_state.parent_execution.is_some()
            || binding.root_execution_id != graph
            || binding.agent_run_id.is_some()
            || identity.execution_id.as_deref() != Some(graph)
            || identity.run_ref.is_some()
            || step.origin.origin_step_id != identity.step_id
            || step.origin.catalog_digest != step.snapshot.catalog_digest
            || !matches!(&step.origin.source, harness_contract::step::CallSource::Provider { attempt_id }
                if !attempt_id.is_empty() && attempt_id == &step.snapshot.provider_attempt.attempt_id)
            || !super::binding_is_owned_by_root(&binding, Some(graph))
        {
            return Err(format!("graph-bound root output original caller mismatch: graph={graph} node={node} kind={:?} step={} actor={:?}",
                original_node.kind, identity.step_id, identity.actor_ref));
        }
        0
    } else {
        0
    };
    if root_owner
        .as_ref()
        .is_some_and(|(_, _, revision)| *revision > graph_state.revision)
        || graph_state.lineage.as_ref().is_none_or(|lineage| {
            lineage.session_id != binding.session_id || lineage.turn_id != binding.turn_id
        })
    {
        return Err("original tool output has foreign graph lineage or owner revision".into());
    }
    let attempts = graph_state
        .recovery_cursor
        .node_attempts
        .get(node)
        .copied()
        .unwrap_or_default()
        .max(packet_attempt);
    let mut original_index = None;
    let original_attempt = root_owner
        .as_ref()
        .map(|(_, attempt, _)| *attempt)
        .or_else(|| (packet_attempt > 0).then_some(packet_attempt));
    let first_attempt = original_attempt.unwrap_or(1);
    let last_attempt = original_attempt.unwrap_or(attempts);
    if last_attempt == 0 || last_attempt > attempts {
        return Err("original tool output attempt has no graph provenance".into());
    }
    for attempt in first_attempt..=last_attempt {
        let index_source = format!("execution-agent-receipts:{graph}:{node}:{attempt}");
        pin_result_source(store, &index_source, cuts)?;
        if let Some(index) = store
            .event_by_idempotency_key(&index_source, &format!("agent-tool-receipt:{key}"))
            .map_err(|error| error.to_string())?
        {
            if root_owner.is_none() && packet_attempt == 0 {
                let states = crate::ExecutionGraphStateStore::new(std::sync::Arc::clone(store));
                let original_binding = states.current_binding(&graph_state, node, attempt)
                    .map_err(|error| error.to_string())?;
                let (ticket, spec) = states.binding_start_ticket(&graph_state, node, &original_binding)
                    .map_err(|error| error.to_string())?;
                if spec.kind != original_node.kind || spec.executor_kind != original_node.executor_kind
                    || ticket.graph_id != graph || ticket.node_id != node || ticket.attempt != attempt
                {
                    return Err("graph-bound root output original start kind conflicts".into());
                }
                if spec.kind == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch {
                    let (calls, _) = crate::host::decode_tool_batch(&ticket.payload_ref)
                        .map_err(|error| error.to_string())?;
                    let mut originals = calls.iter().filter(|call| call.id == step.origin.call_id);
                    let call = originals
                        .next()
                        .ok_or("graph-bound root output original batch call missing")?;
                    let input = harness_contract::step::parse_strict_json(&call.input)
                        .map_err(|error| error.to_string())?;
                    if originals.next().is_some()
                        || call.binding.as_ref() != Some(&step)
                        || task["tool_name"] != call.name
                        || harness_contract::step::canonical_digest("tool-effect-payload", &input)
                            .map_err(|error| error.to_string())?
                            != digest
                    {
                        return Err(
                            "graph-bound root output original batch Step/input conflicts".into(),
                        );
                    }
                }
                for original in process_ticket_start(store, graph, node, attempt, &primary)? {
                    pin_result_source(store, &original.stream_id, cuts)?;
                    if original.kind == "execution_node.transitioned" {
                        let revision = original.payload["graph_revision"]
                            .as_u64()
                            .filter(|revision| *revision > 0)
                            .ok_or("graph-bound root output original start revision missing")?;
                        let lineage = graph_state
                            .lineage
                            .as_ref()
                            .ok_or("graph-bound root output original lineage missing")?;
                        let root_activity = format!("activity:execution:{graph}");
                        let expected = harness_contract::projection::RuntimeActivityBinding {
                            root_execution_id: graph.to_string(),
                            session_id: lineage.session_id.clone(),
                            turn_id: lineage.turn_id.clone(),
                            root_task_id: lineage.root_task_id.clone(),
                            task_id: lineage.task_id.clone(),
                            activity_id: format!("{root_activity}:node:{node}"),
                            node_id: Some(node.to_string()),
                            parent_activity_id: Some(root_activity.clone()),
                            initiator_activity_id: Some(root_activity),
                            team_run_id: None,
                            agent_instance_id: None,
                            agent_run_id: None,
                            skill_id: None,
                            skill_revision: None,
                            skill_activation_id: None,
                            tool_contract_id: None,
                            tool_call_id: None,
                            approval_id: None,
                            parallel_group_id: None,
                            revision,
                            fence: revision,
                            generation: lineage.generation,
                        };
                        if original.activity_binding().as_ref() != Some(&expected)
                            || !original.refs.iter().any(|reference| {
                                reference.kind == "execution_graph" && reference.id == graph
                            })
                        {
                            return Err("graph-bound root output has a foreign original caller".into());
                        }
                    }
                }
            }
            if original_index.replace(index).is_some() {
                return Err("original tool output has ambiguous attempt indexes".into());
            }
        }
    }
    if let Some(index) = original_index {
        if crate::ExecutionCommitService::receipt_observation_sequence(&index).is_none()
            || index.status.as_deref() != Some("completed")
            || index.kind != "execution.agent_tool.receipt"
            || index.actor != primary.actor
            || index.scope != primary.scope
            || index.transaction_id != primary.transaction_id
            || index.commit_cursor != primary.commit_cursor
            || index.transaction_index <= primary.transaction_index
            || index.payload["outcome"] != primary.payload["outcome"]
            || index.payload["resolved_effect_scope"] != primary.payload["resolved_effect_scope"]
            || root_owner.is_some()
                && (index.payload["root_effect_owner"] != primary.payload["root_effect_owner"]
                    || index.payload["root_effect_context"]
                        != primary.payload["root_effect_context"])
        {
            return Err("publication primary/index do not authenticate one original output".into());
        }
    } else {
        return Err("original tool output has no original physical index".into());
    }
    Ok(primary)
}

fn original_output_identity(owner: &DurableRuntimeEvent) -> Result<Option<(&str, u64)>, String> {
    let output = &owner.payload["output_ref"];
    if output.is_null() {
        return Ok(None);
    }
    let reference = owner.payload["full_output_ref"]
        .as_str()
        .ok_or("original Tool output lacks its canonical raw reference")?;
    let digest = output["sha256"]
        .as_str()
        .ok_or("original Tool output SHA missing")?;
    let normalized = digest.strip_prefix("sha256:").unwrap_or(digest);
    let bytes = output["byte_count"]
        .as_u64()
        .ok_or("original Tool output extent missing")?;
    if output["ref_id"] != reference
        || output["tool_call_id"] != owner.payload["tool_call_id"]
        || normalized.len() != 64
        || !normalized.bytes().all(|byte| byte.is_ascii_hexdigit())
        || owner.payload["output_byte_count"]
            .as_u64()
            .is_some_and(|expected| expected != bytes)
    {
        return Err("original Tool output identity crosses its authenticated owner".into());
    }
    Ok(Some((digest, bytes)))
}

fn raw_input_matches_owner(
    payload: &serde_json::Value,
    owner: &DurableRuntimeEvent,
    task: Option<&serde_json::Value>,
) -> bool {
    let Some(recorded) = payload["input_hash"].as_str() else {
        return false;
    };
    let Some(expected) = owner.payload["input_hash"].as_str() else {
        return false;
    };
    if recorded == expected {
        return !recorded.is_empty();
    }
    let source = format!(
        "runtime-tool:{}",
        owner.payload["tool_call_id"].as_str().unwrap_or_default()
    );
    if payload["source_evidence_ref"] != source
        || payload["native_staged_artifact"] == true
        || recorded.len() != 16
        || !recorded.bytes().all(|byte| byte.is_ascii_hexdigit())
        || expected.len() != 64
        || !expected.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return false;
    }
    let plan_input = task
        .and_then(|task| task.get("normalized_input"))
        .and_then(|input| serde_json::to_string(input).ok());
    owner.payload["input_preview"]
        .as_str()
        .into_iter()
        .chain(plan_input.as_deref())
        .any(|input| {
            crate::tool_invocation::stable_hash(input) == expected
                && recorded
                    == format!(
                        "{:016x}",
                        model_protocol::fingerprint::stable_hash_bytes(input.as_bytes())
                    )
        })
}

fn observed_raw_selector_conflict(
    readers: super::ResultContentReaders<'_>,
    owner: &DurableRuntimeEvent,
    selector: &str,
) -> bool {
    let (Some(history), Some(binding), Some(reference)) = (
        readers.history,
        owner.activity_binding(),
        owner.payload["full_output_ref"].as_str(),
    ) else {
        return false;
    };
    let Some(evidence_id) = reference.strip_prefix("tool://") else {
        return false;
    };
    let Ok(mut remaining) = history.domain_event_count_blocking(&binding.session_id) else {
        return false;
    };
    let mut after = 0;
    while remaining > 0 {
        let limit = remaining.min(128);
        let Ok(page) = history.domain_events_by_kind_blocking(
            &binding.session_id,
            "evidence.raw.persisted",
            after,
            limit,
        ) else {
            return false;
        };
        if page.is_empty() || page.len() > limit {
            return false;
        }
        for event in page {
            remaining -= 1;
            if event.session_id == binding.session_id
                && event.kind == "evidence.raw.persisted"
                && event.payload["evidence_id"] == evidence_id
                && event.payload["artifact_selector"] == selector
            {
                return true;
            }
            if event.sequence < after {
                return false;
            }
            let Some(next) = event.sequence.checked_add(1) else {
                return false;
            };
            after = next;
        }
    }
    false
}

fn original_session_raw(
    readers: super::ResultContentReaders<'_>,
    owner: &DurableRuntimeEvent,
    task: Option<&serde_json::Value>,
) -> Result<(ArtifactRef, ResultOrigin), String> {
    let binding = owner
        .activity_binding()
        .ok_or("raw origin has no physical owner")?;
    let reference = owner.payload["full_output_ref"]
        .as_str()
        .ok_or("raw origin lacks its canonical full_output_ref")?;
    let evidence_id = reference
        .strip_prefix("tool://")
        .filter(|id| !id.is_empty())
        .ok_or("raw origin has no canonical Tool identity")?;
    let history = readers.history.ok_or(
        "result_origin_requires_original_immutable_content: original Session history reader missing",
    )?;
    let mut from_sequence = 0;
    let mut original = None;
    let mut original_metadata = None;
    let mut remaining = history
        .domain_event_count_blocking(&binding.session_id)
        .map_err(|error| error.to_string())?;
    while remaining > 0 {
        let limit = remaining.min(128);
        let page = history
            .domain_events_by_kind_blocking(
                &binding.session_id,
                "evidence.raw.persisted",
                from_sequence,
                limit,
            )
            .map_err(|error| error.to_string())?;
        if page.is_empty() {
            break;
        }
        if page.len() > limit {
            return Err("original Session raw history exceeds its finite page budget".into());
        }
        for event in page {
            remaining -= 1;
            if event.session_id != binding.session_id
                || event.kind != "evidence.raw.persisted"
                || event.sequence < from_sequence
            {
                return Err("original Session raw history has an invalid page".into());
            }
            from_sequence = event
                .sequence
                .checked_add(1)
                .ok_or("original Session raw cursor overflow")?;
            let payload = &event.payload;
            if payload["evidence_id"] != evidence_id {
                continue;
            }
            if payload["session_id"] != binding.session_id
                || payload["tool_call_id"] != owner.payload["tool_call_id"]
                || payload["tool_name"] != owner.payload["tool_name"]
                || !raw_input_matches_owner(payload, owner, task)
                || payload["is_error"] != false
                || payload["visibility_scope"] != format!("session:{}", binding.session_id)
            {
                return Err(
                    "original Session raw metadata crosses its exact lifecycle owner".into(),
                );
            }
            let metadata = serde_json::json!([
                payload["session_id"],
                payload["tool_call_id"],
                payload["tool_name"],
                payload["input_hash"],
                payload["is_error"],
                payload["source_evidence_ref"],
                payload["native_staged_artifact"],
            ]);
            if original_metadata
                .as_ref()
                .is_some_and(|previous| previous != &metadata)
            {
                return Err(
                    "original Session raw identity has conflicting producer metadata".into(),
                );
            }
            original_metadata = Some(metadata);
            let access =
                crate::context_evidence::raw::access_from_persisted_payload(evidence_id, payload)
                    .ok_or("original Session raw receipt is incomplete")?;
            let artifact = ArtifactRef::durable(
                access.retrieval_selector,
                access.sha256,
                access.bytes,
                access.media_type,
                access.visibility_scope,
            );
            let call = owner.payload["tool_call_id"]
                .as_str()
                .ok_or("original raw call missing")?;
            let prefix = format!("tool-raw-{call}-");
            let suffix = evidence_id
                .strip_prefix(&prefix)
                .ok_or("raw receipt has a foreign producer format")?;
            let staged = payload["native_staged_artifact"] == true;
            if staged {
                if suffix
                    != artifact
                        .sha256
                        .strip_prefix("sha256:")
                        .unwrap_or(&artifact.sha256)
                {
                    return Err("staged raw identity disagrees with its original digest".into());
                }
            } else if suffix.len() != 16 || !suffix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err("ordinary raw identity is not its original fingerprint format".into());
            }
            if original
                .as_ref()
                .is_some_and(|previous| previous != &artifact)
            {
                return Err("original Session raw identity has conflicting exact receipts".into());
            }
            original = Some(artifact);
        }
    }
    original.map(|artifact| {
        let call = owner.payload["tool_call_id"].as_str().unwrap_or_default();
        let staged = format!("tool://tool-raw-{call}-{}", artifact.sha256.trim_start_matches("sha256:"));
        let origin = if reference == staged {
            ResultOrigin::Strong
        } else if remaining == 0 {
            ResultOrigin::LegacyIncomplete
        } else {
            ResultOrigin::LegacyWeak
        };
        (artifact, origin)
    }).ok_or_else(|| format!(
        "result_origin_requires_original_immutable_content: exact original Session raw mapping missing:{reference}; finite recovery budget exhausted={}", remaining == 0
    ))
}

fn checked_original_bytes(bytes: Vec<u8>, digest: &str, expected: u64) -> Result<Vec<u8>, String> {
    let normalized = digest.strip_prefix("sha256:").unwrap_or(digest);
    let actual = crate::context_evidence::read_sha256(&bytes);
    if normalized.len() != 64
        || !normalized.bytes().all(|byte| byte.is_ascii_hexdigit())
        || bytes.len() as u64 != expected
        || actual.strip_prefix("sha256:").unwrap_or(&actual) != normalized
    {
        return Err("original immutable Tool body disagrees with its exact identity".into());
    }
    Ok(bytes)
}

fn read_original_session_body(
    readers: super::ResultContentReaders<'_>,
    owner: &DurableRuntimeEvent,
    artifact: &ArtifactRef,
) -> Result<Vec<u8>, String> {
    let artifacts = readers.artifacts.ok_or(
        "result_origin_requires_original_immutable_content: original Artifact reader missing",
    )?;
    let binding = owner
        .activity_binding()
        .ok_or("raw body has no original owner")?;
    let bytes = artifacts
        .read_blocking(artifact, &format!("session:{}", binding.session_id), None)
        .map_err(|error| error.to_string())?;
    let bytes = checked_original_bytes(bytes, &artifact.sha256, artifact.bytes)?;
    let call = owner.payload["tool_call_id"]
        .as_str()
        .ok_or("original raw call missing")?;
    let reference = owner.payload["full_output_ref"]
        .as_str()
        .ok_or("original raw reference missing")?;
    let ordinary = format!(
        "tool://tool-raw-{call}-{:016x}",
        model_protocol::fingerprint::stable_hash_bytes(&bytes)
    );
    let digest = artifact
        .sha256
        .strip_prefix("sha256:")
        .unwrap_or(&artifact.sha256);
    let staged = format!("tool://tool-raw-{call}-{digest}");
    if reference != ordinary && reference != staged {
        return Err(
            "original canonical raw identity disagrees with its producer body format".into(),
        );
    }
    Ok(bytes)
}

fn runtime_raw_artifact(owner: &DurableRuntimeEvent) -> Result<Option<ArtifactRef>, String> {
    let Some(value) = owner.payload.get("runtime_raw_content_v1") else {
        return Ok(None);
    };
    let raw: RuntimeRawContent = serde_json::from_value(value.clone())
        .map_err(|error| format!("original Runtime raw anchor is malformed:{error}"))?;
    let binding = owner
        .activity_binding()
        .ok_or("Runtime raw anchor lacks its original owner")?;
    let digest = raw.sha256.strip_prefix("sha256:").unwrap_or(&raw.sha256);
    let prefix = format!("tool://tool-raw-{}-", raw.tool_call_id);
    let suffix = raw
        .ref_id
        .strip_prefix(&prefix)
        .ok_or("Runtime raw anchor has a foreign call identity")?;
    if raw.version != 1
        || owner.payload["tool_call_id"] != raw.tool_call_id
        || owner.payload["full_output_ref"] != raw.ref_id
        || raw.visibility_scope != format!("session:{}", binding.session_id)
        || !raw.selector.starts_with("artifact://")
        || raw.media_type.is_empty()
        || digest.len() != 64
        || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !(suffix.len() == 16 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
            || suffix == digest)
    {
        return Err(
            "original Runtime raw anchor crosses its exact call/content/scope identity".into(),
        );
    }
    Ok(Some(ArtifactRef::durable(
        raw.selector,
        raw.sha256,
        raw.byte_count,
        raw.media_type,
        raw.visibility_scope,
    )))
}

fn canonical_original_bytes(owner: &DurableRuntimeEvent, bytes: &[u8]) -> Result<(), String> {
    let call = owner.payload["tool_call_id"]
        .as_str()
        .ok_or("original raw call missing")?;
    let reference = owner.payload["full_output_ref"]
        .as_str()
        .ok_or("original raw reference missing")?;
    let ordinary = format!(
        "tool://tool-raw-{call}-{:016x}",
        model_protocol::fingerprint::stable_hash_bytes(bytes)
    );
    let digest = crate::context_evidence::read_sha256(bytes);
    let staged = format!(
        "tool://tool-raw-{call}-{}",
        digest.trim_start_matches("sha256:")
    );
    if reference != ordinary && reference != staged {
        return Err(
            "original complete presentation does not authenticate the canonical raw projection"
                .into(),
        );
    }
    Ok(())
}

fn original_tool_body(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    readers: super::ResultContentReaders<'_>,
    owner: &DurableRuntimeEvent,
    task: Option<&serde_json::Value>,
    cuts: &mut std::collections::BTreeMap<String, u64>,
    limit: Option<u64>,
) -> Result<Option<OriginalToolBody>, String> {
    if let Some(artifact) = runtime_raw_artifact(owner)? {
        if limit.is_some_and(|limit| artifact.bytes > limit) {
            return Ok(None);
        }
        let bytes = read_original_session_body(readers, owner, &artifact)?;
        let origin = ResultOrigin::raw_owner(owner);
        return Ok(Some(OriginalToolBody {
            bytes,
            origin,
            content: Some(artifact),
        }));
    }
    let identity = original_output_identity(owner)?;
    if let Some(task) = task {
        match original_tool_primary(store, owner, task, cuts) {
            Ok(primary) if primary.payload["outcome_truncated"] != true => {
                if let Some(output) = primary.payload["outcome"]["output"].as_str() {
                    if limit.is_some_and(|limit| output.len() as u64 > limit) {
                        return Ok(None);
                    }
                    let bytes = output.as_bytes().to_vec();
                    let bytes = match identity {
                        Some((digest, expected)) => {
                            checked_original_bytes(bytes, digest, expected)?
                        }
                        None => bytes,
                    };
                    if canonical_original_bytes(owner, &bytes).is_ok() {
                        return Ok(Some(OriginalToolBody {
                            bytes,
                            origin: ResultOrigin::Strong,
                            content: None,
                        }));
                    }
                }
            }
            Ok(_) => {}
            Err(error)
                if error.starts_with("result_origin_requires_original_immutable_content:") => {}
            Err(error) => return Err(error),
        }
    }
    if let Some(output) = owner.payload["output_preview"]
        .as_str()
        .filter(|preview| owner.payload["output_byte_count"].as_u64() == Some(preview.len() as u64))
    {
        if limit.is_some_and(|limit| output.len() as u64 > limit) {
            return Ok(None);
        }
        let bytes = output.as_bytes().to_vec();
        let bytes = match identity {
            Some((digest, expected)) => checked_original_bytes(bytes, digest, expected)?,
            None => bytes,
        };
        if canonical_original_bytes(owner, &bytes).is_ok() {
            return Ok(Some(OriginalToolBody {
                bytes,
                origin: ResultOrigin::Strong,
                content: None,
            }));
        }
    }
    let artifacts = readers.artifacts.ok_or(
        "result_origin_requires_original_immutable_content: original Artifact reader missing",
    )?;
    if let Some((digest, expected)) = identity {
        if limit.is_some_and(|limit| expected > limit) {
            return Ok(None);
        }
        let binding = owner
            .activity_binding()
            .ok_or("raw body has no original owner")?;
        let bytes = artifacts
            .read_session_content_by_digest_blocking(&binding.session_id, digest, expected)
            .map_err(|error| error.to_string())?;
        let bytes = checked_original_bytes(bytes, digest, expected)?;
        if canonical_original_bytes(owner, &bytes).is_ok() {
            return Ok(Some(OriginalToolBody {
                bytes,
                origin: ResultOrigin::Strong,
                content: None,
            }));
        }
    }
    let (artifact, origin) = original_session_raw(readers, owner, task)?;
    if limit.is_some_and(|limit| artifact.bytes > limit) {
        return Ok(None);
    }
    let bytes = read_original_session_body(readers, owner, &artifact)?;
    Ok(Some(OriginalToolBody {
        bytes,
        origin,
        content: Some(artifact),
    }))
}

fn canonical_raw_result_content(
    body: &OriginalToolBody,
    selected: &ArtifactRef,
    reference: &str,
) -> Result<ArtifactRef, String> {
    let digest = crate::context_evidence::read_sha256(&body.bytes);
    let hint = selected.sha256.is_empty() && selected.bytes == 0 && selected.selector == reference;
    if !hint
        && (selected
            .sha256
            .strip_prefix("sha256:")
            .unwrap_or(&selected.sha256)
            != digest.strip_prefix("sha256:").unwrap_or(&digest)
            || selected.bytes != body.bytes.len() as u64)
    {
        return Err("original Tool result differs from its authenticated reviewer body".into());
    }
    let mut canonical = body.content.clone().unwrap_or_else(|| selected.clone());
    if let Some(original) = &body.content {
        if original
            .sha256
            .strip_prefix("sha256:")
            .unwrap_or(&original.sha256)
            != digest.strip_prefix("sha256:").unwrap_or(&digest)
            || original.bytes != body.bytes.len() as u64
            || original.visibility_scope != selected.visibility_scope
            || original.durability != selected.durability
        {
            return Err(
                "original raw material mapping conflicts with its authenticated body".into(),
            );
        }
    }
    canonical.sha256 = digest;
    canonical.bytes = body.bytes.len() as u64;
    Ok(canonical)
}

#[cfg(test)]
mod material_identity_621_tests {
    use super::*;

    #[test]
    fn r1_621_nonempty_tool_acquisition_uses_authenticated_raw_content_alias() {
        let bytes = b"original tool summary".to_vec();
        let original = ArtifactRef::durable(
            "artifact://raw-summary",
            crate::context_evidence::read_sha256(&bytes),
            bytes.len() as u64,
            "application/json",
            "session:material",
        );
        let body = OriginalToolBody {
            bytes,
            origin: ResultOrigin::Strong,
            content: Some(original.clone()),
        };
        let mut selected = original.clone();
        selected.selector = "tool://original-call".into();
        assert_eq!(
            canonical_raw_result_content(&body, &selected, "tool://original-call").unwrap(),
            original
        );
        selected.sha256 = crate::context_evidence::read_sha256(b"changed summary");
        assert!(canonical_raw_result_content(&body, &selected, "tool://original-call").is_err());
        selected.sha256 = body.content.as_ref().unwrap().sha256.clone();
        selected.bytes += 1;
        assert!(canonical_raw_result_content(&body, &selected, "tool://original-call").is_err());
    }

    #[test]
    fn r1_621_same_producer_summary_and_file_are_distinct_materials() {
        let summary = b"summary".to_vec();
        let file = b"actual produced file contents".to_vec();
        let summary_content = ArtifactRef::durable(
            "artifact://summary",
            crate::context_evidence::read_sha256(&summary),
            summary.len() as u64,
            "application/json",
            "session:material",
        );
        let file_content = ArtifactRef::durable(
            "artifact://file",
            crate::context_evidence::read_sha256(&file),
            file.len() as u64,
            "text/plain",
            "session:material",
        );
        let first = OriginalToolBody {
            bytes: summary,
            origin: ResultOrigin::Strong,
            content: Some(summary_content.clone()),
        };
        let second = OriginalToolBody {
            bytes: file,
            origin: ResultOrigin::Strong,
            content: Some(file_content.clone()),
        };
        assert_ne!(
            canonical_raw_result_content(&first, &summary_content, "tool://same-producer").unwrap(),
            canonical_raw_result_content(&second, &file_content, "tool://same-producer").unwrap()
        );
        assert!(
            canonical_raw_result_content(&first, &file_content, "tool://same-producer").is_err()
        );
    }

    #[test]
    fn r1_621_raw_alias_rejects_foreign_scope_and_conflicting_original_mapping() {
        let bytes = b"original".to_vec();
        let original = ArtifactRef::durable(
            "artifact://original",
            crate::context_evidence::read_sha256(&bytes),
            bytes.len() as u64,
            "text/plain",
            "session:material",
        );
        let mut body = OriginalToolBody {
            bytes,
            origin: ResultOrigin::Strong,
            content: Some(original.clone()),
        };
        let mut foreign = original.clone();
        foreign.visibility_scope = "session:foreign".into();
        assert!(canonical_raw_result_content(&body, &foreign, "tool://original").is_err());
        body.content.as_mut().unwrap().bytes += 1;
        assert!(canonical_raw_result_content(&body, &original, "tool://original").is_err());
    }
}

fn publication_output(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    readers: super::ResultContentReaders<'_>,
    owner: &DurableRuntimeEvent,
    task: Option<&serde_json::Value>,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<Option<(serde_json::Value, ResultOrigin)>, String> {
    struct UniqueReply;
    impl<'de> serde::de::Visitor<'de> for UniqueReply {
        type Value = serde_json::Value;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a publication reply with unique keys")
        }
        fn visit_map<Map>(self, mut map: Map) -> Result<Self::Value, Map::Error>
        where
            Map: serde::de::MapAccess<'de>,
        {
            let mut fields = serde_json::Map::new();
            while let Some((key, value)) = map.next_entry::<String, serde_json::Value>()? {
                if fields.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate publication reply key"));
                }
            }
            Ok(serde_json::Value::Object(fields))
        }
    }
    let Some(body) = original_tool_body(store, readers, owner, task, cuts, Some(256 * 1024))?
    else {
        return Ok(None);
    };
    let mut decoder = serde_json::Deserializer::from_slice(&body.bytes);
    let value = serde::de::Deserializer::deserialize_map(&mut decoder, UniqueReply)
        .map_err(|error| format!("original publication reply is not unambiguous JSON:{error}"))?;
    decoder.end().map_err(|error| error.to_string())?;
    Ok(Some((value, body.origin)))
}
fn publication_sources(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    program: &AgenticProgramProjection,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<(Vec<AgenticProgramProjection>, Vec<DurableRuntimeEvent>), String> {
    let mut programs = vec![program.clone()];
    let mut seen = BTreeSet::from([program.program_id.clone()]);
    while let Some(binding) = programs
        .last()
        .and_then(|origin| origin.continuation.clone())
    {
        if !seen.insert(binding.source_program_id.clone()) {
            return Err("result continuation lineage is cyclic".into());
        }
        let source = format!("agentic-program:{}", binding.source_program_id);
        pin_result_source(store, &source, cuts)?;
        let prior = crate::AgentActionService::new(std::sync::Arc::clone(store))
            .project(&binding.source_program_id)
            .map_err(|error| error.to_string())?;
        if prior.session_id != program.session_id
            || prior.root_execution_id.as_deref() != Some(binding.source_root_id.as_str())
            || prior.objective_id != binding.source_objective_id
            || cuts.get(&source) != Some(&prior.revision)
        {
            return Err("result continuation crosses authenticated Program provenance".into());
        }
        programs.push(prior);
    }
    let mut publications = Vec::new();
    for origin in &programs {
        let source = format!("agentic-program:{}", origin.program_id);
        let events = result_source_events(store, &source, cuts)?;
        if cuts.get(&source) != Some(&origin.revision) {
            return Err(format!(
                "result Program changed during original producer resolution: source {source} is at \
                 revision {} but the reviewed Program projection is at {}",
                cuts.get(&source).copied().unwrap_or_default(),
                origin.revision
            ));
        }
        publications.extend(events);
    }
    Ok((programs, publications))
}

fn original_artifact_publication(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    program: &AgenticProgramProjection,
    programs: &[AgenticProgramProjection],
    publications: &[DurableRuntimeEvent],
    artifact: &crate::agentic::program::AgenticArtifactProjection,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<
    (
        AgentActorBinding,
        DurableRuntimeEvent,
        Option<AgentTaskPacket>,
    ),
    String,
> {
    use harness_contract::agent_action::{AgentAction, AgentActionEnvelope, AgentActorKind};
    let mut original = None;
    let mut original_publication = None;
    for event in publications {
        if event.kind != "agentic.action_applied" {
            continue;
        }
        let envelope: AgentActionEnvelope =
            serde_json::from_value(event.payload["envelope"].clone())
                .map_err(|error| error.to_string())?;
        let matches = match &envelope.action {
            AgentAction::ArtifactCommit(input) => {
                event.payload["entity_ref"] == artifact.artifact_ref
                    && input.content_ref == artifact.content_ref
            }
            AgentAction::TaskSubmit(input) => {
                use sha2::{Digest, Sha256};
                let digest = format!(
                    "{:x}",
                    Sha256::digest(
                        format!(
                            "{}|{}|{}|{}",
                            envelope.actor.program_id,
                            envelope.actor.actor_id,
                            envelope.action_id,
                            envelope.action.kind()
                        )
                        .as_bytes()
                    )
                );
                artifact.artifact_ref == format!("artifact:{}", &digest[..24])
                    && input.artifact_refs.is_empty()
                    && input
                        .deliverable
                        .as_ref()
                        .and_then(|deliverable| deliverable.resolved_content_ref.as_deref())
                        == Some(artifact.content_ref.as_str())
                    && artifact.relates_to.contains(&input.task_ref)
                    && event.payload["entity_ref"] == input.task_ref
            }
            _ => false,
        };
        if !matches {
            continue;
        }
        let owner = &envelope.actor;
        if event.schema_version != 1
            || event.scope != crate::RuntimeEventScope::Program
            || event.actor.as_deref() != Some(owner.actor_id.as_str())
            || owner.session_id != program.session_id
            || owner.actor_id != artifact.committed_by
            || !programs.iter().any(|origin| {
                origin.program_id == owner.program_id
                    && origin.objective_id == owner.objective_id
                    && origin.turn_id == owner.turn_id
                    && origin.root_execution_id == owner.root_execution_id
                    && event.stream_id == format!("agentic-program:{}", origin.program_id)
            })
            || original.replace(owner.clone()).is_some()
        {
            return Err("result original publication actor is foreign or ambiguous".into());
        }
        original_publication = Some(event.clone());
    }
    let owner = original.ok_or("result Artifact has no durable original publication actor")?;
    let packet = match owner.execution_id.as_deref() {
        Some(execution) => Some(result_packet(store, execution, program, cuts)?),
        None if owner.kind == AgentActorKind::Root => None,
        None => return Err("result non-Root publication has no physical producer".into()),
    };
    if let Some(packet) = &packet {
        let scope = packet
            .agentic_binding
            .as_ref()
            .ok_or("result producer packet has no Program binding")?;
        if scope.program_id != owner.program_id
            || Some(scope.agent_id.as_str()) != owner.agent_id.as_deref()
            || owner.actor_id != scope.agent_id
            || artifact
                .claim_execution_id
                .as_deref()
                .is_some_and(|execution| {
                    !super::packet_identity_matches(packet.graph_id(), packet.run_id(), execution)
                })
            || artifact
                .claim_generation
                .is_some_and(|generation| generation != u64::from(packet.attempt))
        {
            return Err(
                "result publication does not match its original physical packet/claim".into(),
            );
        }
    } else if artifact.claim_execution_id.is_some() || artifact.claim_generation.is_some() {
        return Err("Root result publication cannot borrow an Agent claim".into());
    }

    Ok((
        owner,
        original_publication.ok_or("result original publication boundary missing")?,
        packet,
    ))
}

pub(super) fn authenticate_publication_task_link(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    program: &crate::AgenticProgramProjection,
    artifact: &crate::agentic::program::AgenticArtifactProjection,
    task_ref: &str,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<(), String> {
    use crate::agentic::program::AgenticTaskStatus;
    use harness_contract::agent::AgenticExecutionFocus;
    use harness_contract::agent_action::{AgentActionEnvelope, AgentActorKind};

    let (programs, publications) = publication_sources(store, program, cuts)?;
    let (owner, publication, packet) =
        original_artifact_publication(store, program, &programs, &publications, artifact, cuts)?;
    let packet = packet.ok_or("publication Task provenance requires an original Agent packet")?;
    let binding = packet
        .agentic_binding
        .as_ref()
        .ok_or("publication Task provenance has no original Program binding")?;
    let focus = match &binding.focus {
        AgenticExecutionFocus::TaskExecute { task_ref }
        | AgenticExecutionFocus::TaskReview { task_ref } => task_ref.as_str(),
        _ => return Err("publication Task provenance has no original Task focus".into()),
    };
    if !matches!(owner.kind, AgentActorKind::Agent | AgentActorKind::TeamLead)
        || owner.agent_id.is_none()
        || owner.root_execution_id.is_none()
        || focus != task_ref
        || !artifact
            .relates_to
            .iter()
            .any(|reference| reference == task_ref)
    {
        return Err("publication Task provenance has a foreign original Task/actor".into());
    }
    let opening = publications
        .iter()
        .find(|event| event.stream_id == publication.stream_id && event.sequence == 1)
        .ok_or("publication Task provenance has no original Program opening")?;
    if opening.kind != "agentic.program_opened"
        || opening.scope != crate::RuntimeEventScope::Program
        || opening.schema_version != 1
        || opening.payload["objective_id"].as_str() != Some(owner.objective_id.as_str())
        || opening.payload["session_id"].as_str() != Some(owner.session_id.as_str())
        || opening.payload["turn_id"].as_str() != Some(owner.turn_id.as_str())
        || opening.payload["root_execution_id"].as_str() != owner.root_execution_id.as_deref()
    {
        return Err("publication Task provenance has a foreign Program opening".into());
    }
    let mut historical = if let Some(seed) = opening.payload.get("continuation_seed") {
        let seed: AgenticProgramProjection =
            serde_json::from_value(seed.clone()).map_err(|error| error.to_string())?;
        if seed.program_id != owner.program_id
            || seed.objective_id != owner.objective_id
            || seed.session_id != owner.session_id
            || seed.turn_id != owner.turn_id
            || seed.root_execution_id != owner.root_execution_id
            || seed.continuation.is_none()
        {
            return Err("publication Task provenance has a foreign continuation seed".into());
        }
        seed
    } else {
        AgenticProgramProjection::empty(&owner.program_id, &owner.objective_id)
    };
    historical.session_id = owner.session_id.clone();
    historical.turn_id = owner.turn_id.clone();
    historical.root_execution_id = owner.root_execution_id.clone();
    historical.revision = opening.sequence;
    for event in publications.iter().filter(|event| {
        event.stream_id == publication.stream_id
            && event.sequence > opening.sequence
            && event.sequence < publication.sequence
    }) {
        if event.schema_version != 1
            || event.scope != crate::RuntimeEventScope::Program
            || event.commit_cursor > publication.commit_cursor
        {
            return Err("publication Task provenance has an invalid historical event".into());
        }
        if event.kind == "agentic.action_applied" {
            let envelope: AgentActionEnvelope =
                serde_json::from_value(event.payload["envelope"].clone())
                    .map_err(|error| error.to_string())?;
            if envelope.actor.program_id != owner.program_id
                || envelope.actor.objective_id != owner.objective_id
                || envelope.actor.session_id != owner.session_id
                || envelope.actor.turn_id != owner.turn_id
                || envelope.actor.root_execution_id != owner.root_execution_id
                || event.actor.as_deref() != Some(envelope.actor.actor_id.as_str())
            {
                return Err("publication Task provenance has a foreign historical action".into());
            }
            historical.apply(
                &envelope,
                event.payload["entity_ref"].as_str(),
                event.sequence,
                event.created_at_ms,
            );
        } else if event.kind == "agentic.task_lease_renewed" {
            if let Some(task) = event.payload["task_ref"]
                .as_str()
                .and_then(|reference| historical.tasks.get_mut(reference))
            {
                if task.status == AgenticTaskStatus::Claimed
                    && task.claimant.as_deref() == event.payload["agent_id"].as_str()
                    && task.claim_execution_id.as_deref() == event.payload["execution_id"].as_str()
                {
                    task.lease_expires_at_ms = Some(
                        event
                            .created_at_ms
                            .saturating_add(crate::agentic::work_market::claim_lease_ms()),
                    );
                }
            }
        }
        historical.revision = event.sequence;
    }
    let task = historical
        .tasks
        .get(task_ref)
        .ok_or("publication Task provenance has no original Task")?;
    if task.status != AgenticTaskStatus::Claimed
        || task.task_id != task_ref
        || binding.task_team_id != task.team_id
        || task.claimant.as_deref() != owner.agent_id.as_deref()
        || task.claim_execution_id != owner.execution_id
        || task.claim_generation
            != artifact
                .claim_generation
                .ok_or("publication Task provenance lacks its original claim generation")?
        || task.claim_execution_id != artifact.claim_execution_id
        || task
            .claimed_at_ms
            .is_none_or(|time| time > publication.created_at_ms)
        || task
            .lease_expires_at_ms
            .is_none_or(|time| time <= publication.created_at_ms)
    {
        return Err("publication Task provenance does not match its original active claim".into());
    }
    let envelope: AgentActionEnvelope =
        serde_json::from_value(publication.payload["envelope"].clone())
            .map_err(|error| error.to_string())?;
    historical.apply(
        &envelope,
        publication.payload["entity_ref"].as_str(),
        publication.sequence,
        publication.created_at_ms,
    );
    if historical.artifacts.get(&artifact.artifact_ref) != Some(artifact) {
        return Err(
            "publication Task provenance disagrees with its original artifact postimage".into(),
        );
    }
    for source in cuts.keys().cloned().collect::<Vec<_>>() {
        pin_result_source(store, &source, cuts)?;
    }
    Ok(())
}

pub(super) fn resolve_result_metadata(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    program: &AgenticProgramProjection,
    reference: &str,
    content: &ArtifactRef,
    input_cuts: Option<&std::collections::BTreeMap<String, u64>>,
    readers: super::ResultContentReaders<'_>,
) -> Result<ResolvedReviewResult, String> {
    resolve_result_metadata_for_phase(
        store, program, reference, content, input_cuts, readers, false,
    )
}

pub(super) fn resolve_result_metadata_for_phase(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    program: &AgenticProgramProjection,
    reference: &str,
    content: &ArtifactRef,
    input_cuts: Option<&std::collections::BTreeMap<String, u64>>,
    readers: super::ResultContentReaders<'_>,
    allow_history_growth: bool,
) -> Result<ResolvedReviewResult, String> {
    let identity = super::identity::auxiliary_identity(program, reference)?;
    let reference = identity.reference();
    if reference.starts_with("artifact:")
        && !reference.starts_with("artifact://")
        && !matches!(identity, super::identity::MaterialIdentity::Publication(_))
    {
        return Err("result exact publication binding missing".into());
    }
    let authenticated_content = match readers.artifacts {
        Some(artifacts) if content.selector.starts_with("artifact://") => {
            let original = artifacts
                .resolve(&content.selector)
                .map_err(|error| error.to_string())?;
            if !(content.sha256.is_empty() && content.bytes == 0)
                && (original.sha256 != content.sha256 || original.bytes != content.bytes)
            {
                return Err(
                    "result content identity disagrees with its original Artifact metadata".into(),
                );
            }
            original
        }
        _ => content.clone(),
    };
    let content = &authenticated_content;
    if let super::identity::MaterialIdentity::Content(selector) = &identity {
        if !super::same_reference_spelling(selector, &content.selector) {
            return Err(
                "result content selector conflicts with its explicit content identity".into(),
            );
        }
    }
    let mut cuts = std::collections::BTreeMap::new();
    let (programs, publications) = publication_sources(store, program, &mut cuts)?;
    let mut resolved = ResolvedReviewResult {
        origin: ResultOrigin::Strong,
        result_sources: Vec::new(),
        reference: reference.to_string(),
        content: content.clone(),
        kind: if content.media_type.contains("json") {
            GoalResultKind::StructuredData
        } else {
            GoalResultKind::Content
        },
        artifact_kinds: BTreeSet::new(),
        producers: Vec::new(),
        effect_cursor: None,
        effect_sources: Vec::new(),
        effect_scopes: Vec::new(),
        effect_targets: Vec::new(),
        unscoped_effect: false,
        primary_inputs: Vec::new(),
        input_boundaries: Vec::new(),
        process_runs: Vec::new(),
    };
    let mut publication_cursor = 0;
    if reference.starts_with("approval:v1:") {
        let source = format!("approval:{reference}");
        pin_result_source(store, &source, &mut cuts)?;
        let approvals = crate::ApprovalQueue::new(std::sync::Arc::clone(store));
        let (approval, revision) =
            approvals.decision_result_snapshot(&program.session_id, reference)?;
        if cuts.get(&source) != Some(&revision.expected_revision) {
            return Err("external decision changed during result metadata resolution".into());
        }
        let (mut graph_id, node_id) =
            crate::execution_core::graph::executors::parse_graph_approval_id(reference)
                .ok_or("external result has no canonical approval identity")?;
        let roots = programs
            .iter()
            .filter_map(|origin| origin.root_execution_id.as_deref())
            .collect::<BTreeSet<_>>();
        let mut visited = BTreeSet::new();
        loop {
            if !visited.insert(graph_id.clone()) {
                return Err("cyclic external decision graph lineage".into());
            }
            pin_result_source(store, &graph_id, &mut cuts)?;
            let graph = crate::ExecutionGraphStateStore::new(std::sync::Arc::clone(store))
                .load(&graph_id)
                .map_err(|error| error.to_string())?;
            if cuts.get(&graph_id) != Some(&graph.revision)
                || graph
                    .lineage
                    .as_ref()
                    .is_none_or(|lineage| lineage.session_id != program.session_id)
                || (visited.len() == 1
                    && !graph.nodes.iter().any(|node| {
                        node.id == node_id
                            && node.kind
                                == harness_contract::execution_graph::ExecutionNodeKind::Approval
                    }))
            {
                return Err("external decision graph has a foreign original source".into());
            }
            if roots.contains(graph_id.as_str()) {
                break;
            }
            graph_id = graph
                .parent_execution
                .ok_or("external decision belongs to another Objective")?
                .execution_id;
        }
        let decision_body = serde_json::to_vec(&serde_json::json!({
            "kind":"external_decision", "approval":approval,
            "source_revision":revision.expected_revision, "scope":"approval_decision_only"
        }))
        .map_err(|error| error.to_string())?;
        let decision_digest = crate::context_evidence::read_sha256(&decision_body);
        if content.sha256.is_empty() && content.bytes == 0 && content.selector == reference {
            resolved.content.sha256 = decision_digest.clone();
            resolved.content.bytes = decision_body.len() as u64;
        } else if content.sha256.trim_start_matches("sha256:")
            != decision_digest.trim_start_matches("sha256:")
            || content.bytes != decision_body.len() as u64
        {
            return Err(
                "external decision content predates its current authenticated decision source"
                    .into(),
            );
        }
        let decision = approval
            .decision
            .as_ref()
            .ok_or("external decision is missing")?;
        resolved.kind = GoalResultKind::ExternalDecision;
        resolved.artifact_kinds.insert("external_decision".into());
        resolved.producers.push(ResultProducer {
            actor: format!(
                "external-decision:{:?}:{}",
                decision.actor.kind, decision.actor.actor_id
            ),
            execution: format!(
                "approval-decision:{}:{}",
                approval.approval_id, revision.expected_revision
            ),
            packet: None,
            tool: None,
        });
        resolved.result_sources.push(revision);
    } else {
        if let super::identity::MaterialIdentity::Publication(publication) = &identity {
            let artifact = &program.artifacts[publication];
            if !super::same_reference_spelling(&artifact.content_ref, &content.selector) {
                return Err(
                    "result content does not match its original Program publication".into(),
                );
            }
            let (owner, original_publication, packet) = original_artifact_publication(
                store,
                program,
                &programs,
                &publications,
                artifact,
                &mut cuts,
            )?;
            publication_cursor = publication_cursor.max(original_publication.commit_cursor);
            let root = owner
                .root_execution_id
                .as_deref()
                .ok_or("result publication has no bound physical root")?;
            let execution = packet.as_ref().map_or_else(
                || format!("root-execution:{}", root),
                |packet| format!("agent-run:{}", packet.run_id()),
            );
            resolved.input_boundaries.push(result_input_boundary(
                &original_publication,
                execution.clone(),
                packet.clone(),
                None,
            )?);
            resolved.kind = GoalResultKind::Artifact;
            resolved.artifact_kinds.insert(artifact.kind.clone());
            resolved.producers.push(ResultProducer {
                actor: owner.actor_id,
                execution,
                packet,
                tool: None,
            });
        }
        let session_events =
            result_source_events(store, &format!("session:{}", program.session_id), &mut cuts)?;
        let fresh_head = if reference.starts_with("tool://") || readers.artifacts.is_none() {
            None
        } else {
            crate::content_publication::fresh_object_precreation_head(
                &session_events,
                &program.session_id,
                content,
            )?
        };
        let mut lifecycle_effects = Vec::new();
        let mut immutable_gaps = BTreeSet::new();
        for event in &session_events {
            if event.kind != "tool.invocation.completed"
                || event.actor.as_deref() != Some("conversation_runtime")
                || event.scope != crate::RuntimeEventScope::Tool
                || event.status.as_deref() != Some("completed")
                || event.payload["status"] != "completed"
            {
                continue;
            }
            if event.schema_version != 1 {
                return Err("result metadata has an unsupported original tool event schema".into());
            }
            let Some(binding) = event.activity_binding() else {
                continue;
            };
            if binding.session_id != program.session_id {
                continue;
            }
            let task = result_tool_plan(&session_events, event)?;
            let primary = task.map(|task| original_tool_primary(store, event, task, &mut cuts)).transpose()?;
            let producer_parent = primary.as_ref()
                .filter(|primary| !primary.payload["replay"]["parent_execution"].is_null())
                .map(|primary| &primary.payload["replay"]["parent_execution"]);
            let delegated_execution = if let Some(parent) = producer_parent {
                let execution = parent["execution_id"].as_str()
                    .ok_or("result callback original graph missing")?;
                let node = parent["node_id"].as_str()
                    .ok_or("result callback original node missing")?;
                pin_result_source(store, execution, &mut cuts)?;
                let graph = crate::ExecutionGraphStateStore::new(std::sync::Arc::clone(store))
                    .load(execution).map_err(|error| error.to_string())?;
                let original = graph.nodes.iter().find(|candidate| candidate.id == node)
                    .ok_or("result callback original producer node missing")?;
                match original.kind {
                    harness_contract::execution_graph::ExecutionNodeKind::AgentTask => Some(execution),
                    harness_contract::execution_graph::ExecutionNodeKind::InlineModel
                    | harness_contract::execution_graph::ExecutionNodeKind::ToolBatch => None,
                    kind => return Err(format!("result callback unsupported original producer: graph={execution} node={node} kind={kind:?}")),
                }
            } else {
                None
            };
            let delegated_packet = if let Some(execution) = delegated_execution {
                let parent = primary
                    .as_ref()
                    .ok_or("result callback original primary missing")?;
                let packet = result_packet(store, execution, program, &mut cuts)?;
                let Some(scope) = packet.agentic_binding.as_ref() else {
                    continue;
                };
                if !programs.iter().any(|origin| {
                    origin.program_id == scope.program_id && origin.turn_id == binding.turn_id
                }) {
                    continue;
                }
                if Some(packet.run_id()) != binding.agent_run_id.as_deref()
                    || parent.payload["replay"]["parent_execution"]["node_id"] != packet.node_id()
                    || binding.agent_instance_id.as_deref() != Some(packet.agent_id())
                    || packet.assignment.execution_identity.turn_id()
                        != Some(binding.turn_id.as_str())
                    || !super::binding_is_owned_by_root(&binding, Some(&binding.root_execution_id))
                {
                    return Err(
                        "result callback crosses its original delegated physical owner".into(),
                    );
                }
                authenticate_callback_parent_chain(
                    store,
                    packet.graph_id(),
                    &binding,
                    &programs,
                    &mut cuts,
                )?;
                Some(packet)
            } else {
                if primary.is_none() && binding.agent_run_id.is_some() {
                    return Err(
                        "result callback has no authenticated original primary owner".into(),
                    );
                }
                if !programs.iter().any(|origin| {
                    origin.turn_id == binding.turn_id
                        && origin.root_execution_id.as_deref()
                            == Some(binding.root_execution_id.as_str())
                }) || !super::binding_is_owned_by_root(
                    &binding,
                    Some(&binding.root_execution_id),
                ) {
                    continue;
                }
                None
            };
            let effect = task
                .map(|task| {
                    serde_json::from_value::<ToolEffectDescriptor>(task["effect"].clone())
                        .map_err(|_| "result source has a malformed original descriptor")
                })
                .transpose()?;
            let is_effect = !event.payload["tool_name"]
                .as_str()
                .is_some_and(super::policy::goal_metadata_tool)
                && !effect
                    .as_ref()
                    .is_some_and(|effect| effect.effect_kind == ToolEffectKind::Read);
            if is_effect {
                lifecycle_effects.push((
                    delegated_packet.as_ref().map_or_else(
                        || format!("root-execution:{}", binding.root_execution_id),
                        |packet| format!("agent-run:{}", packet.run_id()),
                    ),
                    event,
                    event.payload["tool_name"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    effect.clone(),
                ));
            }
            if event.payload["is_error"] == true {
                continue;
            }
            let raw_ref = event.payload["full_output_ref"].as_str();
            let mut match_origin = ResultOrigin::Strong;
            let mut matches = reference.starts_with("tool://") && raw_ref == Some(reference);
            if matches {
                let body = original_tool_body(store, readers, event, task, &mut cuts, None)?
                    .ok_or("original Tool result body unexpectedly exceeded its capability")?;
                resolved.content = canonical_raw_result_content(&body, content, reference)?;
                match_origin = body.origin;
            }
            let mut publication = false;
            let exclude_direct_identity = event.stream_id
                == format!("session:{}", program.session_id)
                && fresh_head.is_some_and(|head| event.sequence <= head);
            if exclude_direct_identity {
                if event.payload["runtime_raw_content_v1"]["selector"] == content.selector
                    || raw_ref == Some(content.selector.as_str())
                {
                    return Err("fresh object conflicts with an earlier direct raw selector".into());
                }
                if observed_raw_selector_conflict(readers, event, &content.selector) {
                    return Err("fresh object conflicts with an earlier exact raw alias".into());
                }
                if event.payload["tool_name"] == "artifact_publish" {
                    let primary = task
                        .and_then(|task| original_tool_primary(store, event, task, &mut cuts).ok());
                    let outputs = [
                        event.payload["output_preview"].as_str(),
                        primary
                            .as_ref()
                            .and_then(|primary| primary.payload["outcome"]["output"].as_str()),
                    ];
                    for output in outputs.into_iter().flatten() {
                        if serde_json::from_str::<serde_json::Value>(output)
                            .ok()
                            .is_some_and(|value| value["content_ref"] == content.selector)
                        {
                            return Err(
                                "fresh object conflicts with an earlier publication selector"
                                    .into(),
                            );
                        }
                    }
                    if let Ok(Some((value, _))) =
                        publication_output(store, readers, event, task, &mut cuts)
                    {
                        if value["content_ref"] == content.selector {
                            return Err(
                                "fresh object conflicts with an authenticated earlier publication"
                                    .into(),
                            );
                        }
                    }
                }
            }
            if !matches
                && !reference.starts_with("tool://")
                && raw_ref.is_some()
                && !exclude_direct_identity
            {
                if !matches && event.payload["tool_name"] == "artifact_publish" {
                    match publication_output(store, readers, event, task, &mut cuts) {
                        Ok(Some((value, origin))) => {
                            let is_hint = content.sha256.is_empty()
                                && content.bytes == 0
                                && content.selector == reference;
                            publication = value["status"] == "published"
                                && value["content_ref"] == content.selector
                                && (is_hint
                                    || value["sha256"] == content.sha256
                                        && value["bytes"].as_u64() == Some(content.bytes));
                            if publication && is_hint {
                                let digest = value["sha256"]
                                    .as_str()
                                    .ok_or("publication result digest missing")?;
                                let normalized = digest.strip_prefix("sha256:").unwrap_or(digest);
                                if normalized.len() != 64
                                    || !normalized.bytes().all(|byte| byte.is_ascii_hexdigit())
                                {
                                    return Err(
                                        "publication original result digest is invalid".into()
                                    );
                                }
                                resolved.content.sha256 = digest.to_string();
                                resolved.content.bytes = value["bytes"]
                                    .as_u64()
                                    .ok_or("publication result extent missing")?;
                            }
                            matches = publication;
                            if matches {
                                match_origin = origin.clone();
                            }
                            if !matches && origin.require_strong().is_err() {
                                immutable_gaps.insert(format!(
                                    "{}#{}:weak original publication cannot exclude influence",
                                    event.stream_id, event.sequence
                                ));
                            }
                        }
                        Ok(None) => {
                            immutable_gaps.insert(format!(
                                "{}#{}:original publication exceeds its complete-body capability",
                                event.stream_id, event.sequence
                            ));
                        }
                        Err(error) => {
                            immutable_gaps.insert(format!(
                                "{}#{}:{}:{error}",
                                event.stream_id, event.sequence, event.event_id
                            ));
                        }
                    }
                }
                let anchored_raw = runtime_raw_artifact(event)?;
                if !matches {
                    if let Some(raw) = anchored_raw
                        .as_ref()
                        .filter(|raw| raw.selector == content.selector)
                    {
                        read_original_session_body(readers, event, raw)?;
                        if raw.sha256 != content.sha256 || raw.bytes != content.bytes {
                            return Err(
                                "Runtime raw selector conflicts with the result content".into()
                            );
                        }
                        matches = true;
                        match_origin = ResultOrigin::raw_owner(event);
                    }
                }
                let identity = if anchored_raw.is_none() {
                    original_output_identity(event)?
                } else {
                    None
                };
                let possible_alias = match identity {
                    Some((digest, bytes)) => {
                        digest.strip_prefix("sha256:").unwrap_or(digest)
                            == content
                                .sha256
                                .strip_prefix("sha256:")
                                .unwrap_or(&content.sha256)
                            && bytes == content.bytes
                    }
                    None => {
                        !content.sha256.is_empty()
                            && (event.payload["tool_name"] == "read_file"
                                || event.payload["output_byte_count"]
                                    .as_u64()
                                    .is_none_or(|bytes| bytes == content.bytes))
                    }
                };
                if !matches && possible_alias && anchored_raw.is_none() {
                    match original_session_raw(readers, event, task) {
                        Ok((raw, _)) if raw.selector == content.selector => {
                            let bytes = read_original_session_body(readers, event, &raw)?;
                            let body = original_tool_body(
                                store, readers, event, task, &mut cuts, None,
                            )?
                            .ok_or("original raw alias unexpectedly exceeded its capability")?;
                            if body.bytes != bytes {
                                return Err(
                                    "original raw alias conflicts with its complete original body"
                                        .into(),
                                );
                            }
                            if raw.sha256 != content.sha256 || raw.bytes != content.bytes {
                                return Err(
                                    "raw selector identity disagrees with the actual result body"
                                        .into(),
                                );
                            }
                            matches = true;
                            match_origin = body.origin;
                        }
                        Ok((_, origin)) if origin.require_strong().is_err() => {
                            immutable_gaps.insert(format!(
                                "{}#{}:weak raw alias cannot exclude original influence",
                                event.stream_id, event.sequence
                            ));
                        }
                        Ok(_) => {}
                        Err(error)
                            if error.contains("exact original Session raw mapping missing:") =>
                        {
                            immutable_gaps
                                .insert(format!("{}#{}:{error}", event.stream_id, event.sequence));
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            if !matches {
                continue;
            }
            if delegated_packet.is_some() {
                original_tool_primary(
                    store,
                    event,
                    task.ok_or("delegated result callback lacks its original governed plan")?,
                    &mut cuts,
                )?;
            }
            resolved.origin = resolved.origin.merge(match_origin);
            publication_cursor = publication_cursor.max(event.commit_cursor);
            let (execution, packet, actor) = if let Some(packet) = delegated_packet.clone() {
                let run = packet.run_id().to_string();
                let graph = event
                    .refs
                    .iter()
                    .find(|reference| {
                        matches!(reference.kind.as_str(), "execution_graph" | "execution")
                            && (super::packet_identity_matches(
                                packet.graph_id(),
                                packet.run_id(),
                                &reference.id,
                            ) || reference.id == binding.root_execution_id)
                    })
                    .ok_or("result Tool activity has no physical execution reference")?;
                if Some(packet.run_id()) != binding.agent_run_id.as_deref()
                    || !(super::packet_identity_matches(
                        packet.graph_id(),
                        packet.run_id(),
                        &graph.id,
                    ) || graph.id == binding.root_execution_id)
                {
                    return Err("result Tool activity does not match its physical producer".into());
                }
                let actor = packet
                    .agentic_binding
                    .as_ref()
                    .map(|scope| scope.agent_id.clone())
                    .unwrap_or_else(|| run.clone());
                (format!("agent-run:{run}"), Some(packet), actor)
            } else {
                (
                    format!("root-execution:{}", binding.root_execution_id),
                    None,
                    format!("root:{}", program.session_id),
                )
            };
            let original_step = task
                .map(|task| {
                    serde_json::from_value::<crate::step_snapshot::StepCallBinding>(
                        task["step_binding"].clone(),
                    )
                    .map_err(|_| "result producer has no original input Step")
                })
                .transpose()?;
            let input_event = if is_effect && !publication {
                let step = original_step
                    .as_ref()
                    .ok_or("effect input boundary lacks original Step")?;
                let key = step.effect_idempotency_key();
                store
                    .event_by_idempotency_key(
                        &format!("execution-effect:{key}"),
                        &format!("{key}:intent"),
                    )
                    .map_err(|error| error.to_string())?
                    .ok_or("effect original input intent missing")?
            } else {
                event.clone()
            };
            resolved.input_boundaries.push(result_input_boundary(
                &input_event,
                execution.clone(),
                packet.clone(),
                original_step,
            )?);
            let tool = effect.as_ref().map(|effect| {
                (
                    event.payload["tool_name"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    effect.effect_kind,
                )
            });
            if !resolved.producers.iter().any(|producer| {
                producer.actor == actor && producer.execution == execution && producer.tool == tool
            }) {
                resolved.producers.push(ResultProducer {
                    actor,
                    execution,
                    packet,
                    tool,
                });
            }
            if is_effect && !publication {
                let task = task.ok_or("result Tool effect has no original governed plan")?;
                let primary = original_tool_primary(store, event, task, &mut cuts)?;
                let descriptor = effect
                    .as_ref()
                    .ok_or("result effect has no original descriptor")?;
                let scope = require_original_effect_scope(
                    &primary.payload["resolved_effect_scope"],
                    descriptor,
                )?;
                project_result_effect(
                    store,
                    &mut resolved,
                    descriptor,
                    &scope,
                    &primary,
                    &mut cuts,
                )?;
            }
        }
        if !immutable_gaps.is_empty() {
            return Err(format!(
                "result_origin_requires_original_immutable_content:{reference}; original Session access/publication body reader required for {immutable_gaps:?}"
            ));
        }
        for (execution, event, tool, effect) in lifecycle_effects {
            let Some(actor) = resolved
                .producers
                .iter()
                .find(|producer| producer.execution == execution)
                .map(|producer| producer.actor.clone())
            else {
                continue;
            };
            let task = result_tool_plan(&session_events, event)?
                .ok_or("result producer effect has no original governed plan")?;
            let primary = original_tool_primary(store, event, task, &mut cuts)?;
            let descriptor = effect
                .as_ref()
                .ok_or("producer effect has no original descriptor")?;
            let scope = require_original_effect_scope(
                &primary.payload["resolved_effect_scope"],
                descriptor,
            )?;
            project_result_effect(
                store,
                &mut resolved,
                descriptor,
                &scope,
                &primary,
                &mut cuts,
            )?;
            let key = primary.payload["idempotency_key"]
                .as_str()
                .ok_or("producer effect identity missing")?;
            let intent = store
                .event_by_idempotency_key(&primary.stream_id, &format!("{key}:intent"))
                .map_err(|error| error.to_string())?
                .ok_or("producer original input boundary missing")?;
            let step = serde_json::from_value(task["step_binding"].clone())
                .map_err(|_| "producer effect input boundary has no original Step")?;
            resolved.input_boundaries.push(result_input_boundary(
                &intent,
                execution.clone(),
                resolved
                    .producers
                    .iter()
                    .find(|producer| producer.execution == execution)
                    .and_then(|producer| producer.packet.clone()),
                Some(step),
            )?);
            if let Some(effect) = effect {
                let tool = Some((tool, effect.effect_kind));
                if !resolved
                    .producers
                    .iter()
                    .any(|producer| producer.execution == execution && producer.tool == tool)
                {
                    let producer_packet = resolved
                        .producers
                        .iter()
                        .find(|producer| producer.execution == execution)
                        .and_then(|producer| producer.packet.clone());
                    resolved.producers.push(ResultProducer {
                        actor,
                        execution,
                        packet: producer_packet,
                        tool,
                    });
                }
            }
        }
        let packets = resolved
            .producers
            .iter()
            .filter_map(|producer| producer.packet.clone())
            .collect::<Vec<_>>();
        let commits =
            crate::execution_core::graph::ExecutionCommitService::new(std::sync::Arc::clone(store));
        for packet in packets {
            let source = format!(
                "execution-agent-receipts:{}:{}:{}",
                packet.graph_id(),
                packet.node_id(),
                packet.attempt
            );
            let events = result_source_events(store, &source, &mut cuts)?;
            let receipts = commits
                .load_delegated_agent_tool_receipts(
                    packet.graph_id(),
                    packet.node_id(),
                    packet.attempt,
                )
                .map_err(|error| error.to_string())?;
            pin_result_source(store, &source, &mut cuts)?;
            for receipt in receipts {
                if receipt.effect_kind == ToolEffectKind::Read
                    || super::policy::goal_metadata_tool(&receipt.outcome.tool_name)
                {
                    continue;
                }
                let event = events
                    .iter()
                    .find(|event| {
                        event.kind == "execution.agent_tool.receipt"
                            && event.payload["sequence"].as_u64() == Some(receipt.sequence)
                    })
                    .ok_or("result physical effect has no original index event")?;
                let (descriptor, scope, intent) =
                    authenticate_effect_receipt(store, event, &packet, &mut cuts)?;
                let primary = store
                    .event_by_idempotency_key(
                        &intent.stream_id,
                        &format!(
                            "{}:receipt",
                            intent.payload["idempotency_key"]
                                .as_str()
                                .ok_or("result original effect key missing")?
                        ),
                    )
                    .map_err(|error| error.to_string())?
                    .ok_or("result original terminal missing")?;
                project_result_effect(
                    store,
                    &mut resolved,
                    &descriptor,
                    &scope,
                    &primary,
                    &mut cuts,
                )?;
                let mut boundary = result_input_boundary(
                    &intent,
                    format!("agent-run:{}", packet.run_id()),
                    Some(packet.clone()),
                    None,
                )?;
                boundary.call_key = event.payload["idempotency_key"]
                    .as_str()
                    .ok_or("delegated effect has no original call key")?
                    .to_string();
                resolved.input_boundaries.push(boundary);
            }
        }
    }
    if resolved.producers.is_empty() {
        return Err(format!(
            "review result has no Runtime-resolved producer:{reference}"
        ));
    }
    for (source, expected) in &cuts {
        let observed = store
            .stream_revision(source)
            .map_err(|error| error.to_string())?;
        if observed != *expected {
            return Err(format!(
                "result metadata changed after original effect enumeration: result={reference} source={source} expected={expected} observed={observed}; rebuild the review"
            ));
        }
    }
    for run in process_runs_for_boundaries(store, &resolved.input_boundaries, &mut cuts)? {
        if !resolved
            .process_runs
            .iter()
            .any(|previous| previous.primary == run.primary)
        {
            resolved.process_runs.push(run);
        }
    }
    let actual = super::policy::actual_input_snapshot_for_phase(
        store,
        program,
        &resolved.input_boundaries,
        readers,
        input_cuts,
        allow_history_growth,
    )?;
    for (source, revision) in &actual.sources {
        if cuts
            .insert(source.clone(), *revision)
            .is_some_and(|previous| previous != *revision)
        {
            return Err("actual result input source moved during enumeration".into());
        }
    }
    resolved.primary_inputs = actual.immutable_inputs;
    resolved.effect_sources = cuts
        .into_iter()
        .map(
            |(stream_id, expected_revision)| crate::ExpectedStreamRevision {
                stream_id,
                expected_revision,
            },
        )
        .collect();
    Ok(resolved)
}

pub(super) fn original_acquisition_output(
    store: &std::sync::Arc<crate::RuntimeEventStore>,
    primary: &DurableRuntimeEvent,
    session_events: &[DurableRuntimeEvent],
    readers: super::ResultContentReaders<'_>,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<String, String> {
    let seal = crate::context_evidence::decode_read_acquisition(
        &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
    )
    .map_err(str::to_string)?;
    if primary.payload["outcome_truncated"] != true {
        if let Some(output) = primary.payload["outcome"]["output"].as_str() {
            if crate::context_evidence::read_body_seal(&seal.tool_name, output).as_ref()
                == Some(&seal.body)
            {
                return Ok(output.to_string());
            }
        }
    }
    for event in session_events {
        if event.kind != "tool.invocation.completed"
            || event.payload["tool_call_id"] != seal.invocation_id
            || event.payload["tool_name"] != seal.tool_name
        {
            continue;
        }
        let task = result_tool_plan(session_events, event)?;
        if let Some(body) = original_tool_body(store, readers, event, task, cuts, None)? {
            let output = String::from_utf8(body.bytes)
                .map_err(|_| "original acquisition body is not UTF-8")?;
            if crate::context_evidence::read_body_seal(&seal.tool_name, &output).as_ref()
                == Some(&seal.body)
            {
                return Ok(output);
            }
        }
    }
    Err("actual input original body is missing or differs from its acquisition seal".into())
}

pub(super) fn authenticate_read_input(
    store: &crate::RuntimeEventStore,
    event: &DurableRuntimeEvent,
    packet: &AgentTaskPacket,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<(), String> {
    let value = event
        .payload
        .get(crate::context_evidence::READ_ACQUISITION_FIELD)
        .ok_or("result primary input has no original acquisition seal")?;
    let seal = crate::context_evidence::decode_read_acquisition(value).map_err(str::to_string)?;
    if seal.graph_id != packet.graph_id()
        || seal.node_id != packet.node_id()
        || seal.attempt != packet.attempt
        || seal.session_id != packet.session_id()
        || seal.step.run_ref.as_deref() != Some(packet.run_id())
        || event.stream_id != seal.index_stream
        || event.idempotency_key.as_deref() != Some(seal.index_key.as_str())
    {
        return Err("result primary input belongs to a foreign packet/attempt".into());
    }
    pin_result_source(store, &seal.primary_stream, cuts)?;
    let primary = store
        .event_by_idempotency_key(&seal.primary_stream, &seal.primary_key)
        .map_err(|error| error.to_string())?
        .ok_or("result primary input acquisition missing")?;
    if primary.kind != "execution.read.receipt"
        || primary.actor != event.actor
        || primary.scope != crate::RuntimeEventScope::ExecutionNode
        || primary.scope != event.scope
        || primary.transaction_id != event.transaction_id
        || primary.commit_cursor != event.commit_cursor
        || primary
            .payload
            .get(crate::context_evidence::READ_ACQUISITION_FIELD)
            != Some(value)
        || primary.payload["outcome"]["status"] != "executed"
    {
        return Err("result primary input does not authenticate its original acquisition".into());
    }
    Ok(())
}

fn merge_authenticated_result_effect(
    resolved: &mut ResolvedReviewResult,
    descriptor: &ToolEffectDescriptor,
    original_scope: &harness_contract::goal::ProofEffectScope,
    cursor: u64,
) -> Result<(), String> {
    use harness_contract::goal::ProofEffectPurpose;
    let checked = require_original_effect_scope(
        &serde_json::to_value(original_scope).map_err(|error| error.to_string())?,
        descriptor,
    )?;
    if !checked.complete
        || cursor == 0
        || descriptor.uses_network
        || descriptor.spawns_process
        || descriptor.mutates_packages
        || descriptor.mutates_system
        || !matches!(descriptor.effect_kind, ToolEffectKind::Write)
    {
        return Err(format!(
            "result effect has incomplete or unsupported original scope: {}",
            serde_json::json!({
                "tool": descriptor.tool_id.chars().take(128).collect::<String>(),
                "descriptor_hash": descriptor.descriptor_hash.chars().take(128).collect::<String>(),
                "kind": descriptor.effect_kind,
                "permission": descriptor.required_permission,
                "scope_complete": checked.complete,
                "scope_count": descriptor.scopes.len(),
                "target_count": checked.targets.len(),
                "purpose_count": checked.scope_purposes.len(),
                "control_target_absent": descriptor.scopes.iter().all(|scope| scope.target.is_none()),
                "network": descriptor.uses_network,
                "process": descriptor.spawns_process,
                "packages": descriptor.mutates_packages,
                "system": descriptor.mutates_system,
                "cursor": cursor,
            })
        ));
    }
    let business = checked
        .scope_purposes
        .iter()
        .filter(|binding| binding.purpose == ProofEffectPurpose::BusinessEffect)
        .map(|binding| &binding.declared_scope)
        .collect::<Vec<_>>();
    if business.is_empty() {
        return Err("non-read result lacks an authenticated business effect".into());
    }
    for declared in business {
        let targets = checked
            .targets
            .iter()
            .filter(|target| &target.declared_scope == declared)
            .collect::<Vec<_>>();
        if targets.len() != 1
            || targets[0].path.object_kind != harness_contract::context::WorkspaceObjectKind::File
            || !targets[0].exact
        {
            return Err("result business effect lacks an exact original File target".into());
        }
        if !resolved.effect_scopes.contains(declared) {
            resolved.effect_scopes.push(declared.clone());
        }
        if !resolved.effect_targets.contains(targets[0]) {
            resolved.effect_targets.push(targets[0].clone());
        }
    }
    resolved.kind = GoalResultKind::ToolEffect;
    resolved.effect_cursor = Some(resolved.effect_cursor.unwrap_or_default().max(cursor));
    Ok(())
}

fn authenticate_effect_receipt(
    store: &crate::RuntimeEventStore,
    event: &DurableRuntimeEvent,
    packet: &AgentTaskPacket,
    cuts: &mut std::collections::BTreeMap<String, u64>,
) -> Result<
    (
        ToolEffectDescriptor,
        harness_contract::goal::ProofEffectScope,
        DurableRuntimeEvent,
    ),
    String,
> {
    let key = event.payload["idempotency_key"]
        .as_str()
        .ok_or("result effect lacks its original receipt identity")?;
    let source = format!("execution-effect:{key}");
    pin_result_source(store, &source, cuts)?;
    let primary = store
        .event_by_idempotency_key(&source, &format!("{key}:receipt"))
        .map_err(|error| error.to_string())?
        .ok_or("result effect original primary receipt missing")?;
    let intent = store
        .event_by_idempotency_key(&source, &format!("{key}:intent"))
        .map_err(|error| error.to_string())?
        .ok_or("result effect original admitted intent missing")?;
    if intent.schema_version != 1
        || primary.schema_version != 1
        || crate::ExecutionCommitService::receipt_observation_sequence(event).is_none()
        || primary.status.as_deref() != Some("completed")
        || event.status.as_deref() != Some("completed")
        || intent.kind != "execution.effect.intent"
        || intent.actor.as_deref() != Some("governed_tool")
        || intent.payload["effect"]["descriptor_hash"] != primary.payload["descriptor_hash"]
        || intent.payload["effect"]["effect_kind"] != event.payload["effect_kind"]
        || intent.payload["effect"]["scopes"] != event.payload["effect_scopes"]
        || intent.payload["resolved_effect_scope"] != event.payload["resolved_effect_scope"]
    {
        return Err(
            "result effect scope/classifier disagrees with its original admitted descriptor".into(),
        );
    }
    if event.actor.as_deref() != Some("governed_tool")
        || event.scope != crate::RuntimeEventScope::ExecutionNode
        || primary.actor != event.actor
        || primary.scope != event.scope
        || primary.kind != "execution.effect.receipt"
        || primary.transaction_id != event.transaction_id
        || primary.commit_cursor != event.commit_cursor
        || primary.payload["outcome"] != event.payload["outcome"]
        || primary.payload["outcome"]["status"] != "executed"
        || primary.payload["outcome"]["error"].as_str().is_some()
        || primary.payload["replay"]["parent_execution"]["execution_id"] != packet.graph_id()
        || primary.payload["replay"]["parent_execution"]["node_id"] != packet.node_id()
        || !event.refs.iter().any(|reference| {
            reference.kind == "agent_attempt" && reference.id == packet.attempt.to_string()
        })
        || primary.payload["resolved_effect_scope"] != event.payload["resolved_effect_scope"]
    {
        return Err(
            "result effect receipt does not belong to its original physical producer".into(),
        );
    }
    let descriptor: ToolEffectDescriptor = serde_json::from_value(intent.payload["effect"].clone())
        .map_err(|_| "effect admitted descriptor is invalid")?;
    if primary.payload["replay"]["invocation"]["effect"] != intent.payload["effect"]
        || intent.commit_cursor >= primary.commit_cursor
    {
        return Err("effect original invocation or admission cursor disagrees".into());
    }
    let scope =
        require_original_effect_scope(&intent.payload["resolved_effect_scope"], &descriptor)?;
    Ok((descriptor, scope, intent))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn original_native_primary_658(negative: bool) {
        use harness_contract::step::CallSource;
        let (_workspace, services, owner) =
            crate::agentic::execution::reviewed_goal_fixture::native_raw_owner_fixture_for_test()
                .await;
        let store = services.event_store();
        let binding = owner.activity_binding().unwrap();
        let events = store.list_stream(&owner.stream_id).unwrap();
        let task = result_tool_plan(&events, &owner).unwrap().unwrap();
        let step: crate::step_snapshot::StepCallBinding =
            serde_json::from_value(task["step_binding"].clone()).unwrap();
        assert!(
            matches!(&step.origin.source, CallSource::Provider { attempt_id }
            if !attempt_id.is_empty() && attempt_id == &step.snapshot.provider_attempt.attempt_id)
        );
        let mut cuts = std::collections::BTreeMap::new();
        let primary = original_tool_primary(store, &owner, task, &mut cuts).unwrap();
        let parent = &primary.payload["replay"]["parent_execution"];
        let graph = services
            .graph_state_store()
            .load(parent["execution_id"].as_str().unwrap())
            .unwrap();
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == parent["node_id"].as_str().unwrap())
            .unwrap();
        let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref).unwrap();
        let principal = packet.assignment.execution_identity.principal_id();
        assert_ne!(principal, packet.agent_id());
        assert_eq!(step.snapshot.identity.actor_ref.as_deref(), Some(principal));
        assert_eq!(
            step.snapshot.identity.execution_id.as_deref(),
            Some(packet.run_id())
        );
        assert_eq!(binding.agent_run_id.as_deref(), Some(packet.run_id()));
        let index_source = format!(
            "execution-agent-receipts:{}:{}:{}",
            packet.graph_id(),
            packet.node_id(),
            packet.attempt
        );
        let index = store
            .event_by_idempotency_key(
                &index_source,
                &format!("agent-tool-receipt:{}", step.effect_idempotency_key()),
            )
            .unwrap()
            .unwrap();
        assert_eq!(index.schema_version, 2);
        // artifact_publish is a Read goal-metadata tool: the delegated receipt
        // loop skips it (results.rs:3965-3970) and authenticate_effect_receipt
        // is write-effect-specific, so this index pairs with its read-receipt
        // primary instead of authenticating as a business/control effect.
        let read_key = index.payload["idempotency_key"].as_str().unwrap();
        let read_primary = store
            .event_by_idempotency_key(
                &format!("execution-effect:{read_key}"),
                &format!("{read_key}:read-receipt"),
            )
            .unwrap()
            .unwrap();
        assert_eq!(read_primary.schema_version, 1);
        assert_eq!(read_primary.transaction_id, index.transaction_id);
        assert_eq!(read_primary.commit_cursor, index.commit_cursor);
        let session_events = store.list_stream(&owner.stream_id).unwrap();
        let native_events = store.list_stream(&index_source).unwrap();
        let mut original_calls = BTreeSet::new();
        for receipt in native_events.iter().filter(|event| event.kind == "execution.agent_tool.receipt") {
            let call_id = receipt.payload["outcome"]["tool_use_id"].as_str().unwrap();
            let completed = session_events.iter().find(|event| {
                event.kind == "tool.invocation.completed"
                    && event.payload["tool_call_id"] == call_id
            }).unwrap();
            let plan = result_tool_plan(&session_events, completed).unwrap().unwrap();
            let original = original_tool_primary(store, completed, plan, &mut cuts).unwrap();
            assert_eq!(original.transaction_id, receipt.transaction_id);
            assert_eq!(original.commit_cursor, receipt.commit_cursor);
            let descriptor: ToolEffectDescriptor =
                serde_json::from_value(plan["effect"].clone()).unwrap();
            let scope = require_original_effect_scope(
                &original.payload["resolved_effect_scope"], &descriptor,
            ).unwrap();
            assert_eq!(original.payload["resolved_effect_scope"], receipt.payload["resolved_effect_scope"]);
            match descriptor.tool_id.as_str() {
                "read_file" => {
                    assert_eq!(descriptor.effect_kind, ToolEffectKind::Read);
                    assert!(scope.complete);
                    assert_eq!(scope.targets.len(), 1);
                    assert!(scope.scope_purposes.iter().all(|entry|
                        entry.purpose == harness_contract::goal::ProofEffectPurpose::InputRead));
                }
                "artifact_publish" => {
                    assert_eq!(descriptor.effect_kind, ToolEffectKind::Read);
                    assert!(super::super::policy::goal_metadata_tool(&descriptor.tool_id));
                    assert!(!scope.complete && scope.targets.is_empty());
                    assert!(scope.scope_purposes.iter().all(|entry|
                        entry.purpose == harness_contract::goal::ProofEffectPurpose::InputRead));
                    assert_eq!(plan["normalized_input"]["source"], "file");
                    let (_, _, mut not_business) = purpose_projection_fixture();
                    assert!(merge_authenticated_result_effect(
                        &mut not_business, &descriptor, &scope, original.commit_cursor,
                    ).is_err());
                }
                "task_claim" | "task_submit" => {
                    assert!(runtime_control_descriptor(&descriptor));
                }
                other => panic!("unexpected original Native receipt tool: {other}"),
            }
            original_calls.insert(call_id.to_string());
        }
        assert_eq!(
            original_calls,
            BTreeSet::from([
                "native-claim".to_string(),
                "native-preview-read".to_string(),
                "native-full-read".to_string(),
                "native-publication-small".to_string(),
                "native-submit".to_string(),
            ])
        );
        let mut control_tools = BTreeSet::new();
        let mut control_index = None;
        for control in store
            .list_stream(&index_source)
            .unwrap()
            .into_iter()
            .filter(|event| {
                event.kind == "execution.agent_tool.receipt"
                    && matches!(
                        event.payload["outcome"]["tool_name"].as_str(),
                        Some("task_claim" | "task_submit")
                    )
            })
        {
            let (descriptor, scope, intent) =
                authenticate_effect_receipt(store, &control, &packet, &mut cuts).unwrap();
            if control_index.is_none() {
                control_index = Some(control.clone());
            }
            assert!(descriptor.scopes[0].target.is_none());
            let primary = store
                .event_by_idempotency_key(
                    &intent.stream_id,
                    &format!(
                        "{}:receipt",
                        control.payload["idempotency_key"].as_str().unwrap()
                    ),
                )
                .unwrap()
                .unwrap();
            let (_, _, mut resolved) = purpose_projection_fixture();
            project_result_effect(
                store,
                &mut resolved,
                &descriptor,
                &scope,
                &primary,
                &mut cuts,
            )
            .unwrap();
            assert!(resolved.effect_targets.is_empty());
            assert!(resolved.effect_scopes.is_empty());
            assert_eq!(resolved.kind, GoalResultKind::Content);
            let mut foreign = control.clone();
            foreign.payload["effect_scopes"] = serde_json::json!([]);
            assert!(authenticate_effect_receipt(store, &foreign, &packet, &mut cuts).is_err());
            let mut foreign_scope = scope.clone();
            foreign_scope.descriptor_hash = "foreign".into();
            assert!(project_result_effect(
                store,
                &mut resolved,
                &descriptor,
                &foreign_scope,
                &primary,
                &mut cuts
            )
            .is_err());
            control_tools.insert(descriptor.tool_id);
        }
        assert_eq!(
            control_tools,
            BTreeSet::from(["task_claim".to_string(), "task_submit".to_string()])
        );
        for field in [
            "sequence_owner",
            "source_observation_wave_sequence",
            "effect_kind",
            "effect_scopes",
        ] {
            let mut invalid = control_index.clone().unwrap();
            invalid.payload[field] = serde_json::Value::Null;
            assert!(authenticate_effect_receipt(store, &invalid, &packet, &mut cuts).is_err());
        }
        let mut invalid = index.clone();
        invalid.schema_version = 3;
        assert!(authenticate_effect_receipt(store, &invalid, &packet, &mut cuts).is_err());
        assert_eq!(primary.transaction_id, index.transaction_id);
        assert_eq!(primary.commit_cursor, index.commit_cursor);
        assert!(primary.commit_cursor < owner.commit_cursor);
        assert_eq!(
            cuts.get(&primary.stream_id),
            Some(&store.stream_revision(&primary.stream_id).unwrap())
        );
        assert_eq!(
            cuts.get(&index_source),
            Some(&store.stream_revision(&index_source).unwrap())
        );
        assert_eq!(cuts.get(packet.graph_id()), Some(&graph.revision));
        let actions = services.agent_action_service();
        let program = actions
            .project(&packet.agentic_binding.as_ref().unwrap().program_id)
            .unwrap();
        let raw_ref = owner.payload["full_output_ref"].as_str().unwrap();
        let access = services
            .session_evidence_access(
                &binding.session_id,
                raw_ref.strip_prefix("tool://").unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        let content = services
            .artifact_store()
            .resolve(&access.retrieval_selector)
            .unwrap();
        let history = services.session_history_reader();
        let metadata = resolve_result_metadata(
            store,
            &program,
            raw_ref,
            &content,
            None,
            super::super::ResultContentReaders {
                artifacts: Some(services.artifact_store().as_ref()),
                history: history.as_deref(),
                process_inputs: None,
            },
        )
        .unwrap();
        metadata.origin.require_strong().unwrap();
        assert_eq!(metadata.content.selector, content.selector);
        assert_eq!(metadata.content.sha256, content.sha256);
        assert!(metadata.producers.iter().any(|producer| producer
            .packet
            .as_ref()
            .is_some_and(|original| original.graph_id() == packet.graph_id()
                && original.run_id() == packet.run_id())));
        let publication: serde_json::Value =
            serde_json::from_str(primary.payload["outcome"]["output"].as_str().unwrap()).unwrap();
        let published = services
            .artifact_store()
            .resolve(publication["content_ref"].as_str().unwrap())
            .unwrap();
        let published_metadata = resolve_result_metadata(
            store,
            &program,
            &published.selector,
            &published,
            None,
            super::super::ResultContentReaders {
                artifacts: Some(services.artifact_store().as_ref()),
                history: history.as_deref(),
                process_inputs: None,
            },
        )
        .unwrap();
        published_metadata.origin.require_strong().unwrap();
        assert!(published_metadata.producers.iter().any(|producer| producer
            .packet
            .as_ref()
            .is_some_and(|original| original.graph_id() == packet.graph_id()
                && original.run_id() == packet.run_id())));
        if !negative {
            return;
        }
        let before = store.durable_commit_cursor().unwrap();
        for mutation in [
            "principal",
            "instance",
            "definition-agent",
            "execution-graph",
            "run",
            "session",
            "turn",
            "attempt",
            "catalog",
            "step",
            "provider-attempt",
            "process-source",
            "foreign-worker",
            "zero-sequence",
            "worker-revision",
            "system-source",
            "offline-source",
        ] {
            let mut changed = task.clone();
            let value = &mut changed["step_binding"];
            match mutation {
                "principal" => {
                    value["snapshot"]["identity"]["actor_ref"] =
                        serde_json::json!("foreign-principal")
                }
                "instance" => {
                    value["snapshot"]["identity"]["actor_ref"] =
                        serde_json::json!(packet.agent_id())
                }
                "definition-agent" => {
                    value["snapshot"]["identity"]["actor_ref"] =
                        serde_json::json!(packet.agentic_binding.as_ref().unwrap().agent_id)
                }
                "execution-graph" => {
                    value["snapshot"]["identity"]["execution_id"] =
                        serde_json::json!(packet.graph_id())
                }
                "run" => {
                    value["snapshot"]["identity"]["run_ref"] = serde_json::json!("foreign-run")
                }
                "session" => {
                    value["snapshot"]["identity"]["session_id"] =
                        serde_json::json!("foreign-session")
                }
                "turn" => {
                    value["snapshot"]["identity"]["turn_id"] = serde_json::json!("foreign-turn")
                }
                "attempt" => {
                    value["snapshot"]["identity"]["attempt"] = serde_json::json!(packet.attempt + 1)
                }
                "catalog" => {
                    value["snapshot"]["catalog_digest"] = serde_json::json!("foreign-catalog")
                }
                "step" => {
                    value["snapshot"]["identity"]["step_id"] = serde_json::json!("foreign-step")
                }
                "provider-attempt" => {
                    value["snapshot"]["provider_attempt"]["attempt_id"] =
                        serde_json::json!("foreign-attempt")
                }
                "system-source" => {
                    value["origin"]["source"] = serde_json::json!({"kind":"system_derived","parent_step_id":step.origin.origin_step_id})
                }
                "offline-source" => {
                    value["origin"]["source"] = serde_json::json!({"kind":"offline_test"})
                }
                _ => {
                    value["snapshot"]["identity"]["actor_ref"] =
                        serde_json::json!(packet.agent_id());
                    value["snapshot"]["identity"]["execution_id"] =
                        serde_json::json!(packet.graph_id());
                    value["snapshot"]["identity"]["attempt"] = serde_json::json!(packet.attempt);
                    value["snapshot"]["related_revisions"]["worker_run"] =
                        serde_json::json!(if mutation == "worker-revision" {
                            "foreign-worker"
                        } else {
                            packet.run_id()
                        });
                    value["origin"]["source"] = serde_json::json!({"kind":"process",
                        "worker_id":if mutation == "foreign-worker" { "foreign-worker" } else { packet.run_id() },
                        "sequence":if mutation == "zero-sequence" { 0 } else { 1 }});
                }
            }
            assert!(
                original_tool_primary(store, &owner, &changed, &mut Default::default()).is_err(),
                "{mutation}"
            );
            assert_eq!(store.durable_commit_cursor().unwrap(), before);
        }
        let mut changed_owner = owner.clone();
        changed_owner.commit_cursor = primary.commit_cursor;
        assert!(
            original_tool_primary(store, &changed_owner, task, &mut Default::default()).is_err()
        );
        for source in [&primary.stream_id, &index_source, &graph.id] {
            let mut stale_cut = std::collections::BTreeMap::from([(
                source.clone(),
                store.stream_revision(source).unwrap() + 1,
            )]);
            assert!(original_tool_primary(store, &owner, task, &mut stale_cut).is_err());
        }
        assert_eq!(store.durable_commit_cursor().unwrap(), before);
    }

    #[tokio::test]
    async fn r1_658_real_native_original_primary_and_resolver_use_principal_run() {
        original_native_primary_658(false).await;
    }

    #[tokio::test]
    async fn r1_658_real_native_primary_rejects_foreign_typed_identity_and_receipt_cuts() {
        original_native_primary_658(true).await;
    }

    async fn exercise_publication_task_link(mode: u8) {
        use crate::agentic::execution::reviewed_goal_fixture::{
            with_native_task_review_setup, NativeTaskReviewCase,
        };
        use harness_contract::agent_action::{AgentAction, AgentActionEnvelope, AgentActionStatus};
        let root_actor = std::sync::Arc::new(std::sync::Mutex::new(None));
        let retained_root = std::sync::Arc::clone(&root_actor);
        with_native_task_review_setup(
            NativeTaskReviewCase { commit_report: true, ..Default::default() },
            move |services, root, worker, mut input| Box::pin(async move {
                *retained_root.lock().unwrap() = Some(root.actor.clone());
                let actions = services.agent_action_service();
                let program = actions.project(&worker.actor.program_id).unwrap();
                let artifact = program.artifacts.values().find(|artifact| {
                    artifact.claim_execution_id == worker.actor.execution_id
                        && artifact.relates_to.contains(&input.task_ref)
                }).unwrap().clone();
                assert_eq!(program.tasks[&input.task_ref].status, crate::AgenticTaskStatus::Claimed);
                assert!(program.tasks[&input.task_ref].artifact_refs.is_empty());
                let mut cuts = std::collections::BTreeMap::new();
                authenticate_publication_task_link(services.event_store(), &program,
                    &artifact, &input.task_ref, &mut cuts).unwrap();
                assert_eq!(actions.project(&program.program_id).unwrap(), program);
                assert_eq!(cuts.get(&format!("agentic-program:{}", program.program_id)), Some(&program.revision));
                assert!(cuts.contains_key(worker.actor.execution_id.as_deref().unwrap()));
                if mode == 1 {
                    let foreign = actions.apply(&AgentActionEnvelope {
                        action_id: "r1-568-foreign-task".into(), actor: root.actor.clone(),
                        expected_revision: None,
                        action: AgentAction::TaskPublish(serde_json::from_value(serde_json::json!({
                            "team_ref":program.tasks[&input.task_ref].team_id,
                            "title":"Unclaimed foreign provenance", "objective":"Remain independent",
                            "acceptance":"No implicit completion", "purpose":"exploration"
                        })).unwrap()),
                    }).unwrap();
                    assert_eq!(foreign.status, AgentActionStatus::Applied, "{foreign:?}");
                    let foreign_task = foreign.changed_refs[0].clone();
                    let extra_content = services.artifact_store().write_bytes(
                        harness_contract::context::ArtifactWriteDescriptor {
                            media_type: "text/plain".into(),
                            visibility_scope: format!("session:{}", program.session_id),
                            expected_bytes: None, original_name: None,
                        },
                        b"Real independent extra-relation publication",
                    ).await.unwrap();
                    let publication = actions.apply(&AgentActionEnvelope {
                        action_id: "r1-568-extra-task-publication".into(), actor: worker.actor.clone(),
                        expected_revision: None,
                        action: AgentAction::ArtifactCommit(serde_json::from_value(serde_json::json!({
                            "content_ref":extra_content.selector, "kind":"report", "title":"Extra relation",
                            "relates_to":[input.task_ref, foreign_task]
                        })).unwrap()),
                    }).unwrap();
                    assert_eq!(publication.status, AgentActionStatus::Applied, "{publication:?}");
                    let current = actions.project(&program.program_id).unwrap();
                    let extra = &current.artifacts[&publication.changed_refs[0]];
                    let mut cuts = std::collections::BTreeMap::new();
                    authenticate_publication_task_link(services.event_store(), &current,
                        extra, &input.task_ref, &mut cuts).unwrap();
                    for _ in 0..2 {
                        assert!(authenticate_publication_task_link(services.event_store(), &current,
                            extra, &foreign_task, &mut cuts).is_err());
                    }
                    assert_eq!(current.tasks[&foreign_task].status, crate::AgenticTaskStatus::Published);
                    assert!(current.tasks[&foreign_task].artifact_refs.is_empty());
                }
                if mode == 2 {
                    for field in ["content_ref", "committed_by", "claim_execution_id",
                        "claim_generation", "title", "kind", "relates_to"]
                    {
                        let mut value = serde_json::to_value(&artifact).unwrap();
                        value[field] = match field {
                            "claim_generation" => serde_json::json!(artifact.claim_generation.unwrap() + 1),
                            "relates_to" => serde_json::json!([input.task_ref, "task:forged"]),
                            _ => serde_json::json!("forged"),
                        };
                        let changed = serde_json::from_value(value).unwrap();
                        assert!(authenticate_publication_task_link(services.event_store(), &program,
                            &changed, &input.task_ref, &mut std::collections::BTreeMap::new()).is_err(),
                            "{field}");
                    }
                    for field in ["session_id", "turn_id", "root_execution_id", "objective_id"] {
                        let mut value = serde_json::to_value(&program).unwrap();
                        value[field] = serde_json::json!("foreign");
                        let changed = serde_json::from_value(value).unwrap();
                        assert!(authenticate_publication_task_link(services.event_store(), &changed,
                            &artifact, &input.task_ref, &mut std::collections::BTreeMap::new()).is_err(),
                            "{field}");
                    }
                    let source = format!("agentic-program:{}", program.program_id);
                    let mut stale = std::collections::BTreeMap::from([(source, program.revision - 1)]);
                    assert!(authenticate_publication_task_link(services.event_store(), &program,
                        &artifact, &input.task_ref, &mut stale).is_err());
                }
                input.deliverable = None;
                input.artifact_refs = vec![artifact.artifact_ref];
                input.evidence_refs = vec![artifact.content_ref];
                input
            }),
            move |services, review, _| Box::pin(async move {
                let task_ref = match &review.action {
                    AgentAction::TaskReview(input) => &input.task_ref,
                    _ => unreachable!(),
                };
                let actions = services.agent_action_service();
                let program = actions.project(&review.actor.program_id).unwrap();
                assert_eq!(program.tasks[task_ref].status, crate::AgenticTaskStatus::Submitted);
                let artifact = program.artifacts[&program.tasks[task_ref].artifact_refs[0]].clone();
                authenticate_publication_task_link(services.event_store(), &program,
                    &artifact, task_ref, &mut std::collections::BTreeMap::new()).unwrap();
                if mode == 0 {
                    let actor = root_actor.lock().unwrap().clone().unwrap();
                    let withdrawal = services.submit_agent_action(&AgentActionEnvelope {
                        action_id: "r1-568-history-withdrawal".into(), actor,
                        expected_revision: Some(program.revision),
                        action: AgentAction::TaskWithdraw(serde_json::from_value(serde_json::json!({
                            "task_ref":task_ref, "reason_ref":artifact.content_ref,
                            "evidence_refs":[artifact.content_ref]
                        })).unwrap()),
                    }).await.unwrap();
                    assert_eq!(withdrawal.status, AgentActionStatus::Applied, "{withdrawal:?}");
                    let withdrawn = actions.project(&program.program_id).unwrap();
                    assert_ne!(withdrawn.tasks[task_ref].status, crate::AgenticTaskStatus::Accepted);
                    assert!(withdrawn.tasks[task_ref].pending_retirement.is_some()
                        || withdrawn.tasks[task_ref].status == crate::AgenticTaskStatus::Withdrawn);
                    authenticate_publication_task_link(services.event_store(), &withdrawn,
                        &artifact, task_ref, &mut std::collections::BTreeMap::new()).unwrap();
                }
            }),
        ).await;
    }

    #[tokio::test]
    async fn r1_568_publication_provenance_survives_real_submission_and_withdrawal() {
        exercise_publication_task_link(0).await;
    }

    #[tokio::test]
    async fn r1_568_publication_extra_task_relation_rejects_on_every_visit() {
        exercise_publication_task_link(1).await;
    }

    #[tokio::test]
    async fn r1_568_publication_authentication_rejects_postimage_identity_and_cut_tampering() {
        exercise_publication_task_link(2).await;
    }

    fn register_publication_root(
        store: &std::sync::Arc<crate::RuntimeEventStore>,
        actor: &AgentActorBinding,
    ) {
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
        };
        let mut graph = ExecutionGraph::new("Original publication fixture root");
        graph.id = actor.root_execution_id.clone().unwrap();
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: actor.session_id.clone(),
            turn_id: actor.turn_id.clone(),
            root_task_id: graph.id.clone(),
            task_id: graph.id.clone(),
            generation: 1,
        });
        graph.nodes.push(ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "inline_model",
            "Publication fixture root",
        ));
        crate::ExecutionCommitService::new(std::sync::Arc::clone(store))
            .register_graph(graph)
            .unwrap();
    }
    use crate::execution_core::graph::{
        ScopeLockManager, ScopeLockMode, ScopeLockRequest, ScopedResource,
    };

    #[tokio::test]
    async fn r1_native_first_claim_publications_require_exact_postclaim_generation() {
        use crate::agentic::execution::reviewed_goal_fixture::{
            with_native_task_review_setup, NativeTaskReviewCase,
        };
        use harness_contract::agent_action::{AgentAction, AgentActionEnvelope};
        for explicit_commit in [false, true] {
            with_native_task_review_setup(NativeTaskReviewCase {
                commit_report: explicit_commit, ..Default::default()
            },
                move |services, _, worker, mut input| Box::pin(async move {
                    if explicit_commit {
                        let current = services.agent_action_service().project(&worker.actor.program_id).unwrap();
                        let artifacts: Vec<String> = current.artifacts.iter().filter(|(_, artifact)| {
                            artifact.claim_execution_id == worker.actor.execution_id
                                && artifact.claim_generation == Some(1)
                                && artifact.relates_to.contains(&input.task_ref)
                        }).map(|(reference, _)| reference.clone()).collect();
                        assert_eq!(artifacts.len(), 1);
                        let content = current.artifacts[&artifacts[0]].content_ref.clone();
                        input.deliverable = None;
                        input.artifact_refs = artifacts;
                        input.evidence_refs = vec![content];
                    }
                    input
                }), move |services, review, donor| Box::pin(async move {
                    let task_ref = match &review.action {
                        AgentAction::TaskReview(input) => &input.task_ref,
                        _ => unreachable!(),
                    };
                    let program = services.agent_action_service().project(&review.actor.program_id).unwrap();
                    assert_eq!(program.tasks[task_ref].claim_generation, 1);
                    let reference = &program.tasks[task_ref].artifact_refs[0];
                    let artifact = &program.artifacts[reference];
                    assert_eq!(artifact.claim_generation, Some(1));
                    authenticate_publication_task_link(services.event_store(), &program,
                        artifact, task_ref, &mut std::collections::BTreeMap::new()).unwrap();
                    let content = services.artifact_store().resolve(&artifact.content_ref).unwrap();
                    let history = services.session_history_reader();
                    let readers = super::super::ResultContentReaders {
                        artifacts: Some(services.artifact_store().as_ref()), history: history.as_deref(), process_inputs: None,
                    };
                    let resolved = resolve_result_metadata(services.event_store(), &program,
                        reference, &content, None, readers).unwrap();
                    resolved.origin.require_strong().unwrap();
                    assert_eq!(resolved.producers.len(), 1);
                    let producer = &resolved.producers[0];
                    let packet = producer.packet.as_ref().unwrap();
                    assert_eq!(packet.attempt, 1);
                    assert_eq!(artifact.claim_execution_id.as_deref(), Some(packet.graph_id()));
                    assert_ne!(review.actor.execution_id.as_deref(), Some(packet.graph_id()));
                    let call = if explicit_commit { "native-first-claim-artifact" } else { "native-submit" };
                    let tool = if explicit_commit { "artifact_commit" } else { "task_submit" };
                    let completed: Vec<_> = services.event_store()
                        .list_stream(&format!("session:{}", packet.session_id())).unwrap()
                        .into_iter().filter(|event| event.kind == "tool.invocation.completed"
                            && event.payload["tool_call_id"] == call && event.payload["tool_name"] == tool).collect();
                    assert_eq!(completed.len(), 1);
                    assert_eq!(completed[0].activity_binding().unwrap().agent_run_id.as_deref(), Some(packet.run_id()));
                    let publications: Vec<AgentActionEnvelope> = services.event_store()
                        .list_stream(&format!("agentic-program:{}", program.program_id)).unwrap()
                        .into_iter().filter_map(|event| serde_json::from_value(event.payload["envelope"].clone()).ok())
                        .filter(|envelope: &AgentActionEnvelope| envelope.actor.execution_id.as_deref() == Some(packet.graph_id()))
                        .collect();
                    assert!(publications.iter().any(|envelope| matches!(&envelope.action,
                        AgentAction::TaskClaim(input) if &input.task_ref == task_ref)));
                    assert!(publications.iter().any(|envelope| if explicit_commit {
                        matches!(&envelope.action, AgentAction::ArtifactCommit(input)
                            if input.content_ref == artifact.content_ref && input.relates_to.contains(task_ref))
                    } else {
                        matches!(&envelope.action, AgentAction::TaskSubmit(input)
                            if &input.task_ref == task_ref && input.artifact_refs.is_empty()
                                && input.deliverable.as_ref().and_then(|deliverable| deliverable.resolved_content_ref.as_deref())
                                    == Some(artifact.content_ref.as_str()))
                    }));
                    assert_eq!(donor.verification.input_set.as_ref().unwrap().version, 2);
                    for generation in [0, 2] {
                        let mut wrong = program.clone();
                        wrong.artifacts.get_mut(reference).unwrap().claim_generation = Some(generation);
                        let error = resolve_result_metadata(services.event_store(), &wrong,
                            reference, &content, None, readers).err().expect("nonmatching generation must fail");
                        assert_eq!(error, "result publication does not match its original physical packet/claim");
                    }
                    assert_eq!(services.agent_action_service().project(&program.program_id).unwrap(), program);
                })).await;
        }
    }

    #[test]
    fn r1_bounded_history_desc_keyset_preserves_deep_source_proof() {
        let store = crate::RuntimeEventStore::for_test();
        let effect = ToolEffectDescriptor {
            tool_id: "read_file".into(),
            descriptor_hash: "original-accepted-profile-version".into(),
            effect_kind: ToolEffectKind::Read,
            idempotency: harness_contract::tool::ToolIdempotency::Idempotent,
            scopes: Vec::new(),
            required_permission: harness_contract::tool::ToolPermissionMode::ReadOnly,
            approval_class: harness_contract::tool::ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: Default::default(),
        };
        let append = |kind: &str, payload: serde_json::Value| {
            store
                .append(crate::RuntimeEventInput {
                    stream_id: "session:deep-proof".into(),
                    scope: crate::RuntimeEventScope::Session,
                    kind: kind.into(),
                    status: None,
                    actor: None,
                    refs: Vec::new(),
                    payload,
                })
                .unwrap()
        };
        append(
            "tool.execution_plan.created",
            serde_json::json!({"plan_id":"original-plan","tasks":[{"tool_call_id":"original-call","tool_name":"read_file","effect":effect}]}),
        );
        for sequence in 0..4096 {
            append(
                "current.unrelated",
                serde_json::json!({"sequence":sequence}),
            );
        }
        let original = append(
            "tool.invocation.completed",
            serde_json::json!({"governed_plan_id":"original-plan","tool_call_id":"original-call","tool_name":"read_file"}),
        );
        let mut current_policy = effect.clone();
        current_policy.descriptor_hash = "today-must-not-rejudge-historical-lease".into();
        append(
            "tool.execution_plan.created",
            serde_json::json!({"plan_id":"original-plan","tasks":[{"tool_call_id":"original-call","tool_name":"read_file","effect":current_policy}]}),
        );
        assert_eq!(
            tool_effect_from_store(&store, &original).unwrap(),
            Some(effect.clone())
        );
        let mut foreign = original.clone();
        foreign.payload["governed_plan_id"] = serde_json::json!("foreign-plan");
        assert!(tool_effect_from_store(&store, &foreign).unwrap().is_none());
        let tail = store
            .list_stream_before("session:deep-proof", 4098, 64, 1024 * 1024)
            .unwrap();
        assert_eq!(tail.len(), 64);
        assert!(tail
            .windows(2)
            .all(|pair| pair[0].sequence == pair[1].sequence + 1));
        assert_eq!(
            store
                .list_stream_before(
                    "session:deep-proof",
                    tail.last().unwrap().sequence - 1,
                    64,
                    1024 * 1024
                )
                .unwrap()[0]
                .sequence,
            4034
        );
        let mut wrong_tool = original;
        wrong_tool.payload["tool_name"] = serde_json::json!("write_file");
        assert_eq!(
            tool_effect_from_store(&store, &wrong_tool).unwrap_err(),
            "original tool plan does not contain its claimed call"
        );
    }

    #[test]
    fn r1_original_tool_plan_claim_requires_exact_call() {
        let store = crate::RuntimeEventStore::for_test();
        let append = |kind: &str, payload: serde_json::Value| {
            store
                .append(crate::RuntimeEventInput {
                    stream_id: "session:claimed-plan".into(),
                    scope: crate::RuntimeEventScope::Tool,
                    kind: kind.into(),
                    status: None,
                    actor: Some("conversation_runtime".into()),
                    refs: Vec::new(),
                    payload,
                })
                .unwrap()
        };
        append(
            "tool.execution_plan.created",
            serde_json::json!({
                "plan_id":"original-plan", "tasks":[{
                    "tool_call_id":"original-call", "tool_name":"read_file"
                }]
            }),
        );
        let original = append(
            "tool.invocation.completed",
            serde_json::json!({
                "governed_plan_id":"original-plan", "tool_call_id":"original-call",
                "tool_name":"read_file"
            }),
        );
        let events = store.list_stream("session:claimed-plan").unwrap();
        assert_eq!(
            result_tool_plan(&events, &original).unwrap().unwrap()["tool_call_id"],
            "original-call"
        );
        for (field, value) in [
            ("governed_plan_id", serde_json::json!("missing-plan")),
            ("governed_plan_id", serde_json::json!("")),
            ("governed_plan_id", serde_json::json!(42)),
            ("tool_call_id", serde_json::json!("foreign-call")),
            ("tool_call_id", serde_json::json!("")),
            ("tool_call_id", serde_json::Value::Null),
            ("tool_name", serde_json::json!("write_file")),
        ] {
            let mut changed = original.clone();
            changed.payload[field] = value;
            assert!(result_tool_plan(&events, &changed).is_err(), "{field}");
        }
        let mut legacy = original.clone();
        legacy.payload.as_object_mut().unwrap().remove("governed_plan_id");
        assert!(result_tool_plan(&events, &legacy).unwrap().is_none());
        legacy.payload["governed_plan_id"] = serde_json::Value::Null;
        assert!(result_tool_plan(&events, &legacy).unwrap().is_none());
        let no_plan_record = crate::ToolInvocationRecord::started(
            "claimed-plan", 0, "denied-call", "read_file", "{}",
            crate::tool_orchestrator::ToolSafetyCategory::ReadOnly, 1,
        );
        let mut current_unplanned = original.clone();
        current_unplanned.payload = serde_json::to_value(no_plan_record).unwrap();
        assert_eq!(current_unplanned.payload["governed_plan_id"], serde_json::Value::Null);
        assert!(result_tool_plan(&events, &current_unplanned).unwrap().is_none());
        let mut missing_call = original.clone();
        missing_call.payload.as_object_mut().unwrap().remove("tool_call_id");
        assert!(result_tool_plan(&events, &missing_call).is_err());
        let mut duplicated = events.clone();
        let duplicate = duplicated[0].payload["tasks"][0].clone();
        duplicated[0].payload["tasks"].as_array_mut().unwrap().push(duplicate);
        assert!(result_tool_plan(&duplicated, &original).is_err());
        let mut foreign_owner = events.clone();
        foreign_owner[0].actor = Some("foreign-runtime".into());
        assert!(result_tool_plan(&foreign_owner, &original).is_err());
        let mut future_plan = events.clone();
        future_plan[0].commit_cursor = original.commit_cursor + 1;
        assert!(result_tool_plan(&future_plan, &original).is_err());
    }

    #[tokio::test]
    async fn r1_publication_fresh_object_causal_boundaries() {
        use harness_contract::agent_action::{AgentAction, AgentActionEnvelope};
        use harness_contract::context::ArtifactWriteDescriptor;
        use sha2::{Digest, Sha256};
        let defects = [
            ("version", serde_json::json!(2)),
            ("tool_call_id", serde_json::json!("foreign-call")),
            ("ref_id", serde_json::json!("tool://foreign")),
            ("visibility_scope", serde_json::json!("session:foreign")),
            ("sha256", serde_json::json!("truncated")),
            ("byte_count", serde_json::json!(999)),
            ("selector", serde_json::json!("artifact://missing")),
            ("media_type", serde_json::json!("application/foreign")),
            ("legacy", serde_json::Value::Null),
        ];
        for (field, value) in defects {
            for later in [false, true] {
                let root = tempfile::tempdir().unwrap();
                let (journal, backend) =
                    crate::content_publication::tests::JournalFault::new_journal();
                let services = RuntimeServices::builder(root.path().join("home"), root.path())
                    .runtime_event_store(journal)
                    .artifact_store(std::sync::Arc::new(crate::ArtifactStore::for_test_default(
                        root.path().join("blobs"),
                    )))
                    .task_aggregate_service(std::sync::Arc::new(
                        crate::TaskAggregateService::for_test(),
                    ))
                    .build()
                    .unwrap();
                let store = services.event_store();
                let actor: AgentActorBinding = serde_json::from_value(serde_json::json!({
                    "objective_id":"fresh-objective", "program_id":"fresh-program",
                    "session_id":"fresh-session", "turn_id":"fresh-turn",
                    "root_execution_id":"fresh-root", "actor_id":"fresh-root-actor",
                    "kind":"root", "objective_summary":"Fresh causal boundary",
                    "model_lease":"fixture", "permission_ceiling":harness_contract::policy::PermissionMode::WorkspaceWrite
                })).unwrap();
                let actions = crate::AgentActionService::new(std::sync::Arc::clone(store))
                    .with_artifact_store(std::sync::Arc::clone(services.artifact_store()));
                register_publication_root(store, &actor);
                let created = actions
                    .apply(&AgentActionEnvelope {
                        action_id: "fresh-team".into(),
                        actor: actor.clone(),
                        expected_revision: None,
                        action: AgentAction::TeamCreate(
                            serde_json::from_value(serde_json::json!({
                                "name":"Fresh", "mission":"Causal identity"
                            }))
                            .unwrap(),
                        ),
                    })
                    .unwrap();
                assert_eq!(
                    created.status,
                    harness_contract::agent_action::AgentActionStatus::Applied
                );
                let bytes = b"same bytes do not mean same selector";
                let digest = format!("{:x}", Sha256::digest(bytes));
                let legacy = services
                    .artifact_store()
                    .write_bytes(
                        ArtifactWriteDescriptor {
                            media_type: "text/plain".into(),
                            visibility_scope: "session:fresh-session".into(),
                            expected_bytes: None,
                            original_name: None,
                        },
                        bytes,
                    )
                    .await
                    .unwrap();
                let binding: harness_contract::projection::RuntimeActivityBinding =
                    serde_json::from_value(serde_json::json!({
                        "root_execution_id":"fresh-root", "session_id":"fresh-session",
                        "turn_id":"fresh-turn", "root_task_id":"fresh-root", "task_id":"old-task",
                        "activity_id":"old-publication", "agent_run_id":"old-run",
                        "parent_activity_id":"fresh-root", "tool_contract_id":"artifact_publish",
                        "tool_call_id":"old-call",
                        "revision":1, "fence":1, "generation":1
                    }))
                    .unwrap();
                let catalog = tools::ToolCatalog::builtin().with_runtime_tools(vec![
                    tools::RuntimeToolDefinition {
                        name: "artifact_publish".into(),
                        description: Some("Original Runtime evidence access".into()),
                        input_schema: serde_json::json!({"type":"object"}),
                        required_permission: harness_contract::policy::PermissionMode::ReadOnly,
                        effect_resolver: harness_contract::tool::ToolEffectResolverSpec {
                            resolver_id: "runtime.readonly".into(),
                            resolver_version: 1,
                        },
                    }])
                    .unwrap();
                let host = tools::ToolHost::new(
                    "fresh-publication",
                    root.path(),
                    tools::ToolHostSnapshot::new(
                        std::sync::Arc::new(catalog),
                        std::sync::Arc::new(tools::lsp_client::LspRegistry::default()),
                        None,
                    ),
                );
                let descriptor = host
                    .pin_snapshot()
                    .describe_effect("artifact_publish", &serde_json::json!({}));
                assert_eq!(descriptor.tool_id, "artifact_publish");
                assert_eq!(descriptor.effect_kind, ToolEffectKind::Read);
                assert!(!descriptor.uses_network && !descriptor.spawns_process);
                store
                    .append(crate::RuntimeEventInput {
                        stream_id: "session:fresh-session".into(),
                        scope: crate::RuntimeEventScope::Tool,
                        kind: "tool.execution_plan.created".into(),
                        status: Some("planned".into()),
                        actor: Some("conversation_runtime".into()),
                        refs: Vec::new(),
                        payload: serde_json::json!({"plan_id":"old-plan", "tasks":[{
                            "tool_call_id":"old-call", "tool_name":"artifact_publish",
                            "effect":descriptor,
                        }]}),
                    })
                    .unwrap();
                let mut raw = serde_json::json!({
                    "version":1, "tool_call_id":"old-call",
                    "ref_id":"tool://tool-raw-old-call-0123456789abcdef",
                    "selector":legacy.selector, "sha256":legacy.sha256,
                    "byte_count":legacy.bytes, "media_type":"text/plain",
                    "visibility_scope":"session:fresh-session"
                });
                raw[field] = value.clone();
                let mut old = crate::RuntimeEventInput {
                    stream_id: "session:fresh-session".into(),
                    scope: crate::RuntimeEventScope::Tool,
                    kind: "tool.invocation.completed".into(),
                    status: Some("completed".into()),
                    actor: Some("conversation_runtime".into()),
                    refs: Vec::new(),
                    payload: serde_json::json!({"status":"completed", "is_error":false, "governed_plan_id":"old-plan",
                        "tool_name":"artifact_publish", "tool_call_id":"old-call",
                        "full_output_ref":"tool://tool-raw-old-call-0123456789abcdef",
                        "runtime_raw_content_v1":raw}),
                }
                .with_activity_binding(binding)
                .unwrap();
                if field == "legacy" {
                    old.payload
                        .as_object_mut()
                        .unwrap()
                        .remove("runtime_raw_content_v1");
                }
                if !later {
                    store.append(old.clone()).unwrap();
                }
                let artifact = services
                    .publish_authorized_content(bytes, &digest, "text/plain", "fresh-session")
                    .await
                    .unwrap();
                assert_ne!(artifact.selector, legacy.selector);
                assert_eq!(artifact.sha256, legacy.sha256);
                if later {
                    store.append(old).unwrap();
                }
                let committed = actions.apply(&AgentActionEnvelope {
                    action_id:"fresh-commit".into(), actor:actor.clone(), expected_revision:None,
                    action:AgentAction::ArtifactCommit(serde_json::from_value(serde_json::json!({
                        "content_ref":artifact.selector, "kind":"report", "title":"Fresh D", "relates_to":[]
                    })).unwrap()),
                }).unwrap();
                assert_eq!(
                    committed.status,
                    harness_contract::agent_action::AgentActionStatus::Applied
                );
                let reference = committed.changed_refs.first().unwrap();
                let program = actions.project("fresh-program").unwrap();
                let readers = super::super::ResultContentReaders {
                    artifacts: Some(services.artifact_store().as_ref()),
                    history: None,
                    process_inputs: None,
                };
                let mut placeholder = artifact.clone();
                placeholder.media_type = "application/octet-stream".into();
                let metadata = resolve_result_metadata(
                    store,
                    &program,
                    reference,
                    &placeholder,
                    None,
                    readers,
                );
                if later {
                    assert!(
                        metadata.is_err(),
                        "late completion must not be excluded: {field}"
                    );
                    continue;
                }
                let metadata = metadata.unwrap();
                metadata.origin.require_strong().unwrap();
                assert_eq!(metadata.content, artifact);
                assert_eq!(metadata.producers.len(), 1);
                assert_eq!(metadata.producers[0].actor, actor.actor_id);
                let session_cut = metadata
                    .effect_sources
                    .iter()
                    .find(|source| source.stream_id == "session:fresh-session")
                    .unwrap();
                let events = store.list_stream("session:fresh-session").unwrap();
                let old_position = events
                    .iter()
                    .position(|event| {
                        event.kind == "tool.invocation.completed"
                            && event.payload["tool_call_id"] == "old-call"
                    })
                    .unwrap();
                let fact_position = events
                    .iter()
                    .position(|event| event.kind == crate::content_publication::FRESH_OBJECT_KIND)
                    .unwrap();
                let plan_position = events
                    .iter()
                    .position(|event| {
                        event.kind == "tool.execution_plan.created"
                            && event.payload["plan_id"] == "old-plan"
                    })
                    .unwrap();
                assert!(events[plan_position].commit_cursor < events[old_position].commit_cursor);
                assert_eq!(
                    tool_effect_from_store(store, &events[old_position]).unwrap(),
                    Some(descriptor.clone())
                );
                assert_eq!(
                    session_cut.expected_revision,
                    events.last().unwrap().sequence
                );
                let fact = events
                    .iter()
                    .find(|event| event.kind == crate::content_publication::FRESH_OBJECT_KIND)
                    .unwrap();
                assert!(events[old_position].sequence <= fact.payload["precreation_head"].as_u64().unwrap());
                assert_eq!(
                    fact.sequence,
                    fact.payload["precreation_head"].as_u64().unwrap() + 1
                );
                assert!(resolve_result_metadata(
                    store,
                    &program,
                    &legacy.selector,
                    &legacy,
                    None,
                    readers
                )
                .is_err());
                assert_eq!(
                    resolve_result_metadata(
                        store,
                        &program,
                        reference,
                        &placeholder,
                        None,
                        readers
                    )
                    .unwrap()
                    .content,
                    artifact
                );
                let cold_store =
                    std::sync::Arc::new(crate::RuntimeEventStore::from_backend(backend.clone()));
                let cold_metadata = resolve_result_metadata(
                    &cold_store,
                    &program,
                    reference,
                    &placeholder,
                    None,
                    readers,
                )
                .unwrap();
                assert_eq!(cold_metadata.content, artifact);
                assert_eq!(cold_metadata.effect_sources, metadata.effect_sources);
                for (key, invalid) in [
                    ("version", serde_json::json!(2)),
                    ("session_id", serde_json::json!("foreign")),
                    ("precreation_head", serde_json::json!(fact.sequence)),
                    ("precreation_head", serde_json::json!(u64::MAX)),
                    ("unknown", serde_json::json!(true)),
                ] {
                    let mut invalid_events = events.clone();
                    invalid_events[fact_position].payload[key] = invalid;
                    assert!(
                        crate::content_publication::fresh_object_precreation_head(
                            &invalid_events,
                            "fresh-session",
                            &artifact,
                        )
                        .is_err(),
                        "current fresh-object protocol must reject fact {key}"
                    );
                    backend.replace_read("session:fresh-session", Some(invalid_events));
                    assert!(
                        resolve_result_metadata(
                            store,
                            &program,
                            reference,
                            &placeholder,
                            None,
                            readers
                        )
                        .is_err(),
                        "fact {key}"
                    );
                }
                for (key, invalid) in [
                    ("selector", serde_json::json!(legacy.selector)),
                    ("sha256", serde_json::json!("truncated")),
                    ("sha256", serde_json::json!("f".repeat(64))),
                    ("bytes", serde_json::json!(artifact.bytes + 1)),
                    ("media_type", serde_json::json!("application/foreign")),
                    ("durability", serde_json::json!("ephemeral")),
                    ("visibility_scope", serde_json::json!("session:foreign")),
                    ("unknown", serde_json::json!(true)),
                ] {
                    let mut invalid_events = events.clone();
                    invalid_events[fact_position].payload["artifact"][key] = invalid;
                    backend.replace_read("session:fresh-session", Some(invalid_events));
                    assert!(
                        resolve_result_metadata(
                            store,
                            &program,
                            reference,
                            &placeholder,
                            None,
                            readers
                        )
                        .is_err(),
                        "artifact {key}"
                    );
                }
                let mut root_metadata = events.clone();
                root_metadata[old_position].payload["_runtime_activity_binding"]
                    .as_object_mut()
                    .unwrap()
                    .remove("agent_run_id");
                root_metadata[old_position].payload["_runtime_activity_binding"]["activity_id"] =
                    serde_json::json!("activity:execution:fresh-root:owner:old-call");
                for defect in [
                    "missing_plan",
                    "missing_id",
                    "wrong_call",
                    "wrong_tool",
                    "malformed",
                ] {
                    let mut invalid_events = root_metadata.clone();
                    match defect {
                        "missing_plan" => {
                            invalid_events.remove(plan_position);
                        }
                        "missing_id" => {
                            invalid_events[old_position]
                                .payload
                                .as_object_mut()
                                .unwrap()
                                .remove("governed_plan_id");
                        }
                        "wrong_call" => {
                            invalid_events[plan_position].payload["tasks"][0]["tool_call_id"] =
                                serde_json::json!("foreign-call")
                        }
                        "wrong_tool" => {
                            invalid_events[plan_position].payload["tasks"][0]["tool_name"] =
                                serde_json::json!("foreign-tool")
                        }
                        "malformed" => {
                            invalid_events[plan_position].payload["tasks"][0]["effect"] =
                                serde_json::Value::Null
                        }
                        _ => unreachable!(),
                    }
                    backend.replace_read("session:fresh-session", Some(invalid_events));
                    for reader_store in [store, &cold_store] {
                        assert!(
                            resolve_result_metadata(
                                reader_store,
                                &program,
                                reference,
                                &placeholder,
                                None,
                                readers
                            )
                            .is_err(),
                            "descriptor {defect}"
                        );
                    }
                }
                backend.replace_read("session:fresh-session", Some(root_metadata));
                for reader_store in [store, &cold_store] {
                    let unchanged = resolve_result_metadata(
                        reader_store,
                        &program,
                        reference,
                        &placeholder,
                        None,
                        readers,
                    )
                    .unwrap();
                    unchanged.origin.require_strong().unwrap();
                    assert_eq!(unchanged.content, artifact);
                    assert_eq!(unchanged.producers.len(), 1);
                    assert_eq!(unchanged.producers[0].actor, metadata.producers[0].actor);
                    assert_eq!(
                        unchanged.producers[0].execution,
                        metadata.producers[0].execution
                    );
                    assert_eq!(unchanged.producers[0].tool, metadata.producers[0].tool);
                    assert!(unchanged.producers[0].packet.is_none());
                    assert_eq!(unchanged.result_sources, metadata.result_sources);
                    assert_eq!(unchanged.primary_inputs, metadata.primary_inputs);
                    assert_eq!(unchanged.effect_cursor, metadata.effect_cursor);
                    assert_eq!(unchanged.effect_scopes, metadata.effect_scopes);
                    assert_eq!(unchanged.effect_targets, metadata.effect_targets);
                    assert_eq!(unchanged.unscoped_effect, metadata.unscoped_effect);
                    assert_eq!(unchanged.effect_sources, metadata.effect_sources);
                }
                for defect in [
                    "actor",
                    "scope",
                    "status",
                    "schema",
                    "stream",
                    "refs",
                    "transaction",
                    "idempotency",
                    "index",
                    "sequence",
                    "missing",
                    "duplicate",
                    "direct_raw",
                    "direct_reply",
                    "effects",
                    "effects_unknown",
                    "effects_composite",
                    "effects_read_named",
                ] {
                    let mut invalid_events = events.clone();
                    match defect {
                        "actor" => invalid_events[fact_position].actor = Some("foreign".into()),
                        "scope" => invalid_events[fact_position].scope = crate::RuntimeEventScope::Tool,
                        "status" => invalid_events[fact_position].status = Some("unknown".into()),
                        "schema" => invalid_events[fact_position].schema_version = 2,
                        "stream" => invalid_events[fact_position].stream_id = "session:foreign".into(),
                        "refs" => invalid_events[fact_position].refs.clear(),
                        "transaction" => invalid_events[fact_position].transaction_id = "foreign".into(),
                        "idempotency" => invalid_events[fact_position].idempotency_key = None,
                        "index" => invalid_events[fact_position].transaction_index = 1,
                        "sequence" => invalid_events[fact_position].sequence += 1,
                        "missing" => { invalid_events.remove(fact_position); },
                        "duplicate" => invalid_events.push(invalid_events[fact_position].clone()),
                        "direct_raw" => invalid_events[old_position].payload["runtime_raw_content_v1"]["selector"] = serde_json::json!(artifact.selector),
                        "direct_reply" => invalid_events[old_position].payload["output_preview"] = serde_json::json!(serde_json::json!({"status":"published", "content_ref":artifact.selector}).to_string()),
                        "effects" | "effects_unknown" | "effects_composite" | "effects_read_named" => {
                            invalid_events[old_position].payload.as_object_mut().unwrap()
                                .remove("governed_plan_id");
                            invalid_events[old_position].payload["_runtime_activity_binding"].as_object_mut().unwrap().remove("agent_run_id");
                            invalid_events[old_position].payload["_runtime_activity_binding"]["activity_id"] = serde_json::json!("activity:execution:fresh-root:owner:old-call");
                            let tool = match defect {
                                "effects" => "edit",
                                "effects_composite" => "tool_orchestrate",
                                "effects_read_named" => "read_file",
                                _ => "unknown_physical_effect",
                            };
                            invalid_events[old_position].payload["tool_name"] = serde_json::json!(tool);
                            invalid_events[old_position].payload["_runtime_activity_binding"]["tool_contract_id"] = serde_json::json!(tool);
                        },
                        _ => unreachable!(),
                    }
                    backend.replace_read("session:fresh-session", Some(invalid_events));
                    for reader_store in [store, &cold_store] {
                        let rejected = resolve_result_metadata(
                            reader_store,
                            &program,
                            reference,
                            &placeholder,
                            None,
                            readers,
                        )
                        .err()
                        .unwrap_or_else(|| panic!("event {defect}"));
                        if defect.starts_with("effects") {
                            assert_eq!(
                                rejected, "result producer effect has no original governed plan",
                                "event {defect}"
                            );
                        }
                    }
                }
                backend.replace_read("session:fresh-session", None);
                assert_eq!(
                    resolve_result_metadata(
                        store,
                        &program,
                        reference,
                        &placeholder,
                        None,
                        readers
                    )
                    .unwrap()
                    .content,
                    artifact
                );
                store
                    .append(crate::RuntimeEventInput {
                        stream_id: "session:fresh-session".into(),
                        scope: crate::RuntimeEventScope::Session,
                        kind: "test.after_prepare".into(),
                        status: None,
                        actor: None,
                        refs: Vec::new(),
                        payload: serde_json::json!({}),
                    })
                    .unwrap();
                let stale = store.append_transaction(crate::AppendTransactionRequest {
                    transaction_id: "test.stale-fence".into(),
                    expected_streams: metadata.effect_sources,
                    events: vec![crate::RuntimeEventInput {
                        stream_id: "session:fresh-session".into(),
                        scope: crate::RuntimeEventScope::Session,
                        kind: "test.apply".into(),
                        status: None,
                        actor: None,
                        refs: Vec::new(),
                        payload: serde_json::json!({}),
                    }
                    .into()],
                });
                assert!(matches!(
                    stale,
                    Err(crate::RuntimeEventStoreError::StaleRevision { .. })
                ));
            }
        }
    }

    #[tokio::test]
    async fn r1_proof_legacy_raw_metadata_never_mints_strong_origin() {
        let (_workspace, services, original) =
            crate::agentic::execution::reviewed_goal_fixture::native_raw_owner_fixture_for_test()
                .await;
        let history = services.session_history_reader().unwrap();
        let readers = super::super::ResultContentReaders {
            artifacts: Some(services.artifact_store().as_ref()),
            history: Some(history.as_ref()),
            process_inputs: None,
        };
        let events = services
            .event_store()
            .list_stream(&original.stream_id)
            .unwrap();
        let task = result_tool_plan(&events, &original).unwrap();
        let mut cuts = std::collections::BTreeMap::new();
        let strong = original_tool_body(
            services.event_store(),
            readers,
            &original,
            task,
            &mut cuts,
            None,
        )
        .unwrap()
        .unwrap();
        assert!(
            matches!(&strong.origin, ResultOrigin::StrongRaw { anchors } if anchors.len() == 1)
        );
        assert!(strong.origin.require_strong().is_ok());
        let mut legacy_fixture = original.clone();
        legacy_fixture
            .payload
            .as_object_mut()
            .unwrap()
            .remove("runtime_raw_content_v1");
        assert!(legacy_fixture
            .payload
            .get("output_ref")
            .is_none_or(serde_json::Value::is_null));
        let recovered = original_tool_body(
            services.event_store(),
            readers,
            &legacy_fixture,
            task,
            &mut cuts,
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(recovered.bytes, strong.bytes);
        assert!(observed_raw_selector_conflict(
            readers,
            &original,
            &recovered.content.as_ref().unwrap().selector
        ));
        assert!(matches!(
            &recovered.origin,
            ResultOrigin::LegacyWeak | ResultOrigin::LegacyIncomplete
        ));
        let error = recovered.origin.require_strong().unwrap_err();
        assert!(!error.starts_with("review result has no Runtime-resolved producer:"));
        assert_eq!(
            services
                .event_store()
                .list_stream(&original.stream_id)
                .unwrap(),
            events
        );
        let no_session = super::super::ResultContentReaders {
            artifacts: readers.artifacts,
            history: None,
            process_inputs: None,
        };
        assert!(original_tool_body(
            services.event_store(),
            no_session,
            &legacy_fixture,
            task,
            &mut cuts,
            None
        )
        .is_err());
        assert_eq!(
            original_tool_body(
                services.event_store(),
                no_session,
                &original,
                task,
                &mut cuts,
                None
            )
            .unwrap()
            .unwrap()
            .bytes,
            strong.bytes
        );
        for (field, value) in [
            ("version", serde_json::json!(2)),
            ("tool_call_id", serde_json::json!("foreign-call")),
            (
                "ref_id",
                serde_json::json!("tool://tool-raw-foreign-0123456789abcdef"),
            ),
            ("visibility_scope", serde_json::json!("session:foreign")),
            (
                "sha256",
                serde_json::json!(format!("sha256:{}", "0".repeat(64))),
            ),
            ("byte_count", serde_json::json!(strong.bytes.len() + 1)),
            ("selector", serde_json::json!("artifact://missing-raw-body")),
            ("media_type", serde_json::json!("application/foreign")),
        ] {
            let mut invalid_fixture = original.clone();
            invalid_fixture.payload["runtime_raw_content_v1"][field] = value;
            assert!(
                original_tool_body(
                    services.event_store(),
                    readers,
                    &invalid_fixture,
                    task,
                    &mut cuts,
                    None
                )
                .is_err(),
                "{field}"
            );
        }
        let mut presentation_fixture = original.clone();
        presentation_fixture.payload["output_ref"] = serde_json::json!({
            "sha256":"f".repeat(64), "byte_count":1,
        });
        presentation_fixture.payload["output_preview"] =
            serde_json::json!("different presentation");
        presentation_fixture.payload["output_byte_count"] = serde_json::json!(22);
        assert_eq!(
            original_tool_body(
                services.event_store(),
                readers,
                &presentation_fixture,
                task,
                &mut cuts,
                None
            )
            .unwrap()
            .unwrap()
            .bytes,
            strong.bytes
        );
        let mut staged_conflict = original.clone();
        let foreign = format!(
            "tool://tool-raw-{}-{}",
            original.payload["tool_call_id"].as_str().unwrap(),
            "f".repeat(64)
        );
        staged_conflict.payload["full_output_ref"] = serde_json::json!(foreign);
        staged_conflict.payload["runtime_raw_content_v1"]["ref_id"] =
            staged_conflict.payload["full_output_ref"].clone();
        assert!(original_tool_body(
            services.event_store(),
            readers,
            &staged_conflict,
            task,
            &mut cuts,
            None
        )
        .is_err());
        services.shutdown_maintenance().await;
    }

    #[test]
    fn legacy_input_bridge_requires_verified_full_input_under_both_hashes() {
        let input = serde_json::json!({"media_type": "原始输入".repeat(128)});
        let encoded = serde_json::to_string(&input).unwrap();
        let mut owner = DurableRuntimeEvent {
            event_id: "unit-input-identity".into(),
            stream_id: "unit:legacy-input".into(),
            sequence: 1,
            scope: crate::RuntimeEventScope::Tool,
            kind: "unit.input_identity".into(),
            status: None,
            actor: None,
            refs: Vec::new(),
            payload: serde_json::json!({
                "tool_call_id":"legacy-call",
                "input_hash":crate::tool_invocation::stable_hash(&encoded),
            }),
            created_at_ms: 0,
            commit_cursor: 1,
            transaction_id: "unit-input-identity".into(),
            transaction_index: 0,
            schema_version: 1,
            idempotency_key: None,
        };
        let task = serde_json::json!({"normalized_input":input});
        let payload = serde_json::json!({
            "input_hash":format!("{:016x}", model_protocol::fingerprint::stable_hash_bytes(encoded.as_bytes())),
            "source_evidence_ref":"runtime-tool:legacy-call",
            "native_staged_artifact":false,
        });
        for preview in [
            None,
            Some("partial"),
            Some(encoded.as_str()),
            Some("different input"),
        ] {
            owner.payload["input_preview"] = serde_json::to_value(preview).unwrap();
            assert!(raw_input_matches_owner(&payload, &owner, Some(&task)));
            let mut wrong = payload.clone();
            wrong["input_hash"] = serde_json::json!("0000000000000000");
            assert!(!raw_input_matches_owner(&wrong, &owner, Some(&task)));
        }
        owner.payload["input_preview"] = serde_json::Value::Null;
        assert!(!raw_input_matches_owner(&payload, &owner, None));
        let wrong_task = serde_json::json!({"normalized_input":{"media_type":"different"}});
        assert!(!raw_input_matches_owner(
            &payload,
            &owner,
            Some(&wrong_task)
        ));
        owner.payload["input_preview"] = serde_json::json!(encoded);
        assert!(raw_input_matches_owner(&payload, &owner, None));
        let mut wrong_source = payload.clone();
        wrong_source["source_evidence_ref"] = serde_json::json!("runtime-tool:foreign");
        assert!(!raw_input_matches_owner(&wrong_source, &owner, Some(&task)));
        let mut staged = payload.clone();
        staged["native_staged_artifact"] = serde_json::json!(true);
        assert!(!raw_input_matches_owner(&staged, &owner, Some(&task)));
        staged["input_hash"] = owner.payload["input_hash"].clone();
        assert!(raw_input_matches_owner(&staged, &owner, Some(&task)));
    }

    #[tokio::test]
    async fn r1_proof_effect_metadata_independently_recovers_original_result_producers() {
        use harness_contract::agent::{
            AgentBindingSnapshot, AgentCapability, AgentDefinitionId, AgentTaskIntent,
            AgenticExecutionBinding, AgenticExecutionFocus, DefinitionScope, RevisionSelector,
        };
        use harness_contract::agent_action::{AgentAction, AgentActionEnvelope, AgentActorKind};
        use harness_contract::context::{ArtifactWriteDescriptor, ChildExecutionBudgetReservation};
        use harness_contract::execution::ExecutionIdentity;
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
            ExecutionParentBinding,
        };
        use harness_contract::policy::PermissionMode;
        use std::sync::Arc;
        let services = RuntimeServices::in_memory().unwrap();
        let store = Arc::clone(services.event_store());
        let readers = super::super::ResultContentReaders {
            artifacts: Some(services.artifact_store().as_ref()),
            history: None,
            process_inputs: None,
        };
        let actions = crate::AgentActionService::new(Arc::clone(&store))
            .with_artifact_store(Arc::clone(services.artifact_store()));
        let root_actor: AgentActorBinding = serde_json::from_value(serde_json::json!({
            "objective_id":"metadata-objective", "program_id":"metadata-program",
            "session_id":"metadata-session", "turn_id":"metadata-turn",
            "root_execution_id":"metadata-root", "actor_id":"metadata-root-actor",
            "kind":"root", "objective_summary":"Authenticate original result producers",
            "model_lease":"fixture", "permission_ceiling":PermissionMode::WorkspaceWrite
        }))
        .unwrap();
        register_publication_root(&store, &root_actor);
        let apply = |action_id: &str, actor: AgentActorBinding, action: AgentAction| {
            let observation = actions
                .apply(&AgentActionEnvelope {
                    action_id: action_id.into(),
                    actor,
                    expected_revision: None,
                    action,
                })
                .unwrap();
            assert_eq!(
                observation.status,
                harness_contract::agent_action::AgentActionStatus::Applied,
                "{observation:?}"
            );
            observation.changed_refs.first().cloned()
        };
        let team = apply(
            "metadata-team",
            root_actor.clone(),
            AgentAction::TeamCreate(
                serde_json::from_value(
                    serde_json::json!({"name":"Evidence","mission":"Read original content"}),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let worker = apply("metadata-worker", root_actor.clone(), AgentAction::AgentInvite(
            serde_json::from_value(serde_json::json!({
                "team_ref":team, "role":"producer", "mission":"Read original content", "required_capabilities":["read"]
            })).unwrap()
        )).unwrap();
        let task = apply(
            "metadata-task",
            root_actor.clone(),
            AgentAction::TaskPublish(
                serde_json::from_value(serde_json::json!({
                    "team_ref":team, "title":"Original result", "objective":"Read and publish",
                    "acceptance":"Source-backed result", "required_capabilities":["read"]
                }))
                .unwrap(),
            ),
        )
        .unwrap();
        let content = services
            .artifact_store()
            .write_bytes(
                ArtifactWriteDescriptor {
                    media_type: "text/plain".into(),
                    visibility_scope: "session:metadata-session".into(),
                    expected_bytes: None,
                    original_name: None,
                },
                b"original content",
            )
            .await
            .unwrap();
        let root_ref = apply("metadata-root-result", root_actor.clone(), AgentAction::ArtifactCommit(
            serde_json::from_value(serde_json::json!({
                "content_ref":content.selector, "kind":"report", "title":"Root result", "relates_to":[]
            })).unwrap()
        )).unwrap();
        let program = actions.project("metadata-program").unwrap();
        let root_metadata =
            resolve_result_metadata(&store, &program, &root_ref, &content, None, readers).unwrap();
        assert!(authenticate_publication_task_link(
            &store,
            &program,
            &program.artifacts[&root_ref],
            &task,
            &mut std::collections::BTreeMap::new()
        )
        .is_err());
        assert_eq!(root_metadata.producers.len(), 1);
        assert_eq!(root_metadata.producers[0].actor, root_actor.actor_id);
        assert_eq!(
            root_metadata.producers[0].execution,
            "root-execution:metadata-root"
        );
        assert!(root_metadata.effect_cursor.is_none());
        let graph_id = "metadata-agent-graph";
        let node_id = "metadata-agent-node";
        let run_id = "run:metadata-agent-graph";
        let mut binding_request = crate::AgentBindingRequest::new(
            AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/execute").unwrap(),
            RevisionSelector::LatestApprovedStable,
            format!("instance:{run_id}"),
            "metadata-session",
            &task,
        );
        binding_request.team_id = Some(team.clone());
        binding_request.granted_capabilities = vec![AgentCapability::Read];
        binding_request.allowed_tool_contract_refs = vec!["read_file".into()];
        let compiled = services.compile_agent_binding(binding_request).unwrap();
        let snapshot: AgentBindingSnapshot = compiled.snapshot;
        let identity = ExecutionIdentity::for_task_graph(
            "metadata-principal",
            services.path_identity_resolver().workspace_id(),
            "metadata-mission",
            &task,
            "metadata-session",
            "metadata-turn",
            graph_id,
        )
        .unwrap();
        let identity =
            ExecutionIdentity::for_team_node(&identity, &team, "metadata-team-node").unwrap();
        let identity = ExecutionIdentity::for_agent_node(&identity, run_id, node_id).unwrap();
        let intent = AgentTaskIntent {
            selected_agent_id: None,
            definition_ref: None,
            granted_capabilities: vec![AgentCapability::Read],
            principal_id: "metadata-principal".into(),
            source_turn_id: "metadata-turn".into(),
            run_id: run_id.into(),
            task_id: task.clone(),
            root_task_id: task.clone(),
            parent_task_id: None,
            session_id: "metadata-session".into(),
            mission_id: "metadata-mission".into(),
            team_id: Some(team.clone()),
            graph_id: graph_id.into(),
            node_id: node_id.into(),
            attempt: 1,
            expected_graph_revision: 1,
            objective: "Read original content".into(),
            required_acceptance: Default::default(),
            output_acceptance: vec![],
            acceptance: vec![],
            constraints: vec![],
            context_refs: vec![],
            agentic_binding: Some(AgenticExecutionBinding {
                program_id: "metadata-program".into(),
                agent_id: worker.clone(),
                membership_id: AgenticProgramProjection::membership_id(&worker, &team),
                team_id: team.clone(),
                task_team_id: team.clone(),
                source_spec_revision: program.revision,
                task_purpose: None,
                focus: AgenticExecutionFocus::TaskExecute {
                    task_ref: task.clone(),
                },
            }),
            evidence_refs: vec![],
            resource_scopes: vec![],
            allowed_tools: vec!["read_file".into()],
            allowed_skills: vec![],
            permission_ceiling: PermissionMode::ReadOnly,
            model_lease: "fixture".into(),
            budget_lease: ChildExecutionBudgetReservation::single(
                "metadata-budget",
                &worker,
                &task,
                1024,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            managed_invocation: None,
            idempotency_key: "metadata-packet".into(),
        };
        let packet = snapshot.compile_task_packet(intent, identity).unwrap();
        let commits = crate::ExecutionCommitService::new(Arc::clone(&store));
        let mut graph = ExecutionGraph::new("Original result metadata fixture");
        graph.id = graph_id.into();
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: "metadata-session".into(),
            turn_id: "metadata-turn".into(),
            root_task_id: task.clone(),
            task_id: task.clone(),
            generation: 1,
        });
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            "agent_task",
            serde_json::to_string(&packet).unwrap(),
        );
        node.id = node_id.into();
        graph.nodes.push(node);
        commits.register_graph(graph).unwrap();
        let mut agent_actor = root_actor.clone();
        agent_actor.kind = AgentActorKind::Agent;
        agent_actor.actor_id = worker.clone();
        agent_actor.agent_id = Some(worker.clone());
        agent_actor.team_id = Some(team.clone());
        agent_actor.execution_id = Some(graph_id.into());
        apply(
            "metadata-claim",
            agent_actor.clone(),
            AgentAction::TaskClaim(
                serde_json::from_value(serde_json::json!({"task_ref":task})).unwrap(),
            ),
        );
        let workspace = tempfile::tempdir().unwrap();
        let path = workspace.path().join("original.txt");
        std::fs::write(&path, "physically acquired source").unwrap();
        let host = tools::ToolHost::builtin("metadata-reader", workspace.path())
            .with_authorization_lease_verifier(Arc::new(|_| true));
        let lease = host.pin_snapshot();
        let definitions = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| definition.name == "read_file")
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        let step = Arc::new(crate::step_snapshot::BoundStep::bind(
            serde_json::from_value(serde_json::json!({
                "step_id":"metadata-read-step", "session_id":"metadata-session", "turn_id":"metadata-turn",
                "execution_id":graph_id, "actor_ref":packet.agent_id(), "run_ref":run_id, "attempt":1,
                "build_ref":"original-metadata-fixture"
            })).unwrap(), lease, &definitions, &serde_json::json!({}), &serde_json::json!({}),
            std::collections::BTreeMap::from([("worker_run".into(), packet.run_id().to_string())]), i64::MAX,
        ).unwrap());
        let binding = crate::step_snapshot::StepCallBinding::process(
            step.clone(),
            packet.run_id(),
            1,
            "metadata-read",
        );
        assert!(matches!(&binding.origin.source,
            harness_contract::step::CallSource::Process { worker_id, sequence }
            if worker_id == packet.run_id() && *sequence == 1));
        assert_eq!(
            binding.snapshot.identity.actor_ref.as_deref(),
            Some(packet.agent_id())
        );
        assert_eq!(
            binding.snapshot.identity.execution_id.as_deref(),
            Some(packet.graph_id())
        );
        assert_ne!(
            packet.agent_id(),
            packet.assignment.execution_identity.principal_id()
        );
        let input = serde_json::json!({"path":path}).to_string();
        let value = step
            .validate_call(
                &binding.origin,
                "metadata-session",
                "metadata-read",
                "read_file",
                &input,
                1,
            )
            .unwrap();
        let descriptor = step.lease().describe_effect("read_file", &value);
        let effective =
            crate::AuthorizationNegotiator::compile_effective_descriptor(&descriptor, &input);
        let lease = harness_contract::policy::AuthorizationLease {
            lease_id: "metadata-read-lease".into(),
            principal_id: "metadata-principal".into(),
            parent_lease_id: None,
            capability: descriptor.tool_id.clone(),
            scopes: descriptor.scopes.clone(),
            ceiling: PermissionMode::ReadOnly,
            issued_at_ms: 0,
            expires_at_ms: u64::MAX,
            max_uses: 1,
            remaining_uses: 1,
            idempotency_key: "metadata-read".into(),
            policy_revision: 1,
            effect_descriptor_hash: descriptor.descriptor_hash.clone(),
            signature: "fixture-verifier".into(),
            status: harness_contract::policy::AuthorizationLeaseStatus::Active,
        };
        let assessment = harness_contract::policy::CapabilityAssessment {
            assessment_id: "metadata-assessment".into(),
            capability: descriptor.tool_id.clone(),
            effect: effective.descriptor.assessment.clone(),
            requested_scopes: effective.descriptor.scopes.clone(),
            required_mode: PermissionMode::ReadOnly,
            active_ceiling: PermissionMode::ReadOnly,
            parent_ceiling: PermissionMode::ReadOnly,
            risk: harness_contract::policy::RiskLevel::Low,
            path: harness_contract::policy::AuthorizationPath::PolicyAutoGrant,
            lease: None,
            gap: None,
            evidence_refs: vec![],
            assessed_at_ms: 0,
        };
        let authorization = crate::ToolPolicy
            .authorize(&effective, &assessment, "metadata-read", lease, 1)
            .unwrap()
            .authorization;
        let mut request = crate::RuntimeToolExecutionRequest::from_tool_request(
            &crate::tool_dispatch::ToolRequest {
                tool_use_id: "metadata-read".into(),
                tool_name: "read_file".into(),
                input: input.clone(),
                depends_on: vec![],
            },
        );
        request.session_id = Some("metadata-session".into());
        request.policy_revision = 1;
        request.observation_wave_sequence = 1;
        request.idempotency_key = binding.effect_idempotency_key();
        request.parent_execution = Some(ExecutionParentBinding {
            execution_id: graph_id.into(),
            node_id: node_id.into(),
        });
        request.parent_execution_attempt = Some(packet.attempt);
        request.compiled_invocation = Some(
            binding
                .prepare_invocation(Some(host.pin_snapshot()), "read_file", &value)
                .unwrap(),
        );
        request.step_binding = Some(binding);
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
            .unwrap();
        let scope = crate::bound_tool_batch::admitted_effect_scope(
            &request,
            &descriptor,
            Some(host.pin_snapshot()),
            &resolver,
        )
        .unwrap();
        assert_eq!(scope.version, 2);
        scope.validate_purpose_mapping(&descriptor).unwrap();
        let mut legacy_scope = serde_json::to_value(&scope).unwrap();
        legacy_scope["version"] = serde_json::json!(1);
        assert!(require_original_effect_scope(&legacy_scope, &descriptor).is_err());
        let mut missing_purpose = serde_json::to_value(&scope).unwrap();
        missing_purpose.as_object_mut().unwrap().remove("scope_purposes");
        assert!(require_original_effect_scope(&missing_purpose, &descriptor).is_err());
        let output = step
            .lease()
            .execute(&authorization, "read_file", &value)
            .unwrap();
        assert!(output.contains("physically acquired source"));
        commits
            .commit_bound_readonly_tool_receipt(
                &request,
                &crate::RuntimeToolExecutionOutcome {
                    tool_use_id: "metadata-read".into(),
                    tool_name: "read_file".into(),
                    status: crate::RuntimeToolExecutionStatus::Executed,
                    category: crate::ToolSafetyCategory::ReadOnly,
                    output: Some(output),
                    error: None,
                    evidence_ref: "metadata-original-read".into(),
                    observed_evidence: vec![],
                },
                &descriptor,
                &scope,
            )
            .unwrap();
        let result_ref = apply("metadata-agent-result", agent_actor.clone(), AgentAction::ArtifactCommit(
            serde_json::from_value(serde_json::json!({
                "content_ref":content.selector, "kind":"report", "title":"Agent result", "relates_to":[task]
            })).unwrap()
        )).unwrap();
        let program = actions.project("metadata-program").unwrap();
        let receipt_stream = format!(
            "execution-agent-receipts:{graph_id}:{node_id}:{}",
            packet.attempt
        );
        let receipt_revision = store.stream_revision(&receipt_stream).unwrap();
        let discovered = resolve_result(&services, &root_actor, &program, &result_ref)
            .await
            .unwrap();
        let input_cuts = super::super::policy::actual_input_snapshot(
            &store,
            &program,
            &discovered.input_boundaries,
            readers,
            None,
        )
        .unwrap()
        .sources;
        let program_stream = format!("agentic-program:{}", program.program_id);
        assert_eq!(input_cuts.get(&program_stream), Some(&program.revision));
        assert_eq!(input_cuts.get(&receipt_stream), Some(&receipt_revision));
        assert!(input_cuts.iter().all(|(stream_id, expected_revision)| {
            discovered.effect_sources.iter().any(|source| {
                source.stream_id == *stream_id && source.expected_revision == *expected_revision
            })
        }));
        let metadata = resolve_result_metadata(
            &store,
            &program,
            &result_ref,
            &content,
            Some(&input_cuts),
            readers,
        )
        .unwrap();
        let mut missing_program_cut = input_cuts.clone();
        assert_eq!(
            missing_program_cut.remove(&program_stream),
            Some(program.revision)
        );
        let error = resolve_result_metadata(
            &store,
            &program,
            &result_ref,
            &content,
            Some(&missing_program_cut),
            readers,
        )
        .err()
        .expect("missing original Program input cut must be rejected");
        assert!(
            error.contains("actual input source cut is missing or changed")
                && error.contains(&program_stream)
                && error.contains("expected=None")
                && error.contains("allow_history_growth=false"),
            "{error}"
        );
        assert_eq!(metadata.kind, GoalResultKind::Artifact);
        assert_eq!(metadata.producers.len(), 1);
        assert_eq!(metadata.producers[0].actor, worker);
        assert_eq!(
            metadata.producers[0].execution,
            format!("agent-run:{run_id}")
        );
        assert_eq!(metadata.producers[0].packet.as_ref().unwrap(), &packet);
        assert!(metadata.effect_sources.iter().any(|source| source.stream_id
            == format!(
                "execution-agent-receipts:{graph_id}:{node_id}:{}",
                packet.attempt
            )
            && source.expected_revision > 0));
        assert!(metadata
            .effect_sources
            .iter()
            .any(|source| source.stream_id == graph_id));
        let mut wrong = program.clone();
        wrong.artifacts.get_mut(&result_ref).unwrap().committed_by = "forged-producer".into();
        assert!(
            resolve_result_metadata(&store, &wrong, &result_ref, &content, None, readers).is_err()
        );
        let mut wrong = program.clone();
        wrong
            .artifacts
            .get_mut(&result_ref)
            .unwrap()
            .claim_execution_id = Some("foreign-graph".into());
        assert!(
            resolve_result_metadata(&store, &wrong, &result_ref, &content, None, readers).is_err()
        );
        let wrong_content = ArtifactRef::durable(
            "artifact://foreign",
            &content.sha256,
            content.bytes,
            &content.media_type,
            &content.visibility_scope,
        );
        assert!(resolve_result_metadata(
            &store,
            &program,
            &result_ref,
            &wrong_content,
            None,
            readers
        )
        .is_err());
        assert!(metadata
            .producers
            .iter()
            .all(|producer| producer.execution != "forged-producer"));
        assert!(metadata.effect_cursor.is_none());
        for generation in [0, 2] {
            let mut wrong = program.clone();
            wrong
                .artifacts
                .get_mut(&result_ref)
                .unwrap()
                .claim_generation = Some(generation);
            let error =
                resolve_result_metadata(&store, &wrong, &result_ref, &content, None, readers)
                    .err()
                    .expect("foreign claim generation must be rejected");
            assert_eq!(
                error,
                "result publication does not match its original physical packet/claim"
            );
        }
        assert!(super::super::effect_observation_refs(
            &metadata,
            &[],
            Some(&["borrowed-observation".into()])
        )
        .is_err());
        apply(
            "metadata-one-shot",
            agent_actor,
            AgentAction::TaskSubmit(
                serde_json::from_value(serde_json::json!({
                    "task_ref":task, "artifact_refs":[], "evidence_refs":[content.selector],
                    "deliverable":{"block_index":0, "kind":"report", "title":"One-shot result",
                        "resolved_content_ref":content.selector}
                }))
                .unwrap(),
            ),
        );
        let program = actions.project("metadata-program").unwrap();
        let one_shot = program.tasks[&task].artifact_refs.first().unwrap();
        let metadata =
            resolve_result_metadata(&store, &program, one_shot, &content, None, readers).unwrap();
        assert_eq!(metadata.producers.len(), 1);
        assert_eq!(metadata.producers[0].actor, worker);
        assert_eq!(metadata.producers[0].packet.as_ref().unwrap(), &packet);
        assert_eq!(metadata.kind, GoalResultKind::Artifact);
    }

    #[test]
    fn a_refusal_names_the_receipt_the_selector_and_the_next_step() {
        // V27-M4, the structural acceptance: run one30 was told six words -
        // "artifact does not exist" - about a receipt it had really cited, and
        // spent its window concluding the step was blocked. Every refusal on
        // this path must name what it could not read and how to read it.
        let unresolved = unresolvable_receipt_error(
            "objective_review result",
            "artifact://9ecebe69",
            "sha256:9ecebe69",
            "content missing",
            "cite the result ref the Task returned from task_submit, or read it with evidence_retrieve",
        );
        assert!(unresolved.contains("artifact://9ecebe69"), "{unresolved}");
        assert!(unresolved.contains("sha256:9ecebe69"), "{unresolved}");
        assert!(unresolved.contains("evidence_retrieve"), "{unresolved}");
        assert!(unresolved.contains("content missing"), "{unresolved}");

        let unreadable = unreadable_receipt_error(
            "objective_review result",
            "artifact://9ecebe69",
            "sha256:9ecebe69",
            "io error",
            "read it with evidence_retrieve instead of re-issuing the call",
        );
        assert!(unreadable.contains("artifact://9ecebe69"), "{unreadable}");
        assert!(unreadable.contains("sha256:9ecebe69"), "{unreadable}");
        assert!(unreadable.contains("evidence_retrieve"), "{unreadable}");

        let unauthenticated = unauthenticated_receipt_error("tool://42", "no session");
        assert!(unauthenticated.contains("tool://42"), "{unauthenticated}");
        assert!(unauthenticated.contains("no session"), "{unauthenticated}");
    }

    #[test]
    fn historical_observations_cannot_reconstruct_scope_v2_purpose() {
        let descriptor = tools::ToolHost::builtin("historical-purpose", std::env::temp_dir())
            .pin_snapshot()
            .describe_effect(
                "edit",
                &serde_json::json!({
                    "action":"write", "path":"file.txt", "content":"changed"
                }),
            );
        for value in [
            serde_json::Value::Null,
            serde_json::json!({"version":1,"descriptor_hash":descriptor.descriptor_hash,
                "complete":true,"targets":[]}),
            serde_json::json!({"version":2,"descriptor_hash":descriptor.descriptor_hash,
                "complete":true,"targets":[]}),
        ] {
            assert!(require_original_effect_scope(&value, &descriptor).is_err());
        }
    }
    fn purpose_projection_fixture() -> (
        ToolEffectDescriptor,
        harness_contract::goal::ProofEffectScope,
        ResolvedReviewResult,
    ) {
        use harness_contract::context::{
            EvidenceCoverageKind, WorkspaceAccessMode, WorkspaceObjectKind,
        };
        let host = tools::ToolHost::builtin("purpose-projection", std::env::temp_dir());
        let resolution = host.pin_snapshot().describe_effect_resolution(
            "edit",
            &serde_json::json!({
                "action":"write", "path":"review-effect.txt", "content":"business result"
            }),
        );
        let descriptor = resolution.descriptor;
        let targets = descriptor
            .scopes
            .iter()
            .map(|declared| {
                let path = declared.target.as_deref().unwrap();
                let directory = path == "."
                    || resolution.scope_purposes.iter().any(|entry| {
                        entry.declared_scope == *declared
                        && entry.purpose
                            == harness_contract::goal::ProofEffectPurpose::AutoCheckpointMaintenance
                    });
                let write =
                    declared.operation == harness_contract::policy::PermissionOperation::Write;
                harness_contract::goal::ProofEffectTarget {
                    path: harness_contract::context::WorkspacePathIdentity {
                        workspace_id: "workspace".into(),
                        repository_id: "repository".into(),
                        workspace_relative_path: path.into(),
                        repository_relative_path: path.into(),
                        object_kind: if directory {
                            WorkspaceObjectKind::Directory
                        } else {
                            WorkspaceObjectKind::File
                        },
                        observed_revision_or_digest: None,
                    },
                    declared_scope: declared.clone(),
                    access_mode: if write {
                        WorkspaceAccessMode::Write
                    } else {
                        WorkspaceAccessMode::Read
                    },
                    coverage: if write {
                        EvidenceCoverageKind::WriteEffect
                    } else {
                        EvidenceCoverageKind::ScopedContent
                    },
                    exact: !directory,
                    physical_identity: Some(format!("physical:{path}")),
                    alias_checked: true,
                }
            })
            .collect();
        let scope = harness_contract::goal::ProofEffectScope {
            version: 2,
            descriptor_hash: descriptor.descriptor_hash.clone(),
            complete: true,
            targets,
            scope_purposes: resolution.scope_purposes,
        };
        let resolved = ResolvedReviewResult {
            origin: ResultOrigin::Strong,
            result_sources: Vec::new(),
            reference: "result".into(),
            content: ArtifactRef::durable(
                "artifact://result",
                "sha256:result",
                1,
                "text/plain",
                "public",
            ),
            kind: GoalResultKind::Content,
            artifact_kinds: BTreeSet::new(),
            producers: Vec::new(),
            effect_cursor: None,
            effect_sources: Vec::new(),
            effect_scopes: Vec::new(),
            effect_targets: Vec::new(),
            unscoped_effect: false,
            primary_inputs: Vec::new(),
            input_boundaries: Vec::new(),
            process_runs: Vec::new(),
        };
        (descriptor, scope, resolved)
    }

    #[test]
    fn own869_runtime_control_is_not_a_workspace_business_effect() {
        use harness_contract::policy::{PermissionOperation, PermissionResource};
        use harness_contract::tool::{ToolApprovalClass, ToolIdempotency, ToolPermissionMode};
        let (mut descriptor, _, _) = purpose_projection_fixture();
        descriptor.tool_id = "task_claim".into();
        descriptor.effect_kind = ToolEffectKind::Write;
        descriptor.idempotency = ToolIdempotency::IdempotentWithKey;
        descriptor.required_permission = ToolPermissionMode::ReadOnly;
        descriptor.approval_class = ToolApprovalClass::None;
        descriptor.scopes = vec![PermissionScope {
            resource: PermissionResource::Tool,
            operation: PermissionOperation::Control,
            target: Some("task_claim".into()),
        }];
        assert!(runtime_control_descriptor(&descriptor));
        for field in [
            "file",
            "network",
            "process",
            "system",
            "target",
            "permission",
            "name",
        ] {
            let mut invalid = descriptor.clone();
            match field {
                "file" => invalid.scopes[0].resource = PermissionResource::File,
                "network" => invalid.uses_network = true,
                "process" => invalid.spawns_process = true,
                "system" => invalid.mutates_system = true,
                "target" => invalid.scopes[0].target = Some("other".into()),
                "permission" => invalid.required_permission = ToolPermissionMode::DangerFullAccess,
                "name" => invalid.tool_id = "unknown_control".into(),
                _ => unreachable!(),
            }
            assert!(!runtime_control_descriptor(&invalid), "{field}");
        }
    }

    #[test]
    fn business_projection_preserves_raw_scope_and_excludes_only_authenticated_maintenance() {
        let (descriptor, scope, mut resolved) = purpose_projection_fixture();
        let original = serde_json::to_value(&scope).unwrap();
        merge_authenticated_result_effect(&mut resolved, &descriptor, &scope, 12).unwrap();
        assert_eq!(resolved.effect_targets.len(), 1);
        assert_eq!(
            resolved.effect_targets[0].path.workspace_relative_path,
            "review-effect.txt"
        );
        assert_eq!(resolved.effect_cursor, Some(12));
        assert_eq!(serde_json::to_value(&scope).unwrap(), original);
        assert_eq!(scope.targets.len(), descriptor.scopes.len());
        assert!(scope.targets.iter().any(|target| target.path.object_kind
            == harness_contract::context::WorkspaceObjectKind::Directory));
    }

    #[test]
    fn business_directory_unknown_incomplete_and_missing_purpose_are_not_strong_results() {
        let (descriptor, scope, _) = purpose_projection_fixture();
        let mut directory_business = scope.clone();
        for entry in &mut directory_business.scope_purposes {
            if entry.purpose
                == harness_contract::goal::ProofEffectPurpose::AutoCheckpointMaintenance
            {
                entry.purpose = harness_contract::goal::ProofEffectPurpose::BusinessEffect;
            }
        }
        let mut incomplete = scope.clone();
        incomplete.complete = false;
        let mut missing = scope.clone();
        missing.scope_purposes.pop();
        for candidate in [directory_business, incomplete, missing] {
            let (_, _, mut resolved) = purpose_projection_fixture();
            assert!(
                merge_authenticated_result_effect(&mut resolved, &descriptor, &candidate, 12)
                    .is_err()
            );
        }
        let mut unknown = descriptor.clone();
        unknown.effect_kind = ToolEffectKind::Unknown;
        let (_, _, mut resolved) = purpose_projection_fixture();
        assert!(merge_authenticated_result_effect(&mut resolved, &unknown, &scope, 12).is_err());
    }

    #[test]
    fn root_owner_keeps_original_binding_revision_distinct_from_admission_graph_revision() {
        let store = crate::RuntimeEventStore::for_test();
        let mut event = store.append(crate::RuntimeEventInput {
            stream_id:"execution-effect:root-key".into(), scope:crate::RuntimeEventScope::ExecutionNode,
            kind:"execution.read.intent".into(), status:Some("inflight".into()), actor:Some("governed_tool".into()), refs:vec![],
            payload:serde_json::json!({
                "root_effect_context":{"execution_id":"graph","session_id":"session","turn_id":"turn"},
                "root_effect_owner":{"binding":{
                    "root_execution_id":"graph","session_id":"session","turn_id":"turn",
                    "root_task_id":"root-task","task_id":"task","activity_id":"activity:graph:node",
                    "node_id":"node","revision":3,"fence":3,"generation":1
                },"attempt":2,"graph_revision":7},
                "replay":{"parent_execution":null}
            }),
        }).unwrap();
        let (binding, attempt, revision) = original_root_effect_owner(&event).unwrap();
        assert_eq!((binding.revision, attempt, revision), (3, 2, 7));
        let original = event.payload.clone();
        for field in ["binding", "attempt", "graph_revision"] {
            event.payload = original.clone();
            event.payload["root_effect_owner"]
                .as_object_mut()
                .unwrap()
                .remove(field);
            assert!(original_root_effect_owner(&event).is_err(), "{field}");
        }
        event.payload = original.clone();
        event.payload["root_effect_owner"]["node_id"] = serde_json::json!("forged-extra-field");
        assert!(original_root_effect_owner(&event).is_err());
        event.payload = original.clone();
        event.payload["root_effect_owner"]["binding"]["node_id"] = serde_json::Value::Null;
        assert!(original_root_effect_owner(&event).is_err());
        event.payload = original;
        event.payload["root_effect_context"]["turn_id"] = serde_json::json!("other-turn");
        assert!(original_root_effect_owner(&event).is_err());
    }

    #[test]
    fn r1_root_historical_read_write_owner_uses_original_start_after_recovery_and_cancellation() {
        use crate::execution_core::graph::{ExecutionCommitService, ExecutionNodeBinding};
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
            ExecutionNodeStatus,
        };
        for read in [true, false] {
            let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
            let commits = ExecutionCommitService::new(store.clone());
            let mut graph = ExecutionGraph::new("original root history");
            graph.id = "scope2-graph".into();
            graph.lineage = Some(ExecutionGraphLineage {
                session_id: "scope2-session".into(),
                turn_id: "scope2-turn".into(),
                root_task_id: "root-task".into(),
                task_id: "task".into(),
                generation: 1,
            });
            for node_id in ["scope2-node", "other-node"] {
                let mut node = ExecutionNodeSpec::new(
                    ExecutionNodeKind::ToolBatch,
                    "tool_batch",
                    "original payload",
                );
                node.id = node_id.into();
                graph.nodes.push(node);
            }
            graph = commits.register_graph(graph).unwrap().graph;
            graph = commits
                .transition_node(
                    &graph,
                    "scope2-node",
                    ExecutionNodeStatus::Ready,
                    None,
                    vec![],
                )
                .unwrap()
                .graph;
            graph = commits
                .bind_and_start_node(
                    &graph,
                    "scope2-node",
                    ExecutionNodeBinding {
                        executor_kind: "tool_batch".into(),
                        ticket_idempotency_key: "original-ticket:1".into(),
                        attempt: 1,
                        resource_lease_refs: vec![],
                        scope_lease_ref: None,
                        worktree_lease_ref: None,
                    },
                )
                .unwrap()
                .graph;
            let owner = store
                .latest_for_stream("scope2-graph:node:scope2-node")
                .unwrap()
                .unwrap()
                .activity_binding()
                .unwrap();
            graph = commits
                .transition_node(
                    &graph,
                    "other-node",
                    ExecutionNodeStatus::Ready,
                    None,
                    vec![],
                )
                .unwrap()
                .graph;
            assert!(owner.revision < graph.revision);
            let root = crate::CowdExecutionContext {
                execution_id: graph.id.clone(),
                session_id: "scope2-session".into(),
                turn_id: "scope2-turn".into(),
            };
            let workspace = tempfile::tempdir().unwrap();
            std::fs::write(workspace.path().join("proof.txt"), "before").unwrap();
            let host = tools::ToolHost::builtin("root-history-real-tools", workspace.path())
                .with_authorization_lease_verifier(std::sync::Arc::new(
                    crate::AuthorizationNegotiator::verify_lease_signature,
                ));
            let mut request = crate::bound_tool_batch::scope_admission_tests::bound_request(
                &host,
                if read { "read_file" } else { "edit" },
                if read {
                    serde_json::json!({"path":"proof.txt"})
                } else {
                    serde_json::json!({"action":"write","path":"proof.txt","content":"after"})
                },
            );
            let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
            let evaluated = crate::AuthorizationNegotiator::new().assess_effective(
                &crate::PermissionPolicy::new(
                    harness_contract::policy::PermissionMode::WorkspaceWrite,
                ),
                &crate::AuthorizationRequest {
                    principal_id: "scope2-test".into(),
                    capability: request.tool_name.clone(),
                    input: request.input.clone(),
                    idempotency_key: request.tool_use_id.clone(),
                    effect: effect.clone(),
                    parent_ceiling: harness_contract::policy::PermissionMode::WorkspaceWrite,
                    parent_lease_id: None,
                    policy_revision: request.policy_revision,
                    recovery_scope: root.execution_id.clone(),
                    context: crate::PermissionContext::default(),
                    safe_alternatives: vec![],
                },
            );
            request.authorization = Some(
                crate::ToolPolicy
                    .authorize(
                        &evaluated.effective,
                        &evaluated.assessment,
                        &request.tool_use_id,
                        evaluated.assessment.lease.clone().unwrap(),
                        30,
                    )
                    .unwrap()
                    .authorization,
            );
            let resolver =
                crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                    .unwrap();
            let scope = crate::bound_tool_batch::admitted_effect_scope(
                &request,
                &effect,
                Some(host.pin_snapshot()),
                &resolver,
            )
            .unwrap();
            if read {
                commits
                    .begin_root_read_acquisition(&request, &effect, &root, &scope, &owner, 1)
                    .unwrap();
            } else {
                commits
                    .begin_root_tool_effect(&request, &effect, &root, &scope, &owner, 1)
                    .unwrap();
            }
            let unsigned_host =
                tools::ToolHost::builtin("root-history-real-tools", workspace.path());
            let input: serde_json::Value = serde_json::from_str(&request.input).unwrap();
            assert!(matches!(
                unsigned_host.pin_snapshot().validate_authorization(
                    request.authorization.as_ref().unwrap(),
                    &request.tool_name,
                    &input
                ),
                Err(tools::ToolHostError::InvalidLeaseSignature)
            ));
            let mut forged = request.authorization.as_ref().unwrap().clone();
            forged.authorization_lease.signature.push_str("-forged");
            assert!(matches!(
                host.pin_snapshot()
                    .validate_authorization(&forged, &request.tool_name, &input),
                Err(tools::ToolHostError::InvalidLeaseSignature)
            ));
            let output = host
                .pin_snapshot()
                .execute(
                    request.authorization.as_ref().unwrap(),
                    &request.tool_name,
                    &input,
                )
                .unwrap();
            let outcome = crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::Executed,
                category: request.category,
                output: Some(output),
                error: None,
                evidence_ref: "root-history-result".into(),
                observed_evidence: vec![],
            };
            if read {
                commits
                    .commit_root_read_acquisition(&request, &outcome, &effect, &root, &scope)
                    .unwrap();
            } else {
                commits
                    .commit_tool_effect(&request, &effect, &outcome)
                    .unwrap();
            }
            let source = format!("execution-effect:{}", request.idempotency_key);
            let primary = store
                .event_by_idempotency_key(
                    &source,
                    &format!(
                        "{}:{}",
                        request.idempotency_key,
                        if read { "read-receipt" } else { "receipt" }
                    ),
                )
                .unwrap()
                .unwrap();
            let intent = store
                .event_by_idempotency_key(
                    &source,
                    &format!(
                        "{}:{}",
                        request.idempotency_key,
                        if read { "read-intent" } else { "intent" }
                    ),
                )
                .unwrap()
                .unwrap();
            authenticate_root_receipt_owner(&store, &primary, &mut Default::default()).unwrap();
            let originals = root_owner_materials(&store, &intent, &mut Default::default()).unwrap();
            assert!(originals
                .iter()
                .any(|event| event.scope == crate::RuntimeEventScope::ExecutionGraph));
            let mut recovered = graph.clone();
            recovered
                .node_statuses
                .insert("scope2-node".into(), ExecutionNodeStatus::Ready);
            graph = commits
                .commit_recovery(&graph, recovered, vec!["scope2-node".into()], vec![])
                .unwrap()
                .graph;
            graph = commits
                .bind_and_start_node(
                    &graph,
                    "scope2-node",
                    ExecutionNodeBinding {
                        executor_kind: "tool_batch".into(),
                        ticket_idempotency_key: "original-ticket:2".into(),
                        attempt: 2,
                        resource_lease_refs: vec![],
                        scope_lease_ref: None,
                        worktree_lease_ref: None,
                    },
                )
                .unwrap()
                .graph;
            authenticate_root_receipt_owner(&store, &primary, &mut Default::default()).unwrap();
            let mut wrong = intent.clone();
            wrong.payload["root_effect_owner"]["attempt"] = serde_json::json!(2);
            assert!(
                authenticate_original_root_owner(&store, &wrong, &mut Default::default()).is_err()
            );
            graph = commits
                .transition_node(
                    &graph,
                    "scope2-node",
                    ExecutionNodeStatus::Cancelled,
                    None,
                    vec![],
                )
                .unwrap()
                .graph;
            assert_eq!(graph.recovery_cursor.node_attempts["scope2-node"], 2);
            authenticate_root_receipt_owner(&store, &primary, &mut Default::default()).unwrap();
            for fault in [
                "graph-content",
                "graph-transaction",
                "index-effect",
                "index-replay",
            ] {
                ROOT_HISTORY_SOURCE_FAULT.with(|source| source.set(Some(fault)));
                let result =
                    authenticate_root_receipt_owner(&store, &primary, &mut Default::default());
                ROOT_HISTORY_SOURCE_FAULT.with(|source| source.set(None));
                assert!(result.is_err(), "{read}/{fault}");
            }
            assert_eq!(
                std::fs::read_to_string(workspace.path().join("proof.txt")).unwrap(),
                if read { "before" } else { "after" }
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bounded_process_real_writer_and_actual_collector_preserve_failed_v1_and_complete_v2_archives(
    ) {
        use crate::execution_core::graph::{ExecutionCommitService, ExecutionNodeBinding};
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
            ExecutionNodeStatus,
        };
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(crate::RuntimeEventStore::for_test());
        let artifacts = std::sync::Arc::new(crate::ArtifactStore::for_test_default(storage.path()));
        store.bind_artifact_store(artifacts.clone()).unwrap();
        let commits =
            ExecutionCommitService::new(store.clone()).with_artifact_store(artifacts.clone());
        let mut graph = ExecutionGraph::new("original bounded process history");
        graph.id = "scope2-graph".into();
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: "scope2-session".into(),
            turn_id: "scope2-turn".into(),
            root_task_id: "root-task".into(),
            task_id: "task".into(),
            generation: 1,
        });
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::ToolBatch,
            "tool_batch",
            "original bounded tool payload",
        );
        node.id = "scope2-node".into();
        graph.nodes.push(node);
        graph = commits.register_graph(graph).unwrap().graph;
        graph = commits
            .transition_node(
                &graph,
                "scope2-node",
                ExecutionNodeStatus::Ready,
                None,
                vec![],
            )
            .unwrap()
            .graph;
        graph = commits
            .bind_and_start_node(
                &graph,
                "scope2-node",
                ExecutionNodeBinding {
                    executor_kind: "tool_batch".into(),
                    ticket_idempotency_key: "bounded-original-ticket".into(),
                    attempt: 1,
                    resource_lease_refs: vec![],
                    scope_lease_ref: None,
                    worktree_lease_ref: None,
                },
            )
            .unwrap()
            .graph;
        let owner = store
            .latest_for_stream("scope2-graph:node:scope2-node")
            .unwrap()
            .unwrap()
            .activity_binding()
            .unwrap();
        let root = crate::CowdExecutionContext {
            execution_id: graph.id.clone(),
            session_id: "scope2-session".into(),
            turn_id: "scope2-turn".into(),
        };
        let host = tools::ToolHost::builtin("bounded-original-history", workspace.path())
            .with_authorization_lease_verifier(std::sync::Arc::new(
                crate::AuthorizationNegotiator::verify_lease_signature,
            ));
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                .unwrap();
        let manager = std::sync::Arc::new(ScopeLockManager::new());
        let mut boundaries = Vec::new();
        let mut runs = Vec::new();
        for version in [1, 2] {
            std::fs::write(
                workspace.path().join("source.py"),
                format!("value={version}\n"),
            )
            .unwrap();
            let input = serde_json::json!({
                "language":"python","input_files":["source.py"],"timeout_ms":10000,
                "code":"import sys\nvalues={}\nexec(open('source.py').read(),values)\nassert values['value']==2\nsys.stdout.buffer.write(bytes(range(256))*256)\n",
            });
            let mut request = crate::bound_tool_batch::scope_admission_tests::bound_request(
                &host,
                "execute_code",
                input.clone(),
            );
            let mut step = request.step_binding.as_ref().unwrap().clone();
            step.origin.call_id = format!("bounded-version-{version}");
            request.tool_use_id = step.origin.call_id.clone();
            request.idempotency_key = step.effect_idempotency_key();
            request.compiled_invocation = Some(
                step.prepare_invocation(Some(host.pin_snapshot()), "execute_code", &input)
                    .unwrap(),
            );
            request.step_binding = Some(step);
            request.observation_wave_sequence = version;
            let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
            crate::bound_tool_batch::scope_admission_tests::authorize_bounded_fixture(
                &host,
                &mut request,
                store.clone(),
                harness_contract::policy::PermissionMode::DangerFullAccess,
            );
            let (locks, _) =
                crate::bound_tool_batch::bound_effect_lock_requests(&resolver, &effect).unwrap();
            let _lease = manager.acquire(locks, None).await.unwrap();
            let scope = crate::bound_tool_batch::admitted_effect_scope(
                &request,
                &effect,
                Some(host.pin_snapshot()),
                &resolver,
            )
            .unwrap();
            assert!(!scope.complete);
            commits
                .begin_root_tool_effect(&request, &effect, &root, &scope, &owner, 1)
                .unwrap();
            let binding = commits.bounded_process_binding(&request, &effect).unwrap();
            let prepared = host
                .pin_snapshot()
                .prepare_bounded_process(request.authorization.as_ref().unwrap(), &input, binding)
                .unwrap();
            let bundle = artifacts
                .write_bytes(
                    harness_contract::context::ArtifactWriteDescriptor {
                        media_type: "application/octet-stream".into(),
                        visibility_scope: "session:scope2-session".into(),
                        expected_bytes: Some(prepared.source_bundle_bytes().len() as u64),
                        original_name: None,
                    },
                    prepared.source_bundle_bytes(),
                )
                .await
                .unwrap();
            let admission = commits
                .admit_bounded_process(&request, &effect, &prepared, &bundle)
                .unwrap();
            let completed = prepared.run(admission, &tools::ToolExecutionCancellation::new());
            let outcome = commits
                .commit_bounded_process(&request, &effect, completed, &artifacts)
                .await
                .unwrap();
            assert_eq!(outcome.observed_evidence.len(), 1);
            let primary = store
                .event_by_idempotency_key(
                    &format!("execution-effect:{}", request.idempotency_key),
                    &format!("{}:receipt", request.idempotency_key),
                )
                .unwrap()
                .unwrap();
            let run = authenticate_bounded_process(&store, &primary).unwrap();
            process_has_no_external_effects(&run).unwrap();
            let retirement = super::super::validate_bounded_process_retirement(
                &store,
                &primary,
                Some(&artifacts),
            )
            .unwrap();
            assert!(retirement
                .iter()
                .any(|cut| cut.stream_id == primary.stream_id));
            match &run.fact.terminal {
                harness_contract::context::ToolExecutionTerminal::Settled {
                    exit_code,
                    signal,
                    ..
                } => {
                    assert!(signal.is_none());
                    assert_eq!(*exit_code, Some(if version == 1 { 1 } else { 0 }));
                }
                terminal => panic!("actual bounded command must physically settle: {terminal:?}"),
            }
            validate_process_archives(
                &run,
                super::super::ResultContentReaders {
                    artifacts: Some(&artifacts),
                    history: None,
                    process_inputs: None,
                },
            )
            .unwrap();
            let body = artifacts
                .read(&run.fact.body, &run.fact.body.visibility_scope, None)
                .await
                .unwrap();
            let mut presentation: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert!(presentation["stderr_utf8"].is_string());
            if version == 2 {
                assert!(presentation.get("stdout_utf8").is_none());
                presentation["stderr_utf8"] = serde_json::json!("counterfeit complete log");
                let bytes = serde_json::to_vec(&presentation).unwrap();
                let incorrect = artifacts
                    .write_bytes(
                        harness_contract::context::ArtifactWriteDescriptor {
                            media_type: "application/json".into(),
                            visibility_scope: "session:scope2-session".into(),
                            expected_bytes: Some(bytes.len() as u64),
                            original_name: None,
                        },
                        &bytes,
                    )
                    .await
                    .unwrap();
                let mut forged = run.clone();
                forged.fact.body = incorrect;
                assert!(validate_process_archives(
                    &forged,
                    super::super::ResultContentReaders {
                        artifacts: Some(&artifacts),
                        history: None,
                        process_inputs: None,
                    }
                )
                .is_err());
            }
            let intent = store
                .event_by_idempotency_key(
                    &primary.stream_id,
                    &format!("{}:intent", request.idempotency_key),
                )
                .unwrap()
                .unwrap();
            boundaries.push(
                result_input_boundary(
                    &intent,
                    "root-execution:scope2-graph".into(),
                    None,
                    request.step_binding.clone(),
                )
                .unwrap(),
            );
            let mut omitted = run.clone();
            omitted.fact.admission.inputs.clear();
            assert!(validate_process_archives(
                &omitted,
                super::super::ResultContentReaders {
                    artifacts: Some(&artifacts),
                    history: None,
                    process_inputs: None,
                }
            )
            .is_err());
            let mut forged = primary.clone();
            forged.payload["outcome"]["observed_evidence"] = serde_json::json!([]);
            assert!(authenticate_bounded_process(&store, &forged).is_err());
            runs.push(run);
        }
        graph = commits
            .transition_node(
                &graph,
                "scope2-node",
                ExecutionNodeStatus::Cancelled,
                None,
                vec![],
            )
            .unwrap()
            .graph;
        assert_eq!(
            graph.node_statuses["scope2-node"],
            ExecutionNodeStatus::Cancelled
        );
        for run in &runs {
            let original = store
                .event_by_idempotency_key(
                    &run.primary.stream_id,
                    &format!("{}:receipt", run.fact.admission.binding.call_key),
                )
                .unwrap()
                .unwrap();
            authenticate_bounded_process(&store, &original).unwrap();
        }
        let mut program = AgenticProgramProjection::empty(
            "bounded-original-program",
            "bounded-original-objective",
        );
        program.session_id = "scope2-session".into();
        program.turn_id = "scope2-turn".into();
        program.root_execution_id = Some("scope2-graph".into());
        let actual = super::super::policy::actual_input_snapshot(
            &store,
            &program,
            &boundaries,
            super::super::ResultContentReaders {
                artifacts: Some(&artifacts),
                history: None,
                process_inputs: None,
            },
            None,
        )
        .unwrap();
        assert!(actual.file_dependencies.is_empty());
        for fault in [
            "admission-inputs",
            "admission-digest",
            "admission-original-input",
            "admission-workspace",
            "index-fact",
            "index-scope",
        ] {
            BOUNDED_PROCESS_SOURCE_FAULT.with(|source| source.set(Some(fault)));
            let rejected = super::super::policy::actual_input_snapshot(
                &store,
                &program,
                &boundaries,
                super::super::ResultContentReaders {
                    artifacts: Some(&artifacts),
                    history: None,
                    process_inputs: None,
                },
                None,
            );
            BOUNDED_PROCESS_SOURCE_FAULT.with(|source| source.set(None));
            assert!(rejected.is_err(), "{fault}");
        }
        for run in &runs {
            for material in [&run.primary, &run.admission, &run.index] {
                assert!(actual.material_events.contains(material));
            }
        }
        assert_eq!(
            actual
                .canonical_facts
                .iter()
                .filter(|fact| fact["grade"] == "bounded_process_history")
                .count(),
            2
        );
        assert!(runs[1].fact.stdout_bytes > 16384);
        assert_ne!(
            runs[0].fact.admission.inputs[0].sha256,
            runs[1].fact.admission.inputs[0].sha256
        );
        assert!(
            super::super::policy::validate_current_process_inputs(workspace.path(), &runs[0])
                .is_err()
        );
        super::super::policy::validate_current_process_inputs(workspace.path(), &runs[1]).unwrap();
        let writer_request = ScopeLockRequest {
            scope: ScopedResource::workspace_object(
                resolver.resolve_existing("source.py").unwrap(),
            ),
            mode: ScopeLockMode::Write,
        };
        let writer = manager
            .try_acquire([writer_request.clone()])
            .unwrap()
            .unwrap();
        let verifier = super::super::ProcessInputVerifier::new(manager.clone());
        let frame = verifier.frame();
        let readers = super::super::ResultContentReaders {
            artifacts: Some(&artifacts),
            history: None,
            process_inputs: Some(&frame),
        };
        let busy =
            super::super::validate_selected_process_inputs(&store, &runs[1], readers).unwrap_err();
        assert!(busy.contains("busy") && busy.contains("retry"));
        drop(writer);
        assert!(super::super::validate_selected_process_inputs(&store, &runs[0], readers).is_err());
        super::super::validate_selected_process_inputs(&store, &runs[1], readers).unwrap();
        assert!(manager
            .try_acquire([writer_request.clone()])
            .unwrap()
            .is_none());
        drop(frame);
        assert!(manager.try_acquire([writer_request]).unwrap().is_some());
        assert!(super::super::complete_process_body_refs(
            &runs[1],
            &super::super::policy::ReviewDeliverySnapshot {
                reads: Vec::new(),
                sources: Default::default(),
            }
        )
        .is_err());
    }
}
