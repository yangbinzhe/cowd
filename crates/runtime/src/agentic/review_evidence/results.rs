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

pub(super) struct ResolvedReviewResult {
    pub result_sources: Vec<crate::ExpectedStreamRevision>,
    pub reference: String,
    pub content: ArtifactRef,
    pub kind: GoalResultKind,
    pub artifact_kinds: BTreeSet<String>,
    pub producers: Vec<ResultProducer>,
    pub effect_cursor: Option<u64>,
    pub effect_sources: Vec<crate::ExpectedStreamRevision>,
    pub effect_scopes: Vec<PermissionScope>,
    pub unscoped_effect: bool,
}

pub(super) fn tool_effect(
    services: &RuntimeServices,
    event: &DurableRuntimeEvent,
) -> Result<Option<ToolEffectDescriptor>, String> {
    tool_effect_from_store(services.event_store(), event)
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
    let mut offset = 0;
    loop {
        let page = store.list_stream_page_desc(&event.stream_id, 64, offset)?;
        if page.is_empty() {
            return Ok(None);
        }
        offset += page.len();
        for candidate in page {
            if candidate.kind != "tool.execution_plan.created"
                || candidate.payload["plan_id"] != plan_id
            {
                continue;
            }
            return Ok(candidate.payload["tasks"]
                .as_array()
                .and_then(|tasks| {
                    tasks.iter().find(|task| {
                        task["tool_call_id"] == call_id
                            && task["tool_name"] == event.payload["tool_name"]
                    })
                })
                .and_then(|task| serde_json::from_value(task["effect"].clone()).ok()));
        }
    }
}

async fn tool_content(
    services: &RuntimeServices,
    session: &str,
    reference: &str,
) -> Result<ArtifactRef, String> {
    let id = reference
        .strip_prefix("tool://")
        .ok_or("result is not a Tool evidence reference")?;
    let access = services
        .session_evidence_access(session, id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or("result Tool evidence has no authenticated Session receipt")?;
    let content = services
        .artifact_store()
        .resolve(&access.retrieval_selector)
        .map_err(|error| error.to_string())?;
    if content.sha256 != access.sha256
        || content.bytes != access.bytes
        || content.media_type != access.media_type
        || content.visibility_scope != access.visibility_scope
    {
        return Err("result Tool evidence does not match its durable content".into());
    }
    Ok(content)
}

pub(super) async fn resolve_result(
    services: &RuntimeServices,
    actor: &AgentActorBinding,
    program: &AgenticProgramProjection,
    reference: &str,
) -> Result<ResolvedReviewResult, String> {
    if reference.starts_with("approval:v1:") {
        let (content, approval, source) = services
            .approval_result_content(&actor.session_id, reference)
            .await?;
        let (mut graph_id, _) =
            crate::execution_core::graph::executors::parse_graph_approval_id(reference).unwrap();
        let mut roots = BTreeSet::from([program
            .root_execution_id
            .clone()
            .ok_or("result Program has no root")?]);
        let mut prior = program.continuation.clone();
        let mut seen_programs = BTreeSet::from([program.program_id.clone()]);
        while let Some(binding) = prior {
            if !seen_programs.insert(binding.source_program_id.clone()) {
                return Err("cyclic result continuation".into());
            }
            let source_program = services
                .agent_action_service()
                .project(&binding.source_program_id)
                .map_err(|error| error.to_string())?;
            if source_program.session_id != actor.session_id
                || source_program.root_execution_id.as_deref()
                    != Some(binding.source_root_id.as_str())
            {
                return Err("external result continuation crosses authenticated scope".into());
            }
            roots.insert(binding.source_root_id);
            prior = source_program.continuation;
        }
        let mut seen_graphs = BTreeSet::new();
        while !roots.contains(&graph_id) {
            if !seen_graphs.insert(graph_id.clone()) {
                return Err("cyclic external result graph lineage".into());
            }
            let graph = services
                .graph_state_store()
                .load(&graph_id)
                .map_err(|error| error.to_string())?;
            if !graph
                .lineage
                .as_ref()
                .is_some_and(|lineage| lineage.session_id == actor.session_id)
            {
                return Err("external result graph crosses its Session".into());
            }
            graph_id = graph
                .parent_execution
                .ok_or("external decision belongs to another Objective")?
                .execution_id;
        }
        let decision = approval
            .decision
            .as_ref()
            .ok_or("missing external decision")?;
        return Ok(ResolvedReviewResult {
            reference: reference.into(),
            content,
            kind: GoalResultKind::ExternalDecision,
            artifact_kinds: BTreeSet::from(["external_decision".into()]),
            producers: vec![ResultProducer {
                actor: format!(
                    "external-decision:{:?}:{}",
                    decision.actor.kind, decision.actor.actor_id
                ),
                execution: format!(
                    "approval-decision:{}:{}",
                    approval.approval_id, source.expected_revision
                ),
                packet: None,
                tool: None,
            }],
            result_sources: vec![source],
            effect_cursor: None,
            effect_sources: vec![],
            effect_scopes: vec![],
            unscoped_effect: false,
        });
    }
    let content = if let Some(artifact) = program.artifacts.get(reference) {
        services
            .artifact_store()
            .resolve(&artifact.content_ref)
            .map_err(|error| error.to_string())?
    } else if reference.starts_with("tool://") {
        tool_content(services, &actor.session_id, reference).await?
    } else if reference.starts_with("artifact://") {
        services
            .artifact_store()
            .resolve(reference)
            .map_err(|error| error.to_string())?
    } else {
        return Err(format!(
            "review result has no Runtime-resolved producer:{reference}"
        ));
    };
    let candidates = program
        .artifacts
        .values()
        .filter(|artifact| {
            if reference.starts_with("tool://") {
                false
            } else if program.artifacts.contains_key(reference) {
                artifact.artifact_ref == reference
            } else {
                artifact.content_ref == content.selector
            }
        })
        .collect::<Vec<_>>();
    let mut scopes = vec![(
        program.program_id.clone(),
        program
            .root_execution_id
            .clone()
            .ok_or("result Program has no root")?,
        program.turn_id.clone(),
    )];
    let mut continuation = program.continuation.clone();
    let mut seen = BTreeSet::from([program.program_id.clone()]);
    while let Some(source) = continuation {
        if !seen.insert(source.source_program_id.clone()) {
            return Err("result continuation lineage is cyclic".into());
        }
        let prior = services
            .agent_action_service()
            .project(&source.source_program_id)
            .map_err(|error| error.to_string())?;
        if prior.session_id != actor.session_id
            || prior.root_execution_id.as_deref() != Some(source.source_root_id.as_str())
        {
            return Err("result continuation lineage crosses its authenticated Session".into());
        }
        scopes.push((
            prior.program_id.clone(),
            source.source_root_id,
            prior.turn_id.clone(),
        ));
        continuation = prior.continuation;
    }
    let session_source = crate::ExpectedStreamRevision {
        stream_id: format!("session:{}", actor.session_id),
        expected_revision: services
            .event_store()
            .stream_revision(&format!("session:{}", actor.session_id))
            .map_err(|error| error.to_string())?,
    };
    let mut resolved = ResolvedReviewResult {
        result_sources: vec![],
        reference: reference.into(),
        kind: if content.media_type.contains("json") {
            GoalResultKind::StructuredData
        } else {
            GoalResultKind::Content
        },
        content: content.clone(),
        artifact_kinds: BTreeSet::new(),
        producers: vec![],
        effect_cursor: None,
        effect_sources: vec![session_source],
        effect_scopes: vec![],
        unscoped_effect: false,
    };
    if !candidates.is_empty() {
        resolved.kind = GoalResultKind::Artifact;
        let mut producers = Vec::new();
        let mut kinds = BTreeSet::new();
        for artifact in candidates {
            kinds.insert(artifact.kind.clone());
            let execution = if let Some(execution) = &artifact.claim_execution_id {
                Some(execution.clone())
            } else {
                None
            };
            let (actor_ref, execution, packet) = if let Some(execution) = execution {
                let packet = super::physical_packet(services, &execution)?;
                (
                    artifact.committed_by.clone(),
                    format!("agent-run:{}", packet.assignment.run_id),
                    Some(packet),
                )
            } else {
                let mut origin = None;
                for (program_id, root, _) in &scopes {
                    let mut offset = 0;
                    loop {
                        let events = services.event_store().list_stream_page_desc(
                            &format!("agentic-program:{program_id}"),
                            64,
                            offset,
                        )?;
                        if events.is_empty() {
                            break;
                        }
                        offset += events.len();
                        if let Some(event) = events.into_iter().find(|event| {
                            event.kind == "agentic.action_applied"
                                && event.payload["entity_ref"] == artifact.artifact_ref
                        }) {
                            let envelope: harness_contract::agent_action::AgentActionEnvelope =
                                serde_json::from_value(event.payload["envelope"].clone())
                                    .map_err(|error| error.to_string())?;
                            origin = Some((envelope.actor, root.clone()));
                            break;
                        }
                    }
                    if origin.is_some() {
                        break;
                    }
                }
                let (owner, root) = origin.ok_or_else(|| {
                    format!(
                        "result Artifact has no durable publication actor:{}",
                        artifact.artifact_ref
                    )
                })?;
                if let Some(execution) = &owner.execution_id {
                    let packet = super::physical_packet(services, execution)?;
                    (
                        owner.actor_id,
                        format!("agent-run:{}", packet.assignment.run_id),
                        Some(packet),
                    )
                } else {
                    (owner.actor_id, format!("root-execution:{root}"), None)
                }
            };
            if !producers.iter().any(|producer: &ResultProducer| {
                producer.actor == actor_ref && producer.execution == execution
            }) {
                producers.push(ResultProducer {
                    actor: actor_ref,
                    execution,
                    packet,
                    tool: None,
                });
            }
        }
        resolved.producers = producers;
        resolved.artifact_kinds = kinds;
    }

    let mut root_effects = Vec::new();
    for (_, root, turn) in scopes {
        let mut position = None;
        loop {
            let events = services.event_store().events_for_root_execution_kind(
                &root,
                "tool.invocation.completed",
                position,
                64,
            )?;
            if events.is_empty() {
                break;
            }
            for event in events {
                position = Some((event.commit_cursor, event.transaction_index));
                let Some(binding) = event.activity_binding() else {
                    continue;
                };
                if binding.session_id != actor.session_id
                    || binding.turn_id != turn
                    || event.payload["status"] != "completed"
                {
                    continue;
                }
                let effect = tool_effect(services, &event)?;
                let is_effect = !event.payload["tool_name"]
                    .as_str()
                    .is_some_and(super::policy::goal_metadata_tool)
                    && !effect
                        .as_ref()
                        .is_some_and(|effect| effect.effect_kind == ToolEffectKind::Read);
                if binding.agent_run_id.is_none() && is_effect {
                    root_effects.push((
                        format!("root-execution:{root}"),
                        event.commit_cursor,
                        event.payload["tool_name"]
                            .as_str()
                            .unwrap_or_default()
                            .to_owned(),
                        effect.clone(),
                    ));
                }
                let Some(raw_ref) = event.payload["full_output_ref"].as_str() else {
                    continue;
                };
                let raw = match tool_content(services, &actor.session_id, raw_ref).await {
                    Ok(raw) => raw,
                    Err(error) if raw_ref == reference => return Err(error),
                    Err(_) => continue,
                };
                let mut publication = false;
                let mut matches = if reference.starts_with("tool://") {
                    raw_ref == reference
                } else {
                    raw.selector == content.selector
                };
                if !matches
                    && !reference.starts_with("tool://")
                    && event.payload["tool_name"] == "artifact_publish"
                    && raw.bytes <= 256 * 1024
                {
                    let bytes = services
                        .artifact_store()
                        .read(&raw, &raw.visibility_scope, None)
                        .await
                        .map_err(|error| error.to_string())?;
                    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                        publication = value["status"] == "published"
                            && value["content_ref"] == content.selector
                            && value["sha256"] == content.sha256
                            && value["bytes"].as_u64() == Some(content.bytes);
                        matches = publication;
                    }
                }
                if !matches {
                    continue;
                }
                let is_effect = !publication && is_effect;
                let (execution, packet, producer_actor) = if let Some(run) = &binding.agent_run_id {
                    let execution = event
                        .refs
                        .iter()
                        .find(|reference| {
                            matches!(reference.kind.as_str(), "execution_graph" | "execution")
                                && reference.id != root
                        })
                        .map(|reference| reference.id.as_str())
                        .ok_or("result Tool activity has no physical execution reference")?;
                    let packet = super::physical_packet(services, execution)?;
                    if packet.assignment.run_id != *run {
                        return Err("result Tool activity does not match physical producer".into());
                    }
                    let agent = packet
                        .agentic_binding
                        .as_ref()
                        .map(|scope| scope.agent_id.clone())
                        .unwrap_or_else(|| run.clone());
                    (format!("agent-run:{run}"), Some(packet), agent)
                } else {
                    (
                        format!("root-execution:{root}"),
                        None,
                        format!("root:{}", actor.session_id),
                    )
                };
                if !resolved.producers.iter().any(|producer| {
                    producer.actor == producer_actor
                        && producer.execution == execution
                        && producer.tool.as_ref().map(|(name, _)| name.as_str())
                            == event.payload["tool_name"].as_str()
                }) {
                    resolved.producers.push(ResultProducer {
                        actor: producer_actor,
                        execution,
                        packet,
                        tool: effect.as_ref().map(|effect| {
                            (
                                event.payload["tool_name"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .into(),
                                effect.effect_kind,
                            )
                        }),
                    });
                }
                if is_effect {
                    resolved.kind = GoalResultKind::ToolEffect;
                    resolved.effect_cursor = Some(
                        resolved
                            .effect_cursor
                            .unwrap_or_default()
                            .max(event.commit_cursor),
                    );
                    if let Some(effect) = effect.filter(|effect| !effect.scopes.is_empty()) {
                        for scope in effect.scopes {
                            if !resolved.effect_scopes.contains(&scope) {
                                resolved.effect_scopes.push(scope);
                            }
                        }
                    } else {
                        resolved.unscoped_effect = true;
                    }
                }
            }
        }
    }
    // Root publications inherit their physical Root's actual effects even
    // when the explanation has a different content hash from a tool receipt.
    for (execution, cursor, tool, effect) in root_effects {
        let Some(actor) = resolved
            .producers
            .iter()
            .find(|producer| producer.execution == execution)
            .map(|producer| producer.actor.clone())
        else {
            continue;
        };
        resolved.kind = GoalResultKind::ToolEffect;
        resolved.effect_cursor = Some(resolved.effect_cursor.unwrap_or_default().max(cursor));
        if let Some(effect) = effect.filter(|effect| !effect.scopes.is_empty()) {
            for scope in &effect.scopes {
                if !resolved.effect_scopes.contains(scope) {
                    resolved.effect_scopes.push(scope.clone());
                }
            }
            if !resolved.producers.iter().any(|producer| {
                producer.execution == execution
                    && producer
                        .tool
                        .as_ref()
                        .is_some_and(|(name, _)| name == &tool)
            }) {
                resolved.producers.push(ResultProducer {
                    actor,
                    execution,
                    packet: None,
                    tool: Some((tool, effect.effect_kind)),
                });
            }
        } else {
            resolved.unscoped_effect = true;
        }
    }
    // An Artifact is content, not proof that its physical producer made no
    // side effects. Follow every producer's original canonical receipts.
    for producer in &resolved.producers {
        let Some(packet) = &producer.packet else {
            continue;
        };
        let source = format!(
            "execution-agent-receipts:{}:{}:{}",
            packet.graph_id(),
            packet.node_id(),
            packet.attempt
        );
        let revision = services
            .event_store()
            .stream_revision(&source)
            .map_err(|error| error.to_string())?;
        resolved.effect_sources.push(crate::ExpectedStreamRevision {
            stream_id: source,
            expected_revision: revision,
        });
        for receipt in services
            .commit_service()
            .load_delegated_agent_tool_receipts(packet.graph_id(), packet.node_id(), packet.attempt)
            .map_err(|error| error.to_string())?
        {
            if receipt.effect_kind == ToolEffectKind::Read
                || super::policy::goal_metadata_tool(&receipt.outcome.tool_name)
            {
                continue;
            }
            resolved.kind = GoalResultKind::ToolEffect;
            resolved.effect_cursor = Some(
                resolved
                    .effect_cursor
                    .unwrap_or_default()
                    .max(receipt.committed_cursor),
            );
            let scopes =
                delegated_effect_scopes(services.path_identity_resolver().workspace_id(), &receipt);
            if scopes.is_empty() {
                resolved.unscoped_effect = true;
            }
            for scope in scopes {
                if !resolved.effect_scopes.contains(&scope) {
                    resolved.effect_scopes.push(scope);
                }
            }
        }
    }
    if resolved.producers.is_empty() {
        Err(format!(
            "review result has no Runtime-resolved producer:{reference}"
        ))
    } else {
        Ok(resolved)
    }
}

/// Historical receipts may lack the new exact scope field. Only ToolHost's
/// typed write observation can recover that identity; a broad lease or today's
/// filesystem cannot supply missing historical evidence.
pub(super) fn delegated_effect_scopes(
    workspace_id: &str,
    receipt: &crate::execution_core::graph::DurableAgentToolReceipt,
) -> Vec<PermissionScope> {
    if !receipt.effect_scopes.is_empty() {
        return receipt.effect_scopes.clone();
    }
    let mut scopes = receipt.effect_scope.iter().cloned().collect::<Vec<_>>();
    for evidence in &receipt.outcome.observed_evidence {
        let harness_contract::context::EvidenceTargetIdentity::Workspace { scope } =
            &evidence.target
        else {
            continue;
        };
        if scope.access_mode != harness_contract::context::WorkspaceAccessMode::Write
            || scope.coverage != harness_contract::context::EvidenceCoverageKind::WriteEffect
        {
            continue;
        }
        if scope.path.workspace_id != workspace_id
            || scope.path.workspace_relative_path.is_empty()
            || !std::path::Path::new(&scope.path.workspace_relative_path)
                .components()
                .all(|component| matches!(component, std::path::Component::Normal(_)))
        {
            return Vec::new();
        }
        let target = PermissionScope {
            resource: harness_contract::policy::PermissionResource::File,
            operation: harness_contract::policy::PermissionOperation::Write,
            target: Some(scope.path.workspace_relative_path.clone()),
        };
        if !scopes.contains(&target) {
            scopes.push(target);
        }
    }
    scopes
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn delegated_historical_effect_targets_require_typed_same_workspace_write_evidence() {
        let mut receipt = crate::execution_core::graph::DurableAgentToolReceipt {
            committed_cursor: 7,
            sequence: 1,
            effect_scope: None,
            effect_scopes: vec![],
            effect_kind: ToolEffectKind::Write,
            authorized_scopes: vec!["workspace:.".into()],
            outcome: crate::RuntimeToolExecutionOutcome {
                tool_use_id: "write".into(),
                tool_name: "write_file".into(),
                status: crate::RuntimeToolExecutionStatus::Executed,
                category: crate::ToolSafetyCategory::WriteLocal,
                output: None,
                error: None,
                evidence_ref: "historical-receipt".into(),
                observed_evidence: vec![],
            },
        };
        assert!(
            delegated_effect_scopes("workspace:one", &receipt).is_empty(),
            "a broad lease is not actual effect scope"
        );
        let observation: harness_contract::context::ObservedEvidence =
            serde_json::from_value(serde_json::json!({
                "obligation_id":"effect", "target":{"kind":"workspace", "scope":{
                    "access_mode":"write", "coverage":"write_effect", "path":{
                        "workspace_id":"workspace:one", "repository_id":"repository:one",
                        "workspace_relative_path":"file.txt", "repository_relative_path":"file.txt",
                        "object_kind":"file", "observed_revision_or_digest":"sha256:historical"
                    }
                }}
            }))
            .unwrap();
        receipt.outcome.observed_evidence.push(observation.clone());
        assert_eq!(
            delegated_effect_scopes("workspace:one", &receipt)[0]
                .target
                .as_deref(),
            Some("file.txt")
        );
        assert!(delegated_effect_scopes("workspace:other", &receipt).is_empty());
        let mut corrupt = observation;
        if let harness_contract::context::EvidenceTargetIdentity::Workspace { scope } =
            &mut corrupt.target
        {
            scope.path.workspace_relative_path = "../escape".into();
        }
        receipt.outcome.observed_evidence.push(corrupt);
        assert!(
            delegated_effect_scopes("workspace:one", &receipt).is_empty(),
            "partial reconstruction must not hide an unscoped effect"
        );
    }
}
