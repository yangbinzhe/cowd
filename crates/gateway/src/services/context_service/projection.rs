use super::*;

impl ContextService {
    pub(crate) async fn current_context_projection(
        &self,
        memory: &MemoryService,
        connector: &ConnectorService,
        workspace_root: &Path,
        active_envelope: Option<ContextEnvelope>,
        fallback_session_id: Option<String>,
        params: HashMap<String, String>,
    ) -> serde_json::Value {
        let session_id = params
            .get("session_id")
            .cloned()
            .or(fallback_session_id)
            .unwrap_or_else(|| "api-context".to_string());
        let query = params.get("q").cloned().unwrap_or_default();
        let profile = params
            .get("profile")
            .and_then(|value| parse_context_profile(value))
            .unwrap_or(ContextProfile::MainTurn);

        if let Some(envelope) = active_envelope {
            return context_projection_json("runtime", envelope, &params);
        }

        let mut identity = ContextIdentity::main(session_id.clone());
        identity.mode = RuntimeContextBoundary::mode_for_profile(profile);
        let mut dynamic_items = Vec::new();
        let mut omitted_items = Vec::new();
        let mut degraded = Vec::new();

        match memory
            .context_packet(session_id.clone(), "api", query.clone(), 12, 2_000)
            .await
        {
            Ok(packet) => {
                for item in packet.selected {
                    let mut context_item = ContextItem::new(
                        item.atom.id.to_string(),
                        ContextSourceKind::Memory,
                        match item.role {
                            memory::MemoryPacketRole::Orientation => ContextRole::Orientation,
                            memory::MemoryPacketRole::Supporting => ContextRole::Evidence,
                            memory::MemoryPacketRole::Warning
                            | memory::MemoryPacketRole::Conflict => ContextRole::Warning,
                        },
                        format!(
                            "{}\nreason: {}\nevidence: {}",
                            item.atom.title,
                            item.reason,
                            item.atom.evidence_pointer.as_deref().unwrap_or("")
                        ),
                    );
                    context_item.authority = ContextAuthority::Session;
                    context_item.visibility = ContextVisibility::Private;
                    context_item.score = item.atom.confidence;
                    dynamic_items.push(context_item);
                }
                for omitted in packet.omitted {
                    omitted_items.push(ContextOmission {
                        source: ContextSourceKind::Memory,
                        reason: format!("{}: {}", omitted.reason, omitted.title),
                        token_estimate: 0,
                    });
                }
            }
            Err(_) => degraded.push(ContextSourceKind::Memory),
        }

        dynamic_items.extend(self.resource_context_items(connector, workspace_root, &query));

        let mut envelope = RuntimeContextBoundary::build_envelope(ContextEnvelopeRequest {
            profile,
            runtime_header: RuntimeContextBoundary::runtime_header(&identity, profile),
            identity,
            intent: query,
            stable_head: vec!["cowd-context-runtime:v0.8.13".to_string()],
            dynamic_items,
            omitted: omitted_items,
            total_budget_tokens: 8_000,
        });
        envelope.diagnostics.degraded_sources = degraded;
        context_projection_json("synthetic", envelope, &params)
    }

    fn resource_context_items(
        &self,
        connector: &ConnectorService,
        workspace_root: &Path,
        query: &str,
    ) -> Vec<ContextItem> {
        if !connector.resource_directory_path(workspace_root).exists() {
            return Vec::new();
        }
        let resources = if query.trim().is_empty() {
            connector.recent_resources(workspace_root, 5)
        } else {
            connector.search_resources(workspace_root, query, 5)
        }
        .unwrap_or_default();

        resources.into_iter().map(resource_context_item).collect()
    }
}

fn context_projection_json(
    source: &'static str,
    envelope: ContextEnvelope,
    params: &HashMap<String, String>,
) -> serde_json::Value {
    let lean_probe = RuntimeContextBoundary::lean_probe(&envelope);
    let policy_decision = RuntimeContextBoundary::policy_decision(&lean_probe);
    let mode_coverage = RuntimeContextBoundary::mode_coverage_report(
        envelope.identity.session_id.clone(),
        envelope.intent.clone(),
        envelope.assembled.stable_head.clone(),
        envelope.selected.clone(),
        envelope.budget.total_tokens,
    );
    let cache_stability = RuntimeContextBoundary::cache_stability_report(&envelope, &envelope);
    let snapshot = RuntimeContextBoundary::snapshot(&envelope);
    let budget_explanation = RuntimeContextBoundary::budget_explanation(&envelope);
    let agent_view = context_agent_view_from_params(params, &envelope);

    serde_json::json!({
        "enabled": true,
        "source": source,
        "envelope": envelope,
        "lean_probe": lean_probe,
        "policy_decision": policy_decision,
        "cache_stability": cache_stability,
        "mode_coverage": mode_coverage,
        "snapshot": snapshot,
        "budget_explanation": budget_explanation,
        "agent_view": agent_view,
    })
}

fn context_agent_view_from_params(
    params: &HashMap<String, String>,
    envelope: &ContextEnvelope,
) -> Option<runtime::AgentContextView> {
    let agent_id = params
        .get("agent_id")
        .or_else(|| params.get("agent"))
        .map(String::as_str)?
        .trim();
    if agent_id.is_empty() {
        return None;
    }
    let allowed_sources = params
        .get("agent_sources")
        .map(|raw| {
            raw.split(',')
                .filter_map(parse_context_source_kind)
                .collect::<Vec<_>>()
        })
        .filter(|sources| !sources.is_empty())
        .unwrap_or_else(|| {
            vec![
                ContextSourceKind::Task,
                ContextSourceKind::Workspace,
                ContextSourceKind::Memory,
                ContextSourceKind::AgentPeer,
            ]
        });
    Some(RuntimeContextBoundary::agent_context_view(
        envelope,
        AgentContextLease {
            parent_session_id: envelope.identity.session_id.clone(),
            parent_agent_id: envelope.identity.agent_id.clone(),
            child_agent_id: agent_id.to_string(),
            task_contract: params
                .get("agent_task")
                .cloned()
                .unwrap_or_else(|| envelope.intent.clone()),
            allowed_sources,
            max_tokens: params
                .get("agent_budget")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(4_000),
            required_return: vec![
                AgentReturnRequirement::ResultSummary,
                AgentReturnRequirement::Evidence,
                AgentReturnRequirement::Conflicts,
            ],
        },
    ))
}

fn parse_context_source_kind(value: &str) -> Option<ContextSourceKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "stablehead" | "stable_head" => Some(ContextSourceKind::StableHead),
        "runtimeheader" | "runtime_header" => Some(ContextSourceKind::RuntimeHeader),
        "conversation" => Some(ContextSourceKind::Conversation),
        "memory" => Some(ContextSourceKind::Memory),
        "task" => Some(ContextSourceKind::Task),
        "tooltrace" | "tool_trace" => Some(ContextSourceKind::ToolTrace),
        "workspace" => Some(ContextSourceKind::Workspace),
        "agentpeer" | "agent_peer" => Some(ContextSourceKind::AgentPeer),
        "handoff" => Some(ContextSourceKind::Handoff),
        _ => None,
    }
}

fn parse_context_profile(value: &str) -> Option<ContextProfile> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mainturn" | "main" => Some(ContextProfile::MainTurn),
        "sologoal" | "solo" => Some(ContextProfile::SoloGoal),
        "yologoal" | "yolo" => Some(ContextProfile::YoloGoal),
        "subagent" | "sub_agent" => Some(ContextProfile::SubAgent),
        "collaboration" => Some(ContextProfile::Collaboration),
        "review" => Some(ContextProfile::Review),
        "resume" => Some(ContextProfile::Resume),
        "cron" => Some(ContextProfile::Cron),
        _ => None,
    }
}

fn resource_context_item(resource: ExternalResourceRef) -> ContextItem {
    let mut content = format!(
        "resource: {}\nref: {}\nprovider: {}\ntype: {}\nindexed_state: {}",
        resource.title,
        resource.reference,
        resource.provider,
        resource.resource_type,
        resource.indexed_state
    );
    if matches!(resource.indexed_state.as_str(), "stale" | "degraded") {
        content.push_str(
            "\nwarning: resource metadata may be stale or degraded; resolve evidence before relying on details",
        );
    }
    if resource.provider == "feishu" {
        content.push_str(
            "\nbody_policy: metadata_only\nretrieval: use an authorized Feishu read capability before injecting body content",
        );
    }
    let mut item = ContextItem::new(
        resource.reference.clone(),
        ContextSourceKind::Workspace,
        ContextRole::Evidence,
        content,
    );
    item.authority = ContextAuthority::Derived;
    item.visibility = ContextVisibility::Shared;
    item.score = if resource.indexed_state == "stale" {
        0.45
    } else {
        0.7
    };
    item.evidence = vec![resource.reference];
    item
}
