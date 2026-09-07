use super::helpers::{semantic_terms, DispatchMode};
use super::*;

#[derive(Debug, Clone)]
pub(super) struct AgenticExecutionAdmission {
    pub(super) catalog_entry: AgentCatalogEntry,
    pub(super) effective_capabilities: Vec<AgentCapability>,
    pub(super) allowed_tools: Vec<String>,
    pub(super) allowed_skills: Vec<String>,
    pub(super) permission_ceiling: PermissionMode,
}

#[derive(Debug, Clone, Default)]
pub(super) struct AgenticToolHostSnapshot {
    pub(super) tools: BTreeSet<String>,
}

pub(super) fn resolve_agentic_execution_admission(
    services: &RuntimeServices,
    member: &AgentMemberProjection,
    task: &AgenticTaskProjection,
    context: &AgenticDispatchContext,
    mode: DispatchMode,
) -> Result<AgenticExecutionAdmission, String> {
    let required = required_capabilities(member, task, mode);
    let catalog_entry = select_catalog_entry(
        &services.agent_runtime().catalog().all(),
        member,
        task,
        &required,
    )?;
    if let Some(required_profile) = member.model_profile_ref.as_deref() {
        let resolved = services
            .definition_registry()
            .resolve_agent(
                &catalog_entry.definition_ref.definition_id,
                harness_contract::agent::RevisionSelector::ExactApprovedRevision {
                    revision: catalog_entry.definition_ref.revision,
                },
            )
            .map_err(|error| format!("required Agent Definition is not resolvable: {error}"))?;
        if resolved.revision.manifest.model_policy.profile != required_profile {
            return Err(format!(
                "selected Agent Definition model profile `{}` does not satisfy required profile `{required_profile}`",
                resolved.revision.manifest.model_policy.profile
            ));
        }
    }
    let effective_capabilities = required
        .iter()
        .map(|capability| {
            capability_from_name(capability).ok_or_else(|| {
                format!("Agent Definition catalog exposed unsupported capability `{capability}`")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let skill_catalog = services.skill_catalog();
    let (allowed_skills, skill_tool_refs) = effective_skill_grants(&catalog_entry, &skill_catalog);
    let resolved = resolve_agent_capability(AgentCapabilityRequest {
        role_id: member.agent_id.clone(),
        allowed_capabilities: required.clone(),
        evidence_duties: vec![task.acceptance.clone()],
    });
    let mut requested_tools = resolved.allowed_tools.clone();
    requested_tools.extend(AGENT_ACTION_TOOL_IDS.iter().map(|tool| (*tool).to_string()));
    requested_tools.extend(skill_tool_refs.iter().cloned());

    let session_ceiling = services
        .session_execution_policy(&context.session_id)
        .map(|policy| lower_permission(context.permission_ceiling, policy.permission_mode))
        .unwrap_or(context.permission_ceiling);
    let host_snapshot = services.tool_execution_host().map(|host| {
        snapshot_agentic_tools(
            host.as_ref(),
            &requested_tools,
            session_ceiling,
            &context.resource_scopes,
        )
    });
    let allowed_tools = intersect_agentic_tools(
        &resolved,
        &requested_tools,
        &skill_tool_refs,
        host_snapshot.as_ref(),
    )?;

    Ok(AgenticExecutionAdmission {
        catalog_entry,
        effective_capabilities,
        allowed_tools,
        allowed_skills,
        permission_ceiling: session_ceiling,
    })
}

pub(super) fn required_capabilities(
    member: &AgentMemberProjection,
    task: &AgenticTaskProjection,
    mode: DispatchMode,
) -> Vec<String> {
    let mut required = BTreeSet::from(["read".to_string()]);
    let hints = member
        .required_capabilities
        .iter()
        .chain(member.execution_requirements.iter())
        // Review consumes the producer's evidence; it does not automatically
        // repeat every producer effect. The reviewer declares its own needs.
        .chain(
            task.required_capabilities
                .iter()
                .filter(|_| mode == DispatchMode::Execute),
        )
        .chain(
            task.execution_requirements
                .iter()
                .filter(|_| mode == DispatchMode::Execute),
        )
        .map(|capability| normalize_capability(capability))
        .filter(|capability| !capability.is_empty())
        .collect::<Vec<_>>();
    for hint in &hints {
        if capability_from_name(hint).is_some() {
            required.insert(hint.clone());
        }
        for term in capability_terms(hint) {
            for capability in physical_capabilities_for_term(term) {
                required.insert((*capability).to_string());
            }
        }
    }
    required.into_iter().collect()
}

pub(super) fn normalize_capability(capability: &str) -> String {
    capability.trim().replace('-', "_").to_ascii_lowercase()
}

fn capability_terms(capability: &str) -> impl Iterator<Item = &str> {
    capability
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|term| !term.is_empty())
}

/// Translate model-authored expertise labels into Runtime-owned execution
/// effects. These aliases describe generic effects, not business domains.
/// Unknown labels remain semantic ranking context and intentionally grant no
/// extra tool authority.
fn physical_capabilities_for_term(term: &str) -> &'static [&'static str] {
    match term {
        "read" | "reader" | "inspect" | "inspection" => &["read"],
        "search" | "research" | "explore" | "lookup" | "retrieval" | "source" | "sources"
        | "citation" | "citations" | "evidence" | "gathering" => &["search"],
        "web" | "online" | "external" | "internet" | "network" | "url" | "urls" => &["network"],
        "write" | "writer" | "writeup" | "edit" | "author" | "implementation" | "implement"
        | "artifact" | "report" | "script" | "code" => &["write"],
        "test" | "testing" | "verify" | "verification" | "validate" | "validation" | "check"
        | "experiment" | "experimental" | "benchmark" | "counterexample" => &["test"],
        // A Python capability request normally means the worker must author
        // and execute code. The immutable Program permission/resource scopes
        // still decide whether those effects are physically available.
        "python" => &["write", "test"],
        _ => &[],
    }
}

pub(super) fn select_catalog_entry(
    catalog: &[AgentCatalogEntry],
    member: &AgentMemberProjection,
    task: &AgenticTaskProjection,
    required: &[String],
) -> Result<AgentCatalogEntry, String> {
    let required = required.iter().cloned().collect::<BTreeSet<_>>();
    let demand_terms = semantic_terms(&format!(
        "{} {} {} {} {} {} {} {}",
        member.role,
        member.mission,
        member.expertise_hints.join(" "),
        task.title,
        task.objective,
        task.acceptance,
        task.expertise_hints.join(" "),
        task.execution_requirements.join(" "),
    ));
    catalog
        .iter()
        .filter(|entry| {
            member.definition_ref.as_ref().is_none_or(|required_ref| {
                required_ref == entry.definition_ref.definition_id.as_str()
                    || required_ref
                        == &format!(
                            "{}@{}",
                            entry.definition_ref.definition_id.as_str(),
                            entry.definition_ref.revision
                        )
            })
        })
        .filter_map(|entry| {
            let capabilities = entry
                .capabilities
                .iter()
                .map(|capability| normalize_capability(capability))
                .collect::<BTreeSet<_>>();
            required.is_subset(&capabilities).then(|| {
                let entry_terms = semantic_terms(&format!(
                    "{} {} {}",
                    entry.agent_id, entry.name, entry.description
                ));
                let semantic_relevance = entry_terms.intersection(&demand_terms).count();
                let surplus_capabilities = capabilities.len().saturating_sub(required.len());
                (
                    (
                        surplus_capabilities,
                        Reverse(semantic_relevance),
                        entry.definition_ref.definition_id.as_str().to_string(),
                        entry.definition_ref.revision,
                    ),
                    entry.clone(),
                )
            })
        })
        .min_by(|left, right| left.0.cmp(&right.0))
        .map(|(_, entry)| entry)
        .ok_or_else(|| {
            let requested_definition = member
                .definition_ref
                .as_deref()
                .map(|value| format!(" for required Definition {value}"))
                .unwrap_or_default();
            format!(
                "no Agent Definition{requested_definition} satisfies required physical capabilities: {}",
                required.into_iter().collect::<Vec<_>>().join(",")
            )
        })
}

pub(super) fn capability_from_name(capability: &str) -> Option<AgentCapability> {
    match capability {
        "read" => Some(AgentCapability::Read),
        "search" => Some(AgentCapability::Search),
        "write" => Some(AgentCapability::Write),
        "test" => Some(AgentCapability::Test),
        "network" => Some(AgentCapability::Network),
        "connector_action" => Some(AgentCapability::ConnectorAction),
        "matrix_write" => Some(AgentCapability::MatrixWrite),
        _ => None,
    }
}

pub(super) fn effective_skill_grants(
    entry: &AgentCatalogEntry,
    catalog: &RuntimeSkillCatalog,
) -> (Vec<String>, BTreeSet<String>) {
    let profiles = catalog.profiles();
    let mut allowed_skills = entry
        .skill_refs
        .iter()
        .filter(|skill_ref| {
            profiles.iter().any(|profile| {
                matches!(
                    profile.lifecycle_status,
                    harness_contract::skill::SkillLifecycleStatus::UsablePrompt
                        | harness_contract::skill::SkillLifecycleStatus::UsableRuntime
                ) && skill_ref_matches_profile(skill_ref, profile)
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    allowed_skills.sort();
    allowed_skills.dedup();
    let tool_refs = catalog
        .prompt_assets()
        .iter()
        .filter(|asset| {
            allowed_skills.iter().any(|skill_ref| {
                skill_ref.eq_ignore_ascii_case(&asset.skill_id)
                    || asset.version.as_ref().is_some_and(|version| {
                        skill_ref.eq_ignore_ascii_case(&format!("{}@{version}", asset.skill_id))
                    })
            })
        })
        .flat_map(|asset| asset.tool_refs.iter().cloned())
        .collect::<BTreeSet<_>>();
    (allowed_skills, tool_refs)
}

pub(super) fn skill_ref_matches_profile(
    skill_ref: &str,
    profile: &harness_contract::skill::SkillCapabilityProfile,
) -> bool {
    skill_ref.eq_ignore_ascii_case(&profile.skill_id)
        || skill_ref.eq_ignore_ascii_case(&profile.name)
        || profile.version.as_ref().is_some_and(|version| {
            skill_ref.eq_ignore_ascii_case(&format!("{}@{version}", profile.skill_id))
        })
}

pub(super) fn lower_permission(left: PermissionMode, right: PermissionMode) -> PermissionMode {
    if left.rank() <= right.rank() {
        left
    } else {
        right
    }
}

pub(super) fn snapshot_agentic_tools(
    host: &dyn crate::RuntimeExecutionHost,
    requested_tools: &BTreeSet<String>,
    permission_ceiling: PermissionMode,
    resource_scopes: &[String],
) -> AgenticToolHostSnapshot {
    let requested = requested_tools.iter().cloned().collect::<Vec<_>>();
    let tools = host
        .delegated_tool_definitions(&requested)
        .into_iter()
        .filter_map(|definition| {
            let effect =
                host.delegated_tool_effect_descriptor(&definition.name, &serde_json::json!({}))?;
            (permission_ceiling.permits(effect.required_permission)
                && delegated_tool_effect_is_bounded(&effect)
                && tool_effect_has_resource_lease(&definition.name, &effect, resource_scopes))
            .then_some(definition.name)
        })
        .collect();
    AgenticToolHostSnapshot { tools }
}

pub(super) fn tool_effect_has_resource_lease(
    tool_name: &str,
    effect: &harness_contract::tool::ToolEffectDescriptor,
    resource_scopes: &[String],
) -> bool {
    if AGENT_ACTION_TOOL_IDS.contains(&tool_name)
        || matches!(tool_name, "context_retrieve" | "evidence_retrieve")
    {
        return true;
    }
    let requested = crate::governed_tool_plan::resource_scope_from_effect(effect);
    if requested.network {
        return resource_scopes.iter().any(|scope| scope == "network:*");
    }
    if effect.spawns_process {
        return resource_scopes.iter().any(|scope| {
            matches!(
                scope.trim(),
                "read:." | "read:./" | "write:." | "write:./" | "workspace:."
            )
        });
    }
    if requested.kind == "runtime" || requested.paths.is_empty() {
        return true;
    }
    resource_scopes.iter().any(|scope| {
        matches!(
            scope.split_once(':').map(|(kind, _)| kind),
            Some("read" | "write" | "worktree" | "workspace")
        )
    })
}

pub(super) fn intersect_agentic_tools(
    resolved: &ResolvedAgentCapability,
    requested_tools: &BTreeSet<String>,
    skill_tool_refs: &BTreeSet<String>,
    host_snapshot: Option<&AgenticToolHostSnapshot>,
) -> Result<Vec<String>, String> {
    let Some(host_snapshot) = host_snapshot else {
        return Ok(requested_tools.iter().cloned().collect());
    };
    let host_tools = &host_snapshot.tools;
    let host_binding = bind_agent_capability_to_host(resolved, host_tools);
    if !host_binding.missing_capabilities.is_empty() {
        return Err(format!(
            "Agent execution capabilities are unavailable after policy and ToolHost intersection: {}; acceptable tools: {}",
            host_binding.missing_capabilities.join(","),
            host_binding.missing_tool_alternatives.join(",")
        ));
    }
    let required_tools = AGENT_ACTION_TOOL_IDS
        .iter()
        .map(|tool| (*tool).to_string())
        .chain(skill_tool_refs.iter().cloned())
        .collect::<BTreeSet<_>>();
    let unavailable = required_tools
        .difference(host_tools)
        .cloned()
        .collect::<Vec<_>>();
    if !unavailable.is_empty() {
        return Err(format!(
            "Agent execution tool contracts are unavailable after policy, resource and ToolHost intersection: {}",
            unavailable.join(",")
        ));
    }
    let missing_effect_capabilities = resolved
        .requested_capabilities
        .iter()
        .filter(|capability| match capability.as_str() {
            "connector_action" => !host_tools.iter().any(|tool| {
                tool == "mcp_tool"
                    || crate::agent::binding::capability_required_by_tool_contract(tool)
                        == AgentCapability::ConnectorAction
            }),
            "matrix_write" => !host_tools.iter().any(|tool| {
                crate::agent::binding::capability_required_by_tool_contract(tool)
                    == AgentCapability::MatrixWrite
            }),
            _ => false,
        })
        .cloned()
        .collect::<Vec<_>>();
    if !missing_effect_capabilities.is_empty() {
        return Err(format!(
            "Agent execution capabilities lack a concrete bounded tool contract: {}",
            missing_effect_capabilities.join(",")
        ));
    }
    Ok(requested_tools.intersection(&host_tools).cloned().collect())
}
