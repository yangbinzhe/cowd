use super::helpers::{semantic_terms, DispatchMode};
use super::*;

#[derive(Debug, Clone)]
pub(super) struct AgenticExecutionAdmission {
    pub(super) catalog_entry: AgentCatalogEntry,
    pub(super) effective_capabilities: Vec<AgentCapability>,
    pub(super) allowed_tools: Vec<String>,
    pub(super) allowed_skills: Vec<String>,
    pub(super) skill_tool_pins: Vec<harness_contract::skill::SkillInstructionPin>,
    pub(super) skill_tool_contributions: Vec<String>,
    pub(super) permission_ceiling: PermissionMode,
    /// Requested capabilities that had no usable tool in the admitted ToolHost
    /// snapshot (diagnostic; a degradation, not a failure).
    pub(super) missing_capabilities: Vec<String>,
    /// The tool alternatives that would have satisfied those capabilities.
    pub(super) missing_tool_alternatives: Vec<String>,
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
    let skill_catalog = services.skill_catalog();
    let (allowed_skills, skill_tool_refs, skill_tool_pins) =
        effective_skill_grants(&catalog_entry, &skill_catalog)?;
    let resolved = resolve_agent_capability(AgentCapabilityRequest {
        role_id: member.agent_id.clone(),
        allowed_capabilities: required.clone(),
        evidence_duties: vec![task.acceptance.clone()],
    });
    let mut requested_tools = resolved.allowed_tools.clone();
    requested_tools.extend(AGENT_ACTION_TOOL_IDS.iter().map(|tool| (*tool).to_string()));
    requested_tools.insert("artifact_publish".into());
    if required.iter().any(|capability| capability == "write") {
        requested_tools.insert("artifact_materialize".into());
    }

    let independent_tools = requested_tools.clone();
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
    let (mut allowed_tools, missing_capabilities, missing_tool_alternatives) =
        intersect_agentic_tools(
            &resolved,
            &requested_tools,
            &skill_tool_refs,
            host_snapshot.as_ref(),
        )?;
    let effective_capabilities =
        effective_capabilities_after_intersection(&required, &missing_capabilities)?;
    constrain_tools_to_effective_capabilities(&mut allowed_tools, &effective_capabilities);

    let skill_tool_contributions =
        contributed_skill_tools(&skill_tool_refs, &independent_tools, &allowed_tools);
    Ok(AgenticExecutionAdmission {
        catalog_entry,
        effective_capabilities,
        allowed_tools,
        allowed_skills,
        skill_tool_pins,
        skill_tool_contributions,
        permission_ceiling: session_ceiling,
        missing_capabilities,
        missing_tool_alternatives,
    })
}

pub(super) fn contributed_skill_tools(
    skill: &BTreeSet<String>,
    independent: &BTreeSet<String>,
    admitted: &[String],
) -> Vec<String> {
    skill
        .difference(independent)
        .filter(|tool| admitted.contains(tool))
        .cloned()
        .collect()
}

fn constrain_tools_to_effective_capabilities(
    tools: &mut Vec<String>,
    capabilities: &[AgentCapability],
) {
    tools.retain(|tool| crate::agent::binding::tool_contract_is_authorized(tool, capabilities));
}

pub(super) fn effective_capabilities_after_intersection(
    requested: &[String],
    missing: &[String],
) -> Result<Vec<AgentCapability>, String> {
    requested
        .iter()
        .filter(|capability| !missing.contains(capability))
        .map(|capability| {
            capability_from_name(capability).ok_or_else(|| {
                format!("Agent Definition catalog exposed unsupported capability `{capability}`")
            })
        })
        .collect()
}

pub(super) fn capability_gap_constraints(
    missing_capabilities: &[String],
    missing_tool_alternatives: &[String],
) -> Vec<String> {
    if missing_capabilities.is_empty() {
        return Vec::new();
    }
    let alternatives = if missing_tool_alternatives.is_empty() {
        "no usable alternative tool is admitted".to_string()
    } else {
        format!(
            "available alternatives were not admitted: {}",
            missing_tool_alternatives.join(", ")
        )
    };
    missing_capabilities
        .iter()
        .map(|capability| {
            format!(
                "Runtime capability gap: `{capability}` was requested but is not authorized by the admitted ToolHost/resource intersection; {alternatives}. Do not claim work or evidence requiring this capability was performed. If the bound Task acceptance requires it, complete all independent authorized work and record the exact unmet evidence in `unresolved`; do not infer or simulate external access."
            )
        })
        .collect()
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
) -> Result<
    (
        Vec<String>,
        BTreeSet<String>,
        Vec<harness_contract::skill::SkillInstructionPin>,
    ),
    String,
> {
    let profiles = catalog.profiles()?;
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
    let pins = if allowed_skills.is_empty() {
        Vec::new()
    } else {
        catalog
            .instruction_source()
            .ok_or("Skill grants require the original approved instruction source")?
            .required_tool_pins(&allowed_skills)?
    };
    let tool_refs = pins
        .iter()
        .flat_map(|pin| pin.tool_refs.iter().cloned())
        .collect();
    Ok((allowed_skills, tool_refs, pins))
}

pub(super) fn skill_ref_matches_profile(
    reference: &str,
    profile: &harness_contract::skill::SkillCapabilityProfile,
) -> bool {
    profile.matches_reference(reference)
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
    let Some(lease) = host.pin_step_catalog() else {
        return AgenticToolHostSnapshot::default();
    };
    let metadata = lease.catalog_receipt();
    let projection =
        crate::tool_host::DelegatedCatalogProjection::new(&lease, &metadata, requested_tools);
    let exposed = host.delegated_tool_definitions(&requested);
    let tools = projection
        .definitions()
        .filter(|definition| {
            exposed.iter().any(|actual| {
                actual.name == definition.name
                    && actual.input_schema == definition.input_schema
                    && actual.description == definition.description
            })
        })
        .filter_map(|definition| {
            projection
                .discover(
                    definition,
                    requested_tools,
                    permission_ceiling,
                    Some(resource_scopes),
                )
                .map(|descriptor| descriptor.canonical_id)
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
        || matches!(
            tool_name,
            "context_retrieve"
                | "working_context"
                | "private_note"
                | "evidence_retrieve"
                | "artifact_publish"
        )
    {
        return true;
    }
    crate::tool_host::effect_has_resource_lease(
        effect,
        resource_scopes,
        crate::tool_host::lexical_resource_path_is_authorized,
    )
}

// Return the admitted tool subset plus the missing-capability diagnostics.
pub(super) fn intersect_agentic_tools(
    resolved: &ResolvedAgentCapability,
    requested_tools: &BTreeSet<String>,
    skill_tool_refs: &BTreeSet<String>,
    host_snapshot: Option<&AgenticToolHostSnapshot>,
) -> Result<(Vec<String>, Vec<String>, Vec<String>), String> {
    let Some(host_snapshot) = host_snapshot else {
        return Ok((
            requested_tools.iter().cloned().collect(),
            Vec::new(),
            Vec::new(),
        ));
    };
    let host_tools = &host_snapshot.tools;
    let host_binding = bind_agent_capability_to_host(resolved, host_tools);
    if !host_binding.missing_capabilities.is_empty() {
        // A requested capability without a usable tool is a *degradation*, not
        // a dispatch failure. Returning Err here turned one unavailable tool
        // into a "no admissible member" starvation loop. Admit with the
        // available tool subset and record the gap, so the Agent can proceed
        // (and report the limitation) while the missing contract stays visible.
        tracing::warn!(
            missing_capabilities = %host_binding.missing_capabilities.join(","),
            acceptable_tools = %host_binding.missing_tool_alternatives.join(","),
            "agent admission degraded: requested capability lacks a usable tool"
        );
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
    Ok((
        requested_tools.intersection(&host_tools).cloned().collect(),
        host_binding.missing_capabilities,
        host_binding.missing_tool_alternatives,
    ))
}

#[cfg(test)]
mod capability_truth_tests {
    use super::{
        capability_gap_constraints, effective_capabilities_after_intersection,
        intersect_agentic_tools, AgenticToolHostSnapshot,
    };
    use harness_contract::agent::AgentCapability;

    #[test]
    fn r1_composite_effect_admission_never_widens_mixed_grants() {
        struct CatalogHost(tools::ToolHostLease);
        #[async_trait::async_trait]
        impl crate::RuntimeExecutionHost for CatalogHost {
            fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
                Some(self.0.clone())
            }

            fn delegated_tool_definitions(
                &self,
                names: &[String],
            ) -> Vec<crate::ProviderToolDefinition> {
                self.0
                    .snapshot()
                    .catalog
                    .definitions(None)
                    .into_iter()
                    .filter(|definition| names.contains(&definition.name))
                    .map(|definition| crate::ProviderToolDefinition {
                        name: definition.name,
                        description: definition.description,
                        input_schema: definition.input_schema,
                    })
                    .collect()
            }

            fn delegated_tool_effect_descriptor(
                &self,
                name: &str,
                input: &serde_json::Value,
            ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
                Some(self.0.describe_effect(name, input))
            }
            async fn execute_runtime_tool(
                &self,
                _: &crate::RuntimeToolExecutionRequest,
            ) -> crate::RuntimeToolExecutionOutcome {
                panic!("admission never invokes a tool")
            }
        }
        let root = tempfile::tempdir().unwrap();
        let host = CatalogHost(
            tools::ToolHost::builtin("admission-resource-frame", root.path()).pin_snapshot(),
        );
        let mixed = serde_json::json!({"steps":[
            {"id":"edit","op":"call","tool":"edit","input":{"action":"write","path":"held.txt","content":"x"}},
            {"id":"read","op":"call","tool":"read_file","input":{"path":"source.txt"}},
            {"id":"fetch","op":"call","tool":"web_fetch","input":{"url":"https://example.invalid","prompt":"summarize"}}
        ]});
        let effect = host.0.describe_effect("tool_orchestrate", &mixed);
        let permits = |effect: &harness_contract::tool::ToolEffectDescriptor, scopes: &[&str]| {
            super::tool_effect_has_resource_lease(
                "tool_orchestrate",
                effect,
                &scopes
                    .iter()
                    .map(|scope| (*scope).to_string())
                    .collect::<Vec<_>>(),
            )
        };
        for denied in [
            vec!["network:*"],
            vec!["write:held.txt", "read:source.txt"],
            vec!["network:*", "write:wrong.txt", "read:source.txt"],
            vec!["network:*", "read:held.txt", "read:source.txt"],
            vec!["network:*", "write:held.txt"],
        ] {
            assert!(!permits(&effect, &denied));
        }
        let complete = [
            "network:*",
            "write:held.txt",
            "read:source.txt",
            "write:.cowd/checkpoints",
        ];
        assert!(permits(&effect, &complete));
        for missing in 0..complete.len() {
            let mut narrow = complete.to_vec();
            narrow.remove(missing);
            assert!(!permits(&effect, &narrow));
        }
        let compute = serde_json::json!({"steps":[
            {"id":"edit","op":"call","tool":"edit","input":{"action":"write","path":"held.txt","content":"x"}},
            {"id":"compute","op":"compute","language":"python","code":"print(1)"}
        ]});
        let compute = host.0.describe_effect("tool_orchestrate", &compute);
        assert!(compute.spawns_process && !compute.uses_network);
        assert!(!permits(&compute, &["read:."]));
        assert!(!permits(&compute, &["write:held.txt"]));
        assert!(!permits(&compute, &["read:.", "write:held.txt"]));
        assert!(permits(
            &compute,
            &["read:.", "write:held.txt", "write:.cowd/checkpoints"]
        ));
        let pure = host.0.describe_effect(
            "tool_orchestrate",
            &serde_json::json!({"steps":[
                {"id":"compute","op":"compute","language":"python","code":"print(1)"}
            ]}),
        );
        assert!(permits(&pure, &["read:."]));
        assert!(!permits(&pure, &["read:held.txt"]));
        let network = host
            .0
            .describe_effect("web_fetch", &mixed["steps"][2]["input"]);
        assert!(super::tool_effect_has_resource_lease(
            "web_fetch",
            &network,
            &["network:*".into()]
        ));
        assert!(!super::tool_effect_has_resource_lease(
            "web_fetch",
            &network,
            &["read:.".into()]
        ));

        let requested =
            std::collections::BTreeSet::from(["read_file".into(), "tool_orchestrate".into()]);
        let scopes = vec!["read:held.txt".to_string()];
        let snapshot = super::snapshot_agentic_tools(
            &host,
            &requested,
            harness_contract::tool::ToolPermissionMode::ReadOnly,
            &scopes,
        );
        assert_eq!(snapshot.tools, requested);
        assert_eq!(scopes, vec!["read:held.txt"]);
        let probe = host.0.describe_effect("read_file", &serde_json::json!({}));
        assert!(!super::tool_effect_has_resource_lease(
            "read_file",
            &probe,
            &scopes
        ));
        assert!(super::tool_effect_has_resource_lease(
            "read_file",
            &probe,
            &["read:.".into()]
        ));
        let network = super::snapshot_agentic_tools(
            &host,
            &requested,
            harness_contract::tool::ToolPermissionMode::ReadOnly,
            &["network:*".into()],
        );
        assert!(network.tools.contains("tool_orchestrate"));
        assert!(!network.tools.contains("read_file"));
        assert!(!permits(&effect, &["network:*"]));
        let empty = super::snapshot_agentic_tools(
            &host,
            &requested,
            harness_contract::tool::ToolPermissionMode::ReadOnly,
            &[],
        );
        assert!(empty.tools.is_empty());
        let unregistered = super::snapshot_agentic_tools(
            &host,
            &std::collections::BTreeSet::from(["unregistered".into()]),
            harness_contract::tool::ToolPermissionMode::ReadOnly,
            &["read:.".into()],
        );
        assert!(unregistered.tools.is_empty());
        for scopes in [
            vec!["read:src/../secret".into()],
            vec!["execute:held.txt".into()],
        ] {
            assert!(super::snapshot_agentic_tools(
                &host,
                &requested,
                harness_contract::tool::ToolPermissionMode::ReadOnly,
                &scopes,
            )
            .tools
            .is_empty());
        }
        let edit = super::snapshot_agentic_tools(
            &host,
            &std::collections::BTreeSet::from(["edit".into()]),
            harness_contract::tool::ToolPermissionMode::ReadOnly,
            &["write:held.txt".into()],
        );
        assert!(edit.tools.is_empty());
    }

    #[test]
    fn capability_grants_exclude_tools_missing_after_host_intersection() {
        let requested = vec!["read".to_string(), "network".to_string()];
        let effective =
            effective_capabilities_after_intersection(&requested, &["network".to_string()])
                .expect("known requested capabilities");
        assert_eq!(effective, vec![AgentCapability::Read]);
        let effective = effective_capabilities_after_intersection(
            &["read".into(), "write".into()],
            &["write".into()],
        )
        .unwrap();
        let mut tools = vec![
            "read_file".into(),
            "task_claim".into(),
            "edit".into(),
            "artifact_materialize".into(),
        ];
        super::constrain_tools_to_effective_capabilities(&mut tools, &effective);
        assert_eq!(
            tools,
            vec!["read_file".to_string(), "task_claim".to_string()]
        );
        let mut tools = vec!["edit".into(), "artifact_materialize".into()];
        super::constrain_tools_to_effective_capabilities(
            &mut tools,
            &[AgentCapability::Read, AgentCapability::Write],
        );
        assert_eq!(
            tools,
            vec!["edit".to_string(), "artifact_materialize".to_string()]
        );
    }

    #[test]
    fn host_intersection_controls_network_tools_grants_and_gap_constraints_together() {
        let requested_capabilities = vec!["read".to_string(), "network".to_string()];
        let resolved = crate::resolve_agent_capability(crate::AgentCapabilityRequest {
            role_id: "network-bound-worker".to_string(),
            allowed_capabilities: requested_capabilities.clone(),
            evidence_duties: vec!["verify fresh external evidence".to_string()],
        });
        let mut requested_tools = resolved.allowed_tools.clone();
        requested_tools.extend(
            harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
                .iter()
                .map(|tool| (*tool).to_string()),
        );
        for network_available in [false, true] {
            let mut tools = requested_tools.clone();
            if !network_available {
                tools.retain(|tool| {
                    !matches!(tool.as_str(), "web_search" | "web_fetch" | "mcp_tool")
                });
            }
            let snapshot = AgenticToolHostSnapshot { tools };
            let (allowed, missing, alternatives) = intersect_agentic_tools(
                &resolved,
                &requested_tools,
                &std::collections::BTreeSet::new(),
                Some(&snapshot),
            )
            .expect("missing network must remain a runnable degraded admission");
            let grants =
                effective_capabilities_after_intersection(&requested_capabilities, &missing)
                    .expect("known capabilities");
            let constraints = capability_gap_constraints(&missing, &alternatives);
            assert_eq!(
                allowed.iter().any(|tool| tool == "web_search"),
                network_available
            );
            assert_eq!(
                grants.contains(&AgentCapability::Network),
                network_available
            );
            assert_eq!(constraints.is_empty(), network_available);
            if !network_available {
                assert_eq!(missing, ["network"]);
                assert!(constraints[0].contains("unresolved"));
                assert!(grants.contains(&AgentCapability::Read));
            }
        }
    }

    #[test]
    fn missing_network_is_explicit_without_granting_an_alternative() {
        let constraints = capability_gap_constraints(
            &["network".to_string()],
            &["web_search".to_string(), "web_fetch".to_string()],
        );
        assert_eq!(constraints.len(), 1);
        assert!(constraints[0].contains("network"));
        assert!(constraints[0].contains("web_search"));
        assert!(constraints[0].contains("web_fetch"));
        assert!(constraints[0].contains("unresolved"));
        assert!(constraints[0].contains("not authorized"));
    }
}
