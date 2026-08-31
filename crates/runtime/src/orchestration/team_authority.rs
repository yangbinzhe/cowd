//! Runtime-owned Team resource authority.
//!
//! Callers may request collaboration and suggest a published template, but
//! only Runtime derives filesystem, network, and session evidence leases.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use harness_contract::orchestration::ManagedAgentEscalationRequirement;
use harness_contract::team::{FocusPartitionPlan, FocusPartitionSlot};

#[cfg(test)]
use crate::execution_core::RuntimeExecutionDecision;
use crate::orchestration::{
    CapabilityRecipeId, GraphSemanticNode, RuntimeOrchestrationCommand, SemanticFocus,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum TeamAuthorityProfile {
    WorkspaceRead,
    ExternalResearch,
    WorkspaceWrite,
}

/// Canonical semantic contract for one Runtime-owned node in an explicit
/// multi-Team request. Both proactive strategy execution and the provider
/// recovery path consume this contract so template, artifact, and acceptance
/// semantics cannot drift between the two entry points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExplicitTeamNodeContract {
    pub template: &'static str,
    pub output_artifacts: &'static [&'static str],
    pub evidence_contract: &'static [&'static str],
}

#[must_use]
pub(crate) fn explicit_team_node_contract(
    index: usize,
    team_count: usize,
    requires_write: bool,
    requires_external_facts: bool,
) -> ExplicitTeamNodeContract {
    let writer = requires_write && index + 1 == team_count;
    if writer {
        return ExplicitTeamNodeContract {
            template: "cowd/execute-review",
            output_artifacts: &["workspace_change", "terminal_synthesis"],
            evidence_contract: &["implementation", "source_verification", "evidence", "risks"],
        };
    }
    if requires_external_facts {
        return ExplicitTeamNodeContract {
            template: "cowd/external-research-synthesis",
            output_artifacts: &["terminal_synthesis"],
            evidence_contract: &["summary", "evidence", "unresolved"],
        };
    }
    if team_count == 1 {
        return ExplicitTeamNodeContract {
            template: "cowd/parallel-research-synthesis",
            output_artifacts: &["terminal_synthesis"],
            evidence_contract: &["summary", "evidence", "unresolved"],
        };
    }
    ExplicitTeamNodeContract {
        template: "cowd/parallel-research-synthesis",
        output_artifacts: &["terminal_synthesis"],
        evidence_contract: &["summary", "evidence", "unresolved"],
    }
}

#[cfg(test)]
pub(crate) fn bind_semantic_resource_authority(
    request: &mut RuntimeOrchestrationCommand,
    leased_decision: Option<&RuntimeExecutionDecision>,
    workspace_root: &Path,
) {
    let inferred = harness_contract::strategy::decide_strategy(
        &harness_contract::strategy::StrategyInput::from_prompt(&request.intent),
    );
    let understanding = leased_decision
        .map(|decision| &decision.strategy.understanding)
        .unwrap_or(&inferred.understanding);
    bind_semantic_resource_authority_with_understanding(request, understanding, workspace_root);
}

pub(crate) fn bind_semantic_resource_authority_with_understanding(
    request: &mut RuntimeOrchestrationCommand,
    understanding: &harness_contract::strategy::TaskUnderstanding,
    workspace_root: &Path,
) {
    let ephemeral_team_nodes = request
        .ephemeral_team_templates
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let ephemeral_write_nodes = request
        .ephemeral_team_templates
        .iter()
        .filter(|(_, snapshot)| {
            snapshot.revision.manifest.roles.iter().any(|role| {
                role.grant_ceiling
                    .contains(&harness_contract::agent::AgentCapability::Write)
            })
        })
        .map(|(node_id, _)| node_id.clone())
        .collect::<BTreeSet<_>>();
    let Some(proposal) = request.proposal.as_mut() else {
        return;
    };
    // A model proposal may narrow an admitted write strategy, but it cannot
    // widen a read-only user intent into workspace mutation authority.
    let requires_write =
        understanding.requires_write && request.constraints.requires_write.unwrap_or(true);
    bind_required_managed_agent_escalation(proposal, understanding, requires_write);
    // Preserve admitted user intent even when the current permission ceiling
    // is too low. The validator/approval path must reject or elevate it;
    // silently rewriting a write goal into read-only work creates a false
    // success contract.
    request.constraints.requires_write = Some(requires_write);
    // The understanding is the semantic authority for collaboration width.
    // An explicit user/model count wins; otherwise use the inferred number
    // of independent workstreams.  `max_parallel_agents` is only a caller
    // supplied resource ceiling, never a hidden request to create 2–6 roles.
    // ResourceManager still owns live admission after this topology is frozen.
    let semantic_requested_count = usize::from(
        understanding
            .required_team_count
            .max(understanding.independent_workstreams)
            .max(1),
    );
    let requested_count = request
        .constraints
        .max_parallel_agents
        .map(|ceiling| semantic_requested_count.min(ceiling.max(1)))
        .unwrap_or(semantic_requested_count);
    let explicit_team = understanding.requests_multi_agent
        || proposal
            .nodes
            .iter()
            .any(|node| node.recipe == CapabilityRecipeId::Team);
    let profiles = proposal
        .nodes
        .iter()
        .map(|node| {
            let profile = (node.recipe == CapabilityRecipeId::Team).then(|| {
                team_authority_profile(
                    node,
                    requires_write,
                    understanding.requires_external_facts,
                    ephemeral_team_nodes
                        .contains(&node.node_id)
                        .then_some(ephemeral_write_nodes.contains(&node.node_id)),
                )
            });
            if node.recipe == CapabilityRecipeId::Team {
                tracing::debug!(
                    node = %node.node_id,
                    template = ?node.template,
                    output_artifacts = ?node.output_artifacts,
                    requires_write,
                    profile = ?profile,
                    "team authority profile"
                );
            }
            profile
        })
        .collect::<Vec<_>>();
    // Ephemeral snapshots deliberately have no catalog selector on their
    // semantic node. Their immutable role topology is nevertheless already
    // known here, so they must follow the same no-builtin-focus branch as a
    // workspace/user custom template. Otherwise this authority pass injects
    // legacy `researcher`/`synthesizer` plans before the compiler sees the
    // snapshot's user-defined role ids.
    let mut profile_positions = BTreeMap::<TeamAuthorityProfile, usize>::new();
    let mut scopes = Vec::new();
    for (index, node) in proposal.nodes.iter_mut().enumerate() {
        if let Some(profile) = profiles[index] {
            let profile_count = profiles
                .iter()
                .filter(|candidate| **candidate == Some(profile))
                .count();
            let team_position = profile_positions.entry(profile).or_default();
            let node_requires_write = profile == TeamAuthorityProfile::WorkspaceWrite;
            let node_uses_external = profile == TeamAuthorityProfile::ExternalResearch;
            let template = node
                .template
                .as_deref()
                .unwrap_or_default()
                .trim_start_matches("builtin/");
            let direct_explicit_research = explicit_team
                && profile == TeamAuthorityProfile::WorkspaceRead
                && template == "cowd/direct-executor"
                && profile_count > 1;
            // User/workspace templates own their role topology. Runtime
            // derives the bounded resource lease but must not inject
            // builtin-role focus partitions (researcher/implementer/...)
            // that the custom template does not declare. A model-declared
            // custom focus set is nevertheless semantic input, not an
            // authority grant: retain it and bind it to the node's bounded
            // Runtime lease. Dropping it makes every multi-role custom
            // template indistinguishable from an empty-role proposal at the
            // later template compiler.
            let custom_template = ephemeral_team_nodes.contains(&node.node_id)
                || template.starts_with("workspace/")
                || template.starts_with("user/");
            // Do not manufacture a minimum number of role slots.  Template
            // cardinality and the AI-authored semantic plan decide topology;
            // this authority layer only partitions the already granted scope.
            let focus_count = requested_count.max(profile_count);
            if custom_template {
                // The narrow collaboration transport deliberately has no
                // physical `resource_scopes` field.  Its semantic
                // `evidence_contract` is the model-facing place to name
                // bounded sources.  Honor only typed evidence_scope entries;
                // if they are absent, retain the existing intent-derived
                // fallback.  This keeps arbitrary Teams flexible while
                // preventing a broad authorization lease from becoming an
                // unverifiable `read:.` acceptance obligation.
                let declared_scopes = declared_evidence_scopes(&node.evidence_contract);
                let custom_scopes = if declared_scopes.is_empty() {
                    bounded_workspace_focus_scopes(
                        workspace_root,
                        &request.intent,
                        focus_count,
                        node_requires_write,
                        explicit_team,
                    )
                } else {
                    declared_scopes
                };
                tracing::debug!(
                    node = %node.node_id,
                    template,
                    custom_scopes = ?custom_scopes,
                    node_requires_write,
                    "custom template bounded resource lease (no builtin focus partitions)"
                );
                node.resource_scopes = custom_scopes;
                node.resource_scopes.sort();
                node.resource_scopes.dedup();
                node.focuses = bind_declared_focuses_to_node_scopes(
                    std::mem::take(&mut node.focuses),
                    &node.resource_scopes,
                );
                scopes.extend(node.resource_scopes.iter().cloned());
                *team_position = team_position.saturating_add(1);
                continue;
            }
            let plans = derive_team_focus_partition_plans(
                &request.intent,
                workspace_root,
                &[],
                focus_count,
                node_requires_write,
                explicit_team,
                node_uses_external,
            );
            let authorized_focuses = semantic_focuses_from_plans(&plans);
            // Team partitions are an authority-bearing contract. Preserve the
            // semantic topology, but derive authority independently per Team
            // node. A final writer must never turn preceding research nodes
            // into write-capable or template-incompatible roles.
            let runtime_focuses = if direct_explicit_research {
                direct_executor_focus_for_team(
                    &request.intent,
                    workspace_root,
                    &authorized_focuses,
                    *team_position,
                    profile_count,
                )
            } else {
                authorized_focuses_for_team(
                    &authorized_focuses,
                    *team_position,
                    profile_count,
                    node_requires_write,
                )
            };
            // The model chooses the semantic role topology; Runtime owns the
            // authority attached to it.  Replacing a declared role list here
            // made a valid two-role Team silently become a one-role Team when
            // prompt inference chose a narrow parallelism bound.  Preserve
            // the declared focus ids, roles and objectives, but overwrite
            // every resource scope with the matching Runtime-derived lease.
            // Template validation remains the final authority over which role
            // names may execute.
            node.focuses =
                bind_declared_focus_authority(std::mem::take(&mut node.focuses), runtime_focuses);
            *team_position = team_position.saturating_add(1);
            node.resource_scopes = node
                .focuses
                .iter()
                .flat_map(|focus| focus.resource_scopes.iter().cloned())
                .collect();
            node.resource_scopes.sort();
            node.resource_scopes.dedup();
            scopes.extend(node.resource_scopes.iter().cloned());
        }
    }
    scopes.sort();
    scopes.dedup();
    for node in &mut proposal.nodes {
        if node.recipe == CapabilityRecipeId::Team {
            continue;
        }
        if matches!(
            node.recipe,
            CapabilityRecipeId::Agent | CapabilityRecipeId::Review | CapabilityRecipeId::Synthesis
        ) {
            node.resource_scopes = scopes.clone();
        }
        if !node.focuses.is_empty() && !scopes.is_empty() {
            // Model-defined Agent focus text remains useful, but each instance
            // receives one bounded Runtime-derived scope instead of the union.
            for (index, focus) in node.focuses.iter_mut().enumerate() {
                focus.resource_scopes = vec![scopes[index % scopes.len()].clone()];
            }
        }
    }
    // Every proposal originating from an active Session must carry at least
    // one Runtime-derived session evidence lease. Without it, Team template
    // validation rejects collaboration with "no Runtime-cropped evidence
    // lease" even when the user explicitly asked for parallel Teams and the
    // permission ceiling is read-only. The lease is derived from the durable
    // session identity, never from model input.
    if let Some(session_id) = request.session_id.as_deref() {
        let session_scope = format!("session:{session_id}");
        if !scopes.iter().any(|scope| scope.starts_with("session:")) {
            scopes.push(session_scope.clone());
        }
        for node in &mut proposal.nodes {
            if !node
                .resource_scopes
                .iter()
                .any(|scope| scope.starts_with("session:"))
            {
                node.resource_scopes.push(session_scope.clone());
                node.resource_scopes.sort();
                node.resource_scopes.dedup();
            }
        }
    }
    // Full-trust Teams with a bounded write target still need a whole-workspace
    // read lease to investigate sources; otherwise role agents are blocked on
    // their first read/glob even though write is correctly bounded to the
    // explicit artifact. Read never mutates and remains bounded by the
    // danger-full-access ceiling.
    if request
        .constraints
        .permission_ceiling
        .permits(harness_contract::policy::PermissionMode::DangerFullAccess)
    {
        let workspace_read = "read:.".to_string();
        for node in &mut proposal.nodes {
            let has_write = node
                .resource_scopes
                .iter()
                .any(|scope| scope.starts_with("write:"));
            let missing_read = !node
                .resource_scopes
                .iter()
                .any(|scope| scope.starts_with("read:"));
            if has_write
                && missing_read
                && (node.recipe == CapabilityRecipeId::Team
                    || matches!(
                        node.recipe,
                        CapabilityRecipeId::Agent
                            | CapabilityRecipeId::Review
                            | CapabilityRecipeId::Synthesis
                    ))
            {
                node.resource_scopes.push(workspace_read.clone());
                node.resource_scopes.sort();
                node.resource_scopes.dedup();
                if node.recipe == CapabilityRecipeId::Team {
                    scopes.push(workspace_read.clone());
                }
            }
        }
        scopes.sort();
        scopes.dedup();
    }
    request
        .capabilities
        .retain(|value| !value.starts_with("resource:"));
    request
        .capabilities
        .extend(scopes.into_iter().map(|scope| format!("resource:{scope}")));
    request.capabilities.sort();
    request.capabilities.dedup();
}

fn declared_evidence_scopes(evidence_contract: &[String]) -> Vec<String> {
    let mut scopes = evidence_contract
        .iter()
        .filter_map(|criterion| criterion.trim().strip_prefix("evidence_scope:"))
        .map(str::trim)
        .filter(|scope| {
            (scope.starts_with("read:") && !matches!(*scope, "read:." | "read:./"))
                || *scope == "network:*"
        })
        .map(str::to_string)
        .collect::<Vec<_>>();
    scopes.sort();
    scopes.dedup();
    scopes
}

fn bind_declared_focus_authority(
    declared: Vec<SemanticFocus>,
    runtime_focuses: Vec<SemanticFocus>,
) -> Vec<SemanticFocus> {
    if declared.is_empty() {
        return runtime_focuses;
    }

    let mut by_role = BTreeMap::<String, Vec<SemanticFocus>>::new();
    for focus in runtime_focuses {
        by_role
            .entry(focus.role_id.clone())
            .or_default()
            .push(focus);
    }
    let mut role_offsets = BTreeMap::<String, usize>::new();
    declared
        .into_iter()
        .map(|mut focus| {
            let offset = role_offsets.entry(focus.role_id.clone()).or_default();
            let authority = by_role
                .get(&focus.role_id)
                .and_then(|candidates| candidates.get(*offset % candidates.len().max(1)))
                .or_else(|| by_role.values().find_map(|candidates| candidates.first()));
            *offset = offset.saturating_add(1);
            if let Some(authority) = authority {
                focus.resource_scopes = authority.resource_scopes.clone();
            }
            focus
        })
        .collect()
}

/// A custom template's declared role set is semantic topology selected by the
/// model. Unlike builtin focus partitions, Runtime cannot substitute an
/// inferred role for it. Bind every selected role to the same already-bounded
/// node lease; the template's own role policy remains the narrower authority
/// boundary for individual Agent instances.
fn bind_declared_focuses_to_node_scopes(
    declared: Vec<SemanticFocus>,
    node_scopes: &[String],
) -> Vec<SemanticFocus> {
    declared
        .into_iter()
        .map(|mut focus| {
            focus.resource_scopes = node_scopes.to_vec();
            focus
        })
        .collect()
}

/// A frozen user strategy contract outranks the model's semantic preference.
/// Model JSON may opt in to an escalation lane, but it may not omit or turn
/// off an explicitly required native escalation.  The first Team node is the
/// deterministic semantic representative of the user's Team A; exactly one
/// Team owns this obligation, while Runtime still chooses the concrete Agent.
fn bind_required_managed_agent_escalation(
    proposal: &mut crate::orchestration::GraphMutationProposal,
    understanding: &harness_contract::strategy::TaskUnderstanding,
    requires_write: bool,
) {
    if !understanding.requires_managed_collaboration_escalation {
        return;
    }

    let team_count = proposal
        .nodes
        .iter()
        .filter(|node| node.recipe == CapabilityRecipeId::Team)
        .count();
    let mut assigned = false;
    let mut team_index = 0;
    for node in &mut proposal.nodes {
        if node.recipe != CapabilityRecipeId::Team {
            continue;
        }
        node.managed_agent_escalation = if assigned {
            ManagedAgentEscalationRequirement::None
        } else {
            assigned = true;
            ManagedAgentEscalationRequirement::Required
        };
        // A native escalation requirement is an ingress-level execution
        // contract, not a model hint.  Bind its builtin Team nodes to the
        // same Runtime-owned template contract that supplies their focus
        // topology.  Freezing only the escalation enum previously allowed a
        // strategy default's role partitions to be paired with a different
        // model-suggested template, producing an unexecutable graph before
        // the selected Agent could reach the escalation checkpoint.
        let custom_template = node.template.as_deref().is_some_and(|template| {
            template.starts_with("workspace/") || template.starts_with("user/")
        });
        if !custom_template {
            let contract = explicit_team_node_contract(
                team_index,
                team_count.max(1),
                requires_write,
                understanding.requires_external_facts,
            );
            node.template = Some(contract.template.to_string());
            node.output_artifacts = contract
                .output_artifacts
                .iter()
                .map(|value| (*value).to_string())
                .collect();
            node.evidence_contract = contract
                .evidence_contract
                .iter()
                .map(|value| (*value).to_string())
                .collect();
        }
        team_index = team_index.saturating_add(1);
    }
}

fn team_authority_profile(
    node: &GraphSemanticNode,
    request_requires_write: bool,
    request_requires_external_facts: bool,
    ephemeral_requires_write: Option<bool>,
) -> TeamAuthorityProfile {
    let template = node
        .template
        .as_deref()
        .unwrap_or_default()
        .trim_start_matches("builtin/");
    let custom_template = template.starts_with("workspace/")
        || template.starts_with("user/")
        || ephemeral_requires_write.is_some();
    if template == "cowd/external-research-synthesis" {
        return TeamAuthorityProfile::ExternalResearch;
    }
    if node
        .output_artifacts
        .iter()
        .any(|artifact| artifact == "workspace_change")
        || matches!(
            template,
            "cowd/execute-review"
                | "cowd/implementation-review-fix"
                | "cowd/planner-executor-verifier"
        )
    {
        return TeamAuthorityProfile::WorkspaceWrite;
    }
    if matches!(
        template,
        "cowd/parallel-research-synthesis"
            | "cowd/debate-critic-arbiter"
            | "cowd/comparative-synthesis"
    ) {
        return TeamAuthorityProfile::WorkspaceRead;
    }
    // A user-authored/custom template is not covered by the builtin
    // read-only family. When the admitted intent requires workspace writes,
    // Runtime treats the Team as write-capable; the template's own role
    // ceilings and the bounded focus scopes still crop the actual grants.
    let custom_requires_write = ephemeral_requires_write.unwrap_or(request_requires_write);
    if custom_requires_write && (custom_template || node.template.is_none()) {
        TeamAuthorityProfile::WorkspaceWrite
    } else if request_requires_external_facts {
        TeamAuthorityProfile::ExternalResearch
    } else {
        TeamAuthorityProfile::WorkspaceRead
    }
}

pub(crate) fn semantic_focuses_from_plans(plans: &[FocusPartitionPlan]) -> Vec<SemanticFocus> {
    plans
        .iter()
        .flat_map(|plan| {
            plan.slots.iter().map(|slot| SemanticFocus {
                focus_id: slot.focus_id.clone(),
                role_id: plan.role_id.clone(),
                objective: slot.boundary.clone(),
                resource_scopes: slot.capability_cropped_refs.clone(),
                evidence_responsibilities: vec![slot.evidence_responsibility.clone()],
                output_contract: slot.output_contract.clone(),
                output_acceptance: slot.output_acceptance.clone(),
            })
        })
        .collect()
}

/// Split Runtime-authored focus contracts across independent Team nodes.
/// This is an authority boundary: duplicating every focus into every Team
/// creates false same-scope overlap in an otherwise valid Program.
pub(crate) fn authorized_focuses_for_team(
    focuses: &[SemanticFocus],
    team_index: usize,
    team_count: usize,
    requires_write: bool,
) -> Vec<SemanticFocus> {
    if team_count <= 1 || requires_write {
        return focuses.to_vec();
    }
    let mut primary_index = 0_usize;
    let mut selected = Vec::new();
    for focus in focuses {
        if primary_index % team_count == team_index {
            selected.push(focus.clone());
        }
        primary_index = primary_index.saturating_add(1);
    }
    selected
}

fn direct_executor_focus_for_team(
    objective: &str,
    workspace_root: &Path,
    authorized_focuses: &[SemanticFocus],
    team_index: usize,
    team_count: usize,
) -> Vec<SemanticFocus> {
    let mut scopes = explicit_team_workspace_paths(workspace_root, objective, team_index + 1)
        .into_iter()
        .map(|path| format!("read:{path}"))
        .collect::<Vec<_>>();
    if scopes.is_empty() {
        let primary = authorized_focuses.iter().collect::<Vec<_>>();
        for (index, focus) in primary.iter().enumerate() {
            if index % team_count == team_index {
                scopes.extend(focus.resource_scopes.iter().cloned());
            }
        }
        if scopes.is_empty() {
            if let Some(focus) = primary.get(team_index % primary.len().max(1)) {
                scopes.extend(focus.resource_scopes.iter().cloned());
            }
        }
    }
    scopes.sort();
    scopes.dedup();
    if scopes.is_empty() {
        return Vec::new();
    }
    vec![SemanticFocus {
        focus_id: format!("direct-team-{}", team_index + 1),
        role_id: "executor".to_string(),
        objective: format!(
            "Inspect only the {} Runtime-authorized resource scope(s) assigned to Team {}",
            scopes.len(),
            team_index + 1,
        ),
        resource_scopes: scopes,
        evidence_responsibilities: vec![
            "Return a concise evidence-backed summary for the assigned Team resources".to_string(),
        ],
        output_contract: vec!["summary".to_string(), "evidence".to_string()],
        output_acceptance: vec!["summary".to_string(), "evidence".to_string()],
    }]
}

fn explicit_team_workspace_paths(
    workspace_root: &Path,
    objective: &str,
    team_number: usize,
) -> Vec<String> {
    const CHINESE_NUMERALS: [&str; 6] = ["一", "二", "三", "四", "五", "六"];
    let normalized = objective.to_ascii_lowercase();
    let mut occurrences = Vec::<(usize, usize, usize)>::new();
    for number in 1..=6 {
        let chinese = CHINESE_NUMERALS[number - 1];
        let patterns = [
            format!("team {number}"),
            format!("team{number}"),
            format!("team {chinese}"),
            format!("team{chinese}"),
            format!("团队 {number}"),
            format!("团队{number}"),
            format!("第{number}个团队"),
            format!("第{chinese}个团队"),
        ];
        for pattern in patterns {
            for (offset, _) in normalized.match_indices(&pattern) {
                occurrences.push((offset, number, pattern.len()));
            }
        }
    }
    occurrences.sort_unstable();
    occurrences.dedup();

    let mut paths = Vec::new();
    for (position, (offset, number, marker_len)) in occurrences.iter().enumerate() {
        if *number != team_number {
            continue;
        }
        let end = occurrences
            .iter()
            .skip(position + 1)
            .map(|(next, _, _)| *next)
            .find(|next| *next > *offset)
            .unwrap_or(objective.len());
        let start = offset.saturating_add(*marker_len);
        if start >= end {
            continue;
        }
        paths.extend(explicit_workspace_paths(
            workspace_root,
            &objective[start..end],
            false,
        ));
    }
    paths.sort();
    paths.dedup();
    paths
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn derive_team_focus_partition_plans(
    objective: &str,
    workspace_root: &Path,
    forced_scopes: &[String],
    requested_count: usize,
    requires_write: bool,
    explicit_team: bool,
    external_research: bool,
) -> Vec<FocusPartitionPlan> {
    let scopes = if forced_scopes.is_empty() {
        let explicit_local_scopes =
            explicit_workspace_resource_scopes(workspace_root, objective, requires_write);
        tracing::debug!(
            workspace_root = ?workspace_root,
            objective = ?objective.chars().take(200).collect::<String>(),
            requires_write,
            explicit_scopes = ?explicit_local_scopes,
            "derive team focus scopes"
        );
        if !explicit_local_scopes.is_empty() {
            explicit_local_scopes
        } else if external_research && !requires_write {
            return external_research_focus_partition_plans(requested_count);
        } else {
            bounded_workspace_focus_scopes(
                workspace_root,
                objective,
                if requires_write { 1 } else { requested_count },
                requires_write,
                explicit_team,
            )
        }
    } else {
        forced_scopes.to_vec()
    };
    if scopes.is_empty() {
        return Vec::new();
    }
    if requires_write {
        let reviewer_scopes = scopes
            .iter()
            .filter_map(|scope| {
                scope
                    .strip_prefix("write:")
                    .or_else(|| scope.strip_prefix("workspace:"))
                    .map(|path| format!("read:{path}"))
            })
            .collect::<Vec<_>>();
        vec![
            write_focus_partition_plan(objective, scopes.clone()),
            review_focus_partition_plan(reviewer_scopes),
        ]
    } else {
        let read_scopes = (0..requested_count)
            .map(|index| scopes[index % scopes.len()].clone())
            .collect::<Vec<_>>();
        vec![
            automatic_focus_partition_plan(objective, read_scopes),
            support_focus_partition_plan(
                "synthesizer",
                "bounded-synthesis",
                "Synthesize only the evidence returned from the bounded researcher scopes",
                scopes,
            ),
        ]
    }
}

fn review_focus_partition_plan(scopes: Vec<String>) -> FocusPartitionPlan {
    let boundary =
        "Review only the committed implementation paths without mutation or authority expansion";
    FocusPartitionPlan {
        role_id: "reviewer".to_string(),
        shared_baseline: vec![
            "Only committed implementer output and Runtime-owned evidence receipts".to_string(),
        ],
        slots: vec![FocusPartitionSlot {
            focus_id: "bounded-review".to_string(),
            scope_hash: harness_contract::team::focus_scope_hash("reviewer", boundary, &scopes),
            boundary: boundary.to_string(),
            evidence_responsibility:
                "Independently read the committed output and preserve upstream change evidence"
                    .to_string(),
            capability_cropped_refs: scopes,
            overlap_budget_bp: 0,
            novelty_target_bp: 1_000,
            output_contract: vec![
                "review".to_string(),
                "evidence".to_string(),
                "risks".to_string(),
            ],
            output_acceptance: vec![
                "review".to_string(),
                "evidence".to_string(),
                "risks".to_string(),
            ],
        }],
    }
}

fn external_research_focus_partition_plans(requested_count: usize) -> Vec<FocusPartitionPlan> {
    const FOCUSES: &[(&str, &str)] = &[
        (
            "primary-sources",
            "Collect current primary and authoritative sources for the objective",
        ),
        (
            "ecosystem-evidence",
            "Collect independent ecosystem evidence and implementation practice",
        ),
        (
            "contradictions-risks",
            "Search for contradictory evidence, limitations, and material risks",
        ),
        (
            "adoption-economics",
            "Assess adoption constraints, costs, and operational consequences",
        ),
        (
            "future-trajectory",
            "Assess credible emerging directions without presenting forecasts as facts",
        ),
        (
            "verification",
            "Cross-check the strongest claims against independent current sources",
        ),
    ];
    let slots = (0..requested_count.max(1))
        .map(|index| {
            let (base_focus_id, boundary) = FOCUSES[index % FOCUSES.len()];
            let focus_id = if index < FOCUSES.len() {
                base_focus_id.to_string()
            } else {
                format!("{base_focus_id}-{}", index + 1)
            };
            let scopes = vec!["network:*".to_string()];
            FocusPartitionSlot {
                focus_id,
                scope_hash: harness_contract::team::focus_scope_hash(
                    "researcher",
                    boundary,
                    &scopes,
                ),
                boundary: (*boundary).to_string(),
                evidence_responsibility:
                    "Return source-attributed findings, publication dates, conflicts, and unresolved uncertainty. Treat repeated retrievals of the same publisher or artifact as one source; they never increase confidence. Distinguish title-only evidence from verified body content."
                        .to_string(),
                capability_cropped_refs: scopes,
                // All researchers share the network transport while their
                // semantic evidence responsibilities remain disjoint.
                overlap_budget_bp: 10_000,
                novelty_target_bp: 2_500,
                output_contract: vec![
                    "findings".to_string(),
                    "evidence".to_string(),
                    "unresolved".to_string(),
                ],
                output_acceptance: vec!["evidence_scope:network:*".to_string()],
            }
        })
        .collect::<Vec<_>>();
    vec![
        FocusPartitionPlan {
            role_id: "researcher".to_string(),
            shared_baseline: vec![
                "parent objective, current-date boundary, and source-quality requirements"
                    .to_string(),
                "confidence is based on independent source diversity and content quality, never repeated fetch count"
                    .to_string(),
            ],
            slots,
        },
        support_focus_partition_plan(
            "synthesizer",
            "external-synthesis",
            "Reconcile only committed researcher evidence; preserve dates, conflicts, and gaps; deduplicate repeated publishers and artifacts before calibrating confidence",
            vec!["network:*".to_string()],
        ),
    ]
}

pub(crate) fn automatic_focus_partition_plan(
    _objective: &str,
    scopes: Vec<String>,
) -> FocusPartitionPlan {
    let identity_totals = scopes.iter().fold(
        std::collections::BTreeMap::<String, usize>::new(),
        |mut totals, reference| {
            let domain = reference
                .split_once(':')
                .map_or(reference.as_str(), |(_, path)| path)
                .replace('/', "-");
            *totals.entry(domain).or_default() += 1;
            totals
        },
    );
    let mut identity_counts = std::collections::BTreeMap::<String, usize>::new();
    FocusPartitionPlan {
        role_id: "researcher".to_string(),
        shared_baseline: vec![
            "parent objective and capability-cropped session evidence".to_string(),
        ],
        slots: scopes
            .into_iter()
            .enumerate()
            .map(|(index, reference)| {
                let domain = reference
                    .split_once(':')
                    .map_or(reference.as_str(), |(_, path)| path)
                    .replace('/', "-");
                let occurrence = identity_counts.entry(domain.clone()).or_default();
                *occurrence += 1;
                let focus_id = if *occurrence == 1 {
                    domain.clone()
                } else {
                    format!("{domain}-focus-{}", index + 1)
                };
                let focus_angle = match *occurrence {
                    1 => "primary behavior and contract evidence",
                    2 => "independent contradictions, failures, and boundary risks",
                    3 => "integration and lifecycle evidence",
                    _ => "independent verification evidence",
                };
                let evidence_scope = reference
                    .split_once(':')
                    .map_or(reference.as_str(), |(_, path)| path)
                    .to_string();
                let boundary = format!(
                    "Only inspect and judge `{domain}` for {focus_angle}"
                );
                let capability_cropped_refs = vec![reference];
                FocusPartitionSlot {
                    focus_id,
                    scope_hash: harness_contract::team::focus_scope_hash(
                        "researcher",
                        &boundary,
                        &capability_cropped_refs,
                    ),
                    boundary,
                    evidence_responsibility: format!(
                        "Collect capability-authorized {focus_angle} for `{domain}` and identify unresolved gaps"
                    ),
                    capability_cropped_refs,
                    overlap_budget_bp: if identity_totals.get(&domain).copied().unwrap_or(0) > 1 {
                        10_000
                    } else {
                        0
                    },
                    novelty_target_bp: 2_500,
                    output_contract: vec![
                        "findings".to_string(),
                        "evidence".to_string(),
                        "unresolved".to_string(),
                    ],
                    output_acceptance: vec![format!("evidence_scope:{evidence_scope}")],
                }
            })
            .collect(),
    }
}

pub(crate) fn write_focus_partition_plan(
    _objective: &str,
    scopes: Vec<String>,
) -> FocusPartitionPlan {
    let boundary = format!(
        "Implement only inside the {} Runtime-authorized workspace scope(s)",
        scopes.len()
    );
    FocusPartitionPlan {
        role_id: "implementer".to_string(),
        shared_baseline: vec![
            "parent objective and Runtime-verified bounded workspace paths".to_string(),
        ],
        slots: vec![FocusPartitionSlot {
            focus_id: "bounded-implementation".to_string(),
            scope_hash: harness_contract::team::focus_scope_hash("implementer", &boundary, &scopes),
            boundary,
            evidence_responsibility:
                "Produce implementation evidence only from the assigned resource scopes".to_string(),
            capability_cropped_refs: scopes,
            overlap_budget_bp: 0,
            novelty_target_bp: 2_500,
            output_contract: vec![
                "implementation".to_string(),
                "source_verification".to_string(),
                "residual risk".to_string(),
            ],
            output_acceptance: vec![
                "implementation".to_string(),
                "source_verification".to_string(),
            ],
        }],
    }
}

fn support_focus_partition_plan(
    role_id: &str,
    focus_id: &str,
    boundary: &str,
    scopes: Vec<String>,
) -> FocusPartitionPlan {
    FocusPartitionPlan {
        role_id: role_id.to_string(),
        shared_baseline: vec![
            "Only committed outputs from the bounded upstream Team roles".to_string(),
        ],
        slots: vec![FocusPartitionSlot {
            focus_id: focus_id.to_string(),
            scope_hash: harness_contract::team::focus_scope_hash(role_id, boundary, &scopes),
            boundary: boundary.to_string(),
            evidence_responsibility:
                "Preserve source scope identity, conflicts, and unresolved gaps".to_string(),
            capability_cropped_refs: scopes,
            overlap_budget_bp: 0,
            novelty_target_bp: 1_000,
            output_contract: vec![
                "summary".to_string(),
                "evidence".to_string(),
                "unresolved".to_string(),
            ],
            output_acceptance: vec!["evidence".to_string(), "unresolved".to_string()],
        }],
    }
}

pub(crate) fn bounded_workspace_focus_scopes(
    workspace_root: &Path,
    objective: &str,
    requested_count: usize,
    requires_write: bool,
    _explicit_team: bool,
) -> Vec<String> {
    let access = if requires_write { "write" } else { "read" };
    let explicit_paths =
        explicit_workspace_resource_scopes(workspace_root, objective, requires_write);
    if !explicit_paths.is_empty() {
        // An existing path explicitly named by the user is the strongest
        // resource-authority signal. Generic words such as "docs" or
        // "tests" describe an investigation angle; they must not silently
        // widen an exact `/workspace/target` lease.
        return explicit_paths;
    }
    let mut candidates = workspace_focus_candidates(workspace_root, objective)
        .into_iter()
        .map(|path| {
            let score = workspace_focus_score(objective, &path);
            (score, path)
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|(left_score, left), (right_score, right)| {
        right_score.cmp(left_score).then_with(|| left.cmp(right))
    });
    let normalized = objective.to_ascii_lowercase();
    let broad = requests_broad_workspace_scope(&normalized);
    let required = requested_count.max(1);
    let explicitly_named_files = candidates
        .iter()
        .filter(|(score, path)| *score > 0 && workspace_root.join(path).is_file())
        .map(|(_, path)| format!("{access}:{path}"))
        .collect::<Vec<_>>();
    if !explicitly_named_files.is_empty() {
        // A named file is the authoritative resource boundary. Cardinality is
        // expressed by focus slots below, not by inventing unrelated paths to
        // satisfy the requested worker count.
        return explicitly_named_files;
    }
    if requires_write && candidates.iter().all(|(score, _)| *score == 0) {
        // Creating a new artifact without an explicit existing target is a
        // workspace-root operation. Selecting the first directory merely
        // because an explicit Team was requested leaks directory ordering
        // into authority (for example `Code` can win alphabetically) and
        // binds the Agent to an unrelated subtree.
        return vec!["write:.".to_string()];
    }
    let mut selected = candidates
        .iter()
        .filter(|(score, _)| *score > 0)
        .map(|(_, path)| path.clone())
        .take(required)
        .collect::<Vec<_>>();
    if !selected.is_empty() {
        // Resource cardinality and Agent cardinality are different concerns.
        // Multiple semantic focus slots may intentionally share one exact
        // directory; never add unrelated paths merely to fill worker slots.
        return selected
            .into_iter()
            .map(|path| format!("{access}:{path}"))
            .collect();
    }
    if broad && selected.len() < required {
        for (_, candidate) in candidates {
            if selected.len() >= required {
                break;
            }
            if !selected.contains(&candidate) {
                selected.push(candidate);
            }
        }
    }
    if selected.is_empty() {
        return Vec::new();
    }
    selected
        .into_iter()
        .map(|path| format!("{access}:{path}"))
        .collect()
}

pub(crate) fn explicit_workspace_resource_scopes(
    workspace_root: &Path,
    objective: &str,
    requires_write: bool,
) -> Vec<String> {
    let paths = explicit_workspace_paths(workspace_root, objective, requires_write);
    if !requires_write {
        return paths
            .into_iter()
            .map(|path| format!("read:{path}"))
            .collect();
    }

    let write_paths = paths
        .iter()
        .filter(|path| objective_marks_path_for_write(workspace_root, objective, path))
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    if write_paths.is_empty() && paths.len() == 1 {
        // Preserve the established exact-path authority when the objective is
        // unambiguously mutating but does not contain a recognizable natural
        // language target marker. This is still narrower than workspace root.
        return paths
            .into_iter()
            .map(|path| format!("write:{path}"))
            .collect();
    }

    paths
        .into_iter()
        .map(|path| {
            if write_paths.contains(&path) {
                format!("write:{path}")
            } else {
                format!("read:{path}")
            }
        })
        .collect()
}

fn objective_marks_path_for_write(workspace_root: &Path, objective: &str, relative: &str) -> bool {
    const WRITE_MARKERS: &[&str] = &[
        "写入", "生成", "保存", "输出", "创建", "修改", "更新", "编辑", "修复", "重构", "落盘",
        "替换", "改动", "调整", "write", "create", "generate", "save", "modify", "update", "edit",
        "replace", "refactor", "fix",
    ];
    const READ_ONLY_MARKERS: &[&str] = &[
        "只读",
        "不修改",
        "不要修改",
        "不得修改",
        "无需修改",
        "read only",
        "read-only",
        "do not modify",
        "without modifying",
    ];
    const CLAUSE_BOUNDARIES: &[char] = &['。', '；', ';', '\n', '！', '？'];

    let absolute = workspace_root.join(relative).to_string_lossy().to_string();
    let candidates = [absolute, format!("./{relative}"), relative.to_string()];
    candidates.iter().any(|candidate| {
        objective.match_indices(candidate).any(|(offset, _)| {
            let before = &objective[..offset];
            let clause_start = before
                .char_indices()
                .rev()
                .find(|(_, character)| CLAUSE_BOUNDARIES.contains(character))
                .map_or(0, |(index, character)| index + character.len_utf8());
            let clause = before[clause_start..].to_ascii_lowercase();
            !READ_ONLY_MARKERS
                .iter()
                .any(|marker| clause_contains_action(&clause, marker))
                && WRITE_MARKERS
                    .iter()
                    .any(|marker| clause_contains_action(&clause, marker))
        })
    })
}

fn clause_contains_action(clause: &str, marker: &str) -> bool {
    if !marker.is_ascii() {
        return clause.contains(marker);
    }
    clause.match_indices(marker).any(|(offset, value)| {
        let before = clause[..offset].chars().next_back();
        let after = clause[offset + value.len()..].chars().next();
        before.is_none_or(|character| !character.is_ascii_alphanumeric() && character != '_')
            && after.is_none_or(|character| !character.is_ascii_alphanumeric() && character != '_')
    })
}

fn explicit_workspace_paths(
    workspace_root: &Path,
    objective: &str,
    allow_missing: bool,
) -> Vec<String> {
    let Ok(canonical_root) = workspace_root.canonicalize() else {
        return Vec::new();
    };
    let mut paths = objective
        .split(|character: char| {
            character.is_whitespace()
                || matches!(
                    character,
                    ',' | ';'
                        | ':'
                        | '('
                        | ')'
                        | '['
                        | ']'
                        | '{'
                        | '}'
                        | '<'
                        | '>'
                        | '，'
                        | '。'
                        | '；'
                        | '：'
                        | '、'
                        | '（'
                        | '）'
                        | '【'
                        | '】'
                )
        })
        .map(|token| token.trim_matches(['`', '\'', '"']))
        .filter(|token| !is_definition_like_token(workspace_root, token))
        .filter(|token| {
            token.starts_with('/')
                || token.starts_with("./")
                || (token.contains('/') && !token.contains("://") && !token.starts_with("http"))
        })
        .filter_map(|token| {
            let pattern_prefix = workspace_pattern_existing_prefix(token, allow_missing)?;
            let token = pattern_prefix.as_deref().unwrap_or(token);
            if !is_probable_workspace_path_token(workspace_root, token) {
                return None;
            }
            let workspace_candidate = if token.starts_with('/') {
                std::path::PathBuf::from(token)
            } else {
                workspace_root.join(token.trim_start_matches("./"))
            };
            // Bare relative paths such as `crates/runtime` are most often
            // relative to the repository currently running the turn, while
            // the configured workspace root may be a parent of several
            // repositories. Prefer the process cwd when it is inside the
            // workspace root and the path exists there.
            let candidate = if token.starts_with('/') {
                workspace_candidate
            } else if let Ok(cwd) =
                std::env::current_dir().map(|cwd| cwd.canonicalize().unwrap_or(cwd))
            {
                let cwd_candidate = cwd.join(token.trim_start_matches("./"));
                if cwd.starts_with(&canonical_root) && cwd_candidate.exists() {
                    cwd_candidate
                } else {
                    workspace_candidate
                }
            } else {
                workspace_candidate
            };
            workspace_relative_explicit_path(
                workspace_root,
                &canonical_root,
                &candidate,
                allow_missing,
            )
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    paths
}

/// A read-only source family such as `crates/**/*.rs` authorizes its longest
/// explicit existing ancestor, not an arbitrary directory chosen from
/// workspace ordering. Pattern paths never authorize a write or a missing
/// ancestor, and parent traversal remains rejected by canonical containment.
fn workspace_pattern_existing_prefix(token: &str, allow_missing: bool) -> Option<Option<String>> {
    let pattern_segment = |segment: &str| {
        segment == "..."
            || segment.contains('*')
            || segment.contains('?')
            || segment.contains('[')
            || segment.contains(']')
    };
    let segments = token.split('/').collect::<Vec<_>>();
    if segments.iter().any(|segment| *segment == "..") {
        return None;
    }
    let Some(pattern_index) = segments.iter().position(|segment| pattern_segment(segment)) else {
        return Some(None);
    };
    if allow_missing {
        return None;
    }
    let prefix_segments = &segments[..pattern_index];
    if prefix_segments
        .iter()
        .all(|segment| segment.is_empty() || *segment == ".")
    {
        return None;
    }
    let mut prefix = prefix_segments.join("/");
    if token.starts_with('/') && !prefix.starts_with('/') {
        prefix.insert(0, '/');
    }
    while prefix.ends_with('/') && prefix.len() > 1 {
        prefix.pop();
    }
    Some(Some(prefix))
}

/// Tokens are only treated as workspace paths when they are:
/// - explicit (`/…` or `./…`), or
/// - an existing path on disk (workspace root or process cwd), or
/// - a planned artifact carrying a file extension (`docs/report.md`).
/// Everything else is prose: field lists such as `summary/evidence`,
/// template names (“业务/技术双团队研讨”), and definition ids
/// (`cowd/biz-tech-dual-team-deliberation`) are never filesystem paths;
/// parsing them as paths breaks resource acquisition with “workspace path
/// does not exist”.
fn is_probable_workspace_path_token(workspace_root: &Path, token: &str) -> bool {
    if token.starts_with('/') || token.starts_with("./") {
        return true;
    }
    if !token.is_ascii() {
        return false;
    }
    if workspace_root.join(token).exists()
        || std::env::current_dir()
            .map(|cwd| cwd.join(token).exists())
            .unwrap_or(false)
    {
        return true;
    }
    token.rsplit('/').next().is_some_and(|leaf| {
        leaf.rsplit_once('.').is_some_and(|(name, extension)| {
            !name.is_empty()
                && !extension.is_empty()
                && extension
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric())
        })
    })
}

/// Template/Agent Definition references such as
/// `cowd/biz-tech-dual-team-deliberation`, `workspace/cowd/explore@1`, or
/// `builtin/cowd/execute` are not filesystem paths and must never become Team
/// resource leases. The first segment is a well-known definition namespace and
/// the remaining segments carry no file extension, so a token that is also
/// absent on disk is treated as a definition id instead of a planned file.
fn is_definition_like_token(workspace_root: &Path, token: &str) -> bool {
    const DEFINITION_NAMESPACES: &[&str] = &[
        "agent",
        "app",
        "builtin",
        "cowd",
        "definition",
        "skill",
        "template",
        "user",
        "workspace",
        "team",
    ];
    let Some((namespace, rest)) = token.split_once('/') else {
        return false;
    };
    if !DEFINITION_NAMESPACES.contains(&namespace) {
        return false;
    }
    if rest.is_empty() || rest.contains('.') {
        return false;
    }
    let exists_anywhere = workspace_root.join(token).exists()
        || std::env::current_dir()
            .map(|cwd| cwd.join(token).exists())
            .unwrap_or(false);
    !exists_anywhere
}

fn workspace_relative_explicit_path(
    workspace_root: &Path,
    canonical_root: &Path,
    candidate: &Path,
    allow_missing: bool,
) -> Option<String> {
    if let Ok(canonical) = candidate.canonicalize() {
        let relative = canonical.strip_prefix(canonical_root).ok()?;
        return Some(if relative.as_os_str().is_empty() {
            ".".to_string()
        } else {
            relative.to_string_lossy().replace('\\', "/")
        });
    }

    if !allow_missing {
        return None;
    }
    // New artifact paths cannot be canonicalized yet. Accept them only when
    // their lexical identity is workspace-relative and their nearest existing
    // ancestor resolves inside the canonical workspace (no parent traversal or
    // symlink escape).
    let relative = if candidate.is_absolute() {
        candidate.strip_prefix(workspace_root).ok()?.to_path_buf()
    } else {
        candidate.to_path_buf()
    };
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            std::path::Component::Normal(value) => parts.push(value.to_os_string()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => return None,
        }
    }
    if parts.is_empty() {
        return Some(".".to_string());
    }
    let relative = parts.iter().collect::<std::path::PathBuf>();
    let mut ancestor = workspace_root.join(&relative);
    while !ancestor.exists() {
        ancestor = ancestor.parent()?.to_path_buf();
    }
    let canonical_ancestor = ancestor.canonicalize().ok()?;
    canonical_ancestor
        .starts_with(canonical_root)
        .then(|| relative.to_string_lossy().replace('\\', "/"))
}

fn requests_broad_workspace_scope(normalized_objective: &str) -> bool {
    [
        "entire workspace",
        "whole workspace",
        "full workspace",
        "across the workspace",
        "workspace source",
        "workspace code",
        "entire codebase",
        "whole codebase",
        "full codebase",
        "across the codebase",
        "entire repository",
        "whole repository",
        "full repository",
        "across the repository",
        "system-wide",
        "architecture-wide",
        "comprehensive review",
        "comprehensive audit",
        "整个工作区",
        "全工作区",
        "当前工作区",
        "工作区源码",
        "整个代码库",
        "全代码库",
        "整个仓库",
        "全仓库",
        "全仓",
        "整个项目",
        "全项目",
        "全盘",
        "全面审查",
        "全面审计",
        "整体架构",
        "全局架构",
        "系统全局",
    ]
    .iter()
    .any(|marker| normalized_objective.contains(marker))
}

fn workspace_focus_candidates(workspace_root: &Path, objective: &str) -> Vec<String> {
    const EXCLUDED: &[&str] = &[
        ".git",
        ".cargo",
        ".cowd",
        "target",
        "node_modules",
        "dist",
        "build",
        "coverage",
        "test-reports",
    ];
    const PARTITION_ROOTS: &[&str] = &[
        "apps", "crates", "docs", "packages", "scripts", "surfaces", "tests",
    ];
    let Ok(entries) = std::fs::read_dir(workspace_root) else {
        return Vec::new();
    };
    let mut candidates = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') || EXCLUDED.contains(&name.as_str()) {
            continue;
        }
        let path = entry.path();
        if path.is_file() {
            if objective
                .to_ascii_lowercase()
                .contains(&name.to_ascii_lowercase())
            {
                candidates.push(name);
            }
            continue;
        }
        if !path.is_dir() {
            continue;
        }
        if PARTITION_ROOTS.contains(&name.as_str()) {
            let mut children = std::fs::read_dir(&path)
                .into_iter()
                .flatten()
                .flatten()
                .filter(|child| child.path().is_dir())
                .filter_map(|child| {
                    let child_name = child.file_name().to_string_lossy().into_owned();
                    (!child_name.starts_with('.') && !EXCLUDED.contains(&child_name.as_str()))
                        .then(|| format!("{name}/{child_name}"))
                })
                .collect::<Vec<_>>();
            if children.is_empty() {
                candidates.push(name);
            } else {
                candidates.append(&mut children);
            }
        } else {
            candidates.push(name);
        }
    }
    candidates.sort();
    candidates.dedup();
    candidates
}

fn workspace_focus_score(objective: &str, path: &str) -> u16 {
    let objective = objective.to_ascii_lowercase();
    let path_lower = path.to_ascii_lowercase();
    let mut score = path_lower
        .split(['/', '-', '_'])
        .filter(|part| part.len() >= 2 && objective.contains(part))
        .count() as u16
        * 100;
    let leaf = path_lower.rsplit('/').next().unwrap_or(path_lower.as_str());
    if objective
        .split(|character: char| {
            !character.is_ascii_alphanumeric() && character != '-' && character != '_'
        })
        .any(|token| token == leaf)
    {
        score = score.saturating_add(500);
    }
    for (marker, targets) in [
        ("backend", &["crates/gateway", "crates/runtime"][..]),
        ("后端", &["crates/gateway", "crates/runtime"][..]),
        ("api", &["crates/gateway"][..]),
        ("frontend", &["surfaces/webui", "crates/tui"][..]),
        ("前端", &["surfaces/webui", "crates/tui"][..]),
        ("webui", &["surfaces/webui"][..]),
        ("tui", &["crates/tui"][..]),
        ("memory", &["crates/memory"][..]),
        ("matrix", &["crates/matrix"][..]),
        ("test", &["tests", "scripts/test"][..]),
        ("测试", &["tests", "scripts/test"][..]),
        ("docs", &["docs"][..]),
        ("文档", &["docs"][..]),
    ] {
        if objective.contains(marker)
            && targets.iter().any(|target| {
                path_lower == *target || path_lower.starts_with(&format!("{target}/"))
            })
        {
            score = score.saturating_add(250);
        }
    }
    score
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_ephemeral_role_ceiling_overrides_global_write_intent() {
        let node = GraphSemanticNode {
            node_id: "read-only-ephemeral-team".to_string(),
            recipe: CapabilityRecipeId::Team,
            objective: "research without workspace mutation".to_string(),
            depends_on: Vec::new(),
            multiplicity: 1,
            focuses: Vec::new(),
            managed_agent_escalation: ManagedAgentEscalationRequirement::None,
            template: None,
            target_session_id: None,
            output_artifacts: vec!["terminal_synthesis".to_string()],
            evidence_contract: vec!["evidence".to_string()],
            required_evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            required: true,
            dependency: Default::default(),
            cancellation_group: None,
        };

        assert_eq!(
            team_authority_profile(&node, true, false, Some(false)),
            TeamAuthorityProfile::WorkspaceRead,
            "a frozen read-only custom Team must not inherit unrelated global write intent"
        );
        assert_eq!(
            team_authority_profile(&node, true, false, Some(true)),
            TeamAuthorityProfile::WorkspaceWrite,
            "a frozen Team with an explicit write-capable role keeps its write authority"
        );
    }

    #[test]
    fn declared_team_roles_keep_semantics_while_runtime_rebinds_their_scopes() {
        let declared = vec![
            SemanticFocus {
                focus_id: "model-research".to_string(),
                role_id: "researcher".to_string(),
                objective: "inspect primary evidence".to_string(),
                resource_scopes: vec!["write:unsafe".to_string()],
                evidence_responsibilities: vec!["source evidence".to_string()],
                output_contract: Vec::new(),
                output_acceptance: Vec::new(),
            },
            SemanticFocus {
                focus_id: "model-synthesis".to_string(),
                role_id: "synthesizer".to_string(),
                objective: "compare the bounded findings".to_string(),
                resource_scopes: vec!["network:*".to_string()],
                evidence_responsibilities: vec!["synthesis evidence".to_string()],
                output_contract: Vec::new(),
                output_acceptance: Vec::new(),
            },
        ];
        let runtime = vec![
            SemanticFocus {
                focus_id: "runtime-research".to_string(),
                role_id: "researcher".to_string(),
                objective: "runtime boundary".to_string(),
                resource_scopes: vec!["read:Moon".to_string()],
                evidence_responsibilities: Vec::new(),
                output_contract: Vec::new(),
                output_acceptance: Vec::new(),
            },
            SemanticFocus {
                focus_id: "runtime-synthesis".to_string(),
                role_id: "synthesizer".to_string(),
                objective: "runtime boundary".to_string(),
                resource_scopes: vec!["read:Moon".to_string()],
                evidence_responsibilities: Vec::new(),
                output_contract: Vec::new(),
                output_acceptance: Vec::new(),
            },
        ];

        let bound = bind_declared_focus_authority(declared, runtime);
        assert_eq!(bound[0].focus_id, "model-research");
        assert_eq!(bound[1].objective, "compare the bounded findings");
        assert_eq!(bound[0].resource_scopes, vec!["read:Moon"]);
        assert_eq!(bound[1].resource_scopes, vec!["read:Moon"]);
    }

    #[test]
    fn explicit_team_contracts_match_their_builtin_topologies() {
        let first = explicit_team_node_contract(0, 3, true, false);
        let second = explicit_team_node_contract(1, 3, true, false);
        let writer = explicit_team_node_contract(2, 3, true, false);

        assert_eq!(first.template, "cowd/parallel-research-synthesis");
        assert_eq!(second, first);
        assert_eq!(
            first.evidence_contract,
            &["summary", "evidence", "unresolved"]
        );
        assert_eq!(writer.template, "cowd/execute-review");
        assert_eq!(
            writer.output_artifacts,
            &["workspace_change", "terminal_synthesis"]
        );
        assert_eq!(
            writer.evidence_contract,
            &["implementation", "source_verification", "evidence", "risks"]
        );
    }

    #[test]
    fn custom_team_preserves_declared_bounded_evidence_scopes() {
        assert_eq!(
            declared_evidence_scopes(&[
                "evidence".to_string(),
                "evidence_scope:read:crates/runtime/src/orchestration/mod.rs".to_string(),
                "evidence_scope:read:.".to_string(),
                "evidence_scope:network:*".to_string(),
            ]),
            vec![
                "network:*".to_string(),
                "read:crates/runtime/src/orchestration/mod.rs".to_string(),
            ]
        );
    }

    #[test]
    fn managed_escalation_binds_builtin_team_templates_to_its_runtime_contract() {
        let understanding = harness_contract::strategy::understand(
            &harness_contract::strategy::StrategyInput::from_prompt(
                "必须让 Team A 的 Agent 实际调用 request_collaboration_escalation，并在之后创建独立复核 Team。",
            ),
        );
        assert!(understanding.requires_managed_collaboration_escalation);
        let team = |id: &str| GraphSemanticNode {
            node_id: id.to_string(),
            recipe: CapabilityRecipeId::Team,
            objective: "inspect bounded source evidence".to_string(),
            depends_on: Vec::new(),
            multiplicity: 1,
            focuses: Vec::new(),
            managed_agent_escalation: ManagedAgentEscalationRequirement::None,
            template: Some("builtin/cowd/direct-executor".to_string()),
            target_session_id: None,
            output_artifacts: vec!["model-supplied".to_string()],
            evidence_contract: vec!["model-supplied".to_string()],
            required_evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            required: true,
            dependency: Default::default(),
            cancellation_group: None,
        };
        let mut proposal = crate::orchestration::GraphMutationProposal {
            mutation_id: "managed-escalation-template-binding".to_string(),
            target_execution_id: None,
            expected_revision: None,
            nodes: vec![team("team-a"), team("team-b")],
            completion: Default::default(),
            collaboration_program: None,
            collaboration_escalation: None,
            retired_collaboration_instance_ids: Vec::new(),
            reason: "test".to_string(),
        };

        bind_required_managed_agent_escalation(&mut proposal, &understanding, false);

        assert_eq!(
            proposal.nodes[0].managed_agent_escalation,
            ManagedAgentEscalationRequirement::Required
        );
        assert_eq!(
            proposal.nodes[1].managed_agent_escalation,
            ManagedAgentEscalationRequirement::None
        );
        for node in proposal.nodes {
            assert_eq!(
                node.template.as_deref(),
                Some("cowd/parallel-research-synthesis")
            );
            assert_eq!(
                node.evidence_contract,
                ["summary", "evidence", "unresolved"]
            );
        }
    }

    #[test]
    fn definition_references_are_not_derived_into_workspace_leases() {
        let temporary = tempfile::TempDir::new().expect("temporary root");
        let objective = format!(
            "发布模板 cowd/biz-tech-dual-team-deliberation（template_id: cowd/biz-tech-dual-team-deliberation）并写入 {}",
            temporary.path().display()
        );
        let scopes = explicit_workspace_resource_scopes(temporary.path(), &objective, true);
        assert!(
            !scopes
                .iter()
                .any(|scope| scope.contains("biz-tech-dual-team-deliberation")),
            "definition refs must not become workspace leases: {scopes:?}"
        );
        assert!(
            scopes.iter().any(|scope| scope == "write:."),
            "the real workspace write target must still be derived: {scopes:?}"
        );
    }

    #[test]
    fn chinese_template_names_are_not_derived_into_workspace_leases() {
        let temporary = tempfile::TempDir::new().expect("temporary root");
        let objective = format!(
            "发布“业务/技术双团队研讨”模板并写入 {}",
            temporary.path().display()
        );
        let scopes = explicit_workspace_resource_scopes(temporary.path(), &objective, true);
        assert!(
            !scopes.iter().any(|scope| scope.contains("业务")),
            "Chinese template names must not become workspace leases: {scopes:?}"
        );
        assert!(
            scopes.iter().any(|scope| scope == "write:."),
            "the real workspace write target must still be derived: {scopes:?}"
        );
    }

    use crate::orchestration::{
        GraphMutationProposal, GraphSemanticNode, RuntimeOrchestrationConstraints,
        RuntimeOrchestrationOperation,
    };
    use harness_contract::execution_graph::ExecutionCompletionContract;

    fn focus(id: &str, role: &str) -> SemanticFocus {
        let output_contract = if matches!(
            role,
            "synthesizer" | "reviewer" | "arbiter" | "coordinator" | "comparator"
        ) {
            vec!["summary".to_string(), "evidence".to_string()]
        } else {
            vec!["findings".to_string(), "evidence".to_string()]
        };
        SemanticFocus {
            focus_id: id.to_string(),
            role_id: role.to_string(),
            objective: id.to_string(),
            resource_scopes: vec![format!("read:{id}")],
            evidence_responsibilities: vec!["evidence".to_string()],
            output_contract,
            output_acceptance: Vec::new(),
        }
    }

    #[test]
    fn independent_read_teams_partition_every_authorized_focus_without_role_inference() {
        let focuses = vec![
            focus("runtime", "researcher"),
            focus("gateway", "researcher"),
            focus("webui", "researcher"),
            focus("synthesis", "synthesizer"),
        ];
        let left = authorized_focuses_for_team(&focuses, 0, 2, false);
        let right = authorized_focuses_for_team(&focuses, 1, 2, false);
        let focus_ids = |items: &[SemanticFocus]| {
            items
                .iter()
                .map(|focus| focus.focus_id.clone())
                .collect::<std::collections::BTreeSet<_>>()
        };

        let left_ids = focus_ids(&left);
        let right_ids = focus_ids(&right);
        assert_eq!(left_ids.len() + right_ids.len(), 4);
        assert!(left_ids.len().abs_diff(right_ids.len()) <= 1);
        assert_eq!(left_ids.union(&right_ids).count(), 4);
        assert!(left_ids.is_disjoint(&right_ids));
    }

    #[test]
    fn partitioning_does_not_treat_output_contract_as_role_behavior() {
        let mut reviewer = focus("review-1", "实现者");
        reviewer.output_contract = vec![
            "review".to_string(),
            "evidence".to_string(),
            "risks".to_string(),
        ];
        let mut researcher = focus("research-1", "synthesizer");
        researcher.output_contract = vec!["findings".to_string(), "evidence".to_string()];
        let focuses = vec![reviewer, researcher];

        let left = authorized_focuses_for_team(&focuses, 0, 2, false);
        let right = authorized_focuses_for_team(&focuses, 1, 2, false);
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 1);
        assert_eq!(left[0].focus_id, "review-1");
        assert_eq!(right[0].focus_id, "research-1");
        assert_eq!(
            left[0].output_contract,
            vec![
                "review".to_string(),
                "evidence".to_string(),
                "risks".to_string()
            ],
            "the output schema remains data, never a hidden scheduling hint"
        );
    }

    #[test]
    fn external_research_contract_rejects_repeat_fetches_as_independent_confidence() {
        let plans = external_research_focus_partition_plans(2);
        let researcher = plans
            .iter()
            .find(|plan| plan.role_id == "researcher")
            .expect("researcher plan");
        let synthesizer = plans
            .iter()
            .find(|plan| plan.role_id == "synthesizer")
            .expect("synthesizer plan");

        assert!(researcher.shared_baseline.iter().any(|rule| {
            rule.contains("independent source diversity") && rule.contains("never repeated fetch")
        }));
        assert!(researcher.slots.iter().all(|slot| {
            slot.evidence_responsibility.contains("repeated retrievals")
                && slot.evidence_responsibility.contains("title-only evidence")
        }));
        assert!(synthesizer
            .slots
            .iter()
            .all(|slot| { slot.boundary.contains("deduplicate repeated publishers") }));
    }

    #[test]
    fn explicitly_named_root_file_is_an_authorized_team_focus() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(workspace.path().join("README.md"), "# Test").expect("write README");
        std::fs::create_dir(workspace.path().join("crates")).expect("create crates");
        std::fs::create_dir(workspace.path().join("docs")).expect("create docs");

        let scopes = bounded_workspace_focus_scopes(
            workspace.path(),
            "并行阅读当前工作区 README.md 中的架构边界，不要修改文件",
            2,
            false,
            true,
        );

        assert_eq!(scopes, vec!["read:README.md"]);

        let plans = derive_team_focus_partition_plans(
            "并行阅读当前工作区 README.md 中的架构边界，不要修改文件",
            workspace.path(),
            &[],
            3,
            false,
            true,
            false,
        );
        let researchers = plans
            .iter()
            .find(|plan| plan.role_id == "researcher")
            .expect("researcher focus plan");
        assert_eq!(researchers.slots.len(), 3);
        assert!(researchers.slots.iter().all(|slot| {
            slot.capability_cropped_refs == vec!["read:README.md"]
                && slot.overlap_budget_bp == 10_000
        }));
    }

    #[test]
    fn bare_relative_workspace_paths_are_explicit_team_scopes() {
        let workspace = tempfile::tempdir().expect("workspace");
        for relative in ["crates/runtime", "crates/provider"] {
            std::fs::create_dir_all(workspace.path().join(relative)).expect("workspace partition");
        }
        let objective = "启动两个并行团队检查 crates/runtime 与 crates/provider，不要修改文件";
        let scopes = bounded_workspace_focus_scopes(workspace.path(), objective, 2, false, true);
        assert_eq!(scopes, vec!["read:crates/provider", "read:crates/runtime"]);
    }

    #[test]
    fn read_only_source_patterns_bind_their_existing_workspace_ancestor() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("crates/runtime/src"))
            .expect("workspace source tree");

        for pattern in ["crates/**/*.rs", "crates/.../*.rs"] {
            let objective = format!("只读核查 {pattern} 中的源码证据，不修改文件");
            assert_eq!(
                explicit_workspace_resource_scopes(workspace.path(), &objective, false),
                vec!["read:crates"]
            );
        }
    }

    #[test]
    fn source_patterns_never_widen_write_or_escape_the_workspace() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("crates/runtime/src"))
            .expect("workspace source tree");

        assert!(
            explicit_workspace_resource_scopes(workspace.path(), "修改 crates/**/*.rs", true,)
                .is_empty()
        );
        assert!(explicit_workspace_resource_scopes(
            workspace.path(),
            "只读检查 ../secrets/**/*.rs",
            false,
        )
        .is_empty());
    }

    #[test]
    fn write_team_reviewer_receives_only_read_access_to_committed_outputs() {
        let workspace = tempfile::tempdir().expect("workspace");
        let plans = derive_team_focus_partition_plans(
            "analyze sources and write report",
            workspace.path(),
            &[
                "read:src/a.rs".to_string(),
                "read:src/b.rs".to_string(),
                "write:evidence/report.html".to_string(),
            ],
            1,
            true,
            true,
            false,
        );
        let reviewer = plans
            .iter()
            .find(|plan| plan.role_id == "reviewer")
            .expect("reviewer plan");
        assert_eq!(reviewer.slots.len(), 1);
        assert_eq!(
            reviewer.slots[0].capability_cropped_refs,
            vec!["read:evidence/report.html"]
        );
        assert_eq!(
            reviewer.slots[0].output_acceptance,
            vec!["review", "evidence", "risks"]
        );

        let focuses = semantic_focuses_from_plans(&plans);
        let reviewer_focus = focuses
            .iter()
            .find(|focus| focus.role_id == "reviewer")
            .expect("reviewer semantic focus");
        assert_eq!(
            reviewer_focus.output_acceptance,
            vec!["review", "evidence", "risks"]
        );
    }

    #[test]
    fn explicitly_named_directory_is_shared_instead_of_padding_unrelated_scopes() {
        let workspace = tempfile::tempdir().expect("workspace");
        for relative in ["Moon", "Code"] {
            std::fs::create_dir_all(workspace.path().join(relative)).expect("workspace directory");
        }
        let objective = "必须启动两个并行 Agent，仅检查 /workspace/Moon 目录，不修改文件";

        let scopes = bounded_workspace_focus_scopes(workspace.path(), objective, 2, false, true);
        assert_eq!(scopes, vec!["read:Moon"]);

        let plans = derive_team_focus_partition_plans(
            objective,
            workspace.path(),
            &[],
            2,
            false,
            true,
            false,
        );
        let researchers = plans
            .iter()
            .find(|plan| plan.role_id == "researcher")
            .expect("researcher focus plan");
        assert_eq!(researchers.slots.len(), 2);
        assert_ne!(researchers.slots[0].focus_id, researchers.slots[1].focus_id);
        assert!(researchers.slots.iter().all(|slot| {
            slot.capability_cropped_refs == vec!["read:Moon"] && slot.overlap_budget_bp == 10_000
        }));
    }

    #[test]
    fn explicit_absolute_target_wins_over_generic_investigation_angles() {
        let workspace = tempfile::tempdir().expect("workspace");
        for relative in ["Moon", "docs", "tests"] {
            std::fs::create_dir_all(workspace.path().join(relative)).expect("workspace directory");
        }
        let objective = format!(
            "组建包含3个并行智能体的团队，对 {} 做核查：分别检查模块、测试、README 和文档一致性，不得修改任何文件",
            workspace.path().join("Moon").display()
        );

        let scopes = bounded_workspace_focus_scopes(workspace.path(), &objective, 3, false, true);
        assert_eq!(scopes, vec!["read:Moon"]);

        let plans = derive_team_focus_partition_plans(
            &objective,
            workspace.path(),
            &[],
            3,
            false,
            true,
            false,
        );
        let researchers = plans
            .iter()
            .find(|plan| plan.role_id == "researcher")
            .expect("researcher plan");
        assert_eq!(researchers.slots.len(), 3);
        assert!(researchers
            .slots
            .iter()
            .all(|slot| slot.capability_cropped_refs == vec!["read:Moon"]));
    }

    #[test]
    fn explicit_local_target_wins_over_generic_external_research_language() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("Code/AICS")).expect("workspace directory");
        let objective = format!(
            "组建3个并行 researcher，对 {} 做真实代码研究，不得修改任何文件",
            workspace.path().join("Code/AICS").display()
        );

        let plans = derive_team_focus_partition_plans(
            &objective,
            workspace.path(),
            &[],
            3,
            false,
            true,
            true,
        );
        let researchers = plans
            .iter()
            .find(|plan| plan.role_id == "researcher")
            .expect("researcher plan");

        assert_eq!(researchers.slots.len(), 3);
        assert!(researchers
            .slots
            .iter()
            .all(|slot| slot.capability_cropped_refs == vec!["read:Code/AICS"]));
        assert!(plans
            .iter()
            .flat_map(|plan| &plan.slots)
            .flat_map(|slot| &slot.capability_cropped_refs)
            .all(|scope| !scope.starts_with("network:")));
    }

    #[test]
    fn broad_workspace_scope_requires_explicit_global_language() {
        assert!(!requests_broad_workspace_scope(
            "inspect a frontend webui that is not in this workspace"
        ));
        assert!(!requests_broad_workspace_scope(
            "review the runtime crate in this repository"
        ));
        assert!(requests_broad_workspace_scope(
            "perform a comprehensive audit across the repository"
        ));
        assert!(requests_broad_workspace_scope("全面审计整个工作区"));
    }

    #[test]
    fn model_constraints_cannot_widen_read_only_intent_to_write_authority() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("Moon")).expect("workspace directory");
        let mut request = RuntimeOrchestrationCommand {
            intent: "使用两个 Agent 只读检查 Moon，不修改文件".to_string(),
            model_lease: None,
            session_id: Some("session-1".to_string()),
            lineage: None,
            mission_id: None,
            operation: RuntimeOrchestrationOperation::Propose,
            inspect_execution_id: None,
            proposal: Some(GraphMutationProposal {
                mutation_id: "read-only-team".to_string(),
                target_execution_id: None,
                expected_revision: None,
                nodes: vec![GraphSemanticNode {
                    node_id: "team".to_string(),
                    recipe: CapabilityRecipeId::Team,
                    objective: "只读检查 Moon".to_string(),
                    depends_on: Vec::new(),
                    multiplicity: 1,
                    focuses: Vec::new(),
                    managed_agent_escalation:
                        harness_contract::orchestration::ManagedAgentEscalationRequirement::None,
                    template: None,
                    target_session_id: None,
                    output_artifacts: vec!["terminal_synthesis".to_string()],
                    evidence_contract: vec!["summary".to_string()],
                    required_evidence_refs: Vec::new(),
                    resource_scopes: vec!["write:Moon".to_string()],
                    required: true,
                    dependency: Default::default(),
                    cancellation_group: None,
                }],
                completion: ExecutionCompletionContract::default(),
                collaboration_program: None,
                collaboration_escalation: None,
                retired_collaboration_instance_ids: Vec::new(),
                reason: "model requested write despite read-only intent".to_string(),
            }),
            control: None,
            template_proposal: None,
            ephemeral_team_templates: Default::default(),

            collaboration_intent: None,
            collaboration_semantic_intent: None,

            input_disposition: None,
            selection_mode: None,
            strategy_binding: None,
            capabilities: vec!["resource:write:Moon".to_string()],
            evidence_refs: Vec::new(),
            constraints: RuntimeOrchestrationConstraints {
                requires_write: Some(true),
                permission_ceiling: harness_contract::policy::PermissionMode::DangerFullAccess,
                max_parallel_agents: Some(2),
                ..RuntimeOrchestrationConstraints::default()
            },
            surface: None,
        };

        bind_semantic_resource_authority(&mut request, None, workspace.path());

        assert_eq!(request.constraints.requires_write, Some(false));
        assert!(request
            .capabilities
            .iter()
            .any(|capability| capability == "resource:read:Moon"));
        assert!(request
            .capabilities
            .iter()
            .all(|capability| !capability.contains("write:Moon")));
    }

    #[test]
    fn custom_workspace_template_keeps_write_authority_when_intent_requires_write() {
        let workspace = tempfile::tempdir().expect("workspace");
        let node = GraphSemanticNode {
            node_id: "cross-team-collaborative-decision".to_string(),
            recipe: CapabilityRecipeId::Team,
            objective: "生成统一 HTML 决策报告并写入工作区".to_string(),
            depends_on: Vec::new(),
            multiplicity: 1,
            focuses: vec![
                SemanticFocus {
                    focus_id: "writer".to_string(),
                    role_id: "writer".to_string(),
                    objective: "write the bounded decision report".to_string(),
                    resource_scopes: Vec::new(),
                    evidence_responsibilities: vec!["written report".to_string()],
                    output_contract: Vec::new(),
                    output_acceptance: Vec::new(),
                },
                SemanticFocus {
                    focus_id: "reviewer".to_string(),
                    role_id: "reviewer".to_string(),
                    objective: "verify the bounded decision report".to_string(),
                    resource_scopes: Vec::new(),
                    evidence_responsibilities: vec!["verification evidence".to_string()],
                    output_contract: Vec::new(),
                    output_acceptance: Vec::new(),
                },
            ],
            managed_agent_escalation:
                harness_contract::orchestration::ManagedAgentEscalationRequirement::None,
            template: Some("workspace/cross-team-collaborative-decision".to_string()),
            target_session_id: None,
            output_artifacts: vec!["unified-html-decision-report".to_string()],
            evidence_contract: vec!["evidence".to_string()],
            required_evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            required: true,
            dependency: Default::default(),
            cancellation_group: None,
        };
        let mut request = RuntimeOrchestrationCommand {
            intent: format!("启动跨团队协同决策并写入 {}", workspace.path().display()),
            model_lease: None,
            session_id: Some("session-1".to_string()),
            lineage: None,
            mission_id: None,
            operation: RuntimeOrchestrationOperation::Propose,
            inspect_execution_id: None,
            proposal: Some(GraphMutationProposal {
                mutation_id: "custom-write-team".to_string(),
                target_execution_id: None,
                expected_revision: None,
                nodes: vec![node],
                completion: ExecutionCompletionContract::default(),
                collaboration_program: None,
                collaboration_escalation: None,
                retired_collaboration_instance_ids: Vec::new(),
                reason: "custom template write team".to_string(),
            }),
            control: None,
            template_proposal: None,
            ephemeral_team_templates: Default::default(),
            collaboration_intent: None,
            collaboration_semantic_intent: None,
            input_disposition: None,
            selection_mode: None,
            strategy_binding: None,
            capabilities: Vec::new(),
            evidence_refs: Vec::new(),
            constraints: RuntimeOrchestrationConstraints {
                max_parallel_agents: Some(3),
                requires_write: Some(true),
                permission_ceiling: harness_contract::policy::PermissionMode::DangerFullAccess,
                ..RuntimeOrchestrationConstraints::default()
            },
            surface: None,
        };

        let mut understanding = harness_contract::strategy::understand(
            &harness_contract::strategy::StrategyInput::from_prompt(&request.intent),
        );
        understanding.requires_write = true;
        understanding.requests_multi_agent = true;
        understanding.independent_workstreams = 3;
        bind_semantic_resource_authority_with_understanding(
            &mut request,
            &understanding,
            workspace.path(),
        );

        let team = &request.proposal.as_ref().expect("proposal").nodes[0];
        assert_eq!(request.constraints.requires_write, Some(true));
        assert!(
            team.resource_scopes.iter().any(|scope| scope == "write:."),
            "custom write-capable template must receive a bounded workspace write lease: {:?}",
            team.resource_scopes
        );
        assert!(
            team.resource_scopes.iter().any(|scope| scope == "read:."),
            "full-trust write teams must also receive a whole-workspace read lease: {:?}",
            team.resource_scopes
        );
        assert_eq!(
            team.focuses
                .iter()
                .map(|focus| focus.role_id.as_str())
                .collect::<Vec<_>>(),
            vec!["writer", "reviewer"],
            "custom templates must preserve the model-selected role topology rather than replacing or dropping it"
        );
        assert!(team.focuses.iter().all(|focus| {
            !focus.resource_scopes.is_empty()
                && focus
                    .resource_scopes
                    .iter()
                    .all(|scope| team.resource_scopes.contains(scope))
        }));
    }

    #[test]
    fn custom_template_authority_does_not_invent_an_unselected_role() {
        assert!(bind_declared_focuses_to_node_scopes(
            Vec::new(),
            &["read:crates/runtime".to_string()],
        )
        .is_empty());
    }

    #[test]
    fn unnamed_write_artifact_uses_workspace_root_instead_of_arbitrary_directory() {
        let workspace = tempfile::tempdir().expect("workspace");
        for relative in ["Code", "downloads", "unrelated"] {
            std::fs::create_dir_all(workspace.path().join(relative)).expect("workspace directory");
        }

        let scopes = bounded_workspace_focus_scopes(
            workspace.path(),
            "使用第二个团队生成一套 HTML 研究报告网站",
            2,
            true,
            true,
        );

        assert_eq!(scopes, vec!["write:."]);
    }

    #[test]
    fn explicitly_named_new_artifact_gets_an_exact_workspace_scope() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("evidence")).expect("evidence directory");
        let target = workspace.path().join("evidence/report.html");
        let objective = format!("生成中文 HTML 并写入 {}", target.display());

        let scopes = bounded_workspace_focus_scopes(workspace.path(), &objective, 1, true, true);

        assert_eq!(scopes, vec!["write:evidence/report.html"]);
    }

    #[test]
    fn writer_scope_keeps_source_inputs_read_only_and_only_grants_the_report_target_write() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("evidence")).expect("evidence directory");
        for source in ["mission_control.rs", "team_authority.rs", "task_store.rs"] {
            std::fs::write(workspace.path().join("evidence").join(source), "fixture")
                .expect("source fixture");
        }
        let mission = workspace.path().join("evidence/mission_control.rs");
        let authority = workspace.path().join("evidence/team_authority.rs");
        let task = workspace.path().join("evidence/task_store.rs");
        let report = workspace.path().join("evidence/report.html");
        let objective = format!(
            "阅读并分析 {}、{} 和 {}；生成中文报告并写入 {}",
            mission.display(),
            authority.display(),
            task.display(),
            report.display(),
        );

        let scopes = bounded_workspace_focus_scopes(workspace.path(), &objective, 1, true, true);

        assert!(scopes.contains(&"read:evidence/mission_control.rs".to_string()));
        assert!(scopes.contains(&"read:evidence/team_authority.rs".to_string()));
        assert!(scopes.contains(&"read:evidence/task_store.rs".to_string()));
        assert!(scopes.contains(&"write:evidence/report.html".to_string()));
        assert_eq!(
            scopes
                .iter()
                .filter(|scope| scope.starts_with("write:"))
                .count(),
            1,
        );
    }

    #[test]
    fn path_components_that_contain_english_action_substrings_do_not_gain_write_authority() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("evidence/team-fixture"))
            .expect("fixture directory");
        for source in ["mission_control.rs", "team_authority.rs", "task_store.rs"] {
            std::fs::write(
                workspace.path().join("evidence/team-fixture").join(source),
                "fixture",
            )
            .expect("source fixture");
        }
        let mission = workspace
            .path()
            .join("evidence/team-fixture/mission_control.rs");
        let authority = workspace
            .path()
            .join("evidence/team-fixture/team_authority.rs");
        let task = workspace.path().join("evidence/team-fixture/task_store.rs");
        let report = workspace.path().join("evidence/report.html");
        let objective = format!(
            "Team 1阅读并分析 {mission}；Team 2阅读并分析 {authority} 和 {task}。Team 3使用文件写入工具生成中文HTML报告到 {report}",
            mission = mission.display(),
            authority = authority.display(),
            task = task.display(),
            report = report.display(),
        );

        let scopes = bounded_workspace_focus_scopes(workspace.path(), &objective, 1, true, true);

        assert!(scopes.contains(&"read:evidence/team-fixture/mission_control.rs".to_string()));
        assert!(scopes.contains(&"read:evidence/team-fixture/task_store.rs".to_string()));
        assert!(scopes.contains(&"read:evidence/team-fixture/team_authority.rs".to_string()));
        assert!(scopes.contains(&"write:evidence/report.html".to_string()));
        assert_eq!(
            scopes
                .iter()
                .filter(|scope| scope.starts_with("write:"))
                .count(),
            1,
        );
    }

    #[test]
    fn explicit_team_clauses_bind_each_direct_team_to_its_declared_paths() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("evidence/team-fixture"))
            .expect("fixture directory");
        for source in ["mission_control.rs", "team_authority.rs", "task_store.rs"] {
            std::fs::write(
                workspace.path().join("evidence/team-fixture").join(source),
                "fixture",
            )
            .expect("source fixture");
        }
        let mission = workspace
            .path()
            .join("evidence/team-fixture/mission_control.rs");
        let authority = workspace
            .path()
            .join("evidence/team-fixture/team_authority.rs");
        let task = workspace.path().join("evidence/team-fixture/task_store.rs");
        let objective = format!(
            "Team 1和Team 2并行：Team 1阅读 {mission}；Team 2阅读 {authority} 和 {task}。Team 3负责汇总。",
            mission = mission.display(),
            authority = authority.display(),
            task = task.display(),
        );

        let first = direct_executor_focus_for_team(&objective, workspace.path(), &[], 0, 2);
        let second = direct_executor_focus_for_team(&objective, workspace.path(), &[], 1, 2);

        assert_eq!(first.len(), 1);
        assert_eq!(first[0].role_id, "executor");
        assert_eq!(
            first[0].resource_scopes,
            vec!["read:evidence/team-fixture/mission_control.rs"]
        );
        assert_eq!(second.len(), 1);
        assert!(second[0]
            .resource_scopes
            .contains(&"read:evidence/team-fixture/team_authority.rs".to_string()));
        assert!(second[0]
            .resource_scopes
            .contains(&"read:evidence/team-fixture/task_store.rs".to_string()));
        assert_eq!(second[0].resource_scopes.len(), 2);
    }

    #[test]
    fn explicit_existing_write_target_is_not_downgraded_to_read_only() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("evidence")).expect("evidence directory");
        let report = workspace.path().join("evidence/report.html");
        std::fs::write(&report, "old").expect("existing report");
        let objective = format!("更新并写入 {}", report.display());

        assert_eq!(
            bounded_workspace_focus_scopes(workspace.path(), &objective, 1, true, true),
            vec!["write:evidence/report.html"],
        );
    }

    #[test]
    fn mixed_team_authority_preserves_research_and_writer_role_contracts() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("src")).expect("source directory");
        std::fs::create_dir_all(workspace.path().join("evidence")).expect("evidence directory");
        let target = workspace.path().join("evidence/report.html");
        let intent = format!(
            "启动三个团队调研工作区源码，第三个团队写入 {}",
            target.display()
        );
        let lineage = harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
            root_task_id: "task-1".to_string(),
            task_id: "task-1".to_string(),
            generation: 1,
        };
        let node = |id: &str, template: &str, output_artifacts: Vec<String>| GraphSemanticNode {
            node_id: id.to_string(),
            recipe: CapabilityRecipeId::Team,
            objective: intent.clone(),
            depends_on: Vec::new(),
            multiplicity: 1,
            focuses: Vec::new(),
            managed_agent_escalation:
                harness_contract::orchestration::ManagedAgentEscalationRequirement::None,
            template: Some(template.to_string()),
            target_session_id: None,
            output_artifacts,
            evidence_contract: vec!["evidence".to_string()],
            required_evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            required: true,
            dependency: Default::default(),
            cancellation_group: None,
        };
        let mut request = RuntimeOrchestrationCommand {
            intent: intent.clone(),
            model_lease: None,
            session_id: Some("session-1".to_string()),
            lineage: Some(lineage),
            mission_id: Some("mission-1".to_string()),
            operation: RuntimeOrchestrationOperation::Propose,
            inspect_execution_id: None,
            proposal: Some(GraphMutationProposal {
                mutation_id: "mixed-team".to_string(),
                target_execution_id: None,
                expected_revision: None,
                nodes: vec![
                    node(
                        "research-1",
                        "cowd/parallel-research-synthesis",
                        vec!["terminal_synthesis".to_string()],
                    ),
                    node(
                        "research-2",
                        "cowd/parallel-research-synthesis",
                        vec!["terminal_synthesis".to_string()],
                    ),
                    node(
                        "writer",
                        "cowd/execute-review",
                        vec![
                            "workspace_change".to_string(),
                            "terminal_synthesis".to_string(),
                        ],
                    ),
                ],
                completion: ExecutionCompletionContract::default(),
                collaboration_program: None,
                collaboration_escalation: None,
                retired_collaboration_instance_ids: Vec::new(),
                reason: "mixed research and write".to_string(),
            }),
            control: None,
            template_proposal: None,
            ephemeral_team_templates: Default::default(),

            collaboration_intent: None,
            collaboration_semantic_intent: None,

            input_disposition: None,
            selection_mode: None,
            strategy_binding: None,
            capabilities: Vec::new(),
            evidence_refs: Vec::new(),
            constraints: RuntimeOrchestrationConstraints {
                max_parallel_agents: Some(3),
                requires_write: Some(true),
                permission_ceiling: harness_contract::policy::PermissionMode::WorkspaceWrite,
                ..RuntimeOrchestrationConstraints::default()
            },
            surface: None,
        };

        bind_semantic_resource_authority(&mut request, None, workspace.path());

        let nodes = &request.proposal.as_ref().expect("proposal").nodes;
        for research in &nodes[..2] {
            assert!(
                research
                    .focuses
                    .iter()
                    .any(|focus| focus.role_id == "researcher"),
                "authority assigns real scoped focuses without padding to a hard-coded minimum",
            );
            assert!(research
                .focuses
                .iter()
                .any(|focus| focus.role_id == "researcher"));
            assert!(research.focuses.iter().all(|focus| focus
                .resource_scopes
                .iter()
                .all(|scope| scope.starts_with("read:"))));
        }
        let implementer = nodes[2]
            .focuses
            .iter()
            .find(|focus| focus.role_id == "implementer")
            .expect("implementer focus");
        let reviewer = nodes[2]
            .focuses
            .iter()
            .find(|focus| focus.role_id == "reviewer")
            .expect("reviewer focus");
        assert!(implementer
            .resource_scopes
            .iter()
            .all(|scope| scope.starts_with("write:")));
        assert!(reviewer
            .resource_scopes
            .iter()
            .all(|scope| scope.starts_with("read:")));
        assert!(implementer.resource_scopes.iter().all(|scope| {
            scope
                .strip_prefix("write:")
                .is_some_and(|path| reviewer.resource_scopes.contains(&format!("read:{path}")))
        }));
    }

    #[test]
    fn explicit_direct_teams_keep_declared_file_groups_and_writer_mixed_authority() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("evidence/team-fixture"))
            .expect("fixture directory");
        for source in ["mission_control.rs", "team_authority.rs", "task_store.rs"] {
            std::fs::write(
                workspace.path().join("evidence/team-fixture").join(source),
                "fixture",
            )
            .expect("source fixture");
        }
        let mission = workspace
            .path()
            .join("evidence/team-fixture/mission_control.rs");
        let authority = workspace
            .path()
            .join("evidence/team-fixture/team_authority.rs");
        let task = workspace.path().join("evidence/team-fixture/task_store.rs");
        let report = workspace.path().join("evidence/report.html");
        let intent = format!(
            "使用恰好3个Team。Team 1阅读 {mission}；Team 2阅读 {authority} 和 {task}。Team 3生成报告并写入 {report}",
            mission = mission.display(),
            authority = authority.display(),
            task = task.display(),
            report = report.display(),
        );
        let node = |id: &str, template: &str, artifacts: Vec<String>| GraphSemanticNode {
            node_id: id.to_string(),
            recipe: CapabilityRecipeId::Team,
            objective: intent.clone(),
            depends_on: Vec::new(),
            multiplicity: 1,
            focuses: Vec::new(),
            managed_agent_escalation:
                harness_contract::orchestration::ManagedAgentEscalationRequirement::None,
            template: Some(template.to_string()),
            target_session_id: None,
            output_artifacts: artifacts,
            evidence_contract: vec!["evidence".to_string()],
            required_evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            required: true,
            dependency: Default::default(),
            cancellation_group: None,
        };
        let mut request = RuntimeOrchestrationCommand {
            intent: intent.clone(),
            model_lease: None,
            session_id: Some("session-1".to_string()),
            lineage: Some(harness_contract::execution_graph::ExecutionGraphLineage {
                session_id: "session-1".to_string(),
                turn_id: "turn-1".to_string(),
                root_task_id: "task-1".to_string(),
                task_id: "task-1".to_string(),
                generation: 1,
            }),
            mission_id: Some("mission-1".to_string()),
            operation: RuntimeOrchestrationOperation::Propose,
            inspect_execution_id: None,
            proposal: Some(GraphMutationProposal {
                mutation_id: "direct-team".to_string(),
                target_execution_id: None,
                expected_revision: None,
                nodes: vec![
                    node(
                        "research-1",
                        "cowd/direct-executor",
                        vec!["terminal_synthesis".to_string()],
                    ),
                    node(
                        "research-2",
                        "cowd/direct-executor",
                        vec!["terminal_synthesis".to_string()],
                    ),
                    node(
                        "writer",
                        "cowd/execute-review",
                        vec![
                            "workspace_change".to_string(),
                            "terminal_synthesis".to_string(),
                        ],
                    ),
                ],
                completion: ExecutionCompletionContract::default(),
                collaboration_program: None,
                collaboration_escalation: None,
                retired_collaboration_instance_ids: Vec::new(),
                reason: "explicit direct Teams".to_string(),
            }),
            control: None,
            template_proposal: None,
            ephemeral_team_templates: Default::default(),

            collaboration_intent: None,
            collaboration_semantic_intent: None,

            input_disposition: None,
            selection_mode: None,
            strategy_binding: None,
            capabilities: Vec::new(),
            evidence_refs: Vec::new(),
            constraints: RuntimeOrchestrationConstraints {
                max_parallel_agents: Some(3),
                requires_write: Some(true),
                permission_ceiling: harness_contract::policy::PermissionMode::WorkspaceWrite,
                ..RuntimeOrchestrationConstraints::default()
            },
            surface: None,
        };

        bind_semantic_resource_authority(&mut request, None, workspace.path());

        let nodes = &request.proposal.as_ref().expect("proposal").nodes;
        assert_eq!(nodes[0].focuses.len(), 1);
        assert_eq!(nodes[0].focuses[0].role_id, "executor");
        assert_eq!(
            nodes[0].resource_scopes,
            vec![
                "read:evidence/team-fixture/mission_control.rs".to_string(),
                "session:session-1".to_string()
            ]
        );
        assert_eq!(nodes[1].focuses.len(), 1);
        assert!(nodes[1]
            .resource_scopes
            .contains(&"read:evidence/team-fixture/team_authority.rs".to_string()));
        assert!(nodes[1]
            .resource_scopes
            .contains(&"read:evidence/team-fixture/task_store.rs".to_string()));
        assert!(nodes[2]
            .resource_scopes
            .contains(&"write:evidence/report.html".to_string()));
        assert_eq!(
            nodes[2]
                .resource_scopes
                .iter()
                .filter(|scope| scope.starts_with("write:"))
                .count(),
            1,
        );
        assert!(nodes[2]
            .resource_scopes
            .contains(&"read:evidence/team-fixture/task_store.rs".to_string()));
    }

    #[test]
    fn runtime_replaces_model_team_scopes_with_disjoint_authoritative_partitions() {
        let workspace = tempfile::tempdir().expect("workspace");
        for relative in ["crates/runtime", "crates/gateway", "surfaces/webui"] {
            std::fs::create_dir_all(workspace.path().join(relative)).expect("workspace partition");
        }
        let mut request = RuntimeOrchestrationCommand {
            intent: "必须启动 Team 审查 runtime gateway webui 架构".to_string(),
            model_lease: None,
            session_id: Some("session-1".to_string()),
            lineage: Some(harness_contract::execution_graph::ExecutionGraphLineage {
                session_id: "session-1".to_string(),
                turn_id: "turn-1".to_string(),
                root_task_id: "task-root-1".to_string(),
                task_id: "task-root-1".to_string(),
                generation: 1,
            }),
            mission_id: Some("mission-1".to_string()),
            operation: RuntimeOrchestrationOperation::Propose,
            inspect_execution_id: None,
            proposal: Some(GraphMutationProposal {
                mutation_id: "mutation-1".to_string(),
                target_execution_id: None,
                expected_revision: None,
                nodes: vec![GraphSemanticNode {
                    node_id: "team".to_string(),
                    recipe: CapabilityRecipeId::Team,
                    objective: "审查三个边界".to_string(),
                    depends_on: Vec::new(),
                    multiplicity: 1,
                    focuses: vec![
                        SemanticFocus {
                            focus_id: "model-a".to_string(),
                            role_id: "researcher".to_string(),
                            objective: "model scope a".to_string(),
                            resource_scopes: vec!["write:../../outside".to_string()],
                            evidence_responsibilities: Vec::new(),
                            output_contract: Vec::new(),
                            output_acceptance: Vec::new(),
                        },
                        SemanticFocus {
                            focus_id: "model-b".to_string(),
                            role_id: "researcher".to_string(),
                            objective: "model scope b".to_string(),
                            resource_scopes: vec!["write:../../outside".to_string()],
                            evidence_responsibilities: Vec::new(),
                            output_contract: Vec::new(),
                            output_acceptance: Vec::new(),
                        },
                    ],
                    managed_agent_escalation:
                        harness_contract::orchestration::ManagedAgentEscalationRequirement::None,
                    template: None,
                    target_session_id: None,
                    output_artifacts: vec!["terminal_synthesis".to_string()],
                    evidence_contract: vec!["summary".to_string()],
                    required_evidence_refs: Vec::new(),
                    resource_scopes: vec!["write:../../outside".to_string()],
                    required: true,
                    dependency: Default::default(),
                    cancellation_group: None,
                }],
                completion: ExecutionCompletionContract::default(),
                collaboration_program: None,
                collaboration_escalation: None,
                retired_collaboration_instance_ids: Vec::new(),
                reason: "independent review".to_string(),
            }),
            control: None,
            template_proposal: None,
            ephemeral_team_templates: Default::default(),

            collaboration_intent: None,
            collaboration_semantic_intent: None,

            input_disposition: None,
            selection_mode: None,
            strategy_binding: None,
            capabilities: vec!["resource:write:../../outside".to_string()],
            evidence_refs: Vec::new(),
            constraints: RuntimeOrchestrationConstraints {
                max_parallel_agents: Some(3),
                permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
                ..RuntimeOrchestrationConstraints::default()
            },
            surface: None,
        };

        bind_semantic_resource_authority(&mut request, None, workspace.path());

        let node = &request.proposal.as_ref().expect("proposal").nodes[0];
        assert!(node
            .resource_scopes
            .iter()
            .all(
                |scope| (scope.starts_with("read:") || scope.starts_with("session:"))
                    && !scope.contains("..")
            ));
        let researcher_scopes = node
            .focuses
            .iter()
            .filter(|focus| focus.role_id == "researcher")
            .map(|focus| focus.resource_scopes.clone())
            .collect::<Vec<_>>();
        assert!(researcher_scopes.len() >= 2);
        assert!(researcher_scopes.iter().all(|scopes| scopes.len() == 1));
        assert_ne!(researcher_scopes[0], researcher_scopes[1]);
    }

    #[test]
    fn session_origin_team_always_receives_session_evidence_lease() {
        let workspace = tempfile::tempdir().expect("workspace");
        let mut request = RuntimeOrchestrationCommand {
            intent: "并行分析当前工作区并产出结论".to_string(),
            model_lease: None,
            session_id: Some("session-1".to_string()),
            lineage: None,
            mission_id: Some("mission-default-test".to_string()),
            operation: RuntimeOrchestrationOperation::Propose,
            inspect_execution_id: None,
            proposal: Some(GraphMutationProposal {
                mutation_id: "session-team".to_string(),
                target_execution_id: None,
                expected_revision: None,
                nodes: vec![GraphSemanticNode {
                    node_id: "research-1".to_string(),
                    recipe: CapabilityRecipeId::Team,
                    objective: "并行分析当前工作区".to_string(),
                    depends_on: Vec::new(),
                    multiplicity: 1,
                    focuses: Vec::new(),
                    managed_agent_escalation:
                        harness_contract::orchestration::ManagedAgentEscalationRequirement::None,
                    template: Some("cowd/parallel-research-synthesis".to_string()),
                    target_session_id: None,
                    output_artifacts: vec!["terminal_synthesis".to_string()],
                    evidence_contract: vec!["evidence".to_string()],
                    required_evidence_refs: Vec::new(),
                    resource_scopes: Vec::new(),
                    required: true,
                    dependency: Default::default(),
                    cancellation_group: None,
                }],
                completion: ExecutionCompletionContract::default(),
                collaboration_program: None,
                collaboration_escalation: None,
                retired_collaboration_instance_ids: Vec::new(),
                reason: "session lease test".to_string(),
            }),
            control: None,
            template_proposal: None,
            ephemeral_team_templates: Default::default(),

            collaboration_intent: None,
            collaboration_semantic_intent: None,

            input_disposition: None,
            selection_mode: None,
            strategy_binding: None,
            capabilities: Vec::new(),
            evidence_refs: Vec::new(),
            constraints: RuntimeOrchestrationConstraints {
                max_parallel_agents: Some(2),
                permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
                ..RuntimeOrchestrationConstraints::default()
            },
            surface: None,
        };

        bind_semantic_resource_authority(&mut request, None, workspace.path());

        let node = &request.proposal.as_ref().expect("proposal").nodes[0];
        assert!(node
            .resource_scopes
            .iter()
            .any(|scope| scope == "session:session-1"));
        assert!(request
            .capabilities
            .iter()
            .any(|capability| capability == "resource:session:session-1"));
    }
}
