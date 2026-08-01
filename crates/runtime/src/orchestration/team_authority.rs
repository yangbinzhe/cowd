//! Runtime-owned Team resource authority.
//!
//! Callers may request collaboration and suggest a published template, but
//! only Runtime derives filesystem, network, and session evidence leases.

use std::path::Path;

use harness_contract::team::{FocusPartitionPlan, FocusPartitionSlot};

use crate::execution_core::RuntimeExecutionDecision;
use crate::orchestration::{CapabilityRecipeId, RuntimeOrchestrationRequest, SemanticFocus};

pub(crate) fn bind_semantic_resource_authority(
    request: &mut RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
    workspace_root: &Path,
) {
    let Some(proposal) = request.proposal.as_mut() else {
        return;
    };
    let inferred = harness_contract::strategy::decide_strategy(
        &harness_contract::strategy::StrategyInput::from_prompt(&request.intent),
    );
    let understanding = leased_decision
        .map(|decision| &decision.strategy.understanding)
        .unwrap_or(&inferred.understanding);
    let requires_write = understanding.requires_write
        && request
            .constraints
            .permission_ceiling
            .permits(harness_contract::policy::PermissionMode::WorkspaceWrite);
    request.constraints.requires_write = Some(requires_write);
    let requested_count = request
        .constraints
        .max_parallel_agents
        .unwrap_or_else(|| usize::from(understanding.independent_workstreams.max(2)))
        .clamp(2, 6);
    let explicit_team = understanding.requests_multi_agent
        || proposal
            .nodes
            .iter()
            .any(|node| node.recipe == CapabilityRecipeId::Team);
    let plans = derive_team_focus_partition_plans(
        &request.intent,
        workspace_root,
        &[],
        requested_count,
        requires_write,
        explicit_team,
        understanding.requires_external_facts,
    );
    let mut scopes = plans
        .iter()
        .flat_map(|plan| &plan.slots)
        .flat_map(|slot| &slot.capability_cropped_refs)
        .cloned()
        .collect::<Vec<_>>();
    scopes.sort();
    scopes.dedup();
    let authorized_focuses = plans
        .iter()
        .flat_map(|plan| {
            plan.slots.iter().map(|slot| SemanticFocus {
                focus_id: slot.focus_id.clone(),
                role_id: plan.role_id.clone(),
                objective: slot.boundary.clone(),
                resource_scopes: slot.capability_cropped_refs.clone(),
                evidence_responsibilities: vec![slot.evidence_responsibility.clone()],
            })
        })
        .collect::<Vec<_>>();
    for node in &mut proposal.nodes {
        if matches!(
            node.recipe,
            CapabilityRecipeId::Agent
                | CapabilityRecipeId::Team
                | CapabilityRecipeId::Review
                | CapabilityRecipeId::Synthesis
        ) {
            node.resource_scopes = scopes.clone();
        }
        if node.recipe == CapabilityRecipeId::Team {
            // Team partitions are an authority-bearing contract. Preserve the
            // model's semantic request at the node level, but always replace
            // role/focus resource assignments with Runtime-derived partitions.
            node.focuses.clone_from(&authorized_focuses);
        } else if !node.focuses.is_empty() && !authorized_focuses.is_empty() {
            // Model-defined Agent focus text remains useful, but each instance
            // receives one bounded Runtime-derived scope instead of the union.
            for (index, focus) in node.focuses.iter_mut().enumerate() {
                focus.resource_scopes.clone_from(
                    &authorized_focuses[index % authorized_focuses.len()].resource_scopes,
                );
            }
        }
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
    if external_research && !requires_write {
        return external_research_focus_partition_plans(requested_count);
    }
    let scopes = if forced_scopes.is_empty() {
        bounded_workspace_focus_scopes(
            workspace_root,
            objective,
            if requires_write { 1 } else { requested_count },
            requires_write,
            explicit_team,
        )
    } else {
        forced_scopes.to_vec()
    };
    if scopes.is_empty() {
        return Vec::new();
    }
    if requires_write {
        vec![
            write_focus_partition_plan(objective, scopes.clone()),
            support_focus_partition_plan(
                "reviewer",
                "bounded-review",
                "Review implementation evidence across the bounded Team scopes without expanding authority",
                scopes,
            ),
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
    let slots = FOCUSES
        .iter()
        .take(requested_count.clamp(2, FOCUSES.len()))
        .map(|(focus_id, boundary)| {
            let scopes = vec!["network:*".to_string()];
            FocusPartitionSlot {
                focus_id: (*focus_id).to_string(),
                scope_hash: harness_contract::team::focus_scope_hash(
                    "researcher",
                    boundary,
                    &scopes,
                ),
                boundary: (*boundary).to_string(),
                evidence_responsibility:
                    "Return source-attributed findings, publication dates, conflicts, and unresolved uncertainty"
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
            ],
            slots,
        },
        support_focus_partition_plan(
            "synthesizer",
            "external-synthesis",
            "Reconcile only committed researcher evidence; preserve dates, conflicts, and gaps",
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
    explicit_team: bool,
) -> Vec<String> {
    let mut candidates = workspace_focus_candidates(workspace_root)
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
    let broad = explicit_team
        || [
            "architecture",
            "codebase",
            "workspace",
            "repository",
            "system",
            "review",
            "audit",
            "架构",
            "代码",
            "项目",
            "系统",
            "全盘",
            "审查",
            "审计",
        ]
        .iter()
        .any(|marker| normalized.contains(marker));
    let required = if requires_write {
        requested_count.clamp(1, 6)
    } else {
        requested_count.clamp(2, 6)
    };
    let mut selected = candidates
        .iter()
        .filter(|(score, _)| *score > 0)
        .map(|(_, path)| path.clone())
        .take(required)
        .collect::<Vec<_>>();
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
    if selected.len() < if requires_write { 1 } else { 2 } {
        return Vec::new();
    }
    let access = if requires_write { "write" } else { "read" };
    selected
        .into_iter()
        .map(|path| format!("{access}:{path}"))
        .collect()
}

fn workspace_focus_candidates(workspace_root: &Path) -> Vec<String> {
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
        ("mfg", &["crates/app-mfg", "crates/app-mfg-contract"][..]),
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
    use crate::orchestration::{
        GraphMutationProposal, GraphSemanticNode, RuntimeOrchestrationConstraints,
        RuntimeOrchestrationOperation,
    };
    use harness_contract::execution_graph::ExecutionCompletionContract;

    #[test]
    fn runtime_replaces_model_team_scopes_with_disjoint_authoritative_partitions() {
        let workspace = tempfile::tempdir().expect("workspace");
        for relative in ["crates/runtime", "crates/gateway", "surfaces/webui"] {
            std::fs::create_dir_all(workspace.path().join(relative)).expect("workspace partition");
        }
        let mut request = RuntimeOrchestrationRequest {
            intent: "必须启动 Team 审查 runtime gateway webui 架构".to_string(),
            model_lease: None,
            session_id: Some("session-1".to_string()),
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
                        },
                        SemanticFocus {
                            focus_id: "model-b".to_string(),
                            role_id: "researcher".to_string(),
                            objective: "model scope b".to_string(),
                            resource_scopes: vec!["write:../../outside".to_string()],
                            evidence_responsibilities: Vec::new(),
                        },
                    ],
                    template: None,
                    input_refs: Vec::new(),
                    output_artifacts: vec!["terminal_synthesis".to_string()],
                    evidence_contract: vec!["summary".to_string()],
                    required_evidence_refs: Vec::new(),
                    resource_scopes: vec!["write:../../outside".to_string()],
                    required: true,
                    dependency: Default::default(),
                    cancellation_group: None,
                }],
                completion: ExecutionCompletionContract::default(),
                reason: "independent review".to_string(),
            }),
            control: None,
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
            .all(|scope| scope.starts_with("read:") && !scope.contains("..")));
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
}
