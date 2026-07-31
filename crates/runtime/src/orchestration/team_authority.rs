//! Runtime-owned Team resource authority.
//!
//! Callers may request collaboration and suggest a published template, but
//! only Runtime derives filesystem, network, and session evidence leases.

use std::path::{Component, Path};

use harness_contract::team::{FocusPartitionPlan, FocusPartitionSlot};

use crate::execution_core::RuntimeExecutionDecision;

use super::{RuntimeOrchestrationAction, RuntimeOrchestrationRequest};

pub(crate) fn bind_team_resource_authority(
    request: &mut RuntimeOrchestrationRequest,
    leased_decision: Option<&RuntimeExecutionDecision>,
    workspace_root: &Path,
) {
    if request.action != RuntimeOrchestrationAction::RequestTeam {
        return;
    }

    // A provider may suggest a narrow scope, but resource authority is never
    // accepted from model JSON. Preserve the suggestion only long enough for
    // Runtime to validate and crop it against the active workspace.
    let proposed_resource_scopes = request
        .capabilities
        .iter()
        .filter_map(|capability| capability.strip_prefix("resource:"))
        .map(str::to_string)
        .collect::<Vec<_>>();
    request
        .capabilities
        .retain(|capability| !capability.starts_with("resource:"));

    let inferred = harness_contract::strategy::decide_strategy(
        &harness_contract::strategy::StrategyInput::from_prompt(&request.intent),
    );
    let understanding = leased_decision
        .map(|decision| &decision.strategy.understanding)
        .unwrap_or(&inferred.understanding);
    // `requires_write` is authority, not a model preference. The admitted
    // Runtime decision is the only source allowed to grant mutation scope.
    // Overwrite the provider field so validation and compilation consume the
    // same authoritative fact.
    let requires_write = understanding.requires_write;
    request.constraints.requires_write = Some(requires_write);
    let external_research = understanding.requires_external_facts;
    let explicit_team = understanding.requests_multi_agent
        || request.selection_mode == Some(harness_contract::team::TeamSelectionMode::Explicit);
    let requested_count = request
        .constraints
        .max_parallel_agents
        .unwrap_or_else(|| usize::from(understanding.independent_workstreams.max(2)))
        .clamp(2, 6);

    if external_research {
        request.template_hint = Some("cowd/external-research-synthesis".to_string());
    }
    let mut proposed_scopes = request
        .focus_partition_plans
        .iter()
        .flat_map(|plan| &plan.slots)
        .flat_map(|slot| &slot.capability_cropped_refs)
        .map(String::as_str)
        .chain(proposed_resource_scopes.iter().map(String::as_str))
        .filter_map(|scope| {
            recrop_proposed_scope(scope, workspace_root, requires_write, external_research)
        })
        .collect::<Vec<_>>();
    proposed_scopes.sort();
    proposed_scopes.dedup();
    request.focus_partition_plans = derive_team_focus_partition_plans(
        &request.intent,
        workspace_root,
        &proposed_scopes,
        requested_count,
        requires_write,
        explicit_team,
        external_research,
    );

    request.capabilities.extend(
        request
            .focus_partition_plans
            .iter()
            .flat_map(|plan| &plan.slots)
            .flat_map(|slot| &slot.capability_cropped_refs)
            .map(|scope| format!("resource:{scope}")),
    );
    request.capabilities.sort();
    request.capabilities.dedup();
}

fn recrop_proposed_scope(
    scope: &str,
    workspace_root: &Path,
    requires_write: bool,
    external_research: bool,
) -> Option<String> {
    if scope == "network:*" {
        return external_research.then(|| scope.to_string());
    }
    if external_research {
        return None;
    }
    let (access, relative) = scope.split_once(':')?;
    if access != "read" && access != "write" {
        return None;
    }
    if requires_write != (access == "write") {
        return None;
    }
    let relative = Path::new(relative);
    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return None;
    }
    workspace_root
        .join(relative)
        .exists()
        .then(|| format!("{access}:{}", relative.to_string_lossy().replace('\\', "/")))
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

    #[test]
    fn external_research_uses_network_leases_without_workspace_guessing() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(root.path().join("apps/mfg")).expect("fixture");
        let plans = derive_team_focus_partition_plans(
            "Research current WAIC developments",
            root.path(),
            &[],
            3,
            false,
            true,
            true,
        );
        let scopes = plans
            .iter()
            .flat_map(|plan| &plan.slots)
            .flat_map(|slot| &slot.capability_cropped_refs)
            .collect::<Vec<_>>();
        assert!(!scopes.is_empty());
        assert!(scopes.iter().all(|scope| scope.as_str() == "network:*"));
    }

    #[test]
    fn model_supplied_resource_scopes_are_replaced_by_runtime_authority() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(root.path().join("apps/mfg")).expect("fixture");
        let mut request: RuntimeOrchestrationRequest = serde_json::from_value(serde_json::json!({
            "intent": "Research the latest current WAIC developments using a team",
            "action": "request_team",
            "selection_mode": "explicit",
            "capabilities": [
                "resource:write:.",
                "resource:read:apps/mfg",
                "tool:WebSearch"
            ],
            "focus_partition_plans": [{
                "role_id": "researcher",
                "shared_baseline": [],
                "slots": [{
                    "focus_id": "wrong-local-focus",
                    "scope_hash": "model-supplied",
                    "boundary": "inspect apps/mfg",
                    "evidence_responsibility": "local files",
                    "capability_cropped_refs": ["read:apps/mfg"],
                    "overlap_budget_bp": 0,
                    "novelty_target_bp": 0,
                    "output_contract": ["findings"],
                    "output_acceptance": ["evidence_scope:apps/mfg"]
                }]
            }],
            "constraints": {"max_parallel_agents": 3, "requires_write": false}
        }))
        .expect("request");

        bind_team_resource_authority(&mut request, None, root.path());

        assert_eq!(
            request.template_hint.as_deref(),
            Some("cowd/external-research-synthesis")
        );
        assert!(request.capabilities.contains(&"tool:WebSearch".to_string()));
        assert!(!request
            .capabilities
            .iter()
            .any(|capability| capability == "resource:write:."));
        assert!(request
            .capabilities
            .iter()
            .any(|capability| capability == "resource:network:*"));
        assert!(request
            .focus_partition_plans
            .iter()
            .flat_map(|plan| &plan.slots)
            .flat_map(|slot| &slot.capability_cropped_refs)
            .all(|scope| scope == "network:*"));
        assert!(request
            .focus_partition_plans
            .iter()
            .flat_map(|plan| &plan.slots)
            .flat_map(|slot| &slot.output_acceptance)
            .all(|criterion| !criterion.contains("apps/mfg")));
    }

    #[test]
    fn model_cannot_promote_research_request_to_workspace_write() {
        let root = tempfile::tempdir().expect("workspace");
        let objective = "Form a team to research current WAIC developments and synthesize evidence";
        let decision = crate::execution_core::build_runtime_execution_decision(objective, None);
        assert!(!decision.strategy.understanding.requires_write);
        assert!(decision.strategy.understanding.requires_external_facts);
        let mut request: RuntimeOrchestrationRequest = serde_json::from_value(serde_json::json!({
            "intent": objective,
            "action": "request_team",
            "capabilities": ["WebSearch", "WebFetch", "write_file"],
            "constraints": {"max_parallel_agents": 3, "requires_write": true}
        }))
        .expect("request");

        bind_team_resource_authority(&mut request, Some(&decision), root.path());

        assert_eq!(request.constraints.requires_write, Some(false));
        assert_eq!(
            request.template_hint.as_deref(),
            Some("cowd/external-research-synthesis")
        );
        assert!(request
            .capabilities
            .iter()
            .any(|capability| capability == "resource:network:*"));
        assert!(!request
            .capabilities
            .iter()
            .any(|capability| capability.starts_with("resource:write:")));
    }

    #[test]
    fn local_webui_review_keeps_runtime_cropped_workspace_leases() {
        let root = tempfile::tempdir().expect("workspace");
        for relative in ["crates/runtime", "crates/gateway", "surfaces/webui"] {
            std::fs::create_dir_all(root.path().join(relative)).expect("fixture");
        }
        let objective = "这是复杂架构审查，必须实际启动一个多 Agent 协作团队，分别审视 crates/runtime、crates/gateway、surfaces/webui 的策略事件接线、权限边界和用户可见状态，再交叉验证并综合证据。";
        let decision = crate::execution_core::build_runtime_execution_decision(objective, None);
        assert!(
            !decision.strategy.understanding.requires_external_facts,
            "{:?}",
            decision.strategy.understanding
        );
        let mut request: RuntimeOrchestrationRequest = serde_json::from_value(serde_json::json!({
            "intent": objective,
            "action": "request_team",
            "selection_mode": "automatic",
            "capabilities": [
                "resource:read:crates/runtime",
                "resource:read:crates/gateway",
                "resource:read:surfaces/webui"
            ],
            "constraints": {"max_parallel_agents": 3, "requires_write": false}
        }))
        .expect("request");

        bind_team_resource_authority(&mut request, Some(&decision), root.path());

        assert_eq!(
            request
                .capabilities
                .iter()
                .filter(|capability| capability.starts_with("resource:"))
                .cloned()
                .collect::<Vec<_>>(),
            vec![
                "resource:read:crates/gateway".to_string(),
                "resource:read:crates/runtime".to_string(),
                "resource:read:surfaces/webui".to_string(),
            ]
        );
        assert_eq!(request.focus_partition_plans[0].role_id, "researcher");
    }
}
