//! Embedded release-owned builtin Team Template bootstrap.

use std::collections::BTreeMap;

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, ReleaseAssignmentStatus,
    ReleaseAuthorization, ReleaseChannel, RevisionLifecycle, RevisionSelector,
};
use harness_contract::team::definition::{RoleDisplayName, TeamTemplateDisplay};
use harness_contract::team::{
    RoleBehaviorFacet, RoleCardinalityPolicy, RolePartitionPolicy, TeamResultContract,
    TeamRoleDefinition, TeamRoleDependency, TeamRoleTaskContract, TeamTemplateDefinitionId,
    TeamTemplateManifest, TeamTemplateRevisionRef, TeamTopologyContract,
};
use sha2::{Digest, Sha256};

use super::store::{
    TeamDefaultPointer, TeamDefinitionStoreError, TeamTemplateDefinitionStore,
    TeamTemplateStorageLayout,
};
use super::TeamReleaseAssignment;

const RELEASE_ATTESTATION: &str = "embedded-release/cowd-runtime-v1";

fn builtin_role_display_name(role_id: &str) -> &'static str {
    match role_id {
        "executor" => "执行智能体",
        "planner" => "规划智能体",
        "verifier" => "验证智能体",
        "researcher" => "研究智能体",
        "synthesizer" => "汇总智能体",
        "implementer" => "实现智能体",
        "reviewer" => "审查智能体",
        "fixer" => "修复智能体",
        "proposer" => "提案智能体",
        "critic" => "批评智能体",
        "arbiter" => "仲裁智能体",
        "investigator" => "调查智能体",
        "responder" => "响应智能体",
        "commander" => "指挥智能体",
        "scenario" => "场景智能体",
        "comparator" => "对比智能体",
        "workstream" => "工作流智能体",
        "coordinator" => "协调智能体",
        _ => "协作智能体",
    }
}

fn builtin_team_display_name(name: &str) -> &'static str {
    match name {
        "Execute and Review" => "执行与评审",
        "Direct Executor" => "直接执行",
        "Planner Executor Verifier" => "计划执行验证",
        "Parallel Research Synthesis" => "并行研究汇总",
        "External Research Synthesis" => "外部研究汇总",
        "Implementation Review Fix" => "实现评审修复",
        "Debate Critic Arbiter" => "辩论裁决",
        "Incident Response" => "事件响应",
        "Matrix Scenario Ensemble" => "矩阵场景集成",
        "Long-Running Workstreams" => "长任务工作流",
        _ => "自定义团队",
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BuiltinTeamTrust {
    digests: BTreeMap<String, String>,
}

impl BuiltinTeamTrust {
    pub(crate) fn verify(
        &self,
        revision_ref: &TeamTemplateRevisionRef,
        content_digest: &str,
    ) -> Result<(), TeamDefinitionStoreError> {
        let key = revision_key(revision_ref);
        let expected = self.digests.get(&key).ok_or_else(|| {
            TeamDefinitionStoreError::UnresolvablePointer(
                revision_ref.template_id.clone(),
                "builtin team template is not part of the verified release bundle".to_string(),
            )
        })?;
        if expected == content_digest {
            Ok(())
        } else {
            Err(TeamDefinitionStoreError::DigestMismatch {
                subject: format!("builtin team release digest for {key}"),
                expected: expected.clone(),
                actual: content_digest.to_string(),
            })
        }
    }
}

pub(crate) fn bootstrap_builtin_teams<L>(
    store: &TeamTemplateDefinitionStore<L>,
) -> Result<BuiltinTeamTrust, TeamDefinitionStoreError>
where
    L: TeamTemplateStorageLayout,
{
    let instructions = "# Execute and Review\n\nAn implementer produces evidence-backed work; an independent reviewer verifies it before synthesis.\n";
    let execute = AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/execute")
        .map_err(TeamDefinitionStoreError::Contract)?;
    let direct = AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/direct")
        .map_err(TeamDefinitionStoreError::Contract)?;
    let template_id =
        TeamTemplateDefinitionId::new(DefinitionScope::Builtin, "cowd/execute-review")
            .map_err(TeamDefinitionStoreError::Contract)?;
    let manifest = TeamTemplateManifest {
        api_version: "cowd.team/v1".to_string(),
        template_id,
        // The V550 execute/review acceptance and evidence contract changed.
        // Builtin revisions are immutable on disk, so publish a new revision
        // instead of colliding with installations that already stored v1.
        revision: 3,
        name: "Execute and Review".to_string(),
        display: Some(TeamTemplateDisplay {
            team_display_name: Some(builtin_team_display_name("Execute and Review").to_string()),
            role_display_names: vec![
                RoleDisplayName {
                    role_id: "implementer".to_string(),
                    display_name: builtin_role_display_name("implementer").to_string(),
                },
                RoleDisplayName {
                    role_id: "reviewer".to_string(),
                    display_name: builtin_role_display_name("reviewer").to_string(),
                },
            ],
        }),
        lifecycle: RevisionLifecycle::Published,
        topology: TeamTopologyContract {
            protocol_ref: "review_fix@1".to_string(),
            require_synthesis: true,
            require_review: true,
        },
        role_aliases: std::collections::BTreeMap::new(),
        roles: vec![
            TeamRoleDefinition {
                role_id: "implementer".to_string(),
                display_name: None,
                responsibility: "Plan, implement, and provide source-level verification evidence"
                    .to_string(),
                agent_definition_id: execute.clone(),
                agent_selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
                cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
                partition: RolePartitionPolicy::Single,
                behavior: vec![RoleBehaviorFacet::ReacquireEvidence { required: true }],
                grant_ceiling: vec![
                    AgentCapability::Read,
                    AgentCapability::Search,
                    AgentCapability::Write,
                ],
                task_contract: TeamRoleTaskContract {
                    contract_ref: "builtin/execute@1".to_string(),
                    acceptance: vec![
                        "implementation".to_string(),
                        "source_verification".to_string(),
                    ],
                },
            },
            TeamRoleDefinition {
                role_id: "reviewer".to_string(),
                display_name: None,
                responsibility: "Review the implementation evidence and identify remaining risk"
                    .to_string(),
                agent_definition_id: direct.clone(),
                agent_selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
                cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
                partition: RolePartitionPolicy::Single,
                behavior: vec![
                    RoleBehaviorFacet::Verification {
                        mode: "independent".to_string(),
                    },
                    RoleBehaviorFacet::UpstreamConsumption { required: true },
                    RoleBehaviorFacet::ReacquireEvidence { required: true },
                    RoleBehaviorFacet::TerminalCandidate { required: true },
                ],
                grant_ceiling: vec![AgentCapability::Read],
                task_contract: TeamRoleTaskContract {
                    contract_ref: "builtin/review@1".to_string(),
                    acceptance: vec![
                        "review".to_string(),
                        "evidence".to_string(),
                        "risks".to_string(),
                    ],
                },
            },
        ],
        dependencies: vec![TeamRoleDependency {
            from_role_id: "implementer".to_string(),
            to_role_id: "reviewer".to_string(),
        }],
        result_contract: TeamResultContract {
            required_fields: vec![
                "summary".to_string(),
                "implementation".to_string(),
                "source_verification".to_string(),
                "review".to_string(),
                "evidence".to_string(),
                "risks".to_string(),
            ],
            evidence_required: true,
            synthesis_required: true,
        },
        evaluation: harness_contract::team::TeamEvaluationContract::single_release_gate(
            "builtin/cowd/execute-review/team-baseline",
            "team_interoperability",
        ),
        instructions_digest: format!("{:x}", Sha256::digest(instructions.as_bytes())),
    };
    let stored = store.store_revision(manifest, instructions)?;
    store.record_release_assignment(&TeamReleaseAssignment {
        scope: DefinitionScope::Builtin,
        revision_ref: stored.revision.revision_ref.clone(),
        channel: ReleaseChannel::Stable,
        status: ReleaseAssignmentStatus::Active,
        authorization: ReleaseAuthorization::ReleaseAuthorityAttestation {
            attestation_ref: RELEASE_ATTESTATION.to_string(),
        },
        content_digest: stored.revision.content_digest.clone(),
    })?;
    store.set_default_pointer(&TeamDefaultPointer::latest(
        DefinitionScope::Builtin,
        stored.revision.revision_ref.template_id.clone(),
        ReleaseAuthorization::ReleaseAuthorityAttestation {
            attestation_ref: RELEASE_ATTESTATION.to_string(),
        },
    ))?;
    let mut trust = BuiltinTeamTrust::default();
    trust.digests.insert(
        revision_key(&stored.revision.revision_ref),
        stored.revision.content_digest,
    );
    for (manifest, instructions) in additional_builtin_team_manifests(&execute, &direct)? {
        let stored = store.store_revision(manifest, instructions)?;
        store.record_release_assignment(&TeamReleaseAssignment {
            scope: DefinitionScope::Builtin,
            revision_ref: stored.revision.revision_ref.clone(),
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::ReleaseAuthorityAttestation {
                attestation_ref: RELEASE_ATTESTATION.to_string(),
            },
            content_digest: stored.revision.content_digest.clone(),
        })?;
        store.set_default_pointer(&TeamDefaultPointer::latest(
            DefinitionScope::Builtin,
            stored.revision.revision_ref.template_id.clone(),
            ReleaseAuthorization::ReleaseAuthorityAttestation {
                attestation_ref: RELEASE_ATTESTATION.to_string(),
            },
        ))?;
        trust.digests.insert(
            revision_key(&stored.revision.revision_ref),
            stored.revision.content_digest,
        );
    }
    Ok(trust)
}

fn additional_builtin_team_manifests(
    execute: &AgentDefinitionId,
    direct: &AgentDefinitionId,
) -> Result<Vec<(TeamTemplateManifest, &'static str)>, TeamDefinitionStoreError> {
    let explore = AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/explore")
        .map_err(TeamDefinitionStoreError::Contract)?;
    let fixed = RoleCardinalityPolicy::Fixed { count: 1 };
    let parallel = RoleCardinalityPolicy::Adaptive {
        min: 2,
        target: 4,
        max: 8,
    };
    let single = RolePartitionPolicy::Single;
    let focused = RolePartitionPolicy::ByFocus {
        partition_key: "investigation".to_string(),
    };
    let result = |fields: &[&str]| TeamResultContract {
        required_fields: fields.iter().map(|field| (*field).to_string()).collect(),
        evidence_required: fields.contains(&"evidence"),
        synthesis_required: fields.contains(&"summary"),
    };
    // These are published Template facts, not Runtime inference rules.  Each
    // builtin call below chooses one explicitly; custom templates must supply
    // their own typed behavior in the proposal contract.
    let evidence_producer = || vec![RoleBehaviorFacet::ReacquireEvidence { required: true }];
    let upstream_worker = || {
        vec![
            RoleBehaviorFacet::UpstreamConsumption { required: true },
            RoleBehaviorFacet::ReacquireEvidence { required: true },
        ]
    };
    let independent_verifier = || {
        vec![
            RoleBehaviorFacet::Verification {
                mode: "independent".to_string(),
            },
            RoleBehaviorFacet::UpstreamConsumption { required: true },
            RoleBehaviorFacet::ReacquireEvidence { required: true },
            RoleBehaviorFacet::TerminalCandidate { required: true },
        ]
    };
    let terminal_worker = || {
        vec![
            RoleBehaviorFacet::UpstreamConsumption { required: true },
            RoleBehaviorFacet::ReacquireEvidence { required: true },
            RoleBehaviorFacet::TerminalCandidate { required: true },
        ]
    };
    let terminal_reducer = || {
        vec![
            RoleBehaviorFacet::Reducer {
                mode: "finally".to_string(),
            },
            RoleBehaviorFacet::UpstreamConsumption { required: true },
            RoleBehaviorFacet::ReacquireEvidence { required: false },
            RoleBehaviorFacet::TerminalCandidate { required: true },
        ]
    };
    let direct_terminal = || {
        vec![
            RoleBehaviorFacet::ReacquireEvidence { required: true },
            RoleBehaviorFacet::TerminalCandidate { required: true },
        ]
    };
    let role_revision =
        |role_id: &str,
         responsibility: &str,
         agent: AgentDefinitionId,
         agent_revision: u64,
         grant_ceiling: Vec<AgentCapability>,
         cardinality: RoleCardinalityPolicy,
         partition: RolePartitionPolicy,
         acceptance: &[&str],
         behavior: Vec<RoleBehaviorFacet>| TeamRoleDefinition {
            role_id: role_id.to_string(),
            display_name: None,
            responsibility: responsibility.to_string(),
            agent_definition_id: agent,
            agent_selector: RevisionSelector::ExactApprovedRevision {
                revision: agent_revision,
            },
            cardinality,
            partition,
            behavior,
            grant_ceiling,
            task_contract: TeamRoleTaskContract {
                contract_ref: format!("builtin/team-role/{role_id}@1"),
                acceptance: acceptance
                    .iter()
                    .map(|value| (*value).to_string())
                    .collect(),
            },
        };
    let role = |role_id: &str,
                responsibility: &str,
                agent: AgentDefinitionId,
                grant_ceiling: Vec<AgentCapability>,
                cardinality: RoleCardinalityPolicy,
                partition: RolePartitionPolicy,
                acceptance: &[&str],
                behavior: Vec<RoleBehaviorFacet>| {
        role_revision(
            role_id,
            responsibility,
            agent,
            1,
            grant_ceiling,
            cardinality,
            partition,
            acceptance,
            behavior,
        )
    };
    let template = |local_id: &str,
                    name: &str,
                    protocol_ref: &str,
                    roles: Vec<TeamRoleDefinition>,
                    dependencies: Vec<TeamRoleDependency>,
                    result_contract: TeamResultContract,
                    instructions: &'static str|
     -> Result<(TeamTemplateManifest, &'static str), TeamDefinitionStoreError> {
        let template_id = TeamTemplateDefinitionId::new(DefinitionScope::Builtin, local_id)
            .map_err(TeamDefinitionStoreError::contract)?;
        Ok((
            TeamTemplateManifest {
                api_version: "cowd.team/v1".to_string(),
                template_id,
                revision: 2,
                name: name.to_string(),
                display: Some(TeamTemplateDisplay {
                    team_display_name: Some(builtin_team_display_name(name).to_string()),
                    role_display_names: roles
                        .iter()
                        .map(|role| RoleDisplayName {
                            role_id: role.role_id.clone(),
                            display_name: builtin_role_display_name(&role.role_id).to_string(),
                        })
                        .collect(),
                }),
                lifecycle: RevisionLifecycle::Published,
                topology: TeamTopologyContract {
                    protocol_ref: protocol_ref.to_string(),
                    require_synthesis: result_contract.synthesis_required,
                    require_review: roles.iter().any(|role| {
                        role.behavior
                            .iter()
                            .any(|facet| matches!(facet, RoleBehaviorFacet::Verification { .. }))
                    }),
                },
                role_aliases: std::collections::BTreeMap::new(),
                roles,
                dependencies,
                result_contract,
                evaluation: harness_contract::team::TeamEvaluationContract::single_release_gate(
                    format!("builtin/{local_id}/team-baseline"),
                    "team_interoperability",
                ),
                instructions_digest: format!("{:x}", Sha256::digest(instructions.as_bytes())),
            },
            instructions,
        ))
    };

    Ok(vec![
        template(
            "cowd/direct-executor",
            "Direct Executor",
            "direct@1",
            vec![role(
                "executor",
                "Resolve one bounded objective directly and report explicit uncertainty.",
                direct.clone(),
                vec![AgentCapability::Read],
                fixed.clone(),
                single.clone(),
                &["summary", "evidence"],
                direct_terminal(),
            )],
            Vec::new(),
            result(&["summary", "evidence"]),
            "# Direct Executor\n\nUse one bounded Agent to resolve a concise objective with evidence.\n",
        )?,
        template(
            "cowd/planner-executor-verifier",
            "Planner Executor Verifier",
            "review_fix@1",
            vec![
                role("planner", "Establish an evidence-backed plan.", explore.clone(), vec![AgentCapability::Read, AgentCapability::Search], fixed.clone(), single.clone(), &["plan", "evidence"], evidence_producer()),
                role("executor", "Execute the approved bounded plan.", execute.clone(), vec![AgentCapability::Read, AgentCapability::Search, AgentCapability::Write, AgentCapability::Test], fixed.clone(), single.clone(), &["implementation", "evidence"], upstream_worker()),
                role("verifier", "Verify outcomes and remaining risks.", direct.clone(), vec![AgentCapability::Read], fixed.clone(), single.clone(), &["summary", "evidence", "risks"], independent_verifier()),
            ],
            vec![
                TeamRoleDependency { from_role_id: "planner".to_string(), to_role_id: "executor".to_string() },
                TeamRoleDependency { from_role_id: "executor".to_string(), to_role_id: "verifier".to_string() },
            ],
            result(&["summary", "evidence", "risks"]),
            "# Planner Executor Verifier\n\nPlan first, execute within granted permissions, then independently verify.\n",
        )?,
        template(
            "cowd/parallel-research-synthesis",
            "Parallel Research Synthesis",
            "jps@1",
            vec![
                role("researcher", "Investigate a non-overlapping focus partition with evidence.", explore.clone(), vec![AgentCapability::Read, AgentCapability::Search], parallel.clone(), focused.clone(), &["findings", "evidence"], evidence_producer()),
                role("synthesizer", "Reconcile research findings into a grounded synthesis.", direct.clone(), vec![AgentCapability::Read], fixed.clone(), single.clone(), &["summary", "evidence", "unresolved"], terminal_reducer()),
            ],
            vec![TeamRoleDependency { from_role_id: "researcher".to_string(), to_role_id: "synthesizer".to_string() }],
            result(&["summary", "evidence", "unresolved"]),
            "# Parallel Research Synthesis\n\nParallel researchers cover distinct focuses; synthesis preserves conflicts and gaps.\n",
        )?,
        template(
            "cowd/external-research-synthesis",
            "External Research Synthesis",
            "jps@1",
            vec![
                role_revision(
                    "researcher",
                    "Investigate a non-overlapping external evidence focus using only Runtime-leased network access.",
                    explore.clone(),
                    2,
                    vec![
                        AgentCapability::Read,
                        AgentCapability::Search,
                        AgentCapability::Network,
                    ],
                    parallel.clone(),
                    focused.clone(),
                    &["findings", "evidence", "unresolved"],
                    evidence_producer(),
                ),
                role(
                    "synthesizer",
                    "Reconcile dated, source-attributed external findings without repeating acquisition.",
                    direct.clone(),
                    vec![AgentCapability::Read],
                    fixed.clone(),
                    single.clone(),
                    &["summary", "evidence", "unresolved"],
                    terminal_reducer(),
                ),
            ],
            vec![TeamRoleDependency {
                from_role_id: "researcher".to_string(),
                to_role_id: "synthesizer".to_string(),
            }],
            result(&["summary", "evidence", "unresolved"]),
            "# External Research Synthesis\n\nParallel researchers acquire current, source-attributed external evidence through Runtime-owned network leases; synthesis preserves dates, conflicts, and gaps.\n",
        )?,
        template(
            "cowd/implementation-review-fix",
            "Implementation Review Fix",
            "review_fix@1",
            vec![
                role("implementer", "Implement the bounded change and provide verification evidence.", execute.clone(), vec![AgentCapability::Read, AgentCapability::Search, AgentCapability::Write, AgentCapability::Test], fixed.clone(), single.clone(), &["implementation", "evidence"], evidence_producer()),
                role("reviewer", "Independently review implementation evidence and identify defects.", direct.clone(), vec![AgentCapability::Read], fixed.clone(), single.clone(), &["review", "evidence", "risks"], vec![RoleBehaviorFacet::Verification { mode: "independent".to_string() }, RoleBehaviorFacet::UpstreamConsumption { required: true }, RoleBehaviorFacet::ReacquireEvidence { required: true }]),
                role("fixer", "Address accepted review findings and report residual risk.", execute.clone(), vec![AgentCapability::Read, AgentCapability::Search, AgentCapability::Write, AgentCapability::Test], fixed.clone(), single.clone(), &["summary", "evidence", "risks"], terminal_worker()),
            ],
            vec![
                TeamRoleDependency { from_role_id: "implementer".to_string(), to_role_id: "reviewer".to_string() },
                TeamRoleDependency { from_role_id: "reviewer".to_string(), to_role_id: "fixer".to_string() },
            ],
            result(&["summary", "evidence", "risks"]),
            "# Implementation Review Fix\n\nSeparate implementation, review, and remediation responsibilities.\n",
        )?,
        template(
            "cowd/debate-critic-arbiter",
            "Debate Critic Arbiter",
            "debate@1",
            vec![
                role("proposer", "Develop an evidence-backed proposal for one focus.", explore.clone(), vec![AgentCapability::Read, AgentCapability::Search], parallel.clone(), focused.clone(), &["proposal", "evidence"], evidence_producer()),
                role("critic", "Challenge proposals for missing evidence and counterexamples.", explore.clone(), vec![AgentCapability::Read, AgentCapability::Search], parallel.clone(), focused.clone(), &["critique", "evidence", "risks"], vec![RoleBehaviorFacet::Verification { mode: "adversarial".to_string() }, RoleBehaviorFacet::UpstreamConsumption { required: true }, RoleBehaviorFacet::ReacquireEvidence { required: true }]),
                role("arbiter", "Resolve conflicts while preserving unresolved uncertainty.", direct.clone(), vec![AgentCapability::Read], fixed.clone(), single.clone(), &["summary", "evidence", "unresolved"], terminal_reducer()),
            ],
            vec![
                TeamRoleDependency { from_role_id: "proposer".to_string(), to_role_id: "critic".to_string() },
                TeamRoleDependency { from_role_id: "critic".to_string(), to_role_id: "arbiter".to_string() },
            ],
            result(&["summary", "evidence", "unresolved"]),
            "# Debate Critic Arbiter\n\nUse explicit proposal, critique, and arbitration roles.\n",
        )?,
        template(
            "cowd/incident-response",
            "Incident Response",
            "incident@1",
            vec![
                role("investigator", "Establish the incident evidence and scope.", explore.clone(), vec![AgentCapability::Read, AgentCapability::Search], parallel.clone(), focused.clone(), &["findings", "evidence"], evidence_producer()),
                role("responder", "Apply a bounded mitigation plan when permissions allow.", execute.clone(), vec![AgentCapability::Read, AgentCapability::Search, AgentCapability::Write, AgentCapability::Test], fixed.clone(), single.clone(), &["mitigation", "evidence"], upstream_worker()),
                role("commander", "Synthesize status, decisions, and unresolved risk.", direct.clone(), vec![AgentCapability::Read], fixed.clone(), single.clone(), &["summary", "evidence", "risks"], terminal_reducer()),
            ],
            vec![
                TeamRoleDependency { from_role_id: "investigator".to_string(), to_role_id: "responder".to_string() },
                TeamRoleDependency { from_role_id: "responder".to_string(), to_role_id: "commander".to_string() },
            ],
            result(&["summary", "evidence", "risks"]),
            "# Incident Response\n\nInvestigate, mitigate, and communicate through separate bounded roles.\n",
        )?,
        template(
            "cowd/matrix-scenario-ensemble",
            "Matrix Scenario Ensemble",
            "matrix_scenario@1",
            vec![
                role("scenario", "Evaluate one explicit scenario assumption set against leased Matrix snapshots.", explore.clone(), vec![AgentCapability::Read, AgentCapability::Search], parallel.clone(), focused.clone(), &["findings", "evidence"], evidence_producer()),
                role("comparator", "Compare simulation candidates without treating them as observed facts.", direct.clone(), vec![AgentCapability::Read], fixed.clone(), single.clone(), &["summary", "evidence", "unresolved"], terminal_reducer()),
            ],
            vec![TeamRoleDependency { from_role_id: "scenario".to_string(), to_role_id: "comparator".to_string() }],
            result(&["summary", "evidence", "unresolved"]),
            "# Matrix Scenario Ensemble\n\nEvaluate alternative simulated scenarios and preserve their simulated boundary.\n",
        )?,
        template(
            "cowd/long-running-workstreams",
            "Long-Running Workstreams",
            "workstreams@1",
            vec![
                role(
                    "workstream",
                    "Advance one non-overlapping durable workstream and checkpoint its evidence.",
                    execute.clone(),
                    vec![
                        AgentCapability::Read,
                        AgentCapability::Search,
                        AgentCapability::Write,
                        AgentCapability::Test,
                    ],
                    parallel,
                    focused,
                    &["checkpoint", "evidence", "unresolved"],
                    evidence_producer(),
                ),
                role(
                    "coordinator",
                    "Synthesize completed checkpoints, dependencies, and remaining blockers.",
                    direct.clone(),
                    vec![AgentCapability::Read],
                    fixed,
                    single,
                    &["summary", "evidence", "unresolved"],
                    terminal_reducer(),
                ),
            ],
            vec![TeamRoleDependency {
                from_role_id: "workstream".to_string(),
                to_role_id: "coordinator".to_string(),
            }],
            result(&["summary", "evidence", "unresolved"]),
            "# Long-Running Workstreams\n\nRun durable workstreams with bounded checkpoints and a coordinating synthesis.\n",
        )?,
    ])
}

fn revision_key(revision_ref: &TeamTemplateRevisionRef) -> String {
    format!(
        "{}@{}",
        revision_ref.template_id.as_str(),
        revision_ref.revision
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_research_template_binds_network_capability_to_explore_v2() {
        let execute = AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/execute").unwrap();
        let direct = AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/direct").unwrap();
        let templates = additional_builtin_team_manifests(&execute, &direct).unwrap();
        let manifest = templates
            .iter()
            .map(|(manifest, _)| manifest)
            .find(|manifest| {
                manifest.template_id.as_str() == "builtin/cowd/external-research-synthesis"
            })
            .expect("external research template");
        let researcher = manifest
            .roles
            .iter()
            .find(|role| role.role_id == "researcher")
            .expect("researcher");

        assert_eq!(
            researcher.agent_selector,
            RevisionSelector::ExactApprovedRevision { revision: 2 }
        );
        assert!(researcher.grant_ceiling.contains(&AgentCapability::Network));
    }

    #[test]
    fn role_behavior_is_typed_by_role_id_not_display_responsibility() {
        let agent = AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/execute").unwrap();
        let role = |responsibility: &str| TeamRoleDefinition {
            role_id: "implementer".to_string(),
            display_name: None,
            responsibility: responsibility.to_string(),
            agent_definition_id: agent.clone(),
            agent_selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
            cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
            partition: RolePartitionPolicy::Single,
            behavior: vec![RoleBehaviorFacet::ReacquireEvidence { required: true }],
            grant_ceiling: vec![
                AgentCapability::Read,
                AgentCapability::Write,
                AgentCapability::Test,
            ],
            task_contract: TeamRoleTaskContract {
                contract_ref: "builtin/team-role/implementer@1".to_string(),
                acceptance: vec!["implementation".to_string(), "evidence".to_string()],
            },
        };
        let english = role("Implement the bounded change and provide verification evidence.");
        let chinese = role("实现有界变更并提供验证证据。");

        assert_eq!(english.role_id, chinese.role_id);
        assert_eq!(english.agent_selector, chinese.agent_selector);
        assert_eq!(english.cardinality, chinese.cardinality);
        assert_eq!(english.partition, chinese.partition);
        assert_eq!(english.behavior, chinese.behavior);
        assert_eq!(english.grant_ceiling, chinese.grant_ceiling);
        assert_eq!(english.task_contract, chinese.task_contract);
        assert_ne!(english.responsibility, chinese.responsibility);
        assert!(
            !chinese.task_contract.contract_ref.contains("实现"),
            "behavior references must never be derived from display responsibility"
        );
    }
}
