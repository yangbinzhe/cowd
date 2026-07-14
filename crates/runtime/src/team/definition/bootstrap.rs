//! Embedded release-owned builtin Team Template bootstrap.

use std::collections::BTreeMap;

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, ReleaseAssignmentStatus,
    ReleaseAuthorization, ReleaseChannel, RevisionLifecycle, RevisionSelector,
};
use harness_contract::team::{
    RoleCardinalityPolicy, RolePartitionPolicy, TeamResultContract, TeamRoleDefinition,
    TeamRoleDependency, TeamRoleTaskContract, TeamTemplateDefinitionId, TeamTemplateManifest,
    TeamTemplateRevisionRef, TeamTopologyContract,
};
use sha2::{Digest, Sha256};

use super::store::{
    TeamDefaultPointer, TeamDefinitionStoreError, TeamTemplateDefinitionStore,
    TeamTemplateStorageLayout,
};
use super::TeamReleaseAssignment;

const RELEASE_ATTESTATION: &str = "embedded-release/cowd-runtime-v1";

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
        .expect("static builtin id is valid");
    let direct = AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/direct")
        .expect("static builtin id is valid");
    let template_id =
        TeamTemplateDefinitionId::new(DefinitionScope::Builtin, "cowd/execute-review")
            .expect("static builtin template id is valid");
    let manifest = TeamTemplateManifest {
        api_version: "cowd.team/v1".to_string(),
        template_id,
        revision: 1,
        name: "Execute and Review".to_string(),
        lifecycle: RevisionLifecycle::Published,
        topology: TeamTopologyContract {
            protocol_ref: "review_fix@1".to_string(),
            require_synthesis: true,
            require_review: true,
        },
        roles: vec![
            TeamRoleDefinition {
                role_id: "implementer".to_string(),
                responsibility: "Plan, implement, and provide verification evidence".to_string(),
                agent_definition_id: execute.clone(),
                agent_selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
                cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
                partition: RolePartitionPolicy::Single,
                grant_ceiling: vec![
                    AgentCapability::Read,
                    AgentCapability::Search,
                    AgentCapability::Write,
                    AgentCapability::Test,
                ],
                task_contract: TeamRoleTaskContract {
                    contract_ref: "builtin/execute@1".to_string(),
                    acceptance: vec!["implementation".to_string(), "verification".to_string()],
                },
            },
            TeamRoleDefinition {
                role_id: "reviewer".to_string(),
                responsibility: "Review the implementation evidence and identify remaining risk"
                    .to_string(),
                agent_definition_id: direct.clone(),
                agent_selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
                cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
                partition: RolePartitionPolicy::Single,
                grant_ceiling: vec![AgentCapability::Read],
                task_contract: TeamRoleTaskContract {
                    contract_ref: "builtin/review@1".to_string(),
                    acceptance: vec!["review".to_string(), "evidence".to_string()],
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
        .expect("static builtin id is valid");
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
    let role = |role_id: &str,
                responsibility: &str,
                agent: AgentDefinitionId,
                grant_ceiling: Vec<AgentCapability>,
                cardinality: RoleCardinalityPolicy,
                partition: RolePartitionPolicy,
                acceptance: &[&str]| TeamRoleDefinition {
        role_id: role_id.to_string(),
        responsibility: responsibility.to_string(),
        agent_definition_id: agent,
        agent_selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
        cardinality,
        partition,
        grant_ceiling,
        task_contract: TeamRoleTaskContract {
            contract_ref: format!("builtin/team-role/{role_id}@1"),
            acceptance: acceptance
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
        },
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
                revision: 1,
                name: name.to_string(),
                lifecycle: RevisionLifecycle::Published,
                topology: TeamTopologyContract {
                    protocol_ref: protocol_ref.to_string(),
                    require_synthesis: result_contract.synthesis_required,
                    require_review: dependencies.iter().any(|dependency| {
                        dependency.to_role_id.contains("review")
                            || dependency.to_role_id.contains("critic")
                    }),
                },
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
                role("planner", "Establish an evidence-backed plan.", explore.clone(), vec![AgentCapability::Read, AgentCapability::Search], fixed.clone(), single.clone(), &["plan", "evidence"]),
                role("executor", "Execute the approved bounded plan.", execute.clone(), vec![AgentCapability::Read, AgentCapability::Search, AgentCapability::Write, AgentCapability::Test], fixed.clone(), single.clone(), &["implementation", "evidence"]),
                role("verifier", "Verify outcomes and remaining risks.", direct.clone(), vec![AgentCapability::Read], fixed.clone(), single.clone(), &["summary", "evidence", "risks"]),
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
                role("researcher", "Investigate a non-overlapping focus partition with evidence.", explore.clone(), vec![AgentCapability::Read, AgentCapability::Search], parallel.clone(), focused.clone(), &["findings", "evidence"]),
                role("synthesizer", "Reconcile research findings into a grounded synthesis.", direct.clone(), vec![AgentCapability::Read], fixed.clone(), single.clone(), &["summary", "evidence", "unresolved"]),
            ],
            vec![TeamRoleDependency { from_role_id: "researcher".to_string(), to_role_id: "synthesizer".to_string() }],
            result(&["summary", "evidence", "unresolved"]),
            "# Parallel Research Synthesis\n\nParallel researchers cover distinct focuses; synthesis preserves conflicts and gaps.\n",
        )?,
        template(
            "cowd/implementation-review-fix",
            "Implementation Review Fix",
            "review_fix@1",
            vec![
                role("implementer", "Implement the bounded change and provide verification evidence.", execute.clone(), vec![AgentCapability::Read, AgentCapability::Search, AgentCapability::Write, AgentCapability::Test], fixed.clone(), single.clone(), &["implementation", "evidence"]),
                role("reviewer", "Independently review implementation evidence and identify defects.", direct.clone(), vec![AgentCapability::Read], fixed.clone(), single.clone(), &["review", "evidence", "risks"]),
                role("fixer", "Address accepted review findings and report residual risk.", execute.clone(), vec![AgentCapability::Read, AgentCapability::Search, AgentCapability::Write, AgentCapability::Test], fixed.clone(), single.clone(), &["summary", "evidence", "risks"]),
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
                role("proposer", "Develop an evidence-backed proposal for one focus.", explore.clone(), vec![AgentCapability::Read, AgentCapability::Search], parallel.clone(), focused.clone(), &["proposal", "evidence"]),
                role("critic", "Challenge proposals for missing evidence and counterexamples.", explore.clone(), vec![AgentCapability::Read, AgentCapability::Search], parallel.clone(), focused.clone(), &["critique", "evidence", "risks"]),
                role("arbiter", "Resolve conflicts while preserving unresolved uncertainty.", direct.clone(), vec![AgentCapability::Read], fixed.clone(), single.clone(), &["summary", "evidence", "unresolved"]),
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
                role("investigator", "Establish the incident evidence and scope.", explore.clone(), vec![AgentCapability::Read, AgentCapability::Search], parallel.clone(), focused.clone(), &["findings", "evidence"]),
                role("responder", "Apply a bounded mitigation plan when permissions allow.", execute.clone(), vec![AgentCapability::Read, AgentCapability::Search, AgentCapability::Write, AgentCapability::Test], fixed.clone(), single.clone(), &["mitigation", "evidence"]),
                role("commander", "Synthesize status, decisions, and unresolved risk.", direct.clone(), vec![AgentCapability::Read], fixed.clone(), single.clone(), &["summary", "evidence", "risks"]),
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
                role("scenario", "Evaluate one explicit scenario assumption set against leased Matrix snapshots.", explore.clone(), vec![AgentCapability::Read, AgentCapability::Search], parallel.clone(), focused.clone(), &["findings", "evidence"]),
                role("comparator", "Compare simulation candidates without treating them as observed facts.", direct.clone(), vec![AgentCapability::Read], fixed.clone(), single.clone(), &["summary", "evidence", "unresolved"]),
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
                ),
                role(
                    "coordinator",
                    "Synthesize completed checkpoints, dependencies, and remaining blockers.",
                    direct.clone(),
                    vec![AgentCapability::Read],
                    fixed,
                    single,
                    &["summary", "evidence", "unresolved"],
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
