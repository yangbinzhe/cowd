//! AI-authored Team template candidates.
//!
//! The model drafts a structured team template; this module compiles it into
//! a validated `TeamTemplateManifest`, clips every role's grant ceiling to the
//! caller's permission ceiling, produces an audit preview, and publishes it as
//! an immutable User-scope revision. Display names never participate in
//! behavior; permission and acceptance contracts are the only execution facts.

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionLifecycle, RevisionSelector,
};
use harness_contract::policy::PermissionMode;
use harness_contract::team::definition::{RoleDisplayName, TeamTemplateDisplay};
use harness_contract::team::{
    RoleCardinalityPolicy, RolePartitionPolicy, TeamEvaluationContract, TeamResultContract,
    TeamRoleDefinition, TeamRoleDependency, TeamRoleTaskContract, TeamTemplateDefinitionId,
    TeamTemplateManifest, TeamTopologyContract,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::RuntimeDefinitionRegistry;

const AI_TEMPLATE_PROTOCOL: &str = "ai-authored@1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamTemplateProposal {
    pub template_id: String,
    pub name: String,
    #[serde(default)]
    pub team_display_name: Option<String>,
    #[serde(default)]
    pub role_display_names: Vec<RoleDisplayName>,
    pub roles: Vec<ProposedRole>,
    #[serde(default)]
    pub dependencies: Vec<ProposedDependency>,
    #[serde(default)]
    pub result_fields: Vec<String>,
    #[serde(default)]
    pub evidence_required: bool,
    pub instructions: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposedRole {
    pub role_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    pub responsibility: String,
    pub agent_definition_ref: String,
    #[serde(default)]
    pub grant_ceiling: Vec<String>,
    #[serde(default)]
    pub fixed_count: Option<u32>,
    #[serde(default)]
    pub min_count: Option<u32>,
    #[serde(default)]
    pub max_count: Option<u32>,
    #[serde(default)]
    pub acceptance: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposedDependency {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone)]
pub struct TemplateCandidate {
    pub manifest: TeamTemplateManifest,
    pub digest: String,
    pub preview: serde_json::Value,
}

/// Normalizes common model-authoring shortcuts in a `template_proposal` JSON
/// value so the strict contract only sees canonical shapes:
/// - `roles` may be an object keyed by role_id (value = role fields) instead
///   of an array; the role_id is injected when missing.
/// - `role_display_names` may be an object keyed by role_id instead of an
///   array of {role_id, display_name}.
pub(crate) fn normalize_template_proposal(value: &mut serde_json::Value) {
    if value.get("instructions").is_none() {
        value["instructions"] =
            serde_json::json!("# 协作研讨\n\n分工调研、对抗质询并收敛为统一结论。\n");
    }
    if let Some(serde_json::Value::String(field)) = value.get("result_fields").cloned() {
        value["result_fields"] = serde_json::json!([field]);
    }
    if let Some(serde_json::Value::Object(roles)) = value.get_mut("roles") {
        let roles: serde_json::Map<String, serde_json::Value> = std::mem::take(roles);
        let normalized = roles
            .into_iter()
            .map(|(role_id, mut role): (String, serde_json::Value)| {
                if role.is_object() && role.get("role_id").is_none() {
                    role["role_id"] = serde_json::json!(role_id);
                }
                normalize_proposed_role(&mut role);
                role
            })
            .collect::<Vec<_>>();
        value["roles"] = serde_json::json!(normalized);
    } else if let Some(serde_json::Value::Array(roles)) = value.get_mut("roles") {
        for role in roles.iter_mut() {
            normalize_proposed_role(role);
        }
    }
    if let Some(serde_json::Value::Object(displays)) = value.get_mut("role_display_names") {
        let displays: serde_json::Map<String, serde_json::Value> = std::mem::take(displays);
        let normalized = displays
            .into_iter()
            .map(|(role_id, display_name): (String, serde_json::Value)| {
                serde_json::json!({ "role_id": role_id, "display_name": display_name })
            })
            .collect::<Vec<_>>();
        value["role_display_names"] = serde_json::json!(normalized);
    }
}

fn normalize_proposed_role(role: &mut serde_json::Value) {
    let Some(fields) = role.as_object_mut() else {
        return;
    };
    if let Some(serde_json::Value::Object(reference)) = fields.get("agent_definition_ref").cloned()
    {
        let definition = reference
            .get("definition")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .or_else(|| reference.keys().next().cloned());
        let revision = reference
            .get("revision")
            .and_then(serde_json::Value::as_u64)
            .or_else(|| {
                reference
                    .values()
                    .next()
                    .and_then(serde_json::Value::as_u64)
            })
            .unwrap_or(1);
        fields.insert(
            "agent_definition_ref".to_string(),
            serde_json::json!(format!("{}@{}", definition.unwrap_or_default(), revision)),
        );
    }
    if let Some(serde_json::Value::Object(ceiling)) = fields.get("grant_ceiling").cloned() {
        let normalized = ceiling
            .into_iter()
            .filter(|(_, enabled)| enabled.as_bool().unwrap_or(true))
            .map(|(capability, _)| serde_json::json!(capability))
            .collect::<Vec<_>>();
        fields.insert("grant_ceiling".to_string(), serde_json::json!(normalized));
    }
    if let Some(serde_json::Value::String(acceptance)) = fields.get("acceptance").cloned() {
        fields.insert("acceptance".to_string(), serde_json::json!([acceptance]));
    }
}

fn capability_from_name(name: &str) -> Option<AgentCapability> {
    match name.to_ascii_lowercase().as_str() {
        "read" => Some(AgentCapability::Read),
        "search" => Some(AgentCapability::Search),
        "write" => Some(AgentCapability::Write),
        "test" => Some(AgentCapability::Test),
        "network" => Some(AgentCapability::Network),
        _ => None,
    }
}

fn ceiling_allows(ceiling: PermissionMode, capability: AgentCapability) -> bool {
    match capability {
        AgentCapability::Read | AgentCapability::Search => true,
        AgentCapability::Write | AgentCapability::Test => {
            ceiling.permits(PermissionMode::WorkspaceWrite)
                || ceiling.permits(PermissionMode::DangerFullAccess)
        }
        AgentCapability::Network => ceiling.permits(PermissionMode::DangerFullAccess),
        _ => false,
    }
}

fn cardinality(role: &ProposedRole) -> Result<RoleCardinalityPolicy, String> {
    if let Some(count) = role.fixed_count {
        if count == 0 {
            return Err(format!("role `{}` fixed_count must be > 0", role.role_id));
        }
        let count = u16::try_from(count)
            .map_err(|_| format!("role `{}` fixed_count exceeds u16", role.role_id))?;
        return Ok(RoleCardinalityPolicy::Fixed { count });
    }
    let min = u16::try_from(role.min_count.unwrap_or(1))
        .map_err(|_| format!("role `{}` min_count exceeds u16", role.role_id))?;
    let max = u16::try_from(role.max_count.unwrap_or(1))
        .map_err(|_| format!("role `{}` max_count exceeds u16", role.role_id))?
        .max(min);
    if min == 0 || max == 0 {
        return Err(format!(
            "role `{}` cardinality must be positive",
            role.role_id
        ));
    }
    if min == max {
        return Ok(RoleCardinalityPolicy::Fixed { count: min });
    }
    Ok(RoleCardinalityPolicy::Adaptive {
        min,
        target: min.max(1),
        max,
    })
}

fn parse_agent_ref(value: &str) -> Result<(AgentDefinitionId, u64), String> {
    let (path, revision) = match value.split_once('@') {
        Some((path, revision)) => (
            path,
            revision
                .parse::<u64>()
                .map_err(|_| format!("agent_definition_ref `{value}` has an invalid revision"))?,
        ),
        None => (value, 1),
    };
    let (scope, local_id) = match path.split_once('/') {
        Some(("builtin", local)) => (DefinitionScope::Builtin, local),
        Some(("user", local)) => (DefinitionScope::User, local),
        Some(("workspace", local)) => (DefinitionScope::Workspace, local),
        _ => {
            return Err(format!(
                "agent_definition_ref `{value}` must be builtin/<id> or user/<id>"
            ))
        }
    };
    AgentDefinitionId::new(scope, local_id)
        .map(|definition_id| (definition_id, revision))
        .map_err(|error| format!("invalid agent_definition_ref `{value}`: {error}"))
}

pub struct TemplateCandidateCompiler;

impl TemplateCandidateCompiler {
    pub fn compile(
        registry: &RuntimeDefinitionRegistry,
        proposal: &TeamTemplateProposal,
        ceiling: PermissionMode,
    ) -> Result<TemplateCandidate, String> {
        let template_id = TeamTemplateDefinitionId::new(
            DefinitionScope::Workspace,
            proposal
                .template_id
                .trim()
                .strip_prefix("cowd/")
                .unwrap_or(proposal.template_id.trim()),
        )
        .map_err(|error| format!("invalid template_id: {error}"))?;
        let mut role_ids = std::collections::BTreeSet::new();
        let mut roles = Vec::with_capacity(proposal.roles.len());
        let mut clipped_capabilities = Vec::new();
        for role in &proposal.roles {
            if role.role_id.trim().is_empty() {
                return Err("every proposed role needs a non-empty role_id".to_string());
            }
            if !role_ids.insert(role.role_id.as_str()) {
                return Err(format!("duplicate role_id `{}`", role.role_id));
            }
            let (definition_id, revision) = parse_agent_ref(&role.agent_definition_ref)?;
            // The Definition must exist in the registry; AI cannot invent one.
            registry
                .resolve_agent(
                    &definition_id,
                    RevisionSelector::ExactApprovedRevision { revision },
                )
                .map_err(|error| {
                    format!(
                        "role `{}` references unknown Agent Definition `{}`: {error}",
                        role.role_id, role.agent_definition_ref
                    )
                })?;
            let mut grant_ceiling = Vec::new();
            for name in &role.grant_ceiling {
                let capability = capability_from_name(name).ok_or_else(|| {
                    format!(
                        "role `{}` uses unknown capability `{name}` (read|search|write|test|network)",
                        role.role_id
                    )
                })?;
                if !ceiling_allows(ceiling, capability) {
                    // Bounded auto-repair: clip the over-ceiling capability and
                    // record it in the preview so the audit trail shows the
                    // exact compensation applied.
                    clipped_capabilities.push(format!("{}:{}", role.role_id, name));
                    continue;
                }
                grant_ceiling.push(capability);
            }
            if grant_ceiling.is_empty() {
                grant_ceiling.push(AgentCapability::Read);
            }
            grant_ceiling.sort_by_key(|capability| format!("{capability:?}"));
            grant_ceiling.dedup();
            let cardinality = cardinality(role)?;
            let partition = if cardinality.max() == 1 {
                RolePartitionPolicy::Single
            } else {
                RolePartitionPolicy::ByFocus {
                    partition_key: role.role_id.clone(),
                }
            };
            roles.push(TeamRoleDefinition {
                role_id: role.role_id.clone(),
                display_name: role.display_name.clone(),
                responsibility: role.responsibility.clone(),
                agent_definition_id: definition_id,
                agent_selector: RevisionSelector::ExactApprovedRevision { revision },
                cardinality,
                partition,
                grant_ceiling,
                task_contract: TeamRoleTaskContract {
                    contract_ref: format!("ai/team-role/{}@1", role.role_id),
                    acceptance: if role.acceptance.is_empty() {
                        vec!["evidence".to_string()]
                    } else {
                        role.acceptance.clone()
                    },
                },
            });
        }
        let dependencies = proposal
            .dependencies
            .iter()
            .map(|dependency| TeamRoleDependency {
                from_role_id: dependency.from.clone(),
                to_role_id: dependency.to.clone(),
            })
            .collect::<Vec<_>>();
        let result_fields = if proposal.result_fields.is_empty() {
            vec!["summary".to_string(), "evidence".to_string()]
        } else {
            proposal.result_fields.clone()
        };
        let manifest = TeamTemplateManifest {
            api_version: "cowd.team/v1".to_string(),
            template_id,
            revision: 1,
            name: proposal.name.clone(),
            display: Some(TeamTemplateDisplay {
                team_display_name: proposal.team_display_name.clone(),
                role_display_names: proposal.role_display_names.clone(),
            }),
            lifecycle: RevisionLifecycle::Draft,
            topology: TeamTopologyContract {
                protocol_ref: AI_TEMPLATE_PROTOCOL.to_string(),
                require_synthesis: true,
                require_review: dependencies.iter().any(|dependency| {
                    dependency.to_role_id.contains("review")
                        || dependency.to_role_id.contains("critic")
                }),
            },
            roles,
            dependencies,
            result_contract: TeamResultContract {
                required_fields: result_fields.clone(),
                evidence_required: proposal.evidence_required
                    || result_fields.contains(&"evidence".to_string()),
                synthesis_required: true,
            },
            evaluation: TeamEvaluationContract::single_release_gate(
                format!(
                    "ai/{}@1",
                    proposal
                        .template_id
                        .trim()
                        .strip_prefix("cowd/")
                        .unwrap_or(proposal.template_id.trim())
                ),
                "team_interoperability",
            ),
            instructions_digest: format!("{:x}", Sha256::digest(proposal.instructions.as_bytes())),
        };
        manifest
            .validate()
            .map_err(|error| format!("proposed template is invalid: {error}"))?;
        let digest = format!(
            "{:x}",
            Sha256::digest(serde_json::to_string(&manifest).map_err(|error| error.to_string())?)
        );
        let preview = json!({
            "template_id": manifest.template_id.as_str(),
            "revision": manifest.revision,
            "name": manifest.name,
            "team_display_name": manifest.display.as_ref().and_then(|display| display.team_display_name.clone()),
            "digest": digest,
            "roles": manifest.roles.iter().map(|role| json!({
                "role_id": role.role_id,
                "display_name": role.display_name,
                "responsibility": role.responsibility,
                "grant_ceiling": role.grant_ceiling.iter().map(|capability| format!("{capability:?}").to_ascii_lowercase()).collect::<Vec<_>>(),
                "cardinality": format!("{:?}", role.cardinality),
                "acceptance": role.task_contract.acceptance,
            })).collect::<Vec<_>>(),
            "dependencies": manifest.dependencies.iter().map(|dependency| json!({
                "from": dependency.from_role_id,
                "to": dependency.to_role_id,
            })).collect::<Vec<_>>(),
            "result_fields": manifest.result_contract.required_fields,
            "clipped_capabilities": clipped_capabilities,
            "risk_notes": {
                "requires_write": manifest.roles.iter().any(|role| role.grant_ceiling.contains(&AgentCapability::Write)),
                "requires_network": manifest.roles.iter().any(|role| role.grant_ceiling.contains(&AgentCapability::Network)),
            },
        });
        Ok(TemplateCandidate {
            manifest,
            digest,
            preview,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        RuntimeDefinitionRegistry, RuntimeEventInput, RuntimeEventScope, RuntimeServices,
        SubmitGlobalApprovalRequest,
    };
    use harness_contract::agent::AgentExecutorPolicy;
    use harness_contract::agent::{
        AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionManifest,
        AgentEvaluationContract, AgentModelPolicy, AgentOutputContract, CognitiveReadScope,
        CognitiveWriteMode, ReleaseAssignment, ReleaseAssignmentStatus, ReleaseAuthorization,
        ReleaseChannel,
    };
    use harness_contract::core::TaskRisk;
    use harness_contract::policy::{
        ApprovalContext, ApprovalDecisionActor, ApprovalDecisionActorKind, ApprovalDecisionCommand,
        ApprovalDomain, ApprovalGrantScope, ApprovalSource, ApprovalSourceKind,
        ApprovalTimeoutPolicy,
    };

    fn digest(value: &str) -> String {
        use sha2::{Digest, Sha256};
        format!("{:x}", Sha256::digest(value.as_bytes()))
    }

    fn registry() -> (tempfile::TempDir, RuntimeDefinitionRegistry) {
        let temporary = tempfile::TempDir::new().expect("temporary root");
        let storage =
            storage::StorageLayout::default_for_config_home(temporary.path().join("user"));
        let registry = RuntimeDefinitionRegistry::from_storage_layout(
            &storage,
            temporary.path().join("bundle/definitions"),
            temporary.path().join("workspace"),
        )
        .expect("registry");
        (temporary, registry)
    }

    fn publish_agent(registry: &RuntimeDefinitionRegistry, local_id: &str) {
        let instructions = format!("# {local_id}\n\nBounded agent.\n");
        let definition_id =
            AgentDefinitionId::new(DefinitionScope::Workspace, local_id).expect("definition id");
        let stored = registry
            .agents()
            .store_revision(
                AgentDefinitionManifest {
                    api_version: "cowd.agent/v1".to_string(),
                    definition_id: definition_id.clone(),
                    revision: 1,
                    name: local_id.to_string(),
                    description: "Bounded agent".to_string(),
                    lifecycle: RevisionLifecycle::Published,
                    executor: AgentExecutorPolicy::CowdNative,
                    model_policy: AgentModelPolicy {
                        profile: "coding".to_string(),
                        allowed_models: vec!["test-model".to_string()],
                        fallback_allowed: true,
                    },
                    cognitive_policy: AgentCognitivePolicy {
                        context_profile: "team".to_string(),
                        read_scopes: vec![CognitiveReadScope::Session],
                        write_mode: CognitiveWriteMode::CandidateOnly,
                        team_working_state_visible: true,
                    },
                    capability_contract: AgentCapabilityContract {
                        capability_ceiling: vec![AgentCapability::Read],
                        skill_refs: vec![],
                        approval_required_for: vec![],
                    },
                    output_contract: AgentOutputContract::reviewable(),
                    evaluation: AgentEvaluationContract::single_release_gate(local_id, "evidence"),
                    instructions_digest: digest(&instructions),
                },
                &instructions,
            )
            .expect("stored agent");
        registry
            .agents()
            .record_release_assignment(&ReleaseAssignment {
                scope: DefinitionScope::Workspace,
                revision_ref: stored.revision.revision_ref.clone(),
                channel: ReleaseChannel::Stable,
                status: ReleaseAssignmentStatus::Active,
                authorization: ReleaseAuthorization::HumanApproval {
                    approval_ref: format!("approval/{local_id}-v1"),
                },
                content_digest: stored.revision.content_digest,
            })
            .expect("agent release");
    }

    fn business_tech_proposal() -> TeamTemplateProposal {
        TeamTemplateProposal {
            template_id: "cowd/business-tech-deliberation".to_string(),
            name: "业务/技术双团队研讨".to_string(),
            team_display_name: Some("业务技术研讨".to_string()),
            role_display_names: vec![
                RoleDisplayName {
                    role_id: "business_expert".to_string(),
                    display_name: "供应链专家".to_string(),
                },
                RoleDisplayName {
                    role_id: "cto".to_string(),
                    display_name: "CTO".to_string(),
                },
            ],
            roles: vec![
                ProposedRole {
                    role_id: "business_expert".to_string(),
                    display_name: Some("供应链专家".to_string()),
                    responsibility: "分析供应制造与订单履行约束".to_string(),
                    agent_definition_ref: "workspace/cowd/explore@1".to_string(),
                    grant_ceiling: vec!["read".to_string(), "search".to_string()],
                    fixed_count: Some(2),
                    min_count: None,
                    max_count: None,
                    acceptance: vec!["findings".to_string(), "evidence".to_string()],
                },
                ProposedRole {
                    role_id: "cto".to_string(),
                    display_name: Some("CTO".to_string()),
                    responsibility: "裁定技术方案并汇总".to_string(),
                    agent_definition_ref: "workspace/cowd/direct@1".to_string(),
                    grant_ceiling: vec!["read".to_string()],
                    fixed_count: Some(1),
                    min_count: None,
                    max_count: None,
                    acceptance: vec!["summary".to_string(), "evidence".to_string()],
                },
            ],
            dependencies: vec![ProposedDependency {
                from: "business_expert".to_string(),
                to: "cto".to_string(),
            }],
            result_fields: vec!["summary".to_string(), "evidence".to_string()],
            evidence_required: true,
            instructions: "# 民主集中式研讨\n\n业务专家先产出证据，CTO 汇总并裁决。\n".to_string(),
        }
    }

    #[test]
    fn compiles_and_clips_a_business_tech_template() {
        let (_temp, registry) = registry();
        publish_agent(&registry, "cowd/explore");
        publish_agent(&registry, "cowd/direct");
        let candidate = TemplateCandidateCompiler::compile(
            &registry,
            &business_tech_proposal(),
            PermissionMode::ReadOnly,
        )
        .expect("candidate");
        assert_eq!(candidate.manifest.roles.len(), 2);
        assert_eq!(
            candidate
                .manifest
                .display
                .as_ref()
                .unwrap()
                .team_display_name
                .as_deref(),
            Some("业务技术研讨")
        );
        assert_eq!(
            candidate.manifest.roles[0].display_name.as_deref(),
            Some("供应链专家")
        );
        assert!(candidate
            .manifest
            .roles
            .iter()
            .all(|role| !role.grant_ceiling.contains(&AgentCapability::Write)));
        assert_eq!(candidate.preview["digest"], candidate.digest);
        assert!(candidate.manifest.validate().is_ok());
    }

    #[test]
    fn clips_over_ceiling_grants_and_rejects_unknown_definitions() {
        let (_temp, registry) = registry();
        publish_agent(&registry, "cowd/explore");
        publish_agent(&registry, "cowd/direct");
        let mut proposal = business_tech_proposal();
        proposal.roles[0].agent_definition_ref =
            "workspace/cowd/not-a-real-definition@1".to_string();
        assert!(
            TemplateCandidateCompiler::compile(&registry, &proposal, PermissionMode::ReadOnly)
                .is_err()
        );
        proposal = business_tech_proposal();
        proposal.roles[0].grant_ceiling = vec!["write".to_string()];
        let candidate =
            TemplateCandidateCompiler::compile(&registry, &proposal, PermissionMode::ReadOnly)
                .expect("over-ceiling grant is clipped, not rejected");
        assert!(candidate
            .manifest
            .roles
            .iter()
            .all(|role| !role.grant_ceiling.contains(&AgentCapability::Write)));
        assert!(candidate
            .preview
            .get("clipped_capabilities")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|clipped| clipped.len() == 1));
    }

    #[test]
    fn publishes_to_the_user_template_catalog() {
        let (_temp, registry) = registry();
        publish_agent(&registry, "cowd/explore");
        publish_agent(&registry, "cowd/direct");
        let candidate = TemplateCandidateCompiler::compile(
            &registry,
            &business_tech_proposal(),
            PermissionMode::ReadOnly,
        )
        .expect("candidate");
        let stored = registry
            .teams()
            .store_revision(candidate.manifest, &business_tech_proposal().instructions)
            .expect("publish");
        let reloaded = registry
            .teams()
            .read_revision(&stored.revision.revision_ref)
            .expect("reload");
        assert_eq!(reloaded.revision.manifest.name, "业务/技术双团队研讨");
        assert_eq!(
            reloaded
                .revision
                .manifest
                .display
                .as_ref()
                .unwrap()
                .team_display_name
                .as_deref(),
            Some("业务技术研讨")
        );
    }

    #[test]
    fn approval_gated_publish_roundtrip() {
        let (_temp, registry) = registry();
        publish_agent(&registry, "cowd/explore");
        publish_agent(&registry, "cowd/direct");
        let services = RuntimeServices::in_memory().expect("services");
        let candidate = TemplateCandidateCompiler::compile(
            &registry,
            &business_tech_proposal(),
            PermissionMode::ReadOnly,
        )
        .expect("candidate");
        let approval_id = "template-approval:test-roundtrip";
        services
            .event_store()
            .append(RuntimeEventInput {
                stream_id: format!("definition-template-candidate:{approval_id}"),
                scope: RuntimeEventScope::Mission,
                kind: "definition.template.candidate.v1".to_string(),
                status: Some("pending_approval".to_string()),
                actor: None,
                refs: vec![],
                payload: serde_json::json!({
                    "approval_id": approval_id,
                    "manifest": candidate.manifest,
                    "instructions": business_tech_proposal().instructions,
                    "digest": candidate.digest,
                    "preview": candidate.preview,
                }),
            })
            .expect("candidate event");
        assert!(services
            .publish_approved_template_candidate(approval_id)
            .is_err());
        let context = ApprovalContext {
            principal_id: "session:s".to_string(),
            profile_id: "template-publish".to_string(),
            approval_profile: None,
            workspace_key: "w".to_string(),
            session_id: Some("s".to_string()),
            turn_id: None,
            task_id: None,
            capability: "definition.template.publish".to_string(),
            invocation_id: None,
            execution_id: None,
            strategy_decision_ref: None,
            source_surface: None,
            resource_targets: vec![],
            effect: None,
            explicit_ask: true,
            effective_sandbox_posture: None,
            policy_revision: 0,
            requested_sandbox_posture: None,
        };
        let source = ApprovalSource {
            kind: ApprovalSourceKind::Session,
            session_id: Some("s".to_string()),
            agent_id: None,
            team_id: None,
            mission_id: None,
            resource_ref: None,
            review_ref: None,
            application: None,
        };
        services
            .approval_queue()
            .submit_scoped(
                approval_id,
                SubmitGlobalApprovalRequest {
                    source,
                    context,
                    action: "definition.template.publish".to_string(),
                    summary: "publish test template".to_string(),
                    risk: TaskRisk::Low,
                    domain: ApprovalDomain::System,
                    blocks_execution: false,
                    evidence_refs: vec![],
                    timeout_policy: ApprovalTimeoutPolicy::Pending,
                },
            )
            .expect("submit");
        services
            .approval_queue()
            .decide_internal(ApprovalDecisionCommand {
                approval_id: approval_id.to_string(),
                approved: true,
                skip: false,
                reason: "test".to_string(),
                scope: ApprovalGrantScope::Once,
                actor: ApprovalDecisionActor {
                    kind: ApprovalDecisionActorKind::Policy,
                    actor_id: "test".to_string(),
                },
                evidence_refs: vec![],
            })
            .expect("decide");
        let published = services
            .publish_approved_template_candidate(approval_id)
            .expect("publish after approval");
        assert!(published.get("content_digest").is_some());
        let stored = services
            .definition_registry()
            .teams()
            .read_revision(&candidate.manifest.revision_ref())
            .expect("reload published template");
        assert_eq!(
            stored
                .revision
                .manifest
                .display
                .as_ref()
                .unwrap()
                .team_display_name
                .as_deref(),
            Some("业务技术研讨")
        );
    }

    #[test]
    fn normalize_template_proposal_accepts_map_shaped_roles() {
        let mut value = serde_json::json!({
            "template_id": "cowd/test",
            "name": "测试",
            "roles": {
                "business_expert": {
                    "responsibility": "业务分析",
                    "agent_definition_ref": {
                        "definition": "workspace/cowd/explore",
                        "revision": 1
                    },
                    "grant_ceiling": {"read": true, "write": false},
                    "acceptance": "findings"
                },
                "cto": {
                    "responsibility": "技术裁决",
                    "agent_definition_ref": {"workspace/cowd/direct": 1}
                }
            },
            "role_display_names": {
                "business_expert": "供应链专家",
                "cto": "CTO"
            },
            "instructions": "# 测试\n"
        });
        normalize_template_proposal(&mut value);
        let roles = value["roles"].as_array().expect("roles array");
        assert_eq!(roles.len(), 2);
        assert!(roles.iter().any(|role| {
            role["role_id"] == "business_expert"
                && role["responsibility"] == "业务分析"
                && role["agent_definition_ref"] == "workspace/cowd/explore@1"
                && role["grant_ceiling"] == serde_json::json!(["read"])
                && role["acceptance"] == serde_json::json!(["findings"])
        }));
        assert!(roles.iter().any(|role| role["role_id"] == "cto"
            && role["agent_definition_ref"] == "workspace/cowd/direct@1"));
        let displays = value["role_display_names"]
            .as_array()
            .expect("display names array");
        assert!(displays.iter().any(|item| {
            item["role_id"] == "business_expert" && item["display_name"] == "供应链专家"
        }));
        let (_temp, registry) = registry();
        publish_agent(&registry, "cowd/explore");
        publish_agent(&registry, "cowd/direct");
        let proposal: TeamTemplateProposal =
            serde_json::from_value(value).expect("normalized proposal parses");
        assert_eq!(proposal.roles.len(), 2);
    }
}
