use serde::{Deserialize, Serialize};

use crate::execution_graph::{ExecutionCompletionContract, ExecutionDependencyPolicy};
use crate::input_disposition::ModelInputDispositionBatch;

pub const RUNTIME_ORCHESTRATE_TOOL_ID: &str = "runtime_orchestrate";
/// Narrow root-admission port for a collaboration Program.  Unlike
/// `runtime_orchestrate`, this contract never carries inspection, graph
/// revision, template publication, control, or input-disposition concerns.
/// It is deliberately a semantic decision only; Runtime compiles and owns
/// all physical graph identities, leases, permissions, and receipts.
pub const SUBMIT_COLLABORATION_DECISION_TOOL_ID: &str = "submit_collaboration_decision";

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeOrchestrationOperation {
    #[default]
    Inspect,
    Propose,
    ProposeTemplate,
    Revise,
    Control,
    RouteInput,
}

impl RuntimeOrchestrationOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inspect => "inspect",
            Self::Propose => "propose",
            Self::ProposeTemplate => "propose_template",
            Self::Revise => "revise",
            Self::Control => "control",
            Self::RouteInput => "route_input",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityRecipeId {
    Direct,
    Agent,
    Team,
    Review,
    Synthesis,
    SessionDispatch,
}

impl CapabilityRecipeId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Agent => "agent",
            Self::Team => "team",
            Self::Review => "review",
            Self::Synthesis => "synthesis",
            Self::SessionDispatch => "session_dispatch",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelSemanticFocus {
    pub focus_id: String,
    pub role_id: String,
    pub objective: String,
    #[serde(default)]
    pub evidence_responsibilities: Vec<String>,
}

/// Explicit model-selected policy for the optional managed-Agent escalation
/// lane of a Team. This is required on every semantic node so the model cannot
/// accidentally omit a requested escalation by relying on an optional JSON
/// default. Runtime owns the selected Agent, tool receipt, graph fences and
/// the actual follow-up Team.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ManagedAgentEscalationRequirement {
    #[default]
    None,
    #[schemars(
        description = "Require exactly one Runtime-attested managed-Agent escalation after its first source receipt. Use only for a Team whose work may need an independently discovered follow-up Team."
    )]
    Required,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelGraphSemanticNode {
    pub node_id: String,
    pub recipe: CapabilityRecipeId,
    pub objective: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Number of parallel instances Runtime should materialize. Defaults to
    /// one and remains subject to the Runtime concurrency ceiling.
    #[serde(default = "default_multiplicity")]
    #[schemars(range(min = 1, max = 100))]
    pub multiplicity: u16,
    #[serde(default)]
    pub focuses: Vec<ModelSemanticFocus>,
    /// Explicit Runtime-attested follow-up Team request policy. The model
    /// describes only the semantic need; it cannot select an Agent instance,
    /// construct graph ids, or supply a Program revision.
    pub managed_agent_escalation: ManagedAgentEscalationRequirement,
    #[serde(default)]
    pub template: Option<String>,
    /// Explicit destination for `session_dispatch`. Runtime rejects this field
    /// on every other recipe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_session_id: Option<String>,
    #[serde(default)]
    pub output_artifacts: Vec<String>,
    #[serde(default)]
    pub evidence_contract: Vec<String>,
    #[serde(default)]
    pub required_evidence_refs: Vec<String>,
    /// Whether failure prevents the parent execution from completing.
    /// Defaults to true; false is only for optional, cancellable lanes.
    #[serde(default = "default_required")]
    pub required: bool,
    #[serde(default)]
    pub dependency: ExecutionDependencyPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation_group: Option<String>,
}

const fn default_multiplicity() -> u16 {
    1
}

const fn default_required() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelGraphMutationProposal {
    pub mutation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
    pub nodes: Vec<ModelGraphSemanticNode>,
    #[serde(default)]
    pub completion: ExecutionCompletionContract,
    pub reason: String,
}

/// One model-authored workstream for the narrow collaboration-admission
/// contract.  It has no executor, provider, budget, permission, template
/// publication, or physical-graph fields.  The Coordinator derives those
/// facts from the active turn and immutable policy snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelCollaborationWorkstream {
    /// Portable semantic identity, unique within this decision.
    pub workstream_id: String,
    pub objective: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub focuses: Vec<ModelSemanticFocus>,
    #[serde(default)]
    pub output_artifacts: Vec<String>,
    #[serde(default)]
    pub evidence_contract: Vec<String>,
    #[serde(default)]
    pub managed_agent_escalation: ManagedAgentEscalationRequirement,
}

/// Provider-neutral, typed collaboration decision.  This is the semantic IR
/// shared by native function, native structured-output, and future
/// constrained-language codecs.  A transport receipt is accepted only after
/// conversion to this type and normal Runtime validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelCollaborationControlDecision {
    /// Stable per-ingress decision identity chosen by the model. Runtime
    /// combines it with the authenticated turn fence for idempotency.
    pub decision_id: String,
    pub intent: String,
    pub workstreams: Vec<ModelCollaborationWorkstream>,
    pub reason: String,
}

impl ModelCollaborationControlDecision {
    /// Convert only semantic workstream data into the existing, internal
    /// orchestration command shape.  This keeps one graph compiler while
    /// decoupling the root control transport from the huge legacy tool schema.
    #[must_use]
    pub fn into_runtime_orchestration_input(self) -> ModelRuntimeOrchestrationInput {
        ModelRuntimeOrchestrationInput {
            intent: self.intent,
            operation: RuntimeOrchestrationOperation::Propose,
            inspect_execution_id: None,
            proposal: Some(ModelGraphMutationProposal {
                mutation_id: format!("control-decision:{}", self.decision_id),
                target_execution_id: None,
                expected_revision: None,
                nodes: self
                    .workstreams
                    .into_iter()
                    .map(|workstream| ModelGraphSemanticNode {
                        node_id: workstream.workstream_id,
                        recipe: CapabilityRecipeId::Team,
                        objective: workstream.objective,
                        depends_on: workstream.depends_on,
                        multiplicity: 1,
                        focuses: workstream.focuses,
                        managed_agent_escalation: workstream.managed_agent_escalation,
                        template: None,
                        target_session_id: None,
                        output_artifacts: workstream.output_artifacts,
                        evidence_contract: workstream.evidence_contract,
                        required_evidence_refs: Vec::new(),
                        required: true,
                        dependency: ExecutionDependencyPolicy::All,
                        cancellation_group: None,
                    })
                    .collect(),
                completion: ExecutionCompletionContract::default(),
                reason: self.reason,
            }),
            template_proposal: None,
            control: None,
            input_disposition: None,
            evidence_refs: Vec::new(),
            constraints: ModelRuntimeOrchestrationConstraints::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeControlKind {
    Pause,
    Resume,
    Cancel,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeControlScope {
    Mission,
    #[default]
    Graph,
    Agent,
    Team,
    Subgraph,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelRuntimeControlRequest {
    pub target_execution_id: String,
    pub expected_revision: u64,
    #[serde(default)]
    pub scope: RuntimeControlScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_node_id: Option<String>,
    pub action: RuntimeControlKind,
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelRuntimeOrchestrationConstraints {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_parallel_agents: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requires_write: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surface_latency_sensitive: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ModelGrantCapability {
    Read,
    Search,
    Write,
    Test,
    Network,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModelRoleDisplayName {
    pub role_id: String,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModelProposedRole {
    pub role_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Human-facing role name shown in the UI (e.g. 高级供应制造领域CTO). Display only; it never affects execution."
    )]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Team partition label (business/technical/convergence) used only to resolve group dependencies."
    )]
    pub team: Option<String>,
    pub responsibility: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Exact agent definition id copied from runtime_capabilities(detail=agent_catalog), e.g. builtin/cowd/explore@1. Omit or use null to get a safe builtin default matching the grant_ceiling."
    )]
    pub agent_definition_ref: Option<String>,
    #[serde(default)]
    #[schemars(
        description = "Capability ceiling for this role: read|search|write|test|network. Runtime clips it to the session permission ceiling."
    )]
    pub grant_ceiling: Vec<ModelGrantCapability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub fixed_count: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub min_count: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_count: Option<u32>,
    #[serde(default)]
    #[schemars(
        description = "Acceptance criteria the role must satisfy before its evidence is trusted."
    )]
    pub acceptance: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModelProposedDependency {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum ModelTemplateDependencies {
    /// Explicit role-level edges: [{from: role_id, to: role_id}].
    Pairs(Vec<ModelProposedDependency>),
    /// Array of group membership objects: [{"business_team": ["role_id", ...]}].
    GroupArray(Vec<std::collections::BTreeMap<String, serde_json::Value>>),
    /// Object keyed by group label -> member role ids (or nested group labels).
    Groups(std::collections::BTreeMap<String, serde_json::Value>),
    /// Array of `from->to` / `from:to` strings.
    Strings(Vec<String>),
    /// A single `from->to` / `from:to` string.
    String(String),
}

/// Structured AI-authored Team template proposal for operation=propose_template.
///
/// Every field is declared so the model never has to guess shapes. Runtime
/// still normalizes tolerant variants (map/array roles, string ceilings,
/// wrapped JSON) as a safety net, but the schema below is the source of truth
/// the model is expected to follow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModelTemplateProposal {
    #[schemars(
        description = "Publish-local template id. Scope prefixes (cowd/, workspace/, user/) are accepted and normalized; example: biz-tech-dual-team-deliberation"
    )]
    pub template_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(description = "Team display name shown in the UI.")]
    pub team_display_name: Option<String>,
    #[serde(default)]
    pub role_display_names: Vec<ModelRoleDisplayName>,
    pub roles: Vec<ModelProposedRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependencies: Option<ModelTemplateDependencies>,
    #[serde(default)]
    #[schemars(
        description = "Required result fields of the final synthesis; MUST include summary and evidence."
    )]
    pub result_fields: Vec<String>,
    #[serde(default)]
    pub evidence_required: bool,
    #[schemars(description = "Markdown instructions given to every Team role.")]
    pub instructions: String,
}

/// The complete model-visible contract for `runtime_orchestrate`.
///
/// Authenticated Session identity, model/provider leases, capability grants,
/// resource leases, physical graph IDs, strategy bindings and permission
/// ceilings are deliberately absent. Gateway and Runtime bind them after this
/// contract has been validated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelRuntimeOrchestrationInput {
    pub intent: String,
    #[serde(default)]
    pub operation: RuntimeOrchestrationOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inspect_execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal: Option<ModelGraphMutationProposal>,
    /// Structured AI-authored Team template proposal. With
    /// `operation=propose_template` it requests reusable catalog publication.
    /// With `operation=propose`, it must accompany exactly one Team semantic
    /// node and Runtime compiles an immutable session/turn-bound snapshot for
    /// that graph without publishing it to the shared catalog. The runtime
    /// embeds the typed `ModelTemplateProposal` schema into this property for
    /// model guidance, but accepts tolerant variants (wrapped JSON, map/array
    /// roles, string ceilings) at execution time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_proposal: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control: Option<ModelRuntimeControlRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_disposition: Option<ModelInputDispositionBatch>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub constraints: ModelRuntimeOrchestrationConstraints,
}

impl ModelRuntimeOrchestrationInput {
    #[must_use]
    pub fn minimal_example() -> Self {
        Self {
            intent: "Review the implementation and return verified evidence".to_string(),
            operation: RuntimeOrchestrationOperation::Propose,
            inspect_execution_id: None,
            proposal: Some(ModelGraphMutationProposal {
                mutation_id: "review-v1".to_string(),
                target_execution_id: None,
                expected_revision: None,
                nodes: vec![ModelGraphSemanticNode {
                    node_id: "review".to_string(),
                    recipe: CapabilityRecipeId::Review,
                    objective: "Review the implementation against the stated goal".to_string(),
                    depends_on: Vec::new(),
                    multiplicity: 1,
                    focuses: Vec::new(),
                    managed_agent_escalation: ManagedAgentEscalationRequirement::None,
                    template: None,
                    target_session_id: None,
                    output_artifacts: vec!["review_report".to_string()],
                    evidence_contract: vec!["findings".to_string(), "evidence".to_string()],
                    required_evidence_refs: Vec::new(),
                    required: true,
                    dependency: ExecutionDependencyPolicy::All,
                    cancellation_group: None,
                }],
                completion: ExecutionCompletionContract::default(),
                reason: "The objective requires an independently verified result".to_string(),
            }),
            template_proposal: None,
            control: None,
            input_disposition: None,
            evidence_refs: Vec::new(),
            constraints: ModelRuntimeOrchestrationConstraints::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_contract_excludes_runtime_owned_fields_and_input_refs() {
        let schema = serde_json::to_string(&schemars::schema_for!(ModelRuntimeOrchestrationInput))
            .expect("serialize schema");
        for forbidden in [
            "input_refs",
            "session_id",
            "model_lease",
            "permission_ceiling",
            "selection_mode",
            "strategy_binding",
            "capabilities",
            "resource_scopes",
            "surface",
            "input_id",
            "request_id",
            "turn_id",
            "task_id",
            "mission_id",
            "graph_id",
            "lease",
        ] {
            assert!(
                !schema.contains(&format!("\"{forbidden}\"")),
                "model schema leaked Runtime-owned field `{forbidden}`"
            );
        }
        assert!(schema.contains("\"target_session_id\""));
    }

    #[test]
    fn template_proposal_schema_is_structured_for_the_model() {
        let schema = serde_json::to_string(&schemars::schema_for!(ModelTemplateProposal))
            .expect("serialize schema");
        for needle in [
            "\"template_id\"",
            "\"team_display_name\"",
            "\"role_display_names\"",
            "\"grant_ceiling\"",
            "\"agent_definition_ref\"",
            "\"result_fields\"",
            "\"instructions\"",
            "\"dependencies\"",
            "\"read\"",
            "\"search\"",
            "\"write\"",
            "\"test\"",
            "\"network\"",
        ] {
            assert!(
                schema.contains(needle),
                "template_proposal schema must expose `{needle}` to the model"
            );
        }
    }

    #[test]
    fn narrow_collaboration_decision_converts_without_runtime_owned_fields() {
        let input = ModelCollaborationControlDecision {
            decision_id: "review-v1".to_string(),
            intent: "independent architecture review".to_string(),
            workstreams: vec![ModelCollaborationWorkstream {
                workstream_id: "runtime-review".to_string(),
                objective: "review Runtime durable state".to_string(),
                depends_on: Vec::new(),
                focuses: Vec::new(),
                output_artifacts: vec!["review".to_string()],
                evidence_contract: vec!["evidence".to_string()],
                managed_agent_escalation: ManagedAgentEscalationRequirement::None,
            }],
            reason: "independent evidence is required".to_string(),
        };

        let orchestration = input.into_runtime_orchestration_input();
        assert_eq!(
            orchestration.operation,
            RuntimeOrchestrationOperation::Propose
        );
        let proposal = orchestration.proposal.expect("derived proposal");
        assert_eq!(proposal.mutation_id, "control-decision:review-v1");
        assert_eq!(proposal.nodes[0].recipe, CapabilityRecipeId::Team);
        assert!(proposal.nodes[0].template.is_none());
        assert!(orchestration.template_proposal.is_none());
    }

    #[test]
    fn model_template_proposal_accepts_pairs_groups_and_group_arrays() {
        let base = |dependencies: serde_json::Value| {
            serde_json::json!({
                "template_id": "biz-tech-dual-team-deliberation",
                "name": "业务/技术双团队研讨",
                "team_display_name": "业务-技术双团队研讨组",
                "roles": [{
                    "role_id": "business_expert",
                    "display_name": "供应链专家",
                    "responsibility": "分析供应链约束",
                    "agent_definition_ref": "builtin/cowd/explore@1",
                    "grant_ceiling": ["read", "search"],
                    "acceptance": ["findings", "evidence"]
                }],
                "dependencies": dependencies,
                "result_fields": ["summary", "evidence"],
                "instructions": "# 研讨\n"
            })
        };
        let pairs: ModelTemplateProposal = serde_json::from_value(base(serde_json::json!([
            {"from": "business_expert", "to": "synthesizer"}
        ])))
        .expect("pairs shape");
        assert!(matches!(
            pairs.dependencies,
            Some(ModelTemplateDependencies::Pairs(_))
        ));
        let groups: ModelTemplateProposal = serde_json::from_value(base(serde_json::json!({
            "business_team": ["business_expert"],
            "convergence": ["business_team"]
        })))
        .expect("groups shape");
        assert!(matches!(
            groups.dependencies,
            Some(ModelTemplateDependencies::Groups(_))
        ));
        let group_array: ModelTemplateProposal = serde_json::from_value(base(
            serde_json::json!([{"business_team": ["business_expert"]}]),
        ))
        .expect("group-array shape");
        assert!(matches!(
            group_array.dependencies,
            Some(ModelTemplateDependencies::GroupArray(_))
        ));
        let strings: ModelTemplateProposal = serde_json::from_value(base(serde_json::json!([
            "business_expert -> synthesizer",
            "synthesizer : convergence"
        ])))
        .expect("string-array shape");
        assert!(matches!(
            strings.dependencies,
            Some(ModelTemplateDependencies::Strings(_))
        ));
        let single: ModelTemplateProposal =
            serde_json::from_value(base(serde_json::json!("business_expert -> synthesizer")))
                .expect("single-string shape");
        assert!(matches!(
            single.dependencies,
            Some(ModelTemplateDependencies::String(_))
        ));
    }

    #[test]
    fn model_template_proposal_tolerates_extra_fields_for_guidance() {
        let invalid = serde_json::json!({
            "template_id": "t",
            "name": "n",
            "roles": [],
            "instructions": "i",
            "protocol": "democratic_centralism@1",
            "summary": "extra prose the model likes to include"
        });
        let parsed = serde_json::from_value::<ModelTemplateProposal>(invalid)
            .expect("the model-facing contract stays tolerant; runtime validation owns strictness");
        assert_eq!(parsed.template_id, "t");
    }

    #[test]
    fn model_template_proposal_guidance_never_blocks_dependency_strings() {
        let parsed = serde_json::from_value::<ModelTemplateProposal>(serde_json::json!({
            "template_id": "t",
            "name": "n",
            "roles": [],
            "instructions": "i",
            "dependencies": "business_expert -> convergence_arbiter"
        }))
        .expect("single-string dependencies are part of the guidance union");
        assert!(matches!(
            parsed.dependencies,
            Some(ModelTemplateDependencies::String(_))
        ));
    }

    #[test]
    fn route_input_contract_contains_only_semantic_slots() {
        let input: ModelRuntimeOrchestrationInput = serde_json::from_value(serde_json::json!({
            "intent": "route newly arrived work",
            "operation": "route_input",
            "input_disposition": {
                "decisions": [{
                    "input_slots": [0, 1],
                    "action": "add_required_task",
                    "relation": "new_task",
                    "objective": "implement and verify the requested change",
                    "required": true,
                    "confidence_basis_points": 9500,
                    "reason": "both updates describe the same required work"
                }]
            }
        }))
        .expect("semantic route input");
        assert_eq!(input.operation, RuntimeOrchestrationOperation::RouteInput);
        input
            .input_disposition
            .expect("disposition")
            .validate_slots(2)
            .expect("complete slot coverage");
    }

    #[test]
    fn minimal_example_is_valid_model_input() {
        let value =
            serde_json::to_value(ModelRuntimeOrchestrationInput::minimal_example()).expect("value");
        let decoded: ModelRuntimeOrchestrationInput =
            serde_json::from_value(value).expect("typed example");
        assert_eq!(decoded.operation, RuntimeOrchestrationOperation::Propose);
    }

    #[test]
    fn node_controls_keep_safe_defaults_but_require_explicit_escalation_policy() {
        let request: ModelRuntimeOrchestrationInput = serde_json::from_value(serde_json::json!({
            "intent": "Review the implementation",
            "operation": "propose",
            "proposal": {
                "mutation_id": "review-defaults",
                "nodes": [{
                    "node_id": "review",
                    "recipe": "review",
                    "objective": "Review the implementation",
                    "managed_agent_escalation": "none"
                }],
                "reason": "Independent verification is required"
            }
        }))
        .expect("model request uses serde defaults");
        let node = &request.proposal.expect("proposal").nodes[0];
        assert_eq!(node.multiplicity, 1);
        assert!(node.required);
        assert_eq!(node.dependency, ExecutionDependencyPolicy::All);
        assert_eq!(
            node.managed_agent_escalation,
            ManagedAgentEscalationRequirement::None
        );

        let missing_policy =
            serde_json::from_value::<ModelRuntimeOrchestrationInput>(serde_json::json!({
                "intent": "Review the implementation",
                "operation": "propose",
                "proposal": {
                    "mutation_id": "review-missing-escalation-policy",
                    "nodes": [{
                        "node_id": "review",
                        "recipe": "review",
                        "objective": "Review the implementation"
                    }],
                    "reason": "Independent verification is required"
                }
            }));
        assert!(
            missing_policy.is_err(),
            "model must select none or required"
        );
    }
}
