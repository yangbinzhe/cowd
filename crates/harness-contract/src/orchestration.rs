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
    /// Compatibility-only semantic focus input. New root decisions should use
    /// `template` when a user requests named roles: a focus is not a Team
    /// role binding and never creates an Agent identity by itself.
    #[serde(default)]
    pub focuses: Vec<ModelSemanticFocus>,
    /// Optional turn-scoped Team template for this one workstream. It is never
    /// published to the catalog: Runtime validates it, clips its grants to the
    /// authenticated session ceiling, and creates the immutable snapshot. This
    /// lets one Team carry any user-requested set of roles without treating
    /// every role as a separate Team workstream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<ModelTemplateProposal>,
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
        let mut templates = Vec::new();
        let nodes = self
            .workstreams
            .into_iter()
            .map(|workstream| {
                // A custom template keeps evidence acceptance at the role
                // boundary, where each named role can own a distinct source.
                // The Team node still needs their union in order to issue a
                // physical lease.  Requiring the model to duplicate those
                // entries in the optional workstream-level contract created a
                // split-brain contract: accepted role criteria could be
                // rejected at tool execution because the Team lease fell back
                // to an intent-derived directory.  Derive only typed scope
                // criteria here; ordinary role acceptance stays local to the
                // role and is not promoted to a Team-wide obligation.
                let template_evidence_scopes = workstream
                    .template
                    .as_ref()
                    .map(template_declared_evidence_scopes)
                    .unwrap_or_default();
                if let Some(template) = workstream.template {
                    templates.push(serde_json::json!({
                        "node_id": workstream.workstream_id.clone(),
                        "template": template,
                    }));
                }
                ModelGraphSemanticNode {
                    node_id: workstream.workstream_id,
                    recipe: CapabilityRecipeId::Team,
                    objective: workstream.objective,
                    depends_on: workstream.depends_on,
                    multiplicity: 1,
                    // Semantic focus is not an executable role binding. The
                    // optional template above is compiled by Runtime into the
                    // complete immutable Team role partition.
                    focuses: Vec::new(),
                    managed_agent_escalation: workstream.managed_agent_escalation,
                    template: None,
                    target_session_id: None,
                    output_artifacts: workstream.output_artifacts,
                    evidence_contract: merge_workstream_evidence_contract(
                        workstream.evidence_contract,
                        template_evidence_scopes,
                    ),
                    required_evidence_refs: Vec::new(),
                    required: true,
                    dependency: ExecutionDependencyPolicy::All,
                    cancellation_group: None,
                }
            })
            .collect::<Vec<_>>();
        let template_proposal = match templates.len() {
            0 => None,
            1 if nodes.len() == 1 => templates
                .pop()
                .and_then(|entry| entry.get("template").cloned()),
            _ => Some(serde_json::json!({ "teams": templates })),
        };
        ModelRuntimeOrchestrationInput {
            intent: self.intent,
            operation: RuntimeOrchestrationOperation::Propose,
            inspect_execution_id: None,
            proposal: Some(ModelGraphMutationProposal {
                mutation_id: format!("control-decision:{}", self.decision_id),
                target_execution_id: None,
                expected_revision: None,
                nodes,
                completion: ExecutionCompletionContract::default(),
                reason: self.reason,
            }),
            template_proposal,
            control: None,
            input_disposition: None,
            evidence_refs: Vec::new(),
            constraints: ModelRuntimeOrchestrationConstraints::default(),
        }
    }
}

fn template_declared_evidence_scopes(template: &ModelTemplateProposal) -> Vec<String> {
    template
        .roles
        .iter()
        .flat_map(|role| role.acceptance.iter())
        .filter(|criterion| criterion.trim().starts_with("evidence_scope:"))
        .cloned()
        .collect()
}

fn merge_workstream_evidence_contract(
    workstream_contract: Vec<String>,
    template_evidence_scopes: Vec<String>,
) -> Vec<String> {
    let mut merged = normalize_workstream_evidence_contract(workstream_contract);
    merged.extend(normalize_workstream_evidence_contract(
        template_evidence_scopes,
    ));
    merged.sort();
    merged.dedup();
    merged
}

/// Normalize the narrow set of scope spellings that model providers commonly
/// emit for a collaboration workstream.  `scope:path:project/file.rs` means
/// the same bounded read evidence as `evidence_scope:read:project/file.rs`;
/// accepting that alias here keeps the model-facing semantic port tolerant
/// without widening authorization.  Absolute paths and traversal remain
/// untouched and therefore fail the canonical Runtime validation.
fn normalize_workstream_evidence_contract(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| {
            let trimmed = value.trim();
            let Some(path) = trimmed.strip_prefix("scope:path:") else {
                return value;
            };
            let path = path.trim().replace('\\', "/");
            let bounded_relative = !path.is_empty()
                && !path.starts_with('/')
                && !path.split('/').any(|part| part == "..");
            bounded_relative
                .then(|| format!("evidence_scope:read:{path}"))
                .unwrap_or(value)
        })
        .collect()
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
    #[serde(default)]
    #[schemars(
        description = "Named artifacts this role consumes from declared upstream roles. Required for every dependency consumer in a new turn-scoped Team; each value must match an upstream role output_artifacts value."
    )]
    pub input_artifacts: Vec<String>,
    #[serde(default)]
    #[schemars(
        description = "Named artifacts this role publishes for downstream roles or Team synthesis. Required for every dependency producer in a new turn-scoped Team."
    )]
    pub output_artifacts: Vec<String>,
    #[schemars(
        description = "Explicit typed execution behavior. Runtime never infers this from a role name: use reacquire_evidence or verification for independent evidence roles; use reducer plus upstream_consumption and terminal_candidate for a role that synthesizes upstream work."
    )]
    pub behavior: Vec<crate::team::RoleBehaviorFacet>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ModelProposedDependency {
    #[schemars(
        description = "Upstream producer role_id. This role must finish successfully before `to` may start; for evidence synthesis use each evidence-producing role here."
    )]
    pub from: String,
    #[schemars(
        description = "Downstream consumer role_id. This role starts only after `from` finishes; a final reviewer or arbiter that consumes evidence belongs here, never in `from`."
    )]
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
    #[schemars(
        description = "Directional role execution edges. Every pair is {from: upstream producer, to: downstream consumer}; `from` completes before `to` starts. For a final arbiter, put all evidence/review roles in from and the arbiter in to."
    )]
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
            "\"behavior\"",
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
                focuses: vec![ModelSemanticFocus {
                    focus_id: "provider-supplied-role".to_string(),
                    role_id: "not-a-runtime-role".to_string(),
                    objective: "must not become a template partition".to_string(),
                    evidence_responsibilities: Vec::new(),
                }],
                template: None,
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
        assert!(proposal.nodes[0].focuses.is_empty());
        assert!(proposal.nodes[0].template.is_none());
        assert!(orchestration.template_proposal.is_none());
    }

    #[test]
    fn narrow_collaboration_decision_normalizes_bounded_scope_path_evidence() {
        let input = ModelCollaborationControlDecision {
            decision_id: "scope-alias".to_string(),
            intent: "read one source file".to_string(),
            workstreams: vec![ModelCollaborationWorkstream {
                workstream_id: "source-review".to_string(),
                objective: "inspect source".to_string(),
                depends_on: Vec::new(),
                focuses: Vec::new(),
                template: None,
                output_artifacts: Vec::new(),
                evidence_contract: vec!["scope:path:crates/runtime/src/lib.rs".to_string()],
                managed_agent_escalation: ManagedAgentEscalationRequirement::None,
            }],
            reason: "bounded evidence".to_string(),
        };

        let contract = input
            .into_runtime_orchestration_input()
            .proposal
            .expect("proposal")
            .nodes[0]
            .evidence_contract
            .clone();
        assert_eq!(
            contract,
            vec!["evidence_scope:read:crates/runtime/src/lib.rs".to_string()]
        );
    }

    #[test]
    fn narrow_collaboration_decision_preserves_one_custom_multirole_team() {
        let template = ModelTemplateProposal {
            template_id: "cowd/turn-runtime-audit".to_string(),
            name: "运行时审计团队".to_string(),
            team_display_name: Some("运行时审计团队".to_string()),
            role_display_names: Vec::new(),
            roles: vec![
                ModelProposedRole {
                    role_id: "fault_topology_scout".to_string(),
                    display_name: Some("故障拓扑侦察员".to_string()),
                    team: None,
                    responsibility: "梳理调用链".to_string(),
                    agent_definition_ref: None,
                    grant_ceiling: vec![ModelGrantCapability::Read],
                    fixed_count: Some(1),
                    min_count: None,
                    max_count: None,
                    acceptance: vec![
                        "summary".to_string(),
                        "evidence".to_string(),
                        "evidence_scope:read:crates/runtime/src/orchestration/mod.rs".to_string(),
                    ],
                    input_artifacts: Vec::new(),
                    output_artifacts: vec!["topology_evidence".to_string()],
                    behavior: vec![crate::team::RoleBehaviorFacet::ReacquireEvidence {
                        required: true,
                    }],
                },
                ModelProposedRole {
                    role_id: "concurrency_semantics_auditor".to_string(),
                    display_name: Some("并发语义审计员".to_string()),
                    team: None,
                    responsibility: "审计并发语义".to_string(),
                    agent_definition_ref: None,
                    grant_ceiling: vec![ModelGrantCapability::Read],
                    fixed_count: Some(1),
                    min_count: None,
                    max_count: None,
                    acceptance: vec![
                        "summary".to_string(),
                        "evidence".to_string(),
                        "evidence_scope:read:crates/runtime/src/team/template_candidate.rs"
                            .to_string(),
                    ],
                    input_artifacts: Vec::new(),
                    output_artifacts: vec!["summary".to_string(), "evidence".to_string()],
                    behavior: vec![crate::team::RoleBehaviorFacet::TerminalCandidate {
                        required: true,
                    }],
                },
            ],
            dependencies: None,
            result_fields: vec!["summary".to_string(), "evidence".to_string()],
            evidence_required: true,
            instructions: "只读审计并交叉核对证据。".to_string(),
        };
        let input = ModelCollaborationControlDecision {
            decision_id: "runtime-audit".to_string(),
            intent: "审计运行时".to_string(),
            workstreams: vec![ModelCollaborationWorkstream {
                workstream_id: "runtime-audit-team".to_string(),
                objective: "由两个角色共同审计运行时".to_string(),
                depends_on: Vec::new(),
                focuses: Vec::new(),
                template: Some(template),
                output_artifacts: vec!["audit".to_string()],
                evidence_contract: vec!["summary".to_string(), "evidence".to_string()],
                managed_agent_escalation: ManagedAgentEscalationRequirement::None,
            }],
            reason: "用户明确要求自定义角色".to_string(),
        };

        let orchestration = input.into_runtime_orchestration_input();
        let proposal = orchestration.proposal.expect("derived proposal");
        assert_eq!(
            proposal.nodes.len(),
            1,
            "one custom Team remains one workstream"
        );
        let template = orchestration
            .template_proposal
            .expect("custom template retained");
        assert_eq!(template["roles"].as_array().map(Vec::len), Some(2));
        assert_eq!(template["roles"][0]["display_name"], "故障拓扑侦察员");
        assert_eq!(template["roles"][1]["display_name"], "并发语义审计员");
        assert_eq!(
            proposal.nodes[0].evidence_contract,
            vec![
                "evidence".to_string(),
                "evidence_scope:read:crates/runtime/src/orchestration/mod.rs".to_string(),
                "evidence_scope:read:crates/runtime/src/team/template_candidate.rs".to_string(),
                "summary".to_string(),
            ],
            "role-owned typed evidence scopes must become the parent Team lease without promoting arbitrary role acceptance"
        );
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
                    "acceptance": ["findings", "evidence"],
                    "behavior": [{"kind": "reacquire_evidence", "required": true}]
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
