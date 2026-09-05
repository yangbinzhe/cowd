//! Agent-first Runtime capability catalog and model primer.
//!
//! Models choose small semantic actions while Runtime owns identity,
//! authorization, concurrency, persistence, recovery and terminal truth.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::definition_registry::RuntimeTeamTemplateCatalogEntry;
use crate::execution_core::RuntimeExecutionDecision;
use crate::AgentCatalogEntry;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapability {
    pub id: String,
    pub summary: String,
    pub recommended_tools: Vec<String>,
    pub when_to_use: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapabilityManifest {
    pub name: String,
    pub capabilities: Vec<RuntimeCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapabilityCatalog {
    pub name: String,
    pub templates: Vec<RuntimeTemplateSummary>,
    pub protocols: Vec<RuntimeProtocolSummary>,
    pub operation_groups: Vec<RuntimeOperationGroup>,
    pub action_contracts: Vec<RuntimeActionContract>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeProtocolSummary {
    pub protocol_id: String,
    pub version: u32,
    pub availability: String,
    pub summary: String,
    pub role_ids: Vec<String>,
    pub supports_bounded_repair: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeTemplateSummary {
    pub template_id: String,
    pub protocol_id: String,
    pub protocol_version: u32,
    pub availability: String,
    pub requires_review: bool,
    pub best_for: Vec<String>,
    #[serde(default)]
    pub role_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeOperationGroup {
    pub id: String,
    pub summary: String,
    pub operations: Vec<RuntimeOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeOperation {
    pub id: String,
    pub owner: String,
    pub summary: String,
    pub model_intent: String,
    pub validation_gate: String,
    pub output_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeActionContract {
    pub runtime_action: String,
    pub tool_action: String,
    pub when_to_use: String,
    pub required_intent_fields: Vec<String>,
    pub validation: String,
    pub expected_projection: Vec<String>,
}

impl RuntimeCapabilityManifest {
    #[must_use]
    pub fn current() -> Self {
        Self {
            name: "cowd-agent-first-capabilities".to_string(),
            capabilities: vec![
                RuntimeCapability {
                    id: "agent_action_loop".to_string(),
                    summary: "Create and evolve collaborative work through small durable actions."
                        .to_string(),
                    recommended_tools: harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
                        .iter()
                        .map(|tool| (*tool).to_string())
                        .collect(),
                    when_to_use: vec![
                        "Use whenever independent roles, parallel work, discussion, review, or replanning improves the objective."
                            .to_string(),
                    ],
                },
                RuntimeCapability {
                    id: "evidence_and_content".to_string(),
                    summary: "Keep long content in normal model output or durable artifacts and pass compact references through actions."
                        .to_string(),
                    recommended_tools: vec![
                        "context_retrieve".to_string(),
                        "evidence_retrieve".to_string(),
                        "artifact_commit".to_string(),
                    ],
                    when_to_use: vec![
                        "Use references for source evidence, long reports, and final deliverables."
                            .to_string(),
                    ],
                },
            ],
        }
    }
}

impl RuntimeCapabilityCatalog {
    #[must_use]
    pub fn current() -> Self {
        let action_contracts = harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
            .iter()
            .map(|action| RuntimeActionContract {
                runtime_action: (*action).to_string(),
                tool_action: (*action).to_string(),
                when_to_use: action_purpose(action).to_string(),
                required_intent_fields: Vec::new(),
                validation: "Runtime binds actor identity and validates authorization, revision, invariants, and durable references."
                    .to_string(),
                expected_projection: vec!["program_revision".to_string(), "next_actions".to_string()],
            })
            .collect();
        Self {
            name: "cowd-agent-first-capability-catalog".to_string(),
            templates: Vec::new(),
            protocols: Vec::new(),
            operation_groups: vec![RuntimeOperationGroup {
                id: "agent_collaboration".to_string(),
                summary: "One incremental action protocol shared by root, leads, and members."
                    .to_string(),
                operations: harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
                    .iter()
                    .map(|action| RuntimeOperation {
                        id: (*action).to_string(),
                        owner: "runtime.agentic.action_service".to_string(),
                        summary: action_purpose(action).to_string(),
                        model_intent:
                            "Choose the next useful semantic action from current Program state."
                                .to_string(),
                        validation_gate:
                            "Trusted actor binding plus action-specific state invariant."
                                .to_string(),
                        output_refs: vec![
                            "action_receipt".to_string(),
                            "program_revision".to_string(),
                        ],
                    })
                    .collect(),
            }],
            action_contracts,
        }
    }

    /// Dynamic Teams replace catalog templates; old entries never become a
    /// hidden fallback in the model-facing contract.
    #[must_use]
    pub fn from_registry(_entries: &[RuntimeTeamTemplateCatalogEntry]) -> Self {
        Self::current()
    }
}

fn action_purpose(action: &str) -> &'static str {
    match action {
        "state_inspect" => "Read a bounded Program, Team, Task, Topic, or Artifact projection.",
        "team_create" => "Create a Team with a model-authored purpose.",
        "agent_invite" => "Invite a catalog-backed Agent into a Team with a focused role.",
        "task_publish" => "Publish bounded work, dependencies, and acceptance criteria.",
        "task_claim" => "Claim eligible work through a Runtime lease.",
        "task_release" => "Release or re-open claimed work without losing history.",
        "task_submit" => "Submit durable artifacts and evidence for review.",
        "task_review" => "Independently accept, challenge, or request rework.",
        "message_publish" => "Publish a durable scoped discussion entry.",
        "artifact_commit" => "Attach a durable content reference and provenance.",
        "objective_complete_request" => "Ask the Supervisor to verify terminal invariants.",
        _ => "Apply one bounded semantic collaboration action.",
    }
}

#[must_use]
pub fn runtime_capability_primer() -> String {
    [
        "## Agent-first collaboration contract",
        "You own semantic decisions: Team purpose, roles, Tasks, dependencies, discussion, challenge, replanning, and synthesis.",
        "Use small Agent actions incrementally. Do not emit a complete graph, template proposal, or long JSON plan.",
        "Runtime owns actor identity, authorization, revisions, idempotency, leases, concurrent execution, durable artifacts, recovery, and terminal truth.",
        "Publish independent Tasks without artificial dependencies so Runtime can execute them concurrently.",
        "Keep long reasoning and deliverables in normal content or workspace files. Action payloads carry compact semantic fields and durable refs.",
        "A Task is complete only after a real claimant submits durable evidence and a different Agent accepts it.",
        "Request Objective completion only after every required Team has members and accepted work, the final artifact is durable and reviewed, and no objective-level blocker remains. Accepted Task limitations remain visible disclosures and are not a second completion veto.",
        "On rejection or restart, inspect current state and continue from the returned revision; never repeat an unchanged failing action.",
    ]
    .join("\n")
}

#[must_use]
pub fn compact_runtime_capability_primer() -> String {
    runtime_capability_primer()
}

#[must_use]
pub fn runtime_capabilities_response(
    intent: &str,
    surface: Option<&str>,
    profile: Option<&str>,
) -> Value {
    runtime_capabilities_response_with_detail(intent, surface, profile, None)
}

#[must_use]
pub fn runtime_capabilities_response_with_detail(
    intent: &str,
    surface: Option<&str>,
    profile: Option<&str>,
    detail: Option<&str>,
) -> Value {
    runtime_capabilities_response_with_leased_decision(intent, surface, profile, detail, None)
}

#[must_use]
pub fn runtime_capabilities_response_with_leased_decision(
    intent: &str,
    surface: Option<&str>,
    profile: Option<&str>,
    detail: Option<&str>,
    leased_decision: Option<&RuntimeExecutionDecision>,
) -> Value {
    let available_tools = harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
        .iter()
        .map(|tool| (*tool).to_string())
        .chain([
            "context_retrieve".to_string(),
            "evidence_retrieve".to_string(),
        ])
        .collect::<Vec<_>>();
    runtime_capabilities_response_with_leased_decision_and_tools(
        intent,
        surface,
        profile,
        detail,
        leased_decision,
        &available_tools,
        None,
        None,
    )
}

#[must_use]
pub fn runtime_capabilities_response_with_leased_decision_and_tools(
    intent: &str,
    surface: Option<&str>,
    profile: Option<&str>,
    detail: Option<&str>,
    leased_decision: Option<&RuntimeExecutionDecision>,
    available_tool_names: &[String],
    _team_template_entries: Option<&[RuntimeTeamTemplateCatalogEntry]>,
    agent_catalog_entries: Option<&[AgentCatalogEntry]>,
) -> Value {
    let actions = harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
        .iter()
        .filter(|tool| {
            available_tool_names
                .iter()
                .any(|available| available == **tool)
        })
        .copied()
        .collect::<Vec<_>>();
    let agents = agent_catalog_entries
        .unwrap_or_default()
        .iter()
        .map(|agent| {
            json!({
                "agent_id": agent.agent_id,
                "name": agent.name,
                "description": agent.description,
                "capabilities": agent.capabilities,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "type": "runtime_capabilities",
        "architecture": "agent_first",
        "intent": intent,
        "surface": surface,
        "profile": profile,
        "detail": detail.unwrap_or("summary"),
        "available": !actions.is_empty(),
        "actions": actions,
        "agent_catalog": agents,
        "manifest": RuntimeCapabilityManifest::current(),
        "catalog": RuntimeCapabilityCatalog::current(),
        "strategy_hint": leased_decision.map(|decision| json!({
            "complexity": format!("{:?}", decision.strategy.understanding.complexity),
            "risk": format!("{:?}", decision.strategy.understanding.risk),
            "required_team_count": decision.strategy.understanding.required_team_count,
        })),
        "control_contract": {
            "model_owns": ["team purpose", "roles", "tasks", "dependencies", "discussion", "review decision", "replanning", "synthesis"],
            "runtime_owns": ["identity", "authorization", "CAS", "idempotency", "leases", "concurrent execution", "artifact durability", "recovery", "terminal verification"],
            "long_content": "ordinary model content or workspace files; actions carry compact refs",
            "parallelism": "publish independent Tasks together; Runtime admits their Agent graphs concurrently",
            "recovery": "inspect current state and continue from its revision",
        },
        "completion": {
            "request": harness_contract::agent_action::OBJECTIVE_COMPLETE_REQUEST_TOOL_ID,
            "requires": ["required Teams staffed", "accepted Tasks", "independent reviewers", "durable reviewed final artifact", "evidence", "no objective-level blockers"],
            "accepted_task_disclosures": "accepted Task limitations remain visible but are not re-litigated by a second completion owner",
        },
        "context_retrieval": {
            "available": available_tool_names.iter().any(|tool| tool == "context_retrieve"),
            "tool": "context_retrieve",
        },
        "evidence_retrieval": {
            "available": available_tool_names.iter().any(|tool| tool == "evidence_retrieve"),
            "tool": "evidence_retrieve",
        },
    })
}
