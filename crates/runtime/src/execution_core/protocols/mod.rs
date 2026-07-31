//! Versioned deliberation protocol contracts and pure ExecutionGraph compilers.
//!
//! Protocols describe only graph structure and canonical task packets. The
//! graph runner, agent runtime, evidence services, and synthesizer retain all
//! execution, persistence, and publication ownership.

mod contract;
mod debate;
mod incident;
mod jps;
mod result_reducer;
mod review_fix;
mod validation;

use std::collections::BTreeMap;

use harness_contract::agent::AgentTaskIntent;
use harness_contract::context::ContextBudgetLeaseRef;
use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
    ExecutionRecoveryCursor,
};
use thiserror::Error;

use crate::execution_core::graph::executors::{
    AgentTaskExecutor, SynthesizeNodeExecutor, VerifyNodeExecutor,
};

pub use contract::{
    OutputSpec, ProtocolAvailability, ProtocolCompileRequest, ProtocolExecutorKind, ProtocolId,
    ProtocolRef, ProtocolSpec, RepairPolicy, RepairTrigger, RoleDependencyKind, RoleDependencySpec,
    RoleEvidenceMode, RoleSpec, StopPolicy,
};
pub use debate::{compile as compile_debate, DebateProtocolCompiler};
pub use incident::{compile as compile_incident, IncidentProtocolCompiler};
pub use jps::{compile as compile_jps, JpsProtocolCompiler};
pub use result_reducer::ProtocolResultReducer;
pub use review_fix::{compile as compile_review_fix, ReviewFixProtocolCompiler};
pub use validation::{
    validate_protocol_graph, validate_protocol_registry, validate_protocol_request,
    validate_protocol_spec, ProtocolValidationError,
};

#[derive(Debug, Error)]
pub enum ProtocolCompileError {
    #[error(transparent)]
    Validation(#[from] ProtocolValidationError),
    #[error("failed to encode canonical AgentTaskIntent: {0}")]
    PacketEncoding(#[from] serde_json::Error),
}

/// Registry of the protocol contracts that can be compiled by this runtime
/// revision. It has no mutable state and does not imply executor registration.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProtocolRegistry;

impl ProtocolRegistry {
    #[must_use]
    pub fn all() -> Vec<ProtocolSpec> {
        vec![
            debate::spec(),
            jps::spec(),
            review_fix::spec(),
            incident::spec(),
        ]
    }

    #[must_use]
    pub fn spec(id: ProtocolId) -> ProtocolSpec {
        match id {
            ProtocolId::Debate => debate::spec(),
            ProtocolId::Jps => jps::spec(),
            ProtocolId::ReviewFix => review_fix::spec(),
            ProtocolId::Incident => incident::spec(),
        }
    }

    pub fn resolve(protocol: &ProtocolRef) -> Result<ProtocolSpec, ProtocolValidationError> {
        let spec = Self::spec(protocol.id);
        if spec.version != protocol.version {
            return Err(ProtocolValidationError::InvalidRequest {
                protocol: protocol.to_string(),
                reason: format!("unsupported version; current version is {}", spec.version),
            });
        }
        validate_protocol_spec(&spec)?;
        if let ProtocolAvailability::Unavailable {
            available_in,
            reason,
        } = &spec.availability
        {
            return Err(ProtocolValidationError::Unavailable {
                protocol: protocol.to_string(),
                available_in: available_in.clone(),
                reason: reason.clone(),
            });
        }
        Ok(spec)
    }

    pub fn validate() -> Result<(), ProtocolValidationError> {
        validate_protocol_registry(&Self::all())
    }

    pub fn compile(
        request: &ProtocolCompileRequest,
    ) -> Result<ExecutionGraph, ProtocolCompileError> {
        match request.protocol.id {
            ProtocolId::Debate => debate::compile(request),
            ProtocolId::Jps => jps::compile(request),
            ProtocolId::ReviewFix => review_fix::compile(request),
            ProtocolId::Incident => incident::compile(request),
        }
    }
}

pub(crate) struct ProtocolGraphBuilder<'a> {
    spec: &'a ProtocolSpec,
    request: &'a ProtocolCompileRequest,
    graph: ExecutionGraph,
    role_nodes: BTreeMap<String, Vec<String>>,
}

impl<'a> ProtocolGraphBuilder<'a> {
    pub(crate) fn new(spec: &'a ProtocolSpec, request: &'a ProtocolCompileRequest) -> Self {
        Self {
            spec,
            request,
            graph: ExecutionGraph {
                id: request.graph_id.clone(),
                revision: 0,
                objective: request.objective.clone(),
                service_class: if request.parent_execution.is_some() {
                    harness_contract::execution_graph::ExecutionServiceClass::Foreground
                } else {
                    harness_contract::execution_graph::ExecutionServiceClass::Interactive
                },
                parent_execution: request.parent_execution.clone(),
                nodes: Vec::new(),
                edges: Vec::new(),
                node_statuses: BTreeMap::new(),
                node_results: BTreeMap::new(),
                recovery_cursor: ExecutionRecoveryCursor::default(),
            },
            role_nodes: BTreeMap::new(),
        }
    }

    pub(crate) fn add_agent(
        &mut self,
        role_id: &str,
        slot: usize,
        dependencies: &[String],
    ) -> Result<String, ProtocolCompileError> {
        let role = self
            .spec
            .role(role_id)
            .ok_or_else(|| ProtocolValidationError::InvalidSpec {
                protocol: self.spec.protocol_ref().to_string(),
                reason: format!("compiler referenced undeclared role `{role_id}`"),
            })?;
        let role_label = format!("{role_id}-{}", slot + 1);
        let node_id = format!("{}:{role_label}", self.graph.id);
        let agent_id = format!("{}:agent:{role_label}", self.graph.id);
        let mut acceptance = role.output.required_fields.clone();
        if role.output.evidence_required {
            acceptance.push("evidence_backed".to_string());
        }
        if role.output.allows_unresolved {
            acceptance.push("unresolved_explicit".to_string());
        }
        let mut constraints = vec![
            format!("protocol:{}", self.spec.protocol_ref()),
            format!("protocol_role:{role_id}"),
            format!("protocol_slot:{slot}"),
            format!(
                "protocol_stop:max_agent_attempts={}",
                self.spec.stop_policy.max_agent_attempts
            ),
            format!(
                "protocol_repair:max_revisions={}",
                self.spec.repair_policy.max_revisions
            ),
        ];
        constraints.extend(self.request.backend_constraint.iter().cloned());
        if self.spec.stop_policy.allows_unresolved {
            // The graph executor uses this explicit protocol contract to
            // preserve a terminal role failure for the reducer instead of
            // severing every dependent path. The Agent lifecycle itself
            // remains failed/blocked; only the protocol graph can decide
            // whether the surviving evidence is enough for honest synthesis.
            constraints.push("protocol_allows_unresolved:true".to_string());
        }
        constraints.extend(
            self.request
                .resource_scopes
                .iter()
                .map(|scope| format!("resource:{scope}")),
        );
        let idempotency_key = format!("{node_id}:attempt");
        // A protocol graph owns collaboration topology. `runtime_orchestrate`
        // is never available to a role worker, but evidence access follows the
        // declared role contract rather than whether the role happens to be a
        // frontier node. Incident evidence collectors and JPS solutions are
        // intentionally dependent on triage/frame output *and* allowed to
        // acquire their own bounded evidence. Synthesis roles are not.
        let allowed_tools = if role.evidence_mode == RoleEvidenceMode::Acquire {
            self.request
                .allowed_tools
                .iter()
                .filter(|tool| !tool.eq_ignore_ascii_case("runtime_orchestrate"))
                .cloned()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        constraints.push(format!(
            "protocol_evidence_mode:{}",
            role.evidence_mode.as_str()
        ));
        let intent = AgentTaskIntent {
            selected_agent_id: None,
            definition_ref: None,
            granted_capabilities: Vec::new(),
            principal_id: self.request.principal_id.clone(),
            source_turn_id: self.request.source_turn_id.clone(),
            run_id: format!("{}:run:{role_label}", self.graph.id),
            task_id: format!("{}:task:{role_label}", self.graph.id),
            session_id: self.request.session_id.clone(),
            mission_id: self.request.mission_id.clone(),
            team_id: self.request.team_id.clone(),
            graph_id: self.graph.id.clone(),
            node_id: node_id.clone(),
            attempt: 1,
            expected_graph_revision: self.graph.revision,
            objective: format!(
                "{}\n\nProtocol {} role {}: {}\n\n## Role execution boundary\nYou are one bounded worker in an already-compiled protocol graph. Complete only this role's declared output. {} {} Do not create a new team, delegate another agent, or re-decompose the parent objective. When the declared output is supported, return a final role result with unresolved items made explicit.",
                self.request.objective,
                self.spec.protocol_ref(),
                role_id,
                role.responsibility,
                role_evidence_instruction(role.evidence_mode),
                role_slot_focus(role, slot),
            ),
            acceptance,
            constraints,
            context_refs: self.request.context_refs.clone(),
            evidence_refs: self.request.evidence_refs.clone(),
            resource_scopes: self.request.resource_scopes.clone(),
            allowed_tools,
            allowed_skills: self.request.allowed_skills.clone(),
            permission_ceiling: self.request.permission_ceiling.clone(),
            model_lease: self.request.model_lease.clone(),
            budget_lease: ContextBudgetLeaseRef::new(
                format!("{}:{node_id}", self.request.budget_lease_id),
                agent_id,
                "protocol_agent",
                self.request.budget_tokens,
                self.request.budget_revision,
            ),
            managed_invocation: None,
            idempotency_key: idempotency_key.clone(),
        };
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            AgentTaskExecutor::KIND,
            serde_json::to_string(&intent)?,
        );
        node.id = node_id.clone();
        node.idempotency_key = idempotency_key;
        node.acceptance.criteria = intent.acceptance.clone();
        node.retry_policy.max_attempts = self.spec.stop_policy.max_agent_attempts;
        node.resource_scopes = self.request.resource_scopes.clone();
        self.graph.nodes.push(node);
        for dependency in dependencies {
            self.graph.edges.push(ExecutionEdge {
                from: dependency.clone(),
                to: node_id.clone(),
                kind: ExecutionEdgeKind::DependsOn,
            });
        }
        self.role_nodes
            .entry(role_id.to_string())
            .or_default()
            .push(node_id.clone());
        Ok(node_id)
    }

    pub(crate) fn add_terminal_chain(
        &mut self,
        verification_inputs: &[String],
    ) -> (String, String) {
        let verify_id = format!("{}:verify", self.graph.id);
        let synthesize_id = format!("{}:synthesize", self.graph.id);
        let mut verify = ExecutionNodeSpec::new(
            ExecutionNodeKind::Verify,
            VerifyNodeExecutor::KIND,
            format!("protocol:{}:verify", self.spec.protocol_ref()),
        );
        verify.id = verify_id.clone();
        verify.idempotency_key = format!("{verify_id}:attempt");
        verify.acceptance.criteria = vec!["protocol_verification".to_string()];
        let mut synthesize = ExecutionNodeSpec::new(
            ExecutionNodeKind::Synthesize,
            SynthesizeNodeExecutor::KIND,
            format!("protocol:{}:synthesize", self.spec.protocol_ref()),
        );
        synthesize.id = synthesize_id.clone();
        synthesize.idempotency_key = format!("{synthesize_id}:attempt");
        synthesize.acceptance.criteria = self.spec.output.required_fields.clone();
        self.graph.nodes.extend([verify, synthesize]);
        for input in verification_inputs {
            self.graph.edges.push(ExecutionEdge {
                from: input.clone(),
                to: verify_id.clone(),
                kind: ExecutionEdgeKind::DependsOn,
            });
        }
        self.graph.edges.push(ExecutionEdge {
            from: verify_id.clone(),
            to: synthesize_id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        (verify_id, synthesize_id)
    }

    pub(crate) fn finish(self) -> Result<ExecutionGraph, ProtocolCompileError> {
        validate_protocol_graph(self.spec, self.request, &self.graph)?;
        Ok(self.graph)
    }
}

fn role_evidence_instruction(mode: RoleEvidenceMode) -> &'static str {
    match mode {
        RoleEvidenceMode::ObjectiveOnly => {
            "Use the supplied objective and canonical context to frame the work. Do not call tools: record unknowns as unknowns so later evidence roles can resolve them."
        }
        RoleEvidenceMode::Acquire => {
            "Use upstream results before requesting more evidence. When the objective requires source, workspace, file, or current-state evidence, first use an authorized read-only tool to establish the concrete path or fact, then cite that receipt in the role result. Do not substitute model knowledge for requested source evidence. Stop acquiring evidence once the role output is supported."
        }
        RoleEvidenceMode::UpstreamOnly => {
            "Use the completed upstream results as the evidence packet. Do not call tools to rediscover it; reconcile contradictions and state missing evidence explicitly."
        }
    }
}

fn role_slot_focus(role: &RoleSpec, slot: usize) -> &'static str {
    if !role.has_variable_cardinality() {
        return "";
    }
    // Parallel instances must contribute complementary work instead of each
    // restarting the same investigation. The lenses are generic enough for
    // arbitrary domains while still giving a model a concrete divergence cue.
    match slot % 4 {
        0 => "Independent lane: establish the current-state evidence and primary constraint path.",
        1 => "Independent lane: examine alternatives, counterexamples, and tradeoffs.",
        2 => "Independent lane: examine integration boundaries, operational impact, and verification evidence.",
        _ => "Independent lane: examine future risks, unresolved assumptions, and the strongest falsification path.",
    }
}
