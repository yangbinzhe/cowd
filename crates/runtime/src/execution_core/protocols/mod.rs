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

use harness_contract::agent::AgentTaskPacket;
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
    RoleSpec, StopPolicy,
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
    #[error("failed to encode canonical AgentTaskPacket: {0}")]
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
        constraints.extend(
            self.request
                .resource_scopes
                .iter()
                .map(|scope| format!("resource:{scope}")),
        );
        let idempotency_key = format!("{node_id}:attempt");
        let packet = AgentTaskPacket {
            run_id: format!("{}:run:{role_label}", self.graph.id),
            agent_id: agent_id.clone(),
            task_id: format!("{}:task:{role_label}", self.graph.id),
            session_id: self.request.session_id.clone(),
            mission_id: self.request.mission_id.clone(),
            team_id: self.request.team_id.clone(),
            graph_id: self.graph.id.clone(),
            node_id: node_id.clone(),
            attempt: 1,
            expected_graph_revision: self.graph.revision,
            objective: format!(
                "{}\n\nProtocol {} role {}: {}",
                self.request.objective,
                self.spec.protocol_ref(),
                role_id,
                role.responsibility
            ),
            acceptance,
            constraints,
            context_refs: self.request.context_refs.clone(),
            evidence_refs: self.request.evidence_refs.clone(),
            allowed_tools: self.request.allowed_tools.clone(),
            allowed_skills: self.request.allowed_skills.clone(),
            permission_lease: self.request.permission_lease.clone(),
            model_lease: self.request.model_lease.clone(),
            budget_lease: ContextBudgetLeaseRef::new(
                format!("{}:{node_id}", self.request.budget_lease_id),
                agent_id,
                "protocol_agent",
                self.request.budget_tokens,
                self.request.budget_revision,
            ),
            idempotency_key: idempotency_key.clone(),
        };
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            AgentTaskExecutor::KIND,
            serde_json::to_string(&packet)?,
        );
        node.id = node_id.clone();
        node.idempotency_key = idempotency_key;
        node.acceptance.criteria = packet.acceptance.clone();
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
