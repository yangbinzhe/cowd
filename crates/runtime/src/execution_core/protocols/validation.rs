use std::collections::{BTreeMap, BTreeSet};

use harness_contract::agent::AgentTaskPacket;
use harness_contract::execution_graph::{
    validate_execution_graph, ExecutionEdgeKind, ExecutionGraph, ExecutionGraphValidationError,
    ExecutionNodeKind,
};
use thiserror::Error;

use super::{
    ProtocolAvailability, ProtocolCompileRequest, ProtocolExecutorKind, ProtocolSpec,
    RoleDependencyKind,
};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProtocolValidationError {
    #[error("protocol `{protocol}` is unavailable until `{available_in}`: {reason}")]
    Unavailable {
        protocol: String,
        available_in: String,
        reason: String,
    },
    #[error("protocol specification `{protocol}` is invalid: {reason}")]
    InvalidSpec { protocol: String, reason: String },
    #[error("protocol compile request `{protocol}` is invalid: {reason}")]
    InvalidRequest { protocol: String, reason: String },
    #[error("protocol graph `{protocol}` is invalid: {reason}")]
    InvalidGraph { protocol: String, reason: String },
    #[error(transparent)]
    ExecutionGraph(#[from] ExecutionGraphValidationError),
}

pub fn validate_protocol_spec(spec: &ProtocolSpec) -> Result<(), ProtocolValidationError> {
    let protocol = spec.protocol_ref().to_string();
    if spec.version == 0 {
        return Err(invalid_spec(&protocol, "version must be positive"));
    }
    if spec.summary.trim().is_empty() {
        return Err(invalid_spec(&protocol, "summary is empty"));
    }
    if spec.roles.is_empty() {
        return Err(invalid_spec(&protocol, "contains no roles"));
    }
    if spec.stop_policy.max_agent_attempts == 0 {
        return Err(invalid_spec(
            &protocol,
            "max_agent_attempts must be positive",
        ));
    }

    validate_output(&protocol, "protocol", &spec.output)?;

    let mut roles = BTreeSet::new();
    for role in &spec.roles {
        if role.id.trim().is_empty() || !roles.insert(role.id.as_str()) {
            return Err(invalid_spec(
                &protocol,
                "role identifiers must be non-empty and unique",
            ));
        }
        if role.responsibility.trim().is_empty() {
            return Err(invalid_spec(
                &protocol,
                &format!("role `{}` has no responsibility", role.id),
            ));
        }
        if !matches!(role.executor, ProtocolExecutorKind::AgentTask) {
            return Err(invalid_spec(
                &protocol,
                &format!("role `{}` has an unsupported executor", role.id),
            ));
        }
        if role.min_instances == 0 || role.min_instances > role.max_instances {
            return Err(invalid_spec(
                &protocol,
                &format!("role `{}` has invalid cardinality", role.id),
            ));
        }
        validate_output(&protocol, &format!("role `{}`", role.id), &role.output)?;
    }

    for dependency in &spec.dependencies {
        if dependency.consumer_role == dependency.provider_role
            || !roles.contains(dependency.consumer_role.as_str())
            || !roles.contains(dependency.provider_role.as_str())
        {
            return Err(invalid_spec(
                &protocol,
                "dependencies must connect distinct declared roles",
            ));
        }
        if dependency.kind == RoleDependencyKind::CrossFanout {
            let consumer = spec.role(&dependency.consumer_role).expect("declared role");
            let provider = spec.role(&dependency.provider_role).expect("declared role");
            if !consumer.has_variable_cardinality()
                || !provider.has_variable_cardinality()
                || consumer.min_instances != provider.min_instances
                || consumer.max_instances != provider.max_instances
            {
                return Err(invalid_spec(
                    &protocol,
                    "cross-fanout dependencies require matching variable roles",
                ));
            }
        }
    }

    for role in &spec.verify_after_roles {
        if !roles.contains(role.as_str()) {
            return Err(invalid_spec(
                &protocol,
                &format!("verify input role `{role}` is not declared"),
            ));
        }
    }
    if spec.verify_after_roles.is_empty() {
        return Err(invalid_spec(
            &protocol,
            "verify requires at least one input role",
        ));
    }

    match (
        &spec.repair_policy.repair_role,
        spec.repair_policy.max_revisions,
    ) {
        (Some(role), revisions) if revisions > 0 && roles.contains(role.as_str()) => {}
        (None, 0) => {}
        _ => {
            return Err(invalid_spec(
                &protocol,
                "repair role and max revisions must agree",
            ));
        }
    }
    Ok(())
}

pub fn validate_protocol_registry(specs: &[ProtocolSpec]) -> Result<(), ProtocolValidationError> {
    if specs.is_empty() {
        return Err(ProtocolValidationError::InvalidSpec {
            protocol: "registry".to_string(),
            reason: "contains no protocols".to_string(),
        });
    }
    let mut refs = BTreeSet::new();
    for spec in specs {
        validate_protocol_spec(spec)?;
        if !refs.insert(spec.protocol_ref()) {
            return Err(ProtocolValidationError::InvalidSpec {
                protocol: "registry".to_string(),
                reason: format!("duplicate protocol `{}`", spec.protocol_ref()),
            });
        }
    }
    Ok(())
}

pub fn validate_protocol_request(
    spec: &ProtocolSpec,
    request: &ProtocolCompileRequest,
) -> Result<(), ProtocolValidationError> {
    validate_protocol_spec(spec)?;
    let protocol = spec.protocol_ref().to_string();
    if request.protocol != spec.protocol_ref() {
        return Err(invalid_request(
            &protocol,
            &format!("request selected `{}`", request.protocol),
        ));
    }
    if let ProtocolAvailability::Unavailable {
        available_in,
        reason,
    } = &spec.availability
    {
        return Err(ProtocolValidationError::Unavailable {
            protocol,
            available_in: available_in.clone(),
            reason: reason.clone(),
        });
    }
    for (field, value) in [
        ("graph_id", request.graph_id.as_str()),
        ("session_id", request.session_id.as_str()),
        ("objective", request.objective.as_str()),
        ("permission_lease", request.permission_lease.as_str()),
        ("model_lease", request.model_lease.as_str()),
        ("budget_lease_id", request.budget_lease_id.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(invalid_request(&protocol, &format!("{field} is empty")));
        }
    }
    for role in spec
        .roles
        .iter()
        .filter(|role| role.has_variable_cardinality())
    {
        if request.fanout < role.min_instances || request.fanout > role.max_instances {
            return Err(invalid_request(
                &protocol,
                &format!(
                    "fanout {} is outside role `{}` bounds {}..={}",
                    request.fanout, role.id, role.min_instances, role.max_instances
                ),
            ));
        }
    }
    for (index, evidence) in request.evidence_refs.iter().enumerate() {
        if !evidence.is_durable()
            || evidence.evidence_ref.0.ref_type.trim().is_empty()
            || evidence.evidence_ref.0.id.trim().is_empty()
            || evidence.sha256.trim().is_empty()
            || evidence.retrieval_selector.trim().is_empty()
            || evidence.visibility_scope.trim().is_empty()
        {
            return Err(invalid_request(
                &protocol,
                &format!("evidence reference {index} is not durable and addressable"),
            ));
        }
    }
    Ok(())
}

pub fn validate_protocol_graph(
    spec: &ProtocolSpec,
    request: &ProtocolCompileRequest,
    graph: &ExecutionGraph,
) -> Result<(), ProtocolValidationError> {
    validate_protocol_request(spec, request)?;
    validate_execution_graph(graph)?;
    let protocol = spec.protocol_ref().to_string();
    if graph.id != request.graph_id || graph.objective != request.objective || graph.revision != 0 {
        return Err(invalid_graph(
            &protocol,
            "graph identity, objective, or initial revision does not match the request",
        ));
    }

    let mut role_nodes = BTreeMap::<String, Vec<PacketNode>>::new();
    let mut verify_nodes = Vec::new();
    let mut synthesize_nodes = Vec::new();
    for node in &graph.nodes {
        match node.kind {
            ExecutionNodeKind::AgentTask => {
                if node.executor_kind != "agent_task" {
                    return Err(invalid_graph(
                        &protocol,
                        &format!("agent node `{}` does not use `agent_task`", node.id),
                    ));
                }
                let packet: AgentTaskPacket =
                    serde_json::from_str(&node.payload_ref).map_err(|error| {
                        invalid_graph(
                            &protocol,
                            &format!(
                                "agent node `{}` has an invalid AgentTaskPacket: {error}",
                                node.id
                            ),
                        )
                    })?;
                let role = packet
                    .constraints
                    .iter()
                    .find_map(|constraint| constraint.strip_prefix("protocol_role:"))
                    .ok_or_else(|| {
                        invalid_graph(
                            &protocol,
                            &format!("agent node `{}` has no protocol role", node.id),
                        )
                    })?;
                let slot = packet
                    .constraints
                    .iter()
                    .find_map(|constraint| constraint.strip_prefix("protocol_slot:"))
                    .ok_or_else(|| {
                        invalid_graph(
                            &protocol,
                            &format!("agent node `{}` has no protocol slot", node.id),
                        )
                    })?
                    .parse::<usize>()
                    .map_err(|_| {
                        invalid_graph(
                            &protocol,
                            &format!("agent node `{}` has an invalid protocol slot", node.id),
                        )
                    })?;
                let role_spec = spec.role(role).ok_or_else(|| {
                    invalid_graph(
                        &protocol,
                        &format!("agent node `{}` references unknown role `{role}`", node.id),
                    )
                })?;
                validate_packet_node(
                    &protocol,
                    request,
                    node,
                    &packet,
                    role_spec,
                    role,
                    slot,
                    spec.stop_policy.max_agent_attempts,
                )?;
                role_nodes
                    .entry(role.to_string())
                    .or_default()
                    .push(PacketNode {
                        id: node.id.clone(),
                        slot,
                    });
            }
            ExecutionNodeKind::Verify => {
                if node.executor_kind != "verify" || !node.resource_scopes.is_empty() {
                    return Err(invalid_graph(
                        &protocol,
                        &format!("verify node `{}` is not canonical", node.id),
                    ));
                }
                verify_nodes.push(node.id.as_str());
            }
            ExecutionNodeKind::Synthesize => {
                if node.executor_kind != "synthesize" || !node.resource_scopes.is_empty() {
                    return Err(invalid_graph(
                        &protocol,
                        &format!("synthesize node `{}` is not canonical", node.id),
                    ));
                }
                synthesize_nodes.push(node.id.as_str());
            }
            _ => {
                return Err(invalid_graph(
                    &protocol,
                    &format!("node `{}` uses a non-protocol execution kind", node.id),
                ));
            }
        }
    }
    if verify_nodes.len() != 1 || synthesize_nodes.len() != 1 {
        return Err(invalid_graph(
            &protocol,
            "protocol graphs require exactly one verify and one synthesize node",
        ));
    }
    let verify_id = verify_nodes[0];
    let synthesize_id = synthesize_nodes[0];

    for role in &spec.roles {
        let nodes = role_nodes.get(&role.id).map_or(&[][..], Vec::as_slice);
        let expected = if role.id == "repair" && !request.enable_repair {
            0
        } else if role.has_variable_cardinality() {
            request.fanout
        } else {
            role.min_instances
        };
        if nodes.len() != expected {
            return Err(invalid_graph(
                &protocol,
                &format!(
                    "role `{}` has {} nodes, expected {expected}",
                    role.id,
                    nodes.len()
                ),
            ));
        }
        let slots = nodes.iter().map(|node| node.slot).collect::<BTreeSet<_>>();
        if slots.len() != nodes.len() || !slots.iter().copied().eq(0..expected) {
            return Err(invalid_graph(
                &protocol,
                &format!("role `{}` has non-contiguous slots", role.id),
            ));
        }
    }

    let dependencies = graph
        .edges
        .iter()
        .filter(|edge| edge.kind == ExecutionEdgeKind::DependsOn)
        .map(|edge| (edge.from.as_str(), edge.to.as_str()))
        .collect::<BTreeSet<_>>();
    for dependency in &spec.dependencies {
        let consumers = role_nodes
            .get(&dependency.consumer_role)
            .map_or(&[][..], Vec::as_slice);
        let providers = role_nodes
            .get(&dependency.provider_role)
            .map_or(&[][..], Vec::as_slice);
        if consumers.is_empty() || providers.is_empty() {
            if dependency.consumer_role == "repair" && !request.enable_repair {
                continue;
            }
            return Err(invalid_graph(
                &protocol,
                &format!(
                    "dependency `{}` <- `{}` is missing a required role node",
                    dependency.consumer_role, dependency.provider_role
                ),
            ));
        }
        for consumer in consumers {
            for provider in providers {
                let required = match dependency.kind {
                    RoleDependencyKind::All => true,
                    RoleDependencyKind::CrossFanout => consumer.slot != provider.slot,
                };
                if required && !dependencies.contains(&(provider.id.as_str(), consumer.id.as_str()))
                {
                    return Err(invalid_graph(
                        &protocol,
                        &format!(
                            "role `{}` node `{}` is missing dependency from `{}`",
                            dependency.consumer_role, consumer.id, provider.id
                        ),
                    ));
                }
                if !required && dependencies.contains(&(provider.id.as_str(), consumer.id.as_str()))
                {
                    return Err(invalid_graph(
                        &protocol,
                        &format!(
                            "role `{}` node `{}` must not consume its matching `{}` input",
                            dependency.consumer_role, consumer.id, dependency.provider_role
                        ),
                    ));
                }
            }
        }
    }
    for role in &spec.verify_after_roles {
        for node in role_nodes.get(role).map_or(&[][..], Vec::as_slice) {
            if !dependencies.contains(&(node.id.as_str(), verify_id)) {
                return Err(invalid_graph(
                    &protocol,
                    &format!("verify node is missing input from `{}`", node.id),
                ));
            }
        }
    }
    if !dependencies.contains(&(verify_id, synthesize_id)) {
        return Err(invalid_graph(
            &protocol,
            "synthesize node must depend on the protocol verify node",
        ));
    }
    Ok(())
}

struct PacketNode {
    id: String,
    slot: usize,
}

fn validate_packet_node(
    protocol: &str,
    request: &ProtocolCompileRequest,
    node: &harness_contract::execution_graph::ExecutionNodeSpec,
    packet: &AgentTaskPacket,
    role: &super::RoleSpec,
    role_id: &str,
    slot: usize,
    max_agent_attempts: u32,
) -> Result<(), ProtocolValidationError> {
    let expected_protocol = format!("protocol:{}", request.protocol);
    let expected_role = format!("protocol_role:{role_id}");
    let expected_slot = format!("protocol_slot:{slot}");
    let backend_constraint_is_bound = request
        .backend_constraint
        .as_ref()
        .is_none_or(|constraint| packet.constraints.contains(constraint));
    if packet.graph_id != request.graph_id
        || packet.node_id != node.id
        || packet.session_id != request.session_id
        || packet.mission_id != request.mission_id
        || packet.team_id != request.team_id
        || packet.expected_graph_revision != 0
        || packet.attempt != 1
        || packet.idempotency_key != node.idempotency_key
        || packet.permission_lease != request.permission_lease
        || packet.model_lease != request.model_lease
        || packet.context_refs != request.context_refs
        || packet.evidence_refs != request.evidence_refs
        || packet.allowed_tools != request.allowed_tools
        || packet.allowed_skills != request.allowed_skills
        || !packet.constraints.contains(&expected_protocol)
        || !packet.constraints.contains(&expected_role)
        || !packet.constraints.contains(&expected_slot)
        || !backend_constraint_is_bound
    {
        return Err(invalid_graph(
            protocol,
            &format!(
                "agent node `{}` has stale or incomplete packet bindings",
                node.id
            ),
        ));
    }
    if packet.objective.trim().is_empty()
        || packet.run_id.trim().is_empty()
        || packet.agent_id.trim().is_empty()
        || packet.task_id.trim().is_empty()
        || node.resource_scopes != request.resource_scopes
        || node.acceptance.criteria != packet.acceptance
        || node.retry_policy.max_attempts != max_agent_attempts
    {
        return Err(invalid_graph(
            protocol,
            &format!(
                "agent node `{}` has an invalid canonical task contract",
                node.id
            ),
        ));
    }
    let expected_budget_id = format!("{}:{}", request.budget_lease_id, node.id);
    if packet.budget_lease.lease_id != expected_budget_id
        || packet.budget_lease.owner_id != packet.agent_id
        || packet.budget_lease.scope != "protocol_agent"
        || packet.budget_lease.max_tokens != request.budget_tokens
        || packet.budget_lease.revision != request.budget_revision
    {
        return Err(invalid_graph(
            protocol,
            &format!("agent node `{}` has an invalid budget lease", node.id),
        ));
    }
    for field in &role.output.required_fields {
        if !packet.acceptance.contains(field) {
            return Err(invalid_graph(
                protocol,
                &format!("agent node `{}` omits output field `{field}`", node.id),
            ));
        }
    }
    if role.output.evidence_required && !packet.acceptance.contains(&"evidence_backed".to_string())
    {
        return Err(invalid_graph(
            protocol,
            &format!("agent node `{}` omits evidence-backed output", node.id),
        ));
    }
    Ok(())
}

fn validate_output(
    protocol: &str,
    owner: &str,
    output: &super::OutputSpec,
) -> Result<(), ProtocolValidationError> {
    if output.required_fields.is_empty()
        || output
            .required_fields
            .iter()
            .any(|field| field.trim().is_empty())
    {
        return Err(invalid_spec(
            protocol,
            &format!("{owner} output has no complete required fields"),
        ));
    }
    let fields = output.required_fields.iter().collect::<BTreeSet<_>>();
    if fields.len() != output.required_fields.len() {
        return Err(invalid_spec(
            protocol,
            &format!("{owner} output repeats a required field"),
        ));
    }
    Ok(())
}

fn invalid_spec(protocol: &str, reason: &str) -> ProtocolValidationError {
    ProtocolValidationError::InvalidSpec {
        protocol: protocol.to_string(),
        reason: reason.to_string(),
    }
}

fn invalid_request(protocol: &str, reason: &str) -> ProtocolValidationError {
    ProtocolValidationError::InvalidRequest {
        protocol: protocol.to_string(),
        reason: reason.to_string(),
    }
}

fn invalid_graph(protocol: &str, reason: &str) -> ProtocolValidationError {
    ProtocolValidationError::InvalidGraph {
        protocol: protocol.to_string(),
        reason: reason.to_string(),
    }
}
