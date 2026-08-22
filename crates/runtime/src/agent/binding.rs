//! Runtime compilation of immutable Agent execution Bindings.
//!
//! Definition assets declare ceilings; a Binding is the per-run effective
//! intersection after the caller has supplied role, approval, resource and
//! data boundaries. The compiler is intentionally the only place that turns
//! a selector into an executable packet identity.

use std::collections::BTreeSet;

use harness_contract::agent::{
    AgentBindingSnapshot, AgentCapability, AgentDataLease, AgentDefinitionId,
    AgentEvaluationBinding, AgentInstanceRef, AgentReleaseBinding, AgentTaskIntent,
    AgentTaskPacket, DefinitionScope, RevisionSelector,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::agent::definition::ResolvedAgentDefinition;
use crate::AgentCatalogEntry;
use crate::RuntimeDefinitionRegistry;

/// Caller-supplied restrictions. Every value is a ceiling; the compiler never
/// expands a Definition's declared capability, Skill or cognitive scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentBindingRequest {
    pub definition_id: AgentDefinitionId,
    pub selector: RevisionSelector,
    pub instance_id: String,
    /// Typed semantic role id (`implementer`). It is never the display label
    /// and never carries a slot suffix.
    pub role_id: Option<String>,
    /// Typed 1-based slot index within the role.
    pub slot_index: Option<u32>,
    /// Typed focus partition id.
    pub focus: Option<String>,
    /// Legacy combined carrier kept only for durable decode/migration.
    pub role_slot_id: Option<String>,
    pub session_id: String,
    pub task_id: String,
    pub team_id: Option<String>,
    pub granted_capabilities: Vec<AgentCapability>,
    pub allowed_tool_contract_refs: Vec<String>,
    pub allowed_skill_refs: Vec<String>,
    pub fact_boundaries: Vec<String>,
    pub fact_refs: Vec<String>,
    pub matrix_snapshot_refs: Vec<String>,
    pub team_working_state_visible: bool,
}

impl AgentBindingRequest {
    /// A strict bounded request for a direct or delegated Agent task. Empty
    /// grant lists deliberately mean no permission, not "all permissions".
    #[must_use]
    pub fn new(
        definition_id: AgentDefinitionId,
        selector: RevisionSelector,
        instance_id: impl Into<String>,
        session_id: impl Into<String>,
        task_id: impl Into<String>,
    ) -> Self {
        Self {
            definition_id,
            selector,
            instance_id: instance_id.into(),
            role_id: None,
            slot_index: None,
            focus: None,
            role_slot_id: None,
            session_id: session_id.into(),
            task_id: task_id.into(),
            team_id: None,
            granted_capabilities: Vec::new(),
            allowed_tool_contract_refs: Vec::new(),
            allowed_skill_refs: Vec::new(),
            fact_boundaries: Vec::new(),
            fact_refs: Vec::new(),
            matrix_snapshot_refs: Vec::new(),
            team_working_state_visible: false,
        }
    }
}

/// Derive a bounded Binding request from a graph planning intent. The intent
/// is not executable: this function is only called by the Runtime compiler
/// before a graph is registered, so no worker can re-resolve a default later.
pub(crate) fn request_for_intent(
    intent: &AgentTaskIntent,
    catalog_entry: Option<AgentCatalogEntry>,
) -> Result<AgentBindingRequest, AgentBindingError> {
    let (definition_id, selector, granted_capabilities) =
        if let Some(definition_ref) = intent.definition_ref.as_ref() {
            (
                definition_ref.definition_id.clone(),
                RevisionSelector::ExactApprovedRevision {
                    revision: definition_ref.revision,
                },
                intent.granted_capabilities.clone(),
            )
        } else if let Some(entry) = catalog_entry {
            (
                entry.definition_ref.definition_id,
                // The catalog is an eligibility index, not a release pin.
                // Leaving the selector at latest Stable lets Runtime's
                // release router apply an approved Canary deterministically
                // and persist the exact resulting provenance in the Binding.
                // Explicit definition refs above remain exact pins.
                RevisionSelector::LatestApprovedStable,
                entry
                    .capabilities
                    .into_iter()
                    .filter_map(capability_from_name)
                    .collect::<Vec<_>>(),
            )
        } else if intent.selected_agent_id.is_some() {
            return Err(AgentBindingError::InvalidRequest(
            "selected Agent is absent from the Runtime catalog; no fallback Binding is permitted"
                .to_string(),
        ));
        } else {
            fallback_binding_identity(intent)?
        };
    let mut request = AgentBindingRequest::new(
        definition_id,
        selector,
        format!("instance:{}", intent.run_id),
        intent.session_id.clone(),
        intent.task_id.clone(),
    );
    let team_role = intent.team_role_identity.as_ref();
    if intent.team_id.is_some() && team_role.is_none() {
        return Err(AgentBindingError::InvalidRequest(
            "Team Agent intent must carry a typed Team role identity".to_string(),
        ));
    }
    if let Some(team_role) = team_role {
        team_role
            .validate()
            .map_err(|error| AgentBindingError::InvalidRequest(error.to_string()))?;
        request.role_id = Some(team_role.role_id.clone());
        request.slot_index = Some(team_role.slot);
        request.focus = Some(team_role.focus_id.clone());
    }
    request.team_id = intent.team_id.clone();
    request.granted_capabilities = granted_capabilities;
    request.allowed_tool_contract_refs = intent.allowed_tools.clone();
    request.allowed_skill_refs = intent.allowed_skills.clone();
    request.fact_refs = intent
        .context_refs
        .iter()
        .filter(|reference| reference.starts_with("fact:"))
        .cloned()
        .collect();
    request.fact_boundaries = intent
        .context_refs
        .iter()
        .filter_map(|reference| reference.strip_prefix("fact_boundary:").map(str::to_string))
        .collect();
    request.matrix_snapshot_refs = intent
        .context_refs
        .iter()
        .filter(|reference| reference.starts_with("matrix:source_snapshot:"))
        .cloned()
        .collect();
    request.team_working_state_visible = intent.team_id.is_some();
    Ok(request)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledAgentBinding {
    pub snapshot: AgentBindingSnapshot,
    pub selected_by: RevisionSelector,
}

#[derive(Debug, Error)]
pub enum AgentBindingError {
    #[error(transparent)]
    Definition(#[from] crate::DefinitionRegistryError),
    #[error("binding request is invalid: {0}")]
    InvalidRequest(String),
    #[error("binding capability ceiling removes every capability")]
    EmptyEffectiveCapability,
    #[error("requested Skill `{0}` is not declared by the Definition")]
    UndeclaredSkill(String),
    #[error("resolved Definition instructions do not match its content digest")]
    InstructionDigestMismatch,
    #[error("compiled Binding is invalid: {0}")]
    InvalidBinding(String),
}

/// Runtime-owned compiler. It accepts only the scoped Registry resolver, not
/// a filesystem root, preventing packet construction from rediscovering or
/// shadowing Definition assets.
#[derive(Debug, Clone)]
pub struct AgentBindingCompiler {
    registry: std::sync::Arc<RuntimeDefinitionRegistry>,
}

impl AgentBindingCompiler {
    #[must_use]
    pub fn new(registry: std::sync::Arc<RuntimeDefinitionRegistry>) -> Self {
        Self { registry }
    }

    pub fn compile(
        &self,
        request: AgentBindingRequest,
    ) -> Result<CompiledAgentBinding, AgentBindingError> {
        validate_request(&request)?;
        let resolved = self
            .registry
            .resolve_agent(&request.definition_id, request.selector.clone())?;
        self.compile_resolved(request, resolved, None)
    }

    /// Compile from a Definition resolved by Runtime release routing. This is
    /// crate-visible so a Canary revision can only reach execution after the
    /// Runtime governance ledger has selected it; Gateway and surfaces never
    /// receive a direct unreleased-Definition compilation path.
    pub(crate) fn compile_resolved(
        &self,
        request: AgentBindingRequest,
        resolved: ResolvedAgentDefinition,
        release: Option<AgentReleaseBinding>,
    ) -> Result<CompiledAgentBinding, AgentBindingError> {
        validate_request(&request)?;
        let manifest = &resolved.revision.manifest;
        let normalized_instructions = normalize_instructions(&resolved.agent_markdown);
        if digest(&normalized_instructions) != manifest.instructions_digest {
            return Err(AgentBindingError::InstructionDigestMismatch);
        }
        let definition_capabilities = manifest
            .capability_contract
            .capability_ceiling
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let requested_capabilities = request
            .granted_capabilities
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let effective_capabilities = definition_capabilities
            .intersection(&requested_capabilities)
            .copied()
            .collect::<Vec<_>>();
        if effective_capabilities.is_empty() {
            return Err(AgentBindingError::EmptyEffectiveCapability);
        }
        let declared_skills = manifest
            .capability_contract
            .skill_refs
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for skill_ref in &request.allowed_skill_refs {
            if !declared_skills.contains(skill_ref) {
                return Err(AgentBindingError::UndeclaredSkill(skill_ref.clone()));
            }
        }
        let mut skill_refs = request.allowed_skill_refs;
        skill_refs.sort();
        skill_refs.dedup();
        let mut tool_contract_refs = request.allowed_tool_contract_refs;
        tool_contract_refs.sort();
        tool_contract_refs.dedup();
        for tool_ref in &tool_contract_refs {
            let required = capability_required_by_tool_contract(tool_ref);
            if !effective_capabilities.contains(&required) {
                return Err(AgentBindingError::InvalidRequest(format!(
                    "tool contract `{tool_ref}` requires capability `{}` outside the effective grant",
                    required.as_str()
                )));
            }
        }
        let mut fact_boundaries = request.fact_boundaries;
        fact_boundaries.sort();
        fact_boundaries.dedup();
        let mut fact_refs = request.fact_refs;
        fact_refs.sort();
        fact_refs.dedup();
        let mut matrix_snapshot_refs = request.matrix_snapshot_refs;
        matrix_snapshot_refs.sort();
        matrix_snapshot_refs.dedup();
        let data_lease = AgentDataLease {
            session_id: request.session_id,
            task_id: request.task_id,
            team_id: request.team_id,
            read_scopes: manifest.cognitive_policy.read_scopes.clone(),
            write_mode: manifest.cognitive_policy.write_mode,
            team_working_state_visible: manifest.cognitive_policy.team_working_state_visible
                && request.team_working_state_visible,
            fact_boundaries,
            fact_refs,
            matrix_snapshot_refs,
        };
        let role_slot_id = match (&request.role_id, request.slot_index) {
            (Some(role_id), Some(slot)) => Some(format!("{role_id}:{slot}")),
            (Some(role_id), None) => Some(role_id.to_string()),
            _ => request.role_slot_id.clone(),
        };
        let instance = AgentInstanceRef {
            instance_id: request.instance_id,
            role_slot_id,
        };
        let binding_id = binding_id(
            &resolved.revision.revision_ref,
            &resolved.revision.content_digest,
            &instance,
            &data_lease,
        );
        let mut snapshot = AgentBindingSnapshot {
            binding_id,
            definition_ref: resolved.revision.revision_ref,
            definition_digest: resolved.revision.content_digest,
            instructions: normalized_instructions,
            instance,
            executor: manifest.executor.clone(),
            model_policy: manifest.model_policy.clone(),
            effective_capabilities,
            skill_refs,
            tool_contract_refs,
            data_lease,
            release,
            evaluation: None,
            display: None,
            binding_digest: String::new(),
        };
        snapshot.binding_digest = digest(
            &serde_json::to_string(&snapshot)
                .map_err(|error| AgentBindingError::InvalidBinding(error.to_string()))?,
        );
        snapshot
            .validate()
            .map_err(|error| AgentBindingError::InvalidBinding(error.to_string()))?;
        Ok(CompiledAgentBinding {
            snapshot,
            selected_by: resolved.selected_by,
        })
    }

    /// Compile a Runtime-authorized isolated evaluation Binding. Evaluation
    /// provenance is distinct from release routing: it permits the exact
    /// published candidate revision to run only after Runtime validates the
    /// candidate/scenario pair, and never makes that revision selectable by
    /// a normal Stable/default Binding.
    pub(crate) fn compile_evaluation_resolved(
        &self,
        request: AgentBindingRequest,
        resolved: ResolvedAgentDefinition,
        evaluation: AgentEvaluationBinding,
    ) -> Result<CompiledAgentBinding, AgentBindingError> {
        let mut compiled = self.compile_resolved(request, resolved, None)?;
        compiled.snapshot.evaluation = Some(evaluation);
        compiled.snapshot.binding_digest = digest(
            &serde_json::to_string(&compiled.snapshot)
                .map_err(|error| AgentBindingError::InvalidBinding(error.to_string()))?,
        );
        compiled
            .snapshot
            .validate()
            .map_err(|error| AgentBindingError::InvalidBinding(error.to_string()))?;
        Ok(compiled)
    }

    /// Compile a graph planning intent into the sole executable task packet.
    /// The selected catalog identity remains compiler input only; the output
    /// `agent_id` is always the immutable Binding instance identity.
    pub fn compile_task_intent(
        &self,
        intent: AgentTaskIntent,
        catalog_entry: Option<AgentCatalogEntry>,
        execution_identity: harness_contract::execution::ExecutionIdentity,
    ) -> Result<AgentTaskPacket, AgentBindingError> {
        let request = request_for_intent(&intent, catalog_entry)?;
        let typed_role_id = request.role_id.clone();
        let compiled = self.compile(request)?;
        let mut packet = compiled
            .snapshot
            .compile_task_packet(intent, execution_identity)
            .map_err(|error| AgentBindingError::InvalidBinding(error.to_string()))?;
        if let Some(role_id) = typed_role_id {
            // The Binding instance carries the combined `role:slot` identity;
            // the executable assignment keeps the typed semantic role id
            // separate from its slot, exactly as D1 requires.
            packet.assignment.role_id = role_id;
        }
        Ok(packet)
    }
}

fn validate_request(request: &AgentBindingRequest) -> Result<(), AgentBindingError> {
    if request.instance_id.trim().is_empty()
        || request.session_id.trim().is_empty()
        || request.task_id.trim().is_empty()
    {
        return Err(AgentBindingError::InvalidRequest(
            "instance_id, session_id, and task_id are required".to_string(),
        ));
    }
    if request
        .granted_capabilities
        .iter()
        .collect::<BTreeSet<_>>()
        .len()
        != request.granted_capabilities.len()
    {
        return Err(AgentBindingError::InvalidRequest(
            "granted capabilities must be unique".to_string(),
        ));
    }
    for (field, values) in [
        ("tool contract", &request.allowed_tool_contract_refs),
        ("skill", &request.allowed_skill_refs),
        ("fact boundary", &request.fact_boundaries),
        ("fact", &request.fact_refs),
        ("matrix snapshot", &request.matrix_snapshot_refs),
    ] {
        if values.iter().any(|value| value.trim().is_empty()) {
            return Err(AgentBindingError::InvalidRequest(format!(
                "{field} references cannot be blank"
            )));
        }
    }
    if request.fact_boundaries.iter().any(|boundary| {
        !matches!(
            boundary.as_str(),
            "observed" | "inferred" | "hypothetical" | "conflict"
        )
    }) {
        return Err(AgentBindingError::InvalidRequest(
            "Fact boundaries must be observed, inferred, hypothetical, or conflict".to_string(),
        ));
    }
    if request
        .fact_refs
        .iter()
        .any(|reference| !reference.starts_with("fact:"))
    {
        return Err(AgentBindingError::InvalidRequest(
            "Fact references must use the `fact:` prefix".to_string(),
        ));
    }
    if request
        .matrix_snapshot_refs
        .iter()
        .any(|reference| !reference.starts_with("matrix:source_snapshot:"))
    {
        return Err(AgentBindingError::InvalidRequest(
            "Matrix snapshot references must use the `matrix:source_snapshot:` prefix".to_string(),
        ));
    }
    Ok(())
}

fn binding_id(
    definition_ref: &harness_contract::agent::AgentDefinitionRevisionRef,
    definition_digest: &str,
    instance: &AgentInstanceRef,
    lease: &AgentDataLease,
) -> String {
    let canonical = format!(
        "{}@{}:{}:{}:{}:{}:{}",
        definition_ref.definition_id.as_str(),
        definition_ref.revision,
        definition_digest,
        instance.instance_id,
        instance.role_slot_id.as_deref().unwrap_or_default(),
        lease.session_id,
        lease.task_id,
    );
    format!("binding:{}", digest(&canonical))
}

fn normalize_instructions(value: &str) -> String {
    value.replace("\r\n", "\n").trim_end().to_string() + "\n"
}

fn digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

/// Recompute the immutable Binding digest after the frozen Team-slot display
/// identity is attached. The digest always covers the exact serialized
/// Binding bytes so validation can never diverge from the persisted packet.
pub(crate) fn recompute_binding_digest(
    snapshot: &AgentBindingSnapshot,
) -> Result<String, serde_json::Error> {
    let mut unsigned = snapshot.clone();
    unsigned.binding_digest.clear();
    Ok(digest(&serde_json::to_string(&unsigned)?))
}

pub(crate) fn capability_required_by_tool_contract(tool_ref: &str) -> AgentCapability {
    let lower = tool_ref.to_ascii_lowercase();
    if lower.contains("connector") || lower.contains("channel") {
        AgentCapability::ConnectorAction
    } else if lower.contains("matrix") {
        AgentCapability::MatrixWrite
    } else if lower.contains("network")
        || lower.contains("http")
        || lower.contains("fetch")
        || lower.contains("download")
    {
        AgentCapability::Network
    } else if lower.contains("write")
        || lower.contains("edit")
        || lower.contains("patch")
        || lower.contains("bash")
        || lower.contains("shell")
        || lower.contains("rename")
        || lower.contains("delete")
        || lower.contains("mkdir")
    {
        AgentCapability::Write
    } else if lower.contains("test") || lower.contains("check") || lower.contains("build") {
        AgentCapability::Test
    } else if lower.contains("search") || lower.contains("grep") || lower == "toolsearch" {
        AgentCapability::Search
    } else {
        AgentCapability::Read
    }
}

fn capability_from_name(value: String) -> Option<AgentCapability> {
    match value.as_str() {
        "read" => Some(AgentCapability::Read),
        "search" => Some(AgentCapability::Search),
        "write" => Some(AgentCapability::Write),
        "test" => Some(AgentCapability::Test),
        "network" => Some(AgentCapability::Network),
        "connector_action" => Some(AgentCapability::ConnectorAction),
        "matrix_write" => Some(AgentCapability::MatrixWrite),
        _ => None,
    }
}

fn fallback_binding_identity(
    intent: &AgentTaskIntent,
) -> Result<(AgentDefinitionId, RevisionSelector, Vec<AgentCapability>), AgentBindingError> {
    let requires_execute = intent.allowed_tools.iter().any(|tool| {
        let lower = tool.to_ascii_lowercase();
        ["write", "edit", "patch", "bash", "shell", "test"]
            .iter()
            .any(|keyword| lower.contains(keyword))
    });
    let requires_explore = !requires_execute
        && (!intent.allowed_tools.is_empty()
            || intent
                .constraints
                .iter()
                .any(|constraint| constraint.contains("evidence")));
    let (local_id, capabilities) = if requires_execute {
        (
            "cowd/execute",
            vec![
                AgentCapability::Read,
                AgentCapability::Search,
                AgentCapability::Write,
                AgentCapability::Test,
            ],
        )
    } else if requires_explore {
        (
            "cowd/explore",
            vec![AgentCapability::Read, AgentCapability::Search],
        )
    } else {
        ("cowd/direct", vec![AgentCapability::Read])
    };
    Ok((
        AgentDefinitionId::new(DefinitionScope::Builtin, local_id)
            .map_err(|error| AgentBindingError::InvalidRequest(error.to_string()))?,
        RevisionSelector::LatestApprovedStable,
        capabilities,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use harness_contract::agent::{
        AgentCapability, AgentDefinitionId, AgentTaskIntent, DefinitionScope, RevisionSelector,
    };
    use harness_contract::context::ChildExecutionBudgetReservation;
    use harness_contract::policy::PermissionMode;
    use harness_contract::team::TeamRoleIdentity;
    use tempfile::TempDir;

    use super::*;

    fn registry(temp: &TempDir) -> Arc<RuntimeDefinitionRegistry> {
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        Arc::new(
            RuntimeDefinitionRegistry::from_storage_layout(
                &storage::StorageLayout::default_for_config_home(temp.path().join("home")),
                temp.path().join("builtin"),
                workspace,
            )
            .expect("registry"),
        )
    }

    fn team_intent() -> AgentTaskIntent {
        AgentTaskIntent {
            selected_agent_id: None,
            definition_ref: None,
            granted_capabilities: Vec::new(),
            principal_id: "test".to_string(),
            source_turn_id: "turn-1".to_string(),
            run_id: "run-1".to_string(),
            task_id: "task-1".to_string(),
            root_task_id: "task-root-1".to_string(),
            parent_task_id: None,
            session_id: "session-1".to_string(),
            mission_id: "mission-1".to_string(),
            team_id: Some("team-1".to_string()),
            graph_id: "graph-1".to_string(),
            node_id: "node-1".to_string(),
            attempt: 1,
            expected_graph_revision: 1,
            objective: "typed role slot".to_string(),
            team_role_identity: Some(team_identity("implementer", 1, "focus-default")),
            required_acceptance: Default::default(),
            output_acceptance: Vec::new(),
            acceptance: Vec::new(),
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: PermissionMode::ReadOnly,
            model_lease: "model".to_string(),
            budget_lease: ChildExecutionBudgetReservation::single(
                "budget",
                "agent",
                "team",
                100,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            managed_invocation: None,
            idempotency_key: "agent:1".to_string(),
        }
    }

    fn team_identity(role_id: &str, slot: u32, focus_id: &str) -> TeamRoleIdentity {
        TeamRoleIdentity {
            role_id: role_id.to_string(),
            slot,
            focus_id: focus_id.to_string(),
            focus_boundary: "fixture role-local boundary".to_string(),
            evidence_responsibility: "fixture evidence".to_string(),
            focus_scope_hash: "a".repeat(64),
            overlap_budget_bp: 0,
            novelty_target_bp: 0,
            output_acceptance: Vec::new(),
        }
    }

    #[test]
    fn compiles_exact_immutable_binding_and_rejects_capability_escalation() {
        let temp = TempDir::new().expect("temporary root");
        let compiler = AgentBindingCompiler::new(registry(&temp));
        let definition_id =
            AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/execute").expect("builtin id");
        let mut request = AgentBindingRequest::new(
            definition_id,
            RevisionSelector::LatestApprovedStable,
            "instance-1",
            "session-1",
            "task-1",
        );
        request.granted_capabilities = vec![AgentCapability::Read, AgentCapability::Write];
        request.allowed_tool_contract_refs = vec!["tool/read_file".to_string()];
        request.team_working_state_visible = true;
        let compiled = compiler.compile(request.clone()).expect("binding");
        assert_eq!(compiled.snapshot.definition_ref.revision, 1);
        assert_eq!(
            compiled.snapshot.effective_capabilities,
            vec![AgentCapability::Read, AgentCapability::Write]
        );
        assert!(compiled.snapshot.binding_id.starts_with("binding:"));
        assert_eq!(compiled.snapshot.data_lease.session_id, "session-1");
        assert!(!compiled.snapshot.instructions.is_empty());

        let mut escalated_tool = request.clone();
        escalated_tool.granted_capabilities = vec![AgentCapability::Read];
        escalated_tool.allowed_tool_contract_refs = vec!["write_file".to_string()];
        assert!(matches!(
            compiler.compile(escalated_tool),
            Err(AgentBindingError::InvalidRequest(_))
        ));

        request.granted_capabilities = vec![AgentCapability::ConnectorAction];
        assert!(matches!(
            compiler.compile(request),
            Err(AgentBindingError::EmptyEffectiveCapability)
        ));
    }

    #[test]
    fn typed_role_slot_focus_flow_into_the_binding_without_display_guessing() {
        let temp = TempDir::new().expect("temporary root");
        let compiler = AgentBindingCompiler::new(registry(&temp));
        let mut intent = team_intent();
        intent.team_role_identity = Some(team_identity("implementer", 2, "focus-alpha"));
        intent.constraints = vec![
            "team_role:wrong-legacy-role".to_string(),
            "role_slot:wrong-legacy-role:99".to_string(),
            "focus_partition:wrong-legacy-focus".to_string(),
        ];
        let request = request_for_intent(&intent, None).expect("typed request");
        assert_eq!(request.role_id.as_deref(), Some("implementer"));
        assert_eq!(request.slot_index, Some(2));
        assert_eq!(request.focus.as_deref(), Some("focus-alpha"));

        let compiled = compiler.compile(request).expect("binding");
        assert_eq!(
            compiled.snapshot.instance.role_slot_id.as_deref(),
            Some("implementer:2")
        );
        assert!(
            !compiled
                .snapshot
                .instance
                .role_slot_id
                .as_deref()
                .is_some_and(|value| value.contains("focus-alpha")),
            "focus must never leak into the semantic role identity"
        );
    }

    #[test]
    fn legacy_constraint_role_identity_cannot_override_typed_binding() {
        let mut intent = team_intent();
        intent.constraints = vec![
            "team_role:implementer".to_string(),
            "role_slot:reviewer:1".to_string(),
        ];
        let request = request_for_intent(&intent, None).expect("typed identity wins");
        assert_eq!(request.role_id.as_deref(), Some("implementer"));
        assert_eq!(request.slot_index, Some(1));
    }
}
