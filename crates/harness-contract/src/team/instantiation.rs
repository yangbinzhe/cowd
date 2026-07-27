//! Runtime-owned Team instantiation intent.
//!
//! A Surface or model may request a Team, but it never supplies an execution
//! graph. Runtime resolves the selected template revision, Agent revisions,
//! role cardinalities, focus partitions, and all effective leases before any
//! AgentTask node becomes durable.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::agent::{AgentCapability, AgentDefinitionRevisionRef, ValidationError};
use crate::context::ContextBudgetLeaseRef;
use crate::core::TaskRisk;
use crate::execution_graph::ExecutionParentBinding;

use super::{RoleCardinalityPolicy, TeamTemplateDefinitionId, TeamTemplateRevisionRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamSelectionMode {
    Explicit,
    ModelAssisted,
    Automatic,
}

/// Runtime-executable acceptance checks for a Team role slot. Free-form
/// acceptance text is never interpreted with keywords or substring matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamAcceptanceRequirement {
    pub criterion: String,
    pub check: TeamAcceptanceCheck,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TeamAcceptanceCheck {
    StructuredField {
        field: TeamStructuredOutputField,
    },
    ScopedEvidence {
        scopes: Vec<String>,
    },
    WorkspaceChange {
        field: TeamStructuredOutputField,
        scopes: Vec<String>,
    },
    SourceVerification {
        scopes: Vec<String>,
    },
    UpstreamReview,
    /// Pure reducer role: consume predecessor durable evidence without
    /// repeating source/tool acquisition.
    UpstreamEvidence,
    /// Rolling-safe adapter for a pre-typed custom Team contract. The exact
    /// legacy criterion must be returned under `legacy_acceptance` and is
    /// accepted only with scoped durable evidence.
    LegacyEvidenceBound {
        scopes: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamStructuredOutputField {
    Summary,
    Findings,
    Plan,
    Implementation,
    SourceVerification,
    Review,
    Risks,
    Unresolved,
    Proposal,
    Critique,
    Mitigation,
    Checkpoint,
}

impl TeamStructuredOutputField {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Summary => "summary",
            Self::Findings => "findings",
            Self::Plan => "plan",
            Self::Implementation => "implementation",
            Self::SourceVerification => "source_verification",
            Self::Review => "review",
            Self::Risks => "risks",
            Self::Unresolved => "unresolved",
            Self::Proposal => "proposal",
            Self::Critique => "critique",
            Self::Mitigation => "mitigation",
            Self::Checkpoint => "checkpoint",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TeamTemplateSelector {
    Exact {
        revision_ref: TeamTemplateRevisionRef,
    },
    LatestStable {
        template_id: TeamTemplateDefinitionId,
    },
    Default {
        template_id: TeamTemplateDefinitionId,
    },
    Automatic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRoleBindingOverride {
    pub role_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_ref: Option<AgentDefinitionRevisionRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grant_ceiling: Vec<AgentCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRoleCardinalityOverride {
    pub role_id: String,
    pub cardinality: RoleCardinalityPolicy,
}

/// One non-overlapping responsibility assigned to a Team role slot.
///
/// This is deliberately declarative: a model or a human can propose the
/// partition, while Runtime validates it before any Agent instance is
/// created.  It never embeds a prompt, a graph node, or mutable Agent state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FocusPartitionSlot {
    pub focus_id: String,
    pub boundary: String,
    pub evidence_responsibility: String,
    /// Opaque evidence references after the role capability lease has cropped
    /// inaccessible resources. Absolute workspace paths are forbidden.
    #[serde(default)]
    pub capability_cropped_refs: Vec<String>,
    /// Stable digest of boundary plus cropped evidence set.
    #[serde(default)]
    pub scope_hash: String,
    /// Maximum accepted overlap with another slot, in basis points.
    #[serde(default)]
    pub overlap_budget_bp: u16,
    /// Minimum new evidence expected from this slot, in basis points.
    #[serde(default)]
    pub novelty_target_bp: u16,
    #[serde(default)]
    pub output_contract: Vec<String>,
    #[serde(default)]
    pub output_acceptance: Vec<String>,
}

/// Validated per-role work partition for a Team instantiation.
///
/// A plan makes the collaboration boundary inspectable and reproducible. It
/// is not a second scheduler: resolved slots still belong exclusively to the
/// ExecutionGraph created by Runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FocusPartitionPlan {
    pub role_id: String,
    #[serde(default)]
    pub shared_baseline: Vec<String>,
    pub slots: Vec<FocusPartitionSlot>,
}

/// Canonical identity for one capability-cropped focus lease.
///
/// References are treated as a set so callers cannot change identity through
/// ordering or duplicate values. Runtime recomputes this digest at every
/// trust boundary; a caller-supplied arbitrary label is never authoritative.
#[must_use]
pub fn focus_scope_hash(
    role_id: &str,
    boundary: &str,
    capability_cropped_refs: &[String],
) -> String {
    let mut references = capability_cropped_refs.to_vec();
    references.sort();
    references.dedup();
    let mut hasher = Sha256::new();
    hasher.update(role_id.trim().as_bytes());
    hasher.update([0]);
    hasher.update(boundary.trim().as_bytes());
    for reference in references {
        hasher.update([0]);
        hasher.update(reference.trim().as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamStrategyBinding {
    pub decision_id: String,
    pub decision_revision: u64,
    pub decision_lease: String,
    pub turn_ref: String,
}

/// Complete Team request accepted by Runtime.  All fields are declarative
/// ceilings or user intent.  It intentionally has no graph nodes, executor
/// names, mutable Agent runtime IDs, or surface-owned context payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamInstantiationRequest {
    pub request_id: String,
    pub team_id: String,
    pub session_id: String,
    pub mission_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution: Option<ExecutionParentBinding>,
    pub selection_mode: TeamSelectionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy_binding: Option<TeamStrategyBinding>,
    pub template_selector: TeamTemplateSelector,
    pub objective: String,
    #[serde(default)]
    pub acceptance: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk: Option<TaskRisk>,
    #[serde(default)]
    pub role_binding_overrides: Vec<TeamRoleBindingOverride>,
    #[serde(default)]
    pub cardinality_overrides: Vec<TeamRoleCardinalityOverride>,
    #[serde(default)]
    pub focus_partition_plans: Vec<FocusPartitionPlan>,
    pub permission_lease: String,
    pub model_lease: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_lease: Option<ContextBudgetLeaseRef>,
    /// Runtime-issued lifecycle fence inherited by every Agent task in a
    /// Managed Team invocation.  Ordinary Team requests leave this empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_invocation: Option<crate::managed_agent::ManagedAgentInvocationFence>,
    #[serde(default)]
    pub resource_scopes: Vec<String>,
}

impl TeamInstantiationRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        for (field, value) in [
            ("team.request_id", &self.request_id),
            ("team.team_id", &self.team_id),
            ("team.session_id", &self.session_id),
            ("team.mission_id", &self.mission_id),
            ("team.objective", &self.objective),
            ("team.permission_lease", &self.permission_lease),
            ("team.model_lease", &self.model_lease),
        ] {
            if value.trim().is_empty() {
                return Err(ValidationError::MissingField {
                    field: field.to_string(),
                });
            }
        }
        validate_unique_role_ids(
            "team.role_binding_overrides",
            self.role_binding_overrides
                .iter()
                .map(|override_| &override_.role_id),
        )?;
        validate_unique_role_ids(
            "team.cardinality_overrides",
            self.cardinality_overrides
                .iter()
                .map(|override_| &override_.role_id),
        )?;
        validate_unique_role_ids(
            "team.focus_partition_plans",
            self.focus_partition_plans.iter().map(|plan| &plan.role_id),
        )?;
        for override_ in &self.cardinality_overrides {
            override_.cardinality.validate()?;
        }
        match (&self.selection_mode, &self.template_selector) {
            (TeamSelectionMode::Explicit, TeamTemplateSelector::Automatic) => {
                return Err(ValidationError::InvalidContract {
                    message: "explicit Team selection requires an exact, latest-stable, or default template selector"
                        .to_string(),
                });
            }
            (TeamSelectionMode::Automatic, selector)
                if !matches!(selector, TeamTemplateSelector::Automatic) =>
            {
                return Err(ValidationError::InvalidContract {
                    message:
                        "automatic Team selection must not smuggle an explicit template selector"
                            .to_string(),
                });
            }
            _ => {}
        }
        if self.selection_mode == TeamSelectionMode::Automatic {
            let binding =
                self.strategy_binding
                    .as_ref()
                    .ok_or_else(|| ValidationError::MissingField {
                        field: "team.strategy_binding".to_string(),
                    })?;
            for (field, value) in [
                ("team.strategy_binding.decision_id", &binding.decision_id),
                (
                    "team.strategy_binding.decision_lease",
                    &binding.decision_lease,
                ),
                ("team.strategy_binding.turn_ref", &binding.turn_ref),
            ] {
                if value.trim().is_empty() {
                    return Err(ValidationError::MissingField {
                        field: field.to_string(),
                    });
                }
            }
            if binding.decision_revision == 0 {
                return Err(ValidationError::InvalidContract {
                    message: "automatic Team strategy binding revision must be positive"
                        .to_string(),
                });
            }
        }
        if let Some(managed_invocation) = &self.managed_invocation {
            managed_invocation.validate()?;
        }
        if self.resource_scopes.iter().any(|scope| {
            matches!(
                scope.as_str(),
                "workspace" | "read:." | "write:." | "worktree:."
            ) || scope.starts_with('/')
                || scope.contains(":\\")
                || scope.split('/').any(|part| part == "..")
                || scope.split_once(':').is_some_and(|(mode, path)| {
                    matches!(mode, "read" | "write" | "worktree") && {
                        let path = path.trim().replace('\\', "/");
                        path.is_empty()
                            || path == "."
                            || path.starts_with('/')
                            || path.split('/').any(|part| part == "..")
                    }
                })
        }) {
            return Err(ValidationError::InvalidContract {
                message: "Team resource scopes must be bounded relative leases; whole-workspace, absolute, and traversing scopes are forbidden"
                    .to_string(),
            });
        }
        if !self
            .resource_scopes
            .iter()
            .any(|scope| scope.starts_with("read:") || scope.starts_with("write:"))
        {
            return Err(ValidationError::InvalidContract {
                message: "Team execution requires at least one Runtime-cropped read or write resource lease"
                    .to_string(),
            });
        }
        for plan in &self.focus_partition_plans {
            if plan.role_id.trim().is_empty() || plan.slots.is_empty() {
                return Err(ValidationError::InvalidContract {
                    message: "focus partition plans require a role and one or more slots"
                        .to_string(),
                });
            }
            validate_unique_non_empty(
                "team.focus_partition_plans.shared_baseline",
                &plan.shared_baseline,
            )?;
            let mut focus_ids = BTreeSet::new();
            let mut boundaries = BTreeSet::new();
            let mut scope_hashes = BTreeSet::new();
            for slot in &plan.slots {
                if slot.focus_id.trim().is_empty()
                    || slot.boundary.trim().is_empty()
                    || slot.evidence_responsibility.trim().is_empty()
                    || slot.scope_hash.trim().is_empty()
                    || slot.novelty_target_bp > 10_000
                    || slot.overlap_budget_bp > 10_000
                    || slot
                        .output_contract
                        .iter()
                        .any(|item| item.trim().is_empty())
                    || slot
                        .output_acceptance
                        .iter()
                        .any(|item| item.trim().is_empty())
                {
                    return Err(ValidationError::InvalidContract {
                        message: "every focus partition slot requires focus, boundary, evidence responsibility, scope hash, bounded overlap/novelty values, and non-empty output entries"
                            .to_string(),
                    });
                }
                let expected_scope_hash =
                    focus_scope_hash(&plan.role_id, &slot.boundary, &slot.capability_cropped_refs);
                if slot.scope_hash != expected_scope_hash {
                    return Err(ValidationError::InvalidContract {
                        message: format!(
                            "focus `{}` scope hash does not match its role, boundary, and capability-cropped refs",
                            slot.focus_id
                        ),
                    });
                }
                if slot.capability_cropped_refs.iter().any(|reference| {
                    reference.starts_with('/')
                        || reference.contains(":\\")
                        || reference.split('/').any(|part| part == "..")
                }) {
                    return Err(ValidationError::InvalidContract {
                        message:
                            "focus partition evidence must use capability-cropped opaque refs, not absolute or traversing paths"
                                .to_string(),
                    });
                }
                if !focus_ids.insert(slot.focus_id.as_str()) {
                    return Err(ValidationError::DuplicateValue {
                        field: "team.focus_partition_plans.slots.focus_id".to_string(),
                        value: slot.focus_id.clone(),
                    });
                }
                if !boundaries.insert(slot.boundary.as_str()) {
                    return Err(ValidationError::DuplicateValue {
                        field: "team.focus_partition_plans.slots.boundary".to_string(),
                        value: slot.boundary.clone(),
                    });
                }
                if !scope_hashes.insert(slot.scope_hash.as_str()) {
                    return Err(ValidationError::DuplicateValue {
                        field: "team.focus_partition_plans.slots.scope_hash".to_string(),
                        value: slot.scope_hash.clone(),
                    });
                }
                validate_unique_non_empty(
                    "team.focus_partition_plans.slots.capability_cropped_refs",
                    &slot.capability_cropped_refs,
                )?;
                validate_unique_non_empty(
                    "team.focus_partition_plans.slots.output_contract",
                    &slot.output_contract,
                )?;
                validate_unique_non_empty(
                    "team.focus_partition_plans.slots.output_acceptance",
                    &slot.output_acceptance,
                )?;
            }
            for (left_index, left) in plan.slots.iter().enumerate() {
                for right in plan.slots.iter().skip(left_index + 1) {
                    let left_shared = left
                        .capability_cropped_refs
                        .iter()
                        .filter(|reference| {
                            right
                                .capability_cropped_refs
                                .iter()
                                .any(|other| capability_scopes_overlap(reference, other))
                        })
                        .count();
                    let right_shared = right
                        .capability_cropped_refs
                        .iter()
                        .filter(|reference| {
                            left.capability_cropped_refs
                                .iter()
                                .any(|other| capability_scopes_overlap(reference, other))
                        })
                        .count();
                    let shared = left_shared.max(right_shared);
                    let union = left
                        .capability_cropped_refs
                        .len()
                        .saturating_add(right.capability_cropped_refs.len())
                        .saturating_sub(shared);
                    let overlap_bp = if union == 0 {
                        0
                    } else {
                        u16::try_from(shared.saturating_mul(10_000) / union).unwrap_or(10_000)
                    };
                    if overlap_bp > left.overlap_budget_bp.min(right.overlap_budget_bp) {
                        return Err(ValidationError::InvalidContract {
                            message: format!(
                                "focus partitions `{}` and `{}` overlap {overlap_bp}bp above their shared budget",
                                left.focus_id, right.focus_id
                            ),
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

fn capability_scopes_overlap(left: &str, right: &str) -> bool {
    let parse = |scope: &str| {
        let (mode, path) = scope.split_once(':')?;
        let path = path.trim().replace('\\', "/");
        if path.starts_with('/') {
            return None;
        }
        let mut components = Vec::new();
        for component in path.split('/') {
            match component {
                "" | "." => {}
                ".." => return None,
                value if value.contains(':') => return None,
                value => components.push(value),
            }
        }
        (!components.is_empty()).then_some((mode.to_string(), components.join("/")))
    };
    let Some((left_mode, left_path)) = parse(left) else {
        return left == right;
    };
    let Some((right_mode, right_path)) = parse(right) else {
        return left == right;
    };
    let workspace_mode = |mode: &str| matches!(mode, "read" | "write" | "workspace");
    if !workspace_mode(&left_mode) || !workspace_mode(&right_mode) {
        return left == right;
    }
    let contains = |ancestor: &str, descendant: &str| {
        descendant == ancestor
            || descendant
                .strip_prefix(ancestor)
                .is_some_and(|suffix| suffix.starts_with('/'))
    };
    contains(&left_path, &right_path) || contains(&right_path, &left_path)
}

fn validate_unique_non_empty(field: &str, values: &[String]) -> Result<(), ValidationError> {
    let mut seen = BTreeSet::new();
    for value in values {
        if value.trim().is_empty() {
            return Err(ValidationError::MissingField {
                field: field.to_string(),
            });
        }
        if !seen.insert(value.as_str()) {
            return Err(ValidationError::DuplicateValue {
                field: field.to_string(),
                value: value.clone(),
            });
        }
    }
    Ok(())
}

fn validate_unique_role_ids<'a>(
    field: &str,
    values: impl Iterator<Item = &'a String>,
) -> Result<(), ValidationError> {
    let mut seen = BTreeSet::new();
    for value in values {
        if value.trim().is_empty() {
            return Err(ValidationError::MissingField {
                field: field.to_string(),
            });
        }
        if !seen.insert(value) {
            return Err(ValidationError::DuplicateValue {
                field: field.to_string(),
                value: value.clone(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::DefinitionScope;

    fn request() -> TeamInstantiationRequest {
        TeamInstantiationRequest {
            request_id: "request-1".to_string(),
            team_id: "team-1".to_string(),
            session_id: "session-1".to_string(),
            mission_id: "mission-1".to_string(),
            parent_execution: None,
            selection_mode: TeamSelectionMode::Explicit,
            strategy_binding: None,
            template_selector: TeamTemplateSelector::LatestStable {
                template_id: TeamTemplateDefinitionId::new(
                    DefinitionScope::Builtin,
                    "cowd/execute-review",
                )
                .expect("template id"),
            },
            objective: "review implementation".to_string(),
            acceptance: vec!["summary".to_string()],
            risk: None,
            role_binding_overrides: Vec::new(),
            cardinality_overrides: Vec::new(),
            focus_partition_plans: Vec::new(),
            permission_lease: "read_only".to_string(),
            model_lease: "default".to_string(),
            budget_lease: None,
            managed_invocation: None,
            resource_scopes: vec!["read:crates/runtime".to_string()],
        }
    }

    #[test]
    fn request_rejects_duplicate_role_overrides() {
        let mut request = request();
        request.cardinality_overrides = vec![
            TeamRoleCardinalityOverride {
                role_id: "researcher".to_string(),
                cardinality: RoleCardinalityPolicy::Fixed { count: 2 },
            },
            TeamRoleCardinalityOverride {
                role_id: "researcher".to_string(),
                cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
            },
        ];
        assert!(matches!(
            request.validate(),
            Err(ValidationError::DuplicateValue { .. })
        ));
    }

    #[test]
    fn explicit_team_without_a_bounded_runtime_scope_fails_before_execution() {
        let mut request = request();
        request.resource_scopes.clear();
        assert!(matches!(
            request.validate(),
            Err(ValidationError::InvalidContract { .. })
        ));
    }

    #[test]
    fn prefixed_absolute_and_traversing_resource_paths_are_rejected() {
        for scope in ["read:/etc", "write:../outside", "read:C:\\secret"] {
            let mut request = request();
            request.resource_scopes = vec![scope.to_string()];
            assert!(
                request.validate().is_err(),
                "unsafe scope must fail: {scope}"
            );
        }
    }

    #[test]
    fn nested_focus_scopes_count_as_overlap_even_when_strings_differ() {
        let mut request = request();
        let left_boundary = "all crates";
        let right_boundary = "runtime crate";
        request.resource_scopes =
            vec!["read:crates".to_string(), "read:crates/runtime".to_string()];
        request.focus_partition_plans = vec![FocusPartitionPlan {
            role_id: "implementer".to_string(),
            shared_baseline: Vec::new(),
            slots: vec![
                FocusPartitionSlot {
                    focus_id: "parent".to_string(),
                    boundary: left_boundary.to_string(),
                    evidence_responsibility: "parent evidence".to_string(),
                    capability_cropped_refs: vec!["read:crates".to_string()],
                    scope_hash: focus_scope_hash(
                        "implementer",
                        left_boundary,
                        &["read:crates".to_string()],
                    ),
                    overlap_budget_bp: 0,
                    novelty_target_bp: 1,
                    output_contract: vec!["implementation".to_string()],
                    output_acceptance: vec!["implementation".to_string()],
                },
                FocusPartitionSlot {
                    focus_id: "child".to_string(),
                    boundary: right_boundary.to_string(),
                    evidence_responsibility: "child evidence".to_string(),
                    capability_cropped_refs: vec!["read:crates/runtime".to_string()],
                    scope_hash: focus_scope_hash(
                        "implementer",
                        right_boundary,
                        &["read:crates/runtime".to_string()],
                    ),
                    overlap_budget_bp: 0,
                    novelty_target_bp: 1,
                    output_contract: vec!["implementation".to_string()],
                    output_acceptance: vec!["implementation".to_string()],
                },
            ],
        }];

        assert!(request.validate().is_err());
    }

    #[test]
    fn equivalent_curdir_and_repeated_separator_scopes_count_as_overlap() {
        assert!(capability_scopes_overlap(
            "read:crates/runtime",
            "read:crates//runtime"
        ));
        assert!(capability_scopes_overlap(
            "write:crates/runtime",
            "read:crates/./runtime/src"
        ));
    }
}
