//! Runtime-owned Team instantiation intent.
//!
//! A Surface or model may request a Team, but it never supplies an execution
//! graph. Runtime resolves the selected template revision, Agent revisions,
//! role cardinalities, focus partitions, and all effective leases before any
//! AgentTask node becomes durable.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

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
    #[serde(default)]
    pub output_contract: Vec<String>,
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

/// Complete Team request accepted by Runtime.  All fields are declarative
/// ceilings or user intent.  It intentionally has no graph nodes, executor
/// names, mutable Agent runtime IDs, or surface-owned context payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamInstantiationRequest {
    pub request_id: String,
    pub team_id: String,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mission_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution: Option<ExecutionParentBinding>,
    pub selection_mode: TeamSelectionMode,
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
        if let Some(managed_invocation) = &self.managed_invocation {
            managed_invocation.validate()?;
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
            for slot in &plan.slots {
                if slot.focus_id.trim().is_empty()
                    || slot.boundary.trim().is_empty()
                    || slot.evidence_responsibility.trim().is_empty()
                    || slot
                        .output_contract
                        .iter()
                        .any(|item| item.trim().is_empty())
                {
                    return Err(ValidationError::InvalidContract {
                        message: "every focus partition slot requires focus, boundary, evidence responsibility, and non-empty output entries"
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
                validate_unique_non_empty(
                    "team.focus_partition_plans.slots.output_contract",
                    &slot.output_contract,
                )?;
            }
        }
        Ok(())
    }
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
            mission_id: None,
            parent_execution: None,
            selection_mode: TeamSelectionMode::Explicit,
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
            resource_scopes: Vec::new(),
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
}
