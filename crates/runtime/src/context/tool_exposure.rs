//! Runtime-private planning for revisioned tool exposure.

use std::collections::{BTreeMap, BTreeSet};

use harness_contract::tool::{
    ToolActivationDecision, ToolActivationReceipt, ToolActivationStatus, ToolDescriptorHealth,
    ToolDescriptorRef, ToolDiscoveryReceipt, ToolExposureProjection, ToolPermissionMode,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExposureState {
    pub catalog_revision: u64,
    pub bootstrap: BTreeSet<String>,
    pub active: BTreeSet<String>,
    pub deferred: BTreeSet<String>,
    pub reason: String,
    pub revision: u64,
    pub fallback_full: bool,
}

impl ToolExposureState {
    #[must_use]
    pub fn projection(&self, schema_tokens: u64) -> ToolExposureProjection {
        ToolExposureProjection {
            catalog_revision: self.catalog_revision,
            exposure_revision: self.revision,
            bootstrap_ids: self.bootstrap.iter().cloned().collect(),
            active_ids: self.active.iter().cloned().collect(),
            deferred_ids: self.deferred.iter().cloned().collect(),
            fallback_full: self.fallback_full,
            reason: self.reason.clone(),
            schema_tokens,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolExposurePolicy {
    pub allowed_ids: BTreeSet<String>,
    pub maximum_permission: ToolPermissionMode,
    pub supports_dynamic_exposure: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ToolExposurePlanner;

/// 常用工具白名单：在动态暴露模式下自动激活，无需模型先调用 tool_search。
/// 这些工具是几乎所有编程任务的基础操作，延迟激活会浪费模型轮次。
const HOT_TOOLS: &[&str] = &[
    "read_file",
    "read_many",
    "write_file",
    "edit_file",
    "bash",
    "grep_search",
    "glob_search",
];

impl ToolExposurePlanner {
    #[must_use]
    pub fn plan(
        &self,
        discovery: &ToolDiscoveryReceipt,
        bootstrap_ids: impl IntoIterator<Item = String>,
        policy: &ToolExposurePolicy,
    ) -> ToolExposureState {
        let eligible_ids = discovery
            .descriptors
            .iter()
            .filter(|descriptor| {
                activation_decision(descriptor, policy).0 == ToolActivationStatus::Activated
            })
            .map(|descriptor| descriptor.canonical_id.clone())
            .collect::<BTreeSet<_>>();
        let bootstrap = bootstrap_ids
            .into_iter()
            .filter(|id| eligible_ids.contains(id))
            .collect::<BTreeSet<_>>();
        let active = if policy.supports_dynamic_exposure {
            let mut active = bootstrap.clone();
            for hot_tool in HOT_TOOLS {
                if eligible_ids.contains(*hot_tool) {
                    active.insert((*hot_tool).to_string());
                }
            }
            active
        } else {
            eligible_ids.clone()
        };
        let deferred = eligible_ids.difference(&active).cloned().collect();
        ToolExposureState {
            catalog_revision: discovery.catalog_revision,
            bootstrap,
            active,
            deferred,
            reason: if policy.supports_dynamic_exposure {
                "bootstrap tools exposed; discovery candidates are deferred".to_string()
            } else {
                "provider does not support dynamic exposure; full catalog fallback".to_string()
            },
            revision: 1,
            fallback_full: !policy.supports_dynamic_exposure,
        }
    }

    #[must_use]
    pub fn activate(
        &self,
        state: &mut ToolExposureState,
        discovery: &ToolDiscoveryReceipt,
        policy: &ToolExposurePolicy,
    ) -> ToolActivationReceipt {
        let previous_exposure_revision = state.revision;
        if state.catalog_revision != discovery.catalog_revision {
            return ToolActivationReceipt {
                catalog_revision: discovery.catalog_revision,
                previous_exposure_revision,
                exposure_revision: state.revision,
                decisions: discovery
                    .activation_candidates
                    .iter()
                    .map(|id| {
                        decision(
                            id,
                            ToolActivationStatus::Unavailable,
                            "catalog revision changed",
                        )
                    })
                    .collect(),
            };
        }

        let descriptors = discovery
            .descriptors
            .iter()
            .map(|descriptor| (descriptor.canonical_id.as_str(), descriptor))
            .collect::<BTreeMap<_, _>>();
        let mut decisions = Vec::new();
        let mut changed = false;
        for id in &discovery.activation_candidates {
            let Some(descriptor) = descriptors.get(id.as_str()) else {
                decisions.push(decision(
                    id,
                    ToolActivationStatus::NotFound,
                    "descriptor not found",
                ));
                continue;
            };
            let (status, reason) = activation_decision(descriptor, policy);
            if status == ToolActivationStatus::Activated {
                changed |= state.active.insert(id.clone());
                state.deferred.remove(id);
            }
            decisions.push(decision(id, status, reason));
        }
        if changed {
            state.revision = state.revision.saturating_add(1);
            state.reason = "tool discovery activation accepted".to_string();
        }
        ToolActivationReceipt {
            catalog_revision: discovery.catalog_revision,
            previous_exposure_revision,
            exposure_revision: state.revision,
            decisions,
        }
    }
}

fn activation_decision(
    descriptor: &ToolDescriptorRef,
    policy: &ToolExposurePolicy,
) -> (ToolActivationStatus, &'static str) {
    if descriptor.health != ToolDescriptorHealth::Healthy {
        return (
            ToolActivationStatus::Unavailable,
            "tool source is not healthy",
        );
    }
    if !policy.allowed_ids.is_empty() && !policy.allowed_ids.contains(&descriptor.canonical_id) {
        return (
            ToolActivationStatus::Denied,
            "tool is outside the allowed set",
        );
    }
    if permission_rank(descriptor.required_permission) > permission_rank(policy.maximum_permission)
    {
        return (
            ToolActivationStatus::Denied,
            "tool requires a higher permission mode",
        );
    }
    (ToolActivationStatus::Activated, "activation accepted")
}

fn permission_rank(permission: ToolPermissionMode) -> u8 {
    match permission {
        ToolPermissionMode::ReadOnly => 0,
        ToolPermissionMode::WorkspaceWrite => 1,
        ToolPermissionMode::DangerFullAccess
        | ToolPermissionMode::Prompt
        | ToolPermissionMode::Allow => 2,
    }
}

fn decision(
    canonical_id: &str,
    status: ToolActivationStatus,
    reason: impl Into<String>,
) -> ToolActivationDecision {
    ToolActivationDecision {
        canonical_id: canonical_id.to_string(),
        status,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(id: &str, permission: ToolPermissionMode) -> ToolDescriptorRef {
        ToolDescriptorRef {
            canonical_id: id.to_string(),
            display_name: id.to_string(),
            source: "builtin".to_string(),
            schema_hash: format!("schema-{id}"),
            required_permission: permission,
            permission_source: "builtin".to_string(),
            health: ToolDescriptorHealth::Healthy,
        }
    }

    #[test]
    fn activation_changes_revision_only_for_accepted_new_tools() {
        let receipt = ToolDiscoveryReceipt {
            query: "files".to_string(),
            catalog_revision: 3,
            descriptors: vec![
                descriptor("tool_search", ToolPermissionMode::ReadOnly),
                descriptor("custom_reader", ToolPermissionMode::ReadOnly),
                descriptor("write_file", ToolPermissionMode::WorkspaceWrite),
            ],
            activation_candidates: vec!["custom_reader".to_string(), "write_file".to_string()],
        };
        let policy = ToolExposurePolicy {
            allowed_ids: BTreeSet::new(),
            maximum_permission: ToolPermissionMode::ReadOnly,
            supports_dynamic_exposure: true,
        };
        let planner = ToolExposurePlanner;
        let mut state = planner.plan(&receipt, ["tool_search".to_string()], &policy);
        let activation = planner.activate(&mut state, &receipt, &policy);

        assert_eq!(
            activation.activated_ids().collect::<Vec<_>>(),
            vec!["custom_reader"]
        );
        assert_eq!(state.revision, 2);
        assert!(state.active.contains("custom_reader"));
        assert!(!state.active.contains("write_file"));
    }

    #[test]
    fn full_fallback_never_bypasses_permission_filtering() {
        let receipt = ToolDiscoveryReceipt {
            query: String::new(),
            catalog_revision: 1,
            descriptors: vec![
                descriptor("read_file", ToolPermissionMode::ReadOnly),
                descriptor("grep_search", ToolPermissionMode::ReadOnly),
                descriptor("glob_search", ToolPermissionMode::ReadOnly),
                descriptor("write_file", ToolPermissionMode::WorkspaceWrite),
            ],
            activation_candidates: Vec::new(),
        };
        let policy = ToolExposurePolicy {
            allowed_ids: BTreeSet::new(),
            maximum_permission: ToolPermissionMode::ReadOnly,
            supports_dynamic_exposure: false,
        };

        let state = ToolExposurePlanner.plan(&receipt, Vec::new(), &policy);
        assert!(state.fallback_full);
        assert!(state.active.contains("read_file"));
        assert!(state.active.contains("grep_search"));
        assert!(state.active.contains("glob_search"));
        assert!(!state.active.contains("write_file"));
    }

    #[test]
    fn hot_tools_are_active_in_dynamic_exposure_mode() {
        let receipt = ToolDiscoveryReceipt {
            query: String::new(),
            catalog_revision: 1,
            descriptors: vec![
                descriptor("tool_search", ToolPermissionMode::ReadOnly),
                descriptor("read_file", ToolPermissionMode::ReadOnly),
                descriptor("grep_search", ToolPermissionMode::ReadOnly),
                descriptor("glob_search", ToolPermissionMode::ReadOnly),
                descriptor("write_file", ToolPermissionMode::WorkspaceWrite),
                descriptor("bash", ToolPermissionMode::WorkspaceWrite),
                descriptor("mcp_custom_tool", ToolPermissionMode::ReadOnly),
            ],
            activation_candidates: Vec::new(),
        };
        let policy = ToolExposurePolicy {
            allowed_ids: BTreeSet::new(),
            maximum_permission: ToolPermissionMode::WorkspaceWrite,
            supports_dynamic_exposure: true,
        };

        let state = ToolExposurePlanner.plan(&receipt, ["tool_search".to_string()], &policy);
        assert!(state.active.contains("read_file"));
        assert!(state.active.contains("grep_search"));
        assert!(state.active.contains("glob_search"));
        assert!(state.active.contains("write_file"));
        assert!(state.active.contains("bash"));
        assert!(!state.active.contains("mcp_custom_tool"));
    }

    #[test]
    fn hot_tools_not_in_catalog_are_not_activated() {
        let receipt = ToolDiscoveryReceipt {
            query: String::new(),
            catalog_revision: 1,
            descriptors: vec![
                descriptor("tool_search", ToolPermissionMode::ReadOnly),
                descriptor("read_file", ToolPermissionMode::ReadOnly),
            ],
            activation_candidates: Vec::new(),
        };
        let policy = ToolExposurePolicy {
            allowed_ids: BTreeSet::new(),
            maximum_permission: ToolPermissionMode::ReadOnly,
            supports_dynamic_exposure: true,
        };

        let state = ToolExposurePlanner.plan(&receipt, ["tool_search".to_string()], &policy);
        assert!(state.active.contains("read_file"));
        assert!(!state.active.contains("write_file"));
        assert!(!state.active.contains("bash"));
    }

    #[test]
    fn non_hot_tools_remain_deferred() {
        let receipt = ToolDiscoveryReceipt {
            query: String::new(),
            catalog_revision: 1,
            descriptors: vec![
                descriptor("tool_search", ToolPermissionMode::ReadOnly),
                descriptor("read_file", ToolPermissionMode::ReadOnly),
                descriptor("iacc_ingest", ToolPermissionMode::ReadOnly),
                descriptor("mcp_connector", ToolPermissionMode::ReadOnly),
            ],
            activation_candidates: Vec::new(),
        };
        let policy = ToolExposurePolicy {
            allowed_ids: BTreeSet::new(),
            maximum_permission: ToolPermissionMode::ReadOnly,
            supports_dynamic_exposure: true,
        };

        let state = ToolExposurePlanner.plan(&receipt, ["tool_search".to_string()], &policy);
        assert!(state.active.contains("read_file"));
        assert!(!state.active.contains("iacc_ingest"));
        assert!(!state.active.contains("mcp_connector"));
        assert!(state.deferred.contains("iacc_ingest"));
        assert!(state.deferred.contains("mcp_connector"));
    }
}
