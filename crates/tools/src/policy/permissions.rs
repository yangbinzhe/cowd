use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use harness_contract::policy::PermissionMode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionPolicy {
    active_mode: PermissionMode,
    tool_requirements: BTreeMap<String, PermissionMode>,
}

impl PermissionPolicy {
    #[must_use]
    pub fn new(active_mode: PermissionMode) -> Self {
        Self {
            active_mode,
            tool_requirements: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_tool_requirement(
        mut self,
        tool_name: impl Into<String>,
        required_mode: PermissionMode,
    ) -> Self {
        self.tool_requirements
            .insert(tool_name.into(), required_mode);
        self
    }

    #[must_use]
    pub fn active_mode(&self) -> PermissionMode {
        self.active_mode
    }

    #[must_use]
    pub fn required_mode_for(&self, tool_name: &str) -> PermissionMode {
        self.tool_requirements
            .get(tool_name)
            .copied()
            .unwrap_or(PermissionMode::DangerFullAccess)
    }

    #[must_use]
    pub fn authorize_required(
        &self,
        tool_name: &str,
        input: &str,
        required_mode: PermissionMode,
    ) -> EnforcementResult {
        if self.active_mode == PermissionMode::Prompt {
            return EnforcementResult::Allowed;
        }
        if self.active_mode >= required_mode {
            return EnforcementResult::Allowed;
        }
        EnforcementResult::Denied {
            tool: tool_name.to_owned(),
            active_mode: self.active_mode.as_str().to_owned(),
            required_mode: required_mode.as_str().to_owned(),
            reason: format!(
                "'{tool_name}' with input '{input}' requires {} permission, but current mode is {}; current mode is '{}'",
                required_mode.as_str(),
                self.active_mode.as_str(),
                self.active_mode.as_str()
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome")]
pub enum EnforcementResult {
    Allowed,
    Denied {
        tool: String,
        active_mode: String,
        required_mode: String,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionEnforcer {
    policy: PermissionPolicy,
}

impl PermissionEnforcer {
    #[must_use]
    pub fn new(policy: PermissionPolicy) -> Self {
        Self { policy }
    }

    #[must_use]
    pub fn active_mode(&self) -> PermissionMode {
        self.policy.active_mode()
    }

    pub fn check(&self, tool_name: &str, input: &str) -> EnforcementResult {
        let required = self.policy.required_mode_for(tool_name);
        self.check_with_required_mode(tool_name, input, required)
    }

    pub fn check_with_required_mode(
        &self,
        tool_name: &str,
        input: &str,
        required_mode: PermissionMode,
    ) -> EnforcementResult {
        self.policy
            .authorize_required(tool_name, input, required_mode)
    }
}
