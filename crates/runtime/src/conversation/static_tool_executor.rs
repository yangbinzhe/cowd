//! Lightweight in-memory ToolExecutor used by tests and embedders.

use super::*;

type ToolHandler = Box<dyn Fn(&str) -> Result<String, ToolError> + Send + Sync>;

/// Simple in-memory tool executor for tests and lightweight integrations.
#[derive(Default)]
pub struct StaticToolExecutor {
    handlers: BTreeMap<String, ToolHandler>,
}

impl StaticToolExecutor {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn register(
        mut self,
        tool_name: impl Into<String>,
        handler: impl Fn(&str) -> Result<String, ToolError> + Send + Sync + 'static,
    ) -> Self {
        self.handlers.insert(tool_name.into(), Box::new(handler));
        self
    }
}

#[async_trait::async_trait]
impl ToolExecutor for StaticToolExecutor {
    async fn execute_output(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        let output = self
            .handlers
            .get(tool_name)
            .ok_or_else(|| ToolError::new(format!("unknown tool: {tool_name}")))?(
            input
        )?;
        Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
            output,
        ))
    }

    fn registered_tool_effect(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
        use harness_contract::tool::{
            ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
            ToolPermissionMode,
        };

        self.handlers.contains_key(tool_name).then(|| {
            let safety = crate::tool_orchestrator::ToolSafetyCategory::from_tool_name(tool_name);
            let target = ["path", "url", "server", "uri", "target"]
                .into_iter()
                .find_map(|key| input.get(key).and_then(serde_json::Value::as_str))
                .map(str::to_string);
            let (effect_kind, required_permission, mut scope, approval_class) = match safety {
                crate::tool_orchestrator::ToolSafetyCategory::ReadOnly => (
                    ToolEffectKind::Read,
                    ToolPermissionMode::ReadOnly,
                    PermissionScope::new(PermissionResource::File, PermissionOperation::Read),
                    ToolApprovalClass::None,
                ),
                crate::tool_orchestrator::ToolSafetyCategory::WriteLocal => (
                    ToolEffectKind::Write,
                    ToolPermissionMode::WorkspaceWrite,
                    PermissionScope::new(PermissionResource::File, PermissionOperation::Write),
                    ToolApprovalClass::Policy,
                ),
                crate::tool_orchestrator::ToolSafetyCategory::Network => (
                    ToolEffectKind::Network,
                    ToolPermissionMode::DangerFullAccess,
                    PermissionScope::new(PermissionResource::Network, PermissionOperation::Execute),
                    ToolApprovalClass::Policy,
                ),
                crate::tool_orchestrator::ToolSafetyCategory::Destructive => (
                    ToolEffectKind::Destructive,
                    ToolPermissionMode::DangerFullAccess,
                    PermissionScope::new(PermissionResource::Tool, PermissionOperation::Execute),
                    ToolApprovalClass::User,
                ),
            };
            scope.target = target.clone();
            ToolEffectDescriptor {
                tool_id: tool_name.to_string(),
                descriptor_hash: format!(
                    "static:{tool_name}:{effect_kind:?}:{}",
                    target.as_deref().unwrap_or_default()
                ),
                effect_kind,
                idempotency: ToolIdempotency::Unknown,
                scopes: vec![scope],
                required_permission,
                approval_class,
                uses_network: matches!(
                    safety,
                    crate::tool_orchestrator::ToolSafetyCategory::Network
                ),
                spawns_process: false,
                mutates_packages: false,
                mutates_system: matches!(
                    safety,
                    crate::tool_orchestrator::ToolSafetyCategory::Destructive
                ),
                assessment: harness_contract::policy::EffectAssessment {
                    reversibility: match effect_kind {
                        ToolEffectKind::Read | ToolEffectKind::Network => {
                            harness_contract::policy::EffectReversibility::Reversible
                        }
                        ToolEffectKind::Write => {
                            harness_contract::policy::EffectReversibility::Compensatable
                        }
                        _ => harness_contract::policy::EffectReversibility::Irreversible,
                    },
                    externality: match effect_kind {
                        ToolEffectKind::Read => {
                            harness_contract::policy::EffectExternality::Internal
                        }
                        ToolEffectKind::Write => {
                            harness_contract::policy::EffectExternality::Workspace
                        }
                        ToolEffectKind::Network => {
                            harness_contract::policy::EffectExternality::NetworkRead
                        }
                        _ => harness_contract::policy::EffectExternality::System,
                    },
                    data_sensitivity: harness_contract::policy::DataClassification::Internal,
                    novelty: harness_contract::policy::EffectNovelty::Routine,
                    blast_radius: match effect_kind {
                        ToolEffectKind::Read | ToolEffectKind::Network => {
                            harness_contract::policy::EffectBlastRadius::Item
                        }
                        ToolEffectKind::Write => {
                            harness_contract::policy::EffectBlastRadius::Workspace
                        }
                        _ => harness_contract::policy::EffectBlastRadius::System,
                    },
                },
            }
        })
    }

    async fn execute_authorized_output(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        if authorization.tool_id != tool_name {
            return Err(ToolError::new(
                "static tool authorization names a different tool",
            ));
        }
        self.execute_output(tool_name, input).await
    }

    fn has_registered_tools(&self) -> bool {
        !self.handlers.is_empty()
    }

    fn available_tool_names(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }
}
