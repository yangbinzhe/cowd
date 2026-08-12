//! Canonical Runtime approval coordinator.
//!
//! The coordinator owns policy selection and waiting. Durable requests,
//! decisions, and grants remain in [`ApprovalQueue`]; the process-local wait
//! registry is only a wake-up index and is never queried by Gateway or a
//! Surface.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use harness_contract::core::TaskRisk;
use harness_contract::policy::{
    ApprovalContext, ApprovalDecisionActor, ApprovalDecisionActorKind, ApprovalDecisionCommand,
    ApprovalDomain, ApprovalGrant, ApprovalGrantScope, ApprovalProfile, ApprovalRequest,
    ApprovalSource, ApprovalStatus, ApprovalTimeoutPolicy, DataClassification, EffectExternality,
    EffectReversibility, LowRiskTimeoutAction,
};
use harness_contract::tool::{ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind};
use tokio::sync::{Notify, RwLock};

use crate::{ApprovalConfig, ApprovalQueue, CancellationToken, SubmitGlobalApprovalRequest};

pub type ApprovalPendingHook = Arc<dyn Fn(&ApprovalRequest) + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalResolution {
    Approved {
        approval_id: String,
        grant: ApprovalGrant,
    },
    Denied {
        approval_id: String,
        reason: String,
    },
    Cancelled {
        approval_id: String,
        reason: String,
    },
    ControlRequested {
        approval_id: String,
        reason: String,
    },
}

impl ApprovalResolution {
    #[must_use]
    pub fn approval_id(&self) -> &str {
        match self {
            Self::Approved { approval_id, .. }
            | Self::Denied { approval_id, .. }
            | Self::Cancelled { approval_id, .. }
            | Self::ControlRequested { approval_id, .. } => approval_id,
        }
    }
}

#[derive(Debug, Default)]
pub struct ApprovalWaitRegistry {
    waiters: Mutex<BTreeMap<String, Arc<Notify>>>,
}

impl ApprovalWaitRegistry {
    fn register(&self, approval_id: &str) -> Arc<Notify> {
        let mut waiters = self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            waiters
                .entry(approval_id.to_string())
                .or_insert_with(|| Arc::new(Notify::new())),
        )
    }

    fn notify(&self, approval_id: &str) {
        if let Some(waiter) = self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(approval_id)
            .cloned()
        {
            waiter.notify_waiters();
        }
    }

    fn remove(&self, approval_id: &str) {
        self.waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(approval_id);
    }
}

pub struct ApprovalCoordinator {
    queue: Arc<ApprovalQueue>,
    config: Arc<RwLock<ApprovalConfig>>,
    waits: ApprovalWaitRegistry,
}

impl std::fmt::Debug for ApprovalCoordinator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApprovalCoordinator")
            .field("pending_count", &self.queue.pending().len())
            .finish_non_exhaustive()
    }
}

impl ApprovalCoordinator {
    #[must_use]
    pub fn new(queue: Arc<ApprovalQueue>, config: ApprovalConfig) -> Self {
        Self {
            queue,
            config: Arc::new(RwLock::new(config)),
            waits: ApprovalWaitRegistry::default(),
        }
    }

    #[must_use]
    pub fn queue(&self) -> &Arc<ApprovalQueue> {
        &self.queue
    }

    pub async fn config(&self) -> ApprovalConfig {
        self.config.read().await.clone()
    }

    pub async fn update_config(&self, config: ApprovalConfig) -> ApprovalConfig {
        *self.config.write().await = config.clone();
        config
    }

    pub fn notify_decision(&self, approval_id: &str) {
        self.waits.notify(approval_id);
    }

    /// Resolve a tool capability gap through an existing Grant, bounded
    /// Steward decision, or canonical human approval.
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_tool(
        &self,
        source: ApprovalSource,
        mut context: ApprovalContext,
        descriptor: &ToolEffectDescriptor,
        input: &str,
        cancellation: CancellationToken,
        control_notify: Option<Arc<Notify>>,
        pending_hook: Option<ApprovalPendingHook>,
        timeout: Duration,
    ) -> Result<ApprovalResolution, String> {
        let config = self.config().await;
        let approval_profile = context.approval_profile.unwrap_or(config.profile);
        context.approval_profile = Some(approval_profile);
        context.profile_id = match approval_profile {
            ApprovalProfile::Supervised => "supervised",
            ApprovalProfile::Balanced => "balanced",
            ApprovalProfile::Autonomous => "autonomous",
        }
        .to_string();
        context.effect = Some(descriptor.clone());
        if context.resource_targets.is_empty() {
            context.resource_targets = descriptor
                .scopes
                .iter()
                .filter_map(|scope| scope.target.clone())
                .collect();
        }
        let risk = task_risk_for_effect(descriptor);
        if let Some(grant) = self.queue.matching_grant(&context, risk) {
            return Ok(ApprovalResolution::Approved {
                approval_id: grant.approval_id.clone(),
                grant,
            });
        }

        let explicit_ask = context.explicit_ask
            || matches!(
                descriptor.approval_class,
                ToolApprovalClass::User | ToolApprovalClass::Administrator
            );
        context.explicit_ask = explicit_ask;
        let timeout_policy = if risk == TaskRisk::Low
            && !explicit_ask
            && config.low_risk_timeout == LowRiskTimeoutAction::AutoApproveOnce
        {
            ApprovalTimeoutPolicy::AutoApproveOnce
        } else {
            ApprovalTimeoutPolicy::Pending
        };
        let approval_id = context.invocation_id.as_ref().map_or_else(
            || format!("tool-approval:{}", uuid::Uuid::new_v4()),
            |invocation_id| format!("tool-approval:{invocation_id}"),
        );
        let request = self.queue.submit_scoped(
            approval_id.clone(),
            SubmitGlobalApprovalRequest {
                source,
                context: context.clone(),
                action: descriptor.tool_id.clone(),
                summary: summarize_tool_action(&descriptor.tool_id, input),
                risk,
                domain: ApprovalDomain::Execution,
                blocks_execution: true,
                evidence_refs: vec![
                    format!("tool-descriptor:{}", descriptor.descriptor_hash),
                    format!(
                        "tool-invocation:{}",
                        context.invocation_id.as_deref().unwrap_or("n/a")
                    ),
                ],
                timeout_policy,
            },
        )?;

        if !explicit_ask && deterministic_policy_can_approve(descriptor, risk) {
            let receipt = self.queue.decide_internal(ApprovalDecisionCommand {
                approval_id: request.approval_id.clone(),
                approved: true,
                skip: false,
                reason: "known low-risk effect allowed by deterministic Runtime policy".to_string(),
                scope: ApprovalGrantScope::Once,
                actor: ApprovalDecisionActor {
                    kind: ApprovalDecisionActorKind::Policy,
                    actor_id: "runtime-low-risk-policy".to_string(),
                },
                evidence_refs: vec!["approval.policy.known_low_risk".to_string()],
            })?;
            self.notify_decision(&request.approval_id);
            let grant = self
                .queue
                .grant_for_approval(&request.approval_id)
                .ok_or_else(|| "approved request did not create a grant".to_string())?;
            return Ok(ApprovalResolution::Approved {
                approval_id: receipt.approval_id,
                grant,
            });
        }

        if !explicit_ask && steward_can_approve(approval_profile, descriptor, risk) {
            let receipt = self.queue.decide_internal(ApprovalDecisionCommand {
                approval_id: request.approval_id.clone(),
                approved: true,
                skip: false,
                reason: "bounded Steward policy approved a known reversible effect".to_string(),
                scope: ApprovalGrantScope::Once,
                actor: ApprovalDecisionActor {
                    kind: ApprovalDecisionActorKind::StewardAgent,
                    actor_id: "runtime-approval-steward".to_string(),
                },
                evidence_refs: vec!["approval.steward.eligible".to_string()],
            })?;
            self.notify_decision(&request.approval_id);
            let grant = self
                .queue
                .grant_for_approval(&request.approval_id)
                .ok_or_else(|| "approved request did not create a grant".to_string())?;
            return Ok(ApprovalResolution::Approved {
                approval_id: receipt.approval_id,
                grant,
            });
        }

        if let Some(pending_hook) = pending_hook {
            pending_hook(&request);
        }
        self.wait_for_resolution(&request.approval_id, cancellation, control_notify, timeout)
            .await
    }

    async fn wait_for_resolution(
        &self,
        approval_id: &str,
        cancellation: CancellationToken,
        control_notify: Option<Arc<Notify>>,
        low_risk_timeout: Duration,
    ) -> Result<ApprovalResolution, String> {
        let waiter = self.waits.register(approval_id);
        loop {
            let request = self
                .queue
                .get(approval_id)
                .ok_or_else(|| format!("approval request not found: {approval_id}"))?;
            match request.status {
                ApprovalStatus::Approved => {
                    self.waits.remove(approval_id);
                    let grant = self
                        .queue
                        .grant_for_approval(approval_id)
                        .ok_or_else(|| "approved request did not create a grant".to_string())?;
                    return Ok(ApprovalResolution::Approved {
                        approval_id: approval_id.to_string(),
                        grant,
                    });
                }
                ApprovalStatus::Denied | ApprovalStatus::TimedOut | ApprovalStatus::Skipped => {
                    self.waits.remove(approval_id);
                    return Ok(ApprovalResolution::Denied {
                        approval_id: approval_id.to_string(),
                        reason: request.decision.map_or_else(
                            || request.status.as_str().to_string(),
                            |value| value.reason,
                        ),
                    });
                }
                ApprovalStatus::Cancelled | ApprovalStatus::Superseded => {
                    self.waits.remove(approval_id);
                    return Ok(ApprovalResolution::Cancelled {
                        approval_id: approval_id.to_string(),
                        reason: request.decision.map_or_else(
                            || request.status.as_str().to_string(),
                            |value| value.reason,
                        ),
                    });
                }
                ApprovalStatus::Pending => {}
            }

            let timeout_enabled = request.timeout_policy == ApprovalTimeoutPolicy::AutoApproveOnce;
            tokio::select! {
                () = cancellation.cancelled() => {
                    let receipt = self.queue.cancel(
                        approval_id,
                        "the active Turn was cancelled while waiting for approval",
                        false,
                    )?;
                    self.waits.remove(approval_id);
                    return Ok(ApprovalResolution::Cancelled {
                        approval_id: receipt.approval_id,
                        reason: receipt.message,
                    });
                }
                () = waiter.notified() => {}
                () = wait_for_control(control_notify.as_ref()) => {
                    let receipt = self.queue.cancel(
                        approval_id,
                        "a newer Session input superseded the pending approval wait",
                        true,
                    )?;
                    self.waits.remove(approval_id);
                    return Ok(ApprovalResolution::ControlRequested {
                        approval_id: receipt.approval_id,
                        reason: receipt.message,
                    });
                }
                () = tokio::time::sleep(low_risk_timeout), if timeout_enabled => {
                    self.queue.timeout(approval_id)?;
                    self.notify_decision(approval_id);
                }
            }
        }
    }
}

async fn wait_for_control(notify: Option<&Arc<Notify>>) {
    match notify {
        Some(notify) => notify.notified().await,
        None => std::future::pending::<()>().await,
    }
}

fn summarize_tool_action(tool_id: &str, input: &str) -> String {
    const MAX_CHARS: usize = 240;
    // T12: structured MCP approval summary template. An MCP approval must be
    // readable without opening a raw JSON tool input: server, tool, and a
    // bounded argument preview.
    if let Some((server, tool)) = tool_id
        .strip_prefix("mcp__")
        .and_then(|qualified| qualified.split_once("__"))
    {
        let preview: String = input.chars().take(MAX_CHARS).collect();
        let suffix = if input.chars().count() > MAX_CHARS {
            "…".to_string()
        } else {
            String::new()
        };
        return format!("MCP server `{server}` tool `{tool}`: {preview}{suffix}");
    }
    let preview: String = input.chars().take(MAX_CHARS).collect();
    if input.chars().count() > MAX_CHARS {
        format!("{tool_id}: {preview}…")
    } else {
        format!("{tool_id}: {preview}")
    }
}

fn steward_can_approve(
    profile: ApprovalProfile,
    descriptor: &ToolEffectDescriptor,
    risk: TaskRisk,
) -> bool {
    if matches!(
        descriptor.approval_class,
        ToolApprovalClass::User | ToolApprovalClass::Administrator
    ) || descriptor.assessment.data_sensitivity == DataClassification::Secret
        || matches!(
            descriptor.assessment.externality,
            EffectExternality::ExternalMutation
                | EffectExternality::System
                | EffectExternality::Unknown
        )
        || matches!(
            descriptor.effect_kind,
            ToolEffectKind::System | ToolEffectKind::Destructive | ToolEffectKind::Unknown
        )
    {
        return false;
    }
    profile == ApprovalProfile::Autonomous
        && risk == TaskRisk::Medium
        && matches!(
            descriptor.assessment.reversibility,
            EffectReversibility::Reversible | EffectReversibility::Compensatable
        )
}

fn deterministic_policy_can_approve(descriptor: &ToolEffectDescriptor, risk: TaskRisk) -> bool {
    risk == TaskRisk::Low
        && matches!(
            descriptor.effect_kind,
            ToolEffectKind::Read | ToolEffectKind::Network
        )
        && !descriptor.mutates_system
        && !descriptor.mutates_packages
        && descriptor.assessment.data_sensitivity != DataClassification::Secret
        && !matches!(
            descriptor.assessment.externality,
            EffectExternality::ExternalMutation
                | EffectExternality::System
                | EffectExternality::Unknown
        )
}

#[must_use]
pub fn task_risk_for_effect(descriptor: &ToolEffectDescriptor) -> TaskRisk {
    if descriptor.approval_class == ToolApprovalClass::Administrator
        || descriptor.mutates_system
        || matches!(
            descriptor.effect_kind,
            ToolEffectKind::System | ToolEffectKind::Destructive | ToolEffectKind::Unknown
        )
        || matches!(
            descriptor.assessment.externality,
            EffectExternality::System | EffectExternality::Unknown
        )
    {
        return TaskRisk::Critical;
    }
    if descriptor.approval_class == ToolApprovalClass::User
        || descriptor.assessment.data_sensitivity == DataClassification::Secret
        || descriptor.assessment.externality == EffectExternality::ExternalMutation
        || descriptor.assessment.reversibility == EffectReversibility::Irreversible
    {
        return TaskRisk::High;
    }
    if matches!(
        descriptor.effect_kind,
        ToolEffectKind::Write | ToolEffectKind::Process | ToolEffectKind::Package
    ) || matches!(
        descriptor.assessment.reversibility,
        EffectReversibility::Compensatable | EffectReversibility::Unknown
    ) {
        return TaskRisk::Medium;
    }
    TaskRisk::Low
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::policy::{
        EffectAssessment, EffectBlastRadius, EffectNovelty, PermissionOperation,
        PermissionResource, PermissionScope,
    };
    use harness_contract::tool::{ToolIdempotency, ToolPermissionMode};

    fn coordinator(profile: ApprovalProfile) -> Arc<ApprovalCoordinator> {
        Arc::new(ApprovalCoordinator::new(
            Arc::new(ApprovalQueue::new(Arc::new(
                crate::RuntimeEventStore::try_open_in_memory().expect("event store"),
            ))),
            ApprovalConfig::default().with_profile(profile),
        ))
    }

    fn source() -> ApprovalSource {
        ApprovalSource {
            kind: harness_contract::policy::ApprovalSourceKind::Session,
            session_id: Some("session-approval".to_string()),
            agent_id: None,
            team_id: None,
            mission_id: None,
            resource_ref: Some("workspace:test".to_string()),
            review_ref: None,
            application: None,
        }
    }

    fn context(invocation_id: &str) -> ApprovalContext {
        let source = source();
        let mut context = ApprovalContext::owned(&source, "read_file", "workspace:test");
        context.principal_id = "principal:test".to_string();
        context.session_id = source.session_id;
        context.turn_id = Some("turn:test".to_string());
        context.invocation_id = Some(invocation_id.to_string());
        context
    }

    fn descriptor(effect_kind: ToolEffectKind) -> ToolEffectDescriptor {
        ToolEffectDescriptor {
            tool_id: "read_file".to_string(),
            descriptor_hash: "descriptor-v1".to_string(),
            effect_kind,
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![PermissionScope {
                resource: PermissionResource::File,
                operation: PermissionOperation::Read,
                target: Some("/workspace/README.md".to_string()),
            }],
            required_permission: ToolPermissionMode::ReadOnly,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: EffectAssessment {
                reversibility: EffectReversibility::Reversible,
                externality: EffectExternality::Workspace,
                data_sensitivity: DataClassification::Internal,
                novelty: EffectNovelty::Routine,
                blast_radius: EffectBlastRadius::Item,
            },
        }
    }

    #[test]
    fn steward_never_approves_secret_or_external_mutation() {
        let mut secret = descriptor(ToolEffectKind::Read);
        secret.assessment.data_sensitivity = DataClassification::Secret;
        assert!(!steward_can_approve(
            ApprovalProfile::Autonomous,
            &secret,
            TaskRisk::High
        ));
        let mut mutation = descriptor(ToolEffectKind::Network);
        mutation.assessment.externality = EffectExternality::ExternalMutation;
        assert!(!steward_can_approve(
            ApprovalProfile::Autonomous,
            &mutation,
            TaskRisk::High
        ));
    }

    #[test]
    fn autonomous_profile_accepts_bounded_reversible_medium_effect() {
        let write = descriptor(ToolEffectKind::Write);
        assert!(steward_can_approve(
            ApprovalProfile::Autonomous,
            &write,
            TaskRisk::Medium
        ));
    }

    #[tokio::test]
    async fn known_low_risk_read_is_approved_without_human_wait() {
        let coordinator = coordinator(ApprovalProfile::Balanced);
        let pending_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let pending_flag = Arc::clone(&pending_called);
        let result = coordinator
            .resolve_tool(
                source(),
                context("read-low"),
                &descriptor(ToolEffectKind::Read),
                r#"{"path":"README.md"}"#,
                CancellationToken::new(),
                None,
                Some(Arc::new(move |_| {
                    pending_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                })),
                Duration::from_millis(10),
            )
            .await
            .expect("known low-risk read is deterministic");

        let ApprovalResolution::Approved { grant, .. } = result else {
            panic!("low-risk read must be approved");
        };
        assert_eq!(grant.issued_by.kind, ApprovalDecisionActorKind::Policy);
        assert!(!pending_called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn newer_session_control_supersedes_a_sensitive_pending_approval() {
        let coordinator = coordinator(ApprovalProfile::Autonomous);
        let mut secret = descriptor(ToolEffectKind::Read);
        secret.assessment.data_sensitivity = DataClassification::Secret;
        let control = Arc::new(Notify::new());
        let pending = Arc::new(Notify::new());
        let pending_signal = Arc::clone(&pending);
        let coordinator_task = Arc::clone(&coordinator);
        let control_task = Arc::clone(&control);
        let task = tokio::spawn(async move {
            coordinator_task
                .resolve_tool(
                    source(),
                    context("read-secret"),
                    &secret,
                    r#"{"path":"secret.txt"}"#,
                    CancellationToken::new(),
                    Some(control_task),
                    Some(Arc::new(move |_| pending_signal.notify_one())),
                    Duration::from_secs(30),
                )
                .await
        });

        pending.notified().await;
        control.notify_one();
        let resolution = task.await.expect("task joins").expect("resolution");
        assert!(matches!(
            resolution,
            ApprovalResolution::ControlRequested { .. }
        ));
        assert!(coordinator.queue().pending().is_empty());
    }

    #[test]
    fn mcp_approval_summary_uses_server_tool_template() {
        let summary = summarize_tool_action(
            "mcp__filesystem__read_file",
            r#"{"path":"/tmp/a.txt","offset":0}"#,
        );
        assert!(summary.starts_with("MCP server `filesystem` tool `read_file`:"));
        assert!(summary.contains(r#"{"path":"/tmp/a.txt","offset":0}"#));
    }
}
