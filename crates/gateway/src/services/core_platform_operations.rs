//! Core-owned operations that compose governed Gateway capabilities without
//! exposing HTTP routes or Surface ledger internals to APP workers.

use cowd_app_protocol::AppInvocationEnvelopeV1;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api_routes::AppState;

pub(crate) const ACTION_PLAN_OPERATION_ID: &str = "core.cross_plane.action.plan";
pub(crate) const SURFACE_OUTBOX_LIST_OPERATION_ID: &str = "core.surface.outbox.list";
pub(crate) const PLATFORM_OPERATION_IDS: [&str; 2] =
    [ACTION_PLAN_OPERATION_ID, SURFACE_OUTBOX_LIST_OPERATION_ID];

pub(crate) fn supports(operation_id: &str) -> bool {
    PLATFORM_OPERATION_IDS.contains(&operation_id)
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CrossPlaneActionPlanInput {
    pub(crate) actor_identity_ref: Option<String>,
    pub(crate) source_channel: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) requested_capability: String,
    pub(crate) provider_account: Option<String>,
    pub(crate) target_ref: Option<String>,
    pub(crate) resource_ref: Option<String>,
    pub(crate) risk: CrossPlaneRiskV1,
    pub(crate) data_classification: DataClassificationV1,
    pub(crate) identity_trust: IdentityTrustV1,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CrossPlaneRiskV1 {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DataClassificationV1 {
    Public,
    Internal,
    Confidential,
    Secret,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IdentityTrustV1 {
    Verified,
    Claimed,
    Observed,
    Unknown,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(crate) struct CrossPlaneActionPlanOutput {
    pub(crate) action: CrossPlaneActionProjection,
    pub(crate) policy_simulation: CrossPlanePolicySimulation,
    pub(crate) action_preflight: CrossPlaneActionPreflight,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(crate) struct CrossPlaneActionProjection {
    pub(crate) actor_principal: String,
    pub(crate) actor_identity_ref: Option<String>,
    pub(crate) source_channel: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) requested_capability: String,
    pub(crate) provider_account: Option<String>,
    pub(crate) target_ref: Option<String>,
    pub(crate) resource_ref: Option<String>,
    pub(crate) risk: String,
    pub(crate) data_classification: String,
    pub(crate) identity_trust: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(crate) struct CrossPlanePolicySimulation {
    pub(crate) decision: String,
    pub(crate) reason: String,
    pub(crate) matched_grant_id: Option<String>,
    pub(crate) required_approval: Option<String>,
    pub(crate) degrade_to: Option<String>,
    pub(crate) policy_version: String,
    pub(crate) evaluated_at: Option<String>,
    pub(crate) active_grants_before: usize,
    pub(crate) consumed_grant_id: Option<String>,
    pub(crate) remaining_uses_after: Option<u32>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(crate) struct CrossPlaneActionPreflight {
    pub(crate) target_platform: Option<String>,
    pub(crate) platform_readiness: Option<PlatformReadinessProjection>,
    pub(crate) adapter_capability: Option<AdapterCapabilityProjection>,
    pub(crate) dispatch_target: Option<DispatchTargetProjection>,
    pub(crate) executable: bool,
    pub(crate) blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(crate) struct PlatformReadinessProjection {
    pub(crate) name: String,
    pub(crate) platform_type: String,
    pub(crate) enabled: bool,
    pub(crate) status: String,
    pub(crate) configured: bool,
    pub(crate) credential_present: bool,
    pub(crate) missing_required: Vec<String>,
    pub(crate) scopes: Vec<String>,
    pub(crate) capabilities: Vec<String>,
    pub(crate) diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(crate) struct AdapterCapabilityProjection {
    pub(crate) platform: String,
    pub(crate) capability: String,
    pub(crate) operation: String,
    pub(crate) live_supported: bool,
    pub(crate) adapter_bound: bool,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(crate) struct DispatchTargetProjection {
    pub(crate) platform: Option<String>,
    pub(crate) operation: Option<String>,
    pub(crate) target_ref: Option<String>,
    pub(crate) resource_ref: Option<String>,
    pub(crate) session_key: Option<String>,
    pub(crate) has_outbound_message: bool,
    pub(crate) ready: bool,
    pub(crate) blockers: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct SurfaceOutboxListInput {
    pub(crate) surface: SurfaceSelectorV1,
    pub(crate) status: SurfaceOutboxStatusFilterV1,
    pub(crate) offset: usize,
    pub(crate) limit: usize,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SurfaceSelectorV1 {
    All,
    Feishu,
    WechatIlink,
    Wecom,
}

impl SurfaceSelectorV1 {
    fn as_surface(self) -> Option<&'static str> {
        match self {
            Self::All => None,
            Self::Feishu => Some("feishu"),
            Self::WechatIlink => Some("wechat-ilink"),
            Self::Wecom => Some("wecom"),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SurfaceOutboxStatusFilterV1 {
    Active,
    DeadLetter,
    Terminal,
    All,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(crate) struct SurfaceOutboxListOutput {
    pub(crate) total: usize,
    pub(crate) offset: usize,
    pub(crate) limit: usize,
    pub(crate) records: Vec<SurfaceOutboxProjection>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(crate) struct SurfaceOutboxProjection {
    pub(crate) delivery_id: String,
    pub(crate) surface: String,
    pub(crate) recipient: String,
    pub(crate) thread_id: Option<String>,
    pub(crate) idempotency_key: String,
    pub(crate) text_hash: String,
    pub(crate) text_summary: String,
    pub(crate) status: String,
    pub(crate) attempts: u32,
    pub(crate) max_attempts: u32,
    pub(crate) next_retry_at_ms: Option<i64>,
    pub(crate) created_at_ms: i64,
    pub(crate) updated_at_ms: i64,
    pub(crate) sent_at_ms: Option<i64>,
    pub(crate) last_error: Option<String>,
    pub(crate) source_session_id: Option<String>,
    pub(crate) reply_to_message_id: Option<String>,
}

pub(crate) async fn dispatch(
    state: &AppState,
    envelope: &AppInvocationEnvelopeV1,
    operation_id: &str,
    payload: &Value,
) -> Result<Value, String> {
    match operation_id {
        ACTION_PLAN_OPERATION_ID => {
            let input = serde_json::from_value::<CrossPlaneActionPlanInput>(payload.clone())
                .map_err(|error| format!("invalid cross-plane action plan input: {error}"))?;
            let output = crate::api_routes::cross_plane_routes::core_action_plan(
                state,
                envelope.principal.subject.clone(),
                input,
            )
            .await;
            serde_json::to_value(output).map_err(|error| error.to_string())
        }
        SURFACE_OUTBOX_LIST_OPERATION_ID => {
            let input = serde_json::from_value::<SurfaceOutboxListInput>(payload.clone())
                .map_err(|error| format!("invalid Surface outbox list input: {error}"))?;
            surface_outbox_list(state, input)
                .and_then(|output| serde_json::to_value(output).map_err(|error| error.to_string()))
        }
        _ => Err(format!("unknown Core platform operation `{operation_id}`")),
    }
}

fn surface_outbox_list(
    state: &AppState,
    input: SurfaceOutboxListInput,
) -> Result<SurfaceOutboxListOutput, String> {
    if input.limit == 0 || input.limit > 200 || input.offset > 1_000_000 {
        return Err("Surface outbox pagination is outside the governed bounds".to_owned());
    }
    let mut records = match input.surface.as_surface() {
        Some(surface) => state.services.surface.outbox(surface)?,
        None => state.services.surface.all_outbox()?,
    };
    records.retain(|record| match input.status {
        SurfaceOutboxStatusFilterV1::All => true,
        SurfaceOutboxStatusFilterV1::DeadLetter => record.status == "dead_letter",
        SurfaceOutboxStatusFilterV1::Terminal => is_terminal_outbox_status(&record.status),
        SurfaceOutboxStatusFilterV1::Active => !is_terminal_outbox_status(&record.status),
    });
    records.sort_by(|left, right| {
        right
            .updated_at_ms
            .cmp(&left.updated_at_ms)
            .then_with(|| left.delivery_id.cmp(&right.delivery_id))
    });
    let total = records.len();
    let records = records
        .into_iter()
        .skip(input.offset)
        .take(input.limit)
        .map(|record| SurfaceOutboxProjection {
            delivery_id: record.delivery_id,
            surface: record.surface,
            recipient: record.recipient,
            thread_id: record.thread_id,
            idempotency_key: record.idempotency_key,
            text_hash: record.text_hash,
            text_summary: record.text_summary,
            status: record.status,
            attempts: record.attempts,
            max_attempts: record.max_attempts,
            next_retry_at_ms: record.next_retry_at_ms,
            created_at_ms: record.created_at_ms,
            updated_at_ms: record.updated_at_ms,
            sent_at_ms: record.sent_at_ms,
            last_error: record.last_error,
            source_session_id: record.source_session_id,
            reply_to_message_id: record.reply_to_message_id,
        })
        .collect();
    Ok(SurfaceOutboxListOutput {
        total,
        offset: input.offset,
        limit: input.limit,
        records,
    })
}

fn is_terminal_outbox_status(status: &str) -> bool {
    matches!(
        status,
        "sent" | "dead_letter" | "archived" | "failed_terminal"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(operation_id: &str, payload: Value) -> AppInvocationEnvelopeV1 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        AppInvocationEnvelopeV1 {
            schema_version: 1,
            operation_id: operation_id.to_owned(),
            request_id: format!("request-{}", uuid::Uuid::new_v4()),
            correlation_id: format!("correlation-{}", uuid::Uuid::new_v4()),
            causation_id: None,
            deadline_unix_ms: now + 5_000,
            idempotency_key: None,
            expected_revision: None,
            call_chain: vec!["app:fixture".to_owned()],
            max_hops: 4,
            input_schema_digest: cowd_app_protocol::Sha256Digest(
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            ),
            principal: cowd_app_protocol::PrincipalContextV1 {
                subject: "signed-app-subject".to_owned(),
                tenant_id: "deployment-tenant".to_owned(),
                workspace_id: "workspace-1".to_owned(),
                delegation: cowd_app_protocol::DelegationKindV1::Service,
                grant_id: "grant-1".to_owned(),
                authorization_profile_id: "operator".to_owned(),
                authorization_revision: 1,
                granted_capabilities: vec![
                    if operation_id == ACTION_PLAN_OPERATION_ID {
                        "core.cross_plane.read".to_owned()
                    } else {
                        "core.surface.outbox.read".to_owned()
                    },
                    "fixture.read".to_owned(),
                ],
                granted_scopes: Vec::new(),
                credential_epoch: 1,
                expires_at_unix_ms: Some(now + 5_000),
            },
            execution: cowd_app_protocol::ExecutionContextV1 {
                surface: "worker".to_owned(),
                session_id: None,
                turn_id: None,
                task_id: None,
            },
            payload,
        }
    }

    #[test]
    fn platform_dispatch_vocabulary_is_closed_and_unique() {
        let ids = PLATFORM_OPERATION_IDS
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(ids.len(), 2);
        assert!(supports(ACTION_PLAN_OPERATION_ID));
        assert!(supports(SURFACE_OUTBOX_LIST_OPERATION_ID));
        assert!(!supports("core.matrix.health"));
        assert!(!supports("mfg.report.list"));
    }

    #[test]
    fn platform_inputs_are_strict_and_pagination_is_bounded() {
        assert!(
            serde_json::from_value::<CrossPlaneActionPlanInput>(serde_json::json!({
                "actor_identity_ref": null,
                "source_channel": null,
                "session_id": null,
                "requested_capability": "message.feishu.send_text",
                "provider_account": null,
                "target_ref": "channel://feishu/user-1",
                "resource_ref": "text://hello",
                "risk": "low",
                "data_classification": "internal",
                "identity_trust": "verified",
                "unexpected": true
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<SurfaceOutboxListInput>(serde_json::json!({
                "surface": "all",
                "status": "active",
                "offset": 0,
                "limit": 50,
                "unexpected": true
            }))
            .is_err()
        );
        let input = serde_json::from_value::<SurfaceOutboxListInput>(serde_json::json!({
            "surface": "all",
            "status": "active",
            "offset": 0,
            "limit": 50
        }))
        .expect("typed outbox input");
        assert_eq!(input.offset, 0);
        assert_eq!(input.limit, 50);
    }

    #[tokio::test]
    async fn real_app_state_dispatches_both_core_owned_operations() {
        let state = crate::api_routes::tests::test_state();
        let plan_payload = serde_json::json!({
            "actor_identity_ref": null,
            "source_channel": "channel://wechat/chat/source",
            "session_id": "session-1",
            "requested_capability": "message.feishu.send_text",
            "provider_account": null,
            "target_ref": "channel://feishu/user/open-id-1",
            "resource_ref": "text://hello",
            "risk": "low",
            "data_classification": "internal",
            "identity_trust": "verified"
        });
        let plan_envelope = envelope(ACTION_PLAN_OPERATION_ID, plan_payload.clone());
        let plan = dispatch(
            &state,
            &plan_envelope,
            ACTION_PLAN_OPERATION_ID,
            &plan_payload,
        )
        .await
        .expect("Core cross-plane plan");
        assert_eq!(plan["action"]["actor_principal"], "signed-app-subject");
        assert_eq!(
            plan["action"]["requested_capability"],
            "message.feishu.send_text"
        );
        assert!(plan["policy_simulation"]["decision"].is_string());
        assert!(plan["action_preflight"]["executable"].is_boolean());

        let outbox_payload = serde_json::json!({
            "surface": "all",
            "status": "all",
            "offset": 0,
            "limit": 20
        });
        let outbox_envelope = envelope(SURFACE_OUTBOX_LIST_OPERATION_ID, outbox_payload.clone());
        let outbox = dispatch(
            &state,
            &outbox_envelope,
            SURFACE_OUTBOX_LIST_OPERATION_ID,
            &outbox_payload,
        )
        .await
        .expect("Core Surface outbox list");
        assert_eq!(outbox["offset"], 0);
        assert_eq!(outbox["limit"], 20);
        assert!(outbox["total"].is_number());
        assert!(outbox["records"].is_array());
    }
}
