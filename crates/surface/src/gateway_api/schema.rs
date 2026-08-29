use sha2::{Digest, Sha256};

use super::gateway_routes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewaySchemaIdentity {
    pub route_id: &'static str,
    pub request_schema: Option<&'static str>,
    pub response_schema: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExceptionalGatewaySchemaFamily {
    pub owner: &'static str,
    pub evidence_test: &'static str,
    pub schema_names: &'static [&'static str],
}

/// Validation-rich schemas whose bounds, examples, compatibility aliases, or
/// discriminators are intentionally stricter than a plain derive. All other
/// public schemas are generated from `JsonSchema` domain types.
pub const EXCEPTIONAL_GATEWAY_SCHEMAS: ExceptionalGatewaySchemaFamily =
    ExceptionalGatewaySchemaFamily {
        owner: "gateway.api-contract",
        evidence_test: "api_routes::capability_contract::tests::openapi_schema_golden_is_stable",
        schema_names: &[
            "ApprovalExactResponse",
            "ApprovalPendingResponse",
            "ApprovalRespondReceipt",
            "AuthVerifyResponse",
            "CancelSessionTurnReceipt",
            "CancelSessionTurnRequest",
            "ContextCompactionResult",
            "CreateLiveSubscriptionRequest",
            "Empty",
            "EvidenceRef",
            "HumanEntitlementProjection",
            "LiveEnvelope",
            "LiveSubscription",
            "MissionCommand",
            "MissionCommandReceipt",
            "MissionCommandResponse",
            "MissionCommandSagaRecord",
            "MissionCommandTarget",
            "MissionControlAgentNode",
            "MissionControlApprovalNode",
            "MissionControlEventLine",
            "MissionControlGraphEdge",
            "MissionControlGraphNode",
            "MissionControlGraphProjection",
            "MissionControlMissionSummary",
            "MissionControlProjection",
            "MissionControlReadiness",
            "MissionControlResponse",
            "MissionControlSessionNode",
            "MissionControlSummary",
            "MissionControlTaskNode",
            "MissionControlTeamNode",
            "MissionFocusProjection",
            "MissionMaterializedSnapshot",
            "MissionOrganizationResponse",
            "MissionProjectionDelta",
            "MissionWorkspaceProjection",
            "PatchLiveSubscriptionRequest",
            "SendMessageReceipt",
            "SendMessageRequest",
            "SessionFocusClearRequest",
            "SessionInputApplicationReceipt",
            "SessionInputCancelRequest",
            "SessionInputCursor",
            "SessionInputMutationReceipt",
            "SessionInputProjection",
            "SessionInputReclassifyRequest",
            "SessionMissionFocusRequest",
            "SessionTaskFocusRequest",
            "SlashDispatchReceipt",
            "SlashDispatchRequest",
            "StartTaskPhaseRequest",
            "StartTaskRequest",
            "TaskDetailResponse",
            "TaskFailureRequest",
            "TaskFocusProjection",
            "TaskFocusRequest",
            "TaskListResponse",
            "TaskMissionCommitResponse",
            "TaskMissionPreviewResponse",
            "TaskMissionRequest",
            "TaskPhaseArtifactRequest",
            "TaskPhaseReviewRequest",
            "TaskTransitionRequest",
            "TaskTurnsResponse",
            "TurnInboxItem",
            "TurnInboxSnapshot",
        ],
    };

/// Stable digest of the public method/path catalog. Schema producers may add
/// their own component digest, but must include this value as the route basis.
#[must_use]
pub fn gateway_route_catalog_digest() -> String {
    let mut digest = Sha256::new();
    for route in gateway_routes() {
        digest.update(route.method().as_str().as_bytes());
        digest.update([0]);
        digest.update(route.path().template().as_bytes());
        digest.update([b'\n']);
    }
    format!("{:x}", digest.finalize())
}
