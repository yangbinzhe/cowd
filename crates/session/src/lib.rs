#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

pub mod domain;
pub mod error;
pub mod lease;
pub mod lifecycle;
pub mod persistence;

pub use domain::{
    SessionBranchActivation, SessionBranchActivationPhase, SessionBranchActivationTransition,
    SessionCloseDisposition, SessionDomainEvent, SessionDomainEventPage, SessionDomainRef,
    SessionDomainScope, SessionLifecycleIntent, SessionLifecyclePhase, SessionLifecyclePlan,
    SessionLifecycleTransition, SESSION_DOMAIN_EVENT_TYPE,
};
pub use error::{Result as SessionResult, SessionError};
pub use lease::{SessionLease, SessionLeaseRegistry};
pub use lifecycle::{
    SessionActor, SessionAttachment, SessionLifecycleEvent, SessionLifecycleSnapshot,
    SessionLifecycleState, SessionPresenceLedger,
};
pub use persistence::{
    build_context_index_cards, context_index_card_digest, context_index_source_digest,
    public_session_blocks, public_terminal_payload_v1, terminal_public_payload_digest,
    ActiveSessionProjection, CompressionAlgo, ContextIndexCard, ContextIndexCoverage,
    OutboxFailureClass, OutboxStatus, SessionActivationManifest, SessionBranchRequest,
    SessionBranchResult, SessionCancellationAuthorizationPayload, SessionCancellationClaimBinding,
    SessionCancellationRequestedReference, SessionClaimEventAppend, SessionContextDiscoveryPage,
    SessionContextPage, SessionDiscoveryFilter, SessionDiscoveryKind, SessionDiscoveryPage,
    SessionDiscoveryRequest, SessionDiscoveryScope, SessionDiscoverySnapshot, SessionEvent,
    SessionHistoryReader, SessionInputAdmission, SessionLifecycleFenceRequest,
    SessionLifecycleTombstoneRequest, SessionListOptions, SessionListPage, SessionMessage,
    SessionMessageMetadata, SessionPresenceProjection, SessionProjectionRecoveryState,
    SessionRecord, SessionRecoveryManifest, SessionRecoverySignal, SessionRuntimeInputStatus,
    SessionRuntimeOutboxHealth, SessionRuntimeOutboxRecord, SessionRuntimeOutboxRequest,
    SessionSearchResult, SessionSnapshot, SessionStoreBackend, SessionTerminalExecutionFence,
    SessionTerminalPublicPayload, SessionTerminalPublicPayloadUnavailableReason,
    SessionTerminalTranscriptCommit, SessionTerminalTranscriptReceipt, SessionUsageBucket,
    SessionUsageSummary, SharedSessionStoreBackend, StorageExecutionLane,
    StorageExecutionLaneStats, StorageExecutionPlaneConfig, StorageExecutionPlaneStats,
    StorageTier, TieredSessionStore, TieredSessionStoreConfig, UnifiedSessionStore,
    CONTEXT_INDEX_CARD_SCHEMA_VERSION, SESSION_ACTIVATION_MANIFEST_SCHEMA_VERSION,
    SESSION_CANCELLATION_AUTHORIZATION_KIND, SESSION_TERMINAL_PUBLIC_PAYLOAD_DIGEST_PREFIX_V1,
    SESSION_TERMINAL_PUBLIC_PAYLOAD_KIND_V1, SESSION_TERMINAL_PUBLIC_PAYLOAD_MAX_BYTES,
};
pub use persistence::{
    SessionPresenceMutation, SessionPresenceMutationKind, SessionPresenceMutationResult,
    SessionReceiptAuthority, SessionReceiptSource, SessionTerminalAckCommand,
    SessionTerminalAckListQuery, SessionTerminalMessageBinding, SessionTerminalReceiptContext,
    SessionTerminalReceiptQuery,
};

#[cfg(test)]
mod r196_authorization_tests {
    use super::*;
    use harness_contract::turn::{CancellationCause, CancellationReceipt, CancellationStatus};

    fn request() -> SessionClaimEventAppend {
        SessionClaimEventAppend::cancellation_authorization(
            "session".into(),
            "message".into(),
            "turn".into(),
            SessionTerminalExecutionFence {
                request_id: "request".into(),
                input_sequence: 2,
                session_generation: 3,
                claim_owner: "owner".into(),
                claim_token: "secret-raw-token".into(),
                claim_fence_epoch: 4,
            },
            CancellationReceipt {
                cancellation_id: "cancel".into(),
                session_id: "session".into(),
                turn_id: "turn".into(),
                execution_id: "execution".into(),
                actor_id: "actor".into(),
                cause: CancellationCause::UserRequested,
                reason: Some("reason".into()),
                requested_at_ms: 5,
                effective_at_ms: None,
                status: CancellationStatus::Requested,
                journal_sequence: 6,
                projection_revision: 7,
            },
            SessionCancellationRequestedReference {
                event_id: "requested-event".into(),
                stream_id: "stream".into(),
                event_sequence: 6,
                commit_cursor: 7,
                payload_digest: format!("sha256:{}", "a".repeat(64)),
            },
        )
        .unwrap()
    }

    #[test]
    fn r196_authorization_canonical_payload_key_and_locked_binding() {
        let original = request();
        original.validate_locked_event_binding().unwrap();
        assert_eq!(original, request());
        assert!(!original
            .event
            .to_session_event()
            .unwrap()
            .event_json
            .contains("secret-raw-token"));
        let payload: SessionCancellationAuthorizationPayload =
            serde_json::from_value(original.event.payload.clone()).unwrap();
        // Each legitimate change must retain the same key, but change content.
        for field in 0..6 {
            let mut receipt = payload.receipt.clone();
            let mut reference = payload.requested_ref.clone();
            match field {
                0 => receipt.actor_id.push_str("-changed"),
                1 => receipt.reason = None,
                2 => receipt.execution_id.push_str("-changed"),
                3 => reference.event_id.push_str("-changed"),
                4 => reference.payload_digest = format!("sha256:{}", "b".repeat(64)),
                _ => receipt.requested_at_ms += 1,
            }
            let changed = SessionClaimEventAppend::cancellation_authorization(
                original.session_id.clone(),
                original.ingress_message_id.clone(),
                original.turn_id.clone(),
                original.fence.clone(),
                receipt,
                reference,
            )
            .unwrap();
            assert_eq!(changed.event.event_id, original.event.event_id);
            assert_ne!(changed.event, original.event);
            changed.validate_locked_event_binding().unwrap();
        }
        // Mutating public data cannot alter the private frozen purpose.
        for field in 0..20 {
            let mut bad = original.clone();
            match field {
                0 => bad.session_id.push_str("-foreign"),
                1 => bad.ingress_message_id.push_str("-foreign"),
                2 => bad.turn_id.push_str("-foreign"),
                3 => bad.fence.request_id.push_str("-foreign"),
                4 => bad.fence.input_sequence += 1,
                5 => bad.fence.session_generation += 1,
                6 => bad.fence.claim_owner.push_str("-foreign"),
                7 => bad.fence.claim_token.push_str("-foreign"),
                8 => bad.fence.claim_fence_epoch += 1,
                9 => bad.event.event_id.push_str("-foreign"),
                10 => bad.event.created_at_ms += 1,
                11 => bad.event.scope = SessionDomainScope::Session,
                12 => bad.event.refs.push(SessionDomainRef {
                    ref_type: "turn".into(),
                    id: "other".into(),
                    label: None,
                }),
                13 => bad.event.payload["schema_version"] = serde_json::json!(2),
                14 => bad.event.payload["authorization_digest"] = serde_json::json!("forged"),
                15 => bad.event.payload["receipt"]["extra"] = serde_json::json!(true),
                16 => {
                    bad.event.payload["receipt"]
                        .as_object_mut()
                        .unwrap()
                        .remove("status");
                }
                17 => bad.event.payload["claim_binding"]["extra"] = serde_json::json!(true),
                18 => bad.event.payload["requested_ref"]["commit_cursor"] = serde_json::json!(99),
                _ => bad.event.sequence = 99,
            }
            assert!(
                bad.validate_locked_event_binding().is_err(),
                "mutation {field}"
            );
        }
        let mut non_requested = payload.receipt.clone();
        non_requested.status = CancellationStatus::Cancelled;
        assert!(SessionClaimEventAppend::cancellation_authorization(
            original.session_id.clone(),
            original.ingress_message_id.clone(),
            original.turn_id.clone(),
            original.fence.clone(),
            non_requested,
            payload.requested_ref.clone()
        )
        .is_err());
        let mut effective = payload.receipt;
        effective.effective_at_ms = Some(8);
        assert!(SessionClaimEventAppend::cancellation_authorization(
            original.session_id.clone(),
            original.ingress_message_id.clone(),
            original.turn_id.clone(),
            original.fence.clone(),
            effective,
            payload.requested_ref
        )
        .is_err());
    }

    #[test]
    fn r196_reserved_kind_cannot_select_authorization_purpose() {
        let original = request();
        let observation = SessionClaimEventAppend::observation(
            original.session_id,
            original.ingress_message_id,
            original.turn_id,
            original.fence,
            original.event.clone(),
        );
        assert!(observation.validate_locked_event_binding().is_err());
        let event = original.event.to_session_event().unwrap();
        assert!(event.reject_reserved_cancellation_authorization().is_err());
        let mut disguised = event.clone();
        disguised.event_type = "ContextEnvelope".into();
        assert!(disguised
            .reject_reserved_cancellation_authorization()
            .is_err());
        let mut ordinary = original.event;
        ordinary.kind = "r194.observation".into();
        ordinary
            .to_session_event()
            .unwrap()
            .reject_reserved_cancellation_authorization()
            .unwrap();
    }
}
