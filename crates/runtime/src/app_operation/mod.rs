//! Runtime-owned ports for governed APP operations.
//!
//! Runtime knows the stable wire semantics but not Gateway transports,
//! supervisors, APP repositories, or APP databases. Gateway supplies the
//! production adapter at its composition root.

use std::{pin::Pin, sync::Arc};

use async_trait::async_trait;
use cowd_app_protocol::{
    AppArtifactRefV1, AppId, AppInvocationEnvelopeV1, AppProviderResponseV1, AppStreamFrameV1,
    DurableReceiptV1, GenerationId, OperationDescriptorV1, OperationKindV1,
};
use futures::Stream;
use thiserror::Error;

pub type AppOperationStream =
    Pin<Box<dyn Stream<Item = Result<AppStreamFrameV1, AppOperationError>> + Send>>;

#[derive(Debug, Clone, PartialEq)]
pub struct AppOperationRequest {
    pub app_id: AppId,
    pub expected_generation: Option<GenerationId>,
    pub envelope: AppInvocationEnvelopeV1,
}

impl AppOperationRequest {
    pub fn validate(&self, descriptor: &OperationDescriptorV1) -> Result<(), AppOperationError> {
        self.app_id
            .validate_value()
            .map_err(AppOperationError::invalid_request)?;
        if let Some(generation) = &self.expected_generation {
            generation
                .validate_value()
                .map_err(AppOperationError::invalid_request)?;
        }
        self.envelope
            .validate_for(descriptor)
            .map_err(AppOperationError::invalid_request)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppOperationRecovery {
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
    pub operator_action: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppOperationFailureKind {
    InvalidRequest,
    NotFound,
    NotGranted,
    Unavailable,
    Blocked,
    DeadlineExceeded,
    Cancelled,
    RevisionConflict,
    IdempotencyConflict,
    CallCycleDetected,
    ProtocolIncompatible,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("APP operation {kind:?}: {message}")]
pub struct AppOperationError {
    pub kind: AppOperationFailureKind,
    pub message: String,
    pub recovery: AppOperationRecovery,
    pub receipt_id: Option<String>,
}

impl AppOperationError {
    fn invalid_request(error: impl std::fmt::Display) -> Self {
        Self {
            kind: AppOperationFailureKind::InvalidRequest,
            message: error.to_string(),
            recovery: AppOperationRecovery {
                retryable: false,
                retry_after_ms: None,
                operator_action: None,
            },
            receipt_id: None,
        }
    }

    #[must_use]
    pub fn unavailable(message: impl Into<String>, retry_after_ms: u64) -> Self {
        Self {
            kind: AppOperationFailureKind::Unavailable,
            message: message.into(),
            recovery: AppOperationRecovery {
                retryable: true,
                retry_after_ms: Some(retry_after_ms),
                operator_action: Some("inspect the APP with `cowd apps doctor`".to_owned()),
            },
            receipt_id: None,
        }
    }

    #[must_use]
    pub fn blocked(message: impl Into<String>) -> Self {
        Self {
            kind: AppOperationFailureKind::Blocked,
            message: message.into(),
            recovery: AppOperationRecovery {
                retryable: false,
                retry_after_ms: None,
                operator_action: Some("grant the required APP capability".to_owned()),
            },
            receipt_id: None,
        }
    }
}

#[async_trait]
pub trait AppOperationPort: Send + Sync {
    async fn operations(
        &self,
        app_id: &AppId,
    ) -> Result<Vec<OperationDescriptorV1>, AppOperationError>;

    async fn query(
        &self,
        request: AppOperationRequest,
    ) -> Result<AppProviderResponseV1, AppOperationError>;

    async fn command(
        &self,
        request: AppOperationRequest,
    ) -> Result<DurableReceiptV1, AppOperationError>;

    async fn subscribe(
        &self,
        request: AppOperationRequest,
    ) -> Result<AppOperationStream, AppOperationError>;

    async fn export(
        &self,
        request: AppOperationRequest,
    ) -> Result<(AppArtifactRefV1, AppOperationStream), AppOperationError>;

    async fn receipt(
        &self,
        app_id: &AppId,
        receipt_id: &str,
    ) -> Result<DurableReceiptV1, AppOperationError>;
}

#[async_trait]
pub trait CoreBridgePort: Send + Sync {
    async fn operations(
        &self,
        app_id: &AppId,
    ) -> Result<Vec<OperationDescriptorV1>, AppOperationError>;

    async fn invoke(
        &self,
        app_id: &AppId,
        envelope: AppInvocationEnvelopeV1,
    ) -> Result<AppProviderResponseV1, AppOperationError>;

    async fn command(
        &self,
        app_id: &AppId,
        envelope: AppInvocationEnvelopeV1,
    ) -> Result<DurableReceiptV1, AppOperationError>;

    async fn stream(
        &self,
        app_id: &AppId,
        envelope: AppInvocationEnvelopeV1,
    ) -> Result<AppOperationStream, AppOperationError>;
}

#[derive(Clone)]
pub struct AppOperationInvoker {
    port: Arc<dyn AppOperationPort>,
}

impl AppOperationInvoker {
    #[must_use]
    pub fn new(port: Arc<dyn AppOperationPort>) -> Self {
        Self { port }
    }

    pub async fn invoke(
        &self,
        request: AppOperationRequest,
    ) -> Result<AppOperationOutcome, AppOperationError> {
        let descriptor = self
            .port
            .operations(&request.app_id)
            .await?
            .into_iter()
            .find(|candidate| candidate.operation_id == request.envelope.operation_id)
            .ok_or_else(|| {
                AppOperationError::blocked("operation is not present in the granted catalog")
            })?;
        request.validate(&descriptor)?;
        match descriptor.kind {
            OperationKindV1::Query => self
                .port
                .query(request)
                .await
                .map(AppOperationOutcome::Query),
            OperationKindV1::Command => self
                .port
                .command(request)
                .await
                .map(AppOperationOutcome::Command),
            OperationKindV1::Subscribe => self
                .port
                .subscribe(request)
                .await
                .map(AppOperationOutcome::Stream),
            OperationKindV1::Export => {
                let (artifact, stream) = self.port.export(request).await?;
                Ok(AppOperationOutcome::Export { artifact, stream })
            }
        }
    }
}

pub enum AppOperationOutcome {
    Query(AppProviderResponseV1),
    Command(DurableReceiptV1),
    Stream(AppOperationStream),
    Export {
        artifact: AppArtifactRefV1,
        stream: AppOperationStream,
    },
}

impl std::fmt::Debug for AppOperationOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(response) => formatter.debug_tuple("Query").field(response).finish(),
            Self::Command(receipt) => formatter.debug_tuple("Command").field(receipt).finish(),
            Self::Stream(_) => formatter.write_str("Stream(<active>)"),
            Self::Export { artifact, .. } => formatter
                .debug_struct("Export")
                .field("artifact", artifact)
                .finish_non_exhaustive(),
        }
    }
}

#[cfg(test)]
mod tests {
    use cowd_app_protocol::{
        DelegationKindV1, ExecutionContextV1, IdempotencySemanticsV1, OperationDelegationV1,
        PrincipalContextV1, ProtocolValidationError, ReceiptStatusV1, Sha256Digest,
    };
    use futures::stream;
    use serde_json::json;

    use super::*;

    const DIGEST: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    struct FakePort {
        descriptor: OperationDescriptorV1,
    }

    #[async_trait]
    impl AppOperationPort for FakePort {
        async fn operations(
            &self,
            _app_id: &AppId,
        ) -> Result<Vec<OperationDescriptorV1>, AppOperationError> {
            Ok(vec![self.descriptor.clone()])
        }

        async fn query(
            &self,
            request: AppOperationRequest,
        ) -> Result<AppProviderResponseV1, AppOperationError> {
            Ok(AppProviderResponseV1 {
                schema_version: 1,
                request_id: request.envelope.request_id,
                output_schema_digest: digest(),
                revision: Some("1".to_owned()),
                payload: json!({"value": 1}),
            })
        }

        async fn command(
            &self,
            request: AppOperationRequest,
        ) -> Result<DurableReceiptV1, AppOperationError> {
            Ok(DurableReceiptV1 {
                schema_version: 1,
                request_id: request.envelope.request_id,
                receipt_id: "receipt:1".to_owned(),
                idempotency_key: request
                    .envelope
                    .idempotency_key
                    .ok_or_else(|| AppOperationError::invalid_request("missing idempotency key"))?,
                status: ReceiptStatusV1::Completed,
                result_revision: Some("2".to_owned()),
                replayed: false,
                payload_digest: digest(),
                payload: json!({"completed": true}),
            })
        }

        async fn subscribe(
            &self,
            _request: AppOperationRequest,
        ) -> Result<AppOperationStream, AppOperationError> {
            Ok(Box::pin(stream::empty()))
        }

        async fn export(
            &self,
            _request: AppOperationRequest,
        ) -> Result<(AppArtifactRefV1, AppOperationStream), AppOperationError> {
            Err(AppOperationError::blocked("not used by fixture"))
        }

        async fn receipt(
            &self,
            _app_id: &AppId,
            _receipt_id: &str,
        ) -> Result<DurableReceiptV1, AppOperationError> {
            Err(AppOperationError::blocked("not used by fixture"))
        }
    }

    fn digest() -> Sha256Digest {
        Sha256Digest(DIGEST.to_owned())
    }

    fn descriptor(kind: OperationKindV1) -> OperationDescriptorV1 {
        let (read_only, idempotency) = match kind {
            OperationKindV1::Query => (true, IdempotencySemanticsV1::ReadOnly),
            OperationKindV1::Command => (false, IdempotencySemanticsV1::Required),
            OperationKindV1::Subscribe => (true, IdempotencySemanticsV1::SubscriptionCursor),
            OperationKindV1::Export => (true, IdempotencySemanticsV1::ContentAddressed),
        };
        OperationDescriptorV1 {
            operation_id: "reference.operation.v1".to_owned(),
            kind,
            input_schema_digest: digest(),
            output_schema_digest: digest(),
            required_capabilities: vec!["reference-app.use".to_owned()],
            delegation: OperationDelegationV1::Either,
            tenant_scoped: true,
            workspace_scoped: true,
            read_only,
            idempotency,
            default_deadline_ms: 1_000,
            maximum_deadline_ms: 5_000,
            maximum_request_bytes: 1024,
            maximum_response_bytes: 4096,
            maximum_frame_bytes: 1024,
            streaming: matches!(kind, OperationKindV1::Subscribe | OperationKindV1::Export),
            replay_window_seconds: matches!(kind, OperationKindV1::Subscribe).then_some(60),
            degraded_read_allowed: read_only,
            audit_classification: "test".to_owned(),
        }
    }

    fn request(idempotency_key: Option<&str>) -> AppOperationRequest {
        AppOperationRequest {
            app_id: AppId("reference-app".to_owned()),
            expected_generation: None,
            envelope: AppInvocationEnvelopeV1 {
                schema_version: 1,
                operation_id: "reference.operation.v1".to_owned(),
                request_id: "request:1".to_owned(),
                correlation_id: "correlation:1".to_owned(),
                causation_id: None,
                deadline_unix_ms: 4_000_000_000_000,
                idempotency_key: idempotency_key.map(str::to_owned),
                expected_revision: None,
                call_chain: vec!["core:runtime".to_owned()],
                max_hops: 4,
                input_schema_digest: digest(),
                principal: PrincipalContextV1 {
                    subject: "user:1".to_owned(),
                    tenant_id: "tenant:1".to_owned(),
                    workspace_id: "workspace:1".to_owned(),
                    delegation: DelegationKindV1::User,
                    grant_id: "grant:1".to_owned(),
                    authorization_profile_id: "operator".to_owned(),
                    authorization_revision: 1,
                    granted_capabilities: vec!["reference-app.use".to_owned()],
                    granted_scopes: vec!["workspace:read".to_owned()],
                    credential_epoch: 1,
                    expires_at_unix_ms: None,
                },
                execution: ExecutionContextV1 {
                    surface: "test".to_owned(),
                    session_id: None,
                    turn_id: None,
                    task_id: None,
                },
                payload: json!({}),
            },
        }
    }

    #[tokio::test]
    async fn invoker_routes_query_through_the_port() {
        let invoker = AppOperationInvoker::new(Arc::new(FakePort {
            descriptor: descriptor(OperationKindV1::Query),
        }));
        let result = invoker.invoke(request(None)).await.expect("query result");
        assert!(matches!(result, AppOperationOutcome::Query(_)));
    }

    #[tokio::test]
    async fn command_without_idempotency_fails_before_the_adapter() {
        let invoker = AppOperationInvoker::new(Arc::new(FakePort {
            descriptor: descriptor(OperationKindV1::Command),
        }));
        let error = invoker
            .invoke(request(None))
            .await
            .expect_err("command must be rejected");
        assert_eq!(error.kind, AppOperationFailureKind::InvalidRequest);
    }

    #[test]
    fn repeated_authority_is_rejected_by_the_protocol_boundary() {
        let mut request = request(None);
        let error = request
            .envelope
            .append_authority("core:runtime".to_owned())
            .expect_err("cycle must be rejected");
        assert!(matches!(
            error,
            ProtocolValidationError::InvalidField {
                field: "call_chain",
                ..
            }
        ));
    }

    #[test]
    fn operation_errors_preserve_recovery_instructions() {
        let error = AppOperationError::unavailable("worker is starting", 250);
        assert!(error.recovery.retryable);
        assert_eq!(error.recovery.retry_after_ms, Some(250));
        assert!(error.recovery.operator_action.is_some());
    }
}
