//! Product-neutral APP V1 transport names and authentication primitives.
//!
//! These helpers deliberately contain no HTTP client/server or process logic.
//! Callers own transport I/O; this module freezes the bytes both sides sign.

use std::fmt;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{
    digest::{CtOutput, Output},
    Hmac, Mac,
};
use sha2::Sha256;
use thiserror::Error;
use zeroize::Zeroize;

use crate::{AppId, GenerationId};

pub const ENV_APP_ID_V1: &str = "COWD_APP_ID";
pub const ENV_APP_GENERATION_V1: &str = "COWD_APP_GENERATION";
pub const ENV_APP_SOCKET_V1: &str = "COWD_APP_SOCKET";
pub const ENV_APP_CREDENTIAL_FILE_V1: &str = "COWD_APP_CREDENTIAL_FILE";
pub const ENV_CORE_BRIDGE_SOCKET_V1: &str = "COWD_CORE_BRIDGE_SOCKET";
pub const ENV_APP_DATA_DIR_V1: &str = "COWD_APP_DATA_DIR";
pub const ENV_APP_CONFIG_FILE_V1: &str = "COWD_APP_CONFIG_FILE";
pub const ENV_APP_LOG_FORMAT_V1: &str = "COWD_APP_LOG_FORMAT";

pub const APP_HANDSHAKE_PATH_V1: &str = "/_cowd/v1/handshake";
pub const APP_HEALTH_PATH_V1: &str = "/_cowd/v1/health";
pub const APP_OPERATIONS_PATH_V1: &str = "/_cowd/v1/operations";
pub const APP_OPERATION_INVOKE_PATH_V1: &str = "/_cowd/v1/operations/{operation_id}/invoke";
pub const APP_OPERATION_STREAM_PATH_V1: &str = "/_cowd/v1/operations/{operation_id}/stream";
/// Opens one TUI view. The request body is an `AppInvocationEnvelopeV1`
/// selected by the view's signed `open_operation_id`.
pub const APP_TUI_VIEW_OPEN_PATH_V1: &str = "/_cowd/v1/tui/views/{view_id}/open";
/// Invokes one document-local action discriminator through the view's signed
/// `action_operation_id`.
pub const APP_TUI_VIEW_ACTION_PATH_V1: &str = "/_cowd/v1/tui/views/{view_id}/actions/{action_id}";
/// Opens the patch stream selected by the view's signed
/// `stream_operation_id`.
pub const APP_TUI_VIEW_STREAM_PATH_V1: &str = "/_cowd/v1/tui/views/{view_id}/stream";
pub const APP_SUBSCRIPTION_ACK_PATH_V1: &str = "/_cowd/v1/subscriptions/{subscription_id}/ack";
pub const APP_SUBSCRIPTION_PATH_V1: &str = "/_cowd/v1/subscriptions/{subscription_id}";
pub const APP_RECEIPT_PATH_V1: &str = "/_cowd/v1/receipts/{receipt_id}";
pub const APP_SHUTDOWN_PATH_V1: &str = "/_cowd/v1/shutdown";

pub const CORE_OPERATIONS_PATH_V1: &str = "/_cowd/core/v1/operations";
pub const CORE_OPERATION_INVOKE_PATH_V1: &str = "/_cowd/core/v1/operations/{operation_id}/invoke";
pub const CORE_OPERATION_STREAM_PATH_V1: &str = "/_cowd/core/v1/operations/{operation_id}/stream";
pub const CORE_SUBSCRIPTION_ACK_PATH_V1: &str =
    "/_cowd/core/v1/subscriptions/{subscription_id}/ack";
pub const CORE_SUBSCRIPTION_PATH_V1: &str = "/_cowd/core/v1/subscriptions/{subscription_id}";
pub const CORE_RECEIPT_PATH_V1: &str = "/_cowd/core/v1/receipts/{receipt_id}";

pub const UNARY_CONTENT_TYPE_V1: &str = "application/vnd.cowd.app+json;version=1";
pub const STREAM_CONTENT_TYPE_V1: &str = "application/vnd.cowd.app.ndjson;version=1";

pub const HEADER_AUTHORIZATION_V1: &str = "authorization";
pub const HEADER_CONTENT_TYPE_V1: &str = "content-type";
pub const HEADER_PROTOCOL_VERSION_V1: &str = "x-cowd-protocol-version";
pub const HEADER_APP_ID_V1: &str = "x-cowd-app-id";
pub const HEADER_APP_GENERATION_V1: &str = "x-cowd-app-generation";
pub const HEADER_REQUEST_ID_V1: &str = "x-cowd-request-id";
pub const HEADER_CORRELATION_ID_V1: &str = "x-cowd-correlation-id";
pub const HEADER_CAUSATION_ID_V1: &str = "x-cowd-causation-id";
pub const HEADER_DEADLINE_UNIX_MS_V1: &str = "x-cowd-deadline-unix-ms";
pub const HEADER_TRACEPARENT_V1: &str = "traceparent";
pub const HEADER_PRINCIPAL_TOKEN_V1: &str = "x-cowd-principal-token";
pub const HEADER_TENANT_ID_V1: &str = "x-cowd-tenant-id";
pub const HEADER_WORKSPACE_ID_V1: &str = "x-cowd-workspace-id";
pub const HEADER_SESSION_ID_V1: &str = "x-cowd-session-id";
pub const HEADER_TURN_ID_V1: &str = "x-cowd-turn-id";
pub const HEADER_TASK_ID_V1: &str = "x-cowd-task-id";

pub const BOOTSTRAP_AUTH_SCHEME_V1: &str = "CowdBootstrap";
pub const CHANNEL_AUTH_SCHEME_V1: &str = "CowdChannel";
pub const BOOTSTRAP_SECRET_BYTES_V1: usize = 32;
pub const CHANNEL_TOKEN_BYTES_V1: usize = 32;
pub const MAX_WORKER_NONCE_BYTES_V1: usize = 256;
pub const CHANNEL_BINDING_DOMAIN_V1: &str = "cowd.app.channel-token/v1";
pub const WORKER_CHANNEL_PURPOSE_V1: &str = "worker-channel";
pub const CORE_BRIDGE_PURPOSE_V1: &str = "core-bridge";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelPurposeV1 {
    WorkerChannel,
    CoreBridge,
}

impl ChannelPurposeV1 {
    const fn label(self) -> &'static [u8] {
        match self {
            Self::WorkerChannel => WORKER_CHANNEL_PURPOSE_V1.as_bytes(),
            Self::CoreBridge => CORE_BRIDGE_PURPOSE_V1.as_bytes(),
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TransportAuthError {
    #[error("invalid APP identity: {0}")]
    InvalidIdentity(String),
    #[error("bootstrap secret must contain exactly 32 bytes, got {actual}")]
    InvalidSecretLength { actual: usize },
    #[error("channel token must contain exactly 32 bytes, got {actual}")]
    InvalidTokenLength { actual: usize },
    #[error("invalid unpadded base64url value")]
    InvalidBase64Url,
    #[error("invalid Authorization value")]
    InvalidAuthorization,
    #[error("worker PID must be non-zero")]
    InvalidWorkerPid,
    #[error("worker nonce must contain between 1 and 256 bytes")]
    InvalidWorkerNonce,
    #[error("channel authentication failed")]
    AuthenticationFailed,
    #[error("channel binding field exceeds the V1 encoding limit")]
    BindingFieldTooLong,
}

pub struct BootstrapSecretV1([u8; BOOTSTRAP_SECRET_BYTES_V1]);

impl BootstrapSecretV1 {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TransportAuthError> {
        let value: [u8; BOOTSTRAP_SECRET_BYTES_V1] =
            bytes
                .try_into()
                .map_err(|_| TransportAuthError::InvalidSecretLength {
                    actual: bytes.len(),
                })?;
        Ok(Self(value))
    }

    pub fn parse_base64url(encoded: &str) -> Result<Self, TransportAuthError> {
        let mut decoded = decode_canonical_base64url(encoded)?;
        let secret = Self::from_bytes(&decoded);
        decoded.zeroize();
        secret
    }

    #[must_use]
    pub fn expose_base64url(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0)
    }

    fn as_bytes(&self) -> &[u8; BOOTSTRAP_SECRET_BYTES_V1] {
        &self.0
    }
}

impl fmt::Debug for BootstrapSecretV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BootstrapSecretV1([REDACTED])")
    }
}

impl Drop for BootstrapSecretV1 {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

pub struct ChannelTokenV1([u8; CHANNEL_TOKEN_BYTES_V1]);

impl ChannelTokenV1 {
    fn parse_base64url(encoded: &str) -> Result<Self, TransportAuthError> {
        let mut decoded = decode_canonical_base64url(encoded)?;
        let value: Result<[u8; CHANNEL_TOKEN_BYTES_V1], TransportAuthError> = decoded
            .as_slice()
            .try_into()
            .map_err(|_| TransportAuthError::InvalidTokenLength {
                actual: decoded.len(),
            });
        decoded.zeroize();
        value.map(Self)
    }

    #[must_use]
    pub fn expose_base64url(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0)
    }

    fn as_bytes(&self) -> &[u8; CHANNEL_TOKEN_BYTES_V1] {
        &self.0
    }
}

impl fmt::Debug for ChannelTokenV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ChannelTokenV1([REDACTED])")
    }
}

impl Drop for ChannelTokenV1 {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[must_use]
pub fn format_bootstrap_authorization_v1(secret: &BootstrapSecretV1) -> String {
    format!("{BOOTSTRAP_AUTH_SCHEME_V1} {}", secret.expose_base64url())
}

pub fn parse_bootstrap_authorization_v1(
    authorization: &str,
) -> Result<BootstrapSecretV1, TransportAuthError> {
    let encoded = parse_authorization(authorization, BOOTSTRAP_AUTH_SCHEME_V1)?;
    BootstrapSecretV1::parse_base64url(encoded)
}

/// Verifies a strict bootstrap Authorization value without exposing or cloning
/// the expected bootstrap secret.
pub fn verify_bootstrap_authorization_v1(
    expected: &BootstrapSecretV1,
    authorization: &str,
) -> Result<(), TransportAuthError> {
    let encoded = parse_authorization(authorization, BOOTSTRAP_AUTH_SCHEME_V1)?;
    let candidate = BootstrapSecretV1::parse_base64url(encoded)?;
    verify_fixed_secret_bytes(expected.as_bytes(), candidate.as_bytes())
}

#[must_use]
pub fn format_channel_authorization_v1(token: &ChannelTokenV1) -> String {
    format!("{CHANNEL_AUTH_SCHEME_V1} {}", token.expose_base64url())
}

/// Verifies a strict channel Authorization value against an already-derived
/// token, allowing the bootstrap secret to be dropped after the handshake.
pub fn verify_channel_token_authorization_v1(
    expected: &ChannelTokenV1,
    authorization: &str,
) -> Result<(), TransportAuthError> {
    let encoded = parse_authorization(authorization, CHANNEL_AUTH_SCHEME_V1)?;
    let candidate = ChannelTokenV1::parse_base64url(encoded)?;
    verify_fixed_secret_bytes(expected.as_bytes(), candidate.as_bytes())
}

pub fn derive_channel_token_v1(
    secret: &BootstrapSecretV1,
    purpose: ChannelPurposeV1,
    app_id: &AppId,
    generation: &GenerationId,
    worker_pid: u32,
    worker_nonce: &str,
) -> Result<ChannelTokenV1, TransportAuthError> {
    let binding =
        canonical_channel_binding_v1(purpose, app_id, generation, worker_pid, worker_nonce)?;
    let mut hmac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|_| TransportAuthError::AuthenticationFailed)?;
    hmac.update(&binding);
    Ok(ChannelTokenV1(hmac.finalize().into_bytes().into()))
}

pub fn verify_channel_authorization_v1(
    secret: &BootstrapSecretV1,
    purpose: ChannelPurposeV1,
    app_id: &AppId,
    generation: &GenerationId,
    worker_pid: u32,
    worker_nonce: &str,
    authorization: &str,
) -> Result<(), TransportAuthError> {
    let encoded = parse_authorization(authorization, CHANNEL_AUTH_SCHEME_V1)?;
    let candidate = ChannelTokenV1::parse_base64url(encoded)?;
    let expected = derive_channel_token_v1(
        secret,
        purpose,
        app_id,
        generation,
        worker_pid,
        worker_nonce,
    )?;
    verify_fixed_secret_bytes(expected.as_bytes(), candidate.as_bytes())
}

fn verify_fixed_secret_bytes(
    expected: &[u8; CHANNEL_TOKEN_BYTES_V1],
    candidate: &[u8; CHANNEL_TOKEN_BYTES_V1],
) -> Result<(), TransportAuthError> {
    // `CtOutput` delegates equality to RustCrypto's `subtle` implementation.
    // Use the existing HMAC output type solely as a fixed-size, constant-time
    // comparison carrier; no new key material or wire representation exists.
    let mut expected_output = Output::<Hmac<Sha256>>::default();
    expected_output.copy_from_slice(expected);
    let mut candidate_output = Output::<Hmac<Sha256>>::default();
    candidate_output.copy_from_slice(candidate);
    let expected_output = CtOutput::<Hmac<Sha256>>::new(expected_output);
    let candidate_output = CtOutput::<Hmac<Sha256>>::new(candidate_output);
    let authenticated = expected_output == candidate_output;

    let mut expected_output = expected_output.into_bytes();
    expected_output.as_mut_slice().zeroize();
    let mut candidate_output = candidate_output.into_bytes();
    candidate_output.as_mut_slice().zeroize();

    authenticated
        .then_some(())
        .ok_or(TransportAuthError::AuthenticationFailed)
}

fn canonical_channel_binding_v1(
    purpose: ChannelPurposeV1,
    app_id: &AppId,
    generation: &GenerationId,
    worker_pid: u32,
    worker_nonce: &str,
) -> Result<Vec<u8>, TransportAuthError> {
    app_id
        .validate_value()
        .map_err(|error| TransportAuthError::InvalidIdentity(error.to_string()))?;
    generation
        .validate_value()
        .map_err(|error| TransportAuthError::InvalidIdentity(error.to_string()))?;
    if worker_pid == 0 {
        return Err(TransportAuthError::InvalidWorkerPid);
    }
    if worker_nonce.is_empty() || worker_nonce.len() > MAX_WORKER_NONCE_BYTES_V1 {
        return Err(TransportAuthError::InvalidWorkerNonce);
    }

    let mut binding = Vec::with_capacity(
        CHANNEL_BINDING_DOMAIN_V1.len()
            + purpose.label().len()
            + app_id.0.len()
            + generation.0.len()
            + worker_nonce.len()
            + 24,
    );
    append_field(&mut binding, CHANNEL_BINDING_DOMAIN_V1.as_bytes())?;
    append_field(&mut binding, purpose.label())?;
    append_field(&mut binding, app_id.0.as_bytes())?;
    append_field(&mut binding, generation.0.as_bytes())?;
    append_field(&mut binding, &worker_pid.to_be_bytes())?;
    append_field(&mut binding, worker_nonce.as_bytes())?;
    Ok(binding)
}

fn append_field(output: &mut Vec<u8>, value: &[u8]) -> Result<(), TransportAuthError> {
    let length = u32::try_from(value.len()).map_err(|_| TransportAuthError::BindingFieldTooLong)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn parse_authorization<'a>(
    authorization: &'a str,
    expected_scheme: &str,
) -> Result<&'a str, TransportAuthError> {
    let prefix = format!("{expected_scheme} ");
    let encoded = authorization
        .strip_prefix(&prefix)
        .filter(|encoded| {
            !encoded.is_empty() && !encoded.bytes().any(|byte| byte.is_ascii_whitespace())
        })
        .ok_or(TransportAuthError::InvalidAuthorization)?;
    Ok(encoded)
}

fn decode_canonical_base64url(encoded: &str) -> Result<Vec<u8>, TransportAuthError> {
    if encoded.is_empty() || encoded.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(TransportAuthError::InvalidBase64Url);
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| TransportAuthError::InvalidBase64Url)?;
    if URL_SAFE_NO_PAD.encode(&decoded) != encoded {
        return Err(TransportAuthError::InvalidBase64Url);
    }
    Ok(decoded)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn secret() -> BootstrapSecretV1 {
        BootstrapSecretV1::from_bytes(&[0x42; BOOTSTRAP_SECRET_BYTES_V1]).unwrap()
    }

    fn app_id() -> AppId {
        AppId("reference-app".to_string())
    }

    fn generation() -> GenerationId {
        GenerationId(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        )
    }

    #[test]
    fn bootstrap_authorization_round_trips_strict_base64url() {
        let expected = secret();
        let authorization = format_bootstrap_authorization_v1(&expected);
        assert_eq!(
            authorization,
            "CowdBootstrap QkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkI"
        );
        assert_eq!(
            parse_bootstrap_authorization_v1(&authorization)
                .unwrap()
                .expose_base64url(),
            authorization.split_once(' ').unwrap().1
        );
        verify_bootstrap_authorization_v1(&expected, &authorization).unwrap();
        for invalid in [
            "Bearer QkJC",
            "CowdBootstrap",
            "CowdBootstrap  QkJC",
            " CowdBootstrap QkJC",
            "CowdBootstrap QkJC=",
            "CowdBootstrap Qk+J",
        ] {
            assert!(parse_bootstrap_authorization_v1(invalid).is_err());
        }
        for size in [0, 1, 31, 33, 64] {
            let encoded = URL_SAFE_NO_PAD.encode(vec![7; size]);
            assert!(BootstrapSecretV1::parse_base64url(&encoded).is_err());
            assert!(verify_bootstrap_authorization_v1(
                &expected,
                &format!("{BOOTSTRAP_AUTH_SCHEME_V1} {encoded}"),
            )
            .is_err());
        }

        let wrong = BootstrapSecretV1::from_bytes(&[0x43; BOOTSTRAP_SECRET_BYTES_V1]).unwrap();
        assert_eq!(
            verify_bootstrap_authorization_v1(
                &expected,
                &format_bootstrap_authorization_v1(&wrong),
            ),
            Err(TransportAuthError::AuthenticationFailed)
        );
        assert_eq!(
            verify_bootstrap_authorization_v1(&expected, &format!("{authorization}="),),
            Err(TransportAuthError::InvalidBase64Url)
        );
    }

    #[test]
    fn channel_token_matches_frozen_golden_vector() {
        let token = derive_channel_token_v1(
            &secret(),
            ChannelPurposeV1::WorkerChannel,
            &app_id(),
            &generation(),
            4242,
            "reference-worker-nonce",
        )
        .unwrap();
        assert_eq!(
            token.expose_base64url(),
            "5e07V-hGEKPA59ZGnUVHfPh9LGNqfhSSRv0bVB7tFNo"
        );
    }

    #[test]
    fn channel_authorization_round_trips_and_is_domain_separated() {
        let secret = secret();
        let worker = derive_channel_token_v1(
            &secret,
            ChannelPurposeV1::WorkerChannel,
            &app_id(),
            &generation(),
            4242,
            "nonce",
        )
        .unwrap();
        let core = derive_channel_token_v1(
            &secret,
            ChannelPurposeV1::CoreBridge,
            &app_id(),
            &generation(),
            4242,
            "nonce",
        )
        .unwrap();
        assert_ne!(worker.expose_base64url(), core.expose_base64url());
        let authorization = format_channel_authorization_v1(&worker);
        verify_channel_token_authorization_v1(&worker, &authorization).unwrap();
        verify_channel_authorization_v1(
            &secret,
            ChannelPurposeV1::WorkerChannel,
            &app_id(),
            &generation(),
            4242,
            "nonce",
            &authorization,
        )
        .unwrap();
        assert_eq!(
            verify_channel_authorization_v1(
                &secret,
                ChannelPurposeV1::CoreBridge,
                &app_id(),
                &generation(),
                4242,
                "nonce",
                &authorization,
            ),
            Err(TransportAuthError::AuthenticationFailed)
        );
        for (app_id, generation, worker_pid, worker_nonce) in [
            (AppId("other-app".to_string()), generation(), 4242, "nonce"),
            (
                app_id(),
                GenerationId(
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_string(),
                ),
                4242,
                "nonce",
            ),
            (app_id(), generation(), 4243, "nonce"),
            (app_id(), generation(), 4242, "other-nonce"),
        ] {
            assert_eq!(
                verify_channel_authorization_v1(
                    &secret,
                    ChannelPurposeV1::WorkerChannel,
                    &app_id,
                    &generation,
                    worker_pid,
                    worker_nonce,
                    &authorization,
                ),
                Err(TransportAuthError::AuthenticationFailed)
            );
        }
    }

    #[test]
    fn derived_channel_token_rejects_every_binding_mismatch() {
        let secret = secret();
        let expected = derive_channel_token_v1(
            &secret,
            ChannelPurposeV1::WorkerChannel,
            &app_id(),
            &generation(),
            4242,
            "nonce",
        )
        .unwrap();
        let mismatched = [
            derive_channel_token_v1(
                &secret,
                ChannelPurposeV1::CoreBridge,
                &app_id(),
                &generation(),
                4242,
                "nonce",
            )
            .unwrap(),
            derive_channel_token_v1(
                &secret,
                ChannelPurposeV1::WorkerChannel,
                &AppId("other-app".to_string()),
                &generation(),
                4242,
                "nonce",
            )
            .unwrap(),
            derive_channel_token_v1(
                &secret,
                ChannelPurposeV1::WorkerChannel,
                &app_id(),
                &GenerationId(
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_string(),
                ),
                4242,
                "nonce",
            )
            .unwrap(),
            derive_channel_token_v1(
                &secret,
                ChannelPurposeV1::WorkerChannel,
                &app_id(),
                &generation(),
                4243,
                "nonce",
            )
            .unwrap(),
            derive_channel_token_v1(
                &secret,
                ChannelPurposeV1::WorkerChannel,
                &app_id(),
                &generation(),
                4242,
                "other-nonce",
            )
            .unwrap(),
        ];
        for candidate in mismatched {
            assert_eq!(
                verify_channel_token_authorization_v1(
                    &expected,
                    &format_channel_authorization_v1(&candidate),
                ),
                Err(TransportAuthError::AuthenticationFailed)
            );
        }
    }

    #[test]
    fn channel_token_authorization_is_strict_and_survives_secret_drop() {
        let token = {
            let bootstrap_secret = secret();
            derive_channel_token_v1(
                &bootstrap_secret,
                ChannelPurposeV1::WorkerChannel,
                &app_id(),
                &generation(),
                9,
                "ephemeral-secret",
            )
            .unwrap()
        };
        let authorization = format_channel_authorization_v1(&token);
        verify_channel_token_authorization_v1(&token, &authorization).unwrap();

        let mut tampered_bytes = *token.as_bytes();
        tampered_bytes[CHANNEL_TOKEN_BYTES_V1 - 1] ^= 1;
        let tampered = format!(
            "{CHANNEL_AUTH_SCHEME_V1} {}",
            URL_SAFE_NO_PAD.encode(tampered_bytes)
        );
        tampered_bytes.zeroize();
        assert_eq!(
            verify_channel_token_authorization_v1(&token, &tampered),
            Err(TransportAuthError::AuthenticationFailed)
        );

        for invalid in [
            format!("{BOOTSTRAP_AUTH_SCHEME_V1} {}", token.expose_base64url()),
            format!("{authorization}="),
            format!(
                "{CHANNEL_AUTH_SCHEME_V1} {}",
                URL_SAFE_NO_PAD.encode([0_u8; 31])
            ),
            format!(
                "{CHANNEL_AUTH_SCHEME_V1} {}",
                URL_SAFE_NO_PAD.encode([0_u8; 33])
            ),
            format!("{CHANNEL_AUTH_SCHEME_V1} !!!"),
        ] {
            assert!(verify_channel_token_authorization_v1(&token, &invalid).is_err());
        }
    }

    #[test]
    fn repeated_token_verification_is_stateless() {
        let secret = secret();
        let token = derive_channel_token_v1(
            &secret,
            ChannelPurposeV1::WorkerChannel,
            &app_id(),
            &generation(),
            77,
            "repeat",
        )
        .unwrap();
        let authorization = format_channel_authorization_v1(&token);
        let token_before = token.expose_base64url();
        let debug_before = format!("{token:?}");
        for _ in 0..1_000 {
            verify_channel_token_authorization_v1(&token, &authorization).unwrap();
        }
        assert_eq!(token.expose_base64url(), token_before);
        assert_eq!(format!("{token:?}"), debug_before);
        assert_eq!(format_channel_authorization_v1(&token), authorization);
    }

    #[test]
    fn length_prefixes_remove_ambiguous_concatenation() {
        let mut left = Vec::new();
        append_field(&mut left, b"ab").unwrap();
        append_field(&mut left, b"c").unwrap();
        let mut right = Vec::new();
        append_field(&mut right, b"a").unwrap();
        append_field(&mut right, b"bc").unwrap();
        assert_ne!(left, right);
    }

    #[test]
    fn invalid_binding_and_tampered_tokens_fail_closed() {
        let secret = secret();
        let invalid_app = AppId("Bad-App".to_string());
        let invalid_generation = GenerationId("generation-1".to_string());
        for result in [
            derive_channel_token_v1(
                &secret,
                ChannelPurposeV1::WorkerChannel,
                &invalid_app,
                &generation(),
                1,
                "nonce",
            ),
            derive_channel_token_v1(
                &secret,
                ChannelPurposeV1::WorkerChannel,
                &app_id(),
                &invalid_generation,
                1,
                "nonce",
            ),
            derive_channel_token_v1(
                &secret,
                ChannelPurposeV1::WorkerChannel,
                &app_id(),
                &generation(),
                0,
                "nonce",
            ),
            derive_channel_token_v1(
                &secret,
                ChannelPurposeV1::WorkerChannel,
                &app_id(),
                &generation(),
                1,
                "",
            ),
            derive_channel_token_v1(
                &secret,
                ChannelPurposeV1::WorkerChannel,
                &app_id(),
                &generation(),
                1,
                &"x".repeat(MAX_WORKER_NONCE_BYTES_V1 + 1),
            ),
        ] {
            assert!(result.is_err());
        }

        let token = derive_channel_token_v1(
            &secret,
            ChannelPurposeV1::WorkerChannel,
            &app_id(),
            &generation(),
            7,
            "nonce",
        )
        .unwrap();
        let mut encoded = token.expose_base64url().into_bytes();
        encoded[0] = if encoded[0] == b'A' { b'B' } else { b'A' };
        let tampered = format!(
            "{CHANNEL_AUTH_SCHEME_V1} {}",
            String::from_utf8(encoded).unwrap()
        );
        assert_eq!(
            verify_channel_authorization_v1(
                &secret,
                ChannelPurposeV1::WorkerChannel,
                &app_id(),
                &generation(),
                7,
                "nonce",
                &tampered,
            ),
            Err(TransportAuthError::AuthenticationFailed)
        );
        for invalid in [
            "CowdChannel !!!",
            "CowdChannel AA",
            "CowdBootstrap AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ] {
            assert!(verify_channel_authorization_v1(
                &secret,
                ChannelPurposeV1::WorkerChannel,
                &app_id(),
                &generation(),
                7,
                "nonce",
                invalid,
            )
            .is_err());
        }
    }

    #[test]
    fn secret_and_token_debug_output_is_redacted() {
        let secret = secret();
        let token = derive_channel_token_v1(
            &secret,
            ChannelPurposeV1::WorkerChannel,
            &app_id(),
            &generation(),
            1,
            "nonce",
        )
        .unwrap();
        assert_eq!(format!("{secret:?}"), "BootstrapSecretV1([REDACTED])");
        assert_eq!(format!("{token:?}"), "ChannelTokenV1([REDACTED])");
        assert!(!format!("{secret:?}").contains(&secret.expose_base64url()));
        assert!(!format!("{token:?}").contains(&token.expose_base64url()));
    }
}
