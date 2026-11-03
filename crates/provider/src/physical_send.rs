//! Physical send authority: the last gate before a provider HTTP request
//! leaves the process.
//!
//! Two facts are required before any physical send, in this order:
//!
//! 1. an authenticated launcher identity - verified once, at handoff, by
//!    [`crate::LauncherAttestationVerifier`]; and
//! 2. a durable per-send confirmation committed **before** the request leaves.
//!
//! The authority is installed by the runtime that owns the durable store. A
//! client without one refuses every send, so a missing wiring can never
//! silently send. The receipt identity fields are stamped by the authority
//! from the verified attestation: a caller cannot name the candidate, the
//! launcher process or the images it is running under.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::ApiError;
use crate::launcher_attestation::VerifiedLauncherIdentity;

/// The request facts one physical send carries into its durable receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalSendRequest {
    /// Unique per physical send. A receipt is consumed once.
    pub send_id: String,
    /// The endpoint the request is sent to.
    pub endpoint: String,
    pub method: String,
    /// The wire protocol of this send.
    pub protocol: String,
    /// The model this send addresses, when the send addresses one.
    pub model: String,
    /// SHA-256 of the exact request body that leaves the process.
    pub body_sha256: String,
}

/// The durable evidence of one physical send.
///
/// The identity fields are stamped by the authority from the verified
/// launcher attestation; they are never supplied by the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalSendReceipt {
    pub send_id: String,
    pub endpoint: String,
    pub method: String,
    pub protocol: String,
    pub model: String,
    pub body_sha256: String,
    pub candidate_head: String,
    pub crates_sha256: String,
    pub gateway_sha256: String,
    pub harness_sha256: String,
    pub launcher_pid: u32,
    pub launcher_starttime: u64,
    pub nonce: String,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
}

/// The durable owner that commits the receipt before the request leaves.
///
/// The owner returns the receipt it committed. The authority compares it with
/// the presented receipt: a store that returns a different receipt has not
/// proven this send.
pub trait PhysicalSendConfirmation: Send + Sync {
    fn confirm<'a>(
        &'a self,
        receipt: &'a PhysicalSendReceipt,
    ) -> Pin<Box<dyn Future<Output = Result<PhysicalSendReceipt, String>> + Send + 'a>>;
}

/// The installed send capability: one verified launcher identity plus the
/// durable owner that confirms every send.
pub struct PhysicalSendAuthority {
    identity: VerifiedLauncherIdentity,
    confirmation: Arc<dyn PhysicalSendConfirmation>,
    consumed: Mutex<HashSet<String>>,
}

impl std::fmt::Debug for PhysicalSendAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalSendAuthority")
            .field("attestation", self.identity.attestation())
            .finish()
    }
}

impl PhysicalSendAuthority {
    #[must_use]
    pub fn new(
        identity: VerifiedLauncherIdentity,
        confirmation: Arc<dyn PhysicalSendConfirmation>,
    ) -> Self {
        Self {
            identity,
            confirmation,
            consumed: Mutex::new(HashSet::new()),
        }
    }

    /// The verified launcher attestation this authority was built from.
    #[must_use]
    pub fn attestation(&self) -> &crate::launcher_attestation::LauncherAttestation {
        self.identity.attestation()
    }

    fn stamp(&self, request: &PhysicalSendRequest) -> PhysicalSendReceipt {
        let attestation = self.identity.attestation();
        PhysicalSendReceipt {
            send_id: request.send_id.clone(),
            endpoint: request.endpoint.clone(),
            method: request.method.clone(),
            protocol: request.protocol.clone(),
            model: request.model.clone(),
            body_sha256: request.body_sha256.clone(),
            candidate_head: attestation.candidate_head.clone(),
            crates_sha256: attestation.crates_sha256.clone(),
            gateway_sha256: attestation.gateway_sha256.clone(),
            harness_sha256: attestation.harness_sha256.clone(),
            launcher_pid: attestation.launcher_pid,
            launcher_starttime: attestation.launcher_starttime,
            nonce: attestation.nonce.clone(),
            issued_at_ms: attestation.issued_at_ms,
            expires_at_ms: attestation.expires_at_ms,
        }
    }

    fn release(&self, send_id: &str) {
        let mut consumed = self
            .consumed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        consumed.remove(send_id);
    }
}

/// Authorize one physical send.
///
/// The durable confirmation is committed before the caller may send. Without
/// an installed authority the send is refused: the interim guard is the
/// fail-closed default, not an authorization path.
pub async fn authorize_physical_send(
    authority: Option<&PhysicalSendAuthority>,
    request: &PhysicalSendRequest,
) -> Result<PhysicalSendReceipt, ApiError> {
    let Some(authority) = authority else {
        return Err(ApiError::PhysicalSendGate {
            reason: "authenticated physical-send capability is not installed".into(),
        });
    };
    if request.send_id.trim().is_empty() {
        return Err(ApiError::PhysicalSendGate {
            reason: "physical send id is empty".into(),
        });
    }
    let receipt = authority.stamp(request);
    {
        let mut consumed = authority
            .consumed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !consumed.insert(receipt.send_id.clone()) {
            return Err(ApiError::PhysicalSendGate {
                reason: "physical send receipt was already consumed".into(),
            });
        }
    }
    let committed = match authority.confirmation.confirm(&receipt).await {
        Ok(committed) => committed,
        Err(error) => {
            authority.release(&receipt.send_id);
            return Err(ApiError::PhysicalSendGate {
                reason: format!("physical send confirmation failed: {error}"),
            });
        }
    };
    if committed != receipt {
        authority.release(&receipt.send_id);
        return Err(ApiError::PhysicalSendGate {
            reason: "durable physical send receipt does not match the presented send".into(),
        });
    }
    Ok(receipt)
}

/// Explicit operator-selected diagnostic mode. This is not authenticated
/// authority and never constructs a durable PhysicalSendReceipt.
#[derive(Debug, Clone)]
pub struct DiagnosticPhysicalSend { reason: String }
impl DiagnosticPhysicalSend {
    pub fn new(reason: &str) -> Result<Self, String> {
        let reason = reason.trim();
        if reason.is_empty() || reason.len() > 512 || reason.chars().any(char::is_control) {
            return Err("diagnostic send reason must be nonempty, at most 512 bytes and contain no control characters".into());
        }
        Ok(Self { reason: reason.to_owned() })
    }
    pub fn reason(&self) -> &str { &self.reason }
}

/// Existing physical boundary, with an explicitly unauthenticated diagnostic
/// branch. Authenticated authority always takes precedence if present.
pub async fn authorize_physical_send_or_diagnostic(
    authority: Option<&PhysicalSendAuthority>, diagnostic: Option<&DiagnosticPhysicalSend>,
    request: &PhysicalSendRequest, original_request_id: Option<&str>,
) -> Result<(), ApiError> {
    if let Some(authority) = authority {
        authorize_physical_send(Some(authority), request).await?;
        return Ok(());
    }
    let Some(diagnostic) = diagnostic else {
        return Err(ApiError::PhysicalSendGate { reason: "authenticated physical-send capability is not installed".into() });
    };
    if request.send_id.trim().is_empty() {
        return Err(ApiError::PhysicalSendGate { reason: "physical send id is empty".into() });
    }
    let original_request_id = original_request_id.filter(|id| !id.trim().is_empty()).ok_or_else(||
        ApiError::PhysicalSendGate { reason: "diagnostic send has no original provider request identity".into() })?;
    let endpoint = reqwest::Url::parse(&request.endpoint).map(|mut url| {
        let _ = url.set_username(""); let _ = url.set_password(None); url.set_query(None); url.set_fragment(None); url.to_string()
    }).unwrap_or_else(|_| "invalid endpoint".into());
    tracing::warn!(request_id = %original_request_id, send_id = %request.send_id, endpoint = %endpoint, method = %request.method,
        protocol = %request.protocol, model = %request.model, body_sha256 = %request.body_sha256,
        diagnostic_reason = %diagnostic.reason(), authenticated = false,
        "diagnostic provider send permitted before HTTP; not authenticated acceptance evidence");
    Ok(())
}

/// SHA-256 of the exact bytes that leave the process.
#[must_use]
pub fn body_digest(body: &[u8]) -> String {
    format!("{:x}", Sha256::digest(body))
}

/// Encode form parameters exactly as the request body is sent.
///
/// The WHATWG form-urlencoded serializer: unreserved characters stay, a space
/// becomes `+`, and everything else is percent encoded in uppercase hex. The
/// send site uses the returned bytes as the body, so the digest of the body
/// is the digest of the bytes that leave the process.
#[must_use]
pub fn form_urlencode(form: &std::collections::BTreeMap<&str, String>) -> String {
    let mut encoded = String::new();
    for (index, (key, value)) in form.iter().enumerate() {
        if index > 0 {
            encoded.push('&');
        }
        encode_form_component(&mut encoded, key);
        encoded.push('=');
        encode_form_component(&mut encoded, value);
    }
    encoded
}

fn encode_form_component(out: &mut String, value: &str) {
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' => {
                out.push(*byte as char);
            }
            b' ' => out.push('+'),
            other => {
                out.push('%');
                out.push_str(&format!("{other:02X}"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn form<'a>(
        pairs: &'a [(&'a str, &'a str)],
    ) -> std::collections::BTreeMap<&'a str, String> {
        pairs
            .iter()
            .map(|(key, value)| (*key, (*value).to_string()))
            .collect()
    }

    #[test]
    fn form_body_encoding_matches_the_wire_bytes() {
        assert_eq!(form_urlencode(&form(&[("a", "b")])), "a=b");
        assert_eq!(
            form_urlencode(&form(&[("a", "b c"), ("d", "e&f")])),
            "a=b+c&d=e%26f"
        );
        assert_eq!(form_urlencode(&form(&[("k", "ü")])), "k=%C3%BC");
    }
}
