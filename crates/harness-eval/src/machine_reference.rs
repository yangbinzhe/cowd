//! Machine-readable references for everything a failure leaves behind.
//!
//! A report that quotes a payload by re-typing it is a report that can lie:
//! the copy can be truncated, edited, or dropped, and nothing left in the
//! artifact can say so. That is not hypothetical - a live recompute quoted a
//! node-status list back at 200 characters, cut mid-identifier, and the cut
//! read as a kernel fact. Every payload a failure carries is therefore
//! referenced by *where it lives* and *what it hashes to*, and the payload
//! itself stays in the artifact exactly once.
//!
//! Two rules follow, and both are the point of this module:
//!
//!   * a reference names its payload; it never re-types it;
//!   * the prose a reader reads is rendered from the typed block, never
//!     written beside it - one source, so the two cannot drift apart.
//!
//! A `path` is either relative to the artifact that carries the block
//! (`#/failure_diagnostics/failure`) or names the file the block lives in
//! (`live-scenarios/001-<scenario>.json#/failure_diagnostics`). A block that
//! cannot know its own file name says so by being relative; a block that
//! knows it says the whole truth.

use serde_json::Value;
use sha2::{Digest, Sha256};

/// The digest of a payload, as a reference block carries it.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

/// The canonical bytes a payload is digested over.
pub fn canonical_bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or_default()
}

/// A reference to a payload that lives somewhere in the artifact.
///
/// `path` says where the payload is; `sha256` says what it hashes to; `bytes`
/// says how big it is. A reader that follows the path and finds a different
/// digest knows its copy is not the artifact's copy.
pub fn reference_block(path: &str, value: &Value) -> Value {
    let bytes = canonical_bytes(value);
    serde_json::json!({
        "path": path,
        "sha256": sha256_hex(&bytes),
        "bytes": bytes.len(),
    })
}

/// The one shape a failure takes on the way out.
///
/// `code` names the class, `reason` is the prose a reader reads, and `payload`
/// is the typed content that reason was rendered from.
pub fn typed_failure(code: &str, reason: &str, payload: Value) -> Value {
    serde_json::json!({
        "code": code,
        "reason": reason,
        "payload": payload,
    })
}

/// Attach a reference to a typed block, digested over the block itself.
///
/// The digest covers the block *without* the reference, so a reader can
/// recompute it from the payload alone.
pub fn with_reference(path: &str, block: Value) -> Value {
    let reference = reference_block(path, &block);
    let mut object = block.as_object().cloned().unwrap_or_default();
    object.insert("reference".to_string(), reference);
    Value::Object(object)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_reference_names_the_payload_and_its_digest() {
        let payload = json!({"code": "x", "reason": "because", "payload": {"n": 1}});
        let reference = reference_block("live-scenarios/s.json#/failure_diagnostics/failure", &payload);
        let expected = sha256_hex(&canonical_bytes(&payload));
        assert_eq!(reference["path"], "live-scenarios/s.json#/failure_diagnostics/failure");
        assert_eq!(reference["sha256"], expected);
        assert_eq!(
            reference["bytes"].as_u64(),
            Some(canonical_bytes(&payload).len() as u64)
        );
    }

    #[test]
    fn the_digest_covers_the_block_without_its_own_reference() {
        let block = typed_failure("c", "r", json!({"k": "v"}));
        let referenced = with_reference("some/path", block.clone());
        let expected = sha256_hex(&canonical_bytes(&block));
        let reference = referenced.get("reference").expect("a reference");
        assert_eq!(reference["sha256"], expected);
        assert_eq!(referenced.get("code"), Some(&json!("c")));
    }

    #[test]
    fn a_reference_does_not_re_type_the_payload() {
        let payload = json!({"verdict": "failed", "reason": "a long reason that would be cut"});
        let reference = reference_block("p", &payload);
        assert!(reference.get("verdict").is_none(), "the reference names, it does not copy");
        assert!(reference.get("reason").is_none(), "the reference names, it does not copy");
    }
}
