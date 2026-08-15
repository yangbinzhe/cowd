use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{ProtocolValidationError, Sha256Digest};

pub(crate) fn canonical_digest_v1(
    domain: &'static str,
    value: &impl Serialize,
) -> Result<Sha256Digest, ProtocolValidationError> {
    let value = serde_json::to_value(value)
        .map_err(|error| ProtocolValidationError::InvalidJson(error.to_string()))?;
    let bytes = serde_json::to_vec(&canonicalize(value))
        .map_err(|error| ProtocolValidationError::InvalidJson(error.to_string()))?;
    let mut digest = Sha256::new();
    digest.update(domain.as_bytes());
    digest.update([0]);
    digest.update(bytes);
    Ok(Sha256Digest(format!("sha256:{:x}", digest.finalize())))
}

fn canonicalize(value: Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| (key, canonicalize(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize).collect()),
        other => other,
    }
}
