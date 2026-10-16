use std::collections::BTreeMap;
use std::fmt;

use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const STEP_CONTRACT_VERSION: u32 = 1;
pub const CANONICAL_FORMAT_VERSION: u32 = 1;
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StepContractError {
    #[error("invalid strict JSON: {0}")]
    InvalidJson(String),
    #[error(
        "JSON number is not finite or outside the interoperable safe range; encode it as a string"
    )]
    UnsafeNumber,
    #[error("digest domain must not be empty")]
    EmptyDomain,
}

struct StrictValue(Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<Decoder>(decoder: Decoder) -> Result<Self, Decoder::Error>
    where
        Decoder: Deserializer<'de>,
    {
        struct StrictVisitor;

        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = StrictValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("JSON with unique object keys and interoperable numbers")
            }

            fn visit_unit<Error>(self) -> Result<Self::Value, Error> {
                Ok(StrictValue(Value::Null))
            }

            fn visit_bool<Error>(self, value: bool) -> Result<Self::Value, Error> {
                Ok(StrictValue(Value::Bool(value)))
            }

            fn visit_i64<Error: serde::de::Error>(self, value: i64) -> Result<Self::Value, Error> {
                if value.unsigned_abs() > 9_007_199_254_740_991 {
                    return Err(Error::custom(StepContractError::UnsafeNumber));
                }
                Ok(StrictValue(Value::from(value)))
            }

            fn visit_u64<Error: serde::de::Error>(self, value: u64) -> Result<Self::Value, Error> {
                if value > 9_007_199_254_740_991 {
                    return Err(Error::custom(StepContractError::UnsafeNumber));
                }
                Ok(StrictValue(Value::from(value)))
            }

            fn visit_f64<Error: serde::de::Error>(self, value: f64) -> Result<Self::Value, Error> {
                if !value.is_finite() || value.abs() > MAX_SAFE_INTEGER {
                    return Err(Error::custom(StepContractError::UnsafeNumber));
                }
                serde_json::Number::from_f64(value)
                    .map(|number| StrictValue(Value::Number(number)))
                    .ok_or_else(|| Error::custom(StepContractError::UnsafeNumber))
            }

            fn visit_str<Error: serde::de::Error>(self, value: &str) -> Result<Self::Value, Error> {
                Ok(StrictValue(Value::String(value.to_string())))
            }

            fn visit_string<Error>(self, value: String) -> Result<Self::Value, Error> {
                Ok(StrictValue(Value::String(value)))
            }

            fn visit_seq<Sequence>(
                self,
                mut sequence: Sequence,
            ) -> Result<Self::Value, Sequence::Error>
            where
                Sequence: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element::<StrictValue>()? {
                    values.push(value.0);
                }
                Ok(StrictValue(Value::Array(values)))
            }

            fn visit_map<Mapping>(self, mut mapping: Mapping) -> Result<Self::Value, Mapping::Error>
            where
                Mapping: MapAccess<'de>,
            {
                let mut values = serde_json::Map::new();
                while let Some(key) = mapping.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate object key `{key}`"
                        )));
                    }
                    values.insert(key, mapping.next_value::<StrictValue>()?.0);
                }
                Ok(StrictValue(Value::Object(values)))
            }
        }

        decoder.deserialize_any(StrictVisitor)
    }
}

pub fn deserialize_strict_json_value<'de, Decoder>(
    decoder: Decoder,
) -> Result<Value, Decoder::Error>
where
    Decoder: Deserializer<'de>,
{
    StrictValue::deserialize(decoder).map(|value| value.0)
}

pub fn parse_strict_json(input: &str) -> Result<Value, StepContractError> {
    let mut decoder = serde_json::Deserializer::from_str(input);
    let value = StrictValue::deserialize(&mut decoder)
        .map_err(|error| StepContractError::InvalidJson(error.to_string()))?;
    decoder
        .end()
        .map_err(|error| StepContractError::InvalidJson(error.to_string()))?;
    Ok(value.0)
}

fn write_bytes(bytes: &[u8], output: &mut Vec<u8>) {
    output.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    output.extend_from_slice(bytes);
}

pub fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>, StepContractError> {
    fn encode(value: &Value, output: &mut Vec<u8>) -> Result<(), StepContractError> {
        match value {
            Value::Null => output.push(b'0'),
            Value::Bool(false) => output.push(b'f'),
            Value::Bool(true) => output.push(b't'),
            Value::String(text) => {
                output.push(b's');
                write_bytes(text.as_bytes(), output);
            }
            Value::Number(number) => {
                let value = number.as_f64().ok_or(StepContractError::UnsafeNumber)?;
                if !value.is_finite() || value.abs() > MAX_SAFE_INTEGER {
                    return Err(StepContractError::UnsafeNumber);
                }
                output.push(b'n');
                let normalized = if value == 0.0 { 0.0 } else { value };
                output.extend_from_slice(&normalized.to_bits().to_be_bytes());
            }
            Value::Array(values) => {
                output.push(b'a');
                output.extend_from_slice(&(values.len() as u64).to_be_bytes());
                for value in values {
                    encode(value, output)?;
                }
            }
            Value::Object(values) => {
                output.push(b'o');
                output.extend_from_slice(&(values.len() as u64).to_be_bytes());
                let ordered = values.iter().collect::<BTreeMap<_, _>>();
                for (key, value) in ordered {
                    write_bytes(key.as_bytes(), output);
                    encode(value, output)?;
                }
            }
        }
        Ok(())
    }

    let mut bytes = Vec::new();
    encode(value, &mut bytes)?;
    Ok(bytes)
}

pub fn canonical_digest(domain: &str, value: &Value) -> Result<String, StepContractError> {
    if domain.is_empty() {
        return Err(StepContractError::EmptyDomain);
    }
    let canonical = canonical_json_bytes(value)?;
    let mut hasher = Sha256::new();
    hasher.update(b"cowd:canonical-json\0");
    hasher.update(CANONICAL_FORMAT_VERSION.to_be_bytes());
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain.as_bytes());
    hasher.update((canonical.len() as u64).to_be_bytes());
    hasher.update(canonical);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepIdentity {
    pub step_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub execution_id: Option<String>,
    #[serde(default)]
    pub actor_ref: Option<String>,
    #[serde(default)]
    pub run_ref: Option<String>,
    #[serde(default)]
    pub attempt: Option<u32>,
    #[serde(default)]
    pub epoch: Option<String>,
    pub goal_ref: Option<String>,
    pub program_ref: Option<String>,
    pub task_ref: Option<String>,
    pub build_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderAttemptIdentity {
    pub attempt_id: String,
    pub origin_step_id: String,
    pub model_profile_digest: String,
    pub request_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepRenderRef {
    pub step_id: String,
    pub attempt_id: String,
    pub catalog_digest: String,
    pub render_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogAvailability {
    Ready,
    Unknown,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolBindingRef {
    pub tool_id: String,
    pub semantic_revision: String,
    pub schema_digest: String,
    pub examples_digest: String,
    pub effect_descriptor_digest: String,
    pub availability: CatalogAvailability,
    pub availability_reason: String,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepSnapshot {
    pub contract_version: u32,
    pub identity: StepIdentity,
    pub provider_attempt: ProviderAttemptIdentity,
    pub catalog_digest: String,
    pub tools: Vec<ToolBindingRef>,
    pub related_revisions: BTreeMap<String, String>,
    pub context_refs: Vec<String>,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CallSource {
    Provider { attempt_id: String },
    Process { worker_id: String, sequence: u64 },
    SystemDerived { parent_step_id: String },
    OfflineTest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallOrigin {
    pub origin_step_id: String,
    pub call_id: String,
    pub catalog_digest: String,
    pub source: CallSource,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_parser_refuses_duplicate_nested_keys_and_trailing_data() {
        for input in [r#"{"a":1,"a":2}"#, r#"{"a":[{"b":1,"b":2}]}"#, "{} []"] {
            assert!(parse_strict_json(input).is_err(), "{input}");
        }
    }

    #[test]
    fn canonical_numbers_and_keys_have_one_representation() {
        let left = parse_strict_json(r#"{"中":1.0,"a":[-0.0,1e0]}"#).unwrap();
        let right = parse_strict_json(r#"{"a":[0,1],"中":1}"#).unwrap();
        assert_eq!(
            canonical_digest("test", &left),
            canonical_digest("test", &right)
        );
        assert_ne!(
            canonical_digest("test", &left),
            canonical_digest("other", &left)
        );
        assert!(canonical_digest("", &left).is_err());
    }

    #[test]
    fn exact_large_numbers_must_be_strings() {
        for input in ["9007199254740992", "-9007199254740992", "1e400"] {
            assert!(parse_strict_json(input).is_err(), "{input}");
        }
        assert!(parse_strict_json("9007199254740991").is_ok());
        assert!(parse_strict_json(r#""18446744073709551615""#).is_ok());
        assert!(canonical_json_bytes(&serde_json::json!(u64::MAX)).is_err());
    }

    #[test]
    fn array_order_and_string_number_types_are_preserved() {
        assert_ne!(
            canonical_digest("test", &serde_json::json!([1, 2])),
            canonical_digest("test", &serde_json::json!([2, 1]))
        );
        assert_ne!(
            canonical_digest("test", &serde_json::json!("1")),
            canonical_digest("test", &serde_json::json!(1))
        );
    }
}
