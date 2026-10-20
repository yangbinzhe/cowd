//! Canonical terminal transcript and execution-fence validation.

use super::super::types::{
    SessionTerminalPublicPayload, SessionTerminalPublicPayloadUnavailableReason,
    SESSION_TERMINAL_PUBLIC_PAYLOAD_DIGEST_PREFIX_V1, SESSION_TERMINAL_PUBLIC_PAYLOAD_KIND_V1,
    SESSION_TERMINAL_PUBLIC_PAYLOAD_MAX_BYTES,
};
use super::super::{SessionMessage, SessionTerminalTranscriptCommit};
use crate::{SessionError, SessionResult};
use serde::de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserializer, Serialize, Serializer};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::fmt;
use std::io::{self, Write};

type PublicPayloadResult<ValueType> =
    Result<ValueType, SessionTerminalPublicPayloadUnavailableReason>;

pub fn public_terminal_payload_v1(
    content_json: &str,
) -> PublicPayloadResult<SessionTerminalPublicPayload> {
    let Value::Array(blocks) = parse_terminal_json(content_json)? else {
        return Err(SessionTerminalPublicPayloadUnavailableReason::InvalidBlockShape);
    };
    let blocks = public_session_blocks(blocks)?;
    let payload_json = encode_public_json(&Value::Array(blocks))?;
    let content_digest = terminal_public_payload_digest(&payload_json);
    Ok(SessionTerminalPublicPayload {
        payload_kind: SESSION_TERMINAL_PUBLIC_PAYLOAD_KIND_V1,
        payload_json,
        content_digest,
    })
}

pub fn public_session_blocks(blocks: Vec<Value>) -> PublicPayloadResult<Vec<Value>> {
    let mut public_blocks = Vec::new();
    for block in blocks {
        let object = block
            .as_object()
            .ok_or(SessionTerminalPublicPayloadUnavailableReason::InvalidBlockShape)?;
        let block_type = object
            .get("type")
            .and_then(Value::as_str)
            .ok_or(SessionTerminalPublicPayloadUnavailableReason::InvalidBlockShape)?;
        if block_type == "thinking" {
            if !object.get("thinking").is_some_and(Value::is_string)
                || object
                    .get("signature")
                    .is_some_and(|value| !value.is_string())
            {
                return Err(SessionTerminalPublicPayloadUnavailableReason::InvalidBlockField);
            }
            continue;
        }
        validate_public_block(object, block_type)?;
        public_blocks.push(block);
    }
    Ok(public_blocks)
}

#[must_use]
pub fn terminal_public_payload_digest(payload_json: &str) -> String {
    let digest = Sha256::digest(payload_json.as_bytes());
    format!("{SESSION_TERMINAL_PUBLIC_PAYLOAD_DIGEST_PREFIX_V1}{digest:x}")
}

fn validate_public_block(object: &Map<String, Value>, block_type: &str) -> PublicPayloadResult<()> {
    let (required_strings, optional_strings, required_booleans): (&[&str], &[&str], &[&str]) =
        match block_type {
            "text" | "reasoning_summary" => (&["text"], &[], &[]),
            "image" => (&["media_type", "data"], &["source_path"], &[]),
            "tool_use" => (&["id", "name", "input"], &[], &[]),
            "tool_result" => (&["tool_use_id", "tool_name", "output"], &[], &["is_error"]),
            _ => return Err(SessionTerminalPublicPayloadUnavailableReason::UnsupportedBlockType),
        };
    for field in required_strings {
        if !object.get(*field).is_some_and(Value::is_string) {
            return Err(SessionTerminalPublicPayloadUnavailableReason::InvalidBlockField);
        }
    }
    for field in required_booleans {
        if !object.get(*field).is_some_and(Value::is_boolean) {
            return Err(SessionTerminalPublicPayloadUnavailableReason::InvalidBlockField);
        }
    }
    for (field, value) in object {
        if field == "type" {
            continue;
        }
        let common_metadata = matches!(
            field.as_str(),
            "cowd_execution_id" | "cowd_turn_id" | "cowd_turn_ingress_message_id"
        );
        let tool_metadata =
            field == "cowd_tool_instance_id" && matches!(block_type, "tool_use" | "tool_result");
        if required_strings.contains(&field.as_str())
            || optional_strings.contains(&field.as_str())
            || common_metadata
            || tool_metadata
        {
            if !value.is_string() {
                return Err(SessionTerminalPublicPayloadUnavailableReason::InvalidBlockField);
            }
        } else if !required_booleans.contains(&field.as_str()) {
            return Err(SessionTerminalPublicPayloadUnavailableReason::UnsupportedBlockField);
        }
    }
    Ok(())
}

fn parse_terminal_json(content_json: &str) -> PublicPayloadResult<Value> {
    if content_json.len() > SESSION_TERMINAL_PUBLIC_PAYLOAD_MAX_BYTES {
        return Err(SessionTerminalPublicPayloadUnavailableReason::PayloadTooLarge);
    }
    let duplicate_key = Cell::new(false);
    let mut deserializer = serde_json::Deserializer::from_str(content_json);
    let value = StrictJsonSeed {
        duplicate_key: &duplicate_key,
    }
    .deserialize(&mut deserializer)
    .map_err(|_| {
        if duplicate_key.get() {
            SessionTerminalPublicPayloadUnavailableReason::DuplicateObjectKey
        } else {
            SessionTerminalPublicPayloadUnavailableReason::MalformedJson
        }
    })?;
    deserializer
        .end()
        .map_err(|_| SessionTerminalPublicPayloadUnavailableReason::MalformedJson)?;
    Ok(value)
}

struct StrictJsonSeed<'state> {
    duplicate_key: &'state Cell<bool>,
}

impl<'de> DeserializeSeed<'de> for StrictJsonSeed<'_> {
    type Value = Value;

    fn deserialize<Decoder: Deserializer<'de>>(
        self,
        decoder: Decoder,
    ) -> Result<Value, Decoder::Error> {
        decoder.deserialize_any(StrictJsonVisitor {
            duplicate_key: self.duplicate_key,
        })
    }
}

struct StrictJsonVisitor<'state> {
    duplicate_key: &'state Cell<bool>,
}

impl<'de> Visitor<'de> for StrictJsonVisitor<'_> {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_unit<DecodeError: serde::de::Error>(self) -> Result<Value, DecodeError> {
        Ok(Value::Null)
    }

    fn visit_bool<DecodeError: serde::de::Error>(self, value: bool) -> Result<Value, DecodeError> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<DecodeError: serde::de::Error>(self, value: i64) -> Result<Value, DecodeError> {
        Ok(Value::Number(value.into()))
    }

    fn visit_u64<DecodeError: serde::de::Error>(self, value: u64) -> Result<Value, DecodeError> {
        Ok(Value::Number(value.into()))
    }

    fn visit_f64<DecodeError: serde::de::Error>(self, value: f64) -> Result<Value, DecodeError> {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| DecodeError::custom("non-finite JSON number"))
    }

    fn visit_str<DecodeError: serde::de::Error>(self, value: &str) -> Result<Value, DecodeError> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<DecodeError: serde::de::Error>(
        self,
        value: String,
    ) -> Result<Value, DecodeError> {
        Ok(Value::String(value))
    }

    fn visit_seq<Sequence: SeqAccess<'de>>(
        self,
        mut sequence: Sequence,
    ) -> Result<Value, Sequence::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(StrictJsonSeed {
            duplicate_key: self.duplicate_key,
        })? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<Object: MapAccess<'de>>(self, mut object: Object) -> Result<Value, Object::Error> {
        let mut values = Map::new();
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                self.duplicate_key.set(true);
                return Err(Object::Error::custom("duplicate object key"));
            }
            let value = object.next_value_seed(StrictJsonSeed {
                duplicate_key: self.duplicate_key,
            })?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

struct CanonicalPublicJson<'value>(&'value Value);

impl Serialize for CanonicalPublicJson<'_> {
    fn serialize<Encoder: Serializer>(
        &self,
        encoder: Encoder,
    ) -> Result<Encoder::Ok, Encoder::Error> {
        match self.0 {
            Value::Array(values) => {
                let mut sequence = encoder.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(&CanonicalPublicJson(value))?;
                }
                sequence.end()
            }
            Value::Object(values) => {
                let mut entries: Vec<_> = values.iter().collect();
                entries.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
                let mut object = encoder.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    object.serialize_entry(key, &CanonicalPublicJson(value))?;
                }
                object.end()
            }
            value => value.serialize(encoder),
        }
    }
}

#[derive(Default)]
struct PublicPayloadWriter {
    bytes: Vec<u8>,
    limit_exceeded: bool,
}

impl Write for PublicPayloadWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > SESSION_TERMINAL_PUBLIC_PAYLOAD_MAX_BYTES.saturating_sub(self.bytes.len())
        {
            self.limit_exceeded = true;
            return Err(io::Error::other("public payload byte budget exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode_public_json(value: &Value) -> PublicPayloadResult<String> {
    let mut writer = PublicPayloadWriter::default();
    if serde_json::to_writer(&mut writer, &CanonicalPublicJson(value)).is_err() {
        return Err(if writer.limit_exceeded {
            SessionTerminalPublicPayloadUnavailableReason::PayloadTooLarge
        } else {
            SessionTerminalPublicPayloadUnavailableReason::EncodingFailed
        });
    }
    String::from_utf8(writer.bytes)
        .map_err(|_| SessionTerminalPublicPayloadUnavailableReason::EncodingFailed)
}

pub fn validate_terminal_transcript(
    terminal_message_id: &str,
    ingress_message_id: &str,
    session_id: &str,
    messages: &[SessionMessage],
) -> SessionResult<()> {
    if messages.last().is_some_and(|message| {
        message.content_json.len() > SESSION_TERMINAL_PUBLIC_PAYLOAD_MAX_BYTES
    }) {
        return Err(SessionError::InvalidArgument(
            "terminal public payload exceeds the byte budget".into(),
        ));
    }
    if terminal_message_id.trim().is_empty()
        || ingress_message_id.trim().is_empty()
        || session_id.trim().is_empty()
        || messages.is_empty()
        || messages
            .last()
            .is_none_or(|message| message.stable_message_id != terminal_message_id)
    {
        return Err(SessionError::InvalidArgument(
            "terminal transcript requires a non-empty session, ingress, terminal ID, and final row"
                .to_string(),
        ));
    }
    if messages.iter().any(|message| {
        message.stable_message_id.trim().is_empty()
            || message.session_id != session_id
            || message.role.trim().is_empty()
            || serde_json::from_str::<serde_json::Value>(&message.content_json)
                .ok()
                .and_then(|value| value.as_array().cloned())
                .is_none()
    }) {
        return Err(SessionError::InvalidArgument(
            "terminal transcript contains an invalid message row".to_string(),
        ));
    }
    let unique_ids = messages
        .iter()
        .map(|message| message.stable_message_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if unique_ids.len() != messages.len() {
        return Err(SessionError::InvalidArgument(
            "terminal transcript contains duplicate stable message IDs".to_string(),
        ));
    }
    Ok(())
}

pub fn validate_terminal_commit(request: &SessionTerminalTranscriptCommit) -> SessionResult<()> {
    if request.turn_id.trim().is_empty()
        || request.runtime_commit_cursor == 0
        || request.runtime_commit_cursor > i64::MAX as u64
        || request.consumed_input_sequence < request.fence.input_sequence
        || request.fence.request_id.trim().is_empty()
        || request.fence.session_generation == 0
        || request.fence.claim_owner.trim().is_empty()
        || request.fence.claim_token.trim().is_empty()
        || request.fence.claim_fence_epoch == 0
    {
        return Err(SessionError::InvalidArgument(
            "terminal commit requires complete turn, cursor and live execution fence identity"
                .to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod public_payload_tests {
    use super::*;
    use serde_json::json;

    fn payload(raw: &str) -> SessionTerminalPublicPayload {
        public_terminal_payload_v1(raw).unwrap()
    }

    #[test]
    fn r1_message_ack_public_payload_v1_has_exact_golden_bytes() {
        let actual = payload(r#" [ { "type" : "text", "text" : "answer" } ] "#);
        assert_eq!(actual.payload_kind, "session-public-v1");
        assert_eq!(actual.payload_json, r#"[{"text":"answer","type":"text"}]"#);
        assert_eq!(
            actual.content_digest,
            "session-public-v1:sha256:b57b9b32800ac7746beb2853976a07c27b4801b8f6fda240d49f17c2fcc97114",
        );
        let value = parse_terminal_json(
            r#"{"z":[null,true,{"é":2,"a":1}],"a":[-0.0,1.0,1.25e-1,1e100,18446744073709551615,-9223372036854775808]}"#,
        ).unwrap();
        assert_eq!(
            encode_public_json(&value).unwrap(),
            r#"{"a":[-0.0,1.0,0.125,1e+100,18446744073709551615,-9223372036854775808],"z":[null,true,{"a":1,"é":2}]}"#,
        );
    }

    #[test]
    fn r1_message_ack_public_payload_never_exports_private_thinking() {
        let raw = r#"[
            {"type":"reasoning_summary","text":"public summary"},
            {"type":"thinking","thinking":"cowd-provider-transcript:v1:not-decrypted",
             "signature":"cowd-provider-transcript:v1:private-signature",
             "private_extension":{"signature":"private-extension","future":[1,true]}},
            {"type":"text","text":"final answer","cowd_execution_id":"execution-1"}
        ]"#;
        let original = raw.to_owned();
        let actual = payload(raw);
        assert_eq!(
            actual.payload_json,
            r#"[{"text":"public summary","type":"reasoning_summary"},{"cowd_execution_id":"execution-1","text":"final answer","type":"text"}]"#,
        );
        for private in [
            "thinking",
            "signature",
            "cowd-provider-transcript",
            "private-extension",
            "not-decrypted",
        ] {
            assert!(!actual.payload_json.contains(private));
        }
        assert_eq!(raw, original);
        assert!(raw.contains("cowd-provider-transcript:v1:not-decrypted"));
        let changed_seal = raw.replace("not-decrypted", "a-different-ciphertext");
        assert_eq!(payload(&changed_seal), actual);
    }

    #[test]
    fn r1_message_ack_public_payload_preserves_all_six_storage_shapes() {
        let raw = r#"[
            {"type":"text","text":"answer"},
            {"type":"reasoning_summary","text":"summary"},
            {"type":"image","media_type":"image/png","data":"base64-data","source_path":"/public/image.png"},
            {"type":"thinking","thinking":"sealed","signature":"sealed-signature"},
            {"type":"tool_use","id":"call-1","name":"read_file","input":"{\"path\":\"a\"}"},
            {"type":"tool_result","tool_use_id":"call-1","tool_name":"read_file","output":"a\nb","is_error":false}
        ]"#;
        let actual: Vec<Value> = serde_json::from_str(&payload(raw).payload_json).unwrap();
        let source: Vec<Value> = serde_json::from_str(raw).unwrap();
        assert_eq!(
            actual,
            source
                .into_iter()
                .filter(|block| block["type"] != "thinking")
                .collect::<Vec<_>>()
        );
        assert_eq!(actual.len(), 5);
        assert!(actual[4]["is_error"].is_boolean());
        assert_eq!(
            payload(r#"[{"type":"image","media_type":"image/png","data":""}]"#).payload_json,
            r#"[{"data":"","media_type":"image/png","type":"image"}]"#
        );
    }

    #[test]
    fn r1_message_ack_public_payload_keeps_tool_strings_and_causal_metadata_exact() {
        let raw = r#"[
          {"type":"tool_use","id":"duplicate-provider-id","name":"tool","input":" {\"z\":1,\"a\":2,\"a\":3} \n",
           "cowd_execution_id":"exec","cowd_turn_id":"turn","cowd_turn_ingress_message_id":"ingress","cowd_tool_instance_id":"duplicate-provider-id#cowd-0"},
          {"type":"tool_result","tool_use_id":"duplicate-provider-id","tool_name":"tool","output":"{\"signature\":\"ordinary business text\"}","is_error":true,
           "cowd_execution_id":"exec","cowd_turn_id":"turn","cowd_turn_ingress_message_id":"ingress","cowd_tool_instance_id":"duplicate-provider-id#cowd-0"}
        ]"#;
        let source: Value = serde_json::from_str(raw).unwrap();
        let actual: Value = serde_json::from_str(&payload(raw).payload_json).unwrap();
        assert_eq!(actual, source);
        assert_eq!(
            actual[0]["cowd_tool_instance_id"],
            actual[1]["cowd_tool_instance_id"]
        );
        assert!(actual[1]["output"]
            .as_str()
            .unwrap()
            .contains("ordinary business text"));
    }

    #[test]
    fn r1_message_ack_public_payload_wire_keeps_the_exact_public_json_string() {
        let actual = payload(r#"[{"type":"text","text":"汉字🙂\n\"\\/"}]"#);
        let exact = actual.payload_json.clone();
        assert_eq!(exact, r#"[{"text":"汉字🙂\n\"\\/","type":"text"}]"#);
        let encoded = serde_json::to_string(&actual).unwrap();
        let decoded: Value = serde_json::from_str(&encoded).unwrap();
        assert!(decoded["payload_json"].is_string());
        assert_eq!(
            decoded["payload_json"].as_str().unwrap().as_bytes(),
            exact.as_bytes()
        );
        assert_eq!(
            decoded["content_digest"],
            terminal_public_payload_digest(&exact)
        );
        assert_eq!(actual.payload_json, exact);
    }

    #[test]
    fn r1_message_ack_public_payload_utf8_escape_and_no_normalization_are_frozen() {
        let raw = r#"[{"type":"text","text":"汉🙂\u0000\b\f\n\r\t\"\\/"}]"#;
        assert_eq!(
            payload(raw).payload_json,
            r#"[{"text":"汉🙂\u0000\b\f\n\r\t\"\\/","type":"text"}]"#
        );
        let composed = payload(r#"[{"type":"text","text":"é"}]"#);
        let decomposed = payload(r#"[{"type":"text","text":"e\u0301"}]"#);
        assert_ne!(
            composed.payload_json.as_bytes(),
            decomposed.payload_json.as_bytes()
        );
        assert_ne!(composed.content_digest, decomposed.content_digest);
        assert_eq!(composed, payload(r#"[{"text":"\u00e9","type":"text"}]"#));
        let ordered = parse_terminal_json(r#"{"🙂":0,"é":1,"a":2,"é":3}"#).unwrap();
        assert_eq!(
            encode_public_json(&ordered).unwrap(),
            r#"{"a":2,"é":3,"é":1,"🙂":0}"#
        );
    }

    #[test]
    fn r1_message_ack_public_payload_empty_and_private_only_are_public_empty_arrays() {
        let empty = payload("[]");
        let private = payload(r#"[{"type":"thinking","thinking":"sealed","signature":"sealed"}]"#);
        assert_eq!(empty, private);
        assert_eq!(empty.payload_json, "[]");
        assert_eq!(empty.content_digest,
                   "session-public-v1:sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945");
        assert!(public_terminal_payload_v1("").is_err());
    }

    #[test]
    fn r1_message_ack_public_payload_rejects_malformed_json_and_trailing_data() {
        for raw in [
            "",
            "[",
            "[1,]",
            "[] []",
            "[] secret",
            "NaN",
            "Infinity",
            "-Infinity",
            "[1e400]",
            "[\"\\ud800\"]",
            "\u{feff}[]",
        ] {
            assert_eq!(
                public_terminal_payload_v1(raw).unwrap_err(),
                SessionTerminalPublicPayloadUnavailableReason::MalformedJson
            );
        }
        let deep = format!("{}0{}", "[".repeat(130), "]".repeat(130));
        assert_eq!(
            public_terminal_payload_v1(&deep).unwrap_err(),
            SessionTerminalPublicPayloadUnavailableReason::MalformedJson
        );
    }

    #[test]
    fn r1_message_ack_public_payload_rejects_duplicate_keys_before_private_filtering() {
        for raw in [
            r#"[{"type":"text","type":"thinking","text":"public","thinking":"secret"}]"#,
            r#"[{"type":"text","ty\u0070e":"text","text":"public"}]"#,
            r#"[{"type":"thinking","thinking":"secret","future":{"nested":1,"nested":2}}]"#,
            r#"[{"type":"text","text":"one","text":"two"}]"#,
        ] {
            assert_eq!(
                public_terminal_payload_v1(raw).unwrap_err(),
                SessionTerminalPublicPayloadUnavailableReason::DuplicateObjectKey
            );
        }
    }

    #[test]
    fn r1_message_ack_public_payload_rejects_unknown_block_shapes_and_protocol_types() {
        for raw in [
            "null",
            "true",
            "1",
            "{}",
            "\"text\"",
            "[null]",
            "[[]]",
            "[1]",
            r#"[{}]"#,
            r#"[{"type":1}]"#,
        ] {
            assert_eq!(
                public_terminal_payload_v1(raw).unwrap_err(),
                SessionTerminalPublicPayloadUnavailableReason::InvalidBlockShape
            );
        }
        for block_type in ["future_private", "Text", "", "redacted_thinking"] {
            let raw =
                serde_json::to_string(&json!([{"type": block_type, "text": "secret"}])).unwrap();
            assert_eq!(
                public_terminal_payload_v1(&raw).unwrap_err(),
                SessionTerminalPublicPayloadUnavailableReason::UnsupportedBlockType
            );
        }
    }

    #[test]
    fn r1_message_ack_public_payload_rejects_unknown_or_private_public_fields() {
        for field in [
            "thinking",
            "signature",
            "private_extension",
            "usage",
            "payload_kind",
            "cowd_future_metadata",
            "cowd_tool_instance_id",
        ] {
            let mut block = json!({"type":"text","text":"answer"});
            block
                .as_object_mut()
                .unwrap()
                .insert(field.to_owned(), json!("secret-value"));
            let raw = serde_json::to_string(&vec![block]).unwrap();
            let error = public_terminal_payload_v1(&raw).unwrap_err();
            assert_eq!(
                error,
                SessionTerminalPublicPayloadUnavailableReason::UnsupportedBlockField
            );
            assert!(!format!("{error:?} {error}").contains("secret-value"));
            assert!(!serde_json::to_string(&error)
                .unwrap()
                .contains("secret-value"));
        }
    }

    #[test]
    fn r1_message_ack_public_payload_validates_every_required_and_optional_storage_field() {
        let valid = vec![
            json!({"type":"text","text":""}),
            json!({"type":"reasoning_summary","text":""}),
            json!({"type":"image","media_type":"image/png","data":"","source_path":"image"}),
            json!({"type":"thinking","thinking":"sealed","signature":"sealed"}),
            json!({"type":"tool_use","id":"call","name":"tool","input":"not required to be JSON"}),
            json!({"type":"tool_result","tool_use_id":"call","tool_name":"tool","output":"","is_error":false}),
        ];
        for block in valid {
            assert!(public_terminal_payload_v1(
                &serde_json::to_string(&vec![block.clone()]).unwrap()
            )
            .is_ok());
            let fields: Vec<_> = block
                .as_object()
                .unwrap()
                .keys()
                .filter(|field| field.as_str() != "type")
                .cloned()
                .collect();
            for field in fields {
                for replacement in [Value::Null, json!(1), json!({"hidden":"secret"}), json!([])] {
                    let mut changed = block.clone();
                    changed
                        .as_object_mut()
                        .unwrap()
                        .insert(field.clone(), replacement);
                    assert_eq!(
                        public_terminal_payload_v1(&serde_json::to_string(&vec![changed]).unwrap())
                            .unwrap_err(),
                        SessionTerminalPublicPayloadUnavailableReason::InvalidBlockField
                    );
                }
                let mut missing = block.clone();
                missing.as_object_mut().unwrap().remove(&field);
                let result =
                    public_terminal_payload_v1(&serde_json::to_string(&vec![missing]).unwrap());
                if matches!(field.as_str(), "source_path" | "signature") {
                    assert!(result.is_ok());
                } else {
                    assert_eq!(
                        result.unwrap_err(),
                        SessionTerminalPublicPayloadUnavailableReason::InvalidBlockField
                    );
                }
            }
        }
    }

    #[test]
    fn r1_message_ack_public_payload_rejects_invalid_causal_metadata_without_guessing_identity() {
        for field in [
            "cowd_execution_id",
            "cowd_turn_id",
            "cowd_turn_ingress_message_id",
            "cowd_tool_instance_id",
        ] {
            let mut block = json!({"type":"tool_use","id":"call","name":"tool","input":"{}"});
            block
                .as_object_mut()
                .unwrap()
                .insert(field.to_owned(), json!({"id":"foreign"}));
            assert_eq!(
                public_terminal_payload_v1(&serde_json::to_string(&vec![block]).unwrap())
                    .unwrap_err(),
                SessionTerminalPublicPayloadUnavailableReason::InvalidBlockField
            );
        }
        assert_eq!(
            payload(r#"[{"type":"text","text":"ok","cowd_turn_id":""}]"#).payload_json,
            r#"[{"cowd_turn_id":"","text":"ok","type":"text"}]"#
        );
    }

    #[test]
    fn r1_message_ack_public_payload_usage_and_row_metadata_are_not_digest_inputs() {
        let mut message = SessionMessage {
            stable_message_id: "assistant:input".to_owned(),
            session_id: "session".to_owned(),
            sequence: 1,
            role: "assistant".to_owned(),
            content_json: r#"[{"type":"text","text":"answer"}]"#.to_owned(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: Some("{\"input_tokens\":1}".to_owned()),
            created_at_ms: 1,
        };
        let before = payload(&message.content_json);
        message.token_usage_json = Some("{\"input_tokens\":9007199254740993}".to_owned());
        message.created_at_ms = 2;
        message.blocks_count = 42;
        assert_eq!(payload(&message.content_json), before);
        message.content_json = r#"[{"type":"text","text":"different answer"}]"#.to_owned();
        assert_ne!(
            payload(&message.content_json).content_digest,
            before.content_digest
        );
    }

    #[test]
    fn r1_message_ack_public_payload_canonicalizes_syntax_without_rewriting_string_values() {
        let first = r#"[{"type":"text","text":"  answer\n"}]"#;
        let second = " [ { \"text\": \"  answer\\n\", \"type\": \"text\" } ] \n";
        assert_ne!(first.as_bytes(), second.as_bytes());
        assert_eq!(payload(first), payload(second));
        assert_ne!(
            payload(first).content_digest,
            payload(r#"[{"type":"text","text":"answer"}]"#).content_digest
        );
        let reversed = payload(r#"[{"type":"text","text":"two"},{"type":"text","text":"one"}]"#);
        assert_ne!(
            reversed.content_digest,
            payload(r#"[{"type":"text","text":"one"},{"type":"text","text":"two"}]"#)
                .content_digest
        );
    }

    #[test]
    fn r1_message_ack_public_payload_digest_is_version_tagged_exact_utf8() {
        let exact = r#"[{"text":"汉🙂","type":"text"}]"#;
        let expected_hash = format!("{:x}", Sha256::digest(exact.as_bytes()));
        assert_eq!(
            terminal_public_payload_digest(exact),
            format!("session-public-v1:sha256:{expected_hash}")
        );
        assert_eq!(
            terminal_public_payload_digest(exact),
            "session-public-v1:sha256:dd2ff7526d2747c37d11db02036b2c9a5222414bf08c211c155ffee915faa8ed",
        );
        assert_ne!(
            terminal_public_payload_digest(exact),
            terminal_public_payload_digest(&format!("{exact}\n"))
        );
        assert_ne!(
            terminal_public_payload_digest(exact),
            terminal_public_payload_digest(&serde_json::to_string(exact).unwrap())
        );
    }

    #[test]
    fn r1_message_ack_public_payload_enforces_raw_utf8_and_output_byte_budgets() {
        let prefix = r#"[{"text":""#;
        let suffix = r#"","type":"text"}]"#;
        let exact = format!(
            "{prefix}{}{suffix}",
            "a".repeat(SESSION_TERMINAL_PUBLIC_PAYLOAD_MAX_BYTES - prefix.len() - suffix.len())
        );
        assert_eq!(exact.len(), SESSION_TERMINAL_PUBLIC_PAYLOAD_MAX_BYTES);
        assert_eq!(
            payload(&exact).payload_json.len(),
            SESSION_TERMINAL_PUBLIC_PAYLOAD_MAX_BYTES
        );
        let too_large = format!("{exact} ");
        assert_eq!(
            public_terminal_payload_v1(&too_large).unwrap_err(),
            SessionTerminalPublicPayloadUnavailableReason::PayloadTooLarge
        );
        let unicode = format!(
            "{prefix}{}{suffix}",
            "汉".repeat(SESSION_TERMINAL_PUBLIC_PAYLOAD_MAX_BYTES / 3)
        );
        assert!(unicode.chars().count() < SESSION_TERMINAL_PUBLIC_PAYLOAD_MAX_BYTES);
        assert_eq!(
            public_terminal_payload_v1(&unicode).unwrap_err(),
            SessionTerminalPublicPayloadUnavailableReason::PayloadTooLarge
        );
        let expanded =
            Value::String("\u{0000}".repeat(SESSION_TERMINAL_PUBLIC_PAYLOAD_MAX_BYTES / 6 + 1));
        assert_eq!(
            encode_public_json(&expanded).unwrap_err(),
            SessionTerminalPublicPayloadUnavailableReason::PayloadTooLarge
        );
    }

    #[test]
    fn r1_message_ack_public_payload_filter_and_root_exports_share_one_implementation() {
        let raw = r#"[{"type":"thinking","thinking":"sealed"},{"type":"text","text":"answer"}]"#;
        let Value::Array(blocks) = parse_terminal_json(raw).unwrap() else {
            panic!("fixture must be an array")
        };
        let blocks = crate::public_session_blocks(blocks).unwrap();
        assert_eq!(
            encode_public_json(&Value::Array(blocks)).unwrap(),
            payload(raw).payload_json
        );
        let exported: crate::SessionTerminalPublicPayload =
            crate::public_terminal_payload_v1(raw).unwrap();
        let persisted: crate::persistence::SessionTerminalPublicPayload = exported.clone();
        assert_eq!(
            persisted.content_digest,
            crate::terminal_public_payload_digest(&exported.payload_json)
        );
        assert_eq!(
            crate::SESSION_TERMINAL_PUBLIC_PAYLOAD_KIND_V1,
            "session-public-v1"
        );
        assert_eq!(
            crate::SESSION_TERMINAL_PUBLIC_PAYLOAD_DIGEST_PREFIX_V1,
            "session-public-v1:sha256:"
        );
        assert_eq!(
            crate::SESSION_TERMINAL_PUBLIC_PAYLOAD_MAX_BYTES,
            8 * 1024 * 1024
        );
        let error: crate::SessionTerminalPublicPayloadUnavailableReason =
            crate::public_session_blocks(vec![json!({"type":"future"})]).unwrap_err();
        assert_eq!(
            error,
            SessionTerminalPublicPayloadUnavailableReason::UnsupportedBlockType
        );
    }
}
