//! Canonical decoding and validation for durable Runtime terminal payloads.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedTerminalPayload {
    pub(crate) text: String,
    pub(crate) token_usage_json: Option<String>,
    pub(crate) ingress_message_id: Option<String>,
    pub(crate) transcript: Option<Vec<DecodedTerminalTranscriptMessage>>,
    pub(crate) consumed_input_sequence: Option<usize>,
    pub(crate) terminal_presentation: Option<harness_contract::outcome::TerminalPresentation>,
    pub(crate) goal_completion: harness_contract::goal::GoalCompletion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedTerminalTranscriptMessage {
    pub(crate) role: String,
    pub(crate) content_json: String,
    pub(crate) blocks_count: usize,
    pub(crate) tool_use_id: Option<String>,
    pub(crate) tool_name: Option<String>,
    pub(crate) token_usage_json: Option<String>,
}

pub(super) fn annotate_terminal_tool_instances(
    transcript: &mut [DecodedTerminalTranscriptMessage],
    execution_id: Option<&str>,
    turn_id: Option<&str>,
    ingress_message_id: Option<&str>,
) {
    let mut ordinals = HashMap::<String, u64>::new();
    let mut pending = HashMap::<String, VecDeque<String>>::new();
    for message in transcript {
        let Ok(mut blocks) = serde_json::from_str::<Vec<serde_json::Value>>(&message.content_json)
        else {
            continue;
        };
        for block in &mut blocks {
            let Some(object) = block.as_object_mut() else {
                continue;
            };
            if let Some(turn_id) = turn_id {
                object.insert(
                    "cowd_turn_id".to_string(),
                    serde_json::Value::String(turn_id.to_string()),
                );
            }
            if let Some(ingress_message_id) = ingress_message_id {
                object.insert(
                    "cowd_turn_ingress_message_id".to_string(),
                    serde_json::Value::String(ingress_message_id.to_string()),
                );
            }
            if let Some(execution_id) = execution_id {
                object.insert(
                    "cowd_execution_id".to_string(),
                    serde_json::Value::String(execution_id.to_string()),
                );
            }
            let (provider_id, is_use) = match object.get("type").and_then(serde_json::Value::as_str)
            {
                Some("tool_use") => (
                    object
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                    true,
                ),
                Some("tool_result") => (
                    object
                        .get("tool_use_id")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                    false,
                ),
                _ => (None, false),
            };
            let Some(provider_id) = provider_id else {
                continue;
            };
            let instance_id = if is_use {
                let ordinal = ordinals.entry(provider_id.clone()).or_default();
                let instance_id = format!("{provider_id}#cowd-{ordinal}");
                *ordinal = ordinal.saturating_add(1);
                pending
                    .entry(provider_id)
                    .or_default()
                    .push_back(instance_id.clone());
                instance_id
            } else {
                pending
                    .entry(provider_id.clone())
                    .or_default()
                    .pop_front()
                    .unwrap_or_else(|| {
                        let ordinal = ordinals.entry(provider_id.clone()).or_default();
                        let instance_id = format!("{provider_id}#cowd-{ordinal}");
                        *ordinal = ordinal.saturating_add(1);
                        instance_id
                    })
            };
            object.insert(
                "cowd_tool_instance_id".to_string(),
                serde_json::Value::String(instance_id),
            );
        }
        if let Ok(content_json) = serde_json::to_string(&blocks) {
            message.content_json = content_json;
        }
    }
}

pub(crate) async fn load_terminal_payload(
    artifacts: &runtime::ArtifactStore,
    record: &runtime::RuntimeSessionOutboxRecord,
) -> Result<DecodedTerminalPayload, (runtime::RuntimeSessionOutboxFailureClass, String)> {
    let artifact =
        runtime::decode_session_terminal_artifact_ref(&record.payload_ref).map_err(|error| {
            (
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                error,
            )
        })?;
    let payload = artifacts
        .read(&artifact, &format!("session:{}", record.session_id), None)
        .await
        .map_err(|error| {
            let class = match error {
                runtime::ArtifactError::NotFound
                | runtime::ArtifactError::Io(_)
                | runtime::ArtifactError::Blocking(_) => {
                    runtime::RuntimeSessionOutboxFailureClass::Retryable
                }
                _ => runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            };
            (class, error.to_string())
        })?;
    decode_terminal_payload(&payload)
}

pub(crate) fn decode_terminal_payload(
    encoded: &[u8],
) -> Result<DecodedTerminalPayload, (runtime::RuntimeSessionOutboxFailureClass, String)> {
    let payload = serde_json::from_slice::<serde_json::Value>(encoded).map_err(|error| {
        (
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            error.to_string(),
        )
    })?;
    let schema_version = payload
        .get("schema_version")
        .and_then(serde_json::Value::as_u64);
    if !schema_version.is_some_and(|version| {
        (1..=runtime::SESSION_TERMINAL_ARTIFACT_SCHEMA_VERSION).contains(&version)
    }) {
        return Err((
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            format!(
                "terminal artifact schema_version must be in 1..={}",
                runtime::SESSION_TERMINAL_ARTIFACT_SCHEMA_VERSION
            ),
        ));
    }
    let text = payload
        .get("text")
        .and_then(serde_json::Value::as_str)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| {
            (
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                "terminal payload has no visible text".to_string(),
            )
        })?
        .to_string();
    let token_usage_json = decode_terminal_usage(payload.get("token_usage"), true)?;
    let ingress_message_id = Some(
        payload
            .get("ingress_message_id")
            .and_then(serde_json::Value::as_str)
            .filter(|message_id| !message_id.trim().is_empty())
            .ok_or_else(|| {
                (
                    runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                    "terminal transcript requires ingress_message_id".to_string(),
                )
            })?
            .to_string(),
    );
    let consumed_input_sequence = Some(
        payload
            .get("consumed_input_sequence")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| {
                (
                    runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                    "terminal transcript requires consumed_input_sequence".to_string(),
                )
            })?,
    );
    let terminal_presentation = payload
        .get("terminal_presentation")
        .cloned()
        .map(serde_json::from_value::<harness_contract::outcome::TerminalPresentation>)
        .transpose()
        .map_err(|error| {
            (
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                format!("terminal payload has an invalid presentation: {error}"),
            )
        })?;
    let goal_completion = payload
        .get("goal_completion")
        .cloned()
        .map(serde_json::from_value::<harness_contract::goal::GoalCompletion>)
        .transpose()
        .map_err(|error| {
            (
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                format!("terminal payload has an invalid goal_completion: {error}"),
            )
        })?
        .unwrap_or(harness_contract::goal::GoalCompletion::Satisfied);
    if schema_version.is_some_and(|version| version >= 2)
        && !terminal_presentation.as_ref().is_some_and(|presentation| {
            presentation.state == harness_contract::outcome::TerminalPresentationState::Committed
        })
    {
        return Err((
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            "terminal artifact schema_version 2+ requires a committed terminal_presentation"
                .to_string(),
        ));
    }
    if schema_version == Some(3) {
        let collaboration_evidence = payload.get("collaboration_evidence").ok_or_else(|| {
            (
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                "terminal artifact schema_version 3 requires collaboration_evidence".to_string(),
            )
        })?;
        if !collaboration_evidence.is_null()
            && !collaboration_evidence
                .as_str()
                .is_some_and(|evidence| !evidence.trim().is_empty())
        {
            return Err((
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                "terminal artifact collaboration_evidence must be null or a non-empty string"
                    .to_string(),
            ));
        }
    }
    let messages = payload
        .get("transcript")
        .and_then(serde_json::Value::as_array)
        .filter(|messages| !messages.is_empty() && messages.len() <= 10_000)
        .ok_or_else(|| {
            (
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                "terminal transcript must contain 1..=10000 messages".to_string(),
            )
        })?;
    let mut decoded = Vec::with_capacity(messages.len());
    for message in messages {
        let object = message.as_object().ok_or_else(|| {
            (
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                "terminal transcript message must be an object".to_string(),
            )
        })?;
        let role = object
            .get("role")
            .and_then(serde_json::Value::as_str)
            .filter(|role| matches!(*role, "system" | "user" | "assistant" | "tool"))
            .ok_or_else(|| {
                (
                    runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                    "terminal transcript message has an invalid role".to_string(),
                )
            })?
            .to_string();
        let blocks = object
            .get("blocks")
            .and_then(serde_json::Value::as_array)
            .filter(|blocks| !blocks.is_empty())
            .ok_or_else(|| {
                (
                    runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                    "terminal transcript message must contain blocks".to_string(),
                )
            })?;
        let (tool_use_id, tool_name) = blocks
            .iter()
            .find_map(|block| {
                let block = block.as_object()?;
                match block.get("type").and_then(serde_json::Value::as_str)? {
                    "tool_use" => Some((
                        block
                            .get("id")
                            .and_then(serde_json::Value::as_str)
                            .map(ToOwned::to_owned),
                        block
                            .get("name")
                            .and_then(serde_json::Value::as_str)
                            .map(ToOwned::to_owned),
                    )),
                    "tool_result" => Some((
                        block
                            .get("tool_use_id")
                            .and_then(serde_json::Value::as_str)
                            .map(ToOwned::to_owned),
                        block
                            .get("tool_name")
                            .and_then(serde_json::Value::as_str)
                            .map(ToOwned::to_owned),
                    )),
                    _ => None,
                }
            })
            .unwrap_or((None, None));
        decoded.push(DecodedTerminalTranscriptMessage {
            role,
            content_json: serde_json::to_string(blocks).map_err(|error| {
                (
                    runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                    error.to_string(),
                )
            })?,
            blocks_count: blocks.len(),
            tool_use_id,
            tool_name,
            token_usage_json: decode_terminal_usage(object.get("usage"), false)?,
        });
    }
    let terminal = decoded.last_mut().ok_or_else(|| {
        (
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            "terminal transcript has no final message".to_string(),
        )
    })?;
    let terminal_has_text = terminal.role == "assistant"
        && serde_json::from_str::<serde_json::Value>(&terminal.content_json)
            .ok()
            .and_then(|value| value.as_array().cloned())
            .is_some_and(|blocks| {
                blocks.iter().any(|block| {
                    block.get("type").and_then(serde_json::Value::as_str) == Some("text")
                        && block.get("text").and_then(serde_json::Value::as_str)
                            == Some(text.as_str())
                })
            });
    if !terminal_has_text {
        return Err((
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            "terminal transcript final assistant row does not contain terminal text".to_string(),
        ));
    }
    if terminal.token_usage_json.is_none() {
        terminal.token_usage_json = token_usage_json.clone();
    }
    Ok(DecodedTerminalPayload {
        text,
        token_usage_json,
        ingress_message_id,
        transcript: Some(decoded),
        consumed_input_sequence,
        terminal_presentation,
        goal_completion,
    })
}

fn decode_terminal_usage(
    usage: Option<&serde_json::Value>,
    required_core_fields: bool,
) -> Result<Option<String>, (runtime::RuntimeSessionOutboxFailureClass, String)> {
    let Some(usage) = usage else {
        return if required_core_fields {
            Err((
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                "terminal token_usage is required".to_string(),
            ))
        } else {
            Ok(None)
        };
    };
    let usage = usage.as_object().ok_or_else(|| {
        (
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            "terminal token_usage must be an object".to_string(),
        )
    })?;
    for field in [
        "input_tokens",
        "output_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
    ] {
        let value = usage.get(field);
        if required_core_fields
            && matches!(field, "input_tokens" | "output_tokens")
            && value.is_none()
        {
            return Err((
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                format!("terminal token_usage.{field} is required"),
            ));
        }
        if value.is_some_and(|value| value.as_u64().is_none_or(|value| value > i64::MAX as u64)) {
            return Err((
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                format!(
                    "terminal token_usage.{field} must be a non-negative 64-bit database integer"
                ),
            ));
        }
    }
    serde_json::to_string(usage).map(Some).map_err(|error| {
        (
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            error.to_string(),
        )
    })
}
