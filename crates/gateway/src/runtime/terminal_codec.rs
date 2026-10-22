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
    if schema_version != Some(runtime::SESSION_TERMINAL_ARTIFACT_SCHEMA_VERSION) {
        return Err((
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            format!(
                "terminal artifact schema_version must equal {}",
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
    if !terminal_presentation.as_ref().is_some_and(|presentation| {
        presentation.state == harness_contract::outcome::TerminalPresentationState::Committed
    }) {
        return Err((
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            "current terminal artifact requires a committed terminal_presentation".to_string(),
        ));
    }
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
    account_terminal_usage(&mut decoded, payload.get("token_usage").unwrap())?;
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

// Session summaries sum message usage. The terminal aggregate includes child
// executions and is cumulative, whereas transcript rows carry per-call usage.
// Charge its remainder exactly once while retaining the original final call.
fn account_terminal_usage(
    transcript: &mut [DecodedTerminalTranscriptMessage],
    aggregate: &serde_json::Value,
) -> Result<(), (runtime::RuntimeSessionOutboxFailureClass, String)> {
    let corrupt = |reason: &str| {
        (
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            format!("terminal usage accounting: {reason}"),
        )
    };
    let (last, preceding) = transcript
        .split_last_mut()
        .ok_or_else(|| corrupt("empty transcript"))?;
    let provider_usage = last
        .token_usage_json
        .as_deref()
        .map(serde_json::from_str::<serde_json::Value>)
        .transpose()
        .map_err(|_| corrupt("invalid original Provider usage"))?;
    let mut remainder = aggregate
        .as_object()
        .cloned()
        .ok_or_else(|| corrupt("missing aggregate"))?;
    for field in [
        "input_tokens",
        "output_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
    ] {
        let Some(total) = aggregate.get(field).and_then(serde_json::Value::as_u64) else {
            continue;
        };
        let mut measured = 0u64;
        for message in preceding.iter() {
            if let Some(usage) = message.token_usage_json.as_deref() {
                let usage: serde_json::Value = serde_json::from_str(usage)
                    .map_err(|_| corrupt("invalid preceding Provider usage"))?;
                measured = measured
                    .checked_add(
                        usage
                            .get(field)
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or(0),
                    )
                    .ok_or_else(|| corrupt("message usage overflow"))?;
            }
        }
        let remaining = total
            .checked_sub(measured)
            .ok_or_else(|| corrupt("aggregate is below measured calls"))?;
        if provider_usage
            .as_ref()
            .and_then(|usage| usage.get(field))
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|original| original > remaining)
        {
            return Err(corrupt("aggregate is below final Provider call"));
        }
        remainder.insert(field.into(), remaining.into());
    }
    remainder.insert("accounting_scope".into(), "terminal_remainder".into());
    if let Some(provider_usage) = provider_usage {
        remainder.insert("provider_call_usage".into(), provider_usage);
    }
    last.token_usage_json =
        Some(serde_json::to_string(&remainder).map_err(|_| corrupt("encode remainder"))?);
    Ok(())
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

#[cfg(test)]
mod terminal_accounting_tests {
    use super::*;
    fn rows() -> Vec<DecodedTerminalTranscriptMessage> {
        [(10, 3), (6, 2)]
            .into_iter()
            .map(|(input, output)| DecodedTerminalTranscriptMessage {
                role: "assistant".into(),
                content_json: "[]".into(),
                blocks_count: 0,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: Some(
                    serde_json::json!({"input_tokens":input,"output_tokens":output}).to_string(),
                ),
            })
            .collect()
    }
    fn usage(row: &DecodedTerminalTranscriptMessage) -> serde_json::Value {
        serde_json::from_str(row.token_usage_json.as_deref().unwrap()).unwrap()
    }
    #[test]
    fn terminal_accounting_includes_children_once_and_preserves_provider_usage() {
        let mut transcript = rows();
        account_terminal_usage(&mut transcript, &serde_json::json!({"input_tokens":26,"output_tokens":9,"cache_dimensions_known":false})).unwrap();
        assert_eq!(
            usage(&transcript[0]),
            serde_json::json!({"input_tokens":10,"output_tokens":3})
        );
        assert_eq!(
            usage(&transcript[1]),
            serde_json::json!({"input_tokens":16,"output_tokens":6,
            "cache_dimensions_known":false,"accounting_scope":"terminal_remainder",
            "provider_call_usage":{"input_tokens":6,"output_tokens":2}})
        );
        assert_eq!(
            usage(&transcript[0])["input_tokens"].as_u64().unwrap()
                + usage(&transcript[1])["input_tokens"].as_u64().unwrap(),
            26
        );
    }
    #[test]
    fn terminal_accounting_root_only_does_not_double_count_the_aggregate() {
        let mut transcript = rows();
        account_terminal_usage(
            &mut transcript,
            &serde_json::json!({"input_tokens":16,"output_tokens":5}),
        )
        .unwrap();
        assert_eq!(usage(&transcript[1])["input_tokens"], 6);
        assert_eq!(usage(&transcript[1])["output_tokens"], 2);
        transcript[1].token_usage_json = None;
        account_terminal_usage(
            &mut transcript,
            &serde_json::json!({"input_tokens":16,"output_tokens":5}),
        )
        .unwrap();
        assert_eq!(usage(&transcript[1])["input_tokens"], 6);
    }
    #[test]
    fn terminal_accounting_rejects_totals_below_original_calls() {
        for total in [9, 15] {
            assert!(account_terminal_usage(
                &mut rows(),
                &serde_json::json!({"input_tokens":total,"output_tokens":5})
            )
            .is_err());
        }
    }
    #[test]
    fn terminal_accounting_decoder_wires_cumulative_totals_to_session_rows() {
        let mut payload =
            crate::current_terminal_fixture::current_payload("terminal", "ingress", 1, "done");
        payload["token_usage"] =
            serde_json::json!({"input_tokens":26,"output_tokens":9,"cache_dimensions_known":false});
        payload["transcript"] = serde_json::json!([
            {"role":"assistant","blocks":[{"type":"text","text":"working"}],"usage":{"input_tokens":10,"output_tokens":3}},
            {"role":"assistant","blocks":[{"type":"text","text":"done"}],"usage":{"input_tokens":6,"output_tokens":2}}
        ]);
        let decoded = decode_terminal_payload(&serde_json::to_vec(&payload).unwrap()).unwrap();
        let rows = decoded.transcript.unwrap();
        assert_eq!(usage(&rows[0])["input_tokens"], 10);
        assert_eq!(usage(&rows[1])["input_tokens"], 16);
        assert_eq!(usage(&rows[1])["provider_call_usage"]["input_tokens"], 6);
        assert_eq!(usage(&rows[1])["output_tokens"], 6);
        payload["token_usage"]["input_tokens"] = 15.into();
        assert!(decode_terminal_payload(&serde_json::to_vec(&payload).unwrap()).is_err());
    }
    #[tokio::test]
    async fn terminal_accounting_session_summary_counts_the_remainder_once() {
        let store = crate::pg_test_support::session_store();
        let session_id = "terminal-accounting-original";
        let now = chrono::Utc::now().to_rfc3339();
        store
            .create_session(&session::SessionRecord {
                session_id: session_id.into(),
                platform: "test".into(),
                chat_id: session_id.into(),
                user_id: None,
                model: None,
                created_at: now.clone(),
                last_activity: now,
                message_count: 0,
                reset_policy: "manual".into(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                status: "active".into(),
            })
            .await
            .unwrap();
        let mut transcript = rows();
        account_terminal_usage(
            &mut transcript,
            &serde_json::json!({"input_tokens":26,"output_tokens":9}),
        )
        .unwrap();
        let messages = transcript
            .into_iter()
            .enumerate()
            .map(|(sequence, row)| session::SessionMessage {
                stable_message_id: format!("terminal-accounting:{sequence}"),
                session_id: session_id.into(),
                sequence,
                role: row.role,
                content_json: row.content_json,
                blocks_count: row.blocks_count,
                tool_use_id: row.tool_use_id,
                tool_name: row.tool_name,
                token_usage_json: row.token_usage_json,
                created_at_ms: 1,
            })
            .collect::<Vec<_>>();
        // The ordinary batch insert is a raw repository operation. Counters
        // and exact replay belong to the original fenced terminal transaction.
        let request = session::SessionRuntimeOutboxRequest {
            input_id: "accounting-input".into(),
            request_id: "accounting-request".into(),
            turn_id: "accounting-turn".into(),
            message_id: "accounting-ingress".into(),
            session_generation: 1,
            decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
            target_turn_id: None,
            classification_json: None,
            task_route_hint: None,
            created_at_ms: 100,
            runtime_options_json: None,
        };
        store
            .append_ingress_with_runtime_outbox(
                session_id,
                "user",
                Some(r#"[{"type":"text","text":"input"}]"#),
                100,
                &request,
            )
            .await
            .unwrap();
        let claimed = store
            .claim_session_runtime_outbox("accounting-worker", 101, 10000, 1, &[])
            .await
            .unwrap()
            .remove(0);
        let token = claimed.claim_token.clone().unwrap();
        let running = store
            .mark_session_runtime_outbox_running(
                &request.request_id,
                "accounting-worker",
                1,
                &token,
                claimed.revision,
                102,
            )
            .await
            .unwrap();
        let commit = session::SessionTerminalTranscriptCommit {
            terminal_message_id: "terminal-accounting:1".into(),
            ingress_message_id: request.message_id,
            session_id: session_id.into(),
            turn_id: request.turn_id,
            messages,
            runtime_commit_cursor: 42,
            consumed_input_sequence: running.sequence,
            created_at_ms: 103,
            fence: session::SessionTerminalExecutionFence {
                request_id: request.request_id,
                input_sequence: running.sequence,
                session_generation: 1,
                claim_owner: "accounting-worker".into(),
                claim_token: token,
                claim_fence_epoch: running.claim_fence_epoch.unwrap(),
            },
        };
        for replay in [false, true] {
            let receipt = store
                .commit_terminal_transcript_if_fenced(&commit)
                .await
                .unwrap();
            assert_eq!(receipt.inserted, !replay);
            let actual = store.get_session(session_id).await.unwrap().unwrap();
            assert_eq!(
                (
                    actual.input_tokens,
                    actual.output_tokens,
                    actual.message_count
                ),
                (26, 9, 3)
            );
        }
    }
}
