use super::*;

impl ContextService {
    pub(crate) async fn context_envelope_projection(
        &self,
        session: &SessionService,
        session_id: Option<&str>,
        active_session_ids: &[String],
        limit: usize,
    ) -> serde_json::Value {
        let limit = limit.clamp(1, 100);
        let mut session_ids = Vec::new();
        if let Some(session_id) = session_id.map(str::trim).filter(|value| !value.is_empty()) {
            session_ids.push(session_id.to_string());
        } else {
            session_ids.extend(active_session_ids.iter().cloned());
        }

        if session_ids.is_empty() {
            return empty_context_envelope_projection(
                "ready",
                None,
                Some("no active sessions with ContextEnvelope events"),
                limit,
            );
        }

        let mut events = Vec::new();
        let mut total = 0_usize;
        for session_id in &session_ids {
            match session
                .stored_events_by_type_page(session_id, "ContextEnvelope", 0, limit)
                .await
            {
                Ok(Some((session_total, stored_events))) => {
                    total = total.saturating_add(session_total);
                    events.extend(stored_events.into_iter().map(context_envelope_event_json));
                }
                Ok(None) => {
                    return empty_context_envelope_projection(
                        "degraded",
                        Some("session store not available".to_string()),
                        Some("ContextEnvelope events require unified session store"),
                        limit,
                    );
                }
                Err(error) => {
                    return empty_context_envelope_projection(
                        "degraded",
                        Some(format!("failed to load ContextEnvelope events: {error}")),
                        Some("ContextEnvelope projection query failed"),
                        limit,
                    );
                }
            }
        }

        events.sort_by(|left, right| {
            right
                .get("created_at_ms")
                .and_then(serde_json::Value::as_u64)
                .cmp(
                    &left
                        .get("created_at_ms")
                        .and_then(serde_json::Value::as_u64),
                )
                .then_with(|| {
                    right
                        .get("sequence")
                        .and_then(serde_json::Value::as_u64)
                        .cmp(&left.get("sequence").and_then(serde_json::Value::as_u64))
                })
        });
        events.truncate(limit);
        let summaries = events
            .iter()
            .map(context_envelope_summary_json)
            .collect::<Vec<_>>();
        let latest = events.first().cloned();
        context_envelope_projection_json(latest, summaries, events, total, limit)
    }

    pub(crate) async fn context_history(
        &self,
        session: &SessionService,
        session_id: &str,
        from_seq: usize,
        limit: usize,
        include_envelopes: bool,
    ) -> Result<serde_json::Value, ContextServiceError> {
        let Some((total, stored_events)) = session
            .stored_events_by_type_page(session_id, "ContextEnvelope", from_seq, limit)
            .await
            .map_err(|error| {
                ContextServiceError::Internal(format!("failed to load context timeline: {error}"))
            })?
        else {
            return Err(ContextServiceError::StoreUnavailable(
                "session store not available".to_string(),
            ));
        };

        let envelope_events: Vec<serde_json::Value> = stored_events
            .into_iter()
            .map(context_envelope_event_json)
            .collect();
        let summaries: Vec<serde_json::Value> = envelope_events
            .iter()
            .map(context_envelope_summary_json)
            .collect();
        let next_seq = envelope_events
            .last()
            .and_then(|event| event["sequence"].as_u64())
            .map(|sequence| sequence as usize + 1);
        let has_more = envelope_events.len() < total;
        let envelopes = if include_envelopes {
            envelope_events
        } else {
            Vec::new()
        };

        tracing::info!(
            session_id = session_id,
            include_envelopes = include_envelopes,
            total = total,
            from_seq = from_seq,
            limit = limit,
            "context history loaded"
        );

        Ok(serde_json::json!({
            "session_id": session_id,
            "envelopes": envelopes,
            "summaries": summaries,
            "include_envelopes": include_envelopes,
            "total": total,
            "from_seq": from_seq,
            "next_seq": next_seq,
            "limit": limit,
            "has_more": has_more,
        }))
    }

    pub(crate) async fn context_envelope(
        &self,
        session: &SessionService,
        envelope_id: &str,
    ) -> Result<serde_json::Value, ContextServiceError> {
        let Some(event) = session
            .context_event_by_envelope_id(envelope_id)
            .await
            .map_err(|error| {
                ContextServiceError::Internal(format!("failed to load context envelope: {error}"))
            })?
        else {
            return Err(ContextServiceError::NotFound(format!(
                "context envelope {envelope_id} not found"
            )));
        };

        tracing::info!(
            envelope_id = envelope_id,
            session_id = event.session_id.as_str(),
            sequence = event.sequence,
            "context envelope loaded"
        );

        Ok(serde_json::json!({
            "enabled": true,
            "source": "history",
            "context": context_envelope_event_json(event),
        }))
    }

    pub(crate) async fn context_recommendation_stats(
        &self,
        session: &SessionService,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<serde_json::Value, ContextServiceError> {
        let Some((total, stored_events)) = session
            .stored_domain_events_by_kind_page(
                session_id,
                "context.recommendation_action",
                from_seq,
                limit,
            )
            .await
            .map_err(|error| {
                ContextServiceError::Internal(format!(
                    "failed to load context recommendation stats: {error}"
                ))
            })?
        else {
            return Err(ContextServiceError::StoreUnavailable(
                "session store not available".to_string(),
            ));
        };

        let event_count = stored_events.len();
        let mut grouped: HashMap<String, serde_json::Value> = HashMap::new();
        for event in stored_events {
            let payload = session::SessionDomainEvent::from_session_event(&event)
                .map_err(|error| {
                    ContextServiceError::Internal(format!(
                        "failed to decode context recommendation event: {error}"
                    ))
                })?
                .payload;
            let Some(recommendation) = payload
                .get("recommendation")
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            let action = payload
                .get("action")
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("acknowledged");
            let entry = grouped
                .entry(recommendation.to_string())
                .or_insert_with(|| {
                    serde_json::json!({
                        "recommendation": recommendation,
                        "count": 0_u64,
                        "actions": {},
                        "latest_envelope_id": null,
                        "latest_created_at_ms": 0_u64,
                    })
                });
            let count = entry["count"].as_u64().unwrap_or(0) + 1;
            entry["count"] = serde_json::json!(count);
            let action_count = entry["actions"][action].as_u64().unwrap_or(0) + 1;
            entry["actions"][action] = serde_json::json!(action_count);
            if event.created_at_ms >= entry["latest_created_at_ms"].as_u64().unwrap_or(0) {
                entry["latest_created_at_ms"] = serde_json::json!(event.created_at_ms);
                entry["latest_envelope_id"] = payload
                    .get("envelope_id")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
            }
        }

        let mut recommendations: Vec<serde_json::Value> = grouped.into_values().collect();
        recommendations.sort_by(|left, right| {
            right["count"]
                .as_u64()
                .cmp(&left["count"].as_u64())
                .then_with(|| {
                    left["recommendation"]
                        .as_str()
                        .cmp(&right["recommendation"].as_str())
                })
        });

        Ok(serde_json::json!({
            "session_id": session_id,
            "recommendations": recommendations,
            "total": total,
            "from_seq": from_seq,
            "limit": limit,
            "has_more": event_count < total,
        }))
    }
}

fn context_envelope_event_json(event: SessionEvent) -> serde_json::Value {
    let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
        .unwrap_or_else(|_| serde_json::json!({ "raw": event.event_json }));
    let envelope = payload
        .get("envelope")
        .cloned()
        .unwrap_or_else(|| payload.clone());
    let envelope_id = payload
        .get("envelope_id")
        .cloned()
        .or_else(|| envelope.get("id").cloned())
        .unwrap_or(serde_json::Value::Null);
    let run_id = payload
        .get("run_id")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    serde_json::json!({
        "event_id": format!("{}:{}", event.session_id, event.sequence),
        "session_id": event.session_id,
        "type": event.event_type,
        "sequence": event.sequence,
        "created_at_ms": event.created_at_ms,
        "envelope_id": envelope_id,
        "run_id": run_id,
        "envelope": envelope,
    })
}

fn context_envelope_summary_json(event: &serde_json::Value) -> serde_json::Value {
    let envelope = event
        .get("envelope")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let diagnostics = envelope
        .get("diagnostics")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    serde_json::json!({
        "session_id": event.get("session_id").cloned().unwrap_or(serde_json::Value::Null),
        "sequence": event.get("sequence").cloned().unwrap_or(serde_json::Value::Null),
        "created_at_ms": event.get("created_at_ms").cloned().unwrap_or(serde_json::Value::Null),
        "envelope_id": event.get("envelope_id").cloned().unwrap_or_else(|| envelope.get("id").cloned().unwrap_or(serde_json::Value::Null)),
        "run_id": event.get("run_id").cloned().unwrap_or(serde_json::Value::Null),
        "profile": envelope.get("profile").cloned().unwrap_or(serde_json::Value::Null),
        "intent": envelope.get("intent").cloned().unwrap_or(serde_json::Value::Null),
        "pressure_bp": diagnostics.get("pressure_bp").cloned().unwrap_or(serde_json::Value::Null),
        "selected_count": envelope.get("selected").and_then(|value| value.as_array()).map(|items| items.len()).unwrap_or(0),
        "omitted_count": envelope.get("omitted").and_then(|value| value.as_array()).map(|items| items.len()).unwrap_or(0),
    })
}

fn context_envelope_projection_json(
    latest: Option<serde_json::Value>,
    summaries: Vec<serde_json::Value>,
    events: Vec<serde_json::Value>,
    total: usize,
    limit: usize,
) -> serde_json::Value {
    let Some(latest) = latest else {
        return empty_context_envelope_projection("ready", None, None, limit);
    };
    let envelope = latest
        .get("envelope")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let budget = envelope
        .get("budget")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let diagnostics = envelope
        .get("diagnostics")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let selected = envelope
        .get("selected")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let omitted = envelope
        .get("omitted")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let token_budget = budget
        .get("total_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let used_tokens = budget
        .get("used_tokens")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let pressure_bp = diagnostics
        .get("pressure_bp")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_else(|| {
            if token_budget == 0 {
                0
            } else {
                used_tokens.saturating_mul(10_000) / token_budget
            }
        })
        .min(10_000);
    let used_ratio = if token_budget == 0 {
        0.0
    } else {
        used_tokens as f64 / token_budget as f64
    };
    let latest_checkpoint_id = find_string_key(&envelope, "checkpoint_id");
    let compression_status = if latest_checkpoint_id.is_some() {
        "compressed"
    } else if pressure_bp >= 7_000 || !omitted.is_empty() {
        "should_compress"
    } else {
        "below_threshold"
    };
    let recall_quality_status = if pressure_bp >= 9_000 || omitted.len() > selected.len() {
        "degraded"
    } else if !omitted.is_empty() {
        "attention"
    } else {
        "ready"
    };
    let omission_reasons = omitted
        .iter()
        .filter_map(|item| item.get("reason").and_then(serde_json::Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let protected_count = selected
        .iter()
        .filter(|item| {
            item.get("authority")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|authority| matches!(authority, "System" | "Protected" | "User"))
        })
        .count();
    let latest_envelope_id = latest.get("envelope_id").cloned().unwrap_or_else(|| {
        envelope
            .get("id")
            .cloned()
            .unwrap_or(serde_json::Value::Null)
    });
    let restore_pointer = latest_envelope_id
        .as_str()
        .map(|id| format!("context-envelope:{id}"));

    serde_json::json!({
        "kind": "memory.context_envelope_projection",
        "status": "ready",
        "enabled": true,
        "latest_envelope_id": latest_envelope_id,
        "latest_session_id": latest.get("session_id").cloned().unwrap_or(serde_json::Value::Null),
        "latest_event_id": latest.get("event_id").cloned().unwrap_or(serde_json::Value::Null),
        "latest_checkpoint_id": latest_checkpoint_id,
        "last_written_at": latest.get("created_at_ms").cloned().unwrap_or(serde_json::Value::Null),
        "last_restored_at": serde_json::Value::Null,
        "token_budget": token_budget,
        "used_tokens": used_tokens,
        "used_ratio": used_ratio,
        "pressure_bp": pressure_bp,
        "compression_threshold": 0.70_f64,
        "compression_status": compression_status,
        "recall_quality_status": recall_quality_status,
        "selected_count": selected.len(),
        "omitted_count": omitted.len(),
        "protected_count": protected_count,
        "omission_reasons": omission_reasons,
        "restore_pointer": restore_pointer,
        "degraded_reason": serde_json::Value::Null,
        "summaries": summaries,
        "events": events,
        "total": total,
        "limit": limit,
    })
}

fn empty_context_envelope_projection(
    status: &str,
    degraded_reason: Option<String>,
    message: Option<&str>,
    limit: usize,
) -> serde_json::Value {
    serde_json::json!({
        "kind": "memory.context_envelope_projection",
        "status": status,
        "enabled": status != "disabled",
        "latest_envelope_id": serde_json::Value::Null,
        "latest_session_id": serde_json::Value::Null,
        "latest_event_id": serde_json::Value::Null,
        "latest_checkpoint_id": serde_json::Value::Null,
        "last_written_at": serde_json::Value::Null,
        "last_restored_at": serde_json::Value::Null,
        "token_budget": 0_u64,
        "used_tokens": 0_u64,
        "used_ratio": 0.0_f64,
        "pressure_bp": 0_u64,
        "compression_threshold": 0.70_f64,
        "compression_status": if status == "degraded" { "degraded" } else { "below_threshold" },
        "recall_quality_status": status,
        "selected_count": 0_usize,
        "omitted_count": 0_usize,
        "protected_count": 0_usize,
        "omission_reasons": [],
        "restore_pointer": serde_json::Value::Null,
        "degraded_reason": degraded_reason,
        "message": message,
        "summaries": [],
        "events": [],
        "total": 0_usize,
        "limit": limit,
    })
}

fn find_string_key(value: &serde_json::Value, key: &str) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(found) = map.get(key).and_then(serde_json::Value::as_str) {
                return Some(found.to_string());
            }
            map.values().find_map(|item| find_string_key(item, key))
        }
        serde_json::Value::Array(items) => items.iter().find_map(|item| find_string_key(item, key)),
        _ => None,
    }
}
