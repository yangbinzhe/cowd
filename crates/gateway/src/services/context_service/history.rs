use super::*;

impl ContextService {
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
            .stored_events_by_type_page(session_id, "ContextRecommendationAction", from_seq, limit)
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
            let payload = serde_json::from_str::<serde_json::Value>(&event.event_json)
                .unwrap_or_else(|_| serde_json::json!({}));
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
