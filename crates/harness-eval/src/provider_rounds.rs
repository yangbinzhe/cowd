use serde_json::{json, Value};

use crate::report::{ProviderRoundDetail, ProviderRoundSummary, UsageSummary};

pub(crate) fn build_provider_round_detail(
    round_index: usize,
    name: impl Into<String>,
    model: impl Into<String>,
    request: Value,
    events: &[runtime::AssistantEvent],
    elapsed_ms: u128,
    detail_path: impl Into<String>,
) -> ProviderRoundDetail {
    let name = name.into();
    let model = model.into();
    let detail_path = detail_path.into();
    let response_text = provider_response_text(events);
    let usage = provider_usage(events);
    let text_delta_count = events
        .iter()
        .filter(|event| matches!(event, runtime::AssistantEvent::TextDelta(_)))
        .count();
    let tool_use_count = events
        .iter()
        .filter(|event| matches!(event, runtime::AssistantEvent::ToolUse { .. }))
        .count();
    let summary = ProviderRoundSummary {
        round_index,
        name,
        model,
        status: if response_text.trim().is_empty() {
            "failed".to_string()
        } else {
            "passed".to_string()
        },
        elapsed_ms,
        usage,
        text_delta_count,
        tool_use_count,
        request_summary: summarize_json(&request, 320),
        response_summary: summarize_text(&response_text, 320),
        detail_path,
    };
    ProviderRoundDetail {
        summary,
        request,
        events: events.iter().map(provider_event_json).collect(),
        response_text,
    }
}

pub(crate) fn provider_response_text(events: &[runtime::AssistantEvent]) -> String {
    events
        .iter()
        .filter_map(|event| match event {
            runtime::AssistantEvent::TextDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>()
}

pub(crate) fn provider_usage(events: &[runtime::AssistantEvent]) -> UsageSummary {
    let mut usage = UsageSummary {
        usage_source: "provider_event".to_string(),
        ..UsageSummary::default()
    };
    for event in events {
        let runtime::AssistantEvent::Usage(item) = event else {
            continue;
        };
        usage.input_tokens = usage.input_tokens.saturating_add(item.input_tokens);
        usage.output_tokens = usage.output_tokens.saturating_add(item.output_tokens);
        usage.cache_creation_input_tokens = usage
            .cache_creation_input_tokens
            .saturating_add(item.cache_creation_input_tokens);
        usage.cache_read_input_tokens = usage
            .cache_read_input_tokens
            .saturating_add(item.cache_read_input_tokens);
        usage.total_tokens = usage.total_tokens.saturating_add(item.total_tokens());
    }
    if usage.total_tokens == 0 {
        usage.usage_source = "provider_event_missing_usage".to_string();
    }
    usage
}

pub(crate) fn summarize_text(text: &str, limit: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= limit {
        compact
    } else {
        format!("{}...", compact.chars().take(limit).collect::<String>())
    }
}

fn summarize_json(value: &Value, limit: usize) -> String {
    summarize_text(&value.to_string(), limit)
}

fn provider_event_json(event: &runtime::AssistantEvent) -> Value {
    match event {
        runtime::AssistantEvent::ProviderModel { identity } => {
            json!({
                "kind": "provider_model",
                "provider": identity.provider_name,
                "model": identity.model,
                "profile": identity.profile,
                "protocol": identity.protocol,
                "registry_revision": identity.registry_revision,
            })
        }
        runtime::AssistantEvent::ItemStarted {
            index,
            provider_item_id,
            kind,
        } => {
            let item_kind = match kind {
                runtime::AssistantItemKind::Text => "text",
                runtime::AssistantItemKind::PublicReasoning => "public_reasoning",
                runtime::AssistantItemKind::PrivateReasoning => "private_reasoning",
                runtime::AssistantItemKind::ToolCall => "tool_call",
            };
            json!({
                "kind": "item_started",
                "index": index,
                "provider_item_id": provider_item_id,
                "item_kind": item_kind,
            })
        }
        runtime::AssistantEvent::ItemCompleted { index } => {
            json!({"kind": "item_completed", "index": index})
        }
        runtime::AssistantEvent::TextDelta(text) => {
            json!({"kind": "text_delta", "text_summary": summarize_text(text, 240)})
        }
        runtime::AssistantEvent::ReasoningSummaryDelta(text) => {
            json!({"kind": "thinking_delta", "text_summary": summarize_text(text, 240)})
        }
        runtime::AssistantEvent::PrivateReasoningDelta(text) => {
            json!({"kind": "private_reasoning_delta", "length": text.len()})
        }
        runtime::AssistantEvent::SignatureDelta(signature) => {
            json!({"kind": "signature_delta", "length": signature.len()})
        }
        runtime::AssistantEvent::ToolUse { id, name, input } => {
            json!({"kind": "tool_use", "id": id, "name": name, "input_summary": summarize_text(input, 240)})
        }
        runtime::AssistantEvent::Usage(usage) => {
            json!({
                "kind": "usage",
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "cache_creation_input_tokens": usage.cache_creation_input_tokens,
                "cache_read_input_tokens": usage.cache_read_input_tokens,
                "total_tokens": usage.total_tokens()
            })
        }
        runtime::AssistantEvent::MessageStop => json!({"kind": "message_stop"}),
        runtime::AssistantEvent::ToolStart { id, name, preview } => {
            json!({"kind": "tool_start", "id": id, "name": name, "preview": preview})
        }
        runtime::AssistantEvent::ToolProgress { id, name, progress } => {
            json!({"kind": "tool_progress", "id": id, "name": name, "progress": progress})
        }
        runtime::AssistantEvent::ToolComplete {
            id,
            name,
            result_summary,
            exit_code,
        } => {
            json!({"kind": "tool_complete", "id": id, "name": name, "result_summary": result_summary, "exit_code": exit_code})
        }
    }
}
