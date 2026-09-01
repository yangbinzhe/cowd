use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use model_protocol::usage::TokenUsage;
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub struct ProviderCacheCalibrationOptions {
    pub model: String,
    pub stable_context: PathBuf,
    pub output: PathBuf,
    pub allow_real_model: bool,
}

#[derive(Default)]
struct CalibrationEvidence {
    requests: Mutex<
        Vec<(
            runtime::ProviderRequestEvidenceContext,
            runtime::ProviderWireEvidence,
        )>,
    >,
    outcomes: Mutex<
        Vec<(
            runtime::ProviderRequestEvidenceContext,
            runtime::ProviderAttemptOutcomeEvidence,
        )>,
    >,
}

#[async_trait::async_trait]
impl runtime::ProviderWireEvidenceWriter for CalibrationEvidence {
    async fn persist(
        &self,
        context: &runtime::ProviderRequestEvidenceContext,
        evidence: runtime::ProviderWireEvidence,
    ) -> Result<(), runtime::RuntimeError> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((context.clone(), evidence));
        Ok(())
    }

    async fn persist_outcome(
        &self,
        context: &runtime::ProviderRequestEvidenceContext,
        outcome: runtime::ProviderAttemptOutcomeEvidence,
    ) -> Result<(), runtime::RuntimeError> {
        self.outcomes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((context.clone(), outcome));
        Ok(())
    }
}

/// Run three meaningful, append-only requests through the production Provider
/// client. The first request is a cold calibration sample; only rounds two and
/// three form the warm-provider SLO because a three-round cold-inclusive ratio
/// cannot mathematically approach 90% without artificial padding.
pub fn run_provider_cache_calibration(
    options: ProviderCacheCalibrationOptions,
) -> Result<Value, String> {
    if !options.allow_real_model
        || std::env::var("COWD_EVAL_REAL_MODEL").ok().as_deref() != Some("1")
    {
        return Err(
            "cache calibration requires --allow-real-model and COWD_EVAL_REAL_MODEL=1".to_string(),
        );
    }
    if !options.model.to_ascii_lowercase().contains("deepseek") {
        return Err("cache calibration is restricted to an explicit DeepSeek model".to_string());
    }
    let stable_context = std::fs::read_to_string(&options.stable_context).map_err(|error| {
        format!(
            "cannot read stable calibration context {}: {error}",
            options.stable_context.display()
        )
    })?;
    if stable_context.trim().len() < 8_000 {
        return Err(
            "stable calibration context must contain at least 8000 meaningful characters"
                .to_string(),
        );
    }

    let cwd = std::env::current_dir().map_err(|error| error.to_string())?;
    let config = runtime::ConfigLoader::default_for(&cwd)
        .load()
        .map_err(|error| format!("runtime config load failed: {error}"))?;
    let registry = Arc::new(
        runtime::ProviderRegistry::new(config.providers().clone())
            .map_err(|rejected| rejected.diagnostics.errors.join("; "))?,
    );
    let evidence = Arc::new(CalibrationEvidence::default());
    let mut client =
        runtime::ProviderRuntimeClient::new(registry, options.model.clone(), Vec::new())?;
    {
        use runtime::ApiClient;
        client.configure_provider_wire_evidence(Some(evidence.clone()));
    }

    let session_id = format!("provider-cache-calibration-{}", uuid::Uuid::new_v4());
    let questions = [
        "Return compact JSON with keys round=1, cache_risk, and one concrete invariant from the plan.",
        "Return compact JSON with keys round=2, quality_risk, and one concrete acceptance gate from the plan.",
        "Return compact JSON with keys round=3, concurrency_risk, and one concrete fail-closed rule from the plan.",
    ];
    let stable_sections = vec![
        "You are a concise cache-calibration auditor. Treat the following architecture plan as high-value stable context. Answer only the requested compact JSON and never repeat the plan.".to_string(),
        stable_context,
    ];
    let mut history = Vec::<runtime::ConversationMessage>::new();
    let mut rounds = Vec::new();
    for (index, question) in questions.iter().enumerate() {
        history.push(runtime::ConversationMessage {
            role: runtime::MessageRole::User,
            blocks: vec![runtime::ContentBlock::Text {
                text: (*question).to_string(),
            }],
            usage: None,
        });
        let sequence = index + 1;
        let budget = runtime::context_ledger::RequestBudgetReport::for_attempt(
            &options.model,
            128_000,
            2_048,
            128,
            256,
            0,
        );
        let request = runtime::ApiRequest {
            prompt: runtime::PromptAssembly::new(stable_sections.clone()),
            messages: history.clone().into(),
            model: options.model.clone(),
            reasoning_effort_override: None,
            request_compiler_cache_hit: sequence > 1,
            budget: budget.clone(),
            provider_evidence_context: Some(runtime::ProviderRequestEvidenceContext {
                session_id: session_id.clone(),
                request_sequence: sequence,
                request_compiler_cache_hit: sequence > 1,
                budget,
                attempt: 1,
            }),
        };
        let events = {
            use runtime::ApiClient;
            client
                .stream_collect(request)
                .map_err(|error| format!("provider calibration round {sequence} failed: {error}"))?
        };
        let response = events
            .iter()
            .filter_map(|event| match event {
                runtime::AssistantEvent::TextDelta(text) => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        if response.trim().is_empty() {
            return Err(format!(
                "provider calibration round {sequence} returned no assistant text; event_counts={:?}",
                event_kind_counts(&events)
            ));
        }
        let usage = events.iter().rev().find_map(|event| match event {
            runtime::AssistantEvent::Usage(usage) => Some(*usage),
            _ => None,
        });
        let provider = events.iter().find_map(|event| match event {
            runtime::AssistantEvent::ProviderModel { identity } => Some(identity.clone()),
            _ => None,
        });
        history.push(runtime::ConversationMessage {
            role: runtime::MessageRole::Assistant,
            blocks: vec![runtime::ContentBlock::Text {
                text: response.clone(),
            }],
            usage,
        });
        rounds.push(json!({
            "round": sequence,
            "provider": provider,
            "usage": usage.map(usage_json),
            "response_chars": response.chars().count(),
            "response_preview": response.chars().take(400).collect::<String>(),
        }));
    }

    let request_evidence = evidence
        .requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let outcome_evidence = evidence
        .outcomes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let mut cold_usage = TokenUsage::default();
    let mut warm_usage = TokenUsage::default();
    let mut unknown_usage = 0_u64;
    let mut outcomes = Vec::new();
    for (context, outcome) in &outcome_evidence {
        if let Some(usage) = outcome.usage {
            cold_usage = add_usage(cold_usage, usage);
            if context.request_sequence > 1 {
                warm_usage = add_usage(warm_usage, usage);
            }
        } else {
            unknown_usage = unknown_usage.saturating_add(1);
        }
        outcomes.push(json!({
            "round": context.request_sequence,
            "request_id": outcome.request_id,
            "logical_attempt": outcome.logical_attempt,
            "terminal_status": outcome.terminal_status,
            "usage": outcome.usage.map(usage_json),
        }));
    }
    outcomes.sort_by_key(|value| value["round"].as_u64().unwrap_or_default());

    let mut structural_prompt_bytes = 0_u64;
    let mut structural_reused_bytes = 0_u64;
    let mut prefix = Vec::new();
    let mut identities = std::collections::BTreeSet::new();
    for (context, request) in &request_evidence {
        let observation = &request.prefix_observation;
        identities.insert(observation.cache_identity_sha256.clone());
        if context.request_sequence > 1 {
            structural_prompt_bytes =
                structural_prompt_bytes.saturating_add(observation.prompt_bytes);
            structural_reused_bytes =
                structural_reused_bytes.saturating_add(observation.reusable_prefix_bytes);
        }
        prefix.push(json!({
            "round": context.request_sequence,
            "request_id": request.request_context.request_id,
            "cache_identity_sha256": observation.cache_identity_sha256,
            "predecessor_request_id": observation.predecessor_request_id,
            "prompt_bytes": observation.prompt_bytes,
            "reusable_prefix_bytes": observation.reusable_prefix_bytes,
            "structural_reuse_ratio_bp": observation.structural_reuse_ratio_bp,
            "exact_extension": observation.exact_extension,
            "invalidation_reason": observation.invalidation_reason,
            "cold_leader": observation.cold_leader,
            "waited_for_warmup": observation.waited_for_warmup,
        }));
    }
    prefix.sort_by_key(|value| value["round"].as_u64().unwrap_or_default());
    let warm_structural_ratio_bp = ratio_bp(structural_reused_bytes, structural_prompt_bytes);
    let usage_known = unknown_usage == 0 && outcome_evidence.len() == questions.len();
    let identity_stable = identities.len() == 1;
    let warm_provider_ratio_bp = warm_usage.cache_hit_ratio_bp();
    let passed = usage_known
        && identity_stable
        && request_evidence.len() == questions.len()
        && warm_provider_ratio_bp >= 9_500
        && warm_structural_ratio_bp >= 9_500;
    let report = json!({
        "kind": "cowd.provider_cache_calibration.v1",
        "status": if passed { "passed" } else { "failed" },
        "model": options.model,
        "stable_context": options.stable_context,
        "session_id": session_id,
        "rounds": rounds,
        "attempt_outcomes": outcomes,
        "prefix_observations": prefix,
        "summary": {
            "provider_attempts": outcome_evidence.len(),
            "usage_unknown_attempts": unknown_usage,
            "cache_identity_count": identities.len(),
            "cold_inclusive_provider_cache_ratio_bp": cold_usage.cache_hit_ratio_bp(),
            "warm_provider_cache_ratio_bp": warm_provider_ratio_bp,
            "warm_structural_reuse_ratio_bp": warm_structural_ratio_bp,
            "cold_inclusive_usage": usage_json(cold_usage),
            "warm_usage": usage_json(warm_usage),
            "warm_structural_prompt_bytes": structural_prompt_bytes,
            "warm_structural_reused_bytes": structural_reused_bytes,
        },
        "gates": {
            "three_terminal_attempts": outcome_evidence.len() == questions.len(),
            "usage_known": usage_known,
            "one_cache_identity": identity_stable,
            "warm_provider_cache_at_least_95pct": warm_provider_ratio_bp >= 9_500,
            "warm_structural_reuse_at_least_95pct": warm_structural_ratio_bp >= 9_500,
            "cold_inclusive_90pct_not_applicable": true,
        },
        "notes": [
            "The stable context is the audited architecture plan, not synthetic padding.",
            "Cold-inclusive 90% is evaluated only by the long-running real collaboration scenario; a three-round calibration mathematically cannot amortize one cold request to 90%.",
        ],
    });
    write_report(&options.output, &report)?;
    if !passed {
        return Err(format!(
            "provider cache calibration failed; see {}",
            options.output.display()
        ));
    }
    Ok(report)
}

fn event_kind_counts(
    events: &[runtime::AssistantEvent],
) -> std::collections::BTreeMap<&'static str, usize> {
    let mut counts = std::collections::BTreeMap::new();
    for event in events {
        let kind = match event {
            runtime::AssistantEvent::ProviderModel { .. } => "provider_model",
            runtime::AssistantEvent::ItemStarted { .. } => "item_started",
            runtime::AssistantEvent::ItemCompleted { .. } => "item_completed",
            runtime::AssistantEvent::TextDelta(_) => "text_delta",
            runtime::AssistantEvent::ReasoningSummaryDelta(_) => "reasoning_summary_delta",
            runtime::AssistantEvent::PrivateReasoningDelta(_) => "private_reasoning_delta",
            runtime::AssistantEvent::SignatureDelta(_) => "signature_delta",
            runtime::AssistantEvent::ToolUse { .. } => "tool_use",
            runtime::AssistantEvent::Usage(_) => "usage",
            runtime::AssistantEvent::MessageStop => "message_stop",
            runtime::AssistantEvent::ToolStart { .. } => "tool_start",
            runtime::AssistantEvent::ToolProgress { .. } => "tool_progress",
            runtime::AssistantEvent::ToolComplete { .. } => "tool_complete",
        };
        *counts.entry(kind).or_insert(0) += 1;
    }
    counts
}

fn add_usage(left: TokenUsage, right: TokenUsage) -> TokenUsage {
    TokenUsage {
        input_tokens: left.input_tokens.saturating_add(right.input_tokens),
        output_tokens: left.output_tokens.saturating_add(right.output_tokens),
        cache_creation_input_tokens: left
            .cache_creation_input_tokens
            .saturating_add(right.cache_creation_input_tokens),
        cache_read_input_tokens: left
            .cache_read_input_tokens
            .saturating_add(right.cache_read_input_tokens),
    }
}

fn usage_json(usage: TokenUsage) -> Value {
    json!({
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "cache_creation_input_tokens": usage.cache_creation_input_tokens,
        "cache_read_input_tokens": usage.cache_read_input_tokens,
        "prompt_input_tokens": usage.prompt_input_tokens(),
        "cache_hit_ratio_bp": usage.cache_hit_ratio_bp(),
        "total_tokens": usage.total_tokens(),
    })
}

fn ratio_bp(numerator: u64, denominator: u64) -> u32 {
    if denominator == 0 {
        return 0;
    }
    u32::try_from(numerator.saturating_mul(10_000) / denominator)
        .unwrap_or(10_000)
        .min(10_000)
}

fn write_report(path: &Path, report: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(report)
        .map_err(|error| format!("cannot serialize cache calibration report: {error}"))?;
    std::fs::write(path, bytes).map_err(|error| format!("cannot write {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratio_and_usage_keep_every_prompt_bucket() {
        let usage = add_usage(
            TokenUsage {
                input_tokens: 100,
                output_tokens: 3,
                cache_creation_input_tokens: 20,
                cache_read_input_tokens: 880,
            },
            TokenUsage {
                input_tokens: 10,
                output_tokens: 2,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 990,
            },
        );
        assert_eq!(usage.prompt_input_tokens(), 2_000);
        assert_eq!(usage.cache_hit_ratio_bp(), 9_350);
        assert_eq!(ratio_bp(1_900, 2_000), 9_500);
    }
}
