use std::time::Instant;

use serde_json::{json, Value};

use crate::{
    provider_rounds::{build_provider_round_detail, provider_response_text, summarize_text},
    report::{HarnessEvalUsageSummary, ProviderRoundDetail, ProviderRoundSummary},
    HarnessEvalRunnerOptions,
};

#[derive(Debug, Clone)]
pub(crate) struct RealProviderEvalReport {
    pub status: String,
    pub provider_rounds: Vec<ProviderRoundSummary>,
    pub provider_round_details: Vec<ProviderRoundDetail>,
    pub usage: HarnessEvalUsageSummary,
    pub reviewer_markdown: Option<String>,
    pub error: Option<String>,
}

impl RealProviderEvalReport {
    pub(crate) fn blocked(reason: impl Into<String>) -> Self {
        Self {
            status: "blocked".to_string(),
            provider_rounds: Vec::new(),
            provider_round_details: Vec::new(),
            usage: HarnessEvalUsageSummary {
                usage_source: "real_provider_blocked".to_string(),
                ..HarnessEvalUsageSummary::default()
            },
            reviewer_markdown: None,
            error: Some(reason.into()),
        }
    }
}

pub(crate) fn run_deep_real_provider_review(
    options: &HarnessEvalRunnerOptions,
    report_seed: &Value,
) -> RealProviderEvalReport {
    if std::env::var("COWD_EVAL_REAL_MODEL").ok().as_deref() != Some("1") {
        return RealProviderEvalReport::blocked(
            "COWD_EVAL_REAL_MODEL=1 is required before harness-eval consumes provider tokens",
        );
    }

    let Some(model) = options
        .provider
        .clone()
        .or_else(|| std::env::var("COWD_EVAL_MODEL").ok())
        .filter(|value| !value.trim().is_empty())
    else {
        return RealProviderEvalReport::blocked(
            "deep-real requires --provider or COWD_EVAL_MODEL to select the model",
        );
    };

    let started = Instant::now();
    let prompt = reviewer_prompt(report_seed, &model);
    let request_json = json!({
        "model": model,
        "system_prompt": [
            "You are the AI reviewer for Cowd AI Harness deep-real evaluation.",
            "Generate concise but evidence-based Markdown. Do not claim capabilities not supported by the report seed."
        ],
        "user_prompt_summary": summarize_text(&prompt, 1200)
    });
    let request = runtime::ApiRequest {
        system_prompt: vec![
            "You are the AI reviewer for Cowd AI Harness deep-real evaluation.".to_string(),
            "Generate concise but evidence-based Markdown. Do not claim capabilities not supported by the report seed.".to_string(),
        ],
        messages: vec![runtime::ConversationMessage {
            role: runtime::MessageRole::User,
            blocks: vec![runtime::ContentBlock::Text { text: prompt }],
            usage: None,
        }],
        model: model.clone(),
    };

    let events = match runtime::ProviderRuntimeClient::new(model.clone(), Vec::new()).and_then(
        |mut client| {
            use runtime::ApiClient;
            client
                .stream_collect(request)
                .map_err(|error| error.to_string())
        },
    ) {
        Ok(events) => events,
        Err(error) => {
            return RealProviderEvalReport::blocked(format!("provider round failed: {error}"));
        }
    };
    let detail = build_provider_round_detail(
        1,
        "ai_reviewer_full_analysis",
        model,
        request_json,
        &events,
        started.elapsed().as_millis(),
        "provider-rounds/001-round.json",
    );
    let reviewer_markdown = provider_response_text(&events);
    let mut usage = HarnessEvalUsageSummary {
        input_tokens: detail.summary.usage.input_tokens,
        output_tokens: detail.summary.usage.output_tokens,
        cache_creation_input_tokens: detail.summary.usage.cache_creation_input_tokens,
        cache_read_input_tokens: detail.summary.usage.cache_read_input_tokens,
        total_tokens: detail.summary.usage.total_tokens,
        usage_source: detail.summary.usage.usage_source.clone(),
    };
    if usage.total_tokens == 0 {
        usage.usage_source = "provider_event_missing_usage".to_string();
    }
    RealProviderEvalReport {
        status: detail.summary.status.clone(),
        provider_rounds: vec![detail.summary.clone()],
        provider_round_details: vec![detail],
        usage,
        reviewer_markdown: (!reviewer_markdown.trim().is_empty()).then_some(reviewer_markdown),
        error: None,
    }
}

fn reviewer_prompt(report_seed: &Value, model: &str) -> String {
    let mut review_seed = report_seed.clone();
    review_seed["execution_trace"]["provider_rounds"] = json!(1);
    review_seed["execution_trace"]["rounds"] = json!([{
        "round_index": 1,
        "name": "ai_reviewer_full_analysis",
        "model": model,
        "status": "running",
        "note": "this reviewer request is the real provider evidence round"
    }]);
    review_seed["ai_reviewer"] = json!({
        "status": "running",
        "report_source": "provider_round",
        "provider_rounds": 1,
        "model": model
    });
    if let Some(metrics) = review_seed["metrics"].as_array_mut() {
        for metric in metrics {
            if metric["name"].as_str() == Some("provider_rounds") {
                metric["value"] = json!("1");
                metric["notes"] = json!("this reviewer request is provider round 1");
            }
        }
    }
    let seed = serde_json::to_string_pretty(&review_seed).unwrap_or_else(|_| "{}".to_string());
    format!(
        r#"# Task
Generate `full-analysis-report.md` for this Cowd harness eval package.

## Required behavior
- Distinguish deterministic checks, real local tool evidence, and real provider rounds.
- Evaluate whether the evidence truly proves the harness capability.
- Summarize request/response evidence; do not paste full raw payloads.
- Include gaps and risks honestly.
- Output Markdown only.

## Provider round facts
- This request is real provider round 1 for model `{model}`.
- The seed report below has been normalized for review: `execution_trace.provider_rounds=1`.
- In the generated report, use `provider_rounds=1` and explain that the reviewer response itself is the real provider evidence round.

## Report seed
```json
{seed}
```
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reviewer_prompt_overrides_pre_provider_round_seed() {
        let prompt = reviewer_prompt(
            &json!({
                "execution_trace": {"provider_rounds": 0},
                "metrics": [{"name": "provider_rounds", "value": "0"}],
                "provider": "deepseek-v4-flash"
            }),
            "deepseek-v4-flash",
        );

        assert!(prompt.contains("This request is real provider round 1"));
        assert!(prompt.contains("`execution_trace.provider_rounds=1`"));
        assert!(prompt.contains("use `provider_rounds=1`"));
        assert!(prompt.contains(r#""provider_rounds": 1"#));
        assert!(!prompt.contains(r#""provider_rounds": 0"#));
        assert!(prompt.contains(r#""value": "1"#));
        assert!(!prompt.contains(r#""value": "0"#));
    }
}
