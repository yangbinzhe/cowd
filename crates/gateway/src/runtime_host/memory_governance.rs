use std::sync::{Arc, Weak};

use async_trait::async_trait;

use crate::runtime_service::RuntimeService;

pub(crate) struct GatewaySemanticGovernanceResolver {
    runtime: Weak<RuntimeService>,
}

impl GatewaySemanticGovernanceResolver {
    pub(crate) fn new(runtime: &Arc<RuntimeService>) -> Self {
        Self {
            runtime: Arc::downgrade(runtime),
        }
    }
}

#[async_trait]
impl memory::SemanticGovernanceResolver for GatewaySemanticGovernanceResolver {
    async fn resolve(
        &self,
        request: memory::SemanticGovernanceRequest,
    ) -> Result<memory::SemanticGovernanceResponse, String> {
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| "Gateway Runtime is shutting down".to_string())?;
        let model = runtime.configured_model().ok_or_else(|| {
            "no configured model is available for semantic governance".to_string()
        })?;
        let services = runtime.runtime_services();
        let client = runtime::ProviderRuntimeClient::new_with_transport_and_template_cache(
            Arc::clone(services.provider_registry()),
            Arc::clone(services.provider_transport_pool()),
            Arc::clone(services.provider_template_cache()),
            model.clone(),
            Vec::new(),
        )?;
        let candidate_count = request.candidates.len();
        let input = serde_json::to_string(&request).map_err(|error| error.to_string())?;
        let completion = client
            .complete_control_analysis(
                &model,
                SEMANTIC_GOVERNANCE_SYSTEM_PROMPT,
                input,
                semantic_output_budget(candidate_count),
            )
            .await?;
        let mut response = parse_semantic_governance_response(&completion.text)?;
        response.model = Some(completion.model);
        response.input_tokens = completion.input_tokens;
        response.output_tokens = completion.output_tokens;
        Ok(response)
    }
}

const SEMANTIC_GOVERNANCE_SYSTEM_PROMPT: &str = r#"You are Cowd's bounded memory-governance analyst.
Return exactly one JSON object matching:
{"decisions":[{"candidate_id":"...","action":"dismiss|archive|supersede|require_review","canonical_memory_id":null,"confidence_bp":0,"rationale":"..."}]}

Rules:
- Return at most one decision per candidate and never invent candidate or memory ids.
- Preserve evidence. Use require_review whenever facts, authority, scope, or intent are uncertain.
- dismiss is only for a false-positive relationship_refresh candidate.
- archive is only for a truly obsolete stale candidate.
- supersede is only for a duplicate/conflict with one clearly canonical entry; set canonical_memory_id.
- confidence_bp is 0..10000. Automatic changes require very strong evidence.
- Do not output markdown, prose, tools, or additional keys."#;

fn semantic_output_budget(candidate_count: usize) -> u32 {
    let candidate_budget = u32::try_from(candidate_count)
        .unwrap_or(u32::MAX)
        .saturating_mul(160);
    768_u32.saturating_add(candidate_budget).clamp(1_024, 8_192)
}

fn parse_semantic_governance_response(
    raw: &str,
) -> Result<memory::SemanticGovernanceResponse, String> {
    let trimmed = raw.trim();
    if let Ok(response) = serde_json::from_str(trimmed) {
        return Ok(response);
    }
    let fenced = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```JSON"))
        .or_else(|| trimmed.strip_prefix("```"))
        .and_then(|body| body.strip_suffix("```"))
        .map(str::trim)
        .ok_or_else(|| "semantic governance response is not valid JSON".to_string())?;
    serde_json::from_str(fenced)
        .map_err(|error| format!("semantic governance response is invalid: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_response_parser_accepts_plain_and_fenced_json() {
        for raw in [r#"{"decisions":[]}"#, "```json\n{\"decisions\":[]}\n```"] {
            let response = parse_semantic_governance_response(raw).expect("parse response");
            assert!(response.decisions.is_empty());
        }
    }

    #[test]
    fn semantic_output_budget_is_bounded_and_scales_with_work() {
        assert_eq!(semantic_output_budget(0), 1_024);
        assert!(semantic_output_budget(20) > semantic_output_budget(1));
        assert_eq!(semantic_output_budget(usize::MAX), 8_192);
    }
}
