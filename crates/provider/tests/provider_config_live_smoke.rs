use provider::{InputMessage, MessageRequest, MessageResponse, OutputContentBlock, ProviderClient};
use runtime::{ConfigLoader, ProviderConfig};
use serde_json::Value;

#[tokio::test]
#[ignore = "requires COWD_AI_HARNESS_LIVE=1 and configured provider credentials"]
async fn provider_config_live_smoke_returns_structured_health_signal() {
    if std::env::var("COWD_AI_HARNESS_LIVE").ok().as_deref() != Some("1") {
        eprintln!("skipping live provider smoke; set COWD_AI_HARNESS_LIVE=1");
        return;
    }

    let cwd = std::env::current_dir().expect("current dir should be available");
    let config = ConfigLoader::default_for(cwd)
        .load()
        .expect("runtime config should load");
    let requested_model = std::env::var("COWD_AI_HARNESS_LIVE_MODEL")
        .ok()
        .or_else(|| config.model().map(str::to_string))
        .expect("live validation requires COWD_AI_HARNESS_LIVE_MODEL or config model");
    let provider = config
        .providers()
        .resolve_full(&requested_model)
        .cloned()
        .or_else(|| fallback_provider_from_env(&requested_model))
        .unwrap_or_else(|| panic!("no provider configured for live model {requested_model:?}"));

    let client = ProviderClient::from_config(&provider).expect("provider client should build");
    let probes = [
        LiveProbe {
            name: "structured_provider",
            max_tokens: 128,
            prompt:
                "Return exactly this JSON object and nothing else: {\"status\":\"ok\",\"capability\":\"live_provider\"}",
            expected_key: "capability",
            expected_value: "live_provider",
        },
        LiveProbe {
            name: "simple_direct_answer",
            max_tokens: 96,
            prompt:
                "A user asks: what is 2+2? Return only JSON: {\"status\":\"ok\",\"route\":\"direct\",\"answer\":\"4\"}",
            expected_key: "route",
            expected_value: "direct",
        },
        LiveProbe {
            name: "complex_planning",
            max_tokens: 160,
            prompt:
                "A user asks to migrate a service safely. Return only JSON: {\"status\":\"ok\",\"route\":\"plan\",\"steps\":[\"inspect\",\"change\",\"verify\"]}",
            expected_key: "route",
            expected_value: "plan",
        },
    ];

    let mut total_tokens = 0;
    let mut response_ids = Vec::new();
    for probe in probes {
        let response = send_live_probe(&client, &requested_model, &probe)
            .await
            .unwrap_or_else(|error| {
                panic!("live provider probe {} should succeed: {error}", probe.name)
            });

        let text = response_text(&response);
        let parsed = parse_json_object(&text).unwrap_or_else(|| {
            panic!(
                "live provider probe {} should contain a JSON object, got: {text:?}",
                probe.name
            )
        });

        assert_eq!(parsed.get("status").and_then(Value::as_str), Some("ok"));
        assert_eq!(
            parsed.get(probe.expected_key).and_then(Value::as_str),
            Some(probe.expected_value)
        );
        assert!(
            response.total_tokens() > 0 || !response.id.is_empty(),
            "provider response should expose usage or an upstream message id"
        );
        total_tokens += response.total_tokens();
        response_ids.push(format!("{}:{}", probe.name, response.id));
    }

    eprintln!(
        "live_provider model={} provider={} probes={} response_ids={} total_tokens={}",
        requested_model,
        provider.name,
        response_ids.len(),
        response_ids.join(","),
        total_tokens
    );
}

#[derive(Debug, Clone, Copy)]
struct LiveProbe {
    name: &'static str,
    max_tokens: u32,
    prompt: &'static str,
    expected_key: &'static str,
    expected_value: &'static str,
}

fn fallback_provider_from_env(model: &str) -> Option<ProviderConfig> {
    let api_key = std::env::var("OPENAI_API_KEY").ok()?;
    let base_url = std::env::var("OPENAI_BASE_URL").ok()?;
    Some(ProviderConfig {
        base_url,
        api_key,
        models: vec![model.to_string()],
        name: "env-openai-compatible".to_string(),
        protocol: Some("openai-compat".to_string()),
    })
}

async fn send_live_probe(
    client: &ProviderClient,
    requested_model: &str,
    probe: &LiveProbe,
) -> Result<MessageResponse, provider::ApiError> {
    let mut last_empty: Option<MessageResponse> = None;
    for attempt in 1..=3 {
        let response = client
            .send_message(&MessageRequest {
                model: requested_model.to_string(),
                max_tokens: probe.max_tokens,
                messages: vec![InputMessage::user_text(probe.prompt)],
                system: Some(
                    "You are validating an AI harness. Return strict JSON only; no markdown."
                        .to_string(),
                ),
                stream: false,
                temperature: Some(0.0),
                ..Default::default()
            })
            .await?;
        if !response_text(&response).trim().is_empty() {
            return Ok(response);
        }
        eprintln!(
            "live_provider probe={} attempt={attempt} returned empty text response_id={} stop_reason={:?} total_tokens={}",
            probe.name,
            response.id,
            response.stop_reason,
            response.total_tokens()
        );
        last_empty = Some(response);
        tokio::time::sleep(std::time::Duration::from_millis(250 * attempt)).await;
    }

    Ok(last_empty.expect("at least one live attempt should have run"))
}

fn response_text(response: &MessageResponse) -> String {
    response
        .content
        .iter()
        .filter_map(|block| match block {
            OutputContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

fn parse_json_object(text: &str) -> Option<Value> {
    serde_json::from_str::<Value>(text)
        .ok()
        .filter(Value::is_object)
        .or_else(|| {
            let start = text.find('{')?;
            let end = text.rfind('}')?;
            serde_json::from_str::<Value>(&text[start..=end])
                .ok()
                .filter(Value::is_object)
        })
}
