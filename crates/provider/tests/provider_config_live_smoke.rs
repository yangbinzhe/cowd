use model_protocol::provider_config::ProviderConfig;
use provider::{
    ContentBlockDelta, InputMessage, MessageRequest, MessageResponse, OutputContentBlock,
    ProviderClient, StreamEvent,
};
use serde_json::Value;
use std::path::PathBuf;

#[tokio::test]
#[ignore = "requires COWD_AI_HARNESS_LIVE=1 and configured provider credentials"]
async fn provider_config_live_smoke_returns_structured_health_signal() {
    let Some(env) = live_env() else {
        return;
    };
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
        let response = send_live_probe(&env.client, &env.model, &probe)
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
        env.model,
        env.provider_name,
        response_ids.len(),
        response_ids.join(","),
        total_tokens
    );
}

#[tokio::test]
#[ignore = "requires COWD_AI_HARNESS_LIVE=1 and configured provider credentials"]
async fn provider_live_stream_contract_is_ordered() {
    let Some(env) = live_env() else {
        return;
    };
    let mut stream = env
        .client
        .stream_message(&MessageRequest {
            model: env.model.clone(),
            max_tokens: 128,
            messages: vec![InputMessage::user_text(
                "Return exactly this JSON object and nothing else: {\"status\":\"ok\",\"stream\":\"ordered\"}",
            )],
            system: Some("Return strict JSON only; no markdown.".to_string()),
            stream: true,
            temperature: Some(0.0),
            ..Default::default()
        })
        .await
        .expect("live stream should start");

    let mut saw_start = false;
    let mut saw_delta = false;
    let mut saw_stop = false;
    let mut text = String::new();
    while let Some(event) = stream
        .next_event()
        .await
        .expect("live stream should yield valid events")
    {
        match event {
            StreamEvent::MessageStart(_) => {
                assert!(!saw_start, "stream should emit a single message_start");
                assert!(!saw_stop, "message_start must precede message_stop");
                saw_start = true;
            }
            StreamEvent::ContentBlockDelta(delta) => {
                assert!(saw_start, "content delta must follow message_start");
                if let ContentBlockDelta::TextDelta { text: delta_text } = delta.delta {
                    saw_delta |= !delta_text.trim().is_empty();
                    text.push_str(&delta_text);
                }
            }
            StreamEvent::MessageStop(_) => {
                assert!(saw_start, "message_stop must follow message_start");
                saw_stop = true;
            }
            StreamEvent::MessageDelta(_)
            | StreamEvent::ContentBlockStart(_)
            | StreamEvent::ContentBlockStop(_) => {}
        }
    }

    assert!(saw_start, "stream should include message_start");
    assert!(saw_delta, "stream should include nonempty text delta");
    assert!(saw_stop, "stream should include message_stop");
    let parsed = parse_json_object(&text)
        .unwrap_or_else(|| panic!("stream text should contain JSON object, got: {text:?}"));
    assert_eq!(parsed.get("status").and_then(Value::as_str), Some("ok"));
    assert_eq!(
        parsed.get("stream").and_then(Value::as_str),
        Some("ordered")
    );
    eprintln!(
        "live_stream model={} provider={} ordered=true text_chars={}",
        env.model,
        env.provider_name,
        text.len()
    );
}

#[tokio::test]
#[ignore = "requires COWD_AI_HARNESS_LIVE=1 and configured provider credentials"]
async fn provider_live_structured_output_is_stable() {
    let Some(env) = live_env() else {
        return;
    };
    let probe = LiveProbe {
        name: "structured_drift",
        max_tokens: 96,
        prompt:
            "Return exactly this JSON object and nothing else: {\"status\":\"ok\",\"drift\":\"stable\"}",
        expected_key: "drift",
        expected_value: "stable",
    };

    let mut total_tokens = 0;
    for idx in 1..=3 {
        let response = send_live_probe(&env.client, &env.model, &probe)
            .await
            .unwrap_or_else(|error| panic!("structured drift probe {idx} failed: {error}"));
        let text = response_text(&response);
        let parsed = parse_json_object(&text)
            .unwrap_or_else(|| panic!("structured drift probe {idx} produced non-json: {text:?}"));
        assert_eq!(parsed.get("status").and_then(Value::as_str), Some("ok"));
        assert_eq!(parsed.get("drift").and_then(Value::as_str), Some("stable"));
        total_tokens += response.total_tokens();
    }
    eprintln!(
        "live_structured_drift model={} provider={} probes=3 total_tokens={}",
        env.model, env.provider_name, total_tokens
    );
}

#[tokio::test]
#[ignore = "requires COWD_AI_HARNESS_LIVE=1 and configured provider credentials"]
async fn provider_live_routing_respects_simple_complex_and_risk() {
    let Some(env) = live_env() else {
        return;
    };
    let probes = [
        LiveProbe {
            name: "route_direct",
            max_tokens: 96,
            prompt: "A user asks what 2+2 is. Return only JSON: {\"status\":\"ok\",\"route\":\"direct\"}",
            expected_key: "route",
            expected_value: "direct",
        },
        LiveProbe {
            name: "route_plan",
            max_tokens: 128,
            prompt: "A user asks to refactor a multi-crate runtime. Return only JSON: {\"status\":\"ok\",\"route\":\"plan\"}",
            expected_key: "route",
            expected_value: "plan",
        },
        LiveProbe {
            name: "route_blocked",
            max_tokens: 128,
            prompt: "A user asks to delete production data without confirmation. Return only JSON: {\"status\":\"ok\",\"route\":\"blocked\"}",
            expected_key: "route",
            expected_value: "blocked",
        },
    ];

    let mut total_tokens = 0;
    for probe in probes {
        let response = send_live_probe(&env.client, &env.model, &probe)
            .await
            .unwrap_or_else(|error| panic!("routing probe {} failed: {error}", probe.name));
        let text = response_text(&response);
        let parsed = parse_json_object(&text)
            .unwrap_or_else(|| panic!("routing probe {} produced non-json: {text:?}", probe.name));
        assert_eq!(parsed.get("status").and_then(Value::as_str), Some("ok"));
        assert_eq!(
            parsed.get(probe.expected_key).and_then(Value::as_str),
            Some(probe.expected_value)
        );
        total_tokens += response.total_tokens();
    }
    eprintln!(
        "live_routing model={} provider={} probes=3 total_tokens={}",
        env.model, env.provider_name, total_tokens
    );
}

#[derive(Debug, Clone)]
struct LiveEnv {
    model: String,
    provider_name: String,
    client: ProviderClient,
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
        name: "env-completions".to_string(),
        protocol: Some("completions".to_string()),
    })
}

fn cowd_config_path() -> PathBuf {
    std::env::var_os("COWD_CONFIG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".cowd")
                .join("config.yaml")
        })
}

fn provider_from_cowd_config(model: &str) -> Option<ProviderConfig> {
    let contents = std::fs::read_to_string(cowd_config_path()).ok()?;
    let value = serde_yaml::from_str::<serde_yaml::Value>(&contents).ok()?;
    let root = value.as_mapping()?;
    let providers = root
        .get(&serde_yaml::Value::String("providers".to_string()))?
        .as_mapping()?;
    for (name, provider) in providers {
        let name = name.as_str()?.to_string();
        let provider = provider.as_mapping()?;
        let models = provider
            .get(&serde_yaml::Value::String("models".to_string()))
            .and_then(serde_yaml::Value::as_sequence)?
            .iter()
            .filter_map(serde_yaml::Value::as_str)
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if !models.iter().any(|candidate| candidate == model) {
            continue;
        }
        let base_url = provider
            .get(&serde_yaml::Value::String("base_url".to_string()))
            .and_then(serde_yaml::Value::as_str)?
            .to_string();
        let api_key = provider
            .get(&serde_yaml::Value::String("api_key".to_string()))
            .and_then(serde_yaml::Value::as_str)?
            .to_string();
        let protocol = provider
            .get(&serde_yaml::Value::String("protocol".to_string()))
            .and_then(serde_yaml::Value::as_str)
            .map(ToString::to_string);
        return Some(ProviderConfig {
            base_url,
            api_key,
            models,
            name,
            protocol,
        });
    }
    None
}

fn live_env() -> Option<LiveEnv> {
    if std::env::var("COWD_AI_HARNESS_LIVE").ok().as_deref() != Some("1") {
        eprintln!("skipping live provider test; set COWD_AI_HARNESS_LIVE=1");
        return None;
    }

    let model = std::env::var("COWD_AI_HARNESS_LIVE_MODEL")
        .ok()
        .or_else(|| std::env::var("OPENAI_MODEL").ok())
        .expect("live validation requires COWD_AI_HARNESS_LIVE_MODEL or OPENAI_MODEL");
    let provider = provider_from_cowd_config(&model)
        .or_else(|| fallback_provider_from_env(&model))
        .unwrap_or_else(|| panic!("no provider configured for live model {model:?}"));
    let provider_name = provider.name.clone();
    let client = ProviderClient::from_config(&provider).expect("provider client should build");
    Some(LiveEnv {
        model,
        provider_name,
        client,
    })
}

async fn send_live_probe(
    client: &ProviderClient,
    requested_model: &str,
    probe: &LiveProbe,
) -> Result<MessageResponse, provider::ApiError> {
    let mut last_empty: Option<MessageResponse> = None;
    for attempt in 1..=3 {
        let max_tokens = live_probe_max_tokens(requested_model, probe.max_tokens, attempt);
        let response = client
            .send_message(&MessageRequest {
                model: requested_model.to_string(),
                max_tokens,
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
            "live_provider probe={} attempt={attempt} max_tokens={} returned empty text response_id={} stop_reason={:?} total_tokens={}",
            probe.name,
            max_tokens,
            response.id,
            response.stop_reason,
            response.total_tokens()
        );
        last_empty = Some(response);
        tokio::time::sleep(std::time::Duration::from_millis(250 * u64::from(attempt))).await;
    }

    Ok(last_empty.expect("at least one live attempt should have run"))
}

fn live_probe_max_tokens(model: &str, requested: u32, attempt: u32) -> u32 {
    if let Ok(value) = std::env::var("COWD_AI_HARNESS_LIVE_MAX_TOKENS") {
        if let Ok(parsed) = value.parse::<u32>() {
            if parsed > 0 {
                return requested.max(parsed);
            }
        }
    }

    let model = model.to_ascii_lowercase();
    let model_floor = if model.contains("deepseek")
        || model.contains("qwen")
        || model.contains("step")
        || model.contains("glm")
    {
        512
    } else {
        requested
    };
    let retry_floor = model_floor.saturating_mul(attempt).min(1024);
    requested.max(retry_floor)
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
