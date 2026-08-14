use serde::{Deserialize, Serialize};

use crate::model_registry::global_registry;
use crate::provider_config::ProviderProtocol;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityState {
    Supported,
    Unsupported,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilitySource {
    Configured,
    Probed,
    Bundled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityFact {
    pub state: CapabilityState,
    pub source: CapabilitySource,
}

impl CapabilityFact {
    const fn bundled(state: CapabilityState) -> Self {
        Self {
            state,
            source: CapabilitySource::Bundled,
        }
    }

    const fn configured(state: CapabilityState) -> Self {
        Self {
            state,
            source: CapabilitySource::Configured,
        }
    }
}

/// Orthogonal provider/model facts. These describe protocol behavior only;
/// Runtime admission, local concurrency and tool effect safety remain separate
/// contracts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCapabilityProfile {
    pub supports_tool_calls: CapabilityFact,
    /// Whether the wire endpoint accepts an explicit `tool_choice` request
    /// field. This is independent from merely advertising tool schemas.
    #[serde(default = "unknown_capability_fact")]
    pub supports_explicit_tool_choice: CapabilityFact,
    pub supports_multiple_tool_calls: CapabilityFact,
    pub supports_parallel_tool_calls_request: CapabilityFact,
    pub streams_tool_arguments: CapabilityFact,
    pub supports_public_reasoning_summary: CapabilityFact,
    pub requires_reasoning_signature_roundtrip: CapabilityFact,
}

const fn unknown_capability_fact() -> CapabilityFact {
    CapabilityFact::bundled(CapabilityState::Unknown)
}

impl ProviderCapabilityProfile {
    #[must_use]
    pub const fn unknown() -> Self {
        let unknown = CapabilityFact::bundled(CapabilityState::Unknown);
        Self {
            supports_tool_calls: unknown,
            supports_explicit_tool_choice: unknown,
            supports_multiple_tool_calls: unknown,
            supports_parallel_tool_calls_request: unknown,
            streams_tool_arguments: unknown,
            supports_public_reasoning_summary: unknown,
            requires_reasoning_signature_roundtrip: unknown,
        }
    }

    #[must_use]
    pub fn resolve(protocol: ProviderProtocol, model: &str) -> Self {
        let parallel_request = match protocol {
            ProviderProtocol::Anthropic => CapabilityState::Unsupported,
            ProviderProtocol::Completions | ProviderProtocol::Responses => {
                CapabilityState::Supported
            }
        };
        let public_reasoning = match protocol {
            // Cowd does not yet expose a protocol-stable public summary codec
            // for Anthropic Messages or generic Chat Completions.
            ProviderProtocol::Anthropic | ProviderProtocol::Completions => CapabilityState::Unknown,
            ProviderProtocol::Responses => CapabilityState::Supported,
        };
        let signature_roundtrip = match protocol {
            ProviderProtocol::Anthropic => CapabilityState::Supported,
            ProviderProtocol::Completions => CapabilityState::Unknown,
            ProviderProtocol::Responses => CapabilityState::Unsupported,
        };
        let mut profile = Self {
            supports_tool_calls: CapabilityFact::bundled(CapabilityState::Supported),
            supports_explicit_tool_choice: CapabilityFact::bundled(CapabilityState::Supported),
            supports_multiple_tool_calls: CapabilityFact::bundled(CapabilityState::Supported),
            supports_parallel_tool_calls_request: CapabilityFact::bundled(parallel_request),
            streams_tool_arguments: CapabilityFact::bundled(CapabilityState::Supported),
            supports_public_reasoning_summary: CapabilityFact::bundled(public_reasoning),
            requires_reasoning_signature_roundtrip: CapabilityFact::bundled(signature_roundtrip),
        };

        if let Some(info) = global_registry().capacity_model_info(model) {
            apply_configured_tags(&mut profile, &info.capabilities);
        }
        profile
    }
}

fn apply_configured_tags(profile: &mut ProviderCapabilityProfile, tags: &[String]) {
    for tag in tags {
        let (state, name) = tag
            .strip_prefix("no_")
            .map_or((CapabilityState::Supported, tag.as_str()), |name| {
                (CapabilityState::Unsupported, name)
            });
        let fact = CapabilityFact::configured(state);
        match name {
            "tool_use" | "tool_calls" => profile.supports_tool_calls = fact,
            "explicit_tool_choice" => profile.supports_explicit_tool_choice = fact,
            "multiple_tool_calls" => profile.supports_multiple_tool_calls = fact,
            "parallel_tool_calls" => profile.supports_parallel_tool_calls_request = fact,
            "stream_tool_arguments" => profile.streams_tool_arguments = fact,
            "public_reasoning_summary" => profile.supports_public_reasoning_summary = fact,
            "reasoning_signature_roundtrip" => {
                profile.requires_reasoning_signature_roundtrip = fact;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_facts_keep_provider_request_parallelism_orthogonal() {
        let anthropic =
            ProviderCapabilityProfile::resolve(ProviderProtocol::Anthropic, "claude-test");
        assert_eq!(
            anthropic.supports_multiple_tool_calls.state,
            CapabilityState::Supported
        );
        assert_eq!(
            anthropic.supports_parallel_tool_calls_request.state,
            CapabilityState::Unsupported
        );

        let responses = ProviderCapabilityProfile::resolve(ProviderProtocol::Responses, "gpt-test");
        assert_eq!(
            responses.supports_parallel_tool_calls_request.state,
            CapabilityState::Supported
        );
    }

    #[test]
    fn explicit_tool_choice_is_a_capability_fact_not_a_model_prefix_guess() {
        let unknown = ProviderCapabilityProfile::unknown();
        assert_eq!(
            unknown.supports_explicit_tool_choice.state,
            CapabilityState::Unknown
        );

        let completions =
            ProviderCapabilityProfile::resolve(ProviderProtocol::Completions, "deepseek-v4-flash");
        assert_eq!(
            completions.supports_explicit_tool_choice,
            CapabilityFact::bundled(CapabilityState::Supported)
        );

        let mut configured = completions;
        apply_configured_tags(&mut configured, &["no_explicit_tool_choice".to_string()]);
        assert_eq!(
            configured.supports_explicit_tool_choice,
            CapabilityFact::configured(CapabilityState::Unsupported)
        );

        let mut legacy = serde_json::to_value(ProviderCapabilityProfile::unknown())
            .expect("capability profile JSON");
        legacy
            .as_object_mut()
            .expect("capability profile object")
            .remove("supports_explicit_tool_choice");
        let decoded: ProviderCapabilityProfile =
            serde_json::from_value(legacy).expect("legacy capability profile");
        assert_eq!(
            decoded.supports_explicit_tool_choice.state,
            CapabilityState::Unknown
        );
    }
}
