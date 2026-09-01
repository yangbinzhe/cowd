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
    /// Whether Chat Completions assistant tool-call history must carry the
    /// provider's opaque `reasoning_content` continuation field, including an
    /// explicitly empty value. This is distinct from Anthropic-style signed
    /// thinking blocks.
    #[serde(default = "unknown_capability_fact")]
    pub requires_reasoning_content_roundtrip: CapabilityFact,
}

/// Provider-owned prefix-cache persistence semantics used by Runtime's
/// request coordinator. This is deliberately separate from model reasoning
/// and tool capabilities: it changes waiting, never prompt authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderPromptCacheBehavior {
    pub persistence_barrier_ms: u64,
    pub requires_common_prefix_discovery: bool,
}

const fn unknown_capability_fact() -> CapabilityFact {
    CapabilityFact::bundled(CapabilityState::Unknown)
}

impl ProviderCapabilityProfile {
    #[must_use]
    pub fn prompt_cache_behavior(model: &str) -> ProviderPromptCacheBehavior {
        if Self::is_deepseek_v4(model) {
            // DeepSeek's documented disk cache is built asynchronously and
            // needs a second divergent request before their common prefix is
            // persisted as an independently matchable cache unit.
            ProviderPromptCacheBehavior {
                persistence_barrier_ms: 2_000,
                requires_common_prefix_discovery: true,
            }
        } else {
            ProviderPromptCacheBehavior {
                persistence_barrier_ms: 0,
                requires_common_prefix_discovery: false,
            }
        }
    }
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
            requires_reasoning_content_roundtrip: unknown,
        }
    }

    #[must_use]
    pub fn resolve(protocol: ProviderProtocol, model: &str) -> Self {
        Self::resolve_for_reasoning_mode(protocol, model, None)
    }

    /// Resolve provider capability facts after the exact model and reasoning
    /// mode are both known. DeepSeek v4 thinking mode is a documented
    /// endpoint behavior, not a model-name prefix guess: the same model in a
    /// non-thinking mode keeps the default explicit `tool_choice` support,
    /// and any other model with thinking enabled is not downgraded.
    #[must_use]
    pub fn resolve_for_reasoning_mode(
        protocol: ProviderProtocol,
        model: &str,
        reasoning_effort: Option<&str>,
    ) -> Self {
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
        let reasoning_content_roundtrip =
            if protocol == ProviderProtocol::Completions && Self::is_deepseek_v4(model) {
                CapabilityState::Supported
            } else {
                CapabilityState::Unknown
            };
        let mut profile = Self {
            supports_tool_calls: CapabilityFact::bundled(CapabilityState::Supported),
            supports_explicit_tool_choice: CapabilityFact::bundled(CapabilityState::Supported),
            supports_multiple_tool_calls: CapabilityFact::bundled(CapabilityState::Supported),
            supports_parallel_tool_calls_request: CapabilityFact::bundled(parallel_request),
            streams_tool_arguments: CapabilityFact::bundled(CapabilityState::Supported),
            supports_public_reasoning_summary: CapabilityFact::bundled(public_reasoning),
            requires_reasoning_signature_roundtrip: CapabilityFact::bundled(signature_roundtrip),
            requires_reasoning_content_roundtrip: CapabilityFact::bundled(
                reasoning_content_roundtrip,
            ),
        };
        if Self::explicit_tool_choice_known_unsupported(model, reasoning_effort) {
            profile.supports_explicit_tool_choice =
                CapabilityFact::bundled(CapabilityState::Unsupported);
        }

        if let Some(info) = global_registry().capacity_model_info(model) {
            apply_configured_tags(&mut profile, &info.capabilities);
        }
        profile
    }

    /// The single wire-boundary truth for "this exact model must not receive
    /// an explicit `tool_choice` field". DeepSeek v4 and Qwen 3.7 Plus
    /// thinking endpoints reject `tool_choice` with HTTP 400 even when no
    /// `reasoning_effort` is sent. Omit the field unconditionally for these
    /// exact model families unless a configured capability tag explicitly
    /// overrides it. Both the Runtime capability gate and the provider
    /// payload builders call this helper so they cannot drift.
    #[must_use]
    pub fn explicit_tool_choice_known_unsupported(
        model: &str,
        _reasoning_effort: Option<&str>,
    ) -> bool {
        Self::is_deepseek_v4(model) || Self::is_qwen_37_plus(model)
    }

    /// The canonical wire fact for OpenAI-compatible assistant continuation
    /// messages. Configured and environment-derived provider routes must call
    /// this same resolver so constructor choice cannot change protocol
    /// correctness.
    #[must_use]
    pub fn reasoning_content_roundtrip_required(model: &str) -> bool {
        Self::resolve(ProviderProtocol::Completions, model)
            .requires_reasoning_content_roundtrip
            .state
            == CapabilityState::Supported
    }

    /// Exact DeepSeek v4 family check. It matches the canonical model id only;
    /// unknown models are never downgraded by prefix heuristics.
    fn is_deepseek_v4(model: &str) -> bool {
        let lowered = model.trim().to_ascii_lowercase();
        let canonical = lowered.rsplit('/').next().unwrap_or_default();
        matches!(canonical, "deepseek-v4-pro" | "deepseek-v4-flash")
    }

    /// Exact Qwen 3.7 Plus check. It matches the canonical model id only so
    /// unrelated Qwen variants retain their configured/default capability.
    fn is_qwen_37_plus(model: &str) -> bool {
        let lowered = model.trim().to_ascii_lowercase();
        let canonical = lowered.rsplit('/').next().unwrap_or_default();
        matches!(canonical, "qwen3.7-plus" | "qwen-3.7-plus")
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
            "reasoning_content_roundtrip" => {
                profile.requires_reasoning_content_roundtrip = fact;
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
    fn deepseek_disk_cache_behavior_is_centralized_and_other_models_do_not_wait() {
        let deepseek = ProviderCapabilityProfile::prompt_cache_behavior("deepseek-v4-flash");
        assert_eq!(deepseek.persistence_barrier_ms, 2_000);
        assert!(deepseek.requires_common_prefix_discovery);

        let other = ProviderCapabilityProfile::prompt_cache_behavior("some-openai-model");
        assert_eq!(other.persistence_barrier_ms, 0);
        assert!(!other.requires_common_prefix_discovery);
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
            CapabilityFact::bundled(CapabilityState::Unsupported)
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

    #[test]
    fn deepseek_v4_thinking_disables_explicit_tool_choice_without_prefix_guessing() {
        let thinking = ProviderCapabilityProfile::resolve_for_reasoning_mode(
            ProviderProtocol::Completions,
            "deepseek-v4-flash",
            Some("high"),
        );
        assert_eq!(
            thinking.supports_explicit_tool_choice,
            CapabilityFact::bundled(CapabilityState::Unsupported)
        );
        assert_eq!(
            thinking.supports_tool_calls.state,
            CapabilityState::Supported,
            "tools stay advertised; only the explicit tool_choice field is omitted"
        );

        let pro_thinking = ProviderCapabilityProfile::resolve_for_reasoning_mode(
            ProviderProtocol::Completions,
            "deepseek-v4-pro",
            Some("max"),
        );
        assert_eq!(
            pro_thinking.supports_explicit_tool_choice.state,
            CapabilityState::Unsupported
        );
    }

    #[test]
    fn deepseek_v4_default_and_non_thinking_also_omit_explicit_tool_choice() {
        for reasoning in [None, Some("none")] {
            let profile = ProviderCapabilityProfile::resolve_for_reasoning_mode(
                ProviderProtocol::Completions,
                "deepseek-v4-flash",
                reasoning,
            );
            assert_eq!(
                profile.supports_explicit_tool_choice.state,
                CapabilityState::Unsupported,
                "DeepSeek v4 defaults to thinking mode and rejects tool_choice with 400"
            );
        }
    }

    #[test]
    fn deepseek_v4_completions_requires_reasoning_content_roundtrip() {
        let deepseek =
            ProviderCapabilityProfile::resolve(ProviderProtocol::Completions, "deepseek-v4-flash");
        assert_eq!(
            deepseek.requires_reasoning_content_roundtrip,
            CapabilityFact::bundled(CapabilityState::Supported)
        );
        assert!(
            ProviderCapabilityProfile::reasoning_content_roundtrip_required(
                "provider/deepseek-v4-pro"
            )
        );

        let generic =
            ProviderCapabilityProfile::resolve(ProviderProtocol::Completions, "generic-chat");
        assert_eq!(
            generic.requires_reasoning_content_roundtrip.state,
            CapabilityState::Unknown
        );
        assert!(!ProviderCapabilityProfile::reasoning_content_roundtrip_required("qwen3.8-max"));
    }

    #[test]
    fn qwen_37_plus_omits_explicit_tool_choice_without_downgrading_other_qwen_models() {
        let qwen = ProviderCapabilityProfile::resolve_for_reasoning_mode(
            ProviderProtocol::Completions,
            "qwen3.7-plus",
            Some("high"),
        );
        assert_eq!(
            qwen.supports_explicit_tool_choice.state,
            CapabilityState::Unsupported
        );
        assert!(
            ProviderCapabilityProfile::explicit_tool_choice_known_unsupported(
                "provider/qwen-3.7-plus",
                None
            )
        );
        assert!(
            !ProviderCapabilityProfile::explicit_tool_choice_known_unsupported(
                "qwen3.7-turbo",
                Some("high")
            )
        );
    }

    #[test]
    fn other_models_are_not_downgraded_by_thinking_mode_or_prefix() {
        let other_thinking = ProviderCapabilityProfile::resolve_for_reasoning_mode(
            ProviderProtocol::Completions,
            "deepseek-v3",
            Some("high"),
        );
        assert_eq!(
            other_thinking.supports_explicit_tool_choice.state,
            CapabilityState::Supported,
            "only the exact deepseek-v4 family is downgraded"
        );
        assert!(
            !ProviderCapabilityProfile::explicit_tool_choice_known_unsupported(
                "deepseek-v3",
                Some("high")
            ),
            "similar deepseek families must not be downgraded by prefix guessing"
        );
        assert!(
            ProviderCapabilityProfile::explicit_tool_choice_known_unsupported(
                "deepseek-v4-pro",
                Some("high")
            )
        );
    }

    #[test]
    fn configured_profile_can_explicitly_override_thinking_unsupported() {
        let mut thinking = ProviderCapabilityProfile::resolve_for_reasoning_mode(
            ProviderProtocol::Completions,
            "deepseek-v4-flash",
            Some("max"),
        );
        apply_configured_tags(&mut thinking, &["explicit_tool_choice".to_string()]);
        assert_eq!(
            thinking.supports_explicit_tool_choice,
            CapabilityFact::configured(CapabilityState::Supported)
        );
    }
}
