use std::collections::BTreeMap;
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use harness_contract::tool::ToolExposureProjection;
use provider::{
    max_tokens_for_model, ApiError, ContentBlockDelta, ImageSource, InputContentBlock,
    InputMessage, MessageRequest, MessageResponse, OutputContentBlock, ProviderClient,
    StreamEvent as ApiStreamEvent, ToolChoice, ToolDefinition, ToolResultContentBlock,
};

use crate::{
    ApiClient, ApiRequest, AssistantEvent, ConfigLoader, ContentBlock, ConversationMessage,
    MessageRole, PromptCacheEvent, ProviderContextInventory, RuntimeError,
};

use crate::provider_registry::{ProviderRegistry, ProviderRegistrySnapshot};

pub use provider::OutputContentBlock as ProviderOutputContentBlock;
pub use provider::ToolDefinition as ProviderToolDefinition;

#[derive(Clone)]
struct ProviderEntry {
    model: String,
    client: ProviderClient,
}

pub struct ProviderRuntimeClient {
    registry: Arc<ProviderRegistry>,
    chain_models: Vec<String>,
    tool_definitions: Vec<ToolDefinition>,
    tool_exposure: Option<ToolExposureProjection>,
    reasoning_effort: Option<String>,
    emit_output: bool,
    stream_callback: Option<std::sync::mpsc::SyncSender<crate::CowdEvent>>,
}

impl ProviderRuntimeClient {
    pub fn new(
        registry: Arc<ProviderRegistry>,
        model: String,
        tool_definitions: Vec<ToolDefinition>,
    ) -> Result<Self, String> {
        let fallback_config = load_provider_fallback_config();
        Self::new_with_fallback_config(registry, model, tool_definitions, &fallback_config)
    }

    pub fn new_with_fallback_config(
        registry: Arc<ProviderRegistry>,
        model: String,
        tool_definitions: Vec<ToolDefinition>,
        fallbacks: &[String],
    ) -> Result<Self, String> {
        let snapshot = registry.pin();
        build_provider_entry(&snapshot, &model)?;
        let mut chain_models = vec![model];
        if let Some(vision_model) = load_vision_model_config() {
            match build_provider_entry(&snapshot, &vision_model) {
                Ok(_) => chain_models.push(vision_model),
                Err(error) => {
                    tracing::warn!("skipping unavailable vision provider {vision_model}: {error}");
                }
            }
        }
        for fallback_model in fallbacks {
            match build_provider_entry(&snapshot, fallback_model) {
                Ok(_) => chain_models.push(fallback_model.clone()),
                Err(error) => {
                    tracing::warn!(
                        "skipping unavailable fallback provider {fallback_model}: {error}"
                    );
                }
            }
        }
        chain_models.dedup();
        Ok(Self {
            registry,
            chain_models,
            tool_definitions,
            tool_exposure: None,
            reasoning_effort: None,
            emit_output: false,
            stream_callback: None,
        })
    }

    #[must_use]
    pub fn chain_models(&self) -> Vec<&str> {
        self.chain_models.iter().map(String::as_str).collect()
    }

    #[must_use]
    pub fn provider_registry(&self) -> &Arc<ProviderRegistry> {
        &self.registry
    }

    #[must_use]
    pub fn with_emit_output(mut self, emit_output: bool) -> Self {
        self.emit_output = emit_output;
        self
    }

    #[must_use]
    pub fn with_stream_callback(
        mut self,
        stream_callback: Option<std::sync::mpsc::SyncSender<crate::CowdEvent>>,
    ) -> Self {
        self.stream_callback = stream_callback;
        self
    }

    pub fn set_reasoning_effort(&mut self, effort: Option<String>) {
        self.reasoning_effort = effort;
    }

    /// Install the explicit tool schema set selected by Runtime.
    ///
    /// An unconfigured client exposes no tools. Older projections are ignored,
    /// which prevents a delayed planner update from rolling schema visibility
    /// back after a newer activation revision has reached the client.
    pub fn configure_tool_exposure(&mut self, projection: ToolExposureProjection) {
        let is_stale = self.tool_exposure.as_ref().is_some_and(|current| {
            projection.catalog_revision < current.catalog_revision
                || (projection.catalog_revision == current.catalog_revision
                    && projection.exposure_revision < current.exposure_revision)
        });
        if is_stale {
            tracing::warn!(
                catalog_revision = projection.catalog_revision,
                exposure_revision = projection.exposure_revision,
                "ignoring stale provider tool exposure projection"
            );
            return;
        }
        self.tool_exposure = Some(projection);
    }

    pub fn switch_model(&mut self, new_model: &str) -> Result<(), String> {
        let snapshot = self.registry.pin();
        build_provider_entry(&snapshot, new_model)?;
        self.chain_models = vec![new_model.to_string()];
        self.model_fallbacks_extend();
        Ok(())
    }

    fn model_fallbacks_extend(&mut self) {
        let snapshot = self.registry.pin();
        if let Some(vision_model) = load_vision_model_config() {
            match build_provider_entry(&snapshot, &vision_model) {
                Ok(_) => self.chain_models.push(vision_model),
                Err(error) => {
                    tracing::warn!("skipping unavailable vision provider {vision_model}: {error}");
                }
            }
        }
        for fallback_model in load_provider_fallback_config() {
            match build_provider_entry(&snapshot, &fallback_model) {
                Ok(_) => self.chain_models.push(fallback_model),
                Err(error) => {
                    tracing::warn!(
                        "skipping unavailable fallback provider {fallback_model}: {error}"
                    );
                }
            }
        }
        self.chain_models.dedup();
    }

    fn active_tool_definitions(&self) -> Vec<ToolDefinition> {
        tool_definitions_for_exposure(&self.tool_definitions, self.tool_exposure.as_ref())
    }
}

fn tool_definitions_for_exposure(
    definitions: &[ToolDefinition],
    exposure: Option<&ToolExposureProjection>,
) -> Vec<ToolDefinition> {
    let Some(exposure) = exposure else {
        return Vec::new();
    };
    let active_ids = exposure
        .bootstrap_ids
        .iter()
        .chain(exposure.active_ids.iter())
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    definitions
        .iter()
        .filter(|definition| active_ids.contains(definition.name.as_str()))
        .cloned()
        .collect()
}

fn build_provider_entry(
    snapshot: &ProviderRegistrySnapshot,
    model: &str,
) -> Result<ProviderEntry, String> {
    let resolved = model.trim().to_string();
    let client = match snapshot.resolve(&resolved) {
        Some(provider) => ProviderClient::from_config(provider).map_err(|e| e.to_string())?,
        None => {
            tracing::warn!(
                "model '{resolved}' not in providers config, falling back to environment variables"
            );
            ProviderClient::from_model(&resolved).map_err(|e| e.to_string())?
        }
    };
    Ok(ProviderEntry {
        model: resolved,
        client,
    })
}

fn load_provider_fallback_config() -> Vec<String> {
    std::env::current_dir()
        .ok()
        .and_then(|cwd| ConfigLoader::default_for(cwd).load().ok())
        .map_or_else(Vec::new, |config| config.fallbacks().to_vec())
}

fn load_vision_model_config() -> Option<String> {
    std::env::current_dir()
        .ok()
        .and_then(|cwd| ConfigLoader::default_for(cwd).load().ok())
        .and_then(|config| config.aliases().get("vision").cloned())
        .filter(|model| !model.trim().is_empty())
}

impl ApiClient for ProviderRuntimeClient {
    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<
        Box<dyn futures::stream::Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>,
    > {
        let provider_snapshot = self.registry.pin();
        self.stream_with_provider_snapshot(request, provider_snapshot)
    }

    fn configure_tool_exposure(&mut self, projection: ToolExposureProjection) {
        ProviderRuntimeClient::configure_tool_exposure(self, projection);
    }

    fn context_inventory(&self) -> ProviderContextInventory {
        let tools = self.active_tool_definitions();
        let tool_schema_tokens = serde_json::to_string(&tools)
            .map(|json| crate::context_ledger::estimate_text_tokens(&json))
            .unwrap_or(0);
        ProviderContextInventory {
            tool_count: tools.len(),
            tool_schema_tokens,
        }
    }
}

impl ProviderRuntimeClient {
    pub(crate) fn stream_with_provider_snapshot(
        &mut self,
        request: ApiRequest,
        provider_snapshot: ProviderRegistrySnapshot,
    ) -> Pin<
        Box<dyn futures::stream::Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>,
    > {
        let events = async move {
            match self.stream_collect_inner(request, provider_snapshot).await {
                Ok(events) => events.into_iter().map(Ok).collect::<Vec<_>>(),
                Err(error) => vec![Err(error)],
            }
        };
        Box::pin(futures::stream::once(events).flat_map(futures::stream::iter))
    }

    async fn stream_collect_inner(
        &mut self,
        request: ApiRequest,
        provider_snapshot: ProviderRegistrySnapshot,
    ) -> Result<Vec<AssistantEvent>, RuntimeError> {
        let messages = convert_messages(&request.messages);
        let system =
            (!request.system_prompt.is_empty()).then(|| request.system_prompt.join("\n\n"));
        let active_tools = self.active_tool_definitions();
        let tool_choice = (!active_tools.is_empty()).then_some(ToolChoice::Auto);

        let needs_vision = request_has_image_input(&messages);
        // One provider snapshot is pinned for the whole request, including all
        // retries and fallbacks. A concurrent reload only affects later requests.
        let chain = self.candidate_chain(&provider_snapshot, &request.model, needs_vision);
        let mut last_error: Option<ApiError> = None;
        for (index, entry) in chain.iter().enumerate() {
            let message_request = MessageRequest {
                model: entry.model.clone(),
                max_tokens: max_tokens_for_model(&entry.model),
                messages: messages.clone(),
                system: system.clone(),
                tools: (!active_tools.is_empty()).then(|| active_tools.clone()),
                tool_choice: tool_choice.clone(),
                stream: true,
                reasoning_effort: self.reasoning_effort.clone(),
                ..Default::default()
            };

            let attempt = stream_with_provider(
                &entry.client,
                &message_request,
                self.emit_output,
                self.stream_callback.clone(),
            )
            .await;
            match attempt {
                Ok(events) => return Ok(events),
                Err(error) if error.is_retryable() && index + 1 < chain.len() => {
                    tracing::warn!(
                        "provider {} failed with retryable error, falling back: {error}",
                        entry.model
                    );
                    last_error = Some(error);
                }
                Err(error)
                    if needs_vision
                        && index + 1 < chain.len()
                        && looks_like_vision_unsupported(&error) =>
                {
                    tracing::warn!(
                        "provider {} rejected vision input, falling back: {error}",
                        entry.model
                    );
                    last_error = Some(error);
                }
                Err(error) => return Err(RuntimeError::new(error.to_string())),
            }
        }

        Err(RuntimeError::new(last_error.map_or_else(
            || String::from("provider chain exhausted with no attempts"),
            |error| error.to_string(),
        )))
    }

    fn candidate_chain(
        &self,
        snapshot: &ProviderRegistrySnapshot,
        requested_model: &str,
        needs_vision: bool,
    ) -> Vec<ProviderEntry> {
        let base = self
            .chain_models
            .iter()
            .filter(|model| !needs_vision || !model_is_known_without_vision(model))
            .filter_map(|model| match build_provider_entry(snapshot, model) {
                Ok(entry) => Some(entry),
                Err(error) => {
                    tracing::warn!("skipping unavailable provider model {model}: {error}");
                    None
                }
            })
            .collect::<Vec<_>>();
        let base = if base.is_empty() {
            self.chain_models
                .iter()
                .filter_map(|model| build_provider_entry(snapshot, model).ok())
                .collect()
        } else {
            base
        };

        let requested_model = requested_model.trim();
        let mut ordered = Vec::with_capacity(base.len() + usize::from(!requested_model.is_empty()));
        if !requested_model.is_empty() {
            if let Some(existing) = base.iter().find(|entry| entry.model == requested_model) {
                ordered.push(existing.clone());
            } else if !needs_vision || !model_is_known_without_vision(requested_model) {
                match build_provider_entry(snapshot, requested_model) {
                    Ok(entry) => ordered.push(entry),
                    Err(error) => tracing::warn!(
                        "skipping requested provider model {requested_model}: {error}"
                    ),
                }
            }
        }

        for entry in base {
            if !ordered.iter().any(|known| known.model == entry.model) {
                ordered.push(entry);
            }
        }
        ordered
    }
}

fn request_has_image_input(messages: &[InputMessage]) -> bool {
    messages.iter().any(|message| {
        message
            .content
            .iter()
            .any(|block| matches!(block, InputContentBlock::Image { .. }))
    })
}

fn model_is_known_without_vision(model: &str) -> bool {
    let canonical = model.split_once('/').map_or(model, |(_, rest)| rest).trim();
    model_protocol::model_registry::global_registry()
        .get(canonical)
        .is_some_and(|info| {
            !info
                .capabilities
                .iter()
                .any(|capability| capability.eq_ignore_ascii_case("vision"))
        })
}

fn looks_like_vision_unsupported(error: &ApiError) -> bool {
    let text = error.to_string().to_ascii_lowercase();
    (text.contains("image") || text.contains("vision") || text.contains("multimodal"))
        && (text.contains("unsupported")
            || text.contains("not support")
            || text.contains("does not support")
            || text.contains("invalid")
            || text.contains("extra inputs"))
}

#[allow(clippy::too_many_lines)]
async fn stream_with_provider(
    client: &ProviderClient,
    message_request: &MessageRequest,
    emit_output: bool,
    stream_callback: Option<std::sync::mpsc::SyncSender<crate::CowdEvent>>,
) -> Result<Vec<AssistantEvent>, ApiError> {
    let mut stream = client.stream_message(message_request).await?;
    let mut events = Vec::new();
    let mut pending_tools: BTreeMap<u32, (String, String, String)> = BTreeMap::new();
    let mut saw_stop = false;

    while let Some(event) = stream.next_event().await? {
        match event {
            ApiStreamEvent::MessageStart(start) => {
                for block in start.message.content {
                    push_provider_output_block(block, 0, &mut events, &mut pending_tools, true);
                }
            }
            ApiStreamEvent::ContentBlockStart(start) => {
                push_provider_output_block(
                    start.content_block,
                    start.index,
                    &mut events,
                    &mut pending_tools,
                    true,
                );
            }
            ApiStreamEvent::ContentBlockDelta(delta) => match delta.delta {
                ContentBlockDelta::TextDelta { text } => {
                    if !text.is_empty() {
                        if emit_output {
                            print!("{text}");
                            let _ = std::io::stdout().flush();
                        }
                        if let Some(callback) = &stream_callback {
                            let _ = callback
                                .try_send(crate::CowdEvent::TextDelta { text: text.clone() });
                        }
                        events.push(AssistantEvent::TextDelta(text));
                    }
                }
                ContentBlockDelta::InputJsonDelta { partial_json } => {
                    if let Some((_, _, input)) = pending_tools.get_mut(&delta.index) {
                        input.push_str(&partial_json);
                    }
                }
                ContentBlockDelta::ThinkingDelta { .. }
                | ContentBlockDelta::SignatureDelta { .. } => {}
            },
            ApiStreamEvent::ContentBlockStop(stop) => {
                if let Some((id, name, input)) = pending_tools.remove(&stop.index) {
                    events.push(AssistantEvent::ToolUse { id, name, input });
                }
            }
            ApiStreamEvent::MessageDelta(delta) => {
                events.push(AssistantEvent::Usage(delta.usage.token_usage()));
            }
            ApiStreamEvent::MessageStop(_) => {
                saw_stop = true;
                events.push(AssistantEvent::MessageStop);
            }
        }
    }

    push_prompt_cache_record(client, &mut events);

    if !saw_stop
        && events.iter().any(|event| {
            matches!(event, AssistantEvent::TextDelta(text) if !text.is_empty())
                || matches!(event, AssistantEvent::ToolUse { .. })
        })
    {
        events.push(AssistantEvent::MessageStop);
    }

    if events
        .iter()
        .any(|event| matches!(event, AssistantEvent::MessageStop))
    {
        return Ok(events);
    }

    let response = client
        .send_message(&MessageRequest {
            stream: false,
            ..message_request.clone()
        })
        .await?;
    let mut events = response_to_events(response);
    push_prompt_cache_record(client, &mut events);
    Ok(events)
}

fn convert_messages(messages: &[ConversationMessage]) -> Vec<InputMessage> {
    messages
        .iter()
        .filter_map(|message| {
            let role = match message.role {
                MessageRole::System | MessageRole::User | MessageRole::Tool => "user",
                MessageRole::Assistant => "assistant",
            };
            let content = message
                .blocks
                .iter()
                .map(|block| match block {
                    ContentBlock::Text { text } => InputContentBlock::Text { text: text.clone() },
                    ContentBlock::Image {
                        media_type, data, ..
                    } => InputContentBlock::Image {
                        source: ImageSource::base64(media_type.clone(), data.clone()),
                    },
                    ContentBlock::Thinking {
                        thinking,
                        signature,
                    } => InputContentBlock::Thinking {
                        thinking: thinking.clone(),
                        signature: signature.clone(),
                    },
                    ContentBlock::ToolUse { id, name, input } => InputContentBlock::ToolUse {
                        id: id.clone(),
                        name: name.clone(),
                        input: serde_json::from_str(input)
                            .unwrap_or_else(|_| serde_json::json!({ "raw": input })),
                    },
                    ContentBlock::ToolResult {
                        tool_use_id,
                        output,
                        is_error,
                        ..
                    } => InputContentBlock::ToolResult {
                        tool_use_id: tool_use_id.clone(),
                        content: vec![ToolResultContentBlock::Text {
                            text: output.clone(),
                        }],
                        is_error: *is_error,
                    },
                })
                .collect::<Vec<_>>();
            (!content.is_empty()).then(|| InputMessage {
                role: role.to_string(),
                content,
            })
        })
        .collect()
}

pub fn push_provider_output_block(
    block: OutputContentBlock,
    block_index: u32,
    events: &mut Vec<AssistantEvent>,
    pending_tools: &mut BTreeMap<u32, (String, String, String)>,
    streaming_tool_input: bool,
) {
    match block {
        OutputContentBlock::Text { text } => {
            if !text.is_empty() {
                events.push(AssistantEvent::TextDelta(text));
            }
        }
        OutputContentBlock::ToolUse { id, name, input } => {
            let initial_input = if streaming_tool_input
                && input.is_object()
                && input.as_object().is_some_and(serde_json::Map::is_empty)
            {
                String::new()
            } else {
                input.to_string()
            };
            pending_tools.insert(block_index, (id, name, initial_input));
        }
        OutputContentBlock::Thinking { .. } | OutputContentBlock::RedactedThinking { .. } => {}
    }
}

fn response_to_events(response: MessageResponse) -> Vec<AssistantEvent> {
    let mut events = Vec::new();
    let mut pending_tools = BTreeMap::new();

    for (index, block) in response.content.into_iter().enumerate() {
        let index = u32::try_from(index).expect("response block index overflow");
        push_provider_output_block(block, index, &mut events, &mut pending_tools, false);
        if let Some((id, name, input)) = pending_tools.remove(&index) {
            events.push(AssistantEvent::ToolUse { id, name, input });
        }
    }

    events.push(AssistantEvent::Usage(response.usage.token_usage()));
    events.push(AssistantEvent::MessageStop);
    events
}

fn push_prompt_cache_record(client: &ProviderClient, events: &mut Vec<AssistantEvent>) {
    if let Some(record) = client.take_last_prompt_cache_record() {
        if let Some(event) = prompt_cache_record_to_runtime_event(record) {
            events.push(AssistantEvent::PromptCache(event));
        }
    }
}

fn prompt_cache_record_to_runtime_event(
    record: provider::PromptCacheRecord,
) -> Option<PromptCacheEvent> {
    let cache_break = record.cache_break?;
    Some(PromptCacheEvent {
        unexpected: cache_break.unexpected,
        reason: cache_break.reason,
        previous_cache_read_input_tokens: cache_break.previous_cache_read_input_tokens,
        current_cache_read_input_tokens: cache_break.current_cache_read_input_tokens,
        token_drop: cache_break.token_drop,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        looks_like_vision_unsupported, request_has_image_input, tool_definitions_for_exposure,
    };
    use harness_contract::tool::ToolExposureProjection;
    use provider::{ApiError, ImageSource, InputContentBlock, InputMessage, ToolDefinition};
    use serde_json::json;

    fn tool(name: &str) -> ToolDefinition {
        ToolDefinition {
            name: name.to_string(),
            description: None,
            input_schema: json!({"type": "object"}),
        }
    }

    fn exposure(bootstrap: &[&str], active: &[&str], revision: u64) -> ToolExposureProjection {
        ToolExposureProjection {
            catalog_revision: 7,
            exposure_revision: revision,
            bootstrap_ids: bootstrap.iter().map(|id| (*id).to_string()).collect(),
            active_ids: active.iter().map(|id| (*id).to_string()).collect(),
            deferred_ids: Vec::new(),
            fallback_full: false,
            reason: "test".to_string(),
            schema_tokens: 0,
        }
    }

    #[test]
    fn unconfigured_client_exposes_no_tool_schema() {
        let tools = vec![tool("tool_search"), tool("read_file")];
        assert!(tool_definitions_for_exposure(&tools, None).is_empty());
    }

    #[test]
    fn explicit_projection_selects_bootstrap_and_active_ids_only() {
        let tools = vec![tool("tool_search"), tool("read_file"), tool("write_file")];
        let projection = exposure(&["tool_search"], &["read_file"], 3);
        let visible = tool_definitions_for_exposure(&tools, Some(&projection))
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();

        assert_eq!(visible, vec!["tool_search", "read_file"]);
    }

    #[test]
    fn fallback_flag_does_not_bypass_explicit_ids() {
        let tools = vec![tool("tool_search"), tool("dangerous_tool")];
        let mut projection = exposure(&["tool_search"], &[], 1);
        projection.fallback_full = true;
        let visible = tool_definitions_for_exposure(&tools, Some(&projection));

        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].name, "tool_search");
    }

    #[test]
    fn request_has_image_input_detects_structured_image_blocks() {
        let messages = vec![InputMessage {
            role: "user".to_string(),
            content: vec![
                InputContentBlock::Text {
                    text: "describe".to_string(),
                },
                InputContentBlock::Image {
                    source: ImageSource::base64("image/png", "aW1hZ2U="),
                },
            ],
        }];

        assert!(request_has_image_input(&messages));
    }

    #[test]
    fn vision_unsupported_errors_can_continue_fallback_chain() {
        let error = ApiError::Api {
            status: reqwest::StatusCode::BAD_REQUEST,
            error_type: Some("invalid_request".to_string()),
            message: Some("This model does not support image input".to_string()),
            request_id: None,
            body: "image input unsupported".to_string(),
            retryable: false,
            suggested_action: None,
        };

        assert!(looks_like_vision_unsupported(&error));
    }
}
