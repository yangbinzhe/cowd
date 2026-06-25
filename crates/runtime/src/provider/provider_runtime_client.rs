use std::collections::BTreeMap;
use std::io::Write;
use std::pin::Pin;

use provider::{
    max_tokens_for_model, ApiError, ContentBlockDelta, InputContentBlock, InputMessage,
    MessageRequest, MessageResponse, OutputContentBlock, ProviderClient,
    StreamEvent as ApiStreamEvent, ToolChoice, ToolDefinition, ToolResultContentBlock,
};

use crate::{
    resolve_global_provider, ApiClient, ApiRequest, AssistantEvent, ConfigLoader, ContentBlock,
    ConversationMessage, MessageRole, PromptCacheEvent, RuntimeError,
};

pub use provider::OutputContentBlock as ProviderOutputContentBlock;
pub use provider::ToolDefinition as ProviderToolDefinition;

struct ProviderEntry {
    model: String,
    client: ProviderClient,
}

pub struct ProviderRuntimeClient {
    runtime: tokio::runtime::Runtime,
    chain: Vec<ProviderEntry>,
    tool_definitions: Vec<ToolDefinition>,
    reasoning_effort: Option<String>,
    emit_output: bool,
    stream_callback: Option<std::sync::mpsc::SyncSender<crate::CowdEvent>>,
}

impl ProviderRuntimeClient {
    pub fn new(model: String, tool_definitions: Vec<ToolDefinition>) -> Result<Self, String> {
        let fallback_config = load_provider_fallback_config();
        Self::new_with_fallback_config(model, tool_definitions, &fallback_config)
    }

    pub fn new_with_fallback_config(
        model: String,
        tool_definitions: Vec<ToolDefinition>,
        fallbacks: &[String],
    ) -> Result<Self, String> {
        let primary = build_provider_entry(&model)?;
        let mut chain = vec![primary];
        for fallback_model in fallbacks {
            match build_provider_entry(fallback_model) {
                Ok(entry) => chain.push(entry),
                Err(error) => {
                    tracing::warn!(
                        "skipping unavailable fallback provider {fallback_model}: {error}"
                    );
                }
            }
        }
        chain.dedup_by(|a, b| a.model == b.model);
        Ok(Self {
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?,
            chain,
            tool_definitions,
            reasoning_effort: None,
            emit_output: false,
            stream_callback: None,
        })
    }

    #[must_use]
    pub fn chain_models(&self) -> Vec<&str> {
        self.chain
            .iter()
            .map(|entry| entry.model.as_str())
            .collect()
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

    pub fn switch_model(&mut self, new_model: &str) -> Result<(), String> {
        self.chain = vec![build_provider_entry(new_model)?];
        self.model_fallbacks_extend();
        Ok(())
    }

    fn model_fallbacks_extend(&mut self) {
        for fallback_model in load_provider_fallback_config() {
            match build_provider_entry(&fallback_model) {
                Ok(entry) => self.chain.push(entry),
                Err(error) => {
                    tracing::warn!(
                        "skipping unavailable fallback provider {fallback_model}: {error}"
                    );
                }
            }
        }
        self.chain.dedup_by(|a, b| a.model == b.model);
    }
}

fn build_provider_entry(model: &str) -> Result<ProviderEntry, String> {
    let resolved = model.trim().to_string();
    let client = match resolve_global_provider(&resolved) {
        Some(provider) => ProviderClient::from_config(&provider).map_err(|e| e.to_string())?,
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

impl ApiClient for ProviderRuntimeClient {
    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<
        Box<dyn futures::stream::Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>,
    > {
        match self.stream_collect_inner(request) {
            Ok(events) => Box::pin(futures::stream::iter(events.into_iter().map(Ok))),
            Err(error) => Box::pin(futures::stream::iter(std::iter::once(Err(error)))),
        }
    }
}

impl ProviderRuntimeClient {
    fn stream_collect_inner(
        &mut self,
        request: ApiRequest,
    ) -> Result<Vec<AssistantEvent>, RuntimeError> {
        let messages = convert_messages(&request.messages);
        let system =
            (!request.system_prompt.is_empty()).then(|| request.system_prompt.join("\n\n"));
        let tool_choice = (!self.tool_definitions.is_empty()).then_some(ToolChoice::Auto);

        let runtime = &self.runtime;
        let chain = &self.chain;
        let mut last_error: Option<ApiError> = None;
        for (index, entry) in chain.iter().enumerate() {
            let message_request = MessageRequest {
                model: entry.model.clone(),
                max_tokens: max_tokens_for_model(&entry.model),
                messages: messages.clone(),
                system: system.clone(),
                tools: (!self.tool_definitions.is_empty()).then(|| self.tool_definitions.clone()),
                tool_choice: tool_choice.clone(),
                stream: true,
                reasoning_effort: self.reasoning_effort.clone(),
                ..Default::default()
            };

            let attempt = runtime.block_on(stream_with_provider(
                &entry.client,
                &message_request,
                self.emit_output,
                self.stream_callback.clone(),
            ));
            match attempt {
                Ok(events) => return Ok(events),
                Err(error) if error.is_retryable() && index + 1 < chain.len() => {
                    tracing::warn!(
                        "provider {} failed with retryable error, falling back: {error}",
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
