use std::collections::BTreeMap;
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use harness_contract::tool::ToolExposureProjection;
use provider::{
    ApiError, ContentBlockDelta, ImageSource, InputContentBlock, InputMessage, MessageRequest,
    MessageResponse, OutputContentBlock, ProviderClient, StreamEvent as ApiStreamEvent, ToolChoice,
    ToolDefinition, ToolResultContentBlock,
};
use serde::{Deserialize, Serialize};

use crate::{
    ApiClient, ApiRequest, AssistantEvent, ContentBlock, ConversationMessage, MessageRole,
    ProviderContextInventory, RuntimeError,
};

use crate::provider_registry::{ProviderRegistry, ProviderRegistrySnapshot};

const PROVIDER_EVENT_QUEUE_CAPACITY: usize = 64;
const MAX_COALESCED_TEXT_BYTES: usize = 64 * 1024;

pub use provider::OutputContentBlock as ProviderOutputContentBlock;
pub use provider::ToolDefinition as ProviderToolDefinition;

fn request_reasoning_effort(
    model: &str,
    request_override: Option<String>,
    configured: Option<String>,
) -> Option<String> {
    let lowered = model.to_ascii_lowercase();
    let canonical = lowered.rsplit('/').next().unwrap_or(lowered.as_str());
    let qwen_hybrid = canonical.starts_with("qwen3.7-")
        || canonical.starts_with("qwen3.6-")
        || canonical.starts_with("qwen3.5-");
    let deepseek_v4 = matches!(canonical, "deepseek-v4-pro" | "deepseek-v4-flash");
    if deepseek_v4 {
        return request_override
            .filter(|effort| matches!(effort.as_str(), "high" | "max"))
            .or_else(|| {
                configured.map(|effort| match effort.as_str() {
                    "low" | "medium" => "high".to_string(),
                    _ => effort,
                })
            });
    }
    request_override
        .filter(|effort| effort == "none" && qwen_hybrid)
        .or(configured)
}

#[derive(Clone)]
struct ProviderEntry {
    model: String,
    client: ProviderClient,
    request_context: ProviderRequestContext,
}

/// Immutable provider configuration selected for one request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedProviderProfile {
    pub registry_revision: u64,
    pub provider_name: String,
    pub model: String,
    pub base_url: Option<String>,
    pub protocol: Option<String>,
}

/// Request-local provider state. It is created after pinning a registry
/// snapshot and is never stored in a shared transport/client slot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderRequestContext {
    pub request_id: String,
    pub profile: ResolvedProviderProfile,
    pub transport_fingerprint: crate::TransportProfileFingerprint,
    pub attempt: u32,
}

#[derive(Clone)]
pub struct ProviderRuntimeClient {
    registry: Arc<ProviderRegistry>,
    transport_pool: Arc<crate::ProviderTransportPool>,
    tool_definitions: Vec<ToolDefinition>,
    tool_exposure: Option<ToolExposureProjection>,
    reasoning_effort: Option<String>,
    emit_output: bool,
    stream_callback: Option<std::sync::mpsc::SyncSender<crate::CowdEvent>>,
}

/// Bridges one provider request into the runtime's lazy `ApiClient` stream.
///
/// The provider SDK is asynchronous but does not itself implement
/// `futures::Stream`. Keeping the producer in a cancellable task lets us
/// expose each upstream event immediately while still aborting the request
/// when the consumer applies a transport timeout or the turn is cancelled.
struct ProviderEventStream {
    receiver: tokio::sync::mpsc::Receiver<Result<AssistantEvent, RuntimeError>>,
    producer: Option<tokio::task::JoinHandle<()>>,
}

impl futures::Stream for ProviderEventStream {
    type Item = Result<AssistantEvent, RuntimeError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Drop for ProviderEventStream {
    fn drop(&mut self) {
        // The consumer owns the request lifetime. In particular, a runtime
        // transport timeout must not leave a detached provider request running
        // after its graph node has been failed or replanned.
        if let Some(producer) = &self.producer {
            producer.abort();
        }
    }
}

impl ProviderRuntimeClient {
    pub fn new(
        registry: Arc<ProviderRegistry>,
        model: String,
        tool_definitions: Vec<ToolDefinition>,
    ) -> Result<Self, String> {
        Self::new_with_transport_pool(
            registry,
            Arc::new(crate::ProviderTransportPool::default()),
            model,
            tool_definitions,
        )
    }

    pub fn new_with_transport_pool(
        registry: Arc<ProviderRegistry>,
        transport_pool: Arc<crate::ProviderTransportPool>,
        model: String,
        tool_definitions: Vec<ToolDefinition>,
    ) -> Result<Self, String> {
        let snapshot = registry.pin();
        build_provider_entry(&snapshot, &transport_pool, &model)?;
        Ok(Self {
            registry,
            transport_pool,
            tool_definitions,
            tool_exposure: None,
            reasoning_effort: None,
            emit_output: false,
            stream_callback: None,
        })
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
    transport_pool: &crate::ProviderTransportPool,
    model: &str,
) -> Result<ProviderEntry, String> {
    let resolved = model.trim().to_string();
    let (transport_fingerprint, http) = transport_pool
        .checkout_default()
        .map_err(|error| error.to_string())?;
    let (client, provider_name, base_url, protocol) = match snapshot.resolve(&resolved) {
        Some(provider) => {
            let protocol = crate::config::ProviderProtocol::effective_for_provider(provider)
                .map_err(|error| error.to_string())?;
            (
                ProviderClient::from_config_with_effective_protocol_and_http(
                    provider, protocol, http,
                )
                .map_err(|e| e.to_string())?,
                provider.name.clone(),
                Some(provider.base_url.clone()),
                Some(protocol.as_str().to_string()),
            )
        }
        None => {
            tracing::warn!(
                "model '{resolved}' not in providers config, falling back to environment variables"
            );
            (
                ProviderClient::from_model_with_http(&resolved, http).map_err(|e| e.to_string())?,
                "environment".to_string(),
                None,
                None,
            )
        }
    };
    Ok(ProviderEntry {
        model: resolved.clone(),
        client,
        request_context: ProviderRequestContext {
            request_id: uuid::Uuid::new_v4().to_string(),
            profile: ResolvedProviderProfile {
                registry_revision: snapshot.revision(),
                provider_name,
                model: resolved,
                base_url,
                protocol,
            },
            transport_fingerprint,
            attempt: 1,
        },
    })
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
        let mut messages = request
            .prompt
            .contextual_messages()
            .into_iter()
            .map(InputMessage::user_text)
            .collect::<Vec<_>>();
        messages.extend(convert_messages(&request.messages));
        let system = request.prompt.trusted_system_text();
        let active_tools = self.active_tool_definitions();
        let tool_choice = (!active_tools.is_empty()).then_some(ToolChoice::Auto);

        // Runtime selects one candidate and owns the route/retry lifecycle.
        // This adapter owns exactly one pinned wire-protocol attempt.
        let entry =
            match build_provider_entry(&provider_snapshot, &self.transport_pool, &request.model) {
                Ok(entry) => entry,
                Err(error) => {
                    tracing::warn!(
                        model = %request.model,
                        registry_revision = provider_snapshot.revision(),
                        configured_providers = ?provider_snapshot.provider_names(),
                        error = %error,
                        "provider request could not resolve a configured runtime client"
                    );
                    return Box::pin(futures::stream::once(async move {
                        Err(RuntimeError::new(format!(
                            "provider candidate `{}` is unavailable: {error}",
                            request.model
                        )))
                    }));
                }
            };
        let (sender, receiver) = tokio::sync::mpsc::channel(PROVIDER_EVENT_QUEUE_CAPACITY);
        let reasoning_effort = request_reasoning_effort(
            &entry.model,
            request.reasoning_effort_override.clone(),
            self.reasoning_effort.clone(),
        );
        let producer = match tokio::runtime::Handle::try_current() {
            Ok(handle) => Some(
                handle.spawn(forward_provider_attempt(
                    entry,
                    messages,
                    system,
                    active_tools,
                    tool_choice,
                    request
                        .budget
                        .requested_output_tokens
                        .min(u64::from(u32::MAX)) as u32,
                    request
                        .budget
                        .context_window_tokens
                        .min(u64::from(u32::MAX)) as u32,
                    reasoning_effort,
                    self.emit_output,
                    self.stream_callback.clone(),
                    sender,
                )),
            ),
            Err(_) => {
                // `ApiClient::stream` is consumed from async Runtime code, but
                // callers may still construct it in synchronous diagnostics.
                // Return a normal stream error instead of panicking while
                // attempting to spawn a Tokio task without a reactor.
                tracing::warn!(
                    model = %entry.model,
                    "provider stream was created outside an active Tokio runtime"
                );
                let _ = sender.try_send(Err(RuntimeError::new(
                    "provider stream requires an active Tokio runtime",
                )));
                None
            }
        };
        Box::pin(ProviderEventStream { receiver, producer })
    }
}

#[allow(clippy::too_many_arguments)]
async fn forward_provider_attempt(
    entry: ProviderEntry,
    messages: Vec<InputMessage>,
    system: Option<String>,
    active_tools: Vec<ToolDefinition>,
    tool_choice: Option<ToolChoice>,
    max_tokens: u32,
    context_window_limit: u32,
    reasoning_effort: Option<String>,
    emit_output: bool,
    stream_callback: Option<std::sync::mpsc::SyncSender<crate::CowdEvent>>,
    sender: tokio::sync::mpsc::Sender<Result<AssistantEvent, RuntimeError>>,
) {
    let request_context = &entry.request_context;
    tracing::debug!(
        provider_request_id = %request_context.request_id,
        provider = %request_context.profile.provider_name,
        model = %request_context.profile.model,
        registry_revision = request_context.profile.registry_revision,
        transport_fingerprint = request_context.transport_fingerprint.0,
        attempt = request_context.attempt,
        "starting request-local provider attempt"
    );
    let message_request = MessageRequest {
        model: entry.model.clone(),
        max_tokens,
        context_window_limit: Some(context_window_limit),
        messages,
        system,
        tools: (!active_tools.is_empty()).then_some(active_tools),
        tool_choice,
        stream: true,
        reasoning_effort,
        temperature: evaluation_request_temperature(),
        ..Default::default()
    };
    if let Err(error) = forward_provider_stream(
        &entry.client,
        &message_request,
        &entry.model,
        emit_output,
        stream_callback,
        &sender,
    )
    .await
    {
        tracing::warn!(
            model = %entry.model,
            error = %error,
            "provider stream attempt failed before terminal completion"
        );
        let provider_context_window_limit = error.error.context_window_limit_hint();
        let provider_tool_protocol_failure = error.error.is_compatibility_tool_protocol_failure();
        let _ = sender
            .send(Err(RuntimeError::with_provider_failure_metadata(
                error.error.to_string(),
                provider_context_window_limit,
                provider_tool_protocol_failure,
            )))
            .await;
    }
}

fn evaluation_request_temperature() -> Option<f64> {
    (std::env::var("COWD_EVAL_HARNESS").as_deref() == Ok("1")
        && std::env::var("COWD_EVAL_CORPUS_ID").as_deref() == Ok("auto-strategy-v1"))
    .then(|| {
        std::env::var("COWD_MODEL_TEMPERATURE")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
    })
    .flatten()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForwardedProviderStream {
    Completed,
    ConsumerDropped,
}

#[derive(Debug)]
struct ProviderStreamError {
    error: ApiError,
}

impl std::fmt::Display for ProviderStreamError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(formatter)
    }
}

#[allow(clippy::too_many_lines)]
async fn forward_provider_stream(
    client: &ProviderClient,
    message_request: &MessageRequest,
    effective_model: &str,
    emit_output: bool,
    stream_callback: Option<std::sync::mpsc::SyncSender<crate::CowdEvent>>,
    sender: &tokio::sync::mpsc::Sender<Result<AssistantEvent, RuntimeError>>,
) -> Result<ForwardedProviderStream, ProviderStreamError> {
    let mut stream = client
        .stream_message(message_request)
        .await
        .map_err(|error| ProviderStreamError { error })?;
    let mut pending_tools: BTreeMap<u32, (String, String, String)> = BTreeMap::new();
    let mut saw_stop = false;
    let mut emitted = false;
    let mut provider_model_emitted = false;
    let mut pending_text = String::new();

    while let Some(event) = stream
        .next_event()
        .await
        .map_err(|error| ProviderStreamError { error })?
    {
        if !provider_model_emitted {
            if !forward_event(
                sender,
                AssistantEvent::ProviderModel {
                    model: effective_model.to_string(),
                },
                emit_output,
                &stream_callback,
                &mut emitted,
            )
            .await
            {
                return Ok(ForwardedProviderStream::ConsumerDropped);
            }
            provider_model_emitted = true;
        }
        match event {
            ApiStreamEvent::MessageStart(start) => {
                if !flush_pending_text(
                    sender,
                    &mut pending_text,
                    emit_output,
                    &stream_callback,
                    &mut emitted,
                )
                .await
                {
                    return Ok(ForwardedProviderStream::ConsumerDropped);
                }
                let mut events = Vec::new();
                for block in start.message.content {
                    push_provider_output_block(block, 0, &mut events, &mut pending_tools, true);
                }
                if !forward_events(sender, events, emit_output, &stream_callback, &mut emitted)
                    .await
                {
                    return Ok(ForwardedProviderStream::ConsumerDropped);
                }
            }
            ApiStreamEvent::ContentBlockStart(start) => {
                if !flush_pending_text(
                    sender,
                    &mut pending_text,
                    emit_output,
                    &stream_callback,
                    &mut emitted,
                )
                .await
                {
                    return Ok(ForwardedProviderStream::ConsumerDropped);
                }
                let mut events = Vec::new();
                push_provider_output_block(
                    start.content_block,
                    start.index,
                    &mut events,
                    &mut pending_tools,
                    true,
                );
                if !forward_events(sender, events, emit_output, &stream_callback, &mut emitted)
                    .await
                {
                    return Ok(ForwardedProviderStream::ConsumerDropped);
                }
            }
            ApiStreamEvent::ContentBlockDelta(delta) => match delta.delta {
                ContentBlockDelta::TextDelta { text } => {
                    if !text.is_empty()
                        && !forward_text_delta(
                            sender,
                            text,
                            &mut pending_text,
                            emit_output,
                            &stream_callback,
                            &mut emitted,
                        )
                        .await
                    {
                        return Ok(ForwardedProviderStream::ConsumerDropped);
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
                if !flush_pending_text(
                    sender,
                    &mut pending_text,
                    emit_output,
                    &stream_callback,
                    &mut emitted,
                )
                .await
                {
                    return Ok(ForwardedProviderStream::ConsumerDropped);
                }
                if let Some((id, name, input)) = pending_tools.remove(&stop.index) {
                    if !forward_event(
                        sender,
                        AssistantEvent::ToolUse { id, name, input },
                        emit_output,
                        &stream_callback,
                        &mut emitted,
                    )
                    .await
                    {
                        return Ok(ForwardedProviderStream::ConsumerDropped);
                    }
                }
            }
            ApiStreamEvent::MessageDelta(delta) => {
                if !flush_pending_text(
                    sender,
                    &mut pending_text,
                    emit_output,
                    &stream_callback,
                    &mut emitted,
                )
                .await
                {
                    return Ok(ForwardedProviderStream::ConsumerDropped);
                }
                if !forward_event(
                    sender,
                    AssistantEvent::Usage(delta.usage.token_usage()),
                    emit_output,
                    &stream_callback,
                    &mut emitted,
                )
                .await
                {
                    return Ok(ForwardedProviderStream::ConsumerDropped);
                }
            }
            ApiStreamEvent::MessageStop(_) => {
                if !flush_pending_text(
                    sender,
                    &mut pending_text,
                    emit_output,
                    &stream_callback,
                    &mut emitted,
                )
                .await
                {
                    return Ok(ForwardedProviderStream::ConsumerDropped);
                }
                saw_stop = true;
                if !forward_event(
                    sender,
                    AssistantEvent::MessageStop,
                    emit_output,
                    &stream_callback,
                    &mut emitted,
                )
                .await
                {
                    return Ok(ForwardedProviderStream::ConsumerDropped);
                }
            }
        }
    }

    if !flush_pending_text(
        sender,
        &mut pending_text,
        emit_output,
        &stream_callback,
        &mut emitted,
    )
    .await
    {
        return Ok(ForwardedProviderStream::ConsumerDropped);
    }

    if saw_stop {
        return Ok(ForwardedProviderStream::Completed);
    }
    if emitted {
        return if forward_event(
            sender,
            AssistantEvent::MessageStop,
            emit_output,
            &stream_callback,
            &mut emitted,
        )
        .await
        {
            Ok(ForwardedProviderStream::Completed)
        } else {
            Ok(ForwardedProviderStream::ConsumerDropped)
        };
    }

    let response = client
        .send_message(&MessageRequest {
            stream: false,
            ..message_request.clone()
        })
        .await
        .map_err(|error| ProviderStreamError { error })?;
    let mut events = response_to_events(response);
    events.insert(
        0,
        AssistantEvent::ProviderModel {
            model: effective_model.to_string(),
        },
    );
    if forward_events(sender, events, emit_output, &stream_callback, &mut emitted).await {
        Ok(ForwardedProviderStream::Completed)
    } else {
        Ok(ForwardedProviderStream::ConsumerDropped)
    }
}

async fn forward_events(
    sender: &tokio::sync::mpsc::Sender<Result<AssistantEvent, RuntimeError>>,
    events: Vec<AssistantEvent>,
    emit_output: bool,
    stream_callback: &Option<std::sync::mpsc::SyncSender<crate::CowdEvent>>,
    emitted: &mut bool,
) -> bool {
    for event in events {
        if !forward_event(sender, event, emit_output, stream_callback, emitted).await {
            return false;
        }
    }
    true
}

async fn forward_event(
    sender: &tokio::sync::mpsc::Sender<Result<AssistantEvent, RuntimeError>>,
    event: AssistantEvent,
    emit_output: bool,
    stream_callback: &Option<std::sync::mpsc::SyncSender<crate::CowdEvent>>,
    emitted: &mut bool,
) -> bool {
    if let AssistantEvent::TextDelta(text) = &event {
        if emit_output {
            print!("{text}");
            let _ = std::io::stdout().flush();
        }
        if let Some(callback) = stream_callback {
            let _ = callback.try_send(crate::CowdEvent::TextDelta { text: text.clone() });
        }
    }
    *emitted = true;
    let producer_wait_started = Instant::now();
    let sent = sender.send(Ok(event)).await.is_ok();
    crate::execution_core::performance::observe_duration(
        "provider_producer_wait_ms",
        producer_wait_started.elapsed(),
    );
    sent
}

async fn forward_text_delta(
    sender: &tokio::sync::mpsc::Sender<Result<AssistantEvent, RuntimeError>>,
    text: String,
    pending_text: &mut String,
    emit_output: bool,
    stream_callback: &Option<std::sync::mpsc::SyncSender<crate::CowdEvent>>,
    emitted: &mut bool,
) -> bool {
    if pending_text.is_empty() && sender.capacity() > 0 {
        return forward_event(
            sender,
            AssistantEvent::TextDelta(text),
            emit_output,
            stream_callback,
            emitted,
        )
        .await;
    }
    pending_text.push_str(&text);
    if pending_text.len() < MAX_COALESCED_TEXT_BYTES && sender.capacity() == 0 {
        return true;
    }
    flush_pending_text(sender, pending_text, emit_output, stream_callback, emitted).await
}

async fn flush_pending_text(
    sender: &tokio::sync::mpsc::Sender<Result<AssistantEvent, RuntimeError>>,
    pending_text: &mut String,
    emit_output: bool,
    stream_callback: &Option<std::sync::mpsc::SyncSender<crate::CowdEvent>>,
    emitted: &mut bool,
) -> bool {
    if pending_text.is_empty() {
        return true;
    }
    let text = std::mem::take(pending_text);
    forward_event(
        sender,
        AssistantEvent::TextDelta(text),
        emit_output,
        stream_callback,
        emitted,
    )
    .await
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
        let Ok(index) = u32::try_from(index) else {
            break;
        };
        push_provider_output_block(block, index, &mut events, &mut pending_tools, false);
        if let Some((id, name, input)) = pending_tools.remove(&index) {
            events.push(AssistantEvent::ToolUse { id, name, input });
        }
    }

    events.push(AssistantEvent::Usage(response.usage.token_usage()));
    events.push(AssistantEvent::MessageStop);
    events
}

#[cfg(test)]
mod tests {
    use super::{
        build_provider_entry, forward_text_delta, request_reasoning_effort,
        tool_definitions_for_exposure,
    };
    use crate::config::{ProviderConfig, ProvidersConfig};
    use crate::{AssistantEvent, ProviderRegistry, ProviderRuntimeClient, ProviderTransportPool};
    use harness_contract::tool::ToolExposureProjection;
    use provider::ToolDefinition;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

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
    fn provider_client_accepts_one_runtime_selected_model() {
        let registry = Arc::new(
            ProviderRegistry::new(ProvidersConfig {
                providers: HashMap::from([(
                    "test".to_string(),
                    ProviderConfig {
                        name: "test".to_string(),
                        base_url: "https://example.test/v1".to_string(),
                        api_key: "test".to_string(),
                        models: vec!["primary".to_string(), "fallback".to_string()],
                        protocol: Some("completions".to_string()),
                    },
                )]),
            })
            .expect("registry"),
        );
        ProviderRuntimeClient::new(registry, "primary".to_string(), Vec::new())
            .expect("single selected model must be valid");
    }

    #[test]
    fn latest_deepseek_v4_reasoning_effort_is_normalized_without_none() {
        assert_eq!(
            request_reasoning_effort(
                "deepseek-v4-pro",
                Some("none".to_string()),
                Some("medium".to_string()),
            ),
            Some("high".to_string())
        );
        assert_eq!(
            request_reasoning_effort(
                "deepseek/deepseek-v4-flash",
                Some("max".to_string()),
                Some("high".to_string()),
            ),
            Some("max".to_string())
        );
    }

    #[test]
    fn request_context_is_fresh_while_transport_is_reused() {
        let registry = ProviderRegistry::new(ProvidersConfig {
            providers: HashMap::from([(
                "test".to_string(),
                ProviderConfig {
                    name: "test".to_string(),
                    base_url: "https://example.test/v1".to_string(),
                    api_key: "test".to_string(),
                    models: vec!["primary".to_string()],
                    protocol: Some("completions".to_string()),
                },
            )]),
        })
        .expect("registry");
        let snapshot = registry.pin();
        let pool = ProviderTransportPool::new(2);

        let first = build_provider_entry(&snapshot, &pool, "primary").expect("first");
        let second = build_provider_entry(&snapshot, &pool, "primary").expect("second");

        assert_ne!(
            first.request_context.request_id,
            second.request_context.request_id
        );
        assert_eq!(
            first.request_context.transport_fingerprint,
            second.request_context.transport_fingerprint
        );
        assert_eq!(pool.stats().builds, 1);
        assert_eq!(pool.stats().hits, 1);
    }

    #[tokio::test]
    async fn text_deltas_coalesce_while_bounded_queue_is_full() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender
            .send(Ok(AssistantEvent::ProviderModel {
                model: "test".to_string(),
            }))
            .await
            .unwrap();
        let mut pending = String::new();
        let mut emitted = false;

        assert!(
            forward_text_delta(
                &sender,
                "a".to_string(),
                &mut pending,
                false,
                &None,
                &mut emitted
            )
            .await
        );
        assert!(
            forward_text_delta(
                &sender,
                "b".to_string(),
                &mut pending,
                false,
                &None,
                &mut emitted
            )
            .await
        );
        assert_eq!(pending, "ab");
        let _ = receiver.recv().await;
        assert!(super::flush_pending_text(&sender, &mut pending, false, &None, &mut emitted).await);
        assert!(pending.is_empty());
        assert!(matches!(
            receiver.recv().await,
            Some(Ok(AssistantEvent::TextDelta(text))) if text == "ab"
        ));
    }
}
