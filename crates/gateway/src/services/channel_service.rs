use std::{collections::HashSet, sync::Arc};

use channel_adapters::platform::{OutboundDispatch, PlatformRuntime, SendResult, SessionKey};

use super::{service_envelope, ServiceEnvelope};

#[derive(Clone)]
pub(crate) struct ChannelService {
    label: &'static str,
    owner: &'static str,
    runtime: Option<Arc<PlatformRuntime>>,
}

impl ChannelService {
    pub(crate) fn new() -> Self {
        Self {
            label: "channel",
            owner: "0.9.348 Channel service boundary",
            runtime: None,
        }
    }

    pub(crate) fn with_runtime(runtime: Arc<PlatformRuntime>) -> Self {
        Self {
            runtime: Some(runtime),
            ..Self::new()
        }
    }

    pub(crate) fn label(&self) -> &'static str {
        self.label
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }

    pub(crate) fn is_runtime_available(&self) -> bool {
        self.runtime.is_some()
    }

    pub(crate) async fn has_bound_adapter(&self, name: &str) -> bool {
        let Some(runtime) = &self.runtime else {
            return false;
        };
        runtime.has_bound_adapter(name).await
    }

    pub(crate) async fn list_bound_adapters(&self) -> Vec<String> {
        let Some(runtime) = &self.runtime else {
            return Vec::new();
        };
        runtime.list_bound_adapters().await
    }

    pub(crate) async fn bound_adapter_set(&self) -> HashSet<String> {
        self.list_bound_adapters().await.into_iter().collect()
    }

    pub(crate) async fn dispatch_payload(
        &self,
        platform: &str,
        dispatch: OutboundDispatch,
    ) -> Result<SendResult, String> {
        let Some(runtime) = &self.runtime else {
            return Err("channel runtime unavailable".to_string());
        };
        runtime
            .dispatch_payload(platform, dispatch)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) fn session_key(value: &str) -> SessionKey {
        SessionKey::from(value)
    }
}
