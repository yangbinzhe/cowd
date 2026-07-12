use std::{path::Path, sync::Arc};

use runtime::{DurableRuntimeEvent, RuntimeEventInput, RuntimeEventScope, RuntimeEventStore};

/// Gateway-scoped access to the durable runtime lifecycle truth source.
#[derive(Clone)]
pub(crate) struct RuntimeEventService {
    store: Arc<RuntimeEventStore>,
}

impl RuntimeEventService {
    pub(crate) fn from_store(store: Arc<RuntimeEventStore>) -> Self {
        Self { store }
    }

    #[allow(
        clippy::panic,
        reason = "this constructor is used by legacy infallible static service assembly; startup must not proceed without the durable event store"
    )]
    pub(crate) fn open(config_home: &Path) -> Self {
        let path = config_home.join("storage/runtime-events.sqlite");
        let store = RuntimeEventStore::open(&path).unwrap_or_else(|error| {
            panic!(
                "failed to open Gateway runtime event store at {}: {error}",
                path.display()
            )
        });
        Self {
            store: Arc::new(store),
        }
    }

    #[cfg(test)]
    pub(crate) fn in_memory() -> Self {
        Self {
            store: Arc::new(
                RuntimeEventStore::open_in_memory()
                    .expect("in-memory runtime event store should open"),
            ),
        }
    }

    pub(crate) fn store(&self) -> &Arc<RuntimeEventStore> {
        &self.store
    }

    pub(crate) fn append(
        &self,
        stream_id: impl Into<String>,
        scope: RuntimeEventScope,
        kind: impl Into<String>,
        payload: serde_json::Value,
    ) -> Result<DurableRuntimeEvent, String> {
        self.store.append(RuntimeEventInput {
            stream_id: stream_id.into(),
            scope,
            kind: kind.into(),
            status: None,
            actor: Some("gateway".to_string()),
            refs: Vec::new(),
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_service_owns_runtime_lifecycle_events() {
        let service = RuntimeEventService::in_memory();
        service
            .append(
                "task-1",
                RuntimeEventScope::Task,
                "task.started",
                serde_json::json!({"task_id": "task-1"}),
            )
            .unwrap();

        let events = service.store().list_stream("task-1").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].scope, RuntimeEventScope::Task);
        assert_eq!(events[0].kind, "task.started");
    }
}
