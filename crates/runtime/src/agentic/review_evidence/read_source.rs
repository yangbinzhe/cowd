//! The original read authenticator has one storage boundary, including Root history.
//! Event discovery remains with its caller; this port makes no budget/cut promises.
pub(super) trait ReadAuthSource {
    fn stream_revision(&mut self, stream: &str) -> Result<u64, String>;
    fn event_by_idempotency_key(
        &mut self,
        stream: &str,
        key: &str,
    ) -> Result<Option<crate::DurableRuntimeEvent>, String>;
    fn list_stream_after(
        &mut self,
        stream: &str,
        after: u64,
        through: u64,
        limit: usize,
        bytes: usize,
    ) -> Result<Vec<crate::DurableRuntimeEvent>, String>;
    fn transaction_stream_revision_exact(
        &mut self,
        transaction: &str,
        stream: &str,
    ) -> Result<Option<crate::CommittedStreamRevision>, String>;
    fn resolve_graph_record(
        &mut self,
        record: &crate::DurableRuntimeEvent,
    ) -> Result<serde_json::Value, String>;
}

pub(super) struct StoreReadSource<'a>(pub(super) &'a crate::RuntimeEventStore);
impl ReadAuthSource for StoreReadSource<'_> {
    fn stream_revision(&mut self, stream: &str) -> Result<u64, String> {
        self.0
            .stream_revision(stream)
            .map_err(|error| error.to_string())
    }
    fn event_by_idempotency_key(
        &mut self,
        stream: &str,
        key: &str,
    ) -> Result<Option<crate::DurableRuntimeEvent>, String> {
        self.0
            .event_by_idempotency_key(stream, key)
            .map_err(|error| error.to_string())
    }
    fn list_stream_after(
        &mut self,
        stream: &str,
        after: u64,
        through: u64,
        limit: usize,
        bytes: usize,
    ) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.0
            .list_stream_after(stream, after, through, limit, bytes)
            .map_err(|error| error.to_string())
    }
    fn transaction_stream_revision_exact(
        &mut self,
        transaction: &str,
        stream: &str,
    ) -> Result<Option<crate::CommittedStreamRevision>, String> {
        self.0
            .transaction_stream_revision_exact(transaction, stream)
            .map_err(|error| error.to_string())
    }
    fn resolve_graph_record(
        &mut self,
        record: &crate::DurableRuntimeEvent,
    ) -> Result<serde_json::Value, String> {
        self.0
            .resolve_graph_record(record)
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
pub(super) struct KeyOverrideSource<'a, F> {
    pub(super) source: StoreReadSource<'a>,
    pub(super) key: F,
}
#[cfg(test)]
impl<F> ReadAuthSource for KeyOverrideSource<'_, F>
where
    F: FnMut(&str, &str) -> Result<Option<crate::DurableRuntimeEvent>, String>,
{
    fn stream_revision(&mut self, stream: &str) -> Result<u64, String> {
        self.source.stream_revision(stream)
    }
    fn event_by_idempotency_key(
        &mut self,
        stream: &str,
        key: &str,
    ) -> Result<Option<crate::DurableRuntimeEvent>, String> {
        (self.key)(stream, key)
    }
    fn list_stream_after(
        &mut self,
        stream: &str,
        after: u64,
        through: u64,
        limit: usize,
        bytes: usize,
    ) -> Result<Vec<crate::DurableRuntimeEvent>, String> {
        self.source
            .list_stream_after(stream, after, through, limit, bytes)
    }
    fn transaction_stream_revision_exact(
        &mut self,
        transaction: &str,
        stream: &str,
    ) -> Result<Option<crate::CommittedStreamRevision>, String> {
        self.source
            .transaction_stream_revision_exact(transaction, stream)
    }
    fn resolve_graph_record(
        &mut self,
        record: &crate::DurableRuntimeEvent,
    ) -> Result<serde_json::Value, String> {
        self.source.resolve_graph_record(record)
    }
}
