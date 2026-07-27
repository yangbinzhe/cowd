/// Gateway read-only projection facade for Runtime lifecycle events.
///
/// The durable event store remains a Runtime implementation detail.  This
/// facade deliberately has no generic append method: Gateway services must use
/// a typed Runtime command for every lifecycle mutation they own.
#[derive(Clone)]
pub(crate) struct RuntimeEventService {
    reader: runtime::RuntimeEventReader,
    #[cfg(test)]
    fixture: runtime::RuntimeFixtureEventPort,
}

impl RuntimeEventService {
    pub(crate) fn from_runtime_services(services: &runtime::RuntimeServices) -> Self {
        Self {
            reader: services.event_reader(),
            #[cfg(test)]
            fixture: services.fixture_event_port(),
        }
    }

    pub(crate) fn list_stream(
        &self,
        stream_id: &str,
    ) -> Result<Vec<runtime::DurableRuntimeEvent>, String> {
        self.reader.list_stream(stream_id)
    }

    pub(crate) fn events_after_cursor(
        &self,
        cursor: u64,
        max_commits: usize,
    ) -> Result<Vec<runtime::CommittedEventBatch>, String> {
        self.reader.events_after_cursor(cursor, max_commits)
    }

    pub(crate) fn subscribe_commits(&self) -> tokio::sync::watch::Receiver<u64> {
        self.reader.subscribe_commits()
    }

    pub(crate) fn list_scope(
        &self,
        scope: runtime::RuntimeEventScope,
        limit: usize,
    ) -> Result<Vec<runtime::DurableRuntimeEvent>, String> {
        self.reader.list_scope(scope, limit)
    }

    pub(crate) fn all_events(
        &self,
        limit: usize,
    ) -> Result<Vec<runtime::DurableRuntimeEvent>, String> {
        self.reader.all_events(limit)
    }

    pub(crate) fn replay_report(
        &self,
        limit: usize,
    ) -> Result<runtime::RuntimeReplayReport, String> {
        self.reader.replay_report(limit)
    }

    pub(crate) fn session_timeline_events(
        &self,
        session_id: &str,
        after_position: Option<(u64, u32)>,
        limit: usize,
    ) -> Result<Vec<runtime::DurableRuntimeEvent>, String> {
        self.reader
            .session_timeline_events(session_id, after_position, limit)
    }

    #[cfg(test)]
    pub(crate) fn append_fixture(
        &self,
        event: runtime::RuntimeEventInput,
    ) -> Result<runtime::DurableRuntimeEvent, String> {
        self.fixture.append_for_test(event)
    }
}
