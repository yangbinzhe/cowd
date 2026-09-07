//! Query, pagination, snapshot, and retention operations for the PostgresSessionStore adapter.

use super::*;

impl PostgresSessionStore {
    pub(super) fn query_sessions(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> session::SessionResult<Vec<SessionRecord>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(statement, params)
            .map_err(postgres_error)?
            .iter()
            .map(row_to_session)
            .collect()
    }

    pub(super) fn query_messages(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> session::SessionResult<Vec<SessionMessage>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(statement, params)
            .map_err(postgres_error)?
            .iter()
            .map(row_to_message)
            .collect()
    }

    pub(super) fn query_events(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> session::SessionResult<Vec<SessionEvent>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(statement, params)
            .map_err(postgres_error)?
            .iter()
            .map(row_to_event)
            .collect()
    }

    pub(super) fn query_runtime_outbox(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(statement, params)
            .map_err(postgres_error)?
            .iter()
            .map(row_to_runtime_outbox)
            .collect()
    }

    pub(super) fn count_events_sql(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> session::SessionResult<usize> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let count: i64 = connection
            .query_one(statement, params)
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        from_i64(count, "event count")
    }

    pub(super) fn delete_events_sql(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> session::SessionResult<usize> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let deleted = connection
            .execute(statement, params)
            .map_err(postgres_error)?;
        Ok(deleted as usize)
    }
}
