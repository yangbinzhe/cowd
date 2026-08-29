//! Query, pagination, snapshot, and retention operations for the PostgresSessionStore adapter.

use super::*;

impl PostgresSessionStore {
    /// Export every normalized PG table in canonical SQL order. This is a
    /// cutover-only API; normal request handling stays on the selected owner.
    pub fn export_migration_snapshot(&self) -> session::SessionResult<SessionMigrationSnapshot> {
        let mut connection = self.executor.checkout_background().map_err(storage_error)?;
        let sessions = connection
            .query("SELECT session_id,platform,chat_id,user_id,model,created_at,last_activity,message_count,reset_policy,metadata_json,input_tokens,output_tokens,status FROM session_records ORDER BY session_id", &[])
            .map_err(postgres_error)?
            .iter()
            .map(row_to_session)
            .collect::<session::SessionResult<_>>()
            .map_err(|error| migration_export_error("session_records", error))?;
        let input_admissions = connection
            .query(
                "SELECT session_id,input_generation,input_admission_open
                   FROM session_records ORDER BY session_id",
                &[],
            )
            .map_err(postgres_error)?
            .iter()
            .map(|row| {
                Ok(SessionInputAdmission {
                    session_id: row.try_get(0).map_err(postgres_error)?,
                    generation: i64_to_u64(
                        row.try_get(1).map_err(postgres_error)?,
                        "session input generation",
                    )?,
                    open: row.try_get(2).map_err(postgres_error)?,
                })
            })
            .collect::<session::SessionResult<Vec<_>>>()?;
        let lifecycle_intents = connection
            .query(
                "SELECT operation_id,session_id,disposition,phase,last_stable_phase,
                        expected_generation,created_at_ms,updated_at_ms,last_error,revision
                   FROM session_lifecycle_intents ORDER BY operation_id",
                &[],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_lifecycle_intent)
            .collect::<session::SessionResult<Vec<_>>>()?;
        let branch_activations = connection
            .query(
                "SELECT operation_id,source_session_id,target_session_id,source_message_count,
                        phase,created_at_ms,updated_at_ms,last_error,revision
                   FROM session_branch_activations ORDER BY operation_id",
                &[],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_branch_activation)
            .collect::<session::SessionResult<Vec<_>>>()?;
        let associations = connection.query("SELECT session_id,memory_id,created_at FROM session_memory_associations ORDER BY session_id,memory_id",&[]).map_err(postgres_error)?.iter().map(|row| Ok(SessionMemoryAssociation { session_id: row.try_get(0).map_err(postgres_error)?, memory_id: row.try_get(1).map_err(postgres_error)?, created_at: row.try_get(2).map_err(postgres_error)?})).collect::<session::SessionResult<_>>()?;
        let messages = connection.query("SELECT stable_message_id,session_id,sequence,role,content_json,blocks_count,tool_use_id,tool_name,token_usage_json,created_at_ms FROM session_messages ORDER BY session_id,sequence",&[]).map_err(postgres_error)?.iter().map(row_to_message).collect::<session::SessionResult<_>>()?;
        let events = connection.query("SELECT session_id,event_type,event_json,sequence,created_at_ms FROM session_events ORDER BY session_id,sequence",&[]).map_err(postgres_error)?.iter().map(row_to_event).collect::<session::SessionResult<_>>()?;
        let checkpoints = connection.query("SELECT session_id,checkpoint_id FROM session_event_checkpoints ORDER BY session_id,checkpoint_id",&[]).map_err(postgres_error)?.iter().map(|row| Ok(SessionEventCheckpoint {session_id: row.try_get(0).map_err(postgres_error)?,checkpoint_id: row.try_get(1).map_err(postgres_error)?})).collect::<session::SessionResult<_>>()?;
        let snapshots = connection.query("SELECT session_id,event_idx,messages_json,created_at_ms FROM session_snapshots ORDER BY session_id,event_idx",&[]).map_err(postgres_error)?.iter().map(row_to_snapshot).collect::<session::SessionResult<_>>()?;
        let runtime_outbox = connection
            .query("SELECT input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,decision,target_turn_id,classification_json,task_route_hint_json,status,runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json FROM session_runtime_outbox ORDER BY request_id",&[])
            .map_err(postgres_error)?
            .iter()
            .map(row_to_runtime_outbox)
            .collect::<session::SessionResult<_>>()
            .map_err(|error| migration_export_error("session_runtime_outbox", error))?;
        let runtime_history = pg_history_rows(&mut connection, "session_runtime_outbox_history")?;
        Ok(SessionMigrationSnapshot {
            schema_version: 6,
            sessions,
            input_admissions,
            lifecycle_intents,
            branch_activations,
            associations,
            messages,
            events,
            checkpoints,
            snapshots,
            runtime_outbox,
            runtime_history,
        })
    }

    /// Import only into an empty target or one already holding the identical
    /// snapshot. A conflicting nonempty target is refused; no dual write is
    /// introduced as a fallback.
    pub fn import_migration_snapshot(
        &self,
        snapshot: &SessionMigrationSnapshot,
    ) -> session::SessionResult<()> {
        if snapshot.schema_version != 6 {
            return Err(session::SessionError::Store(format!(
                "unsupported session migration schema {}",
                snapshot.schema_version
            )));
        }
        let existing = self.export_migration_snapshot()?;
        if !snapshot_is_empty(&existing) {
            if existing.canonical_digest()? == snapshot.canonical_digest()? {
                return Ok(());
            }
            return Err(session::SessionError::Store(
                "refusing divergent non-empty PostgreSQL session target".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_background().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        for session in &snapshot.sessions {
            upsert_session_tx(&mut transaction, session)?;
        }
        for admission in &snapshot.input_admissions {
            let changed = transaction
                .execute(
                    "UPDATE session_records
                        SET input_generation=$1,input_admission_open=$2
                      WHERE session_id=$3",
                    &[
                        &to_u64_i64(admission.generation, "session input generation")?,
                        &admission.open,
                        &admission.session_id,
                    ],
                )
                .map_err(postgres_error)?;
            if changed != 1 {
                return Err(session::SessionError::Store(format!(
                    "session admission `{}` has no imported owner",
                    admission.session_id
                )));
            }
        }
        for intent in &snapshot.lifecycle_intents {
            import_lifecycle_intent_tx(&mut transaction, intent)?;
        }
        for activation in &snapshot.branch_activations {
            import_branch_activation_tx(&mut transaction, activation)?;
        }
        for association in &snapshot.associations {
            transaction.execute("INSERT INTO session_memory_associations(session_id,memory_id,created_at) VALUES($1,$2,$3)", &[&association.session_id,&association.memory_id,&association.created_at]).map_err(postgres_error)?;
        }
        for message in &snapshot.messages {
            insert_message_tx(&mut transaction, message)?;
        }
        for event in &snapshot.events {
            transaction.execute("INSERT INTO session_events(session_id,sequence,event_type,event_json,created_at_ms) VALUES($1,$2,$3,$4,$5)", &[&event.session_id,&to_i64(event.sequence,"event sequence")?,&event.event_type,&event.event_json,&to_u64_i64(event.created_at_ms,"event time")?]).map_err(postgres_error)?;
        }
        for checkpoint in &snapshot.checkpoints {
            transaction
                .execute(
                    "INSERT INTO session_event_checkpoints(session_id,checkpoint_id) VALUES($1,$2)",
                    &[&checkpoint.session_id, &checkpoint.checkpoint_id],
                )
                .map_err(postgres_error)?;
        }
        for item in &snapshot.snapshots {
            transaction.execute("INSERT INTO session_snapshots(session_id,event_idx,messages_json,created_at_ms) VALUES($1,$2,$3,$4)", &[&item.session_id,&to_i64(item.event_idx,"snapshot index")?,&item.messages_json,&to_u64_i64(item.created_at_ms,"snapshot time")?]).map_err(postgres_error)?;
        }
        for item in &snapshot.runtime_outbox {
            import_runtime_outbox_tx(&mut transaction, item)?;
        }
        for item in &snapshot.runtime_history {
            import_history_tx(&mut transaction, "session_runtime_outbox_history", item)?;
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(())
    }

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
