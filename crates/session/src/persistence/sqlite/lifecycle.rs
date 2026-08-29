//! Lifecycle operations for the SqliteSessionStore adapter.

use super::*;

impl SqliteSessionStore {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Open (or create) a session database at `path`.
    ///
    /// Creates any missing parent directories and initialises the schema if
    /// the database is new.
    pub fn open(path: &Path) -> Result<Self> {
        let handle = storage::StorageHandle::sqlite(
            "session",
            path.to_path_buf(),
            "memory",
            "session_store_path_adapter_since_0.9.315",
        );
        Self::open_storage_handle(&handle)
    }

    /// Open a session database through a typed storage handle.
    pub fn open_storage_handle(handle: &storage::StorageHandle) -> Result<Self> {
        if handle.backend != storage::StorageBackendKind::Sqlite {
            return Err(SessionError::Store(format!(
                "storage handle `{}` is not sqlite-backed",
                handle.domain
            )));
        }
        let path = &handle.path;
        let db_path = path
            .to_str()
            .ok_or_else(|| SessionError::Store("non-UTF-8 session db path".to_string()))?
            .to_owned();
        // Create parent directories if needed (skip for ":memory:").
        if db_path != IN_MEMORY_PATH {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    SessionError::Store(format!("cannot create session db dir: {e}"))
                })?;
            }
        }
        let pool = new_pool(&db_path, 10)?;
        let store = Self {
            pool,
            _pool_tracker: SqlitePoolGuard::register(),
        };
        let conn = store.conn()?;
        init_schema(&conn)?;
        Ok(store)
    }

    /// Open an in-memory session database (useful for testing).
    pub fn open_in_memory() -> Result<Self> {
        let pool = new_pool(IN_MEMORY_PATH, 1)?;
        let store = Self {
            pool,
            _pool_tracker: SqlitePoolGuard::register(),
        };
        let conn = store.conn()?;
        init_schema(&conn)?;
        Ok(store)
    }

    pub fn conn(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        let conn = self
            .pool
            .get()
            .map_err(|e| SessionError::Store(e.to_string()))?;
        set_conn_pragmas(&conn)?;
        Ok(conn)
    }

    // -----------------------------------------------------------------------
    // CRUD
    // -----------------------------------------------------------------------

    /// Insert a new session record.
    ///
    /// Uses `INSERT OR IGNORE` so calling this for an already-existing session
    /// is a harmless no-op.
    pub fn create_session(&self, session: &SessionRecord) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"INSERT OR IGNORE INTO sessions
               (session_id, platform, chat_id, user_id, model,
                created_at, last_activity, message_count, reset_policy, metadata_json,
                input_tokens, output_tokens, status,
                created_at_ms, updated_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                session.session_id,
                session.platform,
                session.chat_id,
                session.user_id,
                session.model,
                session.created_at,
                session.last_activity,
                session.message_count,
                session.reset_policy,
                session.metadata_json,
                session.input_tokens,
                session.output_tokens,
                session.status,
                iso_to_ms(&session.created_at),
                iso_to_ms(&session.last_activity),
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Retrieve a session record by its ID, or `None` if not found.
    pub fn get_session(&self, session_id: &str) -> Result<Option<SessionRecord>> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT session_id, platform, chat_id, user_id, model,
                      created_at, last_activity, message_count, reset_policy, metadata_json,
                      input_tokens, output_tokens, status
               FROM sessions WHERE session_id = ?1",
            params![session_id],
            row_to_record,
        )
        .optional()
        .map_err(sql_err)
    }

    /// Retrieve a bounded set of Session records in one database round trip.
    pub fn get_sessions_by_ids(&self, session_ids: &[String]) -> Result<Vec<SessionRecord>> {
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = vec!["?"; session_ids.len()].join(",");
        let sql = format!(
            r"SELECT session_id, platform, chat_id, user_id, model,
                      created_at, last_activity, message_count, reset_policy, metadata_json,
                      input_tokens, output_tokens, status
                 FROM sessions
                WHERE session_id IN ({placeholders})
                ORDER BY session_id ASC"
        );
        let conn = self.conn()?;
        let mut statement = conn.prepare(&sql).map_err(sql_err)?;
        let rows = statement
            .query_map(params_from_iter(session_ids.iter()), row_to_record)
            .map_err(sql_err)?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row.map_err(sql_err)?);
        }
        Ok(records)
    }

    /// Read the body-free recovery projection for one Session.
    pub fn get_session_recovery_manifest(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionRecoveryManifest>> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT session_id, durable_cursor, event_cursor, history_revision,
                     transcript_messages, transcript_bytes,
                     latest_checkpoint_sequence, latest_checkpoint_event_id,
                     index_generation, indexed_through_sequence, index_card_count,
                     index_pending,
                     in_flight_turn,
                     pending_approval, active_writer_or_attachment,
                     mission_agent_team_continuation, last_activity_ms,
                     manifest_revision
                FROM session_recovery_manifest
               WHERE session_id=?1",
            params![session_id],
            row_to_recovery_manifest,
        )
        .optional()
        .map_err(sql_err)
    }

    pub fn get_session_presence_projection(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionPresenceProjection>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT session_id,state,attachments_json,next_sequence,revision,updated_at_ms
               FROM session_presence_projection WHERE session_id=?1",
            params![session_id],
            |row| {
                let next_sequence = row.get::<_, i64>(3)?;
                let revision = row.get::<_, i64>(4)?;
                let updated_at_ms = row.get::<_, i64>(5)?;
                Ok(SessionPresenceProjection {
                    session_id: row.get(0)?,
                    state: row.get(1)?,
                    attachments_json: row.get(2)?,
                    next_sequence: usize::try_from(next_sequence)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, next_sequence))?,
                    revision: u64::try_from(revision)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(4, revision))?,
                    updated_at_ms: u64::try_from(updated_at_ms)
                        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(5, updated_at_ms))?,
                })
            },
        )
        .optional()
        .map_err(sql_err)
    }

    pub fn upsert_session_presence_projection(
        &self,
        projection: &SessionPresenceProjection,
    ) -> Result<()> {
        let next_sequence = i64::try_from(projection.next_sequence)
            .map_err(|_| SessionError::Store("presence next_sequence overflow".to_string()))?;
        let revision = i64::try_from(projection.revision)
            .map_err(|_| SessionError::Store("presence revision overflow".to_string()))?;
        let updated_at_ms = i64::try_from(projection.updated_at_ms)
            .map_err(|_| SessionError::Store("presence updated_at_ms overflow".to_string()))?;
        let conn = self.conn()?;
        conn.execute(
            r"INSERT INTO session_presence_projection(
                   session_id,state,attachments_json,next_sequence,revision,updated_at_ms
               ) VALUES (?1,?2,?3,?4,?5,?6)
               ON CONFLICT(session_id) DO UPDATE SET
                   state=excluded.state,
                   attachments_json=excluded.attachments_json,
                   next_sequence=excluded.next_sequence,
                   revision=excluded.revision,
                   updated_at_ms=excluded.updated_at_ms",
            params![
                projection.session_id,
                projection.state,
                projection.attachments_json,
                next_sequence,
                revision,
                updated_at_ms,
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    pub fn compare_and_upsert_session_presence_projection(
        &self,
        projection: &SessionPresenceProjection,
        expected_revision: Option<u64>,
    ) -> Result<bool> {
        let next_sequence = i64::try_from(projection.next_sequence)
            .map_err(|_| SessionError::Store("presence next_sequence overflow".to_string()))?;
        let revision = i64::try_from(projection.revision)
            .map_err(|_| SessionError::Store("presence revision overflow".to_string()))?;
        let updated_at_ms = i64::try_from(projection.updated_at_ms)
            .map_err(|_| SessionError::Store("presence updated_at_ms overflow".to_string()))?;
        let conn = self.conn()?;
        let changed = match expected_revision {
            Some(expected_revision) => {
                let expected_revision = i64::try_from(expected_revision).map_err(|_| {
                    SessionError::Store("presence expected revision overflow".to_string())
                })?;
                conn.execute(
                    r"UPDATE session_presence_projection
                         SET state=?2,
                             attachments_json=?3,
                             next_sequence=?4,
                             revision=?5,
                             updated_at_ms=?6
                       WHERE session_id=?1 AND revision=?7",
                    params![
                        projection.session_id,
                        projection.state,
                        projection.attachments_json,
                        next_sequence,
                        revision,
                        updated_at_ms,
                        expected_revision,
                    ],
                )
            }
            None => conn.execute(
                r"INSERT INTO session_presence_projection(
                       session_id,state,attachments_json,next_sequence,revision,updated_at_ms
                   ) VALUES (?1,?2,?3,?4,?5,?6)
                   ON CONFLICT(session_id) DO NOTHING",
                params![
                    projection.session_id,
                    projection.state,
                    projection.attachments_json,
                    next_sequence,
                    revision,
                    updated_at_ms,
                ],
            ),
        }
        .map_err(sql_err)?;
        Ok(changed == 1)
    }

    pub fn delete_session_presence_projection(&self, session_id: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "DELETE FROM session_presence_projection WHERE session_id=?1",
            params![session_id],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    pub fn get_session_recovery_manifests_by_ids(
        &self,
        session_ids: &[String],
    ) -> Result<Vec<SessionRecoveryManifest>> {
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = vec!["?"; session_ids.len()].join(",");
        let sql = format!(
            r"SELECT session_id, durable_cursor, event_cursor, history_revision,
                     transcript_messages, transcript_bytes,
                     latest_checkpoint_sequence, latest_checkpoint_event_id,
                     index_generation, indexed_through_sequence, index_card_count,
                     index_pending, in_flight_turn, pending_approval,
                     active_writer_or_attachment, mission_agent_team_continuation,
                     last_activity_ms, manifest_revision
                FROM session_recovery_manifest
               WHERE session_id IN ({placeholders})
               ORDER BY session_id ASC"
        );
        let conn = self.conn()?;
        let mut statement = conn.prepare(&sql).map_err(sql_err)?;
        let rows = statement
            .query_map(
                params_from_iter(session_ids.iter()),
                row_to_recovery_manifest,
            )
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    /// Rebuild the body-free activation manifest from canonical rows.
    ///
    /// This repair path intentionally leaves source messages and events
    /// untouched. It marks the navigation index pending so the asynchronous
    /// projector can verify/rebuild cards after activation.
    pub fn rebuild_session_recovery_manifest(
        &self,
        session_id: &str,
        now_ms: u64,
    ) -> Result<Option<SessionRecoveryManifest>> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(sql_err)?;
        let inserted = tx
            .execute(
                r"INSERT OR IGNORE INTO session_recovery_manifest(
                       session_id, last_activity_ms, manifest_revision
                   )
                   SELECT session_id, MAX(created_at_ms, updated_at_ms), 1
                     FROM sessions
                    WHERE session_id=?1",
                params![session_id],
            )
            .map_err(sql_err)?;
        if inserted == 0 {
            let session_exists = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sessions WHERE session_id=?1)",
                    params![session_id],
                    |row| row.get::<_, bool>(0),
                )
                .map_err(sql_err)?;
            if !session_exists {
                tx.commit().map_err(sql_err)?;
                return Ok(None);
            }
        }
        tx.execute(
            r"UPDATE session_recovery_manifest
                  SET durable_cursor=COALESCE((
                          SELECT MAX(sequence)+1 FROM messages
                           WHERE session_id=?1
                      ),0),
                      event_cursor=COALESCE((
                          SELECT MAX(sequence)+1 FROM session_events
                           WHERE session_id=?1
                      ),0),
                      history_revision=COALESCE((
                          SELECT COUNT(*) FROM messages WHERE session_id=?1
                      ),0),
                      transcript_messages=COALESCE((
                          SELECT COUNT(*) FROM messages WHERE session_id=?1
                      ),0),
                      transcript_bytes=COALESCE((
                          SELECT SUM(
                              length(CAST(stable_message_id AS BLOB))
                              + length(CAST(session_id AS BLOB))
                              + length(CAST(role AS BLOB))
                              + length(CAST(content_json AS BLOB))
                              + length(CAST(COALESCE(token_usage_json,'') AS BLOB))
                              + length(CAST(COALESCE(tool_use_id,'') AS BLOB))
                              + length(CAST(COALESCE(tool_name,'') AS BLOB))
                          ) FROM messages WHERE session_id=?1
                      ),0),
                      latest_checkpoint_sequence=(
                          SELECT MAX(sequence) FROM session_events
                           WHERE session_id=?1
                             AND event_type='SessionDomainEvent'
                             AND json_extract(event_json,'$.kind')=
                                 'memory.semantic_checkpoint.created'
                      ),
                      latest_checkpoint_event_id=(
                          SELECT json_extract(event_json,'$.event_id')
                            FROM session_events
                           WHERE session_id=?1
                             AND event_type='SessionDomainEvent'
                             AND json_extract(event_json,'$.kind')=
                                 'memory.semantic_checkpoint.created'
                           ORDER BY sequence DESC LIMIT 1
                      ),
                      index_generation=COALESCE((
                          SELECT MAX(generation) FROM session_context_index_cards
                           WHERE session_id=?1
                      ),0),
                      indexed_through_sequence=(
                          SELECT MAX(source_end_sequence)
                            FROM session_context_index_cards WHERE session_id=?1
                      ),
                      index_card_count=COALESCE((
                          SELECT COUNT(*) FROM session_context_index_cards
                           WHERE session_id=?1
                      ),0),
                      index_pending=CASE WHEN EXISTS(
                          SELECT 1 FROM messages WHERE session_id=?1
                      ) OR EXISTS(
                          SELECT 1 FROM session_events
                           WHERE session_id=?1
                             AND event_type='SessionDomainEvent'
                             AND json_extract(event_json,'$.kind')=
                                 'memory.semantic_checkpoint.created'
                      ) THEN 1 ELSE 0 END,
                      in_flight_turn=EXISTS(
                          SELECT 1 FROM session_runtime_outbox
                           WHERE session_id=?1
                             AND status IN (
                                 'accepted','classified','queued','claimed',
                                 'running','reclassified'
                             )
                      ),
                      active_writer_or_attachment=COALESCE((
                          SELECT CASE WHEN json_array_length(
                              json_extract(event_json,'$.snapshot.attachments')
                          ) > 0 THEN 1 ELSE 0 END
                            FROM session_events
                           WHERE session_id=?1
                             AND event_type='session.lifecycle.v1'
                           ORDER BY sequence DESC LIMIT 1
                      ),0),
                      last_activity_ms=MAX(last_activity_ms,?2),
                      manifest_revision=manifest_revision+1
                WHERE session_id=?1",
            params![session_id, now_ms as i64],
        )
        .map_err(sql_err)?;
        tx.execute(
            r"INSERT INTO session_context_index_outbox(
                   session_id, source_sequence, operation, status,
                   created_at_ms, updated_at_ms
               )
               SELECT ?1,0,'reconcile','pending',?2,?2
                WHERE EXISTS(SELECT 1 FROM messages WHERE session_id=?1)
               ON CONFLICT(session_id, source_sequence, operation) DO UPDATE SET
                   status='pending',
                   updated_at_ms=MAX(updated_at_ms,excluded.updated_at_ms)",
            params![session_id, now_ms as i64],
        )
        .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        self.get_session_recovery_manifest(session_id)
    }

    /// Page active Session manifests without reading transcript rows.
    pub fn list_active_session_recovery_manifests(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<SessionRecoveryManifest>> {
        let conn = self.conn()?;
        let mut statement = conn
            .prepare(
                r"SELECT manifest.session_id, manifest.durable_cursor,
                         manifest.event_cursor, manifest.history_revision,
                         manifest.transcript_messages, manifest.transcript_bytes,
                         manifest.latest_checkpoint_sequence,
                         manifest.latest_checkpoint_event_id,
                         manifest.index_generation,
                         manifest.indexed_through_sequence,
                         manifest.index_card_count,
                         manifest.index_pending,
                         manifest.in_flight_turn,
                         manifest.pending_approval,
                         manifest.active_writer_or_attachment,
                         manifest.mission_agent_team_continuation,
                         manifest.last_activity_ms, manifest.manifest_revision
                    FROM session_recovery_manifest AS manifest
                    JOIN sessions ON sessions.session_id=manifest.session_id
                   WHERE sessions.status='active'
                   ORDER BY manifest.last_activity_ms DESC, manifest.session_id ASC
                   LIMIT ?1 OFFSET ?2",
            )
            .map_err(sql_err)?;
        let rows = statement
            .query_map(
                params![limit as i64, offset as i64],
                row_to_recovery_manifest,
            )
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    pub fn list_required_session_recovery_manifests(
        &self,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<SessionRecoveryManifest>> {
        let conn = self.conn()?;
        let mut statement = conn
            .prepare(
                r"SELECT manifest.session_id, manifest.durable_cursor,
                         manifest.event_cursor, manifest.history_revision,
                         manifest.transcript_messages, manifest.transcript_bytes,
                         manifest.latest_checkpoint_sequence,
                         manifest.latest_checkpoint_event_id,
                         manifest.index_generation,
                         manifest.indexed_through_sequence,
                         manifest.index_card_count,
                         manifest.index_pending,
                         manifest.in_flight_turn,
                         manifest.pending_approval,
                         manifest.active_writer_or_attachment,
                         manifest.mission_agent_team_continuation,
                         manifest.last_activity_ms, manifest.manifest_revision
                    FROM session_recovery_manifest AS manifest
                    JOIN sessions ON sessions.session_id=manifest.session_id
                   WHERE sessions.status='active'
                     AND (
                         manifest.in_flight_turn=1
                         OR manifest.pending_approval=1
                         OR manifest.mission_agent_team_continuation=1
                     )
                   ORDER BY manifest.last_activity_ms DESC, manifest.session_id ASC
                   LIMIT ?1 OFFSET ?2",
            )
            .map_err(sql_err)?;
        let rows = statement
            .query_map(
                params![limit as i64, offset as i64],
                row_to_recovery_manifest,
            )
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    /// Update one external durable recovery signal without overwriting other
    /// independently-owned signals.
    pub fn set_session_recovery_signal(
        &self,
        session_id: &str,
        signal: SessionRecoverySignal,
        active: bool,
        observed_at_ms: u64,
    ) -> Result<SessionRecoveryManifest> {
        let column = match signal {
            SessionRecoverySignal::PendingApproval => "pending_approval",
            SessionRecoverySignal::ActiveWriterOrAttachment => "active_writer_or_attachment",
            SessionRecoverySignal::MissionAgentTeamContinuation => {
                "mission_agent_team_continuation"
            }
        };
        let conn = self.conn()?;
        conn.execute(
            &format!(
                "UPDATE session_recovery_manifest
                    SET {column}=?2,
                        last_activity_ms=MAX(last_activity_ms, ?3),
                        manifest_revision=manifest_revision + 1
                  WHERE session_id=?1"
            ),
            params![session_id, active, observed_at_ms as i64],
        )
        .map_err(sql_err)?;
        drop(conn);
        self.get_session_recovery_manifest(session_id)?
            .ok_or_else(|| {
                SessionError::Store(format!(
                    "session recovery manifest `{session_id}` does not exist"
                ))
            })
    }

    /// Overwrite all mutable fields of an existing session record.
    ///
    /// `session_id` is used as the lookup key; the row is silently unchanged
    /// if it does not exist.
    pub fn update_session(&self, session: &SessionRecord) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"UPDATE sessions SET
               platform      = ?2,
               chat_id       = ?3,
               user_id       = ?4,
               model         = ?5,
               last_activity = ?6,
               message_count = ?7,
               reset_policy  = ?8,
               metadata_json = ?9,
               input_tokens  = ?10,
               output_tokens = ?11,
               status = ?12,
               updated_at_ms = ?13
               WHERE session_id = ?1",
            params![
                session.session_id,
                session.platform,
                session.chat_id,
                session.user_id,
                session.model,
                session.last_activity,
                session.message_count,
                session.reset_policy,
                session.metadata_json,
                session.input_tokens,
                session.output_tokens,
                session.status,
                iso_to_ms(&session.last_activity),
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Upsert a session record (insert or replace all fields).
    ///
    /// Equivalent to calling [`create_session`] then [`update_session`].  Use
    /// this when you don't know whether the row already exists.
    pub fn upsert_session(&self, session: &SessionRecord) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"INSERT INTO sessions
               (session_id, platform, chat_id, user_id, model,
                created_at, last_activity, message_count, reset_policy, metadata_json,
                input_tokens, output_tokens, status,
                created_at_ms, updated_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
               ON CONFLICT(session_id) DO UPDATE SET
                 platform = excluded.platform,
                 chat_id = excluded.chat_id,
                 user_id = excluded.user_id,
                 model = excluded.model,
                 created_at = excluded.created_at,
                 last_activity = excluded.last_activity,
                 message_count = excluded.message_count,
                 reset_policy = excluded.reset_policy,
                 metadata_json = excluded.metadata_json,
                 input_tokens = excluded.input_tokens,
                 output_tokens = excluded.output_tokens,
                 status = excluded.status,
                 created_at_ms = excluded.created_at_ms,
                 updated_at_ms = excluded.updated_at_ms",
            params![
                session.session_id,
                session.platform,
                session.chat_id,
                session.user_id,
                session.model,
                session.created_at,
                session.last_activity,
                session.message_count,
                session.reset_policy,
                session.metadata_json,
                session.input_tokens,
                session.output_tokens,
                session.status,
                iso_to_ms(&session.created_at),
                iso_to_ms(&session.last_activity),
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    pub fn plan_session_lifecycle(
        &self,
        plan: &SessionLifecyclePlan,
    ) -> Result<SessionLifecycleIntent> {
        validate_plan_identity(
            &plan.operation_id,
            &plan.session_id,
            plan.expected_generation,
        )?;
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        if let Some(existing) = query_lifecycle_intent(&tx, &plan.operation_id)? {
            if existing.session_id == plan.session_id
                && existing.disposition == plan.disposition
                && existing.expected_generation == plan.expected_generation
            {
                tx.commit().map_err(sql_err)?;
                return Ok(existing);
            }
            return Err(SessionError::Store(format!(
                "Session lifecycle operation `{}` is bound to another identity",
                plan.operation_id
            )));
        }
        let admission = query_input_admission(&tx, &plan.session_id)?.ok_or_else(|| {
            SessionError::Store(format!("session `{}` not found", plan.session_id))
        })?;
        if admission.generation != plan.expected_generation || !admission.open {
            return Err(SessionError::Store(format!(
                "Session lifecycle plan `{}` expected open generation {}, found generation {} open={}",
                plan.operation_id,
                plan.expected_generation,
                admission.generation,
                admission.open
            )));
        }
        tx.execute(
            r"INSERT INTO session_lifecycle_intents
                (operation_id, session_id, disposition, phase, last_stable_phase,
                 expected_generation, created_at_ms, updated_at_ms, last_error, revision)
               VALUES (?1, ?2, ?3, 'planned', 'planned', ?4, ?5, ?5, NULL, 0)",
            params![
                plan.operation_id,
                plan.session_id,
                plan.disposition.as_str(),
                plan.expected_generation as i64,
                plan.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        let intent = query_lifecycle_intent(&tx, &plan.operation_id)?.ok_or_else(|| {
            SessionError::Store("Session lifecycle plan produced no readable row".to_string())
        })?;
        tx.commit().map_err(sql_err)?;
        Ok(intent)
    }

    pub fn get_session_lifecycle_intent(
        &self,
        operation_id: &str,
    ) -> Result<Option<SessionLifecycleIntent>> {
        let conn = self.conn()?;
        query_lifecycle_intent(&conn, operation_id)
    }

    pub fn list_recoverable_session_lifecycle_intents(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionLifecycleIntent>> {
        let conn = self.conn()?;
        let mut statement = conn
            .prepare(
                r"SELECT operation_id, session_id, disposition, phase, last_stable_phase,
                          expected_generation, created_at_ms, updated_at_ms, last_error, revision
                     FROM session_lifecycle_intents
                    WHERE phase != 'unloaded'
                    ORDER BY updated_at_ms ASC, operation_id ASC
                    LIMIT ?1",
            )
            .map_err(sql_err)?;
        let rows = statement
            .query_map(params![limit as i64], row_to_lifecycle_intent)
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    pub fn fence_session_lifecycle(
        &self,
        request: &SessionLifecycleFenceRequest,
    ) -> Result<SessionLifecycleIntent> {
        validate_fence_metadata(
            &request.actor,
            &request.reason,
            &request.transitional_status,
        )?;
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current =
            query_lifecycle_intent(&tx, &request.transition.operation_id)?.ok_or_else(|| {
                SessionError::Store(format!(
                    "Session lifecycle intent `{}` does not exist",
                    request.transition.operation_id
                ))
            })?;
        request.transition.validate(&current)?;
        if request.transition.next_phase != SessionLifecyclePhase::AdmissionFenced
            || request.event.session_id != current.session_id
        {
            return Err(SessionError::Store(
                "Session lifecycle fence identity or phase is invalid".to_string(),
            ));
        }
        let admission = query_input_admission(&tx, &current.session_id)?.ok_or_else(|| {
            SessionError::Store(format!("session `{}` not found", current.session_id))
        })?;
        if admission.generation != current.expected_generation || !admission.open {
            return Err(SessionError::Store(format!(
                "Session lifecycle fence `{}` lost generation authority",
                current.operation_id
            )));
        }
        let active = {
            let mut statement = tx
                .prepare(
                    r"SELECT request_id FROM session_runtime_outbox
                       WHERE session_id=?1 AND session_generation=?2
                         AND status IN (
                             'accepted','classified','queued','claimed',
                             'running','reclassified','blocked'
                         )
                       ORDER BY sequence ASC, request_id ASC",
                )
                .map_err(sql_err)?;
            let rows = statement
                .query_map(
                    params![current.session_id, current.expected_generation as i64],
                    |row| row.get::<_, String>(0),
                )
                .map_err(sql_err)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_err)?
        };
        let next_generation = current
            .expected_generation
            .checked_add(1)
            .ok_or_else(|| SessionError::Store("Session generation overflow".to_string()))?;
        let changed = tx
            .execute(
                r"UPDATE sessions
                     SET input_generation=?1, input_admission_open=0, status=?2,
                         last_activity=?3, updated_at_ms=MAX(updated_at_ms, ?4)
                   WHERE session_id=?5 AND input_generation=?6
                     AND input_admission_open=1",
                params![
                    next_generation as i64,
                    request.transitional_status,
                    DateTime::<Utc>::from_timestamp_millis(request.transition.updated_at_ms as i64)
                        .unwrap_or_else(Utc::now)
                        .to_rfc3339(),
                    request.transition.updated_at_ms as i64,
                    current.session_id,
                    current.expected_generation as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "Session lifecycle fence `{}` changed during admission close",
                current.operation_id
            )));
        }
        for request_id in active {
            let before = query_outbox(&tx, &request_id)?.ok_or_else(|| {
                SessionError::Store(format!(
                    "outbox `{request_id}` disappeared during lifecycle fence"
                ))
            })?;
            tx.execute(
                r"UPDATE session_runtime_outbox
                     SET status='expired', claim_owner=NULL, claim_token=NULL,
                         claim_fence_epoch=NULL,
                         claim_expires_at_ms=NULL, last_error=?1,
                         terminal_at_ms=?2, updated_at_ms=?2, revision=revision+1
                   WHERE request_id=?3 AND session_generation=?4 AND revision=?5",
                params![
                    request.reason,
                    request.transition.updated_at_ms as i64,
                    request_id,
                    current.expected_generation as i64,
                    before.revision as i64,
                ],
            )
            .map_err(sql_err)?;
            let expired = query_outbox(&tx, &request_id)?.ok_or_else(|| {
                SessionError::Store(format!("expired outbox `{request_id}` disappeared"))
            })?;
            append_outbox_history(
                &tx,
                &expired,
                "lifecycle_fence",
                Some(&request.actor),
                Some(&request.reason),
                before.status.as_str(),
                SessionRuntimeInputStatus::Expired.as_str(),
                request.transition.updated_at_ms,
            )?;
        }
        let closed = SessionInputAdmission {
            session_id: current.session_id.clone(),
            generation: next_generation,
            open: false,
        };
        append_admission_timeline_event(
            &tx,
            &current.session_id,
            current.expected_generation,
            &closed,
            &request.actor,
            &request.reason,
            request.transition.updated_at_ms,
        )?;
        append_allocated_event_tx(&tx, &request.event)?;
        let intent = transition_lifecycle_intent_tx(&tx, &request.transition)?;
        tx.commit().map_err(sql_err)?;
        Ok(intent)
    }

    pub fn transition_session_lifecycle(
        &self,
        transition: &SessionLifecycleTransition,
    ) -> Result<SessionLifecycleIntent> {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let intent = transition_lifecycle_intent_tx(&tx, transition)?;
        tx.commit().map_err(sql_err)?;
        Ok(intent)
    }

    pub fn commit_session_lifecycle_tombstone(
        &self,
        request: &SessionLifecycleTombstoneRequest,
    ) -> Result<SessionLifecycleIntent> {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current =
            query_lifecycle_intent(&tx, &request.transition.operation_id)?.ok_or_else(|| {
                SessionError::Store(format!(
                    "Session lifecycle intent `{}` does not exist",
                    request.transition.operation_id
                ))
            })?;
        request.transition.validate(&current)?;
        if request.transition.next_phase != SessionLifecyclePhase::TombstoneCommitted
            || request.record.session_id != current.session_id
            || request.event.session_id != current.session_id
        {
            return Err(SessionError::Store(
                "Session lifecycle tombstone identity or phase is invalid".to_string(),
            ));
        }
        let changed = tx
            .execute(
                r"UPDATE sessions SET
                     platform=?2, chat_id=?3, user_id=?4, model=?5,
                     last_activity=?6, message_count=?7, reset_policy=?8,
                     metadata_json=?9, input_tokens=?10, output_tokens=?11,
                     status=?12, updated_at_ms=?13
                   WHERE session_id=?1 AND input_generation=?14
                     AND input_admission_open=0",
                params![
                    request.record.session_id,
                    request.record.platform,
                    request.record.chat_id,
                    request.record.user_id,
                    request.record.model,
                    request.record.last_activity,
                    request.record.message_count,
                    request.record.reset_policy,
                    request.record.metadata_json,
                    request.record.input_tokens,
                    request.record.output_tokens,
                    request.record.status,
                    request.transition.updated_at_ms as i64,
                    current.expected_generation.saturating_add(1) as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "Session lifecycle tombstone `{}` lost fenced Session authority",
                current.operation_id
            )));
        }
        append_allocated_event_tx(&tx, &request.event)?;
        let intent = transition_lifecycle_intent_tx(&tx, &request.transition)?;
        tx.commit().map_err(sql_err)?;
        Ok(intent)
    }

    /// Permanently remove a session and all its memory associations.
    pub fn delete_session(&self, session_id: &str) -> Result<()> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(sql_err)?;
        // FK ON DELETE CASCADE handles cleanup; manual delete is belt-and-suspenders
        tx.execute(
            "DELETE FROM session_memories WHERE session_id = ?1",
            params![session_id],
        )
        .map_err(sql_err)?;
        tx.execute(
            "DELETE FROM sessions WHERE session_id = ?1",
            params![session_id],
        )
        .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        Ok(())
    }

    /// List all session records ordered by `last_activity DESC`.
    pub fn list_sessions(&self) -> Result<Vec<SessionRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT session_id, platform, chat_id, user_id, model,
                          created_at, last_activity, message_count, reset_policy, metadata_json,
                          input_tokens, output_tokens, status
                   FROM sessions ORDER BY last_activity DESC",
            )
            .map_err(sql_err)?;
        let rows = stmt.query_map([], row_to_record).map_err(sql_err)?;
        let mut records = Vec::new();
        for r in rows {
            records.push(r.map_err(sql_err)?);
        }
        Ok(records)
    }

    /// List a filtered, sorted page of sessions directly in SQLite.
    ///
    /// This is the API-facing path for large workspaces. It avoids loading all
    /// sessions into memory before filtering and paginating.
    pub fn list_sessions_page(&self, opts: &SessionListOptions<'_>) -> Result<SessionListPage> {
        let conn = self.conn()?;
        let limit = bounded_limit(opts.limit, 1, 500);
        let offset = opts.offset;
        let (where_sql, mut values) = session_list_where_clause(opts);

        let count_sql = format!("SELECT COUNT(*) FROM sessions{where_sql}");
        let total: i64 = conn
            .query_row(&count_sql, params_from_iter(values.iter()), |row| {
                row.get(0)
            })
            .map_err(sql_err)?;

        let sort_expr = session_sort_expression(opts.sort);
        let sort_order = session_sort_order(opts.order);
        let page_sql = format!(
            r"SELECT session_id, platform, chat_id, user_id, model,
                      created_at, last_activity, message_count, reset_policy, metadata_json,
                      input_tokens, output_tokens, status
                 FROM sessions{where_sql}
                ORDER BY {sort_expr} {sort_order}, session_id ASC
                LIMIT ? OFFSET ?"
        );
        values.push(Value::Integer(limit as i64));
        values.push(Value::Integer(offset as i64));

        let mut stmt = conn.prepare(&page_sql).map_err(sql_err)?;
        let rows = stmt
            .query_map(params_from_iter(values.iter()), row_to_record)
            .map_err(sql_err)?;
        let mut records = Vec::new();
        for r in rows {
            records.push(r.map_err(sql_err)?);
        }
        Ok(SessionListPage {
            records,
            total: total as usize,
        })
    }

    pub fn session_usage_summary(&self, recent_limit: usize) -> Result<SessionUsageSummary> {
        let conn = self.conn()?;
        let (session_count, message_count, input_tokens, output_tokens) = conn
            .query_row(
                "SELECT COUNT(*),COALESCE(SUM(message_count),0),
                        COALESCE(SUM(input_tokens),0),COALESCE(SUM(output_tokens),0)
                   FROM sessions WHERE status NOT IN ('deleted','deleting')",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .map_err(sql_err)?;
        let load_buckets =
            |column: &str| -> Result<std::collections::BTreeMap<String, SessionUsageBucket>> {
                let sql = format!(
                    "SELECT COALESCE(NULLIF(TRIM({column}),''),'unknown'),COUNT(*),
                        COALESCE(SUM(message_count),0),COALESCE(SUM(input_tokens),0),
                        COALESCE(SUM(output_tokens),0)
                   FROM sessions WHERE status NOT IN ('deleted','deleting')
                  GROUP BY 1 ORDER BY 1"
                );
                let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            SessionUsageBucket {
                                session_count: row.get::<_, i64>(1)? as usize,
                                message_count: row.get(2)?,
                                input_tokens: row.get(3)?,
                                output_tokens: row.get(4)?,
                            },
                        ))
                    })
                    .map_err(sql_err)?;
                rows.collect::<rusqlite::Result<std::collections::BTreeMap<_, _>>>()
                    .map_err(sql_err)
            };
        let recent_sessions = self
            .list_sessions_page(&SessionListOptions {
                unrestricted: true,
                include_deleted: false,
                sort: "last_activity",
                order: "desc",
                limit: bounded_limit(recent_limit, 1, 200),
                ..SessionListOptions::default()
            })?
            .records;
        Ok(SessionUsageSummary {
            session_count: session_count as usize,
            message_count,
            input_tokens,
            output_tokens,
            by_platform: load_buckets("platform")?,
            by_model: load_buckets("model")?,
            recent_sessions,
        })
    }

    /// Discover Session metadata and transcript matches inside the current
    /// Session's durable workspace/actor boundary.
    ///
    /// The current Session row is the authority source. A caller cannot widen
    /// the query by supplying a workspace or principal in tool input.
    pub fn discover_browsable_sessions(
        &self,
        current_session_id: &str,
        query: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<SessionListPage> {
        let conn = self.conn()?;
        let limit = bounded_limit(limit, 1, 100);
        let query = query.map(str::trim).filter(|query| !query.is_empty());
        let mut values = vec![Value::Text(current_session_id.to_string())];
        let mut query_clause = String::new();

        if let Some(query) = query {
            let like = format!("%{}%", escape_like_pattern(query));
            values.push(Value::Text(like));
            query_clause.push_str(
                r" AND (
                       s.session_id LIKE ? ESCAPE '\' COLLATE NOCASE
                    OR s.platform LIKE ? ESCAPE '\' COLLATE NOCASE
                    OR s.chat_id LIKE ? ESCAPE '\' COLLATE NOCASE
                    OR COALESCE(s.metadata_json, '') LIKE ? ESCAPE '\' COLLATE NOCASE",
            );
            for _ in 0..3 {
                values.push(values[1].clone());
            }
            if let Some(fts_query) = fts_literal_terms(query) {
                values.push(Value::Text(fts_query));
                query_clause.push_str(
                    r" OR EXISTS (
                           SELECT 1
                             FROM messages m
                             JOIN messages_fts ON m.id = messages_fts.rowid
                            WHERE m.session_id = s.session_id
                              AND messages_fts MATCH ?
                       )",
                );
            }
            query_clause.push(')');
        }

        let authority_clause = r"
            FROM sessions s
            JOIN sessions current ON current.session_id = ?
           WHERE s.status NOT IN ('deleted', 'deleting')
             AND (
                    s.session_id = current.session_id
                 OR (
                        NULLIF(json_extract(current.metadata_json, '$.workspace_root'), '') IS NOT NULL
                    AND json_extract(s.metadata_json, '$.workspace_root')
                        = json_extract(current.metadata_json, '$.workspace_root')
                    AND (
                           (
                               NULLIF(json_extract(current.metadata_json, '$.owner_principal_id'), '') IS NOT NULL
                           AND json_extract(s.metadata_json, '$.owner_principal_id')
                               = json_extract(current.metadata_json, '$.owner_principal_id')
                           )
                        OR (
                               NULLIF(json_extract(current.metadata_json, '$.owner_principal_id'), '') IS NULL
                           AND NULLIF(current.user_id, '') IS NOT NULL
                           AND s.platform = current.platform
                           AND s.user_id = current.user_id
                           )
                       )
                    )
                 )";
        let count_sql = format!("SELECT COUNT(*) {authority_clause}{query_clause}");
        let total = conn
            .query_row(&count_sql, params_from_iter(values.iter()), |row| {
                row.get::<_, i64>(0)
            })
            .map_err(sql_err)?;

        let page_sql = format!(
            r"SELECT s.session_id, s.platform, s.chat_id, s.user_id, s.model,
                      s.created_at, s.last_activity, s.message_count, s.reset_policy,
                      s.metadata_json, s.input_tokens, s.output_tokens, s.status
                 {authority_clause}{query_clause}
                ORDER BY s.last_activity DESC, s.session_id ASC
                LIMIT ? OFFSET ?"
        );
        values.push(Value::Integer(limit as i64));
        values.push(Value::Integer(offset as i64));
        let mut stmt = conn.prepare(&page_sql).map_err(sql_err)?;
        let rows = stmt
            .query_map(params_from_iter(values.iter()), row_to_record)
            .map_err(sql_err)?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row.map_err(sql_err)?);
        }
        Ok(SessionListPage {
            records,
            total: total.max(0) as usize,
        })
    }

    /// List all sessions for a given platform, ordered by `last_activity DESC`.
    pub fn list_sessions_by_platform(&self, platform: &str) -> Result<Vec<SessionRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT session_id, platform, chat_id, user_id, model,
                          created_at, last_activity, message_count, reset_policy, metadata_json,
                          input_tokens, output_tokens, status
                   FROM sessions WHERE platform = ?1 ORDER BY last_activity DESC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![platform], row_to_record)
            .map_err(sql_err)?;
        let mut records = Vec::new();
        for r in rows {
            records.push(r.map_err(sql_err)?);
        }
        Ok(records)
    }

    /// List all sessions bound to a workspace root through metadata_json.
    ///
    /// This is the DB-backed replacement for the deprecated filesystem
    /// `SessionStore` workspace namespace. Records without a
    /// `metadata_json.workspace_root` value are intentionally excluded.
    pub fn list_sessions_by_workspace_root(
        &self,
        workspace_root: &str,
    ) -> Result<Vec<SessionRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT session_id, platform, chat_id, user_id, model,
                          created_at, last_activity, message_count, reset_policy, metadata_json,
                          input_tokens, output_tokens, status
                   FROM sessions
                  WHERE json_extract(metadata_json, '$.workspace_root') = ?1
                  ORDER BY last_activity DESC, session_id ASC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![workspace_root], row_to_record)
            .map_err(sql_err)?;
        let mut records = Vec::new();
        for r in rows {
            records.push(r.map_err(sql_err)?);
        }
        Ok(records)
    }

    /// Search sessions using FTS5 full-text search.
    ///
    /// Searches across platform, chat_id, user_id, and metadata_json.
    /// Returns results with highlighted snippets from metadata.
    pub fn search_sessions(&self, query: &str, limit: usize) -> Result<Vec<SessionSearchResult>> {
        let conn = self.conn()?;

        // Join sessions with FTS5 and get snippets
        let sql = r"
            SELECT s.session_id, s.platform, s.chat_id, s.user_id,
                   s.created_at, s.last_activity, s.message_count,
                   snippet(sessions_fts, 4, '<mark>', '</mark>', '...', 32) as snippet
            FROM sessions s
            JOIN sessions_fts fts ON s.session_id = fts.session_id
            WHERE sessions_fts MATCH ?1
            ORDER BY rank
            LIMIT ?2
        ";

        let mut stmt = conn.prepare(sql).map_err(sql_err)?;
        let rows = stmt
            .query_map(params![query, limit as i64], |row| {
                Ok(SessionSearchResult {
                    session_id: row.get(0)?,
                    platform: row.get(1)?,
                    chat_id: row.get(2)?,
                    user_id: row.get(3)?,
                    created_at: row.get(4)?,
                    last_activity: row.get(5)?,
                    message_count: row.get(6)?,
                    snippet: row.get(7)?,
                })
            })
            .map_err(sql_err)?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(sql_err)?);
        }
        Ok(results)
    }

    /// Search sessions with platform filter.
    pub fn search_sessions_by_platform(
        &self,
        query: &str,
        platform: &str,
        limit: usize,
    ) -> Result<Vec<SessionSearchResult>> {
        let conn = self.conn()?;

        let sql = r"
            SELECT s.session_id, s.platform, s.chat_id, s.user_id,
                   s.created_at, s.last_activity, s.message_count,
                   snippet(sessions_fts, 4, '<mark>', '</mark>', '...', 32) as snippet
            FROM sessions s
            JOIN sessions_fts fts ON s.session_id = fts.session_id
            WHERE sessions_fts MATCH ?1 AND s.platform = ?2
            ORDER BY rank
            LIMIT ?3
        ";

        let mut stmt = conn.prepare(sql).map_err(sql_err)?;
        let rows = stmt
            .query_map(params![query, platform, limit as i64], |row| {
                Ok(SessionSearchResult {
                    session_id: row.get(0)?,
                    platform: row.get(1)?,
                    chat_id: row.get(2)?,
                    user_id: row.get(3)?,
                    created_at: row.get(4)?,
                    last_activity: row.get(5)?,
                    message_count: row.get(6)?,
                    snippet: row.get(7)?,
                })
            })
            .map_err(sql_err)?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(sql_err)?);
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Session ↔ Memory associations
    // -----------------------------------------------------------------------

    /// Link a memory ID to a session.
    ///
    /// `INSERT OR IGNORE` makes this idempotent.
    pub fn associate_memory(&self, session_id: &str, memory_id: &str) -> Result<()> {
        let conn = self.conn()?;
        let now = Utc::now().to_rfc3339();
        conn.execute(
            r"INSERT OR IGNORE INTO session_memories (session_id, memory_id, created_at)
               VALUES (?1, ?2, ?3)",
            params![session_id, memory_id, now],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Return all memory IDs associated with `session_id`.
    pub fn get_session_memories(&self, session_id: &str) -> Result<Vec<String>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT memory_id FROM session_memories WHERE session_id = ?1 ORDER BY created_at",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![session_id], |row| row.get::<_, String>(0))
            .map_err(sql_err)?;
        let mut ids = Vec::new();
        for r in rows {
            ids.push(r.map_err(sql_err)?);
        }
        Ok(ids)
    }

    /// Remove the association between a session and a memory.
    pub fn disassociate_memory(&self, session_id: &str, memory_id: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "DELETE FROM session_memories WHERE session_id = ?1 AND memory_id = ?2",
            params![session_id, memory_id],
        )
        .map_err(sql_err)?;
        Ok(())
    }
}
