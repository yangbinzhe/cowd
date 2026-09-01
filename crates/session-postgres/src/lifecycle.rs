//! Lifecycle operations for the PostgresSessionStore adapter.

use super::*;

impl PostgresSessionStore {
    pub fn new(executor: PostgresExecutor) -> session::SessionResult<Self> {
        prepare_legacy_session_usage_for_migration(&executor)?;
        executor
            .apply_migrations(SESSION_DOMAIN, SESSION_MIGRATIONS)
            .map_err(storage_error)?;
        Ok(Self { executor })
    }

    pub fn connect(
        config: PostgresConnectionConfig,
        resolver: &dyn SecretRefResolver,
    ) -> session::SessionResult<Self> {
        PostgresExecutor::connect(config, resolver)
            .map_err(storage_error)
            .and_then(Self::new)
    }

    #[must_use]
    pub fn executor(&self) -> &PostgresExecutor {
        &self.executor
    }

    pub fn create_session(&self, session: &SessionRecord) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "INSERT INTO session_records(
                    session_id, platform, chat_id, user_id, model, created_at,
                    last_activity, message_count, reset_policy, metadata_json,
                    input_tokens, output_tokens, status,
                    created_at_ms, updated_at_ms
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
                    cowd_safe_session_epoch_ms($6), cowd_safe_session_epoch_ms($7))
                 ON CONFLICT(session_id) DO NOTHING",
                &session_params(session),
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn get_session(&self, session_id: &str) -> session::SessionResult<Option<SessionRecord>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(SESSION_SELECT_BY_ID, &[&session_id])
            .map_err(postgres_error)?
            .map(|row| row_to_session(&row))
            .transpose()
    }

    pub fn get_sessions_by_ids(
        &self,
        session_ids: &[String],
    ) -> session::SessionResult<Vec<SessionRecord>> {
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT session_id, platform, chat_id, user_id, model,
                        created_at, last_activity, message_count, reset_policy, metadata_json,
                        input_tokens, output_tokens, status
                   FROM session_records
                  WHERE session_id = ANY($1)
                  ORDER BY session_id ASC",
                &[&session_ids],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_session)
            .collect()
    }

    pub fn get_session_recovery_manifest(
        &self,
        session_id: &str,
    ) -> session::SessionResult<Option<SessionRecoveryManifest>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT session_id, durable_cursor, event_cursor, history_revision,
                        transcript_messages, transcript_bytes,
                        latest_checkpoint_sequence, latest_checkpoint_event_id,
                        index_generation, indexed_through_sequence, index_card_count,
                        index_pending,
                        in_flight_turn,
                        pending_approval, active_writer_or_attachment,
                        mission_agent_team_continuation, last_activity_ms,
                        manifest_revision
                   FROM session_recovery_manifest
                  WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_recovery_manifest(&row))
            .transpose()
    }

    pub fn get_session_presence_projection(
        &self,
        session_id: &str,
    ) -> session::SessionResult<Option<SessionPresenceProjection>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT session_id,state,attachments_json::text,next_sequence,revision,updated_at_ms
                   FROM session_presence_projection WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .map(|row| {
                Ok(SessionPresenceProjection {
                    session_id: row.try_get(0).map_err(postgres_error)?,
                    state: row.try_get(1).map_err(postgres_error)?,
                    attachments_json: row.try_get(2).map_err(postgres_error)?,
                    next_sequence: from_i64(
                        row.try_get(3).map_err(postgres_error)?,
                        "presence next sequence",
                    )?,
                    revision: i64_to_u64(
                        row.try_get(4).map_err(postgres_error)?,
                        "presence revision",
                    )?,
                    updated_at_ms: i64_to_u64(
                        row.try_get(5).map_err(postgres_error)?,
                        "presence updated time",
                    )?,
                })
            })
            .transpose()
    }

    pub fn upsert_session_presence_projection(
        &self,
        projection: &SessionPresenceProjection,
    ) -> session::SessionResult<()> {
        let next_sequence = to_i64(projection.next_sequence, "presence next sequence")?;
        let revision = to_u64_i64(projection.revision, "presence revision")?;
        let updated_at_ms = to_u64_i64(projection.updated_at_ms, "presence updated time")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "INSERT INTO session_presence_projection(
                     session_id,state,attachments_json,next_sequence,revision,updated_at_ms
                 ) VALUES ($1,$2,$3::text::jsonb,$4,$5,$6)
                 ON CONFLICT(session_id) DO UPDATE SET
                     state=EXCLUDED.state,
                     attachments_json=EXCLUDED.attachments_json,
                     next_sequence=EXCLUDED.next_sequence,
                     revision=EXCLUDED.revision,
                     updated_at_ms=EXCLUDED.updated_at_ms",
                &[
                    &projection.session_id,
                    &projection.state,
                    &projection.attachments_json,
                    &next_sequence,
                    &revision,
                    &updated_at_ms,
                ],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn compare_and_upsert_session_presence_projection(
        &self,
        projection: &SessionPresenceProjection,
        expected_revision: Option<u64>,
    ) -> session::SessionResult<bool> {
        let next_sequence = to_i64(projection.next_sequence, "presence next sequence")?;
        let revision = to_u64_i64(projection.revision, "presence revision")?;
        let updated_at_ms = to_u64_i64(projection.updated_at_ms, "presence updated time")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let changed = match expected_revision {
            Some(expected_revision) => {
                let expected_revision =
                    to_u64_i64(expected_revision, "presence expected revision")?;
                connection.execute(
                    "UPDATE session_presence_projection
                        SET state=$2,
                            attachments_json=$3::text::jsonb,
                            next_sequence=$4,
                            revision=$5,
                            updated_at_ms=$6
                      WHERE session_id=$1 AND revision=$7",
                    &[
                        &projection.session_id,
                        &projection.state,
                        &projection.attachments_json,
                        &next_sequence,
                        &revision,
                        &updated_at_ms,
                        &expected_revision,
                    ],
                )
            }
            None => connection.execute(
                "INSERT INTO session_presence_projection(
                     session_id,state,attachments_json,next_sequence,revision,updated_at_ms
                 ) VALUES ($1,$2,$3::text::jsonb,$4,$5,$6)
                 ON CONFLICT(session_id) DO NOTHING",
                &[
                    &projection.session_id,
                    &projection.state,
                    &projection.attachments_json,
                    &next_sequence,
                    &revision,
                    &updated_at_ms,
                ],
            ),
        }
        .map_err(postgres_error)?;
        Ok(changed == 1)
    }

    pub fn delete_session_presence_projection(
        &self,
        session_id: &str,
    ) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "DELETE FROM session_presence_projection WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn get_session_recovery_manifests_by_ids(
        &self,
        session_ids: &[String],
    ) -> session::SessionResult<Vec<SessionRecoveryManifest>> {
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT session_id, durable_cursor, event_cursor, history_revision,
                        transcript_messages, transcript_bytes,
                        latest_checkpoint_sequence, latest_checkpoint_event_id,
                        index_generation, indexed_through_sequence, index_card_count,
                        index_pending, in_flight_turn, pending_approval,
                        active_writer_or_attachment, mission_agent_team_continuation,
                        last_activity_ms, manifest_revision
                   FROM session_recovery_manifest
                  WHERE session_id = ANY($1)
                  ORDER BY session_id ASC",
                &[&session_ids],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_recovery_manifest)
            .collect()
    }

    pub fn rebuild_session_recovery_manifest(
        &self,
        session_id: &str,
        now_ms: u64,
    ) -> session::SessionResult<Option<SessionRecoveryManifest>> {
        let mut connection = self.executor.checkout_background().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let exists = transaction
            .query_one(
                "SELECT EXISTS(
                     SELECT 1 FROM session_records WHERE session_id=$1
                 )",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .get::<_, bool>(0);
        if !exists {
            transaction.commit().map_err(postgres_error)?;
            return Ok(None);
        }
        transaction
            .execute(
                "SELECT cowd_refresh_session_recovery_manifest($1, TRUE)",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        let now_ms = to_u64_i64(now_ms, "manifest rebuild time")?;
        transaction
            .execute(
                "UPDATE session_recovery_manifest
                    SET event_cursor=COALESCE((
                            SELECT MAX(sequence)+1 FROM session_events
                             WHERE session_id=$1
                        ),0),
                        latest_checkpoint_sequence=(
                            SELECT MAX(sequence) FROM session_events
                             WHERE session_id=$1
                               AND event_type='SessionDomainEvent'
                               AND event_json::jsonb ->> 'kind'=
                                   'memory.semantic_checkpoint.created'
                        ),
                        latest_checkpoint_event_id=(
                            SELECT event_json::jsonb ->> 'event_id'
                              FROM session_events
                             WHERE session_id=$1
                               AND event_type='SessionDomainEvent'
                               AND event_json::jsonb ->> 'kind'=
                                   'memory.semantic_checkpoint.created'
                             ORDER BY sequence DESC LIMIT 1
                        ),
                        index_generation=COALESCE((
                            SELECT MAX(generation)
                              FROM session_context_index_cards
                             WHERE session_id=$1
                        ),0),
                        indexed_through_sequence=(
                            SELECT MAX(source_end_sequence)
                              FROM session_context_index_cards
                             WHERE session_id=$1
                        ),
                        index_card_count=COALESCE((
                            SELECT COUNT(*) FROM session_context_index_cards
                             WHERE session_id=$1
                        ),0),
                        index_pending=EXISTS(
                            SELECT 1 FROM session_messages WHERE session_id=$1
                        ) OR EXISTS(
                            SELECT 1 FROM session_events
                             WHERE session_id=$1
                               AND event_type='SessionDomainEvent'
                               AND event_json::jsonb ->> 'kind'=
                                   'memory.semantic_checkpoint.created'
                        ),
                        last_activity_ms=GREATEST(last_activity_ms,$2),
                        manifest_revision=manifest_revision+1
                  WHERE session_id=$1",
                &[&session_id, &now_ms],
            )
            .map_err(postgres_error)?;
        transaction
            .execute(
                "INSERT INTO session_context_index_outbox(
                     session_id,source_sequence,operation,status,
                     created_at_ms,updated_at_ms
                 )
                 SELECT $1,0,'reconcile','pending',$2,$2
                  WHERE EXISTS(
                      SELECT 1 FROM session_messages WHERE session_id=$1
                  )
                 ON CONFLICT(session_id,source_sequence,operation) DO UPDATE
                     SET status='pending',
                         updated_at_ms=GREATEST(
                             session_context_index_outbox.updated_at_ms,
                             EXCLUDED.updated_at_ms
                         )",
                &[&session_id, &now_ms],
            )
            .map_err(postgres_error)?;
        transaction.commit().map_err(postgres_error)?;
        self.get_session_recovery_manifest(session_id)
    }

    pub fn list_active_session_recovery_manifests(
        &self,
        offset: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionRecoveryManifest>> {
        let offset = to_i64(offset, "recovery manifest offset")?;
        let limit = to_i64(limit.max(1), "recovery manifest limit")?;
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT manifest.session_id, manifest.durable_cursor,
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
                   JOIN session_records AS record
                     ON record.session_id=manifest.session_id
                  WHERE record.status='active'
                  ORDER BY manifest.last_activity_ms DESC, manifest.session_id ASC
                  LIMIT $1 OFFSET $2",
                &[&limit, &offset],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_recovery_manifest)
            .collect()
    }

    pub fn list_required_session_recovery_manifests(
        &self,
        offset: usize,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionRecoveryManifest>> {
        let offset = to_i64(offset, "required recovery manifest offset")?;
        let limit = to_i64(limit.max(1), "required recovery manifest limit")?;
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT manifest.session_id, manifest.durable_cursor,
                        manifest.event_cursor, manifest.history_revision,
                        manifest.transcript_messages, manifest.transcript_bytes,
                        manifest.latest_checkpoint_sequence,
                        manifest.latest_checkpoint_event_id,
                        manifest.index_generation,
                        manifest.indexed_through_sequence,
                        manifest.index_card_count,
                        manifest.index_pending,
                        manifest.in_flight_turn, manifest.pending_approval,
                        manifest.active_writer_or_attachment,
                        manifest.mission_agent_team_continuation,
                        manifest.last_activity_ms, manifest.manifest_revision
                   FROM session_recovery_manifest AS manifest
                   JOIN session_records AS record
                     ON record.session_id=manifest.session_id
                  WHERE record.status='active'
                    AND (
                        manifest.in_flight_turn
                        OR manifest.pending_approval
                        OR manifest.mission_agent_team_continuation
                    )
                  ORDER BY manifest.last_activity_ms DESC, manifest.session_id ASC
                  LIMIT $1 OFFSET $2",
                &[&limit, &offset],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_recovery_manifest)
            .collect()
    }

    pub fn set_session_recovery_signal(
        &self,
        session_id: &str,
        signal: SessionRecoverySignal,
        active: bool,
        observed_at_ms: u64,
    ) -> session::SessionResult<SessionRecoveryManifest> {
        let column = match signal {
            SessionRecoverySignal::PendingApproval => "pending_approval",
            SessionRecoverySignal::ActiveWriterOrAttachment => "active_writer_or_attachment",
            SessionRecoverySignal::MissionAgentTeamContinuation => {
                "mission_agent_team_continuation"
            }
        };
        let observed_at_ms = to_u64_i64(observed_at_ms, "recovery observed_at_ms")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let statement = format!(
            "UPDATE session_recovery_manifest
                SET {column}=$2,
                    last_activity_ms=GREATEST(last_activity_ms, $3),
                    manifest_revision=manifest_revision + 1
              WHERE session_id=$1
          RETURNING session_id, durable_cursor, event_cursor, history_revision,
                    transcript_messages, transcript_bytes,
                    latest_checkpoint_sequence, latest_checkpoint_event_id,
                    index_generation, indexed_through_sequence, index_card_count,
                    index_pending, in_flight_turn, pending_approval,
                    active_writer_or_attachment, mission_agent_team_continuation,
                    last_activity_ms, manifest_revision"
        );
        connection
            .query_opt(&statement, &[&session_id, &active, &observed_at_ms])
            .map_err(postgres_error)?
            .map(|row| row_to_recovery_manifest(&row))
            .transpose()?
            .ok_or_else(|| {
                session::SessionError::Store(format!(
                    "session recovery manifest `{session_id}` does not exist"
                ))
            })
    }

    pub fn update_session(&self, session: &SessionRecord) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "UPDATE session_records SET
                    platform=$2, chat_id=$3, user_id=$4, model=$5, created_at=$6,
                    last_activity=$7, message_count=$8, reset_policy=$9, metadata_json=$10,
                    input_tokens=$11, output_tokens=$12, status=$13,
                    created_at_ms=cowd_safe_session_epoch_ms($6),
                    updated_at_ms=cowd_safe_session_epoch_ms($7)
                 WHERE session_id=$1",
                &session_params(session),
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn upsert_session(&self, session: &SessionRecord) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "INSERT INTO session_records(
                    session_id, platform, chat_id, user_id, model, created_at,
                    last_activity, message_count, reset_policy, metadata_json,
                    input_tokens, output_tokens, status,
                    created_at_ms, updated_at_ms
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
                    cowd_safe_session_epoch_ms($6), cowd_safe_session_epoch_ms($7))
                 ON CONFLICT(session_id) DO UPDATE SET
                    platform=EXCLUDED.platform, chat_id=EXCLUDED.chat_id,
                    user_id=EXCLUDED.user_id, model=EXCLUDED.model,
                    created_at=EXCLUDED.created_at, last_activity=EXCLUDED.last_activity,
                    message_count=EXCLUDED.message_count, reset_policy=EXCLUDED.reset_policy,
                    metadata_json=EXCLUDED.metadata_json, input_tokens=EXCLUDED.input_tokens,
                    output_tokens=EXCLUDED.output_tokens, status=EXCLUDED.status,
                    created_at_ms=EXCLUDED.created_at_ms,
                    updated_at_ms=EXCLUDED.updated_at_ms",
                &session_params(session),
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn delete_session(&self, session_id: &str) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "DELETE FROM session_records WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn mark_session_closed(&self, session_id: &str) -> session::SessionResult<()> {
        let now_at = chrono::Utc::now();
        let now = now_at.to_rfc3339();
        let now_ms = now_at.timestamp_millis().max(0);
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "UPDATE session_records
                    SET status='closed', last_activity=$1,
                        updated_at_ms=GREATEST(updated_at_ms, $2)
                  WHERE session_id=$3",
                &[&now, &now_ms, &session_id],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn list_sessions(&self) -> session::SessionResult<Vec<SessionRecord>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT session_id, platform, chat_id, user_id, model, created_at,
                        last_activity, message_count, reset_policy, metadata_json,
                        input_tokens, output_tokens, status
                   FROM session_records ORDER BY last_activity DESC, session_id ASC",
                &[],
            )
            .map_err(postgres_error)?
            .iter()
            .map(row_to_session)
            .collect()
    }

    pub fn list_sessions_by_platform(
        &self,
        platform: &str,
    ) -> session::SessionResult<Vec<SessionRecord>> {
        self.query_sessions(
            "SELECT session_id, platform, chat_id, user_id, model, created_at,
                    last_activity, message_count, reset_policy, metadata_json,
                    input_tokens, output_tokens, status
               FROM session_records WHERE platform=$1
               ORDER BY last_activity DESC, session_id ASC",
            &[&platform],
        )
    }

    pub fn list_sessions_by_workspace_root(
        &self,
        workspace_root: &str,
    ) -> session::SessionResult<Vec<SessionRecord>> {
        self.query_sessions(
            "SELECT session_id, platform, chat_id, user_id, model, created_at,
                    last_activity, message_count, reset_policy, metadata_json,
                    input_tokens, output_tokens, status
               FROM session_records
              WHERE metadata_json IS NOT NULL
                AND metadata_json::jsonb ->> 'workspace_root' = $1
              ORDER BY last_activity DESC, session_id ASC",
            &[&workspace_root],
        )
    }

    pub fn list_sessions_page(
        &self,
        options: &SessionListOptions<'_>,
    ) -> session::SessionResult<SessionListPage> {
        let sort = match options.sort {
            "created_at" => "created_at",
            "message_count" => "message_count",
            "model" => "COALESCE(model, '')",
            "title" => "COALESCE(metadata_json::jsonb ->> 'title', '')",
            _ => "last_activity",
        };
        let order = if options.order.eq_ignore_ascii_case("asc") {
            "ASC"
        } else {
            "DESC"
        };
        let query = options.query.filter(|value| !value.trim().is_empty());
        let status = options.status.filter(|value| !value.trim().is_empty());
        let model = options.model.filter(|value| !value.trim().is_empty());
        let owner_principal_id = options
            .owner_principal_id
            .filter(|value| !value.trim().is_empty());
        let visible_session_ids = options.visible_session_ids;
        let unrestricted = options.unrestricted;
        let include_deleted = options.include_deleted;
        let limit = i64::try_from(bounded_limit(options.limit, 1, 500))
            .map_err(|_| session::SessionError::Store("session page limit overflow".to_string()))?;
        let offset = i64::try_from(options.offset).map_err(|_| {
            session::SessionError::Store("session page offset overflow".to_string())
        })?;
        let where_clause = "WHERE ($1::text IS NULL OR to_tsvector('simple',
                coalesce(platform, '') || ' ' || coalesce(chat_id, '') || ' ' ||
                coalesce(user_id, '') || ' ' || coalesce(metadata_json, ''))
                @@ websearch_to_tsquery('simple', $1)
                OR platform ILIKE '%' || $1 || '%' OR chat_id ILIKE '%' || $1 || '%')
             AND ($2::text IS NULL OR status = $2)
             AND ($3::text IS NULL OR model = $3)
             AND ($6::boolean
                  OR metadata_json::jsonb ->> 'owner_principal_id' = $4
                  OR session_id = ANY($5::text[]))
             AND ($2::text IS NOT NULL OR $7::boolean
                  OR status NOT IN ('deleted', 'deleting'))";
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let total: i64 = connection
            .query_one(
                &format!("SELECT COUNT(*) FROM session_records {where_clause}"),
                &[
                    &query,
                    &status,
                    &model,
                    &owner_principal_id,
                    &visible_session_ids,
                    &unrestricted,
                    &include_deleted,
                ],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let rows = connection
            .query(
                &format!(
                    "SELECT session_id, platform, chat_id, user_id, model, created_at,
                            last_activity, message_count, reset_policy, metadata_json,
                            input_tokens, output_tokens, status
                       FROM session_records {where_clause}
                      ORDER BY {sort} {order}, session_id ASC LIMIT $8 OFFSET $9"
                ),
                &[
                    &query,
                    &status,
                    &model,
                    &owner_principal_id,
                    &visible_session_ids,
                    &unrestricted,
                    &include_deleted,
                    &limit,
                    &offset,
                ],
            )
            .map_err(postgres_error)?;
        let records = rows
            .iter()
            .map(row_to_session)
            .collect::<session::SessionResult<_>>()?;
        Ok(SessionListPage {
            records,
            total: usize::try_from(total).map_err(|_| {
                session::SessionError::Store("session page count overflow".to_string())
            })?,
        })
    }

    pub fn session_usage_summary(
        &self,
        recent_limit: usize,
    ) -> session::SessionResult<SessionUsageSummary> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let totals = connection
            .query_one(
                "SELECT COUNT(*),COALESCE(SUM(message_count),0)::BIGINT,
                        COALESCE(SUM(input_tokens),0)::BIGINT,
                        COALESCE(SUM(output_tokens),0)::BIGINT
                   FROM session_records
                  WHERE status NOT IN ('deleted','deleting')",
                &[],
            )
            .map_err(postgres_error)?;
        let load_buckets =
            |connection: &mut PostgresConnection,
             column: &str|
             -> session::SessionResult<BTreeMap<String, SessionUsageBucket>> {
                let rows = connection
                    .query(
                        &format!(
                            "SELECT COALESCE(NULLIF(BTRIM({column}),''),'unknown'),COUNT(*),
                                COALESCE(SUM(message_count),0)::BIGINT,
                                COALESCE(SUM(input_tokens),0)::BIGINT,
                                COALESCE(SUM(output_tokens),0)::BIGINT
                           FROM session_records
                          WHERE status NOT IN ('deleted','deleting')
                          GROUP BY 1 ORDER BY 1"
                        ),
                        &[],
                    )
                    .map_err(postgres_error)?;
                rows.iter()
                    .map(|row| {
                        let count = row.try_get::<_, i64>(1).map_err(postgres_error)?;
                        Ok((
                            row.try_get(0).map_err(postgres_error)?,
                            SessionUsageBucket {
                                session_count: usize::try_from(count).map_err(|_| {
                                    session::SessionError::Store(
                                        "usage bucket session count overflow".to_string(),
                                    )
                                })?,
                                message_count: row.try_get(2).map_err(postgres_error)?,
                                input_tokens: row.try_get(3).map_err(postgres_error)?,
                                output_tokens: row.try_get(4).map_err(postgres_error)?,
                            },
                        ))
                    })
                    .collect()
            };
        let session_count_i64 = totals.try_get::<_, i64>(0).map_err(postgres_error)?;
        let by_platform = load_buckets(&mut connection, "platform")?;
        let by_model = load_buckets(&mut connection, "model")?;
        drop(connection);
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
            session_count: usize::try_from(session_count_i64).map_err(|_| {
                session::SessionError::Store("usage session count overflow".to_string())
            })?,
            message_count: totals.try_get(1).map_err(postgres_error)?,
            input_tokens: totals.try_get(2).map_err(postgres_error)?,
            output_tokens: totals.try_get(3).map_err(postgres_error)?,
            by_platform,
            by_model,
            recent_sessions,
        })
    }

    pub fn discover_browsable_sessions(
        &self,
        current_session_id: &str,
        query: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> session::SessionResult<SessionListPage> {
        let query = query.map(str::trim).filter(|query| !query.is_empty());
        let limit = i64::try_from(bounded_limit(limit, 1, 100)).map_err(|_| {
            session::SessionError::Store("Session discovery limit overflow".to_string())
        })?;
        let offset = i64::try_from(offset).map_err(|_| {
            session::SessionError::Store("Session discovery offset overflow".to_string())
        })?;
        let authority_clause = r"
            FROM session_records s
            JOIN session_records current ON current.session_id=$1
           WHERE s.status NOT IN ('deleted', 'deleting')
             AND (
                    s.session_id=current.session_id
                 OR (
                        NULLIF(current.metadata_json::jsonb ->> 'workspace_root', '') IS NOT NULL
                    AND s.metadata_json::jsonb ->> 'workspace_root'
                        = current.metadata_json::jsonb ->> 'workspace_root'
                    AND (
                           (
                               NULLIF(current.metadata_json::jsonb ->> 'owner_principal_id', '') IS NOT NULL
                           AND s.metadata_json::jsonb ->> 'owner_principal_id'
                               = current.metadata_json::jsonb ->> 'owner_principal_id'
                           )
                        OR (
                               NULLIF(current.metadata_json::jsonb ->> 'owner_principal_id', '') IS NULL
                           AND NULLIF(current.user_id, '') IS NOT NULL
                           AND s.platform=current.platform
                           AND s.user_id=current.user_id
                           )
                       )
                    )
                 )
             AND (
                    $2::text IS NULL
                 OR to_tsvector('simple',
                        coalesce(s.session_id, '') || ' ' || coalesce(s.platform, '') || ' ' ||
                        coalesce(s.chat_id, '') || ' ' || coalesce(s.metadata_json, ''))
                    @@ websearch_to_tsquery('simple', $2)
                 OR s.session_id ILIKE '%' || $2 || '%'
                 OR s.platform ILIKE '%' || $2 || '%'
                 OR s.chat_id ILIKE '%' || $2 || '%'
                 OR coalesce(s.metadata_json, '') ILIKE '%' || $2 || '%'
                 OR EXISTS (
                        SELECT 1
                          FROM session_messages m
                         WHERE m.session_id=s.session_id
                           AND to_tsvector('simple',
                               coalesce(m.role, '') || ' ' || coalesce(m.content_json, '') || ' ' ||
                               coalesce(m.tool_name, ''))
                               @@ websearch_to_tsquery('simple', $2)
                    )
                 )";
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let total: i64 = connection
            .query_one(
                &format!("SELECT COUNT(*) {authority_clause}"),
                &[&current_session_id, &query],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let rows = connection
            .query(
                &format!(
                    r"SELECT s.session_id, s.platform, s.chat_id, s.user_id, s.model,
                              s.created_at, s.last_activity, s.message_count, s.reset_policy,
                              s.metadata_json, s.input_tokens, s.output_tokens,
                              s.status
                         {authority_clause}
                        ORDER BY s.last_activity DESC, s.session_id ASC
                        LIMIT $3 OFFSET $4"
                ),
                &[&current_session_id, &query, &limit, &offset],
            )
            .map_err(postgres_error)?;
        let records = rows
            .iter()
            .map(row_to_session)
            .collect::<session::SessionResult<Vec<_>>>()?;
        Ok(SessionListPage {
            records,
            total: usize::try_from(total).map_err(|_| {
                session::SessionError::Store("Session discovery count overflow".to_string())
            })?,
        })
    }

    pub fn search_sessions(
        &self,
        query: &str,
        platform: Option<&str>,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionSearchResult>> {
        let limit = i64::try_from(bounded_limit(limit, 1, 500)).map_err(|_| {
            session::SessionError::Store("session search limit overflow".to_string())
        })?;
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let rows = connection
            .query(
                "SELECT session_id, platform, chat_id, user_id, created_at, last_activity,
                        message_count, null::text
                   FROM session_records
                  WHERE ($2::text IS NULL OR platform=$2)
                    AND (to_tsvector('simple', coalesce(platform, '') || ' ' ||
                         coalesce(chat_id, '') || ' ' || coalesce(user_id, '') || ' ' ||
                         coalesce(metadata_json, '')) @@ websearch_to_tsquery('simple', $1)
                         OR platform ILIKE '%' || $1 || '%' OR chat_id ILIKE '%' || $1 || '%')
                  ORDER BY last_activity DESC, session_id ASC LIMIT $3",
                &[&query, &platform, &limit],
            )
            .map_err(postgres_error)?;
        rows.iter().map(row_to_session_search).collect()
    }

    pub fn associate_memory(
        &self,
        session_id: &str,
        memory_id: &str,
    ) -> session::SessionResult<()> {
        let created_at = chrono::Utc::now().to_rfc3339();
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "INSERT INTO session_memory_associations(session_id, memory_id, created_at)
                 VALUES ($1,$2,$3) ON CONFLICT(session_id, memory_id) DO NOTHING",
                &[&session_id, &memory_id, &created_at],
            )
            .map_err(postgres_error)?;
        Ok(())
    }

    pub fn get_session_memories(&self, session_id: &str) -> session::SessionResult<Vec<String>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query(
                "SELECT memory_id FROM session_memory_associations
                 WHERE session_id=$1 ORDER BY memory_id ASC",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .iter()
            .map(|row| row.try_get(0).map_err(postgres_error))
            .collect()
    }

    pub fn disassociate_memory(
        &self,
        session_id: &str,
        memory_id: &str,
    ) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        connection
            .execute(
                "DELETE FROM session_memory_associations WHERE session_id=$1 AND memory_id=$2",
                &[&session_id, &memory_id],
            )
            .map_err(postgres_error)?;
        Ok(())
    }
}
