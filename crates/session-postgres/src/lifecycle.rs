//! Lifecycle operations for the PostgresSessionStore adapter.

use super::*;

fn protect_receipt_attachments(
    previous: Option<&SessionPresenceProjection>,
    candidate: Option<&SessionPresenceProjection>,
) -> session::SessionResult<()> {
    let tagged = |projection: Option<&SessionPresenceProjection>| -> session::SessionResult<Vec<session::SessionAttachment>> {
        Ok(projection.map(receipt_attachments).transpose()?.unwrap_or_default().into_iter().filter(|attachment|
            attachment.receipt_protocol.is_some() || attachment.client_id.is_some()
                || attachment.client_expires_at_ms.is_some() || attachment.receipt_source.is_some()).collect())
    };
    if let (Some(previous), Some(candidate)) = (previous, candidate) {
        if !tagged(Some(previous))?.is_empty()
            && previous.revision.checked_add(1) != Some(candidate.revision)
        {
            return Err(receipt_conflict());
        }
    }
    let mut previous = tagged(previous)?;
    let mut candidate = tagged(candidate)?;
    previous.sort_by(|left, right| left.actor.id.cmp(&right.actor.id));
    candidate.sort_by(|left, right| left.actor.id.cmp(&right.actor.id));
    if previous != candidate {
        return Err(receipt_conflict());
    }
    Ok(())
}

fn write_legacy_presence_tx(
    transaction: &mut PostgresTransaction<'_>,
    projection: &SessionPresenceProjection,
    expected_revision: Option<u64>,
) -> session::SessionResult<u64> {
    transaction.execute(
        "INSERT INTO session_presence_projection(session_id,state,attachments_json,next_sequence,revision,updated_at_ms)
         VALUES($1,$2,$3::text::jsonb,$4,$5,$6) ON CONFLICT(session_id) DO UPDATE SET
         state=EXCLUDED.state,attachments_json=EXCLUDED.attachments_json,next_sequence=EXCLUDED.next_sequence,
         revision=EXCLUDED.revision,updated_at_ms=EXCLUDED.updated_at_ms
         WHERE session_presence_projection.revision=$7",
        &[&projection.session_id,&projection.state,&projection.attachments_json,&to_i64(projection.next_sequence,"presence sequence")?,
            &to_u64_i64(projection.revision,"presence revision")?,&to_u64_i64(projection.updated_at_ms,"presence time")?,
            &expected_revision.map(|revision| to_u64_i64(revision,"presence expected revision")).transpose()?],
    ).map_err(postgres_error)
}

pub(super) fn receipt_conflict() -> session::SessionError {
    session::SessionError::StaleExecutionFence(
        "receipt attachment or binding is no longer eligible".into(),
    )
}

pub(super) fn receipt_clock_tx(
    transaction: &mut PostgresTransaction<'_>,
) -> session::SessionResult<u64> {
    let row = transaction
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
            &[],
        )
        .map_err(postgres_error)?;
    i64_to_u64(row.try_get(0).map_err(postgres_error)?, "receipt time")
}

pub(super) fn receipt_parent_tx(
    transaction: &mut PostgresTransaction<'_>,
    session_id: &str,
) -> session::SessionResult<()> {
    transaction
        .query_opt(
            "SELECT session_id FROM session_records WHERE session_id=$1 FOR KEY SHARE",
            &[&session_id],
        )
        .map_err(postgres_error)?
        .ok_or_else(|| session::SessionError::NotFound("session".into()))?;
    Ok(())
}

pub(super) fn receipt_presence_tx(
    transaction: &mut PostgresTransaction<'_>,
    session_id: &str,
    lock: bool,
) -> session::SessionResult<Option<SessionPresenceProjection>> {
    let sql = if lock {
        "SELECT session_id,state,attachments_json::text,next_sequence,revision,updated_at_ms FROM session_presence_projection WHERE session_id=$1 FOR UPDATE"
    } else {
        "SELECT session_id,state,attachments_json::text,next_sequence,revision,updated_at_ms FROM session_presence_projection WHERE session_id=$1"
    };
    transaction
        .query_opt(sql, &[&session_id])
        .map_err(postgres_error)?
        .map(|row| {
            Ok(SessionPresenceProjection {
                session_id: row.try_get(0).map_err(postgres_error)?,
                state: row.try_get(1).map_err(postgres_error)?,
                attachments_json: row.try_get(2).map_err(postgres_error)?,
                next_sequence: from_i64(
                    row.try_get(3).map_err(postgres_error)?,
                    "presence sequence",
                )?,
                revision: i64_to_u64(row.try_get(4).map_err(postgres_error)?, "presence revision")?,
                updated_at_ms: i64_to_u64(
                    row.try_get(5).map_err(postgres_error)?,
                    "presence time",
                )?,
            })
        })
        .transpose()
}

pub(super) fn receipt_attachments(
    projection: &SessionPresenceProjection,
) -> session::SessionResult<Vec<session::SessionAttachment>> {
    serde_json::from_str(&projection.attachments_json)
        .map_err(|_| session::SessionError::Store("invalid durable presence".into()))
}

pub(super) fn receipt_attachment_current(
    projection: &SessionPresenceProjection,
    authority: &session::SessionReceiptAuthority,
    now: u64,
) -> session::SessionResult<session::SessionAttachment> {
    if authority
        .credential_expires_at_ms
        .is_some_and(|deadline| now >= deadline)
        || authority.principal_id.is_empty()
        || authority.client_id.is_empty()
        || !authority
            .actor_id
            .starts_with(&format!("principal:{}:surface:", authority.principal_id))
    {
        return Err(receipt_conflict());
    }
    let attachments = receipt_attachments(projection)?;
    let mut selected = attachments
        .into_iter()
        .filter(|attachment| attachment.actor.id == authority.actor_id);
    let attachment = selected.next().ok_or_else(receipt_conflict)?;
    if selected.next().is_some()
        || attachment.session_id != authority.session_id
        || attachment.receipt_protocol.as_deref()
            != Some(session::SESSION_TERMINAL_PUBLIC_PAYLOAD_KIND_V1)
        || attachment.client_id.as_deref() != Some(authority.client_id.as_str())
        || attachment
            .client_expires_at_ms
            .is_none_or(|deadline| now >= deadline)
        || attachment.receipt_source != authority.receipt_source
    {
        return Err(receipt_conflict());
    }
    if let Some(source) = &attachment.receipt_source {
        validate_receipt_source(source, authority, now)?;
        if source.principal_id != authority.principal_id
            || source.session_id != authority.session_id
            || source.expires_at_ms <= now
            || source.purpose != "terminal_receipt"
            || source.frame_nonce.is_empty()
            || source.transport_id.is_empty()
            || source.execution_id.is_empty()
            || source.turn_id.is_empty()
        {
            return Err(receipt_conflict());
        }
    }
    Ok(attachment)
}

pub(super) fn validate_receipt_source(
    source: &session::SessionReceiptSource,
    authority: &session::SessionReceiptAuthority,
    now: u64,
) -> session::SessionResult<()> {
    if source.principal_id != authority.principal_id
        || source.session_id != authority.session_id
        || source.expires_at_ms <= now
        || source.purpose != "terminal_receipt"
        || [
            &source.tenant_id,
            &source.workspace_id,
            &source.app_id,
            &source.frame_nonce,
            &source.transport_id,
            &source.turn_id,
            &source.execution_id,
            &source.root_task_id,
        ]
        .iter()
        .any(|value| value.is_empty())
    {
        return Err(receipt_conflict());
    }
    let digest = source
        .app_generation
        .strip_prefix("sha256:")
        .ok_or_else(receipt_conflict)?;
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(receipt_conflict());
    }
    for value in [
        &source.profile_revision,
        &source.credential_epoch,
        &source.attachment_revision,
        &source.lineage_generation,
    ] {
        harness_contract::live::ReceiptCursor::parse(value).map_err(|_| receipt_conflict())?;
    }
    Ok(())
}

impl PostgresSessionStore {
    pub fn mutate_session_presence_if_revision(
        &self,
        command: &session::SessionPresenceMutation,
    ) -> session::SessionResult<Option<session::SessionPresenceMutationResult>> {
        let authority = &command.authority;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        receipt_parent_tx(&mut transaction, &authority.session_id)?;
        let previous = receipt_presence_tx(&mut transaction, &authority.session_id, true)?;
        if previous.as_ref().map(|projection| projection.revision) != command.expected_revision {
            return Ok(None);
        }
        let now = receipt_clock_tx(&mut transaction)?;
        let mut attachments = previous
            .as_ref()
            .map(receipt_attachments)
            .transpose()?
            .unwrap_or_default();
        let existing = attachments
            .iter()
            .find(|attachment| attachment.actor.id == authority.actor_id)
            .cloned();
        let mut selected = None;
        match &command.kind {
            session::SessionPresenceMutationKind::AttachOrReaffirm {
                actor,
                candidate_client_id,
                ttl_ms,
            } => {
                if let Some(existing) = &existing {
                    let has_tag = existing.client_id.is_some()
                        || existing.receipt_protocol.is_some()
                        || existing.client_expires_at_ms.is_some()
                        || existing.receipt_source.is_some();
                    if has_tag
                        && (existing.client_id.as_ref().is_none_or(String::is_empty)
                            || existing.receipt_protocol.as_deref()
                                != Some(session::SESSION_TERMINAL_PUBLIC_PAYLOAD_KIND_V1)
                            || existing.client_expires_at_ms.is_none())
                    {
                        return Err(receipt_conflict());
                    }
                }
                if actor.id != authority.actor_id
                    || authority.principal_id.is_empty()
                    || *ttl_ms == 0
                    || !actor
                        .id
                        .starts_with(&format!("principal:{}:surface:", authority.principal_id))
                    || actor.surface.is_empty()
                    || !matches!(actor.role.as_deref(), Some("reader" | "writer"))
                {
                    return Err(receipt_conflict());
                }
                let active = existing.as_ref().filter(|attachment| {
                    attachment.receipt_protocol.is_some()
                        && attachment
                            .client_expires_at_ms
                            .is_none_or(|deadline| now < deadline)
                });
                let client_id = if let Some(existing) = active {
                    let projection = previous.as_ref().ok_or_else(receipt_conflict)?;
                    receipt_attachment_current(projection, authority, now)?;
                    if existing.actor != *actor {
                        return Err(receipt_conflict());
                    }
                    authority.client_id.clone()
                } else {
                    if !authority.client_id.is_empty()
                        || candidate_client_id.len() != 36
                        || !candidate_client_id
                            .bytes()
                            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
                    {
                        return Err(receipt_conflict());
                    }
                    candidate_client_id.clone()
                };
                let mut deadline = now.checked_add(*ttl_ms).ok_or_else(receipt_conflict)?;
                if let Some(expiry) = authority.credential_expires_at_ms {
                    deadline = deadline.min(expiry);
                }
                if let Some(source) = &authority.receipt_source {
                    validate_receipt_source(source, authority, now)?;
                    if source.principal_id != authority.principal_id
                        || source.session_id != authority.session_id
                    {
                        return Err(receipt_conflict());
                    }
                    deadline = deadline.min(source.expires_at_ms);
                }
                if deadline <= now {
                    return Err(receipt_conflict());
                }
                let attachment = session::SessionAttachment {
                    session_id: authority.session_id.clone(),
                    actor: actor.clone(),
                    attached_at_ms: active.map_or(now, |attachment| attachment.attached_at_ms),
                    last_seen_ms: now,
                    client_id: Some(client_id),
                    client_expires_at_ms: Some(deadline),
                    receipt_protocol: Some(session::SESSION_TERMINAL_PUBLIC_PAYLOAD_KIND_V1.into()),
                    receipt_source: authority.receipt_source.clone(),
                };
                attachments.retain(|attachment| attachment.actor.id != authority.actor_id);
                attachments.push(attachment.clone());
                selected = Some(attachment);
            }
            session::SessionPresenceMutationKind::Detach => {
                receipt_attachment_current(
                    previous.as_ref().ok_or_else(receipt_conflict)?,
                    authority,
                    now,
                )?;
                attachments.retain(|attachment| attachment.actor.id != authority.actor_id);
            }
            session::SessionPresenceMutationKind::Expire => {
                let existing = existing.ok_or_else(receipt_conflict)?;
                if existing
                    .client_expires_at_ms
                    .is_none_or(|deadline| now < deadline)
                {
                    return Err(receipt_conflict());
                }
                attachments.retain(|attachment| attachment.actor.id != authority.actor_id);
            }
            session::SessionPresenceMutationKind::RevokeExactActor => {
                if existing.is_none() || authority.actor_id.is_empty() {
                    return Err(receipt_conflict());
                }
                attachments.retain(|attachment| attachment.actor.id != authority.actor_id);
            }
        }
        let revision = command
            .expected_revision
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(receipt_conflict)?;
        let projection = SessionPresenceProjection {
            session_id: authority.session_id.clone(),
            state: if attachments.is_empty() {
                "detached"
            } else {
                "attached"
            }
            .into(),
            attachments_json: serde_json::to_string(&attachments)?,
            next_sequence: previous
                .as_ref()
                .map_or(0, |projection| projection.next_sequence)
                .checked_add(1)
                .ok_or_else(receipt_conflict)?,
            revision,
            updated_at_ms: now,
        };
        let changed = if previous.is_some() {
            transaction.execute("UPDATE session_presence_projection SET state=$2,attachments_json=$3::text::jsonb,next_sequence=$4,revision=$5,updated_at_ms=$6 WHERE session_id=$1",
                &[&projection.session_id,&projection.state,&projection.attachments_json,&to_i64(projection.next_sequence,"presence sequence")?,&to_u64_i64(revision,"presence revision")?,&to_u64_i64(now,"presence time")?])
        } else {
            transaction.execute("INSERT INTO session_presence_projection(session_id,state,attachments_json,next_sequence,revision,updated_at_ms) VALUES($1,$2,$3::text::jsonb,$4,$5,$6) ON CONFLICT DO NOTHING",
                &[&projection.session_id,&projection.state,&projection.attachments_json,&to_i64(projection.next_sequence,"presence sequence")?,&to_u64_i64(revision,"presence revision")?,&to_u64_i64(now,"presence time")?])
        }.map_err(postgres_error)?;
        if changed != 1 {
            return Ok(None);
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(Some(session::SessionPresenceMutationResult {
            projection,
            attachment: selected,
        }))
    }
}

/// Canonical current actor/workspace predicate shared by catalog and active search.
pub(super) const BROWSABLE_SESSION_AUTHORITY: &str = r"(
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
                 )";

impl PostgresSessionStore {
    pub fn new(executor: PostgresExecutor) -> session::SessionResult<Self> {
        validate_current_session_catalog(&executor)?;
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
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        receipt_parent_tx(&mut transaction, &projection.session_id)?;
        let current = receipt_presence_tx(&mut transaction, &projection.session_id, true)?;
        protect_receipt_attachments(current.as_ref(), Some(projection))?;
        if write_legacy_presence_tx(
            &mut transaction,
            projection,
            current.as_ref().map(|projection| projection.revision),
        )? != 1
        {
            return Err(receipt_conflict());
        }
        transaction.commit().map_err(postgres_error)
    }

    pub fn compare_and_upsert_session_presence_projection(
        &self,
        projection: &SessionPresenceProjection,
        expected_revision: Option<u64>,
    ) -> session::SessionResult<bool> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        receipt_parent_tx(&mut transaction, &projection.session_id)?;
        let current = receipt_presence_tx(&mut transaction, &projection.session_id, true)?;
        if current.as_ref().map(|projection| projection.revision) != expected_revision {
            return Ok(false);
        }
        protect_receipt_attachments(current.as_ref(), Some(projection))?;
        let changed = write_legacy_presence_tx(&mut transaction, projection, expected_revision)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(changed == 1)
    }

    pub fn delete_session_presence_projection(
        &self,
        session_id: &str,
    ) -> session::SessionResult<()> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        match receipt_parent_tx(&mut transaction, session_id) {
            Ok(()) => {}
            Err(session::SessionError::NotFound(_)) => return Ok(()),
            Err(error) => return Err(error),
        }
        let current = receipt_presence_tx(&mut transaction, session_id, true)?;
        protect_receipt_attachments(current.as_ref(), None)?;
        transaction
            .execute(
                "DELETE FROM session_presence_projection WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        transaction.commit().map_err(postgres_error)
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
        let query = options
            .query
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let status = options
            .status
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let model = options
            .model
            .map(str::trim)
            .filter(|value| !value.is_empty());
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
        // Listing is a literal substring filter, not the separately exposed
        // full-text search language. strpos preserves %, _ and backslashes
        // without SQL wildcard or tsquery reinterpretation.
        let where_clause = "WHERE ($1::text IS NULL
                OR strpos(lower(session_id), lower($1)) > 0
                OR strpos(lower(platform), lower($1)) > 0
                OR strpos(lower(chat_id), lower($1)) > 0
                OR strpos(lower(coalesce(user_id, '')), lower($1)) > 0
                OR strpos(lower(coalesce(model, '')), lower($1)) > 0
                OR strpos(lower(status), lower($1)) > 0
                OR strpos(lower(coalesce(metadata_json, '')), lower($1)) > 0)
             AND ($2::text IS NULL OR lower(status) = lower($2))
             AND ($3::text IS NULL OR lower(model) = lower($3))
             AND ($6::boolean
                  OR metadata_json::jsonb ->> 'owner_principal_id' = $4
                  OR session_id = ANY($5::text[]))
             AND ($2::text IS NOT NULL OR $7::boolean
                  OR lower(status) NOT IN ('deleted', 'deleting'))";
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
             AND $BROWSABLE_AUTHORITY
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
                 )"
        .replace("$BROWSABLE_AUTHORITY", BROWSABLE_SESSION_AUTHORITY);
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
