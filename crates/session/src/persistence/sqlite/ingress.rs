//! Ingress operations for the SqliteSessionStore adapter.

use super::*;

impl SqliteSessionStore {
    /// Atomically persist a stable user message and its Runtime ingress outbox row.
    ///
    /// Reusing `request_id` with identical identities is idempotent. Reusing it
    /// for another turn/message is rejected rather than silently overwriting data.
    pub fn append_message_with_runtime_outbox(
        &self,
        message: &SessionMessage,
        request: &SessionRuntimeOutboxRequest,
    ) -> Result<SessionRuntimeOutboxRecord> {
        validate_outbox_identity(message, request)?;
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;

        if let Some(existing) = query_outbox(&tx, &request.request_id)? {
            if existing.input_id == request.input_id
                && existing.turn_id == request.turn_id
                && existing.message_id == request.message_id
                && existing.session_id == message.session_id
                && existing.sequence == message.sequence
                && existing.session_generation == request.session_generation
                && existing.decision == request.decision
                && existing.target_turn_id == request.target_turn_id
            {
                tx.commit().map_err(sql_err)?;
                return Ok(existing);
            }
            return Err(SessionError::Store(format!(
                "outbox request_id `{}` is already bound to another message",
                request.request_id
            )));
        }
        require_input_admission(&tx, &message.session_id, request.session_generation)?;

        tx.execute(
            r"INSERT INTO messages
                (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                 tool_use_id, tool_name, token_usage_json, created_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                request.message_id,
                message.session_id,
                message.sequence as i64,
                message.role,
                message.content_json,
                message.blocks_count as i64,
                message.tool_use_id,
                message.tool_name,
                message.token_usage_json,
                message.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        refresh_session_message_summary_tx(&tx, &message.session_id, message.created_at_ms)?;
        let stored =
            insert_runtime_input_outbox(&tx, &message.session_id, message.sequence, request)?;
        tx.commit().map_err(sql_err)?;
        Ok(stored)
    }

    /// Persist an ingress message and Runtime request while allocating the
    /// session-local message sequence inside the same write transaction.
    ///
    /// Surface and Gateway callers must use this entry point for live input;
    /// accepting a caller-computed sequence would create a race between
    /// concurrent surfaces writing to the same session.
    pub fn append_ingress_with_runtime_outbox(
        &self,
        session_id: &str,
        role: &str,
        content_json: Option<&str>,
        created_at_ms: u64,
        request: &SessionRuntimeOutboxRequest,
    ) -> Result<SessionRuntimeOutboxRecord> {
        validate_runtime_input_request(request)?;
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        if let Some(existing) = query_outbox(&tx, &request.request_id)? {
            if existing.input_id == request.input_id
                && existing.session_id == session_id
                && existing.message_id == request.message_id
                && existing.turn_id == request.turn_id
                && existing.session_generation == request.session_generation
                && existing.decision == request.decision
                && existing.target_turn_id == request.target_turn_id
            {
                tx.commit().map_err(sql_err)?;
                return Ok(existing);
            }
            return Err(SessionError::Store(format!(
                "outbox request `{}` conflicts with its committed ingress",
                request.request_id
            )));
        }
        require_input_admission(&tx, session_id, request.session_generation)?;
        let sequence = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence), -1) + 1 FROM messages WHERE session_id = ?1",
                params![session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)? as usize;
        tx.execute(
            r"INSERT INTO messages
                (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                 created_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6)",
            params![
                request.message_id,
                session_id,
                sequence as i64,
                role,
                content_json.unwrap_or("[]"),
                created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        refresh_session_message_summary_tx(&tx, session_id, created_at_ms)?;
        let stored = insert_runtime_input_outbox(&tx, session_id, sequence, request)?;
        tx.commit().map_err(sql_err)?;
        Ok(stored)
    }

    /// Claim due ingress rows under a renewable lease.
    pub fn claim_session_runtime_outbox(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        if worker_id.trim().is_empty() || lease_ms == 0 || limit == 0 {
            return Err(SessionError::Store(
                "outbox claim requires worker_id, positive lease and positive limit".to_string(),
            ));
        }
        let claim_expires_at_ms = now_ms.saturating_add(lease_ms);
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let candidates = {
            let mut stmt = tx
                .prepare(
                    r"WITH ordered AS (
                           SELECT o.request_id, o.revision, o.status, o.session_id,
                                  o.session_generation, o.sequence, o.next_attempt_at_ms,
                                  o.claim_expires_at_ms,
                                  ROW_NUMBER() OVER (
                                      PARTITION BY o.session_id, o.session_generation
                                      ORDER BY o.sequence ASC, o.request_id ASC
                                  ) AS session_rank
                             FROM session_runtime_outbox o
                             JOIN sessions s ON s.session_id = o.session_id
                            WHERE o.status IN (
                                      'accepted', 'classified', 'queued', 'claimed',
                                      'running', 'reclassified'
                                  )
                              AND o.session_generation = s.input_generation
                              AND s.input_admission_open = 1
                       )
                       SELECT request_id, revision, status, session_id, session_generation
                         FROM ordered candidate
                        WHERE session_rank = 1
                          AND (
                              (status IN ('queued', 'reclassified')
                                  AND next_attempt_at_ms <= ?1)
                              OR (status IN ('claimed', 'running')
                                  AND claim_expires_at_ms <= ?1)
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM session_runtime_outbox held
                               WHERE held.session_id = candidate.session_id
                                 AND held.session_generation = candidate.session_generation
                                 AND held.request_id != candidate.request_id
                                 AND held.status IN ('claimed', 'running')
                                 AND held.claim_expires_at_ms > ?1
                          )
                        ORDER BY next_attempt_at_ms ASC, sequence ASC, request_id ASC
                        LIMIT ?2",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![now_ms as i64, limit as i64], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?.max(0) as u64,
                    ))
                })
                .map_err(sql_err)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_err)?
        };

        let mut claimed = Vec::with_capacity(candidates.len());
        for (request_id, revision, from_status, session_id, session_generation) in candidates {
            let claim_token = uuid::Uuid::new_v4().to_string();
            let changed = tx
                .execute(
                    r"UPDATE session_runtime_outbox
                          SET status = 'claimed',
                              attempts = attempts + 1,
                              claim_owner = ?1,
                              claim_token = ?2,
                              claim_fence_epoch = revision + 1,
                              claim_expires_at_ms = ?3,
                              updated_at_ms = ?4,
                              revision = revision + 1
                        WHERE request_id = ?5 AND revision = ?6
                          AND session_id = ?7 AND session_generation = ?8
                          AND (
                              (status IN ('queued', 'reclassified')
                                  AND next_attempt_at_ms <= ?4)
                              OR (status IN ('claimed', 'running')
                                  AND claim_expires_at_ms <= ?4)
                          )
                          AND EXISTS (
                              SELECT 1 FROM sessions
                               WHERE sessions.session_id = ?7
                                 AND sessions.input_generation = ?8
                                 AND sessions.input_admission_open = 1
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM session_runtime_outbox earlier
                               WHERE earlier.session_id = ?7
                                 AND earlier.session_generation = ?8
                                 AND earlier.sequence < session_runtime_outbox.sequence
                                 AND earlier.status IN (
                                     'accepted', 'classified', 'queued', 'claimed',
                                     'running', 'reclassified'
                                 )
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM session_runtime_outbox held
                               WHERE held.session_id = ?7
                                 AND held.session_generation = ?8
                                 AND held.request_id != ?5
                                 AND held.status IN ('claimed', 'running')
                                 AND held.claim_expires_at_ms > ?4
                          )",
                    params![
                        worker_id,
                        claim_token,
                        claim_expires_at_ms as i64,
                        now_ms as i64,
                        request_id,
                        revision as i64,
                        session_id,
                        session_generation as i64,
                    ],
                )
                .map_err(sql_err)?;
            if changed == 1 {
                let record = query_outbox(&tx, &request_id)?.ok_or_else(|| {
                    SessionError::Store(format!("claimed outbox `{request_id}` disappeared"))
                })?;
                append_outbox_history(
                    &tx,
                    &record,
                    if matches!(from_status.as_str(), "claimed" | "running") {
                        "reclaim"
                    } else {
                        "claim"
                    },
                    Some(worker_id),
                    Some(record.claim_token.as_deref().unwrap_or_default()),
                    &from_status,
                    SessionRuntimeInputStatus::Claimed.as_str(),
                    now_ms,
                )?;
                claimed.push(record);
            }
        }
        tx.commit().map_err(sql_err)?;
        Ok(claimed)
    }

    /// Move a claimed input into Runtime execution. This is a separate fenced
    /// transition so terminal writes can prove that execution actually began.
    #[allow(clippy::too_many_arguments)]
    pub fn mark_session_runtime_outbox_running(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        self.transition_owned_outbox(
            request_id,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[SessionRuntimeInputStatus::Claimed],
            |tx, current| {
                tx.execute(
                    r"UPDATE session_runtime_outbox
                          SET status = 'running', updated_at_ms = ?1,
                              revision = revision + 1
                        WHERE request_id = ?2 AND status = 'claimed'
                          AND session_generation = ?3 AND claim_owner = ?4
                          AND claim_token = ?5 AND revision = ?6",
                    params![
                        now_ms as i64,
                        request_id,
                        session_generation as i64,
                        worker_id,
                        claim_token,
                        expected_revision as i64,
                    ],
                )
                .map_err(sql_err)?;
                Ok(("start", SessionRuntimeInputStatus::Running, current.status))
            },
        )
    }

    /// Persist successful direct delivery to an active Runtime turn. This is
    /// deliberately non-terminal: only the target turn's terminal cursor can
    /// prove that the input affected its committed result.
    #[allow(clippy::too_many_arguments)]
    pub fn attach_session_runtime_outbox(
        &self,
        input_id: &str,
        session_generation: u64,
        expected_revision: u64,
        target_turn_id: &str,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        if input_id.trim().is_empty()
            || target_turn_id.trim().is_empty()
            || actor.trim().is_empty()
            || reason.trim().is_empty()
        {
            return Err(SessionError::Store(
                "Session input attachment requires input, target, actor and reason".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_outbox_by_input_id(&tx, input_id)?
            .ok_or_else(|| SessionError::Store(format!("session input `{input_id}` not found")))?;
        require_input_admission(&tx, &current.session_id, session_generation)?;
        if current.session_generation != session_generation
            || current.revision != expected_revision
            || current.decision != InputRoutingDecision::SupplementCurrentTurn
            || current.target_turn_id.as_deref() != Some(target_turn_id)
            || !matches!(
                current.status,
                SessionRuntimeInputStatus::Accepted
                    | SessionRuntimeInputStatus::Classified
                    | SessionRuntimeInputStatus::Queued
                    | SessionRuntimeInputStatus::Reclassified
            )
        {
            return Err(SessionError::Store(format!(
                "session input `{input_id}` is not attachable at generation {session_generation} revision {expected_revision}"
            )));
        }
        if runtime_turn_is_terminal(&tx, &current.session_id, session_generation, target_turn_id)? {
            return Err(SessionError::StaleExecutionFence(format!(
                "target turn `{target_turn_id}` became terminal before input `{input_id}` attachment"
            )));
        }
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET status='attached', claim_owner=NULL, claim_token=NULL,
                          claim_fence_epoch=NULL, claim_expires_at_ms=NULL,
                          terminal_at_ms=NULL, runtime_commit_cursor=NULL,
                          failure_class=NULL, last_error=NULL,
                          updated_at_ms=?1, revision=revision+1
                    WHERE input_id=?2 AND session_generation=?3 AND revision=?4
                      AND decision='supplement_current_turn'
                      AND target_turn_id=?5
                      AND status IN ('accepted','classified','queued','reclassified')",
                params![
                    now_ms as i64,
                    input_id,
                    session_generation as i64,
                    expected_revision as i64,
                    target_turn_id,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "session input `{input_id}` changed during attachment"
            )));
        }
        let attached = query_outbox_by_input_id(&tx, input_id)?.ok_or_else(|| {
            SessionError::Store(format!("attached Session input `{input_id}` disappeared"))
        })?;
        append_outbox_history(
            &tx,
            &attached,
            "attach",
            Some(actor),
            Some(reason),
            current.status.as_str(),
            SessionRuntimeInputStatus::Attached.as_str(),
            now_ms,
        )?;
        append_input_timeline_event(
            &tx,
            &request_from_outbox(&attached),
            &attached.session_id,
            attached.sequence,
            SessionRuntimeInputStatus::Attached.timeline_event_kind(),
            SessionRuntimeInputStatus::Attached,
            Some(actor),
            Some(reason),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(attached)
    }

    /// Acknowledge a running ingress row. `Attached` records successful
    /// delivery to a target turn without claiming durable application; only
    /// that turn's terminal commit may promote it to `Supplemented`.
    #[allow(clippy::too_many_arguments)]
    pub fn ack_session_runtime_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        acknowledged_status: SessionRuntimeInputStatus,
        runtime_commit_cursor: u64,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        if !matches!(
            acknowledged_status,
            SessionRuntimeInputStatus::Attached
                | SessionRuntimeInputStatus::Completed
                | SessionRuntimeInputStatus::Supplemented
                | SessionRuntimeInputStatus::Cancelled
        ) {
            return Err(SessionError::Store(
                "ack status must be attached, completed, supplemented, or cancelled".to_string(),
            ));
        }
        self.transition_owned_outbox(
            request_id,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[SessionRuntimeInputStatus::Running],
            |tx, current| {
                if acknowledged_status == SessionRuntimeInputStatus::Attached {
                    let target_turn_id = current.target_turn_id.as_deref().ok_or_else(|| {
                        SessionError::Store(format!(
                            "attached acknowledgement for `{request_id}` has no target turn"
                        ))
                    })?;
                    if runtime_turn_is_terminal(
                        tx,
                        &current.session_id,
                        session_generation,
                        target_turn_id,
                    )? {
                        return Err(SessionError::StaleExecutionFence(format!(
                            "target turn `{target_turn_id}` became terminal before input `{request_id}` attachment"
                        )));
                    }
                }
                tx.execute(
                    r"UPDATE session_runtime_outbox
                          SET status = ?1,
                              runtime_commit_cursor = CASE WHEN ?1 = 'attached' THEN NULL ELSE ?2 END,
                              claim_owner = NULL, claim_expires_at_ms = NULL,
                              claim_token = NULL, claim_fence_epoch = NULL,
                              terminal_at_ms = CASE WHEN ?1 = 'attached' THEN NULL ELSE ?3 END,
                              failure_class = NULL, last_error = NULL,
                              updated_at_ms = ?3, revision = revision + 1
                        WHERE request_id = ?4 AND status = 'running'
                          AND session_generation = ?5 AND claim_owner = ?6
                          AND claim_token = ?7 AND revision = ?8",
                    params![
                        acknowledged_status.as_str(),
                        runtime_commit_cursor as i64,
                        now_ms as i64,
                        request_id,
                        session_generation as i64,
                        worker_id,
                        claim_token,
                        expected_revision as i64,
                    ],
                )
                .map_err(sql_err)?;
                Ok(("ack", acknowledged_status, current.status))
            },
        )
    }

    /// Extend a live ingress claim. The revision is advanced so stale workers
    /// can no longer acknowledge or fail work after ownership has moved.
    #[allow(clippy::too_many_arguments)]
    pub fn renew_session_runtime_outbox_lease(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        if lease_ms == 0 {
            return Err(SessionError::Store(
                "outbox lease renewal requires a positive lease".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_outbox(&tx, request_id)?.ok_or_else(|| {
            SessionError::Store(format!("session runtime outbox `{request_id}` not found"))
        })?;
        let admission = query_input_admission(&tx, &current.session_id)?.ok_or_else(|| {
            SessionError::Store(format!("session `{}` not found", current.session_id))
        })?;
        if !current.status.holds_claim()
            || current.session_generation != session_generation
            || admission.generation != session_generation
            || !admission.open
            || current.claim_owner.as_deref() != Some(worker_id)
            || current.claim_token.as_deref() != Some(claim_token)
            || current.revision != expected_revision
            || current
                .claim_expires_at_ms
                .is_none_or(|expires| expires <= now_ms)
        {
            return Err(SessionError::Store(format!(
                "stale outbox lease renewal for `{request_id}`"
            )));
        }
        let expires_at = now_ms.saturating_add(lease_ms);
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET claim_expires_at_ms = ?1, updated_at_ms = ?2,
                          revision = revision + 1
                    WHERE request_id = ?3 AND status = 'claimed'
                      AND session_generation = ?4 AND claim_owner = ?5
                      AND claim_token = ?6 AND revision = ?7",
                params![
                    expires_at as i64,
                    now_ms as i64,
                    request_id,
                    session_generation as i64,
                    worker_id,
                    claim_token,
                    expected_revision as i64,
                ],
            )
            .map_err(sql_err)?;
        let changed = if changed == 0 && current.status == SessionRuntimeInputStatus::Running {
            tx.execute(
                r"UPDATE session_runtime_outbox
                      SET claim_expires_at_ms = ?1, updated_at_ms = ?2,
                          revision = revision + 1
                    WHERE request_id = ?3 AND status = 'running'
                      AND session_generation = ?4 AND claim_owner = ?5
                      AND claim_token = ?6 AND revision = ?7",
                params![
                    expires_at as i64,
                    now_ms as i64,
                    request_id,
                    session_generation as i64,
                    worker_id,
                    claim_token,
                    expected_revision as i64,
                ],
            )
            .map_err(sql_err)?
        } else {
            changed
        };
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "outbox lease for `{request_id}` changed during renewal"
            )));
        }
        let renewed = query_outbox(&tx, request_id)?.ok_or_else(|| {
            SessionError::Store(format!("renewed outbox `{request_id}` disappeared"))
        })?;
        append_outbox_history(
            &tx,
            &renewed,
            "renew_lease",
            Some(worker_id),
            None,
            current.status.as_str(),
            current.status.as_str(),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(renewed)
    }

    /// Classify a failed claim and either schedule retry or block it.
    #[allow(clippy::too_many_arguments)]
    pub fn fail_session_runtime_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        failure_class: OutboxFailureClass,
        error: &str,
        retry_at_ms: u64,
        max_attempts: u32,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        self.transition_owned_outbox(
            request_id,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[
                SessionRuntimeInputStatus::Claimed,
                SessionRuntimeInputStatus::Running,
            ],
            |tx, current| {
                let retry = failure_class == OutboxFailureClass::Retryable
                    && current.attempts < max_attempts.max(1);
                let next_status = if retry {
                    SessionRuntimeInputStatus::Queued
                } else if matches!(
                    failure_class,
                    OutboxFailureClass::AuthorizationBlocked | OutboxFailureClass::CorruptPayload
                ) {
                    SessionRuntimeInputStatus::Blocked
                } else {
                    SessionRuntimeInputStatus::Failed
                };
                tx.execute(
                    r"UPDATE session_runtime_outbox
                          SET status = ?1, next_attempt_at_ms = ?2,
                              claim_owner = NULL, claim_expires_at_ms = NULL,
                              claim_token = NULL, claim_fence_epoch = NULL,
                              terminal_at_ms = ?3,
                              failure_class = ?4, last_error = ?5,
                              updated_at_ms = ?6, revision = revision + 1
                        WHERE request_id = ?7
                          AND status IN ('claimed', 'running')
                          AND session_generation = ?8 AND claim_owner = ?9
                          AND claim_token = ?10 AND revision = ?11",
                    params![
                        next_status.as_str(),
                        if retry { retry_at_ms } else { now_ms } as i64,
                        if next_status == SessionRuntimeInputStatus::Failed {
                            Some(now_ms as i64)
                        } else {
                            None
                        },
                        failure_class.as_str(),
                        error,
                        now_ms as i64,
                        request_id,
                        session_generation as i64,
                        worker_id,
                        claim_token,
                        expected_revision as i64,
                    ],
                )
                .map_err(sql_err)?;
                Ok((
                    if retry {
                        "retry"
                    } else if next_status == SessionRuntimeInputStatus::Blocked {
                        "block"
                    } else {
                        "fail"
                    },
                    next_status,
                    current.status,
                ))
            },
        )
    }

    /// Reclassify worker-owned supplement/control work when its target turn is
    /// no longer live. Decision replacement, claim release, queue visibility,
    /// history, and Session timeline are committed atomically.
    #[allow(clippy::too_many_arguments)]
    pub fn requeue_claimed_session_runtime_outbox(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        decision: InputRoutingDecision,
        target_turn_id: Option<&str>,
        classification_json: Option<&str>,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        let candidate = SessionRuntimeOutboxRequest {
            input_id: "validation".to_string(),
            request_id: request_id.to_string(),
            turn_id: "validation".to_string(),
            message_id: "validation".to_string(),
            session_generation,
            decision,
            target_turn_id: target_turn_id.map(str::to_string),
            classification_json: classification_json.map(str::to_string),
            task_route_hint: None,
            created_at_ms: now_ms,
            runtime_options_json: None,
        };
        validate_runtime_input_request(&candidate)?;
        if worker_id.trim().is_empty() || claim_token.trim().is_empty() || reason.trim().is_empty()
        {
            return Err(SessionError::Store(
                "claimed input requeue requires worker, claim token, and reason".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_outbox(&tx, request_id)?
            .ok_or_else(|| SessionError::Store(format!("outbox `{request_id}` not found")))?;
        let admission = query_input_admission(&tx, &current.session_id)?.ok_or_else(|| {
            SessionError::Store(format!("session `{}` not found", current.session_id))
        })?;
        if !current.status.holds_claim()
            || current.session_generation != session_generation
            || admission.generation != session_generation
            || !admission.open
            || current.claim_owner.as_deref() != Some(worker_id)
            || current.claim_token.as_deref() != Some(claim_token)
            || current.revision != expected_revision
            || current
                .claim_expires_at_ms
                .is_none_or(|expires| expires <= now_ms)
        {
            return Err(SessionError::Store(format!(
                "outbox `{request_id}` generation/token/status/revision fence mismatch"
            )));
        }
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET decision = ?1, target_turn_id = ?2, classification_json = ?3,
                          status = 'reclassified', next_attempt_at_ms = ?4,
                          claim_owner = NULL, claim_token = NULL,
                          claim_fence_epoch = NULL,
                          claim_expires_at_ms = NULL, failure_class = NULL,
                          last_error = NULL, terminal_at_ms = NULL,
                          updated_at_ms = ?4, revision = revision + 1
                    WHERE request_id = ?5
                      AND session_generation = ?6
                      AND claim_owner = ?7 AND claim_token = ?8
                      AND revision = ?9 AND status IN ('claimed', 'running')",
                params![
                    input_decision_as_str(decision),
                    target_turn_id,
                    classification_json,
                    now_ms as i64,
                    request_id,
                    session_generation as i64,
                    worker_id,
                    claim_token,
                    expected_revision as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "outbox `{request_id}` changed during claimed requeue"
            )));
        }
        let updated = query_outbox(&tx, request_id)?.ok_or_else(|| {
            SessionError::Store(format!("requeued outbox `{request_id}` disappeared"))
        })?;
        append_outbox_history(
            &tx,
            &updated,
            "owner_reclassify_requeue",
            Some(worker_id),
            Some(reason),
            current.status.as_str(),
            SessionRuntimeInputStatus::Reclassified.as_str(),
            now_ms,
        )?;
        append_input_timeline_event(
            &tx,
            &request_from_outbox(&updated),
            &updated.session_id,
            updated.sequence,
            "session.input.reclassified.v1",
            updated.status,
            Some(worker_id),
            Some(reason),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }

    /// Manually release a blocked row while retaining attempts and audit history.
    pub fn retry_blocked_session_runtime_outbox(
        &self,
        request_id: &str,
        session_generation: u64,
        expected_revision: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(SessionError::Store(
                "manual outbox retry requires actor and reason".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_outbox(&tx, request_id)?
            .ok_or_else(|| SessionError::Store(format!("outbox `{request_id}` not found")))?;
        if current.status != SessionRuntimeInputStatus::Blocked
            || current.session_generation != session_generation
            || current.revision != expected_revision
        {
            return Err(SessionError::Store(format!(
                "outbox `{request_id}` is not blocked at revision {expected_revision}"
            )));
        }
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET status = 'queued', next_attempt_at_ms = ?1,
                          claim_owner = NULL, claim_expires_at_ms = NULL,
                          claim_token = NULL, claim_fence_epoch = NULL,
                          terminal_at_ms = NULL,
                          failure_class = NULL, last_error = NULL, updated_at_ms = ?1,
                          revision = revision + 1
                    WHERE request_id = ?2 AND status = 'blocked'
                      AND session_generation = ?3 AND revision = ?4",
                params![
                    now_ms as i64,
                    request_id,
                    session_generation as i64,
                    expected_revision as i64
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "outbox `{request_id}` changed during manual retry"
            )));
        }
        let updated = query_outbox(&tx, request_id)?.ok_or_else(|| {
            SessionError::Store(format!("retried outbox `{request_id}` disappeared"))
        })?;
        append_outbox_history(
            &tx,
            &updated,
            "manual_retry",
            Some(actor),
            Some(reason),
            SessionRuntimeInputStatus::Blocked.as_str(),
            SessionRuntimeInputStatus::Queued.as_str(),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }

    /// Cancel a non-terminal durable input. Incrementing the revision and
    /// clearing the claim token immediately fences any in-flight worker.
    pub fn cancel_session_runtime_outbox(
        &self,
        input_id: &str,
        session_generation: u64,
        expected_revision: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(SessionError::Store(
                "session input cancellation requires actor and reason".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_outbox_by_input_id(&tx, input_id)?
            .ok_or_else(|| SessionError::Store(format!("session input `{input_id}` not found")))?;
        if current.session_generation != session_generation
            || current.revision != expected_revision
            || current.status.is_terminal()
        {
            return Err(SessionError::Store(format!(
                "session input `{input_id}` cannot be cancelled at generation {session_generation} revision {expected_revision}"
            )));
        }
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET status = 'cancelled', claim_owner = NULL, claim_token = NULL,
                          claim_fence_epoch = NULL,
                          claim_expires_at_ms = NULL, last_error = ?1,
                          terminal_at_ms = ?2, updated_at_ms = ?2,
                          revision = revision + 1
                    WHERE input_id = ?3 AND session_generation = ?4
                      AND revision = ?5
                      AND status NOT IN (
                          'rejected_duplicate', 'rejected_policy',
                          'completed', 'supplemented', 'failed', 'cancelled', 'expired'
                      )",
                params![
                    reason,
                    now_ms as i64,
                    input_id,
                    session_generation as i64,
                    expected_revision as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "session input `{input_id}` changed during cancellation"
            )));
        }
        let updated = query_outbox_by_input_id(&tx, input_id)?.ok_or_else(|| {
            SessionError::Store(format!("cancelled session input `{input_id}` disappeared"))
        })?;
        append_outbox_history(
            &tx,
            &updated,
            "cancel",
            Some(actor),
            Some(reason),
            current.status.as_str(),
            SessionRuntimeInputStatus::Cancelled.as_str(),
            now_ms,
        )?;
        append_input_timeline_event(
            &tx,
            &request_from_outbox(&updated),
            &updated.session_id,
            updated.sequence,
            "session.input.cancelled.v1",
            updated.status,
            Some(actor),
            Some(reason),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }

    /// Replace a queued classification without creating another input source
    /// of truth. Claimed/running rows must first be cancelled by their owner.
    #[allow(clippy::too_many_arguments)]
    pub fn reclassify_session_runtime_outbox(
        &self,
        input_id: &str,
        session_generation: u64,
        expected_revision: u64,
        decision: InputRoutingDecision,
        target_turn_id: Option<&str>,
        classification_json: Option<&str>,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionRuntimeOutboxRecord> {
        let candidate = SessionRuntimeOutboxRequest {
            input_id: input_id.to_string(),
            request_id: "validation".to_string(),
            turn_id: "validation".to_string(),
            message_id: "validation".to_string(),
            session_generation,
            decision,
            target_turn_id: target_turn_id.map(str::to_string),
            classification_json: classification_json.map(str::to_string),
            task_route_hint: None,
            created_at_ms: now_ms,
            runtime_options_json: None,
        };
        validate_runtime_input_request(&candidate)?;
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(SessionError::Store(
                "session input reclassification requires actor and reason".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        require_input_admission(
            &tx,
            &query_outbox_by_input_id(&tx, input_id)?
                .ok_or_else(|| {
                    SessionError::Store(format!("session input `{input_id}` not found"))
                })?
                .session_id,
            session_generation,
        )?;
        let current = query_outbox_by_input_id(&tx, input_id)?
            .ok_or_else(|| SessionError::Store(format!("session input `{input_id}` not found")))?;
        if current.session_generation != session_generation
            || current.revision != expected_revision
            || !matches!(
                current.status,
                SessionRuntimeInputStatus::Accepted
                    | SessionRuntimeInputStatus::Classified
                    | SessionRuntimeInputStatus::Queued
                    | SessionRuntimeInputStatus::Reclassified
                    | SessionRuntimeInputStatus::Attached
                    | SessionRuntimeInputStatus::Blocked
            )
        {
            return Err(SessionError::Store(format!(
                "session input `{input_id}` is not reclassifiable at generation {session_generation} revision {expected_revision}"
            )));
        }
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET decision = ?1, target_turn_id = ?2, classification_json = ?3,
                          status = 'reclassified', next_attempt_at_ms = ?4,
                          failure_class = NULL, last_error = NULL, terminal_at_ms = NULL,
                          claim_owner = NULL, claim_token = NULL,
                          claim_fence_epoch = NULL, claim_expires_at_ms = NULL,
                          updated_at_ms = ?4, revision = revision + 1
                    WHERE input_id = ?5 AND session_generation = ?6
                      AND revision = ?7
                      AND status IN (
                        'accepted', 'classified', 'queued', 'reclassified',
                        'attached', 'blocked'
                      )",
                params![
                    input_decision_as_str(decision),
                    target_turn_id,
                    classification_json,
                    now_ms as i64,
                    input_id,
                    session_generation as i64,
                    expected_revision as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "session input `{input_id}` changed during reclassification"
            )));
        }
        let updated = query_outbox_by_input_id(&tx, input_id)?.ok_or_else(|| {
            SessionError::Store(format!(
                "reclassified session input `{input_id}` disappeared"
            ))
        })?;
        append_outbox_history(
            &tx,
            &updated,
            "reclassify",
            Some(actor),
            Some(reason),
            current.status.as_str(),
            SessionRuntimeInputStatus::Reclassified.as_str(),
            now_ms,
        )?;
        append_input_timeline_event(
            &tx,
            &request_from_outbox(&updated),
            &updated.session_id,
            updated.sequence,
            "session.input.reclassified.v1",
            updated.status,
            Some(actor),
            Some(reason),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }

    pub fn get_session_input_admission(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionInputAdmission>> {
        let conn = self.conn()?;
        query_input_admission(&conn, session_id)
    }

    /// Close admission and revoke the current generation atomically. Every
    /// active row from the revoked generation becomes expired in the same
    /// transaction, so stale workers can never commit terminal state.
    pub fn close_session_input_admission(
        &self,
        session_id: &str,
        expected_generation: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionInputAdmission> {
        self.advance_session_input_generation(
            session_id,
            expected_generation,
            false,
            actor,
            reason,
            now_ms,
        )
    }

    /// Advance Session authority and choose whether the new generation accepts
    /// ingress. This is used by branch/reopen flows after their durable
    /// lifecycle mutation has selected the new owner.
    pub fn advance_session_input_generation(
        &self,
        session_id: &str,
        expected_generation: u64,
        open: bool,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> Result<SessionInputAdmission> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(SessionError::Store(
                "session generation advance requires actor and reason".to_string(),
            ));
        }
        let next_generation = expected_generation
            .checked_add(1)
            .ok_or_else(|| SessionError::Store("session generation overflow".to_string()))?;
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_input_admission(&tx, session_id)?
            .ok_or_else(|| SessionError::Store(format!("session `{session_id}` not found")))?;
        if current.generation != expected_generation {
            return Err(SessionError::Store(format!(
                "session `{session_id}` generation changed from expected {expected_generation}"
            )));
        }
        let active = {
            let mut stmt = tx
                .prepare(
                    r"SELECT request_id FROM session_runtime_outbox
                       WHERE session_id = ?1 AND session_generation = ?2
                         AND status IN (
                             'accepted', 'classified', 'queued', 'claimed',
                             'running', 'reclassified', 'blocked'
                         )
                       ORDER BY sequence ASC, request_id ASC",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![session_id, expected_generation as i64], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(sql_err)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_err)?
        };
        let changed = tx
            .execute(
                r"UPDATE sessions
                      SET input_generation = ?1, input_admission_open = ?2,
                          updated_at_ms = MAX(updated_at_ms, ?3)
                    WHERE session_id = ?4 AND input_generation = ?5",
                params![
                    next_generation as i64,
                    open,
                    now_ms as i64,
                    session_id,
                    expected_generation as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "session `{session_id}` generation changed during advance"
            )));
        }
        for request_id in active {
            let before = query_outbox(&tx, &request_id)?.ok_or_else(|| {
                SessionError::Store(format!(
                    "outbox `{request_id}` disappeared during generation advance"
                ))
            })?;
            tx.execute(
                r"UPDATE session_runtime_outbox
                      SET status = 'expired', claim_owner = NULL, claim_token = NULL,
                          claim_fence_epoch = NULL,
                          claim_expires_at_ms = NULL, last_error = ?1,
                          terminal_at_ms = ?2, updated_at_ms = ?2,
                          revision = revision + 1
                    WHERE request_id = ?3 AND session_generation = ?4
                      AND revision = ?5",
                params![
                    reason,
                    now_ms as i64,
                    request_id,
                    expected_generation as i64,
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
                "generation_expire",
                Some(actor),
                Some(reason),
                before.status.as_str(),
                SessionRuntimeInputStatus::Expired.as_str(),
                now_ms,
            )?;
        }
        let admission = query_input_admission(&tx, session_id)?.ok_or_else(|| {
            SessionError::Store(format!(
                "session `{session_id}` disappeared after generation advance"
            ))
        })?;
        append_admission_timeline_event(
            &tx,
            session_id,
            expected_generation,
            &admission,
            actor,
            reason,
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(admission)
    }

    pub fn get_session_runtime_outbox(
        &self,
        request_id: &str,
    ) -> Result<Option<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        query_outbox(&conn, request_id)
    }

    pub fn get_session_runtime_outbox_by_input_id(
        &self,
        input_id: &str,
    ) -> Result<Option<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        query_outbox_by_input_id(&conn, input_id)
    }

    pub fn set_session_input_application_receipt(
        &self,
        input_ids: &[String],
        expected_revisions: &[u64],
        receipt: &harness_contract::input_disposition::SessionInputApplicationReceipt,
        now_ms: u64,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        receipt
            .validate_shape()
            .map_err(SessionError::InvalidArgument)?;
        if input_ids.is_empty() || input_ids.len() != expected_revisions.len() {
            return Err(SessionError::InvalidArgument(
                "application receipt requires one expected revision per input".to_string(),
            ));
        }
        let requested = input_ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let receipt_inputs = receipt
            .input_ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        if requested.len() != input_ids.len() || requested != receipt_inputs {
            return Err(SessionError::InvalidArgument(
                "application receipt input set does not match the fenced update set".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let mut current = Vec::with_capacity(input_ids.len());
        for (input_id, expected_revision) in input_ids.iter().zip(expected_revisions) {
            let record = query_outbox_by_input_id(&tx, input_id)?.ok_or_else(|| {
                SessionError::Store(format!("session input `{input_id}` not found"))
            })?;
            if record.revision != *expected_revision {
                return Err(SessionError::StaleExecutionFence(format!(
                    "session input `{input_id}` revision {} does not match {expected_revision}",
                    record.revision
                )));
            }
            if !receipt.can_follow(record.application_receipt.as_ref()) {
                return Err(SessionError::InvalidArgument(format!(
                    "application receipt transition is invalid for input `{input_id}`"
                )));
            }
            current.push(record);
        }
        let leader = current
            .iter()
            .min_by_key(|record| (record.sequence, record.input_id.as_str()))
            .map(|record| record.input_id.as_str());
        if leader != Some(receipt.leader_input_id.as_str())
            || current
                .iter()
                .any(|record| record.session_id != current[0].session_id)
        {
            return Err(SessionError::InvalidArgument(
                "application receipt leader or Session scope is invalid".to_string(),
            ));
        }
        let receipt_json = serde_json::to_string(receipt)
            .map_err(|error| SessionError::Store(error.to_string()))?;
        for ((input_id, expected_revision), before) in
            input_ids.iter().zip(expected_revisions).zip(current.iter())
        {
            let projection =
                applied_input_projection(receipt, before.target_turn_id.as_deref(), now_ms);
            let decision = projection.as_ref().map_or(before.decision, |value| value.0);
            let status = projection.as_ref().map_or(before.status, |value| value.1);
            let target_turn_id = projection
                .as_ref()
                .map_or_else(|| before.target_turn_id.clone(), |value| value.2.clone());
            let terminal_at_ms = projection
                .as_ref()
                .map_or(before.terminal_at_ms, |value| value.3);
            let reclassified = status == SessionRuntimeInputStatus::Reclassified;
            let changed = tx
                .execute(
                    "UPDATE session_runtime_outbox
                        SET application_receipt_json=?1, decision=?2, target_turn_id=?3,
                            status=?4, terminal_at_ms=?5,
                            next_attempt_at_ms=CASE WHEN ?6 THEN ?7 ELSE next_attempt_at_ms END,
                            claim_owner=CASE WHEN ?6 THEN NULL ELSE claim_owner END,
                            claim_token=CASE WHEN ?6 THEN NULL ELSE claim_token END,
                            claim_fence_epoch=CASE WHEN ?6 THEN NULL ELSE claim_fence_epoch END,
                            claim_expires_at_ms=CASE WHEN ?6 THEN NULL ELSE claim_expires_at_ms END,
                            updated_at_ms=?7, revision=revision+1
                      WHERE input_id=?8 AND revision=?9",
                    params![
                        receipt_json,
                        input_decision_as_str(decision),
                        target_turn_id,
                        status.as_str(),
                        terminal_at_ms.map(|value| value as i64),
                        reclassified,
                        now_ms as i64,
                        input_id,
                        *expected_revision as i64
                    ],
                )
                .map_err(sql_err)?;
            if changed != 1 {
                return Err(SessionError::StaleExecutionFence(format!(
                    "session input `{input_id}` changed during application receipt commit"
                )));
            }
        }
        let mut updated = Vec::with_capacity(input_ids.len());
        for input_id in input_ids {
            updated.push(query_outbox_by_input_id(&tx, input_id)?.ok_or_else(|| {
                SessionError::Store(format!("session input `{input_id}` disappeared"))
            })?);
        }
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }

    /// Load one turn's exact durable relation without applying the bounded
    /// history-page limit used by catalog views.
    pub fn session_runtime_outbox_for_turn_relation(
        &self,
        session_id: &str,
        session_generation: u64,
        turn_id: &str,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        let mut statement = conn
            .prepare(
                r"SELECT input_id, request_id, turn_id, message_id, session_id, sequence,
                         session_generation, decision, target_turn_id, classification_json, task_route_hint_json,
                         status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                         claim_owner, claim_token, claim_expires_at_ms, failure_class,
                         last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                         runtime_options_json, claim_fence_epoch, application_receipt_json
                    FROM session_runtime_outbox
                   WHERE session_id=?1 AND session_generation=?2
                     AND (turn_id=?3 OR target_turn_id=?3)
                   ORDER BY sequence ASC, request_id ASC",
            )
            .map_err(sql_err)?;
        let records = statement
            .query_map(
                params![session_id, session_generation as i64, turn_id],
                row_to_outbox,
            )
            .map_err(sql_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_err)?;
        Ok(records)
    }

    /// Bounded durable ingress history for one Session.  Runtime/Suface
    /// observers use it only to recover execution identity and ingress state;
    /// detailed execution facts remain owned by Runtime's graph projection.
    pub fn session_runtime_outbox_for_session(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT input_id, request_id, turn_id, message_id, session_id, sequence,
                         session_generation, decision, target_turn_id, classification_json, task_route_hint_json,
                         status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                         claim_owner, claim_token, claim_expires_at_ms, failure_class,
                         last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                         runtime_options_json, claim_fence_epoch, application_receipt_json
                    FROM session_runtime_outbox
                   WHERE session_id = ?1
                   ORDER BY updated_at_ms DESC, sequence DESC, request_id DESC
                   LIMIT ?2",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, bounded_limit(limit, 1, 500) as i64],
                row_to_outbox,
            )
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    /// Fetch a bounded execution history for several Sessions with one query.
    ///
    /// The row-number bound is per Session, so a busy Session cannot starve the
    /// remaining page. This is the durable recovery path for catalog views;
    /// active execution truth is reconciled from Runtime memory afterwards.
    pub fn session_runtime_outbox_for_sessions(
        &self,
        session_ids: &[String],
        per_session_limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn()?;
        let session_ids_json = serde_json::to_string(session_ids)
            .map_err(|error| SessionError::Store(error.to_string()))?;
        let mut stmt = conn
            .prepare(
                r"WITH ranked AS (
                    SELECT input_id, request_id, turn_id, message_id, session_id, sequence,
                           session_generation, decision, target_turn_id, classification_json, task_route_hint_json,
                           status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                           claim_owner, claim_token, claim_expires_at_ms, failure_class,
                           last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                           runtime_options_json, claim_fence_epoch, application_receipt_json,
                           ROW_NUMBER() OVER (
                               PARTITION BY session_id
                               ORDER BY updated_at_ms DESC, sequence DESC, request_id DESC
                           ) AS row_number
                      FROM session_runtime_outbox
                     WHERE session_id IN (SELECT value FROM json_each(?1))
                       AND target_turn_id IS NULL
                       AND decision NOT IN ('reject_duplicate', 'reject_policy')
                )
                SELECT input_id, request_id, turn_id, message_id, session_id, sequence,
                       session_generation, decision, target_turn_id, classification_json, task_route_hint_json,
                       status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                       claim_owner, claim_token, claim_expires_at_ms, failure_class,
                       last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                       runtime_options_json, claim_fence_epoch, application_receipt_json
                  FROM ranked
                 WHERE row_number <= ?2
                 ORDER BY session_id ASC, updated_at_ms DESC, sequence DESC, request_id DESC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![
                    session_ids_json,
                    bounded_limit(per_session_limit, 1, 500) as i64
                ],
                row_to_outbox,
            )
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    /// Bounded durable work that may still need observer recovery after a
    /// Gateway restart.  Materialized ingress is terminal for this carrier;
    /// the terminal transcript/outbox remains the source for reply recovery.
    pub fn active_session_runtime_outbox(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT input_id, request_id, turn_id, message_id, session_id, sequence,
                         session_generation, decision, target_turn_id, classification_json, task_route_hint_json,
                         status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                         claim_owner, claim_token, claim_expires_at_ms, failure_class,
                         last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                         runtime_options_json, claim_fence_epoch, application_receipt_json
                    FROM session_runtime_outbox
                   WHERE status NOT IN (
                       'rejected_duplicate', 'rejected_policy',
                       'completed', 'supplemented', 'failed', 'cancelled', 'expired'
                   )
                   ORDER BY updated_at_ms DESC, sequence DESC, request_id DESC
                   LIMIT ?1",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![bounded_limit(limit, 1, 500) as i64], row_to_outbox)
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    pub fn session_runtime_outbox_health(&self) -> Result<SessionRuntimeOutboxHealth> {
        let conn = self.conn()?;
        let mut health = SessionRuntimeOutboxHealth::default();
        let mut stmt = conn
            .prepare("SELECT status, COUNT(*) FROM session_runtime_outbox GROUP BY status")
            .map_err(sql_err)?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(sql_err)?;
        for row in rows {
            let (status, count) = row.map_err(sql_err)?;
            let count = count as usize;
            match SessionRuntimeInputStatus::parse(&status).map_err(sql_err)? {
                SessionRuntimeInputStatus::Accepted => health.accepted = count,
                SessionRuntimeInputStatus::Classified => health.classified = count,
                SessionRuntimeInputStatus::Queued => health.queued = count,
                SessionRuntimeInputStatus::RejectedDuplicate => {
                    health.rejected_duplicate = count;
                }
                SessionRuntimeInputStatus::RejectedPolicy => health.rejected_policy = count,
                SessionRuntimeInputStatus::Claimed => health.claimed = count,
                SessionRuntimeInputStatus::Running => health.running = count,
                SessionRuntimeInputStatus::Reclassified => health.reclassified = count,
                SessionRuntimeInputStatus::Attached => health.attached = count,
                SessionRuntimeInputStatus::Completed => health.completed = count,
                SessionRuntimeInputStatus::Supplemented => health.supplemented = count,
                SessionRuntimeInputStatus::Failed => health.failed = count,
                SessionRuntimeInputStatus::Blocked => health.blocked = count,
                SessionRuntimeInputStatus::Cancelled => health.cancelled = count,
                SessionRuntimeInputStatus::Expired => health.expired = count,
            }
        }
        health.runnable_depth = health
            .accepted
            .saturating_add(health.classified)
            .saturating_add(health.queued)
            .saturating_add(health.claimed)
            .saturating_add(health.running)
            .saturating_add(health.reclassified);
        health.oldest_runnable_created_at_ms = conn
            .query_row(
                "SELECT MIN(created_at_ms) FROM session_runtime_outbox
                  WHERE status IN ('accepted','classified','queued','claimed','running','reclassified')",
                [],
                |row| row.get::<_, Option<i64>>(0),
            )
            .map_err(sql_err)?
            .map(|value| value.max(0) as u64);
        Ok(health)
    }

    /// Return blocked ingress rows for operational inspection. The bounded
    /// result is ordered deterministically so operators can retry the oldest
    /// poison item first.
    pub fn blocked_session_runtime_outbox(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionRuntimeOutboxRecord>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT input_id, request_id, turn_id, message_id, session_id, sequence,
                         session_generation, decision, target_turn_id, classification_json, task_route_hint_json,
                         status, runtime_commit_cursor, attempts, next_attempt_at_ms,
                         claim_owner, claim_token, claim_expires_at_ms, failure_class,
                         last_error, revision, created_at_ms, updated_at_ms, terminal_at_ms,
                         runtime_options_json, claim_fence_epoch, application_receipt_json
                    FROM session_runtime_outbox
                   WHERE status = 'blocked'
                   ORDER BY updated_at_ms ASC, sequence ASC, request_id ASC
                   LIMIT ?1",
            )
            .map_err(sql_err)?;
        let records = stmt
            .query_map(params![bounded_limit(limit, 1, 500) as i64], row_to_outbox)
            .map_err(sql_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_err)?;
        Ok(records)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn transition_owned_outbox<F>(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
        allowed_statuses: &[SessionRuntimeInputStatus],
        transition: F,
    ) -> Result<SessionRuntimeOutboxRecord>
    where
        F: FnOnce(
            &rusqlite::Transaction<'_>,
            &SessionRuntimeOutboxRecord,
        ) -> Result<(
            &'static str,
            SessionRuntimeInputStatus,
            SessionRuntimeInputStatus,
        )>,
    {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_outbox(&tx, request_id)?
            .ok_or_else(|| SessionError::Store(format!("outbox `{request_id}` not found")))?;
        let admission = query_input_admission(&tx, &current.session_id)?.ok_or_else(|| {
            SessionError::Store(format!("session `{}` not found", current.session_id))
        })?;
        if !allowed_statuses.contains(&current.status)
            || current.session_generation != session_generation
            || admission.generation != session_generation
            || !admission.open
            || current.claim_owner.as_deref() != Some(worker_id)
            || current.claim_token.as_deref() != Some(claim_token)
            || current.revision != expected_revision
            || current
                .claim_expires_at_ms
                .is_none_or(|expires| expires <= now_ms)
        {
            return Err(SessionError::Store(format!(
                "outbox `{request_id}` generation/token/status/revision fence mismatch"
            )));
        }
        let (action, to_status, from_status) = transition(&tx, &current)?;
        let updated = query_outbox(&tx, request_id)?.ok_or_else(|| {
            SessionError::Store(format!("transitioned outbox `{request_id}` disappeared"))
        })?;
        if updated.revision != expected_revision + 1 || updated.status != to_status {
            return Err(SessionError::Store(format!(
                "outbox `{request_id}` transition lost an optimistic update"
            )));
        }
        append_outbox_history(
            &tx,
            &updated,
            action,
            Some(worker_id),
            updated.last_error.as_deref(),
            from_status.as_str(),
            to_status.as_str(),
            now_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(updated)
    }
}
