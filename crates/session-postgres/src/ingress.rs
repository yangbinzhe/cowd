//! Ingress operations for the PostgresSessionStore adapter.

use super::*;

impl PostgresSessionStore {
    pub fn append_message_with_runtime_outbox(
        &self,
        message: &SessionMessage,
        request: &SessionRuntimeOutboxRequest,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        validate_runtime_request(message, request)?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&message.session_id],
            )
            .map_err(postgres_error)?;
        if let Some(existing) = runtime_outbox_tx(&mut transaction, &request.request_id)? {
            if existing.input_id == request.input_id
                && existing.turn_id == request.turn_id
                && existing.message_id == request.message_id
                && existing.session_id == message.session_id
                && existing.sequence == message.sequence
                && existing.session_generation == request.session_generation
                && existing.decision == request.decision
                && existing.target_turn_id == request.target_turn_id
            {
                transaction.commit().map_err(postgres_error)?;
                return Ok(existing);
            }
            return Err(session::SessionError::Store(format!(
                "outbox request_id `{}` is already bound to another message",
                request.request_id
            )));
        }
        require_input_admission_tx(
            &mut transaction,
            &message.session_id,
            request.session_generation,
        )?;
        insert_message_tx(&mut transaction, message)?;
        refresh_session_message_summary_tx(
            &mut transaction,
            &message.session_id,
            message.created_at_ms,
        )?;
        let record = insert_runtime_outbox_tx(&mut transaction, message, request)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    pub fn append_ingress_with_runtime_outbox(
        &self,
        session_id: &str,
        role: &str,
        content_json: Option<&str>,
        created_at_ms: u64,
        request: &SessionRuntimeOutboxRequest,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        if session_id.trim().is_empty()
            || role.trim().is_empty()
            || request.input_id.trim().is_empty()
        {
            return Err(session::SessionError::Store(
                "ingress outbox requires non-empty session, role and request identities"
                    .to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&session_id],
            )
            .map_err(postgres_error)?;
        if let Some(existing) = runtime_outbox_tx(&mut transaction, &request.request_id)? {
            if existing.input_id == request.input_id
                && existing.session_id == session_id
                && existing.message_id == request.message_id
                && existing.turn_id == request.turn_id
                && existing.session_generation == request.session_generation
                && existing.decision == request.decision
                && existing.target_turn_id == request.target_turn_id
            {
                transaction.commit().map_err(postgres_error)?;
                return Ok(existing);
            }
            return Err(session::SessionError::Store(format!(
                "outbox request `{}` conflicts with its committed ingress",
                request.request_id
            )));
        }
        validate_runtime_input_request(request)?;
        require_input_admission_tx(&mut transaction, session_id, request.session_generation)?;
        let sequence: i64 = transaction
            .query_one(
                "SELECT COALESCE(MAX(sequence), -1) + 1 FROM session_messages WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
            .try_get(0)
            .map_err(postgres_error)?;
        let content_json = content_json.unwrap_or("[]");
        let message = SessionMessage {
            stable_message_id: request.message_id.clone(),
            session_id: session_id.to_string(),
            sequence: from_i64(sequence, "message sequence")?,
            role: role.to_string(),
            content_json: content_json.to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms,
        };
        insert_message_tx(&mut transaction, &message)?;
        refresh_session_message_summary_tx(&mut transaction, session_id, created_at_ms)?;
        let record = insert_runtime_outbox_tx(&mut transaction, &message, request)?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    pub fn claim_session_runtime_outbox(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        if worker_id.trim().is_empty() || lease_ms == 0 || limit == 0 {
            return Err(session::SessionError::Store(
                "outbox claim requires worker_id, positive lease and positive limit".to_string(),
            ));
        }
        let now = to_u64_i64(now_ms, "runtime outbox clock")?;
        let expires = now
            .checked_add(to_u64_i64(lease_ms, "runtime outbox lease")?)
            .ok_or_else(|| {
                session::SessionError::Store("runtime outbox lease overflow".to_string())
            })?;
        let limit = to_i64(limit.min(500), "runtime outbox limit")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let rows = transaction.query(
            "WITH ranked AS (
                 SELECT o.request_id,o.status,o.session_id,o.session_generation,o.sequence,
                        o.next_attempt_at_ms,o.claim_expires_at_ms,
                        ROW_NUMBER() OVER (
                            PARTITION BY o.session_id,o.session_generation
                            ORDER BY o.sequence ASC,o.request_id ASC
                        ) AS session_rank
                   FROM session_runtime_outbox o
                   JOIN session_records s ON s.session_id=o.session_id
                  WHERE o.status IN (
                            'accepted','classified','queued','claimed','running','reclassified'
                        )
                    AND o.session_generation=s.input_generation
                    AND s.input_admission_open=TRUE
             ), candidates AS (
                 SELECT o.request_id,o.status AS previous_status
                   FROM session_runtime_outbox o
                   JOIN ranked r ON r.request_id=o.request_id
                  WHERE r.session_rank=1
                    AND (
                        (r.status IN ('queued','reclassified') AND r.next_attempt_at_ms <= $1)
                        OR (r.status IN ('claimed','running') AND r.claim_expires_at_ms <= $1)
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM session_runtime_outbox held
                         WHERE held.session_id=r.session_id
                           AND held.session_generation=r.session_generation
                           AND held.request_id<>r.request_id
                           AND held.status IN ('claimed','running')
                           AND held.claim_expires_at_ms > $1
                    )
                  ORDER BY r.next_attempt_at_ms ASC,r.sequence ASC,r.request_id ASC
                  FOR UPDATE OF o SKIP LOCKED LIMIT $2
             ), updated AS (
                 UPDATE session_runtime_outbox o
                    SET status='claimed',attempts=o.attempts+1,claim_owner=$3,
                        claim_token=o.request_id || ':' || (o.revision+1)::text
                            || ':' || $1::text || ':' || $3,
                        claim_fence_epoch=o.revision+1,
                        claim_expires_at_ms=$4,updated_at_ms=$1,revision=o.revision+1
                   FROM candidates c WHERE o.request_id=c.request_id
                 RETURNING o.*,c.previous_status
             )
             SELECT input_id,request_id,turn_id,message_id,session_id,sequence,session_generation,
                    decision,target_turn_id,classification_json,task_route_hint_json,status,runtime_commit_cursor,attempts,
                    next_attempt_at_ms,claim_owner,claim_token,claim_expires_at_ms,failure_class,
                    last_error,revision,created_at_ms,updated_at_ms,terminal_at_ms,
                    runtime_options_json,claim_fence_epoch,application_receipt_json,previous_status
               FROM updated
              ORDER BY next_attempt_at_ms ASC,sequence ASC,request_id ASC",
            &[&now,&limit,&worker_id,&expires],
        ).map_err(postgres_error)?;
        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let record = row_to_runtime_outbox(&row)?;
            let previous: String = row.try_get(27).map_err(postgres_error)?;
            let previous = parse_runtime_status(&previous)?;
            append_runtime_history_tx(
                &mut transaction,
                &record,
                if previous.holds_claim() {
                    "reclaim"
                } else {
                    "claim"
                },
                Some(worker_id),
                None,
                previous,
                SessionRuntimeInputStatus::Claimed,
                record.claim_token.as_deref(),
                now_ms,
            )?;
            claimed.push(record);
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(claimed)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn mark_session_runtime_outbox_running(
        &self,
        request_id: &str,
        worker_id: &str,
        session_generation: u64,
        claim_token: &str,
        expected_revision: u64,
        now_ms: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current = runtime_outbox_for_update(&mut transaction, request_id)?;
        assert_runtime_lease(
            &mut transaction,
            &current,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[SessionRuntimeInputStatus::Claimed],
        )?;
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                SET status='running',updated_at_ms=$1,revision=revision+1
              WHERE request_id=$2 AND status='claimed' AND session_generation=$3
                AND claim_owner=$4 AND claim_token=$5 AND revision=$6",
                &[
                    &to_u64_i64(now_ms, "runtime clock")?,
                    &request_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &worker_id,
                    &claim_token,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "outbox `{request_id}` changed during running transition"
            )));
        }
        let record = runtime_outbox_for_update(&mut transaction, request_id)?;
        append_runtime_history_tx(
            &mut transaction,
            &record,
            "start",
            Some(worker_id),
            Some(expected_revision),
            current.status,
            SessionRuntimeInputStatus::Running,
            None,
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

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
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        if input_id.trim().is_empty()
            || target_turn_id.trim().is_empty()
            || actor.trim().is_empty()
            || reason.trim().is_empty()
        {
            return Err(session::SessionError::Store(
                "Session input attachment requires input, target, actor and reason".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current = runtime_outbox_by_input_id_for_update(&mut transaction, input_id)?;
        require_input_admission_tx(&mut transaction, &current.session_id, session_generation)?;
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
            return Err(session::SessionError::Store(format!(
                "session input `{input_id}` is not attachable at generation {session_generation} revision {expected_revision}"
            )));
        }
        if runtime_turn_is_terminal_tx(
            &mut transaction,
            &current.session_id,
            session_generation,
            target_turn_id,
        )? {
            return Err(session::SessionError::StaleExecutionFence(format!(
                "target turn `{target_turn_id}` became terminal before input `{input_id}` attachment"
            )));
        }
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                    SET status='attached',claim_owner=NULL,claim_token=NULL,
                        claim_fence_epoch=NULL,claim_expires_at_ms=NULL,
                        terminal_at_ms=NULL,runtime_commit_cursor=NULL,
                        failure_class=NULL,last_error=NULL,
                        updated_at_ms=$1,revision=revision+1
                  WHERE input_id=$2 AND session_generation=$3 AND revision=$4
                    AND decision='supplement_current_turn' AND target_turn_id=$5
                    AND status IN ('accepted','classified','queued','reclassified')",
                &[
                    &to_u64_i64(now_ms, "runtime clock")?,
                    &input_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                    &target_turn_id,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "session input `{input_id}` changed during attachment"
            )));
        }
        let attached = runtime_outbox_by_input_id_for_update(&mut transaction, input_id)?;
        append_runtime_history_tx(
            &mut transaction,
            &attached,
            "attach",
            Some(actor),
            Some(expected_revision),
            current.status,
            SessionRuntimeInputStatus::Attached,
            Some(reason),
            now_ms,
        )?;
        append_input_timeline_event_tx(
            &mut transaction,
            &request_from_outbox(&attached),
            &attached.session_id,
            attached.sequence,
            SessionRuntimeInputStatus::Attached.timeline_event_kind(),
            SessionRuntimeInputStatus::Attached,
            Some(actor),
            Some(reason),
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(attached)
    }

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
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        if !matches!(
            acknowledged_status,
            SessionRuntimeInputStatus::Attached
                | SessionRuntimeInputStatus::Completed
                | SessionRuntimeInputStatus::Supplemented
                | SessionRuntimeInputStatus::Cancelled
        ) {
            return Err(session::SessionError::Store(
                "ack status must be attached, completed, supplemented, or cancelled".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current = runtime_outbox_for_update(&mut transaction, request_id)?;
        assert_runtime_lease(
            &mut transaction,
            &current,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[SessionRuntimeInputStatus::Running],
        )?;
        if acknowledged_status == SessionRuntimeInputStatus::Attached {
            let target_turn_id = current.target_turn_id.as_deref().ok_or_else(|| {
                session::SessionError::Store(format!(
                    "attached acknowledgement for `{request_id}` has no target turn"
                ))
            })?;
            if runtime_turn_is_terminal_tx(
                &mut transaction,
                &current.session_id,
                session_generation,
                target_turn_id,
            )? {
                return Err(session::SessionError::StaleExecutionFence(format!(
                    "target turn `{target_turn_id}` became terminal before input `{request_id}` attachment"
                )));
            }
        }
        let runtime_commit_cursor = (acknowledged_status != SessionRuntimeInputStatus::Attached)
            .then(|| to_u64_i64(runtime_commit_cursor, "runtime cursor"))
            .transpose()?;
        let terminal_at_ms = (acknowledged_status != SessionRuntimeInputStatus::Attached)
            .then(|| to_u64_i64(now_ms, "runtime clock"))
            .transpose()?;
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                SET status=$1,runtime_commit_cursor=$2,claim_owner=NULL,claim_token=NULL,
                    claim_fence_epoch=NULL,
                    claim_expires_at_ms=NULL,terminal_at_ms=$3,failure_class=NULL,last_error=NULL,
                    updated_at_ms=$4,revision=revision+1
              WHERE request_id=$5 AND status='running' AND session_generation=$6
                AND claim_owner=$7 AND claim_token=$8 AND revision=$9",
                &[
                    &acknowledged_status.as_str(),
                    &runtime_commit_cursor,
                    &terminal_at_ms,
                    &to_u64_i64(now_ms, "runtime clock")?,
                    &request_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &worker_id,
                    &claim_token,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "outbox `{request_id}` changed during terminal transition"
            )));
        }
        let record = runtime_outbox_for_update(&mut transaction, request_id)?;
        append_runtime_history_tx(
            &mut transaction,
            &record,
            "ack",
            Some(worker_id),
            Some(expected_revision),
            current.status,
            acknowledged_status,
            None,
            now_ms,
        )?;
        append_input_timeline_event_tx(
            &mut transaction,
            &request_from_outbox(&record),
            &record.session_id,
            record.sequence,
            acknowledged_status.timeline_event_kind(),
            acknowledged_status,
            Some(worker_id),
            None,
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

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
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        if lease_ms == 0 {
            return Err(session::SessionError::Store(
                "outbox lease renewal requires a positive lease".to_string(),
            ));
        }
        let now = to_u64_i64(now_ms, "runtime outbox clock")?;
        let expires = now
            .checked_add(to_u64_i64(lease_ms, "runtime outbox lease")?)
            .ok_or_else(|| {
                session::SessionError::Store("runtime outbox lease overflow".to_string())
            })?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let existing = runtime_outbox_for_update(&mut transaction, request_id)?;
        assert_runtime_lease(
            &mut transaction,
            &existing,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[
                SessionRuntimeInputStatus::Claimed,
                SessionRuntimeInputStatus::Running,
            ],
        )?;
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                SET claim_expires_at_ms=$1,updated_at_ms=$2,revision=revision+1
              WHERE request_id=$3 AND status IN ('claimed','running')
                AND session_generation=$4 AND claim_owner=$5
                AND claim_token=$6 AND revision=$7",
                &[
                    &expires,
                    &now,
                    &request_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &worker_id,
                    &claim_token,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "outbox lease for `{request_id}` changed during renewal"
            )));
        }
        let record = runtime_outbox_for_update(&mut transaction, request_id)?;
        append_runtime_history_tx(
            &mut transaction,
            &record,
            "renew_lease",
            Some(worker_id),
            Some(expected_revision),
            existing.status,
            existing.status,
            None,
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

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
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let existing = runtime_outbox_for_update(&mut transaction, request_id)?;
        assert_runtime_lease(
            &mut transaction,
            &existing,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[
                SessionRuntimeInputStatus::Claimed,
                SessionRuntimeInputStatus::Running,
            ],
        )?;
        let retry = failure_class == OutboxFailureClass::Retryable
            && existing.attempts < max_attempts.max(1);
        let next = if retry {
            SessionRuntimeInputStatus::Queued
        } else if matches!(
            failure_class,
            OutboxFailureClass::AuthorizationBlocked | OutboxFailureClass::CorruptPayload
        ) {
            SessionRuntimeInputStatus::Blocked
        } else {
            SessionRuntimeInputStatus::Failed
        };
        let now = to_u64_i64(now_ms, "runtime outbox clock")?;
        let retry_at = if retry {
            to_u64_i64(retry_at_ms, "runtime outbox retry")?
        } else {
            now
        };
        let row = transaction
            .query_one(
                "UPDATE session_runtime_outbox
                SET status=$1,next_attempt_at_ms=$2,claim_owner=NULL,claim_token=NULL,
                    claim_fence_epoch=NULL,
                    claim_expires_at_ms=NULL,terminal_at_ms=$3,failure_class=$4,last_error=$5,
                    updated_at_ms=$6,revision=revision+1
              WHERE request_id=$7 AND status IN ('claimed','running')
                AND session_generation=$8 AND claim_owner=$9
                AND claim_token=$10 AND revision=$11
            RETURNING input_id,request_id,turn_id,message_id,session_id,sequence,
                      session_generation,decision,target_turn_id,classification_json,task_route_hint_json,status,
                      runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                      claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                      updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json",
                &[
                    &next.as_str(),
                    &retry_at,
                    &if next == SessionRuntimeInputStatus::Failed {
                        Some(now)
                    } else {
                        None
                    },
                    &failure_class.as_str(),
                    &error,
                    &now,
                    &request_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &worker_id,
                    &claim_token,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        let record = row_to_runtime_outbox(&row)?;
        append_runtime_history_tx(
            &mut transaction,
            &record,
            if retry {
                "retry"
            } else if next == SessionRuntimeInputStatus::Blocked {
                "block"
            } else {
                "fail"
            },
            Some(worker_id),
            Some(expected_revision),
            existing.status,
            next,
            Some(error),
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

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
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        let validation = SessionRuntimeOutboxRequest {
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
        validate_runtime_input_request(&validation)?;
        if worker_id.trim().is_empty() || claim_token.trim().is_empty() || reason.trim().is_empty()
        {
            return Err(session::SessionError::Store(
                "claimed input requeue requires worker, claim token, and reason".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current = runtime_outbox_for_update(&mut transaction, request_id)?;
        assert_runtime_lease(
            &mut transaction,
            &current,
            worker_id,
            session_generation,
            claim_token,
            expected_revision,
            now_ms,
            &[
                SessionRuntimeInputStatus::Claimed,
                SessionRuntimeInputStatus::Running,
            ],
        )?;
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                SET decision=$1,target_turn_id=$2,classification_json=$3,status='reclassified',
                    next_attempt_at_ms=$4,claim_owner=NULL,claim_token=NULL,
                    claim_fence_epoch=NULL,
                    claim_expires_at_ms=NULL,failure_class=NULL,last_error=NULL,terminal_at_ms=NULL,
                    updated_at_ms=$4,revision=revision+1
              WHERE request_id=$5 AND session_generation=$6 AND claim_owner=$7
                AND claim_token=$8 AND revision=$9 AND status IN ('claimed','running')",
                &[
                    &input_decision_as_str(decision),
                    &target_turn_id,
                    &classification_json,
                    &to_u64_i64(now_ms, "runtime clock")?,
                    &request_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &worker_id,
                    &claim_token,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "outbox `{request_id}` changed during claimed requeue"
            )));
        }
        let updated = runtime_outbox_for_update(&mut transaction, request_id)?;
        append_runtime_history_tx(
            &mut transaction,
            &updated,
            "owner_reclassify_requeue",
            Some(worker_id),
            Some(expected_revision),
            current.status,
            SessionRuntimeInputStatus::Reclassified,
            Some(reason),
            now_ms,
        )?;
        append_input_timeline_event_tx(
            &mut transaction,
            &request_from_outbox(&updated),
            &updated.session_id,
            updated.sequence,
            "session.input.reclassified.v1",
            updated.status,
            Some(worker_id),
            Some(reason),
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(updated)
    }

    pub fn retry_blocked_session_runtime_outbox(
        &self,
        request_id: &str,
        session_generation: u64,
        expected_revision: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(session::SessionError::Store(
                "manual outbox retry requires actor and reason".to_string(),
            ));
        }
        let now = to_u64_i64(now_ms, "runtime outbox clock")?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let existing = runtime_outbox_for_update(&mut transaction, request_id)?;
        require_input_admission_tx(&mut transaction, &existing.session_id, session_generation)?;
        if existing.status != SessionRuntimeInputStatus::Blocked
            || existing.session_generation != session_generation
            || existing.revision != expected_revision
        {
            return Err(session::SessionError::Store(format!(
                "outbox `{request_id}` is not blocked at revision {expected_revision}"
            )));
        }
        let row = transaction
            .query_one(
                "UPDATE session_runtime_outbox
                SET status='queued',next_attempt_at_ms=$1,claim_owner=NULL,claim_token=NULL,
                    claim_fence_epoch=NULL,
                    claim_expires_at_ms=NULL,failure_class=NULL,last_error=NULL,
                    terminal_at_ms=NULL,updated_at_ms=$1,revision=revision+1
              WHERE request_id=$2 AND session_generation=$3 AND revision=$4 AND status='blocked'
            RETURNING input_id,request_id,turn_id,message_id,session_id,sequence,
                      session_generation,decision,target_turn_id,classification_json,task_route_hint_json,status,
                      runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                      claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                      updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json",
                &[
                    &now,
                    &request_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        let record = row_to_runtime_outbox(&row)?;
        append_runtime_history_tx(
            &mut transaction,
            &record,
            "manual_retry",
            Some(actor),
            Some(expected_revision),
            SessionRuntimeInputStatus::Blocked,
            SessionRuntimeInputStatus::Queued,
            Some(reason),
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(record)
    }

    pub fn cancel_session_runtime_outbox(
        &self,
        input_id: &str,
        session_generation: u64,
        expected_revision: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(session::SessionError::Store(
                "session input cancellation requires actor and reason".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current = runtime_outbox_by_input_id_for_update(&mut transaction, input_id)?;
        if current.session_generation != session_generation
            || current.revision != expected_revision
            || current.status.is_terminal()
        {
            return Err(session::SessionError::Store(format!(
                "session input `{input_id}` cannot be cancelled at generation {session_generation} revision {expected_revision}"
            )));
        }
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                SET status='cancelled',claim_owner=NULL,claim_token=NULL,
                    claim_fence_epoch=NULL,
                    claim_expires_at_ms=NULL,last_error=$1,terminal_at_ms=$2,
                    updated_at_ms=$2,revision=revision+1
              WHERE input_id=$3 AND session_generation=$4 AND revision=$5
                AND status NOT IN ('completed','supplemented','failed','cancelled','expired')",
                &[
                    &reason,
                    &to_u64_i64(now_ms, "runtime clock")?,
                    &input_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "session input `{input_id}` changed during cancellation"
            )));
        }
        let updated = runtime_outbox_by_input_id_for_update(&mut transaction, input_id)?;
        append_runtime_history_tx(
            &mut transaction,
            &updated,
            "cancel",
            Some(actor),
            Some(expected_revision),
            current.status,
            SessionRuntimeInputStatus::Cancelled,
            Some(reason),
            now_ms,
        )?;
        append_input_timeline_event_tx(
            &mut transaction,
            &request_from_outbox(&updated),
            &updated.session_id,
            updated.sequence,
            "session.input.cancelled.v1",
            updated.status,
            Some(actor),
            Some(reason),
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(updated)
    }

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
    ) -> session::SessionResult<SessionRuntimeOutboxRecord> {
        let validation = SessionRuntimeOutboxRequest {
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
        validate_runtime_input_request(&validation)?;
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(session::SessionError::Store(
                "session input reclassification requires actor and reason".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current = runtime_outbox_by_input_id_for_update(&mut transaction, input_id)?;
        require_input_admission_tx(&mut transaction, &current.session_id, session_generation)?;
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
            return Err(session::SessionError::Store(format!(
                "session input `{input_id}` is not reclassifiable at generation {session_generation} revision {expected_revision}"
            )));
        }
        let changed = transaction
            .execute(
                "UPDATE session_runtime_outbox
                SET decision=$1,target_turn_id=$2,classification_json=$3,status='reclassified',
                    next_attempt_at_ms=$4,failure_class=NULL,last_error=NULL,terminal_at_ms=NULL,
                    claim_owner=NULL,claim_token=NULL,claim_fence_epoch=NULL,
                    claim_expires_at_ms=NULL,
                    updated_at_ms=$4,revision=revision+1
              WHERE input_id=$5 AND session_generation=$6 AND revision=$7
                AND status IN (
                  'accepted','classified','queued','reclassified','attached','blocked'
                )",
                &[
                    &input_decision_as_str(decision),
                    &target_turn_id,
                    &classification_json,
                    &to_u64_i64(now_ms, "runtime clock")?,
                    &input_id,
                    &to_u64_i64(session_generation, "session generation")?,
                    &to_u64_i64(expected_revision, "runtime revision")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "session input `{input_id}` changed during reclassification"
            )));
        }
        let updated = runtime_outbox_by_input_id_for_update(&mut transaction, input_id)?;
        append_runtime_history_tx(
            &mut transaction,
            &updated,
            "reclassify",
            Some(actor),
            Some(expected_revision),
            current.status,
            SessionRuntimeInputStatus::Reclassified,
            Some(reason),
            now_ms,
        )?;
        append_input_timeline_event_tx(
            &mut transaction,
            &request_from_outbox(&updated),
            &updated.session_id,
            updated.sequence,
            "session.input.reclassified.v1",
            updated.status,
            Some(actor),
            Some(reason),
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(updated)
    }

    pub fn set_session_input_application_receipt(
        &self,
        input_ids: &[String],
        expected_revisions: &[u64],
        receipt: &harness_contract::input_disposition::SessionInputApplicationReceipt,
        now_ms: u64,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        receipt
            .validate_shape()
            .map_err(session::SessionError::InvalidArgument)?;
        if input_ids.is_empty() || input_ids.len() != expected_revisions.len() {
            return Err(session::SessionError::InvalidArgument(
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
            return Err(session::SessionError::InvalidArgument(
                "application receipt input set does not match the fenced update set".to_string(),
            ));
        }
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let mut current = Vec::with_capacity(input_ids.len());
        for (input_id, expected_revision) in input_ids.iter().zip(expected_revisions) {
            let record = runtime_outbox_by_input_id_for_update(&mut transaction, input_id)?;
            if record.revision != *expected_revision {
                return Err(session::SessionError::StaleExecutionFence(format!(
                    "session input `{input_id}` revision {} does not match {expected_revision}",
                    record.revision
                )));
            }
            if !receipt.can_follow(record.application_receipt.as_ref()) {
                return Err(session::SessionError::InvalidArgument(format!(
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
            return Err(session::SessionError::InvalidArgument(
                "application receipt leader or Session scope is invalid".to_string(),
            ));
        }
        let receipt_json = serde_json::to_string(receipt)
            .map_err(|error| session::SessionError::Store(error.to_string()))?;
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
            let changed = transaction
                .execute(
                    "UPDATE session_runtime_outbox
                        SET application_receipt_json=$1,decision=$2,target_turn_id=$3,
                            status=$4,terminal_at_ms=$5,
                            next_attempt_at_ms=CASE WHEN $6 THEN $7 ELSE next_attempt_at_ms END,
                            claim_owner=CASE WHEN $6 THEN NULL ELSE claim_owner END,
                            claim_token=CASE WHEN $6 THEN NULL ELSE claim_token END,
                            claim_fence_epoch=CASE WHEN $6 THEN NULL ELSE claim_fence_epoch END,
                            claim_expires_at_ms=CASE WHEN $6 THEN NULL ELSE claim_expires_at_ms END,
                            updated_at_ms=$7,revision=revision+1
                      WHERE input_id=$8 AND revision=$9",
                    &[
                        &receipt_json,
                        &input_decision_as_str(decision),
                        &target_turn_id,
                        &status.as_str(),
                        &terminal_at_ms
                            .map(|value| to_u64_i64(value, "terminal timestamp"))
                            .transpose()?,
                        &reclassified,
                        &to_u64_i64(now_ms, "runtime clock")?,
                        input_id,
                        &to_u64_i64(*expected_revision, "runtime revision")?,
                    ],
                )
                .map_err(postgres_error)?;
            if changed != 1 {
                return Err(session::SessionError::StaleExecutionFence(format!(
                    "session input `{input_id}` changed during application receipt commit"
                )));
            }
        }
        let mut updated = Vec::with_capacity(input_ids.len());
        for input_id in input_ids {
            updated.push(runtime_outbox_by_input_id_for_update(
                &mut transaction,
                input_id,
            )?);
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(updated)
    }

    pub fn get_session_input_admission(
        &self,
        session_id: &str,
    ) -> session::SessionResult<Option<SessionInputAdmission>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT session_id,input_generation,input_admission_open
                   FROM session_records WHERE session_id=$1",
                &[&session_id],
            )
            .map_err(postgres_error)?
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
            .transpose()
    }

    pub fn close_session_input_admission(
        &self,
        session_id: &str,
        expected_generation: u64,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> session::SessionResult<SessionInputAdmission> {
        self.advance_session_input_generation(
            session_id,
            expected_generation,
            false,
            actor,
            reason,
            now_ms,
        )
    }

    pub fn advance_session_input_generation(
        &self,
        session_id: &str,
        expected_generation: u64,
        open: bool,
        actor: &str,
        reason: &str,
        now_ms: u64,
    ) -> session::SessionResult<SessionInputAdmission> {
        if actor.trim().is_empty() || reason.trim().is_empty() {
            return Err(session::SessionError::Store(
                "session generation advance requires actor and reason".to_string(),
            ));
        }
        let next_generation = expected_generation.checked_add(1).ok_or_else(|| {
            session::SessionError::Store("session generation overflow".to_string())
        })?;
        let mut connection = self.executor.checkout_critical().map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        let current =
            query_input_admission_tx(&mut transaction, session_id, true)?.ok_or_else(|| {
                session::SessionError::Store(format!("session `{session_id}` not found"))
            })?;
        if current.generation != expected_generation {
            return Err(session::SessionError::Store(format!(
                "session `{session_id}` generation changed from expected {expected_generation}"
            )));
        }
        let rows = transaction
            .query(
                "SELECT request_id,status,revision
               FROM session_runtime_outbox
              WHERE session_id=$1 AND session_generation=$2
                AND status IN (
                    'accepted','classified','queued','claimed','running','reclassified','blocked'
                )
              ORDER BY sequence ASC,request_id ASC FOR UPDATE",
                &[
                    &session_id,
                    &to_u64_i64(expected_generation, "session generation")?,
                ],
            )
            .map_err(postgres_error)?;
        let changed = transaction
            .execute(
                "UPDATE session_records
                SET input_generation=$1,input_admission_open=$2,
                    updated_at_ms=GREATEST(updated_at_ms,$3)
              WHERE session_id=$4 AND input_generation=$5",
                &[
                    &to_u64_i64(next_generation, "session generation")?,
                    &open,
                    &to_u64_i64(now_ms, "runtime clock")?,
                    &session_id,
                    &to_u64_i64(expected_generation, "session generation")?,
                ],
            )
            .map_err(postgres_error)?;
        if changed != 1 {
            return Err(session::SessionError::Store(format!(
                "session `{session_id}` generation changed during advance"
            )));
        }
        for row in rows {
            let request_id: String = row.try_get(0).map_err(postgres_error)?;
            let previous =
                parse_runtime_status(&row.try_get::<_, String>(1).map_err(postgres_error)?)?;
            let revision = i64_to_u64(row.try_get(2).map_err(postgres_error)?, "runtime revision")?;
            let expired = transaction
                .query_one(
                    "UPDATE session_runtime_outbox
                    SET status='expired',claim_owner=NULL,claim_token=NULL,
                        claim_fence_epoch=NULL,
                        claim_expires_at_ms=NULL,last_error=$1,terminal_at_ms=$2,
                        updated_at_ms=$2,revision=revision+1
                  WHERE request_id=$3 AND session_generation=$4 AND revision=$5
                RETURNING input_id,request_id,turn_id,message_id,session_id,sequence,
                          session_generation,decision,target_turn_id,classification_json,task_route_hint_json,status,
                          runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                          claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                          updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json",
                    &[
                        &reason,
                        &to_u64_i64(now_ms, "runtime clock")?,
                        &request_id,
                        &to_u64_i64(expected_generation, "session generation")?,
                        &to_u64_i64(revision, "runtime revision")?,
                    ],
                )
                .map_err(postgres_error)?;
            let expired = row_to_runtime_outbox(&expired)?;
            append_runtime_history_tx(
                &mut transaction,
                &expired,
                "generation_expire",
                Some(actor),
                Some(revision),
                previous,
                SessionRuntimeInputStatus::Expired,
                Some(reason),
                now_ms,
            )?;
        }
        let admission =
            query_input_admission_tx(&mut transaction, session_id, false)?.ok_or_else(|| {
                session::SessionError::Store(format!(
                    "session `{session_id}` disappeared after generation advance"
                ))
            })?;
        append_admission_timeline_event_tx(
            &mut transaction,
            session_id,
            expected_generation,
            &admission,
            actor,
            reason,
            now_ms,
        )?;
        transaction.commit().map_err(postgres_error)?;
        Ok(admission)
    }

    pub fn get_session_runtime_outbox(
        &self,
        request_id: &str,
    ) -> session::SessionResult<Option<SessionRuntimeOutboxRecord>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(RUNTIME_OUTBOX_SELECT, &[&request_id])
            .map_err(postgres_error)?
            .map(|row| row_to_runtime_outbox(&row))
            .transpose()
    }

    pub fn get_session_runtime_outbox_by_input_id(
        &self,
        input_id: &str,
    ) -> session::SessionResult<Option<SessionRuntimeOutboxRecord>> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        connection
            .query_opt(
                "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                        session_generation,decision,target_turn_id,classification_json,task_route_hint_json,status,
                        runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                        claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                        updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json
                   FROM session_runtime_outbox WHERE input_id=$1",
                &[&input_id],
            )
            .map_err(postgres_error)?
            .map(|row| row_to_runtime_outbox(&row))
            .transpose()
    }

    pub fn session_runtime_outbox_for_session(
        &self,
        session_id: &str,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        self.query_runtime_outbox(
            "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                    session_generation,decision,target_turn_id,classification_json,task_route_hint_json,status,
                    runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                    claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                    updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json FROM session_runtime_outbox
              WHERE session_id=$1 ORDER BY updated_at_ms DESC,sequence DESC,request_id DESC LIMIT $2",
            &[&session_id,&to_i64(limit.clamp(1,500), "runtime outbox limit")?],
        )
    }

    pub fn session_runtime_outbox_for_turn_relation(
        &self,
        session_id: &str,
        session_generation: u64,
        turn_id: &str,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        self.query_runtime_outbox(
            "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                    session_generation,decision,target_turn_id,classification_json,task_route_hint_json,status,
                    runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                    claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                    updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json
               FROM session_runtime_outbox
              WHERE session_id=$1 AND session_generation=$2
                AND (turn_id=$3 OR target_turn_id=$3)
              ORDER BY sequence ASC,request_id ASC",
            &[
                &session_id,
                &to_u64_i64(session_generation, "session generation")?,
                &turn_id,
            ],
        )
    }

    pub fn session_runtime_outbox_for_sessions(
        &self,
        session_ids: &[String],
        per_session_limit: usize,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        if session_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.query_runtime_outbox(
            "WITH ranked AS (
                 SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                        session_generation,decision,target_turn_id,classification_json,task_route_hint_json,status,
                        runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                        claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                        updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json,
                        ROW_NUMBER() OVER (
                            PARTITION BY session_id
                            ORDER BY updated_at_ms DESC,sequence DESC,request_id DESC
                        ) AS row_number
                   FROM session_runtime_outbox
                  WHERE session_id = ANY($1::text[])
                    AND target_turn_id IS NULL
                    AND decision NOT IN ('reject_duplicate','reject_policy')
             )
             SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                    session_generation,decision,target_turn_id,classification_json,task_route_hint_json,status,
                    runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                    claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                    updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json
               FROM ranked
              WHERE row_number <= $2
              ORDER BY session_id ASC,updated_at_ms DESC,sequence DESC,request_id DESC",
            &[
                &session_ids,
                &to_i64(
                    bounded_limit(per_session_limit, 1, 500),
                    "runtime outbox per-session limit",
                )?,
            ],
        )
    }

    pub fn active_session_runtime_outbox(
        &self,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        self.query_runtime_outbox(
            "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                    session_generation,decision,target_turn_id,classification_json,task_route_hint_json,status,
                    runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                    claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                    updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json FROM session_runtime_outbox
              WHERE status NOT IN ('completed','supplemented','failed','cancelled','expired')
              ORDER BY updated_at_ms DESC,sequence DESC,request_id DESC LIMIT $1",
            &[&to_i64(bounded_limit(limit, 1, 500), "runtime outbox limit")?],
        )
    }

    pub fn blocked_session_runtime_outbox(
        &self,
        limit: usize,
    ) -> session::SessionResult<Vec<SessionRuntimeOutboxRecord>> {
        self.query_runtime_outbox(
            "SELECT input_id,request_id,turn_id,message_id,session_id,sequence,
                    session_generation,decision,target_turn_id,classification_json,task_route_hint_json,status,
                    runtime_commit_cursor,attempts,next_attempt_at_ms,claim_owner,claim_token,
                    claim_expires_at_ms,failure_class,last_error,revision,created_at_ms,
                    updated_at_ms,terminal_at_ms,runtime_options_json,claim_fence_epoch,application_receipt_json FROM session_runtime_outbox
              WHERE status='blocked' ORDER BY updated_at_ms ASC,sequence ASC,request_id ASC LIMIT $1",
            &[&to_i64(limit.clamp(1,500), "runtime outbox limit")?],
        )
    }

    pub fn session_runtime_outbox_health(
        &self,
    ) -> session::SessionResult<SessionRuntimeOutboxHealth> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let rows = connection
            .query(
                "SELECT status,COUNT(*) FROM session_runtime_outbox GROUP BY status",
                &[],
            )
            .map_err(postgres_error)?;
        let mut health = SessionRuntimeOutboxHealth::default();
        for row in rows {
            let status: String = row.try_get(0).map_err(postgres_error)?;
            let count = from_i64(
                row.try_get(1).map_err(postgres_error)?,
                "runtime outbox count",
            )?;
            match parse_runtime_status(&status)? {
                SessionRuntimeInputStatus::Accepted => health.accepted = count,
                SessionRuntimeInputStatus::Classified => health.classified = count,
                SessionRuntimeInputStatus::Queued => health.queued = count,
                SessionRuntimeInputStatus::RejectedDuplicate => {
                    health.rejected_duplicate = count;
                }
                SessionRuntimeInputStatus::RejectedPolicy => {
                    health.rejected_policy = count;
                }
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
        health.oldest_runnable_created_at_ms = connection
            .query_one(
                "SELECT MIN(created_at_ms) FROM session_runtime_outbox
                  WHERE status IN ('accepted','classified','queued','claimed','running','reclassified')",
                &[],
            )
            .map_err(postgres_error)?
            .try_get::<_, Option<i64>>(0)
            .map_err(postgres_error)?
            .map(|value| value.max(0) as u64);
        Ok(health)
    }
}
