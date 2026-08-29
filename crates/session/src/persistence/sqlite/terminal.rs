//! Terminal operations for the SqliteSessionStore adapter.

use super::*;

impl SqliteSessionStore {
    // -----------------------------------------------------------------------
    // Message persistence
    // -----------------------------------------------------------------------

    /// Insert a single message (INSERT OR REPLACE on the (session_id, sequence)
    /// unique constraint).
    pub fn insert_message(&self, msg: &SessionMessage) -> Result<()> {
        let conn = self.conn()?;
        let message_id = if msg.stable_message_id.trim().is_empty() {
            legacy_message_id(&msg.session_id, msg.sequence)
        } else {
            msg.stable_message_id.clone()
        };
        conn.execute(
            r"INSERT INTO messages
                (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                 tool_use_id, tool_name, token_usage_json, created_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
               ON CONFLICT(session_id, sequence) DO UPDATE SET
                   role = excluded.role,
                   content_json = excluded.content_json,
                   blocks_count = excluded.blocks_count,
                   tool_use_id = excluded.tool_use_id,
                   tool_name = excluded.tool_name,
                   token_usage_json = excluded.token_usage_json,
                   created_at_ms = excluded.created_at_ms",
            params![
                message_id,
                msg.session_id,
                msg.sequence as i64,
                msg.role,
                msg.content_json,
                msg.blocks_count as i64,
                msg.tool_use_id,
                msg.tool_name,
                msg.token_usage_json,
                msg.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    pub fn commit_terminal_transcript_if_fenced(
        &self,
        request: &SessionTerminalTranscriptCommit,
    ) -> Result<SessionTerminalTranscriptReceipt> {
        validate_terminal_transcript(
            &request.terminal_message_id,
            &request.ingress_message_id,
            &request.session_id,
            &request.messages,
        )?;
        validate_terminal_commit(request)?;
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let admission = query_input_admission(&tx, &request.session_id)?.ok_or_else(|| {
            SessionError::StaleExecutionFence(format!(
                "session `{}` no longer exists",
                request.session_id
            ))
        })?;
        let current = query_outbox(&tx, &request.fence.request_id)?.ok_or_else(|| {
            SessionError::StaleExecutionFence(format!(
                "input `{}` no longer exists",
                request.fence.request_id
            ))
        })?;
        if current.status == SessionRuntimeInputStatus::Completed
            && current.runtime_commit_cursor == Some(request.runtime_commit_cursor)
        {
            if current.session_id != request.session_id
                || current.message_id != request.ingress_message_id
                || current.turn_id != request.turn_id
                || current.sequence != request.fence.input_sequence
                || current.session_generation != request.fence.session_generation
                || current.claim_owner.as_deref() != Some(request.fence.claim_owner.as_str())
                || current.claim_token.as_deref() != Some(request.fence.claim_token.as_str())
                || current.claim_fence_epoch != Some(request.fence.claim_fence_epoch)
            {
                return Err(SessionError::StaleExecutionFence(format!(
                    "completed input `{}` identity does not match terminal replay",
                    request.fence.request_id
                )));
            }
            let messages = load_committed_terminal_transcript_tx(
                &tx,
                &request.terminal_message_id,
                &request.messages,
            )?;
            tx.commit().map_err(sql_err)?;
            return Ok(SessionTerminalTranscriptReceipt {
                messages,
                inserted: false,
                input: current,
            });
        }
        let fence_valid = current.session_id == request.session_id
            && current.message_id == request.ingress_message_id
            && current.turn_id == request.turn_id
            && current.sequence == request.fence.input_sequence
            && current.status == SessionRuntimeInputStatus::Running
            && current.session_generation == request.fence.session_generation
            && admission.generation == request.fence.session_generation
            && admission.open
            && current.claim_owner.as_deref() == Some(request.fence.claim_owner.as_str())
            && current.claim_token.as_deref() == Some(request.fence.claim_token.as_str())
            && current.claim_fence_epoch == Some(request.fence.claim_fence_epoch)
            && current
                .claim_expires_at_ms
                .is_some_and(|expires| expires > request.created_at_ms);
        if !fence_valid {
            return Err(SessionError::StaleExecutionFence(format!(
                "request={} generation={} claim_fence_epoch={} current_status={:?} current_revision={}",
                request.fence.request_id,
                request.fence.session_generation,
                request.fence.claim_fence_epoch,
                current.status,
                current.revision
            )));
        }
        let newest_pending_sequence = tx
            .query_row(
                r"SELECT MAX(sequence)
                     FROM session_runtime_outbox
                    WHERE session_id=?1 AND session_generation=?2
                      AND sequence>?3
                      AND status NOT IN (
                        'rejected_duplicate','rejected_policy','completed',
                        'supplemented','failed','cancelled','expired'
                      )
                      AND decision IN (
                        'supplement_current_turn',
                        'interrupt_and_replan',
                        'control_or_approval'
                      )",
                params![
                    request.session_id,
                    request.fence.session_generation as i64,
                    request.fence.input_sequence as i64,
                ],
                |row| row.get::<_, Option<i64>>(0),
            )
            .map_err(sql_err)?
            .map(|value| value.max(0) as usize);
        if newest_pending_sequence
            .is_some_and(|sequence| sequence > request.consumed_input_sequence)
        {
            return Err(SessionError::StaleExecutionFence(format!(
                "terminal input cursor {} is behind pending Session input {}",
                request.consumed_input_sequence,
                newest_pending_sequence.unwrap_or_default()
            )));
        }
        let consumed_request_ids = {
            let mut statement = tx
                .prepare(
                    r"SELECT request_id
                         FROM session_runtime_outbox
                        WHERE session_id=?1 AND session_generation=?2
                          AND sequence>?3 AND sequence<=?4
                          AND status IN (
                            'accepted','classified','queued','claimed','running',
                            'reclassified','attached'
                          )
                          AND decision IN (
                            'supplement_current_turn',
                            'interrupt_and_replan',
                            'control_or_approval'
                          )
                        ORDER BY sequence ASC",
                )
                .map_err(sql_err)?;
            let request_ids = statement
                .query_map(
                    params![
                        request.session_id,
                        request.fence.session_generation as i64,
                        request.fence.input_sequence as i64,
                        request.consumed_input_sequence as i64,
                    ],
                    |row| row.get::<_, String>(0),
                )
                .map_err(sql_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_err)?;
            request_ids
        };
        for request_id in consumed_request_ids {
            let before = query_outbox(&tx, &request_id)?.ok_or_else(|| {
                SessionError::Store(format!(
                    "consumed Session input `{request_id}` disappeared during terminal commit"
                ))
            })?;
            let changed = tx
                .execute(
                    r"UPDATE session_runtime_outbox
                          SET status='supplemented', terminal_at_ms=?1,
                              runtime_commit_cursor=?2,
                              claim_owner=NULL, claim_token=NULL,
                              claim_fence_epoch=NULL, claim_expires_at_ms=NULL,
                              failure_class=NULL, last_error=NULL,
                              updated_at_ms=?1, revision=revision+1
                        WHERE request_id=?3 AND revision=?4
                          AND status IN (
                            'accepted','classified','queued','claimed','running',
                            'reclassified','attached'
                          )",
                    params![
                        request.created_at_ms as i64,
                        request.runtime_commit_cursor as i64,
                        request_id,
                        before.revision as i64,
                    ],
                )
                .map_err(sql_err)?;
            if changed != 1 {
                return Err(SessionError::StaleExecutionFence(format!(
                    "consumed Session input `{request_id}` changed during terminal commit"
                )));
            }
            let supplemented = query_outbox(&tx, &request_id)?.ok_or_else(|| {
                SessionError::Store(format!(
                    "supplemented Session input `{request_id}` disappeared"
                ))
            })?;
            append_outbox_history(
                &tx,
                &supplemented,
                "terminal_input_cursor_commit",
                Some(&request.fence.claim_owner),
                None,
                before.status.as_str(),
                SessionRuntimeInputStatus::Supplemented.as_str(),
                request.created_at_ms,
            )?;
            append_input_timeline_event(
                &tx,
                &request_from_outbox(&supplemented),
                &supplemented.session_id,
                supplemented.sequence,
                SessionRuntimeInputStatus::Supplemented.timeline_event_kind(),
                SessionRuntimeInputStatus::Supplemented,
                Some(&request.fence.claim_owner),
                None,
                request.created_at_ms,
            )?;
        }
        let (messages, inserted) = append_terminal_transcript_tx(
            &tx,
            &request.terminal_message_id,
            &request.ingress_message_id,
            &request.session_id,
            &request.messages,
            request.created_at_ms,
        )?;
        let changed = tx
            .execute(
                r"UPDATE session_runtime_outbox
                      SET status='completed', runtime_commit_cursor=?1,
                          claim_expires_at_ms=NULL,
                          terminal_at_ms=?2, failure_class=NULL, last_error=NULL,
                          updated_at_ms=?2, revision=revision+1
                    WHERE request_id=?3 AND sequence=?4 AND status='running'
                      AND session_generation=?5 AND claim_owner=?6
                      AND claim_token=?7 AND claim_fence_epoch=?8 AND revision=?9",
                params![
                    request.runtime_commit_cursor as i64,
                    request.created_at_ms as i64,
                    request.fence.request_id,
                    request.fence.input_sequence as i64,
                    request.fence.session_generation as i64,
                    request.fence.claim_owner,
                    request.fence.claim_token,
                    request.fence.claim_fence_epoch as i64,
                    current.revision as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::StaleExecutionFence(format!(
                "input `{}` changed during terminal commit",
                request.fence.request_id
            )));
        }
        let completed = query_outbox(&tx, &request.fence.request_id)?.ok_or_else(|| {
            SessionError::Store(format!(
                "completed input `{}` disappeared",
                request.fence.request_id
            ))
        })?;
        append_outbox_history(
            &tx,
            &completed,
            "terminal_commit",
            Some(&request.fence.claim_owner),
            None,
            SessionRuntimeInputStatus::Running.as_str(),
            SessionRuntimeInputStatus::Completed.as_str(),
            request.created_at_ms,
        )?;
        append_input_timeline_event(
            &tx,
            &request_from_outbox(&completed),
            &completed.session_id,
            completed.sequence,
            SessionRuntimeInputStatus::Completed.timeline_event_kind(),
            SessionRuntimeInputStatus::Completed,
            Some(&request.fence.claim_owner),
            None,
            request.created_at_ms,
        )?;
        tx.commit().map_err(sql_err)?;
        Ok(SessionTerminalTranscriptReceipt {
            messages,
            inserted,
            input: completed,
        })
    }

    /// Insert multiple messages in a single transaction.
    pub fn insert_messages_batch(&self, messages: &[SessionMessage]) -> Result<()> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(sql_err)?;
        {
            let mut stmt = tx
                .prepare(
                    r"INSERT INTO messages
                       (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                        tool_use_id, tool_name, token_usage_json, created_at_ms)
                      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                      ON CONFLICT(session_id, sequence) DO UPDATE SET
                          role = excluded.role,
                          content_json = excluded.content_json,
                          blocks_count = excluded.blocks_count,
                          tool_use_id = excluded.tool_use_id,
                          tool_name = excluded.tool_name,
                          token_usage_json = excluded.token_usage_json,
                          created_at_ms = excluded.created_at_ms",
                )
                .map_err(sql_err)?;
            for msg in messages {
                stmt.execute(params![
                    if msg.stable_message_id.trim().is_empty() {
                        legacy_message_id(&msg.session_id, msg.sequence)
                    } else {
                        msg.stable_message_id.clone()
                    },
                    msg.session_id,
                    msg.sequence as i64,
                    msg.role,
                    msg.content_json,
                    msg.blocks_count as i64,
                    msg.tool_use_id,
                    msg.tool_name,
                    msg.token_usage_json,
                    msg.created_at_ms as i64,
                ])
                .map_err(sql_err)?;
            }
        }
        tx.commit().map_err(sql_err)?;
        Ok(())
    }

    pub fn copy_session_messages_at_cutoff(
        &self,
        source_session_id: &str,
        target_session_id: &str,
        source_message_count: usize,
    ) -> Result<usize> {
        if source_session_id.trim().is_empty()
            || target_session_id.trim().is_empty()
            || source_session_id == target_session_id
        {
            return Err(SessionError::Store(
                "branch copy requires distinct non-empty source and target sessions".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        for session_id in [source_session_id, target_session_id] {
            let exists = tx
                .query_row(
                    "SELECT 1 FROM sessions WHERE session_id = ?1",
                    params![session_id],
                    |_| Ok(()),
                )
                .optional()
                .map_err(sql_err)?
                .is_some();
            if !exists {
                return Err(SessionError::Store(format!(
                    "branch session `{session_id}` does not exist"
                )));
            }
        }
        let target_count = tx
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
                params![target_session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)?;
        if target_count != 0 {
            return Err(SessionError::Store(format!(
                "branch target `{target_session_id}` already contains messages"
            )));
        }
        let copied = tx
            .execute(
                r"INSERT INTO messages
                    (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                     tool_use_id, tool_name, token_usage_json, created_at_ms)
                  SELECT 'branch:' || ?2 || ':' || stable_message_id,
                         ?2, sequence, role, content_json, blocks_count,
                         tool_use_id, tool_name, token_usage_json, created_at_ms
                    FROM messages
                   WHERE session_id = ?1 AND sequence < ?3
                   ORDER BY sequence",
                params![
                    source_session_id,
                    target_session_id,
                    source_message_count as i64
                ],
            )
            .map_err(sql_err)?;
        let last_created_at = tx
            .query_row(
                "SELECT COALESCE(MAX(created_at_ms), 0) FROM messages WHERE session_id = ?1",
                params![target_session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)?
            .max(0) as u64;
        refresh_session_message_summary_tx(&tx, target_session_id, last_created_at)?;
        refresh_session_usage_summary_tx(&tx, target_session_id)?;
        tx.commit().map_err(sql_err)?;
        Ok(copied)
    }

    pub fn branch_session_at_cutoff(
        &self,
        request: &SessionBranchRequest,
    ) -> Result<SessionBranchResult> {
        if request.operation_id.trim().is_empty()
            || request.source_session_id.trim().is_empty()
            || request.target.session_id.trim().is_empty()
            || request.source_session_id == request.target.session_id
        {
            return Err(SessionError::Store(
                "branch requires distinct source and target identities".to_string(),
            ));
        }
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let source_exists = tx
            .query_row(
                "SELECT 1 FROM sessions WHERE session_id = ?1",
                params![request.source_session_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(sql_err)?
            .is_some();
        if !source_exists {
            return Err(SessionError::Store(format!(
                "branch source `{}` does not exist",
                request.source_session_id
            )));
        }
        if let Some(activation) = query_branch_activation(&tx, &request.operation_id)? {
            if activation.source_session_id != request.source_session_id
                || activation.target_session_id != request.target.session_id
                || activation.source_message_count != request.source_message_count
            {
                return Err(SessionError::Store(format!(
                    "branch operation `{}` is bound to another source/cutoff/target identity",
                    request.operation_id
                )));
            }
            let target = tx
                .query_row(
                    r"SELECT session_id, platform, chat_id, user_id, model,
                              created_at, last_activity, message_count, reset_policy,
                              metadata_json, input_tokens, output_tokens, status
                         FROM sessions WHERE session_id=?1",
                    params![request.target.session_id],
                    row_to_record,
                )
                .optional()
                .map_err(sql_err)?
                .ok_or_else(|| {
                    SessionError::Store(format!(
                        "branch operation `{}` has no durable target",
                        request.operation_id
                    ))
                })?;
            tx.commit().map_err(sql_err)?;
            return Ok(SessionBranchResult {
                target,
                copied_message_count: activation.source_message_count,
                source_message_count: activation.source_message_count,
                activation,
            });
        }
        let target_exists = tx
            .query_row(
                "SELECT 1 FROM sessions WHERE session_id = ?1",
                params![request.target.session_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(sql_err)?
            .is_some();
        if target_exists {
            return Err(SessionError::Store(format!(
                "branch target `{}` already exists",
                request.target.session_id
            )));
        }
        let source_count = tx
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
                params![request.source_session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)?;
        let source_count = usize::try_from(source_count).map_err(|_| {
            SessionError::Store("branch source message count exceeds usize".to_string())
        })?;
        if request.source_message_count > source_count {
            return Err(SessionError::Store(format!(
                "branch cutoff {} exceeds source message count {source_count}",
                request.source_message_count
            )));
        }
        let cutoff = request.source_message_count;

        tx.execute(
            r"INSERT INTO sessions
               (session_id, platform, chat_id, user_id, model,
                created_at, last_activity, message_count, reset_policy, metadata_json,
                input_tokens, output_tokens, status,
                created_at_ms, updated_at_ms)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8, ?9, 0, 0, ?10, ?11, ?12)",
            params![
                request.target.session_id,
                request.target.platform,
                request.target.chat_id,
                request.target.user_id,
                request.target.model,
                request.target.created_at,
                request.target.last_activity,
                request.target.reset_policy,
                request.target.metadata_json,
                request.target.status,
                iso_to_ms(&request.target.created_at),
                iso_to_ms(&request.target.last_activity),
            ],
        )
        .map_err(sql_err)?;
        let copied = tx
            .execute(
                r"INSERT INTO messages
                    (stable_message_id, session_id, sequence, role, content_json, blocks_count,
                     tool_use_id, tool_name, token_usage_json, created_at_ms)
                  SELECT 'branch:' || ?2 || ':' || stable_message_id,
                         ?2, sequence, role, content_json, blocks_count,
                         tool_use_id, tool_name, token_usage_json, created_at_ms
                    FROM messages
                   WHERE session_id = ?1 AND sequence < ?3
                   ORDER BY sequence",
                params![
                    request.source_session_id,
                    request.target.session_id,
                    i64::try_from(cutoff).map_err(|_| SessionError::Store(
                        "branch cutoff exceeds SQLite i64 range".to_string()
                    ))?
                ],
            )
            .map_err(sql_err)?;
        let last_created_at = tx
            .query_row(
                "SELECT COALESCE(MAX(created_at_ms), 0) FROM messages WHERE session_id = ?1",
                params![request.target.session_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(sql_err)?
            .max(0) as u64;
        refresh_session_message_summary_tx(&tx, &request.target.session_id, last_created_at)?;
        refresh_session_usage_summary_tx(&tx, &request.target.session_id)?;
        for (session_id, event_type, event_json) in [
            (
                request.source_session_id.as_str(),
                "SessionBranched",
                request.source_event_json.as_str(),
            ),
            (
                request.target.session_id.as_str(),
                "BranchCreated",
                request.target_event_json.as_str(),
            ),
        ] {
            let event_json = branch_event_json(event_json, copied, cutoff)?;
            let sequence: i64 = tx
                .query_row(
                    "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                    params![session_id],
                    |row| row.get(0),
                )
                .map_err(sql_err)?;
            let sequence_usize = usize::try_from(sequence).map_err(|_| {
                SessionError::Store("branch event sequence exceeds usize".to_string())
            })?;
            let event = SessionEvent {
                session_id: session_id.to_string(),
                event_type: event_type.to_string(),
                event_json,
                sequence: sequence_usize,
                created_at_ms: request.created_at_ms,
            };
            let event_json = event_json_with_allocated_sequence(&event, sequence_usize)?;
            tx.execute(
                r"INSERT INTO session_events
                   (session_id, event_type, event_json, sequence, created_at_ms)
                  VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    session_id,
                    event_type,
                    event_json,
                    sequence,
                    i64::try_from(request.created_at_ms).map_err(|_| SessionError::Store(
                        "branch timestamp exceeds SQLite i64 range".to_string()
                    ))?,
                ],
            )
            .map_err(sql_err)?;
        }
        tx.execute(
            r"INSERT INTO session_branch_activations
                (operation_id, source_session_id, target_session_id,
                 source_message_count, phase, created_at_ms, updated_at_ms,
                 last_error, revision)
               VALUES (?1, ?2, ?3, ?4, 'branch_committed', ?5, ?5, NULL, 0)",
            params![
                request.operation_id,
                request.source_session_id,
                request.target.session_id,
                cutoff as i64,
                request.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        let activation = query_branch_activation(&tx, &request.operation_id)?.ok_or_else(|| {
            SessionError::Store("branch transaction produced no activation receipt".to_string())
        })?;
        tx.commit().map_err(sql_err)?;

        let mut target = request.target.clone();
        target.message_count = i64::try_from(copied).map_err(|_| {
            SessionError::Store("branch message count exceeds i64 range".to_string())
        })?;
        Ok(SessionBranchResult {
            target,
            copied_message_count: copied,
            source_message_count: cutoff,
            activation,
        })
    }

    pub fn get_session_branch_activation(
        &self,
        operation_id: &str,
    ) -> Result<Option<SessionBranchActivation>> {
        let conn = self.conn()?;
        query_branch_activation(&conn, operation_id)
    }

    pub fn list_recoverable_session_branch_activations(
        &self,
        limit: usize,
    ) -> Result<Vec<SessionBranchActivation>> {
        let conn = self.conn()?;
        let mut statement = conn
            .prepare(
                r"SELECT operation_id, source_session_id, target_session_id,
                          source_message_count, phase, created_at_ms, updated_at_ms,
                          last_error, revision
                     FROM session_branch_activations
                    WHERE phase != 'activated'
                    ORDER BY updated_at_ms ASC, operation_id ASC
                    LIMIT ?1",
            )
            .map_err(sql_err)?;
        let rows = statement
            .query_map(params![limit as i64], row_to_branch_activation)
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    pub fn transition_session_branch_activation(
        &self,
        transition: &SessionBranchActivationTransition,
    ) -> Result<SessionBranchActivation> {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let current = query_branch_activation(&tx, &transition.operation_id)?.ok_or_else(|| {
            SessionError::Store(format!(
                "Session branch activation `{}` does not exist",
                transition.operation_id
            ))
        })?;
        transition.validate(&current)?;
        let changed = tx
            .execute(
                r"UPDATE session_branch_activations
                     SET phase=?1, updated_at_ms=?2, last_error=?3,
                         revision=revision+1
                   WHERE operation_id=?4 AND phase=?5 AND revision=?6",
                params![
                    transition.next_phase.as_str(),
                    transition.updated_at_ms as i64,
                    transition.error,
                    transition.operation_id,
                    transition.expected_phase.as_str(),
                    transition.expected_revision as i64,
                ],
            )
            .map_err(sql_err)?;
        if changed != 1 {
            return Err(SessionError::Store(format!(
                "Session branch activation `{}` changed during transition",
                transition.operation_id
            )));
        }
        let activation =
            query_branch_activation(&tx, &transition.operation_id)?.ok_or_else(|| {
                SessionError::Store(format!(
                    "Session branch activation `{}` disappeared after transition",
                    transition.operation_id
                ))
            })?;
        tx.commit().map_err(sql_err)?;
        Ok(activation)
    }
}
