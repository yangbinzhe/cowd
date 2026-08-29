//! Query, pagination, snapshot, and retention operations for the SqliteSessionStore adapter.

use super::*;

impl SqliteSessionStore {
    /// Retrieve messages for a session with pagination.
    pub fn get_messages(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"WITH page_start AS (
                       SELECT sequence
                         FROM messages
                        WHERE session_id = ?1
                        ORDER BY sequence ASC
                        LIMIT 1 OFFSET ?3
                   )
                   SELECT stable_message_id, session_id, sequence, role, content_json,
                          blocks_count, tool_use_id, tool_name,
                          token_usage_json, created_at_ms
                     FROM messages
                    WHERE session_id = ?1
                      AND sequence >= (SELECT sequence FROM page_start)
                    ORDER BY sequence ASC
                    LIMIT ?2",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, limit as i64, offset as i64],
                row_to_message,
            )
            .map_err(sql_err)?;
        let mut msgs = Vec::new();
        for r in rows {
            msgs.push(r.map_err(sql_err)?);
        }
        Ok(msgs)
    }

    /// Retrieve messages for a session starting at `from_sequence`.
    ///
    /// This keyset-style path is stable for deep history paging because it
    /// uses the `(session_id, sequence)` index instead of scanning through a
    /// large OFFSET window.
    pub fn get_messages_from_sequence(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT stable_message_id, session_id, sequence, role, content_json,
                          blocks_count, tool_use_id, tool_name,
                          token_usage_json, created_at_ms
                   FROM messages
                  WHERE session_id = ?1 AND sequence >= ?2
                  ORDER BY sequence ASC
                  LIMIT ?3",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, from_sequence as i64, limit as i64],
                row_to_message,
            )
            .map_err(sql_err)?;
        let mut msgs = Vec::new();
        for r in rows {
            msgs.push(r.map_err(sql_err)?);
        }
        Ok(msgs)
    }

    /// Retrieve several exact half-open transcript ranges with one checked-out
    /// connection. Runtime preselects these ranges from context cards.
    pub fn get_messages_in_ranges(
        &self,
        session_id: &str,
        ranges: &[(usize, usize)],
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let limit = bounded_limit(limit, 1, 2_048);
        let ranges = ranges
            .iter()
            .take(128)
            .filter(|(start, end)| start < end)
            .map(|(start, end)| {
                Ok((
                    i64::try_from(*start).map_err(|_| {
                        SessionError::InvalidArgument("message range start overflow".to_string())
                    })?,
                    i64::try_from(*end).map_err(|_| {
                        SessionError::InvalidArgument("message range end overflow".to_string())
                    })?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn()?;
        let predicates = (0..ranges.len())
            .map(|index| {
                let start = index.saturating_mul(2).saturating_add(2);
                let end = start.saturating_add(1);
                format!("(sequence >= ?{start} AND sequence < ?{end})")
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        let limit_parameter = ranges.len().saturating_mul(2).saturating_add(2);
        let sql = format!(
            "SELECT stable_message_id, session_id, sequence, role, content_json,
                    blocks_count, tool_use_id, tool_name,
                    token_usage_json, created_at_ms
               FROM messages
              WHERE session_id = ?1 AND ({predicates})
              ORDER BY sequence ASC
              LIMIT ?{limit_parameter}"
        );
        let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
        let mut values = Vec::with_capacity(ranges.len().saturating_mul(2).saturating_add(2));
        values.push(rusqlite::types::Value::Text(session_id.to_string()));
        for (start, end) in ranges {
            values.push(rusqlite::types::Value::Integer(start));
            values.push(rusqlite::types::Value::Integer(end));
        }
        values.push(rusqlite::types::Value::Integer(
            i64::try_from(limit).unwrap_or(i64::MAX),
        ));
        let messages = stmt
            .query_map(rusqlite::params_from_iter(values.iter()), row_to_message)
            .map_err(sql_err)?
            .map(|row| row.map_err(sql_err))
            .collect();
        messages
    }

    pub fn get_message_by_stable_id(
        &self,
        session_id: &str,
        stable_message_id: &str,
    ) -> Result<Option<SessionMessage>> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT stable_message_id, session_id, sequence, role, content_json,
                     blocks_count, tool_use_id, tool_name, token_usage_json,
                     created_at_ms
                FROM messages
               WHERE session_id=?1 AND stable_message_id=?2",
            params![session_id, stable_message_id],
            row_to_message,
        )
        .optional()
        .map_err(sql_err)
    }

    pub fn get_message_by_sequence(
        &self,
        session_id: &str,
        sequence: usize,
    ) -> Result<Option<SessionMessage>> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT stable_message_id, session_id, sequence, role, content_json,
                     blocks_count, tool_use_id, tool_name, token_usage_json,
                     created_at_ms
                FROM messages
               WHERE session_id=?1 AND sequence=?2",
            params![session_id, sequence as i64],
            row_to_message,
        )
        .optional()
        .map_err(sql_err)
    }

    pub fn get_message_metadata_page(
        &self,
        session_id: &str,
        from_sequence: usize,
        limit: usize,
    ) -> Result<Vec<SessionMessageMetadata>> {
        let conn = self.conn()?;
        let mut statement = conn
            .prepare(
                r"SELECT stable_message_id, session_id, sequence, role,
                         blocks_count, tool_use_id, tool_name, created_at_ms,
                         length(CAST(content_json AS BLOB))
                    FROM messages
                   WHERE session_id=?1 AND sequence>=?2
                   ORDER BY sequence ASC
                   LIMIT ?3",
            )
            .map_err(sql_err)?;
        let result = statement
            .query_map(
                params![
                    session_id,
                    from_sequence as i64,
                    bounded_limit(limit, 1, 2_048) as i64
                ],
                row_to_message_metadata,
            )
            .map_err(sql_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_err);
        result
    }

    pub fn get_context_index_cards(
        &self,
        session_id: &str,
        limit: usize,
    ) -> Result<Vec<ContextIndexCard>> {
        let conn = self.conn()?;
        let mut statement = conn
            .prepare(
                r"SELECT card_id, parent_card_id, session_id,
                         source_start_sequence, source_end_sequence,
                         source_message_count, source_digest, summary, scope,
                         authority, generation, created_at_ms, updated_at_ms
                    FROM session_context_index_cards
                   WHERE session_id=?1
                   ORDER BY
                       CASE WHEN parent_card_id IS NULL THEN 0 ELSE 1 END,
                       source_start_sequence DESC
                   LIMIT ?2",
            )
            .map_err(sql_err)?;
        let result = statement
            .query_map(
                params![session_id, bounded_limit(limit, 1, 2_048) as i64],
                row_to_context_index_card,
            )
            .map_err(sql_err)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_err);
        result
    }

    /// Atomically replace one Session's rebuildable navigation index.
    ///
    /// This is intentionally an explicit background operation. Message
    /// appends only enqueue outbox rows in their own transaction.
    pub fn reconcile_session_context_index(
        &self,
        session_id: &str,
        card_span: usize,
        parent_span: usize,
        now_ms: u64,
    ) -> Result<ContextIndexCoverage> {
        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let messages = {
            let mut statement = tx
                .prepare(
                    r"SELECT stable_message_id, session_id, sequence, role, content_json,
                             blocks_count, tool_use_id, tool_name, token_usage_json,
                             created_at_ms
                        FROM messages
                       WHERE session_id=?1
                       ORDER BY sequence ASC",
                )
                .map_err(sql_err)?;
            let result = statement
                .query_map(params![session_id], row_to_message)
                .map_err(sql_err)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(sql_err)?;
            result
        };
        let current_generation: u64 = tx
            .query_row(
                "SELECT index_generation FROM session_recovery_manifest WHERE session_id=?1",
                params![session_id],
                |row| Ok(row.get::<_, i64>(0)?.max(0) as u64),
            )
            .optional()
            .map_err(sql_err)?
            .ok_or_else(|| {
                SessionError::Store(format!(
                    "session activation manifest `{session_id}` does not exist"
                ))
            })?;
        let generation = current_generation.saturating_add(1);
        let cards = build_context_index_cards(
            session_id,
            &messages,
            card_span,
            parent_span,
            generation,
            now_ms,
        );
        tx.execute(
            "DELETE FROM session_context_index_cards WHERE session_id=?1",
            params![session_id],
        )
        .map_err(sql_err)?;
        for card in &cards {
            tx.execute(
                r"INSERT INTO session_context_index_cards(
                       card_id, parent_card_id, session_id,
                       source_start_sequence, source_end_sequence,
                       source_message_count, source_digest, summary, scope,
                       authority, generation, created_at_ms, updated_at_ms
                   ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
                params![
                    card.card_id,
                    card.parent_card_id,
                    card.session_id,
                    card.source_start_sequence as i64,
                    card.source_end_sequence as i64,
                    card.source_message_count as i64,
                    card.source_digest,
                    card.summary,
                    card.scope,
                    card.authority,
                    card.generation as i64,
                    card.created_at_ms as i64,
                    card.updated_at_ms as i64,
                ],
            )
            .map_err(sql_err)?;
        }
        let indexed_through_sequence = messages.last().map(|message| message.sequence);
        tx.execute(
            r"UPDATE session_recovery_manifest
                  SET index_generation=?2,
                      indexed_through_sequence=?3,
                      index_card_count=?4,
                      index_pending=0,
                      manifest_revision=manifest_revision + 1
                WHERE session_id=?1",
            params![
                session_id,
                generation as i64,
                indexed_through_sequence.map(|value| value as i64),
                cards.len() as i64,
            ],
        )
        .map_err(sql_err)?;
        tx.execute(
            r"UPDATE session_context_index_outbox
                  SET status='completed', attempts=attempts + 1,
                      updated_at_ms=?2
                WHERE session_id=?1 AND status!='completed'",
            params![session_id, now_ms as i64],
        )
        .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        let leaf_cards = cards
            .iter()
            .filter(|card| card.parent_card_id.is_some() || cards.len() == 1)
            .cloned()
            .collect::<Vec<_>>();
        let covered_messages = leaf_cards
            .iter()
            .map(|card| card.source_message_count)
            .sum();
        Ok(ContextIndexCoverage {
            session_id: session_id.to_string(),
            source_messages: messages.len(),
            covered_messages,
            card_count: cards.len(),
            indexed_through_sequence,
            generation,
            complete: covered_messages == messages.len(),
            source_digest: context_index_source_digest(&messages),
            card_digest: context_index_card_digest(&cards),
        })
    }

    /// Retrieve ALL messages for a session (unbounded, no pagination).
    pub fn get_all_messages(&self, session_id: &str) -> Result<Vec<SessionMessage>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT stable_message_id, session_id, sequence, role, content_json, blocks_count,
                        tool_use_id, tool_name, token_usage_json, created_at_ms
                 FROM messages WHERE session_id = ?1 ORDER BY sequence ASC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![session_id], row_to_message)
            .map_err(sql_err)?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row.map_err(sql_err)?);
        }
        if messages.len() > 1000 {
            tracing::warn!(
                session_id,
                count = messages.len(),
                "get_all_messages: large session, consider pagination"
            );
        }
        Ok(messages)
    }

    /// Count the number of messages in a session.
    pub fn get_message_count(&self, session_id: &str) -> Result<usize> {
        let conn = self.conn()?;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        Ok(count as usize)
    }

    /// Delete all messages in a session starting from `from_sequence` (inclusive).
    ///
    /// Returns the number of rows deleted.
    pub fn delete_messages_from(&self, session_id: &str, from_sequence: usize) -> Result<usize> {
        let conn = self.conn()?;
        let removed = conn
            .execute(
                "DELETE FROM messages WHERE session_id = ?1 AND sequence >= ?2",
                params![session_id, from_sequence as i64],
            )
            .map_err(sql_err)?;
        Ok(removed)
    }

    /// Search messages using FTS5 full-text search.
    ///
    /// Optionally filter by `session_id`. Searches across role and
    /// extracted text content from `content_json`.
    pub fn search_messages(
        &self,
        query: &str,
        session_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        let conn = self.conn()?;
        if let Some(sid) = session_id {
            let mut stmt = conn
                .prepare(
                    r"SELECT m.stable_message_id, m.session_id, m.sequence, m.role, m.content_json,
                              m.blocks_count, m.tool_use_id, m.tool_name,
                              m.token_usage_json, m.created_at_ms
                       FROM messages m
                       JOIN messages_fts fts ON m.id = fts.rowid
                      WHERE messages_fts MATCH ?1 AND m.session_id = ?2
                      ORDER BY rank
                      LIMIT ?3",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![query, sid, limit as i64], row_to_message)
                .map_err(sql_err)?;
            let mut msgs = Vec::new();
            for r in rows {
                msgs.push(r.map_err(sql_err)?);
            }
            Ok(msgs)
        } else {
            let mut stmt = conn
                .prepare(
                    r"SELECT m.stable_message_id, m.session_id, m.sequence, m.role, m.content_json,
                              m.blocks_count, m.tool_use_id, m.tool_name,
                              m.token_usage_json, m.created_at_ms
                       FROM messages m
                       JOIN messages_fts fts ON m.id = fts.rowid
                      WHERE messages_fts MATCH ?1
                      ORDER BY rank
                      LIMIT ?2",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(params![query, limit as i64], row_to_message)
                .map_err(sql_err)?;
            let mut msgs = Vec::new();
            for r in rows {
                msgs.push(r.map_err(sql_err)?);
            }
            Ok(msgs)
        }
    }

    /// Search only the supplied session authority set.  Gateway resolves that
    /// set before issuing the query so an unauthorised high-ranked FTS row can
    /// neither displace an authorised result nor be exposed to the caller.
    pub fn search_messages_in_sessions(
        &self,
        query: &str,
        session_ids: &[String],
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        if session_ids.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let conn = self.conn()?;
        let scope_json = serde_json::to_string(session_ids).map_err(|error| {
            SessionError::Store(format!("encode search session scope: {error}"))
        })?;
        let mut stmt = conn
            .prepare(
                r"SELECT m.stable_message_id, m.session_id, m.sequence, m.role, m.content_json,
                          m.blocks_count, m.tool_use_id, m.tool_name,
                          m.token_usage_json, m.created_at_ms
                     FROM messages m
                     JOIN messages_fts fts ON m.id = fts.rowid
                    WHERE messages_fts MATCH ?1
                      AND m.session_id IN (SELECT value FROM json_each(?2))
                    ORDER BY rank
                    LIMIT ?3",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![query, scope_json, limit as i64], row_to_message)
            .map_err(sql_err)?;
        let mut messages = Vec::new();
        for row in rows {
            messages.push(row.map_err(sql_err)?);
        }
        Ok(messages)
    }

    pub fn search_messages_visible(
        &self,
        query: &str,
        owner_principal_id: Option<&str>,
        visible_session_ids: &[String],
        unrestricted: bool,
        limit: usize,
    ) -> Result<Vec<SessionMessage>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let conn = self.conn()?;
        let visible_json = serde_json::to_string(visible_session_ids).map_err(|error| {
            SessionError::Store(format!("encode visible Session scope: {error}"))
        })?;
        let mut stmt = conn
            .prepare(
                r"SELECT message.stable_message_id, message.session_id, message.sequence,
                          message.role, message.content_json, message.blocks_count,
                          message.tool_use_id, message.tool_name,
                          message.token_usage_json, message.created_at_ms
                     FROM messages AS message
                     JOIN messages_fts AS fts ON message.id=fts.rowid
                     JOIN sessions AS session ON session.session_id=message.session_id
                    WHERE messages_fts MATCH ?1
                      AND session.status NOT IN ('deleted','deleting')
                      AND (
                          ?4
                          OR json_extract(session.metadata_json, '$.owner_principal_id')=?2
                          OR session.session_id IN (
                              SELECT value FROM json_each(?3)
                          )
                      )
                    ORDER BY rank
                    LIMIT ?5",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![
                    query,
                    owner_principal_id.unwrap_or_default(),
                    visible_json,
                    unrestricted,
                    bounded_limit(limit, 1, 500) as i64
                ],
                row_to_message,
            )
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    // -----------------------------------------------------------------------
    // Event log
    // -----------------------------------------------------------------------

    /// Append a mutation event to the session's event log.
    pub fn append_event(&self, event: &SessionEvent) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"INSERT INTO session_events
               (session_id, event_type, event_json, sequence, created_at_ms)
              VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                event.session_id,
                event.event_type,
                event.event_json,
                event.sequence as i64,
                event.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Allocate the next session-local sequence and append one event in the
    /// same SQLite transaction. The input sequence is treated as a placeholder.
    pub fn append_event_allocating_sequence(&self, event: &SessionEvent) -> Result<SessionEvent> {
        let mut appended = self.append_events_allocating_sequence(std::slice::from_ref(event))?;
        appended
            .pop()
            .ok_or_else(|| SessionError::Store("event allocation returned no row".to_string()))
    }

    pub fn append_session_domain_event_if_absent_allocating_sequence(
        &self,
        event: &SessionEvent,
        event_id: &str,
    ) -> Result<(SessionEvent, bool)> {
        if event.event_type != SESSION_DOMAIN_EVENT_TYPE || event_id.trim().is_empty() {
            return Err(SessionError::Store(
                "idempotent domain append requires SessionDomainEvent and a non-empty event_id"
                    .to_string(),
            ));
        }
        let encoded_event_id = serde_json::from_str::<serde_json::Value>(&event.event_json)
            .ok()
            .and_then(|value| {
                value
                    .get("event_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .ok_or_else(|| {
                SessionError::Store(
                    "idempotent domain append requires event_json.event_id".to_string(),
                )
            })?;
        if encoded_event_id != event_id {
            return Err(SessionError::Store(
                "idempotent domain append event_id does not match event_json".to_string(),
            ));
        }

        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let existing = tx
            .query_row(
                r"SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                    FROM session_events
                   WHERE session_id = ?1
                     AND event_type = ?2
                     AND json_extract(event_json, '$.event_id') = ?3
                   LIMIT 1",
                params![event.session_id, SESSION_DOMAIN_EVENT_TYPE, event_id],
                row_to_event,
            )
            .optional()
            .map_err(sql_err)?;
        if let Some(existing) = existing {
            if !SessionDomainEvent::semantically_equivalent(&existing, event).map_err(|error| {
                SessionError::Store(format!(
                    "failed to compare idempotent session-domain event content: {error}"
                ))
            })? {
                return Err(SessionError::IdempotencyConflict {
                    namespace: "session_domain_event",
                    key: event_id.to_string(),
                });
            }
            tx.commit().map_err(sql_err)?;
            return Ok((existing, true));
        }

        let sequence: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                params![event.session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        let stored_sequence = usize::try_from(sequence).map_err(|_| {
            SessionError::Store(
                "allocated session event sequence is negative or too large".to_string(),
            )
        })?;
        let event_json = event_json_with_allocated_sequence(event, stored_sequence)?;
        let created_at_ms = i64::try_from(event.created_at_ms).map_err(|_| {
            SessionError::Store("session event timestamp exceeds SQLite i64 range".to_string())
        })?;
        tx.execute(
            r"INSERT INTO session_events
               (session_id, event_type, event_json, sequence, created_at_ms)
              VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                event.session_id,
                event.event_type,
                event_json,
                sequence,
                created_at_ms,
            ],
        )
        .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        let mut stored = event.clone();
        stored.sequence = stored_sequence;
        stored.event_json = event_json;
        Ok((stored, false))
    }

    pub fn get_session_domain_event_by_id(
        &self,
        session_id: &str,
        event_id: &str,
    ) -> Result<Option<SessionEvent>> {
        if event_id.trim().is_empty() {
            return Ok(None);
        }
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                FROM session_events
               WHERE session_id = ?1
                 AND event_type = ?2
                 AND json_extract(event_json, '$.event_id') = ?3
               LIMIT 1",
            params![session_id, SESSION_DOMAIN_EVENT_TYPE, event_id],
            row_to_event,
        )
        .optional()
        .map_err(sql_err)
    }

    /// Allocate contiguous sequences and append a same-session event batch in
    /// one `BEGIN IMMEDIATE` transaction.
    pub fn append_events_allocating_sequence(
        &self,
        events: &[SessionEvent],
    ) -> Result<Vec<SessionEvent>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let session_id = events[0].session_id.as_str();
        if session_id.trim().is_empty() || events.iter().any(|event| event.session_id != session_id)
        {
            return Err(SessionError::Store(
                "atomic session event batch must contain one non-empty session_id".to_string(),
            ));
        }

        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let first_sequence: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;

        let mut appended = Vec::with_capacity(events.len());
        for (offset, event) in events.iter().enumerate() {
            let offset = i64::try_from(offset).map_err(|_| {
                SessionError::Store("session event batch offset exceeds i64 range".to_string())
            })?;
            let sequence = first_sequence.checked_add(offset).ok_or_else(|| {
                SessionError::Store("session event sequence overflow".to_string())
            })?;
            let stored_sequence = usize::try_from(sequence).map_err(|_| {
                SessionError::Store(
                    "allocated session event sequence is negative or too large".to_string(),
                )
            })?;
            let event_json = event_json_with_allocated_sequence(event, stored_sequence)?;
            let created_at_ms = i64::try_from(event.created_at_ms).map_err(|_| {
                SessionError::Store("session event timestamp exceeds SQLite i64 range".to_string())
            })?;
            tx.execute(
                r"INSERT INTO session_events
                   (session_id, event_type, event_json, sequence, created_at_ms)
                  VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    event.session_id,
                    event.event_type,
                    event_json,
                    sequence,
                    created_at_ms,
                ],
            )
            .map_err(sql_err)?;
            let mut stored = event.clone();
            stored.sequence = stored_sequence;
            stored.event_json = event_json;
            appended.push(stored);
        }
        tx.commit().map_err(sql_err)?;
        Ok(appended)
    }

    /// Atomically append a compaction event bundle unless its semantic
    /// checkpoint has already committed for this session. `None` means a
    /// previous attempt committed the exact checkpoint and the caller must
    /// reuse it instead of emitting duplicate facts/events.
    pub fn append_events_allocating_sequence_if_checkpoint_absent(
        &self,
        events: &[SessionEvent],
        checkpoint_id: &str,
    ) -> Result<Option<Vec<SessionEvent>>> {
        if events.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let session_id = events[0].session_id.as_str();
        if session_id.trim().is_empty() || events.iter().any(|event| event.session_id != session_id)
        {
            return Err(SessionError::Store(
                "atomic session event batch must contain one non-empty session_id".to_string(),
            ));
        }
        if checkpoint_id.trim().is_empty() {
            return Err(SessionError::Store(
                "checkpoint-aware event batch requires a non-empty checkpoint_id".to_string(),
            ));
        }

        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let exists: i64 = tx
            .query_row(
                r"SELECT COUNT(*) FROM session_events
                    WHERE session_id = ?1
                      AND event_type = ?2
                      AND json_extract(event_json, '$.kind') = 'memory.semantic_checkpoint.created'
                      AND json_extract(event_json, '$.payload.checkpoint.checkpoint_id') = ?3",
                params![session_id, SESSION_DOMAIN_EVENT_TYPE, checkpoint_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        if exists > 0 {
            tx.commit().map_err(sql_err)?;
            return Ok(None);
        }

        let first_sequence: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        let mut appended = Vec::with_capacity(events.len());
        for (offset, event) in events.iter().enumerate() {
            let offset = i64::try_from(offset).map_err(|_| {
                SessionError::Store("session event batch offset exceeds i64 range".to_string())
            })?;
            let sequence = first_sequence.checked_add(offset).ok_or_else(|| {
                SessionError::Store("session event sequence overflow".to_string())
            })?;
            let stored_sequence = usize::try_from(sequence).map_err(|_| {
                SessionError::Store(
                    "allocated session event sequence is negative or too large".to_string(),
                )
            })?;
            let event_json = event_json_with_allocated_sequence(event, stored_sequence)?;
            let created_at_ms = i64::try_from(event.created_at_ms).map_err(|_| {
                SessionError::Store("session event timestamp exceeds SQLite i64 range".to_string())
            })?;
            tx.execute(
                r"INSERT INTO session_events
                   (session_id, event_type, event_json, sequence, created_at_ms)
                  VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    event.session_id,
                    event.event_type,
                    event_json,
                    sequence,
                    created_at_ms,
                ],
            )
            .map_err(sql_err)?;
            let mut stored = event.clone();
            stored.sequence = stored_sequence;
            stored.event_json = event_json;
            appended.push(stored);
        }
        tx.commit().map_err(sql_err)?;
        Ok(Some(appended))
    }

    /// Atomically de-duplicate a context envelope and allocate its sequence.
    pub fn append_context_envelope_event_if_absent_allocating_sequence(
        &self,
        event: &SessionEvent,
    ) -> Result<Option<SessionEvent>> {
        if event.event_type != "ContextEnvelope" {
            return self.append_event_allocating_sequence(event).map(Some);
        }
        let envelope_id = serde_json::from_str::<serde_json::Value>(&event.event_json)
            .ok()
            .and_then(|payload| {
                payload
                    .pointer("/envelope/id")
                    .or_else(|| payload.get("envelope_id"))
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            })
            .ok_or_else(|| {
                SessionError::Store(
                    "ContextEnvelope append requires envelope.id or envelope_id".to_string(),
                )
            })?;

        let mut conn = self.conn()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_err)?;
        let exists: i64 = tx
            .query_row(
                r"SELECT COUNT(*) FROM session_events
                  WHERE event_type = 'ContextEnvelope'
                    AND COALESCE(
                        json_extract(event_json, '$.envelope.id'),
                        json_extract(event_json, '$.envelope_id')
                    ) = ?1",
                params![envelope_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        if exists > 0 {
            tx.commit().map_err(sql_err)?;
            return Ok(None);
        }
        let sequence: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                params![event.session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        tx.execute(
            r"INSERT INTO session_events
               (session_id, event_type, event_json, sequence, created_at_ms)
              VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                event.session_id,
                event.event_type,
                event.event_json,
                sequence,
                event.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        let mut stored = event.clone();
        stored.sequence = sequence as usize;
        Ok(Some(stored))
    }

    /// Append a context envelope event only if this envelope id is not already present.
    ///
    /// Returns `true` when a row was inserted and `false` when an existing
    /// `ContextEnvelope` row with the same `envelope.id` already exists.
    pub fn append_context_envelope_event_if_absent(&self, event: &SessionEvent) -> Result<bool> {
        self.append_context_envelope_event_if_absent_allocating_sequence(event)
            .map(|stored| stored.is_some())
    }

    /// Retrieve events for a session starting from `from_seq` (inclusive).
    /// Ordered by sequence ascending.
    pub fn get_events(&self, session_id: &str, from_seq: usize) -> Result<Vec<SessionEvent>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                 FROM session_events
                 WHERE session_id = ?1 AND sequence >= ?2
                 ORDER BY sequence ASC",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(params![session_id, from_seq as i64], row_to_event)
            .map_err(sql_err)?;
        let mut events = Vec::new();
        for r in rows {
            events.push(r.map_err(sql_err)?);
        }
        Ok(events)
    }

    /// Retrieve at most `limit` events for a session starting from `from_seq`.
    /// Ordered by sequence ascending.
    pub fn get_events_limited(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                 FROM session_events
                 WHERE session_id = ?1 AND sequence >= ?2
                 ORDER BY sequence ASC
                 LIMIT ?3",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, from_seq as i64, limit as i64],
                row_to_event,
            )
            .map_err(sql_err)?;
        let mut events = Vec::new();
        for r in rows {
            events.push(r.map_err(sql_err)?);
        }
        Ok(events)
    }

    /// Retrieve canonical Session-domain events only.
    pub fn get_session_domain_timeline_limited(
        &self,
        session_id: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        self.get_events_by_type_limited(session_id, SESSION_DOMAIN_EVENT_TYPE, from_seq, limit)
    }

    pub fn count_session_domain_timeline_from(
        &self,
        session_id: &str,
        from_seq: usize,
    ) -> Result<usize> {
        self.count_events_by_type_from(session_id, SESSION_DOMAIN_EVENT_TYPE, from_seq)
    }

    pub fn get_session_domain_events_by_kind_limited(
        &self,
        session_id: &str,
        kind: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                    FROM session_events
                   WHERE session_id = ?1
                     AND event_type = ?2
                     AND json_extract(event_json, '$.kind') = ?3
                     AND sequence >= ?4
                   ORDER BY sequence ASC
                   LIMIT ?5",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![
                    session_id,
                    SESSION_DOMAIN_EVENT_TYPE,
                    kind,
                    from_seq as i64,
                    limit as i64,
                ],
                row_to_event,
            )
            .map_err(sql_err)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql_err)
    }

    /// Resolve the newest event of one kind through the covering expression
    /// index. This is O(log n) and never scans an arbitrary prefix.
    pub fn get_latest_session_domain_event_by_kind(
        &self,
        session_id: &str,
        kind: &str,
    ) -> Result<Option<SessionEvent>> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                FROM session_events
               WHERE session_id=?1
                 AND event_type=?2
                 AND json_extract(event_json, '$.kind')=?3
               ORDER BY sequence DESC
               LIMIT 1",
            params![session_id, SESSION_DOMAIN_EVENT_TYPE, kind],
            row_to_event,
        )
        .optional()
        .map_err(sql_err)
    }

    pub fn count_session_domain_events_by_kind_from(
        &self,
        session_id: &str,
        kind: &str,
        from_seq: usize,
    ) -> Result<usize> {
        let conn = self.conn()?;
        let count: i64 = conn
            .query_row(
                r"SELECT COUNT(*) FROM session_events
                   WHERE session_id = ?1
                     AND event_type = ?2
                     AND json_extract(event_json, '$.kind') = ?3
                     AND sequence >= ?4",
                params![session_id, SESSION_DOMAIN_EVENT_TYPE, kind, from_seq as i64,],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        usize::try_from(count)
            .map_err(|_| SessionError::Store("domain event count exceeds usize".to_string()))
    }

    pub fn has_session_domain_event_kind(&self, kind: &str) -> Result<bool> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT EXISTS(
                SELECT 1 FROM session_events
                 WHERE event_type=?1
                   AND json_extract(event_json, '$.kind')=?2
                 LIMIT 1
            )",
            params![SESSION_DOMAIN_EVENT_TYPE, kind],
            |row| row.get(0),
        )
        .map_err(sql_err)
    }

    pub fn has_session_with_domain_event_kinds(&self, kinds: &[String]) -> Result<bool> {
        if kinds.is_empty() {
            return Ok(false);
        }
        let conn = self.conn()?;
        let kinds_json = serde_json::to_string(kinds)
            .map_err(|error| SessionError::Store(format!("encode event kinds: {error}")))?;
        conn.query_row(
            r"SELECT EXISTS(
                SELECT session_id
                  FROM session_events
                 WHERE event_type=?1
                   AND json_extract(event_json, '$.kind') IN (
                       SELECT value FROM json_each(?2)
                   )
                 GROUP BY session_id
                HAVING COUNT(DISTINCT json_extract(event_json, '$.kind')) >= ?3
                 LIMIT 1
            )",
            params![SESSION_DOMAIN_EVENT_TYPE, kinds_json, kinds.len() as i64],
            |row| row.get(0),
        )
        .map_err(sql_err)
    }

    /// Retrieve at most `limit` events of one type for a session.
    /// Ordered by sequence ascending.
    pub fn get_events_by_type_limited(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
        limit: usize,
    ) -> Result<Vec<SessionEvent>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                "SELECT id, session_id, event_type, event_json, sequence, created_at_ms
                 FROM session_events
                 WHERE session_id = ?1 AND event_type = ?2 AND sequence >= ?3
                 ORDER BY sequence ASC
                 LIMIT ?4",
            )
            .map_err(sql_err)?;
        let rows = stmt
            .query_map(
                params![session_id, event_type, from_seq as i64, limit as i64],
                row_to_event,
            )
            .map_err(sql_err)?;
        let mut events = Vec::new();
        for r in rows {
            events.push(r.map_err(sql_err)?);
        }
        Ok(events)
    }

    /// Count events for a session starting from `from_seq`.
    pub fn count_events_from(&self, session_id: &str, from_seq: usize) -> Result<usize> {
        let conn = self.conn()?;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id = ?1 AND sequence >= ?2",
                params![session_id, from_seq as i64],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        Ok(count as usize)
    }

    /// Count events of one type for a session starting from `from_seq`.
    pub fn count_events_by_type_from(
        &self,
        session_id: &str,
        event_type: &str,
        from_seq: usize,
    ) -> Result<usize> {
        let conn = self.conn()?;
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id = ?1 AND event_type = ?2 AND sequence >= ?3",
                params![session_id, event_type, from_seq as i64],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        Ok(count as usize)
    }

    /// Retrieve a context envelope event by its envelope id.
    pub fn get_context_event_by_envelope_id(
        &self,
        envelope_id: &str,
    ) -> Result<Option<SessionEvent>> {
        let conn = self.conn()?;
        conn.query_row(
            r"SELECT id, session_id, event_type, event_json, sequence, created_at_ms
              FROM session_events
              WHERE event_type = 'ContextEnvelope'
                AND json_extract(event_json, '$.envelope.id') = ?1
              ORDER BY created_at_ms DESC
              LIMIT 1",
            params![envelope_id],
            row_to_event,
        )
        .optional()
        .map_err(sql_err)
    }

    /// Return the next append sequence for a session event.
    pub fn next_event_sequence(&self, session_id: &str) -> Result<usize> {
        let conn = self.conn()?;
        let next: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 0) FROM session_events WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .map_err(sql_err)?;
        Ok(next.max(0) as usize)
    }

    /// Delete all events from `from_sequence` onward in a session.
    pub fn delete_events_from(&self, session_id: &str, from_sequence: usize) -> Result<usize> {
        let conn = self.conn()?;
        let deleted = conn
            .execute(
                "DELETE FROM session_events WHERE session_id = ?1 AND sequence >= ?2",
                params![session_id, from_sequence as i64],
            )
            .map_err(sql_err)?;
        Ok(deleted)
    }

    /// Delete events of one type from `from_sequence` onward in a session.
    pub fn delete_events_by_type_from(
        &self,
        session_id: &str,
        event_type: &str,
        from_sequence: usize,
    ) -> Result<usize> {
        let conn = self.conn()?;
        let deleted = conn
            .execute(
                "DELETE FROM session_events WHERE session_id = ?1 AND event_type = ?2 AND sequence >= ?3",
                params![session_id, event_type, from_sequence as i64],
            )
            .map_err(sql_err)?;
        Ok(deleted)
    }

    /// Save a full-message-list snapshot at a given event index.
    pub fn save_snapshot(&self, snapshot: &SessionSnapshot) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            r"INSERT INTO session_snapshots
               (session_id, event_idx, messages_json, created_at_ms)
              VALUES (?1, ?2, ?3, ?4)",
            params![
                snapshot.session_id,
                snapshot.event_idx as i64,
                snapshot.messages_json,
                snapshot.created_at_ms as i64,
            ],
        )
        .map_err(sql_err)?;
        Ok(())
    }

    /// Return the most recent snapshot for a session, or `None`.
    pub fn get_latest_snapshot(&self, session_id: &str) -> Result<Option<SessionSnapshot>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT id, session_id, event_idx, messages_json, created_at_ms
             FROM session_snapshots
             WHERE session_id = ?1
             ORDER BY event_idx DESC
             LIMIT 1",
            params![session_id],
            row_to_snapshot,
        )
        .optional()
        .map_err(sql_err)
    }

    // -----------------------------------------------------------------------
    // Maintenance
    // -----------------------------------------------------------------------

    /// Delete sessions whose `last_activity` is older than `cutoff_iso8601`.
    ///
    /// Returns the number of sessions that were removed.
    pub fn prune_before(&self, cutoff_iso8601: &str) -> Result<usize> {
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(sql_err)?;
        // Remove associated memories first.
        tx.execute(
            r"DELETE FROM session_memories WHERE session_id IN (
                SELECT session_id FROM sessions WHERE last_activity < ?1
              )",
            params![cutoff_iso8601],
        )
        .map_err(sql_err)?;
        let removed = tx
            .execute(
                "DELETE FROM sessions WHERE last_activity < ?1",
                params![cutoff_iso8601],
            )
            .map_err(sql_err)?;
        tx.commit().map_err(sql_err)?;
        Ok(removed)
    }

    /// Delete sessions whose `last_activity` is older than `cutoff_iso8601`,
    /// cleaning up both the SQLite records and any corresponding JSONL/JSON
    /// files on disk under `sessions_dir`.
    ///
    /// Returns the number of sessions that were removed.
    pub fn prune_with_files(&self, cutoff_iso8601: &str, sessions_dir: &Path) -> Result<usize> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare("SELECT session_id FROM sessions WHERE last_activity < ?1")
            .map_err(sql_err)?;
        let ids: Vec<String> = stmt
            .query_map(params![cutoff_iso8601], |row| row.get::<_, String>(0))
            .map_err(sql_err)?
            .filter_map(|r| r.ok())
            .collect();
        let count = ids.len();
        for id in &ids {
            self.delete_session(id)?;
            for ext in &["jsonl", "json"] {
                let path = sessions_dir.join(format!("{id}.{ext}"));
                let _ = std::fs::remove_file(&path);
                if *ext == "jsonl" {
                    if let Ok(entries) = std::fs::read_dir(sessions_dir) {
                        for entry in entries.flatten() {
                            let name = entry.file_name();
                            let name_str = name.to_string_lossy();
                            if name_str.starts_with(&format!("{id}.rot-"))
                                && name_str.ends_with(".jsonl")
                            {
                                let _ = std::fs::remove_file(entry.path());
                            }
                        }
                    }
                }
            }
        }
        Ok(count)
    }

    /// Mark a session as closed.
    ///
    /// Updates the session's status to `'closed'` and refreshes
    /// `last_activity`.  Messages are preserved for auditing.
    pub fn mark_session_closed(&self, session_id: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE sessions SET status = 'closed', last_activity = ?1 WHERE session_id = ?2",
            params![chrono::Utc::now().to_rfc3339(), session_id],
        )
        .map_err(sql_err)?;
        Ok(())
    }
}
