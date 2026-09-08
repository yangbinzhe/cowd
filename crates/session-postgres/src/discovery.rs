//! Active context discovery owns no state beyond the canonical Session tables.
use super::*;
use session::{
    SessionDiscoveryKind, SessionDiscoveryPage, SessionDiscoveryRequest, SessionDiscoveryScope,
    SessionDiscoverySnapshot,
};
use std::collections::{BTreeMap, BTreeSet};

impl PostgresSessionStore {
    pub fn discover_context_page(
        &self,
        request: &SessionDiscoveryRequest,
    ) -> session::SessionResult<SessionDiscoveryPage> {
        let filter = &request.filter;
        let workspace = filter.scope == SessionDiscoveryScope::Workspace;
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .map_err(postgres_error)?;
        let current=transaction.query_opt("SELECT session_discovery_actor_key(platform,user_id,metadata_json) FROM session_records WHERE session_id=$1 AND status NOT IN ('deleted','deleting')",&[&filter.current_session_id]).map_err(postgres_error)?
            .ok_or_else(||session::SessionError::Store("current Session is unavailable for context discovery".into()))?;
        let mut keys = filter
            .authorized_session_ids
            .iter()
            .map(|id| format!("session:{id}"))
            .collect::<BTreeSet<_>>();
        keys.insert(format!("session:{}", filter.current_session_id));
        if workspace {
            if let Some(actor) = current.get::<_, Option<String>>(0) {
                keys.insert(actor);
            }
        }
        let keys = keys.into_iter().collect::<Vec<_>>();
        let revisions=transaction.query("SELECT s.key,coalesce(r.revision,0)::BIGINT FROM unnest($1::TEXT[]) s(key) LEFT JOIN session_discovery_invalidations r USING(key) ORDER BY s.key",&[&keys]).map_err(postgres_error)?.into_iter().map(|row|(row.get::<_,String>(0),row.get::<_,i64>(1))).collect::<BTreeMap<_,_>>();
        let snapshot = match &request.snapshot {
            Some(snapshot) if snapshot.revisions == revisions => snapshot.clone(),
            Some(_) => {
                return Err(session::SessionError::Store(
                    "Session discovery source or authority changed; restart discovery".into(),
                ))
            }
            None => SessionDiscoverySnapshot {
                fence: transaction
                    .query_one("SELECT pg_current_snapshot()::TEXT", &[])
                    .map_err(postgres_error)?
                    .get(0),
                revisions,
            },
        };
        let authorized=format!("s.status NOT IN ('deleted','deleting') AND pg_visible_in_snapshot(s.discovery_creation_xid,$4::TEXT::pg_snapshot) AND (s.session_id=ANY($2) OR ($3 AND {}))",lifecycle::BROWSABLE_SESSION_AUTHORITY);
        let limit = request.limit.clamp(1, 100);
        let sql_limit = (limit + 1) as i64;
        let mut result = SessionDiscoveryPage {
            sessions: Vec::new(),
            messages: Vec::new(),
            snapshot: snapshot.clone(),
            next_session_id: None,
            next_sequence: None,
        };
        match filter.kind {
            SessionDiscoveryKind::Sessions => {
                let sql=format!("SELECT s.session_id,s.platform,s.chat_id,s.user_id,s.model,s.created_at,s.last_activity,s.message_count,s.reset_policy,s.metadata_json,s.input_tokens,s.output_tokens,s.status
                    FROM session_records s JOIN session_records current ON current.session_id=$1
                    WHERE {authorized} AND ($5::TEXT IS NULL OR s.session_id>$5)
                    AND ($6::TEXT IS NULL OR
                        to_tsvector('simple',coalesce(s.session_id,'')||' '||coalesce(s.platform,'')||' '||coalesce(s.chat_id,'')||' '||coalesce(s.metadata_json,'')) @@ websearch_to_tsquery('simple',$6)
                        OR strpos(lower(s.session_id||' '||s.platform||' '||s.chat_id||' '||coalesce(s.metadata_json,'')),lower($6))>0
                        OR EXISTS (SELECT 1 FROM session_messages m WHERE m.session_id=s.session_id
                            AND pg_visible_in_snapshot(m.discovery_creation_xid,$4::TEXT::pg_snapshot)
                            AND (to_tsvector('simple',m.role||' '||m.content_json||' '||coalesce(m.tool_name,'')) @@ websearch_to_tsquery('simple',$6)
                                OR strpos(lower(m.content_json),lower($6))>0)))
                    ORDER BY s.session_id LIMIT $7");
                let mut records = transaction
                    .query(
                        &sql,
                        &[
                            &filter.current_session_id,
                            &filter.authorized_session_ids,
                            &workspace,
                            &snapshot.fence,
                            &request.after_session_id,
                            &filter.query,
                            &sql_limit,
                        ],
                    )
                    .map_err(postgres_error)?
                    .iter()
                    .map(row_to_session)
                    .collect::<session::SessionResult<Vec<_>>>()?;
                let more = records.len() > limit;
                records.truncate(limit);
                result.next_session_id = more.then(|| records.last().unwrap().session_id.clone());
                result.sessions = records;
            }
            SessionDiscoveryKind::Messages => {
                let after_sequence = request
                    .after_sequence
                    .map(|n| to_i64(n, "message cursor sequence"))
                    .transpose()?;
                let before = filter
                    .before_sequence
                    .map(|n| to_i64(n, "message before sequence"))
                    .transpose()?;
                let sql=format!("SELECT m.stable_message_id,m.session_id,m.sequence,m.role,m.content_json,m.blocks_count,m.tool_use_id,m.tool_name,m.token_usage_json,m.created_at_ms
                    FROM session_messages m JOIN session_records s ON s.session_id=m.session_id JOIN session_records current ON current.session_id=$1
                    WHERE {authorized} AND pg_visible_in_snapshot(m.discovery_creation_xid,$4::TEXT::pg_snapshot)
                      AND ($5::TEXT IS NULL OR s.session_id>$5 OR (s.session_id=$5 AND m.sequence<$6))
                      AND ($7::TEXT IS NULL OR to_tsvector('simple',m.role||' '||m.content_json||' '||coalesce(m.tool_name,'')) @@ websearch_to_tsquery('simple',$7)
                          OR strpos(lower(m.content_json),lower($7))>0)
                      AND ($8::BIGINT IS NULL OR m.sequence<$8)
                    ORDER BY s.session_id,m.sequence DESC LIMIT $9");
                let mut messages = transaction
                    .query(
                        &sql,
                        &[
                            &filter.current_session_id,
                            &filter.authorized_session_ids,
                            &workspace,
                            &snapshot.fence,
                            &request.after_session_id,
                            &after_sequence,
                            &filter.query,
                            &before,
                            &sql_limit,
                        ],
                    )
                    .map_err(postgres_error)?
                    .iter()
                    .map(row_to_message)
                    .collect::<session::SessionResult<Vec<_>>>()?;
                let more = messages.len() > limit;
                messages.truncate(limit);
                if more {
                    let last = messages.last().unwrap();
                    result.next_session_id = Some(last.session_id.clone());
                    result.next_sequence = Some(last.sequence);
                }
                result.messages = messages;
            }
        }
        transaction.commit().map_err(postgres_error)?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[ignore = "requires isolated COWD_TEST_POSTGRES_URL"]
    fn session_discovery_reaches_all_scopes_and_snapshot_messages_without_offset_drift() {
        let resolver = storage::StaticSecretRefResolver::new([(
            "session-discovery".into(),
            std::env::var("COWD_TEST_POSTGRES_URL").unwrap(),
        )]);
        let database = PostgresExecutor::connect(
            PostgresConnectionConfig::new("test", "session-discovery", "session-discovery-test"),
            &resolver,
        )
        .unwrap();
        let schema = format!(
            "session_discovery_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        database
            .checkout_critical()
            .unwrap()
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .unwrap();
        let store = PostgresSessionStore::new(database.scoped_namespace(&schema).unwrap()).unwrap();
        let record = |id: &str, owner: &str| {
            SessionRecord {session_id:id.into(),platform:"test".into(),chat_id:id.into(),user_id:None,model:None,created_at:"2026-09-08T00:00:00Z".into(),last_activity:"2026-09-08T00:00:00Z".into(),message_count:0,reset_policy:"manual".into(),metadata_json:Some(serde_json::json!({"workspace_root":"/test","owner_principal_id":owner,"title":"needle"}).to_string()),input_tokens:0,output_tokens:0,status:"active".into()}
        };
        let message = |id: &str, sequence: usize| SessionMessage {
            stable_message_id: format!("{id}-m{sequence}"),
            session_id: id.into(),
            sequence,
            role: "user".into(),
            content_json: serde_json::json!([{"type":"text","text":"needle transcript"}])
                .to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 1,
        };
        for i in 0..600 {
            let id = format!("session-{i:03}");
            store.create_session(&record(&id, "owner")).unwrap();
            store.insert_message(&message(&id, 0)).unwrap();
        }
        store.create_session(&record("private", "other")).unwrap();
        store.insert_message(&message("private", 0)).unwrap();
        for sequence in 1..140 {
            store
                .insert_message(&message("session-000", sequence))
                .unwrap();
        }
        let mut request = SessionDiscoveryRequest {
            filter: session::SessionDiscoveryFilter {
                kind: SessionDiscoveryKind::Sessions,
                scope: SessionDiscoveryScope::Workspace,
                current_session_id: "session-000".into(),
                authorized_session_ids: vec!["session-000".into()],
                query: Some("needle".into()),
                before_sequence: None,
            },
            limit: 37,
            after_session_id: None,
            after_sequence: None,
            snapshot: None,
        };
        let mut page = store.discover_context_page(&request).unwrap();
        let mut sessions = BTreeSet::new();
        store.create_session(&record("future", "owner")).unwrap();
        let mut active = record("session-599", "owner");
        active.last_activity = "2026-09-09T00:00:00Z".into();
        store.update_session(&active).unwrap();
        loop {
            for record in page.sessions {
                assert!(sessions.insert(record.session_id));
            }
            let Some(next) = page.next_session_id else {
                break;
            };
            request.after_session_id = Some(next);
            request.snapshot = Some(page.snapshot);
            page = store.discover_context_page(&request).unwrap();
        }
        assert_eq!(sessions.len(), 600);
        assert!(sessions.contains("session-599"));
        assert!(!sessions.contains("private"));
        assert!(!sessions.contains("future"));
        request.filter.kind = SessionDiscoveryKind::Messages;
        request.after_session_id = None;
        request.snapshot = None;
        let mut connection = store.executor.checkout_critical().unwrap();
        let mut pending = connection.transaction().unwrap();
        let inflight = message("session-000", 140);
        pending.execute("INSERT INTO session_messages(stable_message_id,session_id,sequence,role,content_json,blocks_count,created_at_ms) VALUES($1,$2,140,'user',$3,1,1)",&[&inflight.stable_message_id,&inflight.session_id,&inflight.content_json]).unwrap();
        let mut page = store.discover_context_page(&request).unwrap();
        let original = page.snapshot.clone();
        pending.commit().unwrap();
        let mut messages = BTreeSet::new();
        loop {
            for message in page.messages {
                assert!(messages.insert(message.stable_message_id));
            }
            let Some(next) = page.next_session_id else {
                break;
            };
            request.after_session_id = Some(next);
            request.after_sequence = page.next_sequence;
            request.snapshot = Some(page.snapshot);
            store
                .insert_message(&message("session-000", 1000 + messages.len()))
                .unwrap();
            page = store.discover_context_page(&request).unwrap();
        }
        assert_eq!(messages.len(), 739);
        assert!(messages.contains("session-599-m0"));
        assert!(!messages.contains("session-000-m140"));
        request.after_session_id = None;
        request.after_sequence = None;
        request.snapshot = Some(original);
        let mut private = message("private", 0);
        private.content_json = "[]".into();
        private.blocks_count = 0;
        store.insert_message(&private).unwrap();
        store.discover_context_page(&request).unwrap();
        let mut changed = message("session-000", 0);
        changed.content_json = serde_json::json!([{"type":"text","text":"changed"}]).to_string();
        store.insert_message(&changed).unwrap();
        assert!(store
            .discover_context_page(&request)
            .unwrap_err()
            .to_string()
            .contains("source or authority changed"));
        request.snapshot = None;
        request.snapshot = Some(store.discover_context_page(&request).unwrap().snapshot);
        store
            .executor
            .checkout_critical()
            .unwrap()
            .execute(
                "DELETE FROM session_messages WHERE session_id='session-001' AND sequence=0",
                &[],
            )
            .unwrap();
        assert!(store.discover_context_page(&request).is_err());
        request.snapshot = None;
        request.snapshot = Some(store.discover_context_page(&request).unwrap().snapshot);
        store
            .update_session(&record("session-000", "different"))
            .unwrap();
        assert!(store.discover_context_page(&request).is_err());
        request.snapshot = None;
        let narrowed = store.discover_context_page(&request).unwrap();
        assert!(narrowed
            .messages
            .iter()
            .all(|m| m.session_id == "session-000"));
        drop(connection);
        drop(store);
        database
            .checkout_critical()
            .unwrap()
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .unwrap();
    }
}
