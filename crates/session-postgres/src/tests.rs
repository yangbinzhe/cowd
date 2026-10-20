use std::sync::{Arc, Barrier};
#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

use super::*;

#[path = "../test-support/terminal_receipt_scenario.rs"]
mod terminal_receipt_scenario;

#[test]
#[ignore = "requires a parent-owned isolated PostgreSQL producer allocation"]
fn r1_real_client_receipt_scenario_step() {
    terminal_receipt_scenario::run().unwrap();
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL for the original private fixture"]
fn r1_message_ack_real_client_scenario_rejects_foreign_or_replayed_steps() {
    terminal_receipt_scenario::safety_selftest().unwrap();
}

#[test]
fn r1_receipt_source_app_generation_is_content_addressed_not_decimal() {
    let authority = session::SessionReceiptAuthority {
        session_id: "session".into(),
        actor_id: "actor".into(),
        principal_id: "principal".into(),
        client_id: "client".into(),
        credential_expires_at_ms: None,
        receipt_source: None,
    };
    let source = session::SessionReceiptSource {
        principal_id: "principal".into(),
        tenant_id: "tenant".into(),
        workspace_id: "workspace".into(),
        app_id: "app".into(),
        app_generation: format!("sha256:{}", "ab".repeat(32)),
        profile_revision: "1".into(),
        credential_epoch: "2".into(),
        frame_nonce: "nonce".into(),
        transport_id: "port".into(),
        attachment_revision: "3".into(),
        expires_at_ms: 1000,
        session_id: "session".into(),
        turn_id: "turn".into(),
        execution_id: "execution".into(),
        root_task_id: "root".into(),
        lineage_generation: "4".into(),
        purpose: "terminal_receipt".into(),
    };
    assert!(lifecycle::validate_receipt_source(&source, &authority, 999).is_ok());
    let mut uppercase = source.clone();
    uppercase.app_generation = format!("sha256:{}", "AB".repeat(32));
    assert!(lifecycle::validate_receipt_source(&uppercase, &authority, 999).is_ok());
    for generation in [
        "1".to_string(),
        "arbitrary".into(),
        "sha256:".into(),
        "ab".repeat(32),
        format!("sha256:{}", "gg".repeat(32)),
        format!("sha256:{} ", "ab".repeat(32)),
        format!("sha256:{}", "a".repeat(63)),
    ] {
        let mut invalid = source.clone();
        invalid.app_generation = generation;
        assert!(lifecycle::validate_receipt_source(&invalid, &authority, 999).is_err());
    }
    for index in 0..4 {
        for invalid_revision in ["0", "01", "-1", "9223372036854775808", "sha256:abcd", " 1"] {
            let mut invalid = source.clone();
            let field = match index {
                0 => &mut invalid.profile_revision,
                1 => &mut invalid.credential_epoch,
                2 => &mut invalid.attachment_revision,
                _ => &mut invalid.lineage_generation,
            };
            *field = invalid_revision.into();
            assert!(lifecycle::validate_receipt_source(&invalid, &authority, 999).is_err());
        }
    }
    assert!(lifecycle::validate_receipt_source(&source, &authority, 1000).is_err());
    let mut foreign = authority.clone();
    foreign.session_id = "foreign".into();
    assert!(lifecycle::validate_receipt_source(&source, &foreign, 999).is_err());
    foreign = authority.clone();
    foreign.principal_id = "foreign".into();
    assert!(lifecycle::validate_receipt_source(&source, &foreign, 999).is_err());
    let mut wrong_purpose = source;
    wrong_purpose.purpose = "other".into();
    assert!(lifecycle::validate_receipt_source(&wrong_purpose, &authority, 999).is_err());
}

struct TerminalReceiptFixture {
    scope: PostgresTestScope,
    store: PostgresSessionStore,
    commit: SessionTerminalTranscriptCommit,
    authority: session::SessionReceiptAuthority,
}

impl TerminalReceiptFixture {
    fn new() -> Self {
        Self::create(true)
    }

    fn pending() -> Self {
        Self::create(false)
    }

    fn create(commit_terminal: bool) -> Self {
        let scope = PostgresTestScope::new();
        let store = real_store(&scope);
        let session_id = unique_id("r1-receipt");
        store.create_session(&session(&session_id)).unwrap();
        let request = runtime_request(
            &unique_id("receipt-input"),
            1,
            InputRoutingDecision::StartNewTurn,
            None,
            100,
        );
        append_runtime_input(&store, &session_id, &request);
        let claimed = store
            .claim_session_runtime_outbox("receipt-worker", 101, 10000, 1, &[])
            .unwrap()
            .remove(0);
        let token = claimed.claim_token.clone().unwrap();
        let running = store
            .mark_session_runtime_outbox_running(
                &request.request_id,
                "receipt-worker",
                1,
                &token,
                claimed.revision,
                102,
            )
            .unwrap();
        let message = SessionMessage {
            stable_message_id: format!("assistant:{}",request.message_id),session_id:session_id.clone(),sequence:0,
            role:"assistant".into(),content_json:r#"[{"type":"thinking","thinking":"sealed-private","signature":"sealed-signature"},{"type":"text","text":"public answer"}]"#.into(),
            blocks_count:2,tool_use_id:None,tool_name:None,token_usage_json:None,created_at_ms:0,
        };
        let commit = SessionTerminalTranscriptCommit {
            terminal_message_id: message.stable_message_id.clone(),
            ingress_message_id: request.message_id,
            session_id: session_id.clone(),
            turn_id: request.turn_id,
            messages: vec![message],
            runtime_commit_cursor: 9007199254740993,
            consumed_input_sequence: running.sequence,
            created_at_ms: 103,
            fence: session::SessionTerminalExecutionFence {
                request_id: request.request_id,
                input_sequence: running.sequence,
                session_generation: 1,
                claim_owner: "receipt-worker".into(),
                claim_token: token,
                claim_fence_epoch: running.claim_fence_epoch.unwrap(),
            },
        };
        if commit_terminal {
            store.commit_terminal_transcript_if_fenced(&commit).unwrap();
        }
        let mut authority = session::SessionReceiptAuthority {
            session_id,
            actor_id: "principal:receipt-user:surface:receipt-tab".into(),
            principal_id: "receipt-user".into(),
            client_id: String::new(),
            credential_expires_at_ms: None,
            receipt_source: None,
        };
        let grant = store
            .mutate_session_presence_if_revision(&session::SessionPresenceMutation {
                authority: authority.clone(),
                expected_revision: None,
                kind: session::SessionPresenceMutationKind::AttachOrReaffirm {
                    actor: session::SessionActor {
                        id: authority.actor_id.clone(),
                        surface: "test".into(),
                        role: Some("reader".into()),
                    },
                    candidate_client_id: "00000000-0000-4000-8000-000000000001".into(),
                    ttl_ms: 3600000,
                },
            })
            .unwrap()
            .unwrap();
        authority.client_id = grant.attachment.unwrap().client_id.unwrap();
        Self {
            scope,
            store,
            commit,
            authority,
        }
    }

    fn query(&self) -> session::SessionTerminalReceiptQuery {
        session::SessionTerminalReceiptQuery {
            authority: self.authority.clone(),
            message_id: self.commit.terminal_message_id.clone(),
        }
    }

    fn command(&self) -> session::SessionTerminalAckCommand {
        session::SessionTerminalAckCommand {
            query: self.query(),
            binding: session::SessionTerminalMessageBinding {
                runtime_commit_cursor: self.commit.runtime_commit_cursor,
                content_digest: session::public_terminal_payload_v1(
                    &self.commit.messages[0].content_json,
                )
                .unwrap()
                .content_digest,
            },
        }
    }

    fn history(&self) -> Vec<harness_contract::live::SessionMessageAck> {
        self.store
            .list_terminal_message_acks(&session::SessionTerminalAckListQuery {
                authority: self.authority.clone(),
                after_cursor: 0,
                limit: 100,
            })
            .unwrap()
            .into_iter()
            .map(|entry| match entry {
                harness_contract::live::SessionMessageAckHistory::Historical { receipt } => receipt,
            })
            .collect()
    }

    fn rewrite(&self, raw: &str) {
        let mut connection = self.store.executor.checkout_critical().unwrap();
        connection
            .execute(
                "UPDATE session_messages SET content_json=$2 WHERE stable_message_id=$1",
                &[&self.commit.terminal_message_id, &raw],
            )
            .unwrap();
    }

    fn assert_unavailable(&self) {
        assert!(matches!(
            self.store
                .get_terminal_message_receipt_context(&self.query())
                .unwrap()
                .context,
            harness_contract::live::SessionMessageReceiptContext::Unavailable { .. }
        ));
        assert!(self
            .store
            .record_terminal_message_ack(&self.command())
            .is_err());
    }
}

#[test]
fn r1_message_ack_clean_baseline_has_only_current_schema() {
    assert_eq!(SESSION_MIGRATIONS.len(), 1);
    assert_eq!(SESSION_MIGRATIONS[0].id, "session.current-baseline");
    let ddl = SESSION_MIGRATIONS[0].statements.join("\n");
    for required in [
        "session_terminal_binding_pair",
        "session_terminal_message_identity",
        "session_terminal_message_acks",
        "REFERENCES session_messages(session_id, stable_message_id)",
        "existing session message cannot acquire terminal binding",
        "claim_fence_epoch",
        "session_runtime_claim_fence_epoch_positive",
        "application_receipt_json",
        "input_id TEXT NOT NULL",
        "session_presence_projection",
        "idx_session_records_casefold_status_model",
        "idx_session_runtime_outbox_target_turn",
    ] {
        assert!(
            ddl.contains(required),
            "missing current constraint: {required}"
        );
    }
    for forbidden in [
        "ALTER TABLE",
        "DELETE FROM session_events",
        "session_mission_outbox",
        "cowd_session_usage_json_for_legacy_cast",
    ] {
        assert!(
            !ddl.contains(forbidden),
            "obsolete baseline path: {forbidden}"
        );
    }
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_exact_idempotent_and_restart_durable() {
    let fixture = TerminalReceiptFixture::new();
    let receipt = fixture
        .store
        .record_terminal_message_ack(&fixture.command())
        .unwrap();
    assert_eq!(
        receipt.runtime_commit_cursor.value().unwrap(),
        9007199254740993
    );
    let reopened = real_store(&fixture.scope);
    assert_eq!(
        reopened
            .record_terminal_message_ack(&fixture.command())
            .unwrap(),
        receipt
    );
    assert_eq!(fixture.history(), vec![receipt]);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_wrong_tuple_never_writes_or_overwrites() {
    let fixture = TerminalReceiptFixture::new();
    let mut command = fixture.command();
    command.binding.runtime_commit_cursor += 1;
    assert!(fixture.store.record_terminal_message_ack(&command).is_err());
    command = fixture.command();
    command.binding.content_digest = format!("session-public-v1:sha256:{}", "0".repeat(64));
    assert!(fixture.store.record_terminal_message_ack(&command).is_err());
    assert!(fixture.history().is_empty());
    let received = fixture
        .store
        .record_terminal_message_ack(&fixture.command())
        .unwrap();
    assert!(fixture.store.record_terminal_message_ack(&command).is_err());
    assert_eq!(fixture.history(), vec![received]);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_raw_rewrite_invalidates_even_when_p1_is_equal() {
    let fixture = TerminalReceiptFixture::new();
    let original =
        session::public_terminal_payload_v1(&fixture.commit.messages[0].content_json).unwrap();
    fixture.rewrite(&format!(" {} ", fixture.commit.messages[0].content_json));
    assert_eq!(
        session::public_terminal_payload_v1(&format!(
            " {} ",
            fixture.commit.messages[0].content_json
        ))
        .unwrap(),
        original
    );
    fixture.assert_unavailable();
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_historical_fact_survives_binding_invalidation() {
    let fixture = TerminalReceiptFixture::new();
    let received = fixture
        .store
        .record_terminal_message_ack(&fixture.command())
        .unwrap();
    fixture.rewrite(r#"[{"type":"text","text":"edited"}]"#);
    fixture.assert_unavailable();
    assert_eq!(fixture.history(), vec![received]);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_a_b_a_and_completed_replay_never_resurrect() {
    let fixture = TerminalReceiptFixture::new();
    let received = fixture
        .store
        .record_terminal_message_ack(&fixture.command())
        .unwrap();
    fixture.rewrite(r#"[{"type":"text","text":"B"}]"#);
    assert!(fixture
        .store
        .commit_terminal_transcript_if_fenced(&fixture.commit)
        .is_err());
    fixture.rewrite(&fixture.commit.messages[0].content_json);
    assert!(
        !fixture
            .store
            .commit_terminal_transcript_if_fenced(&fixture.commit)
            .unwrap()
            .inserted
    );
    fixture.assert_unavailable();
    assert_eq!(fixture.history(), vec![received]);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_existing_rows_cannot_acquire_or_replace_binding() {
    let fixture = TerminalReceiptFixture::new();
    let mut connection = fixture.store.executor.checkout_critical().unwrap();
    assert!(connection.execute("UPDATE session_messages SET terminal_runtime_commit_cursor=terminal_runtime_commit_cursor+1 WHERE stable_message_id=$1",&[&fixture.commit.terminal_message_id]).is_err());
    connection.execute("UPDATE session_messages SET terminal_runtime_commit_cursor=NULL,terminal_content_digest=NULL WHERE stable_message_id=$1",&[&fixture.commit.terminal_message_id]).unwrap();
    assert!(connection.execute("UPDATE session_messages SET terminal_runtime_commit_cursor=$2,terminal_content_digest=$3 WHERE stable_message_id=$1",
        &[&fixture.commit.terminal_message_id,&to_u64_i64(fixture.commit.runtime_commit_cursor,"cursor").unwrap(),&fixture.command().binding.content_digest]).is_err());
    drop(connection);
    fixture.assert_unavailable();
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_usage_only_preserves_current_binding_and_history() {
    let fixture = TerminalReceiptFixture::new();
    let received = fixture
        .store
        .record_terminal_message_ack(&fixture.command())
        .unwrap();
    let mut connection = fixture.store.executor.checkout_critical().unwrap();
    connection.execute("UPDATE session_messages SET token_usage_json='{\"output_tokens\":2}',blocks_count=99,created_at_ms=created_at_ms+1 WHERE stable_message_id=$1",&[&fixture.commit.terminal_message_id]).unwrap();
    drop(connection);
    assert_eq!(
        fixture
            .store
            .record_terminal_message_ack(&fixture.command())
            .unwrap(),
        received
    );
    assert!(fixture
        .store
        .commit_terminal_transcript_if_fenced(&fixture.commit)
        .is_err());
    assert_eq!(fixture.history(), vec![received]);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_replay_checks_raw_and_public_binding_without_backfill() {
    let fixture = TerminalReceiptFixture::new();
    let replay = fixture
        .store
        .commit_terminal_transcript_if_fenced(&fixture.commit)
        .unwrap();
    assert!(!replay.inserted);
    fixture.rewrite(&format!(" {}", fixture.commit.messages[0].content_json));
    assert!(fixture
        .store
        .commit_terminal_transcript_if_fenced(&fixture.commit)
        .is_err());
    fixture.rewrite(&fixture.commit.messages[0].content_json);
    fixture
        .store
        .commit_terminal_transcript_if_fenced(&fixture.commit)
        .unwrap();
    fixture.assert_unavailable();
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_attachment_mutations_cannot_bypass_tagged_cas() {
    let fixture = TerminalReceiptFixture::new();
    let current = fixture
        .store
        .get_session_presence_projection(&fixture.authority.session_id)
        .unwrap()
        .unwrap();
    let mut forged = current.clone();
    forged.attachments_json = "[]".into();
    assert!(fixture
        .store
        .upsert_session_presence_projection(&forged)
        .is_err());
    assert!(fixture
        .store
        .compare_and_upsert_session_presence_projection(&forged, Some(current.revision))
        .is_err());
    assert!(fixture
        .store
        .delete_session_presence_projection(&fixture.authority.session_id)
        .is_err());
    assert_eq!(
        fixture
            .store
            .get_session_presence_projection(&fixture.authority.session_id)
            .unwrap()
            .unwrap()
            .attachments_json,
        current.attachments_json
    );
    fixture
        .store
        .record_terminal_message_ack(&fixture.command())
        .unwrap();
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_attachment_pg_cas_and_ttl_are_linearized() {
    let fixture = TerminalReceiptFixture::new();
    let current = fixture
        .store
        .get_session_presence_projection(&fixture.authority.session_id)
        .unwrap()
        .unwrap();
    let command = session::SessionPresenceMutation {
        authority: fixture.authority.clone(),
        expected_revision: Some(current.revision),
        kind: session::SessionPresenceMutationKind::Detach,
    };
    let left = real_store(&fixture.scope);
    let right = real_store(&fixture.scope);
    let barrier = Arc::new(Barrier::new(2));
    let gate = barrier.clone();
    let other = command.clone();
    let worker = std::thread::spawn(move || {
        gate.wait();
        left.mutate_session_presence_if_revision(&other)
            .unwrap()
            .is_some()
    });
    barrier.wait();
    let won = right
        .mutate_session_presence_if_revision(&command)
        .unwrap()
        .is_some();
    assert_ne!(won, worker.join().unwrap());
    assert!(fixture
        .store
        .record_terminal_message_ack(&fixture.command())
        .is_err());
    let expired = TerminalReceiptFixture::new();
    let mut connection = expired.store.executor.checkout_critical().unwrap();
    connection.execute("UPDATE session_presence_projection SET attachments_json=jsonb_set(attachments_json,'{0,client_expires_at_ms}','0') WHERE session_id=$1",&[&expired.authority.session_id]).unwrap();
    drop(connection);
    assert!(expired
        .store
        .record_terminal_message_ack(&expired.command())
        .is_err());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_terminal_binding_and_input_commit_roll_back_together() {
    let fixture = TerminalReceiptFixture::pending();
    let mut connection = fixture.store.executor.checkout_critical().unwrap();
    let events_before: i64 = connection
        .query_one(
            "SELECT COUNT(*) FROM session_events WHERE session_id=$1",
            &[&fixture.authority.session_id],
        )
        .unwrap()
        .get(0);
    connection.batch_execute("CREATE FUNCTION r1_reject_receipt_final() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'r1-final-fault-hit'; END $$; CREATE TRIGGER r1_receipt_final_fault BEFORE INSERT ON session_messages FOR EACH ROW WHEN (NEW.role='assistant') EXECUTE FUNCTION r1_reject_receipt_final()").unwrap();
    let error = fixture
        .store
        .commit_terminal_transcript_if_fenced(&fixture.commit)
        .unwrap_err();
    assert!(error.to_string().contains("r1-final-fault-hit"));
    assert!(fixture
        .store
        .get_message_by_stable_id(
            &fixture.authority.session_id,
            &fixture.commit.terminal_message_id
        )
        .unwrap()
        .is_none());
    assert_eq!(
        fixture
            .store
            .get_session_runtime_outbox(&fixture.commit.fence.request_id)
            .unwrap()
            .unwrap()
            .status,
        SessionRuntimeInputStatus::Running
    );
    let events_after_failure: i64 = connection
        .query_one(
            "SELECT COUNT(*) FROM session_events WHERE session_id=$1",
            &[&fixture.authority.session_id],
        )
        .unwrap()
        .get(0);
    assert_eq!(events_after_failure, events_before);
    connection.batch_execute("DROP TRIGGER r1_receipt_final_fault ON session_messages; DROP FUNCTION r1_reject_receipt_final()").unwrap();
    let mut oversized = fixture.commit.clone();
    oversized.messages[0].content_json = serde_json::json!([{"type":"text","text":"x".repeat(session::SESSION_TERMINAL_PUBLIC_PAYLOAD_MAX_BYTES)}]).to_string();
    assert!(fixture
        .store
        .commit_terminal_transcript_if_fenced(&oversized)
        .is_err());
    assert!(fixture
        .store
        .get_message_by_stable_id(
            &fixture.authority.session_id,
            &fixture.commit.terminal_message_id
        )
        .unwrap()
        .is_none());
    let events_after_oversized: i64 = connection
        .query_one(
            "SELECT COUNT(*) FROM session_events WHERE session_id=$1",
            &[&fixture.authority.session_id],
        )
        .unwrap()
        .get(0);
    assert_eq!(events_after_oversized, events_before);
    assert_eq!(
        fixture
            .store
            .get_session_runtime_outbox(&fixture.commit.fence.request_id)
            .unwrap()
            .unwrap()
            .status,
        SessionRuntimeInputStatus::Running
    );
    fixture
        .store
        .commit_terminal_transcript_if_fenced(&fixture.commit)
        .unwrap();
    let events_after_commit: i64 = connection
        .query_one(
            "SELECT COUNT(*) FROM session_events WHERE session_id=$1",
            &[&fixture.authority.session_id],
        )
        .unwrap()
        .get(0);
    assert!(events_after_commit > events_before);
    fixture
        .store
        .record_terminal_message_ack(&fixture.command())
        .unwrap();
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_old_catalog_is_rejected_without_data_changes() {
    let scope = PostgresTestScope::new();
    let executor = scope.reconnect();
    let mut connection = executor.checkout_critical().unwrap();
    connection.batch_execute("CREATE TABLE session_records(session_id TEXT PRIMARY KEY, payload TEXT NOT NULL); INSERT INTO session_records VALUES('old','immutable old bytes'); CREATE TABLE cowd_schema_migrations(id TEXT PRIMARY KEY,domain TEXT NOT NULL,checksum TEXT NOT NULL); INSERT INTO cowd_schema_migrations VALUES('session.0023.casefolded-list-filter','session','original-checksum')").unwrap();
    let before: String = connection
        .query_one(
            "SELECT row_to_json(record)::text FROM session_records record",
            &[],
        )
        .unwrap()
        .get(0);
    let catalog_before: String = connection
        .query_one(
            "SELECT row_to_json(record)::text FROM cowd_schema_migrations record",
            &[],
        )
        .unwrap()
        .get(0);
    drop(connection);
    assert!(PostgresSessionStore::new(executor.clone()).is_err());
    let mut connection = executor.checkout_online_read().unwrap();
    let current: String = connection
        .query_one(
            "SELECT row_to_json(record)::text FROM session_records record",
            &[],
        )
        .unwrap()
        .get(0);
    let catalog: String = connection
        .query_one(
            "SELECT row_to_json(record)::text FROM cowd_schema_migrations record",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(current, before);
    assert_eq!(catalog, catalog_before);
    assert!(!connection
        .query_one(
            "SELECT to_regclass('session_terminal_message_acks') IS NOT NULL",
            &[]
        )
        .unwrap()
        .get::<_, bool>(0));
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn r1_message_ack_authorized_delete_and_archive_do_not_resurrect_receipts() {
    let fixture = TerminalReceiptFixture::new();
    fixture
        .store
        .record_terminal_message_ack(&fixture.command())
        .unwrap();
    let root = std::path::PathBuf::from("/media/yi/Datas/workspace/plan/2026-10/1016-cowd-future-kernel/evidence/execution/kernel-terminal-20261016/R1-04/fixtures").join(unique_id("archive"));
    let unified = session::UnifiedSessionStore::from_backend(Arc::new(real_store(&fixture.scope)));
    let tiered = session::TieredSessionStore::new(
        unified,
        session::TieredSessionStoreConfig {
            archive_path: root.clone(),
            ..Default::default()
        },
    );
    tiered
        .archive_session(&fixture.authority.session_id)
        .await
        .unwrap();
    assert!(fixture.history().is_empty());
    tiered
        .restore_session(&fixture.authority.session_id)
        .await
        .unwrap();
    fixture.assert_unavailable();
    let messages = fixture
        .store
        .get_all_messages(&fixture.authority.session_id)
        .unwrap();
    fixture
        .store
        .delete_messages_from(&fixture.authority.session_id, 0)
        .unwrap();
    let encoded = serde_json::to_vec(&messages).unwrap();
    let mut legacy = u32::try_from(encoded.len()).unwrap().to_le_bytes().to_vec();
    legacy.push((encoded.len().min(15) as u8) << 4);
    if encoded.len() >= 15 {
        let mut extra = encoded.len() - 15;
        while extra >= 255 {
            legacy.push(255);
            extra -= 255;
        }
        legacy.push(extra as u8);
    }
    legacy.extend_from_slice(&encoded);
    std::fs::write(
        root.join(format!("{}.lz4", fixture.authority.session_id)),
        legacy,
    )
    .unwrap();
    tiered
        .restore_session(&fixture.authority.session_id)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .store
            .get_all_messages(&fixture.authority.session_id)
            .unwrap(),
        messages
    );
    fixture.assert_unavailable();
    assert!(fixture.history().is_empty());
    fixture
        .store
        .delete_session(&fixture.authority.session_id)
        .unwrap();
    let mut connection = fixture.store.executor.checkout_online_read().unwrap();
    assert_eq!(
        connection
            .query_one(
                "SELECT count(*) FROM session_terminal_message_acks WHERE session_id=$1",
                &[&fixture.authority.session_id]
            )
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_both_branch_copy_paths_drop_bindings() {
    let fixture = TerminalReceiptFixture::new();
    let received = fixture
        .store
        .record_terminal_message_ack(&fixture.command())
        .unwrap();
    let target = unique_id("copy-target");
    fixture.store.create_session(&session(&target)).unwrap();
    fixture
        .store
        .copy_session_messages_at_cutoff(&fixture.authority.session_id, &target, 2)
        .unwrap();
    let other = unique_id("branch-target");
    fixture
        .store
        .branch_session_at_cutoff(&SessionBranchRequest {
            operation_id: unique_id("receipt-branch"),
            source_session_id: fixture.authority.session_id.clone(),
            source_message_count: 2,
            target: session(&other),
            source_event_json: "{}".into(),
            target_event_json: "{}".into(),
            created_at_ms: 200,
        })
        .unwrap();
    let mut connection = fixture.store.executor.checkout_online_read().unwrap();
    for target in [target, other] {
        let row = connection.query_one("SELECT count(*),count(terminal_runtime_commit_cursor),count(terminal_content_digest) FROM session_messages WHERE session_id=$1",&[&target]).unwrap();
        assert_eq!(row.get::<_, i64>(0), 2);
        assert_eq!(row.get::<_, i64>(1), 0);
        assert_eq!(row.get::<_, i64>(2), 0);
        assert_eq!(
            connection
                .query_one(
                    "SELECT count(*) FROM session_terminal_message_acks WHERE session_id=$1",
                    &[&target]
                )
                .unwrap()
                .get::<_, i64>(0),
            0
        );
    }
    drop(connection);
    assert_eq!(fixture.history(), vec![received]);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_detach_rewrite_delete_races_have_one_order() {
    for mutation in ["rewrite", "detach", "expiry", "delete"] {
        let fixture = TerminalReceiptFixture::new();
        let mut blocker = fixture.store.executor.checkout_critical().unwrap();
        let mut transaction = blocker.transaction().unwrap();
        transaction
            .query_one(
                "SELECT session_id FROM session_records WHERE session_id=$1 FOR UPDATE",
                &[&fixture.authority.session_id],
            )
            .unwrap();
        let gate = Arc::new(Barrier::new(2));
        let other_gate = gate.clone();
        let store = real_store(&fixture.scope);
        let command = fixture.command();
        let worker = std::thread::spawn(move || {
            other_gate.wait();
            store.record_terminal_message_ack(&command)
        });
        gate.wait();
        match mutation {
            "rewrite" => {
                transaction
                    .execute(
                        "UPDATE session_messages SET content_json='[]' WHERE stable_message_id=$1",
                        &[&fixture.commit.terminal_message_id],
                    )
                    .unwrap();
            }
            "detach" => {
                transaction.execute("UPDATE session_presence_projection SET attachments_json='[]'::jsonb,revision=revision+1 WHERE session_id=$1",&[&fixture.authority.session_id]).unwrap();
            }
            "expiry" => {
                transaction.execute("UPDATE session_presence_projection SET attachments_json=jsonb_set(attachments_json,'{0,client_expires_at_ms}','0') WHERE session_id=$1",&[&fixture.authority.session_id]).unwrap();
            }
            _ => {
                transaction
                    .execute(
                        "DELETE FROM session_records WHERE session_id=$1",
                        &[&fixture.authority.session_id],
                    )
                    .unwrap();
            }
        }
        transaction.commit().unwrap();
        assert!(worker.join().unwrap().is_err());
        let row = blocker
            .query_one(
                "SELECT count(*) FROM session_terminal_message_acks WHERE session_id=$1",
                &[&fixture.authority.session_id],
            )
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 0);
    }
    let fixture = TerminalReceiptFixture::new();
    let received = fixture
        .store
        .record_terminal_message_ack(&fixture.command())
        .unwrap();
    fixture.rewrite("[]");
    assert_eq!(fixture.history(), vec![received]);
    fixture.assert_unavailable();
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn r1_message_ack_frame_source_fields_survive_receipt_presence_cas() {
    let fixture = TerminalReceiptFixture::new();
    let current = fixture
        .store
        .get_session_presence_projection(&fixture.authority.session_id)
        .unwrap()
        .unwrap();
    fixture
        .store
        .mutate_session_presence_if_revision(&session::SessionPresenceMutation {
            authority: fixture.authority.clone(),
            expected_revision: Some(current.revision),
            kind: session::SessionPresenceMutationKind::Detach,
        })
        .unwrap()
        .unwrap();
    let mut authority = fixture.authority.clone();
    authority.client_id.clear();
    authority.receipt_source = Some(session::SessionReceiptSource {
        principal_id: authority.principal_id.clone(),
        tenant_id: "tenant".into(),
        workspace_id: "workspace".into(),
        app_id: "app".into(),
        app_generation: format!("sha256:{}", "ab".repeat(32)),
        profile_revision: "1".into(),
        credential_epoch: "1".into(),
        frame_nonce: "frame".into(),
        transport_id: "port".into(),
        attachment_revision: "1".into(),
        expires_at_ms: i64::MAX as u64,
        session_id: authority.session_id.clone(),
        turn_id: fixture.commit.turn_id.clone(),
        execution_id: "root-execution".into(),
        root_task_id: "root-task".into(),
        lineage_generation: "1".into(),
        purpose: "terminal_receipt".into(),
    });
    let grant = fixture
        .store
        .mutate_session_presence_if_revision(&session::SessionPresenceMutation {
            authority: authority.clone(),
            expected_revision: Some(current.revision + 1),
            kind: session::SessionPresenceMutationKind::AttachOrReaffirm {
                actor: session::SessionActor {
                    id: authority.actor_id.clone(),
                    surface: "frame".into(),
                    role: Some("reader".into()),
                },
                candidate_client_id: "00000000-0000-4000-8000-000000000002".into(),
                ttl_ms: 3600000,
            },
        })
        .unwrap()
        .unwrap();
    authority.client_id = grant.attachment.unwrap().client_id.unwrap();
    let reopened = real_store(&fixture.scope);
    let mut query = fixture.query();
    query.authority = authority.clone();
    assert!(matches!(
        reopened
            .get_terminal_message_receipt_context(&query)
            .unwrap()
            .context,
        harness_contract::live::SessionMessageReceiptContext::Available { .. }
    ));
    query.authority.receipt_source.as_mut().unwrap().frame_nonce = "foreign".into();
    assert!(reopened
        .get_terminal_message_receipt_context(&query)
        .is_err());
    let mut legacy = grant.projection;
    legacy.revision += 1;
    reopened
        .upsert_session_presence_projection(&legacy)
        .unwrap();
    assert!(
        serde_json::from_str::<Vec<session::SessionAttachment>>(&legacy.attachments_json).unwrap()
            [0]
        .receipt_source
        .is_some()
    );
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_current_schema_preserves_records_and_indexes_casefolded_listing() {
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let old = PostgresSessionStore::new(executor).unwrap();
    let mut record = session("preserved-v22");
    record.status = "Active".into();
    record.model = Some("FAST".into());
    old.create_session(&record).unwrap();
    drop(old);
    let upgraded = PostgresSessionStore::new(fixture.reconnect()).unwrap();
    assert_eq!(upgraded.get_session("preserved-v22").unwrap(), Some(record));
    let mut connection = upgraded.executor.checkout_critical().unwrap();
    connection.batch_execute("INSERT INTO session_records(
        session_id,platform,chat_id,model,created_at,last_activity,reset_policy,status,message_count)
        SELECT 'casefold-'||n,'test','test',CASE WHEN n%2=0 THEN 'Fast' ELSE 'Slow' END,
          '2026-01-01T00:00:00Z',to_char(timestamp '2026-01-01' + n*interval '1 second', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
          'None',CASE WHEN n%3=0 THEN 'Active' ELSE 'Closed' END,n
        FROM generate_series(0,9999) n;
        ANALYZE session_records;").unwrap();
    let plan: serde_json::Value = connection
        .query_one(
            "EXPLAIN (FORMAT JSON) SELECT session_id FROM session_records
         WHERE lower(status)=lower($1) AND lower(model)=lower($2)
         ORDER BY last_activity DESC,session_id ASC LIMIT 7",
            &[&"active", &"fast"],
        )
        .unwrap()
        .get(0);
    assert!(
        plan.to_string()
            .contains("idx_session_records_casefold_status_model"),
        "{plan}"
    );
    drop(connection);
    let page = upgraded
        .list_sessions_page(&SessionListOptions {
            status: Some(" ACTIVE "),
            model: Some(" fast "),
            unrestricted: true,
            sort: "last_activity",
            order: "desc",
            limit: 7,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(page.total, 1668);
    assert_eq!(page.records.len(), 7);
    assert!(page
        .records
        .windows(2)
        .all(|rows| rows[0].last_activity >= rows[1].last_activity));
    assert!(page
        .records
        .iter()
        .all(|record| record.status.eq_ignore_ascii_case("active")
            && record.model.as_ref().unwrap().eq_ignore_ascii_case("fast")));
    let reopened = PostgresSessionStore::new(fixture.reconnect()).unwrap();
    assert!(reopened.get_session("preserved-v22").unwrap().is_some());
}

fn session(id: &str) -> SessionRecord {
    SessionRecord {
        session_id: id.to_string(),
        platform: "test".to_string(),
        chat_id: "chat".to_string(),
        user_id: Some("user".to_string()),
        model: Some("model".to_string()),
        created_at: "2026-07-23T00:00:00Z".to_string(),
        last_activity: "2026-07-23T00:00:00Z".to_string(),
        message_count: 0,
        reset_policy: "manual".to_string(),
        metadata_json: Some(
            r#"{"workspace_root":"/work","title":"session migration"}"#.to_string(),
        ),
        input_tokens: 0,
        output_tokens: 0,
        status: "active".to_string(),
    }
}

fn real_store(scope: &PostgresTestScope) -> PostgresSessionStore {
    PostgresSessionStore::new(scope.reconnect()).expect("owned PostgreSQL Session store")
}

fn clear_isolated_store(store: &PostgresSessionStore) {
    let mut connection = store
        .executor
        .checkout_background()
        .expect("isolated PostgreSQL test connection");
    connection
        .batch_execute(
            "TRUNCATE TABLE
                    session_branch_activations,
                    session_lifecycle_intents,
                    session_presence_projection,
                    session_runtime_outbox_history,
                    session_runtime_outbox,
                    session_event_checkpoints,
                    session_snapshots,
                    session_events,
                    session_messages,
                    session_memory_associations,
                    session_recovery_manifest,
                    session_records
                 CASCADE",
        )
        .expect("clear isolated PostgreSQL Session store");
}

fn unique_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos()
    )
}

fn runtime_request(
    id: &str,
    generation: u64,
    decision: InputRoutingDecision,
    target_turn_id: Option<&str>,
    created_at_ms: u64,
) -> SessionRuntimeOutboxRequest {
    SessionRuntimeOutboxRequest {
        input_id: format!("input-{id}"),
        request_id: format!("request-{id}"),
        turn_id: format!("turn-{id}"),
        message_id: format!("message-{id}"),
        session_generation: generation,
        decision,
        target_turn_id: target_turn_id.map(str::to_string),
        classification_json: Some(
            serde_json::json!({"classifier":"test.v1","reason":"contract"}).to_string(),
        ),
        task_route_hint: None,
        created_at_ms,
        runtime_options_json: Some(r#"{"profile":"test"}"#.to_string()),
    }
}

fn append_runtime_input(
    store: &PostgresSessionStore,
    session_id: &str,
    request: &SessionRuntimeOutboxRequest,
) -> SessionRuntimeOutboxRecord {
    store
        .append_ingress_with_runtime_outbox(
            session_id,
            "user",
            Some(r#"[{"type":"text","text":"test input"}]"#),
            request.created_at_ms,
            request,
        )
        .expect("append durable runtime input")
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_usage_summary_decodes_bigint_aggregates() {
    let fixture = PostgresTestScope::new();
    let store = real_store(&fixture);
    clear_isolated_store(&store);
    let session_id = unique_id("usage-summary");
    let mut record = session(&session_id);
    record.platform = "webui".to_string();
    record.model = Some("deepseek-v4-flash".to_string());
    record.message_count = 7;
    record.input_tokens = 12_345;
    record.output_tokens = 678;
    store.create_session(&record).expect("create usage Session");

    let usage = store
        .session_usage_summary(10)
        .expect("decode PostgreSQL usage aggregates");
    assert_eq!(usage.session_count, 1);
    assert_eq!(usage.message_count, 7);
    assert_eq!(usage.input_tokens, 12_345);
    assert_eq!(usage.output_tokens, 678);
    assert_eq!(usage.by_platform["webui"].input_tokens, 12_345);
    assert_eq!(usage.by_model["deepseek-v4-flash"].output_tokens, 678);
    assert_eq!(usage.recent_sessions.len(), 1);
    assert_eq!(usage.recent_sessions[0].session_id, session_id);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_reads_selected_context_ranges_with_one_query() {
    let fixture = PostgresTestScope::new();
    let store = real_store(&fixture);
    clear_isolated_store(&store);
    let session_id = unique_id("context-ranges");
    store
        .create_session(&session(&session_id))
        .expect("create Session");
    let messages = (0..24)
        .map(|sequence| SessionMessage {
            stable_message_id: format!("{session_id}:message:{sequence}"),
            session_id: session_id.clone(),
            sequence,
            role: "user".to_string(),
            content_json: serde_json::json!([
                {"type":"text","text":format!("message {sequence}")}
            ])
            .to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: sequence as u64,
        })
        .collect::<Vec<_>>();
    store
        .insert_messages_batch(&messages)
        .expect("insert messages");

    let selected = store
        .get_messages_in_ranges(&session_id, &[(2, 5), (12, 15)], 32)
        .expect("read exact selected ranges");
    assert_eq!(
        selected
            .iter()
            .map(|message| message.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3, 4, 12, 13, 14]
    );
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_message_batch_eliminates_round_trips_and_beats_legacy_by_fifteen_percent() {
    let fixture = PostgresTestScope::new();
    let store = real_store(&fixture);
    clear_isolated_store(&store);
    let mut legacy_samples = Vec::new();
    let mut batch_samples = Vec::new();

    for round in 0..3 {
        let legacy_session = unique_id(&format!("batch-legacy-{round}"));
        let batch_session = unique_id(&format!("batch-recordset-{round}"));
        store
            .create_session(&session(&legacy_session))
            .expect("create legacy benchmark Session");
        store
            .create_session(&session(&batch_session))
            .expect("create recordset benchmark Session");
        let messages = |session_id: &str| {
            (0..400)
                .map(|sequence| SessionMessage {
                    stable_message_id: format!("{session_id}:message:{sequence}"),
                    session_id: session_id.to_string(),
                    sequence,
                    role: if sequence % 2 == 0 {
                        "user"
                    } else {
                        "assistant"
                    }
                    .to_string(),
                    content_json: serde_json::json!([
                        {"type":"text","text":format!("batch message {sequence}")}
                    ])
                    .to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: sequence as u64,
                })
                .collect::<Vec<_>>()
        };
        let legacy_messages = messages(&legacy_session);
        let batch_messages = messages(&batch_session);

        let run_legacy = || {
            let started = std::time::Instant::now();
            let mut connection = store
                .executor
                .checkout_critical()
                .expect("legacy benchmark connection");
            let mut transaction = connection.transaction().expect("legacy benchmark tx");
            for message in &legacy_messages {
                insert_message_tx(&mut transaction, message).expect("legacy row insert");
            }
            transaction.commit().expect("legacy benchmark commit");
            started.elapsed()
        };
        let run_batch = || {
            let started = std::time::Instant::now();
            store
                .insert_messages_batch(&batch_messages)
                .expect("recordset batch insert");
            started.elapsed()
        };
        if round % 2 == 0 {
            legacy_samples.push(run_legacy());
            batch_samples.push(run_batch());
        } else {
            batch_samples.push(run_batch());
            legacy_samples.push(run_legacy());
        }
        assert_eq!(store.get_message_count(&legacy_session).unwrap(), 400);
        assert_eq!(store.get_message_count(&batch_session).unwrap(), 400);
    }

    legacy_samples.sort_unstable();
    batch_samples.sort_unstable();
    let legacy = legacy_samples[1];
    let batch = batch_samples[1];
    let improvement = (legacy.as_secs_f64() - batch.as_secs_f64()) / legacy.as_secs_f64() * 100.0;
    eprintln!(
        "postgres message batch: legacy={legacy:?} recordset={batch:?} improvement={improvement:.2}%"
    );
    assert!(
        batch.as_nanos().saturating_mul(100) <= legacy.as_nanos().saturating_mul(85),
        "recordset batch improvement {improvement:.2}% is below the 15% phase gate"
    );
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_activation_index_and_manifest_repair_preserve_session_contract() {
    let fixture = PostgresTestScope::new();
    let store = real_store(&fixture);
    clear_isolated_store(&store);
    let session_id = unique_id("activation-index");
    store
        .create_session(&session(&session_id))
        .expect("create Session");
    let messages = (0..300)
        .map(|sequence| SessionMessage {
            stable_message_id: format!("{session_id}:message:{sequence}"),
            session_id: session_id.clone(),
            sequence,
            role: if sequence % 2 == 0 {
                "user"
            } else {
                "assistant"
            }
            .to_string(),
            content_json: serde_json::json!([
                {"type":"text","text":format!("message {sequence}")}
            ])
            .to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: sequence as u64,
        })
        .collect::<Vec<_>>();
    store
        .insert_messages_batch(&messages)
        .expect("insert messages");
    store
        .append_event(&SessionEvent {
            session_id: session_id.clone(),
            event_type: session::SESSION_DOMAIN_EVENT_TYPE.to_string(),
            event_json: serde_json::json!({
                "event_id": "pg-checkpoint",
                "session_id": session_id,
                "sequence": 0,
                "scope": "runtime",
                "kind": "memory.semantic_checkpoint.created",
                "payload": {},
                "created_at_ms": 500
            })
            .to_string(),
            sequence: 0,
            created_at_ms: 500,
        })
        .expect("append checkpoint");

    assert_eq!(
        store
            .get_message_by_stable_id(&session_id, &format!("{session_id}:message:299"))
            .expect("exact message")
            .expect("message exists")
            .sequence,
        299
    );
    assert_eq!(
        store
            .get_message_metadata_page(&session_id, 298, 8)
            .expect("metadata")
            .len(),
        2
    );
    assert_eq!(
        store
            .get_latest_session_domain_event_by_kind(
                &session_id,
                "memory.semantic_checkpoint.created",
            )
            .expect("latest checkpoint")
            .expect("checkpoint exists")
            .sequence,
        0
    );
    let coverage = store
        .reconcile_session_context_index(&session_id, 128, 4, 600)
        .expect("reconcile context index");
    assert!(coverage.complete);
    assert_eq!(coverage.covered_messages, 300);

    let mut connection = store.executor.checkout_critical().expect("connection");
    connection
        .execute(
            "DELETE FROM session_recovery_manifest WHERE session_id=$1",
            &[&session_id],
        )
        .expect("remove manifest");
    drop(connection);
    let rebuilt = store
        .rebuild_session_recovery_manifest(&session_id, 700)
        .expect("rebuild manifest")
        .expect("manifest exists");
    assert_eq!(rebuilt.transcript_messages, 300);
    assert_eq!(
        rebuilt.latest_checkpoint_event_id.as_deref(),
        Some("pg-checkpoint")
    );
    assert!(rebuilt.index_pending);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn current_postgres_claim_fence_epoch_survives_same_schema_restart() {
    let fixture = PostgresTestScope::new();
    let store = real_store(&fixture);
    store
        .create_session(&session("claim-fence-restart"))
        .unwrap();
    let request = runtime_request(
        "claim-fence-restart",
        1,
        InputRoutingDecision::StartNewTurn,
        None,
        100,
    );
    append_runtime_input(&store, "claim-fence-restart", &request);
    let claimed = store
        .claim_session_runtime_outbox("worker", 101, 1000, 1, &[])
        .unwrap()
        .remove(0);
    let running = store
        .mark_session_runtime_outbox_running(
            &request.request_id,
            "worker",
            1,
            claimed.claim_token.as_deref().unwrap(),
            claimed.revision,
            102,
        )
        .unwrap();
    drop(store);
    let reopened = real_store(&fixture)
        .get_session_runtime_outbox(&request.request_id)
        .unwrap()
        .unwrap();
    assert_eq!(reopened.claim_fence_epoch, running.claim_fence_epoch);
    assert_eq!(reopened.claim_token, running.claim_token);
    assert_eq!(reopened.status, SessionRuntimeInputStatus::Running);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_fenced_terminal_commit_preserves_atomic_identity_contract() {
    let fixture = PostgresTestScope::new();
    let store = real_store(&fixture);
    let session_id = unique_id("terminal-fence");
    let id = unique_id("terminal-input");
    store
        .create_session(&session(&session_id))
        .expect("create fenced terminal session");
    let request = runtime_request(&id, 1, InputRoutingDecision::StartNewTurn, None, 100);
    append_runtime_input(&store, &session_id, &request);
    let claimed = store
        .claim_session_runtime_outbox("runtime-worker", 101, 1_000, 1, &[])
        .expect("claim input")
        .remove(0);
    let token = claimed.claim_token.clone().expect("claim token");
    let running = store
        .mark_session_runtime_outbox_running(
            &request.request_id,
            "runtime-worker",
            1,
            &token,
            claimed.revision,
            102,
        )
        .expect("mark running");
    let renewed = store
        .renew_session_runtime_outbox_lease(
            &request.request_id,
            "runtime-worker",
            1,
            &token,
            running.revision,
            103,
            1_000,
        )
        .expect("renew running lease");
    assert!(renewed.revision > running.revision);
    let messages = vec![
        SessionMessage {
            stable_message_id: format!("tool-{id}"),
            session_id: session_id.clone(),
            sequence: 0,
            role: "tool".to_string(),
            content_json: r#"[{"type":"text","text":"evidence"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: Some(format!("tool-use-{id}")),
            tool_name: Some("read".to_string()),
            token_usage_json: None,
            created_at_ms: 0,
        },
        SessionMessage {
            stable_message_id: format!("tool-secondary-{id}"),
            session_id: session_id.clone(),
            sequence: 0,
            role: "tool".to_string(),
            content_json: r#"[{"type":"text","text":"secondary evidence"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: Some(format!("tool-use-secondary-{id}")),
            tool_name: Some("read".to_string()),
            token_usage_json: None,
            created_at_ms: 0,
        },
        SessionMessage {
            stable_message_id: format!("assistant-{id}"),
            session_id: session_id.clone(),
            sequence: 0,
            role: "assistant".to_string(),
            content_json: r#"[{"type":"text","text":"done"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: Some(r#"{"output_tokens":1}"#.to_string()),
            created_at_ms: 0,
        },
    ];
    let commit = SessionTerminalTranscriptCommit {
        terminal_message_id: format!("assistant-{id}"),
        ingress_message_id: request.message_id.clone(),
        session_id: session_id.clone(),
        turn_id: request.turn_id.clone(),
        messages,
        runtime_commit_cursor: 42,
        consumed_input_sequence: running.sequence,
        created_at_ms: 104,
        fence: session::SessionTerminalExecutionFence {
            request_id: request.request_id.clone(),
            input_sequence: running.sequence,
            session_generation: 1,
            claim_owner: "runtime-worker".to_string(),
            claim_token: token,
            claim_fence_epoch: running
                .claim_fence_epoch
                .expect("running input owns an immutable claim fence"),
        },
    };
    let mut wrong_sequence = commit.clone();
    wrong_sequence.fence.input_sequence = wrong_sequence.fence.input_sequence.saturating_add(1);
    wrong_sequence.consumed_input_sequence = wrong_sequence.fence.input_sequence;
    assert!(matches!(
        store.commit_terminal_transcript_if_fenced(&wrong_sequence),
        Err(session::SessionError::StaleExecutionFence(_))
    ));
    let receipt = store
        .commit_terminal_transcript_if_fenced(&commit)
        .expect("commit with renewed live fence");
    assert!(receipt.inserted);
    assert_eq!(receipt.input.status, SessionRuntimeInputStatus::Completed);
    assert_eq!(receipt.input.revision, renewed.revision + 1);
    assert_eq!(store.get_message_count(&session_id).unwrap(), 4);
    let replay = store
        .commit_terminal_transcript_if_fenced(&commit)
        .expect("exact replay");
    assert!(!replay.inserted);
    assert_eq!(replay.messages, receipt.messages);

    let mut reordered_intermediate = commit.clone();
    reordered_intermediate.messages.swap(0, 1);
    assert!(matches!(
        store.commit_terminal_transcript_if_fenced(&reordered_intermediate),
        Err(session::SessionError::StaleExecutionFence(_))
    ));
    assert_eq!(store.get_message_count(&session_id).unwrap(), 4);

    let mut conflicting = commit.clone();
    conflicting.terminal_message_id = format!("assistant-conflict-{id}");
    conflicting
        .messages
        .last_mut()
        .expect("terminal row")
        .stable_message_id = conflicting.terminal_message_id.clone();
    assert!(matches!(
        store.commit_terminal_transcript_if_fenced(&conflicting),
        Err(session::SessionError::StaleExecutionFence(_))
    ));
    assert_eq!(store.get_message_count(&session_id).unwrap(), 4);
    store
        .delete_session(&session_id)
        .expect("delete fenced terminal session");

    let stale_session = unique_id("terminal-stale");
    let stale_id = unique_id("terminal-stale-input");
    store
        .create_session(&session(&stale_session))
        .expect("create stale terminal session");
    let stale_request =
        runtime_request(&stale_id, 1, InputRoutingDecision::StartNewTurn, None, 200);
    append_runtime_input(&store, &stale_session, &stale_request);
    let stale_claim = store
        .claim_session_runtime_outbox("runtime-worker-old", 201, 50, 1, &[])
        .expect("claim stale input")
        .remove(0);
    let stale_token = stale_claim.claim_token.clone().expect("stale token");
    let stale_running = store
        .mark_session_runtime_outbox_running(
            &stale_request.request_id,
            "runtime-worker-old",
            1,
            &stale_token,
            stale_claim.revision,
            202,
        )
        .expect("mark stale input running");
    let stale_commit = SessionTerminalTranscriptCommit {
        terminal_message_id: format!("assistant-{stale_id}"),
        ingress_message_id: stale_request.message_id.clone(),
        session_id: stale_session.clone(),
        turn_id: stale_request.turn_id.clone(),
        messages: vec![SessionMessage {
            stable_message_id: format!("assistant-{stale_id}"),
            session_id: stale_session.clone(),
            sequence: 0,
            role: "assistant".to_string(),
            content_json: r#"[{"type":"text","text":"must not commit"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 0,
        }],
        runtime_commit_cursor: 43,
        consumed_input_sequence: stale_running.sequence,
        created_at_ms: 250,
        fence: session::SessionTerminalExecutionFence {
            request_id: stale_request.request_id.clone(),
            input_sequence: stale_running.sequence,
            session_generation: 1,
            claim_owner: "runtime-worker-old".to_string(),
            claim_token: stale_token.clone(),
            claim_fence_epoch: stale_running
                .claim_fence_epoch
                .expect("running input owns an immutable claim fence"),
        },
    };
    let reclaimed = store
        .claim_session_runtime_outbox("runtime-worker-new", 251, 1_000, 1, &[])
        .expect("reclaim expired input")
        .remove(0);
    assert_ne!(reclaimed.claim_token.as_deref(), Some(stale_token.as_str()));
    assert!(matches!(
        store.commit_terminal_transcript_if_fenced(&stale_commit),
        Err(session::SessionError::StaleExecutionFence(_))
    ));
    assert_eq!(store.get_message_count(&stale_session).unwrap(), 1);
    store
        .advance_session_input_generation(
            &stale_session,
            1,
            true,
            "test",
            "replace stale generation",
            252,
        )
        .expect("advance stale generation");
    assert!(matches!(
        store.commit_terminal_transcript_if_fenced(&stale_commit),
        Err(session::SessionError::StaleExecutionFence(_))
    ));
    assert_eq!(store.get_message_count(&stale_session).unwrap(), 1);
    store
        .delete_session(&stale_session)
        .expect("delete stale terminal session");
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_terminal_commit_and_generation_advance_share_one_lock_order() {
    let fixture = PostgresTestScope::new();
    let store = real_store(&fixture);
    let session_id = unique_id("terminal-lock-order");
    let id = unique_id("terminal-lock-input");
    store
        .create_session(&session(&session_id))
        .expect("create lock-order session");
    let request = runtime_request(&id, 1, InputRoutingDecision::StartNewTurn, None, 100);
    append_runtime_input(&store, &session_id, &request);
    let claimed = store
        .claim_session_runtime_outbox("runtime-worker", 101, 1_000, 1, &[])
        .expect("claim lock-order input")
        .remove(0);
    let token = claimed.claim_token.clone().expect("claim token");
    let running = store
        .mark_session_runtime_outbox_running(
            &request.request_id,
            "runtime-worker",
            1,
            &token,
            claimed.revision,
            102,
        )
        .expect("mark lock-order input running");
    let commit = SessionTerminalTranscriptCommit {
        terminal_message_id: format!("assistant-{id}"),
        ingress_message_id: request.message_id.clone(),
        session_id: session_id.clone(),
        turn_id: request.turn_id.clone(),
        messages: vec![SessionMessage {
            stable_message_id: format!("assistant-{id}"),
            session_id: session_id.clone(),
            sequence: 0,
            role: "assistant".to_string(),
            content_json: r#"[{"type":"text","text":"lock order"}]"#.to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 0,
        }],
        runtime_commit_cursor: 44,
        consumed_input_sequence: running.sequence,
        created_at_ms: 103,
        fence: session::SessionTerminalExecutionFence {
            request_id: request.request_id,
            input_sequence: running.sequence,
            session_generation: 1,
            claim_owner: "runtime-worker".to_string(),
            claim_token: token,
            claim_fence_epoch: running
                .claim_fence_epoch
                .expect("running input owns an immutable claim fence"),
        },
    };
    let barrier = Arc::new(Barrier::new(2));
    let commit_worker = {
        let store = store.clone();
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            store.commit_terminal_transcript_if_fenced(&commit)
        })
    };
    let generation_worker = {
        let store = store.clone();
        let barrier = Arc::clone(&barrier);
        let session_id = session_id.clone();
        std::thread::spawn(move || {
            barrier.wait();
            store.advance_session_input_generation(
                &session_id,
                1,
                true,
                "lock-order-test",
                "race terminal commit",
                104,
            )
        })
    };
    let commit_result = commit_worker.join().expect("commit worker");
    let generation = generation_worker
        .join()
        .expect("generation worker")
        .expect("generation advance cannot deadlock");
    assert_eq!(generation.generation, 2);
    let messages = store
        .get_message_count(&session_id)
        .expect("count lock-order transcript");
    match commit_result {
        Ok(receipt) => {
            assert!(receipt.inserted);
            assert_eq!(messages, 2);
        }
        Err(session::SessionError::StaleExecutionFence(_)) => {
            assert_eq!(messages, 1);
        }
        Err(error) => panic!("unexpected terminal commit result: {error}"),
    }
    store
        .delete_session(&session_id)
        .expect("delete lock-order session");
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_branch_command_commits_every_artifact_or_nothing() {
    let fixture = PostgresTestScope::new();
    let store = real_store(&fixture);
    let source = unique_id("branch-command-source");
    let target = unique_id("branch-command-target");
    let rollback_target = unique_id("branch-command-rollback");
    store
        .create_session(&session(&source))
        .expect("create branch source");
    for sequence in 0..2 {
        store
            .insert_message(&SessionMessage {
                stable_message_id: format!("{source}-message-{sequence}"),
                session_id: source.clone(),
                sequence,
                role: "user".to_string(),
                content_json: format!(r#"[{{"type":"text","text":"source message {sequence}"}}]"#),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: Some(r#"{"input_tokens":3,"output_tokens":2}"#.to_string()),
                created_at_ms: 100 + sequence as u64,
            })
            .expect("append source message");
    }
    let source_generation = store
        .get_session_input_admission(&source)
        .expect("read source admission")
        .expect("source admission exists")
        .generation;
    let request = SessionBranchRequest {
        operation_id: format!("branch-{source}-{target}-1"),
        source_session_id: source.clone(),
        source_message_count: 1,
        target: session(&target),
        source_event_json: serde_json::json!({
            "kind": "session.branched",
            "target_session_id": target.clone(),
            "source_message_count": 1
        })
        .to_string(),
        target_event_json: serde_json::json!({
            "kind": "session.branch_created",
            "source_session_id": source.clone(),
            "source_message_count": 1
        })
        .to_string(),
        created_at_ms: 200,
    };
    let result = store
        .branch_session_at_cutoff(&request)
        .expect("branch command commits");
    assert_eq!(result.copied_message_count, 1);
    assert_eq!(result.source_message_count, 1);
    assert_eq!(result.target.message_count, 1);
    assert_eq!(
        result.activation.phase,
        SessionBranchActivationPhase::BranchCommitted
    );
    let replay = store
        .branch_session_at_cutoff(&request)
        .expect("committed branch retry resumes activation receipt");
    assert_eq!(replay.target.session_id, target);
    assert_eq!(replay.copied_message_count, 1);
    let persisted_target = store
        .get_session(&target)
        .expect("read target")
        .expect("target exists");
    assert_eq!(persisted_target.message_count, 1);
    assert_eq!(persisted_target.input_tokens, 3);
    assert_eq!(persisted_target.output_tokens, 2);
    let copied = store
        .get_all_messages(&target)
        .expect("read branch messages");
    assert_eq!(copied.len(), 1);
    assert_eq!(copied[0].sequence, 0);
    assert!(copied[0]
        .stable_message_id
        .starts_with(&format!("branch:{target}:")));
    let source_events = store
        .get_events_limited(&source, 0, 10)
        .expect("read source branch event");
    assert_eq!(
        source_events
            .iter()
            .filter(|event| event.event_type == "SessionBranched")
            .count(),
        1
    );
    let target_events = store
        .get_events_limited(&target, 0, 10)
        .expect("read target branch event");
    assert_eq!(
        target_events
            .iter()
            .filter(|event| event.event_type == "BranchCreated")
            .count(),
        1
    );
    assert_eq!(
        store
            .get_session_input_admission(&source)
            .expect("read source admission after branch")
            .expect("source admission remains")
            .generation,
        source_generation,
        "branch command must not advance source generation"
    );
    let activation_pending = store
        .transition_session_branch_activation(&SessionBranchActivationTransition {
            operation_id: request.operation_id.clone(),
            expected_revision: result.activation.revision,
            expected_phase: SessionBranchActivationPhase::BranchCommitted,
            next_phase: SessionBranchActivationPhase::ActivationPending,
            updated_at_ms: 204,
            error: None,
        })
        .expect("fence Gateway activation after branch commit");
    let failed = store
        .transition_session_branch_activation(&SessionBranchActivationTransition {
            operation_id: request.operation_id.clone(),
            expected_revision: activation_pending.revision,
            expected_phase: SessionBranchActivationPhase::ActivationPending,
            next_phase: SessionBranchActivationPhase::Failed,
            updated_at_ms: 205,
            error: Some("simulated activation failure".to_string()),
        })
        .expect("persist branch activation failure");
    assert!(store
        .list_recoverable_session_branch_activations(10)
        .expect("list recoverable branch activations")
        .iter()
        .any(|activation| activation.operation_id == request.operation_id));
    let pending = store
        .transition_session_branch_activation(&SessionBranchActivationTransition {
            operation_id: request.operation_id.clone(),
            expected_revision: failed.revision,
            expected_phase: SessionBranchActivationPhase::Failed,
            next_phase: SessionBranchActivationPhase::ActivationPending,
            updated_at_ms: 206,
            error: None,
        })
        .expect("resume branch activation");
    let activated = store
        .transition_session_branch_activation(&SessionBranchActivationTransition {
            operation_id: request.operation_id.clone(),
            expected_revision: pending.revision,
            expected_phase: SessionBranchActivationPhase::ActivationPending,
            next_phase: SessionBranchActivationPhase::Activated,
            updated_at_ms: 207,
            error: None,
        })
        .expect("complete branch activation");
    assert_eq!(activated.phase, SessionBranchActivationPhase::Activated);

    let source_event_count = source_events.len();
    let rollback_request = SessionBranchRequest {
        operation_id: format!("branch-{source}-{rollback_target}-2"),
        source_session_id: source.clone(),
        source_message_count: 2,
        target: session(&rollback_target),
        source_event_json: r#"{"kind":"session.branched"}"#.to_string(),
        target_event_json: "{invalid-json".to_string(),
        created_at_ms: 201,
    };
    assert!(store.branch_session_at_cutoff(&rollback_request).is_err());
    assert!(store
        .get_session(&rollback_target)
        .expect("read rolled back target")
        .is_none());
    assert_eq!(
        store
            .get_events_limited(&source, 0, 10)
            .expect("read source events after rollback")
            .len(),
        source_event_count
    );

    store.delete_session(&target).expect("delete branch target");
    store.delete_session(&source).expect("delete branch source");
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_presence_projection_is_mutable_and_does_not_append_history() {
    let fixture = PostgresTestScope::new();
    let session_id = unique_id("presence-projection");
    let store = real_store(&fixture);
    store
        .create_session(&session(&session_id))
        .expect("create presence Session");
    let attachment = session::SessionAttachment {
        session_id: session_id.clone(),
        actor: session::SessionActor {
            id: "web-1".to_string(),
            surface: "webui".to_string(),
            role: Some("reader".to_string()),
        },
        attached_at_ms: 100,
        last_seen_ms: 100,
        client_id: None,
        client_expires_at_ms: None,
        receipt_protocol: None,
        receipt_source: None,
    };
    store
        .upsert_session_presence_projection(&SessionPresenceProjection {
            session_id: session_id.clone(),
            state: "attached".to_string(),
            attachments_json: serde_json::to_string(&vec![attachment]).unwrap(),
            next_sequence: 1,
            revision: 1,
            updated_at_ms: 100,
        })
        .expect("insert presence projection");
    assert!(
        store
            .get_session_recovery_manifest(&session_id)
            .expect("presence recovery manifest")
            .expect("presence recovery row")
            .active_writer_or_attachment
    );

    store
        .upsert_session_presence_projection(&SessionPresenceProjection {
            session_id: session_id.clone(),
            state: "detached".to_string(),
            attachments_json: "[]".to_string(),
            next_sequence: 1,
            revision: 2,
            updated_at_ms: 200,
        })
        .expect("expire presence projection");
    assert!(
        !store
            .get_session_recovery_manifest(&session_id)
            .expect("expired recovery manifest")
            .expect("expired recovery row")
            .active_writer_or_attachment
    );
    assert!(
        store
            .get_events_limited(&session_id, 0, 10)
            .expect("presence Session history")
            .is_empty(),
        "mutable presence must not append immutable Session events"
    );
    store
        .delete_session(&session_id)
        .expect("delete presence Session");
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_lifecycle_intent_recovers_each_phase_and_commits_one_tombstone() {
    let fixture = PostgresTestScope::new();
    let session_id = unique_id("lifecycle-recovery");
    let operation_id = format!("session-lifecycle:archive:{session_id}");
    let store = real_store(&fixture);
    store
        .create_session(&session(&session_id))
        .expect("create lifecycle session");
    let input = runtime_request(
        &format!("{session_id}-input"),
        1,
        InputRoutingDecision::StartNewTurn,
        None,
        100,
    );
    append_runtime_input(&store, &session_id, &input);
    let planned = store
        .plan_session_lifecycle(&SessionLifecyclePlan {
            operation_id: operation_id.clone(),
            session_id: session_id.clone(),
            disposition: session::SessionCloseDisposition::Archive,
            expected_generation: 1,
            created_at_ms: 110,
        })
        .expect("plan lifecycle");
    assert_eq!(planned.phase, SessionLifecyclePhase::Planned);
    drop(store);

    let store = real_store(&fixture);
    let fenced = store
        .fence_session_lifecycle(&SessionLifecycleFenceRequest {
            transition: SessionLifecycleTransition {
                operation_id: operation_id.clone(),
                expected_revision: planned.revision,
                expected_phase: SessionLifecyclePhase::Planned,
                next_phase: SessionLifecyclePhase::AdmissionFenced,
                updated_at_ms: 120,
                error: None,
            },
            actor: "postgres-contract".to_string(),
            reason: "archive".to_string(),
            transitional_status: "archiving".to_string(),
            event: SessionEvent {
                session_id: session_id.clone(),
                event_type: "session.archive_started".to_string(),
                event_json: r#"{"kind":"session.archive_started"}"#.to_string(),
                sequence: 0,
                created_at_ms: 120,
            },
        })
        .expect("fence lifecycle");
    assert_eq!(fenced.phase, SessionLifecyclePhase::AdmissionFenced);
    assert!(store
        .session_runtime_outbox_for_session(&session_id, 10)
        .unwrap()
        .iter()
        .all(|input| input.status == SessionRuntimeInputStatus::Expired));
    let failed = store
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id.clone(),
            expected_revision: fenced.revision,
            expected_phase: SessionLifecyclePhase::AdmissionFenced,
            next_phase: SessionLifecyclePhase::Failed,
            updated_at_ms: 121,
            error: Some("simulated worker crash".to_string()),
        })
        .expect("persist lifecycle failure");
    drop(store);

    let store = real_store(&fixture);
    assert!(store
        .list_recoverable_session_lifecycle_intents(10)
        .unwrap()
        .iter()
        .any(|intent| intent.operation_id == operation_id));
    let resumed = store
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id.clone(),
            expected_revision: failed.revision,
            expected_phase: SessionLifecyclePhase::Failed,
            next_phase: SessionLifecyclePhase::AdmissionFenced,
            updated_at_ms: 130,
            error: None,
        })
        .expect("resume lifecycle");
    let drained = store
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id.clone(),
            expected_revision: resumed.revision,
            expected_phase: SessionLifecyclePhase::AdmissionFenced,
            next_phase: SessionLifecyclePhase::RuntimeDrained,
            updated_at_ms: 140,
            error: None,
        })
        .expect("mark Runtime drained");
    drop(store);

    let store = real_store(&fixture);
    let mut record = store
        .get_session(&session_id)
        .unwrap()
        .expect("lifecycle Session");
    record.status = "archived".to_string();
    record.last_activity = "2026-07-26T00:00:01Z".to_string();
    record.metadata_json = Some(r#"{"tombstone":{"kind":"archived"}}"#.to_string());
    let tombstone = SessionLifecycleTombstoneRequest {
        transition: SessionLifecycleTransition {
            operation_id: operation_id.clone(),
            expected_revision: drained.revision,
            expected_phase: SessionLifecyclePhase::RuntimeDrained,
            next_phase: SessionLifecyclePhase::TombstoneCommitted,
            updated_at_ms: 150,
            error: None,
        },
        record,
        event: SessionEvent {
            session_id: session_id.clone(),
            event_type: "session.archived".to_string(),
            event_json: r#"{"kind":"session.archived"}"#.to_string(),
            sequence: 0,
            created_at_ms: 150,
        },
    };
    let committed = store
        .commit_session_lifecycle_tombstone(&tombstone)
        .expect("commit lifecycle tombstone");
    assert_eq!(committed.phase, SessionLifecyclePhase::TombstoneCommitted);
    drop(store);

    let store = real_store(&fixture);
    assert_eq!(
        store
            .get_events_limited(&session_id, 0, 100)
            .unwrap()
            .iter()
            .filter(|event| event.event_type == "session.archived")
            .count(),
        1
    );
    let unloaded = store
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id.clone(),
            expected_revision: committed.revision,
            expected_phase: SessionLifecyclePhase::TombstoneCommitted,
            next_phase: SessionLifecyclePhase::Unloaded,
            updated_at_ms: 160,
            error: None,
        })
        .expect("mark lifecycle unloaded");
    assert_eq!(unloaded.phase, SessionLifecyclePhase::Unloaded);
    assert!(store
        .commit_session_lifecycle_tombstone(&tombstone)
        .is_err());
    assert_eq!(
        store
            .get_events_limited(&session_id, 0, 100)
            .unwrap()
            .iter()
            .filter(|event| event.event_type == "session.archived")
            .count(),
        1
    );
    store
        .delete_session(&session_id)
        .expect("cleanup lifecycle Session");
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_delete_lifecycle_recovers_stable_phases_and_commits_one_tombstone() {
    let fixture = PostgresTestScope::new();
    let session_id = unique_id("delete-lifecycle-recovery");
    let operation_id = format!("session-lifecycle:delete:{session_id}");
    let store = real_store(&fixture);
    store
        .create_session(&session(&session_id))
        .expect("create delete lifecycle Session");
    let planned = store
        .plan_session_lifecycle(&SessionLifecyclePlan {
            operation_id: operation_id.clone(),
            session_id: session_id.clone(),
            disposition: session::SessionCloseDisposition::Delete,
            expected_generation: 1,
            created_at_ms: 100,
        })
        .expect("plan delete lifecycle");
    drop(store);

    let store = real_store(&fixture);
    let fenced = store
        .fence_session_lifecycle(&SessionLifecycleFenceRequest {
            transition: SessionLifecycleTransition {
                operation_id: operation_id.clone(),
                expected_revision: planned.revision,
                expected_phase: SessionLifecyclePhase::Planned,
                next_phase: SessionLifecyclePhase::AdmissionFenced,
                updated_at_ms: 110,
                error: None,
            },
            actor: "postgres-contract".to_string(),
            reason: "delete".to_string(),
            transitional_status: "deleting".to_string(),
            event: SessionEvent {
                session_id: session_id.clone(),
                event_type: "session.delete_started".to_string(),
                event_json: r#"{"kind":"session.delete_started"}"#.to_string(),
                sequence: 0,
                created_at_ms: 110,
            },
        })
        .expect("fence delete lifecycle");
    drop(store);

    let store = real_store(&fixture);
    let drained = store
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id.clone(),
            expected_revision: fenced.revision,
            expected_phase: SessionLifecyclePhase::AdmissionFenced,
            next_phase: SessionLifecyclePhase::RuntimeDrained,
            updated_at_ms: 120,
            error: None,
        })
        .expect("mark delete Runtime drained");
    drop(store);

    let store = real_store(&fixture);
    let mut record = store
        .get_session(&session_id)
        .unwrap()
        .expect("delete lifecycle Session");
    record.status = "deleted".to_string();
    record.last_activity = "2026-07-26T00:00:01Z".to_string();
    record.metadata_json = Some(r#"{"tombstone":{"kind":"deleted"}}"#.to_string());
    let request = SessionLifecycleTombstoneRequest {
        transition: SessionLifecycleTransition {
            operation_id: operation_id.clone(),
            expected_revision: drained.revision,
            expected_phase: SessionLifecyclePhase::RuntimeDrained,
            next_phase: SessionLifecyclePhase::TombstoneCommitted,
            updated_at_ms: 130,
            error: None,
        },
        record,
        event: SessionEvent {
            session_id: session_id.clone(),
            event_type: "session.deleted".to_string(),
            event_json: r#"{"kind":"session.deleted"}"#.to_string(),
            sequence: 0,
            created_at_ms: 130,
        },
    };
    let committed = store
        .commit_session_lifecycle_tombstone(&request)
        .expect("commit delete lifecycle tombstone");
    drop(store);

    let store = real_store(&fixture);
    assert_eq!(
        store
            .get_events_limited(&session_id, 0, 100)
            .unwrap()
            .iter()
            .filter(|event| event.event_type == "session.deleted")
            .count(),
        1
    );
    let unloaded = store
        .transition_session_lifecycle(&SessionLifecycleTransition {
            operation_id: operation_id,
            expected_revision: committed.revision,
            expected_phase: SessionLifecyclePhase::TombstoneCommitted,
            next_phase: SessionLifecyclePhase::Unloaded,
            updated_at_ms: 140,
            error: None,
        })
        .expect("unload deleted Session");
    assert_eq!(unloaded.phase, SessionLifecyclePhase::Unloaded);
    assert!(store.commit_session_lifecycle_tombstone(&request).is_err());
    assert_eq!(
        store
            .get_events_limited(&session_id, 0, 100)
            .unwrap()
            .iter()
            .filter(|event| event.event_type == "session.deleted")
            .count(),
        1
    );
    store
        .delete_session(&session_id)
        .expect("cleanup deleted Session");
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_durable_input_contract_is_fenced_ordered_and_auditable() {
    let fixture = PostgresTestScope::new();
    let store = real_store(&fixture);
    let source = unique_id("durable-source");
    let peer = unique_id("durable-peer");
    let branch = unique_id("durable-branch");
    let rejected = unique_id("durable-rejected");
    for id in [&source, &peer, &branch, &rejected] {
        store
            .create_session(&session(id))
            .expect("create isolated session");
    }

    let source_first = runtime_request(
        &format!("{source}-1"),
        1,
        InputRoutingDecision::StartNewTurn,
        None,
        100,
    );
    let source_second = runtime_request(
        &format!("{source}-2"),
        1,
        InputRoutingDecision::EnqueueNextStep,
        None,
        101,
    );
    let peer_first = runtime_request(
        &format!("{peer}-1"),
        1,
        InputRoutingDecision::StartNewTurn,
        None,
        102,
    );
    let wrong_generation = runtime_request(
        &format!("{source}-wrong-generation"),
        2,
        InputRoutingDecision::StartNewTurn,
        None,
        99,
    );
    assert!(store
        .append_ingress_with_runtime_outbox(
            &source,
            "user",
            Some(r#"[{"type":"text","text":"must roll back"}]"#),
            99,
            &wrong_generation,
        )
        .is_err());
    assert_eq!(
        store.get_message_count(&source).expect("message count"),
        0,
        "rejected generation must not leave a transcript row"
    );
    let first = append_runtime_input(&store, &source, &source_first);
    let second = append_runtime_input(&store, &source, &source_second);
    let peer_record = append_runtime_input(&store, &peer, &peer_first);
    let rejected_duplicate = append_runtime_input(
        &store,
        &rejected,
        &runtime_request(
            &format!("{rejected}-duplicate"),
            1,
            InputRoutingDecision::RejectDuplicate,
            None,
            103,
        ),
    );
    let rejected_policy = append_runtime_input(
        &store,
        &rejected,
        &runtime_request(
            &format!("{rejected}-policy"),
            1,
            InputRoutingDecision::RejectPolicy,
            None,
            104,
        ),
    );
    assert_eq!(
        rejected_duplicate.status,
        SessionRuntimeInputStatus::RejectedDuplicate
    );
    assert_eq!(
        rejected_policy.status,
        SessionRuntimeInputStatus::RejectedPolicy
    );
    assert_eq!(rejected_duplicate.terminal_at_ms, Some(103));
    assert_eq!(rejected_policy.terminal_at_ms, Some(104));
    assert_eq!(first.status, SessionRuntimeInputStatus::Queued);
    assert_eq!(first.revision, 2);
    assert_eq!(second.sequence, 1);
    assert_eq!(
        store
            .get_session_runtime_outbox_by_input_id(&source_first.input_id)
            .expect("lookup input")
            .expect("input exists"),
        first
    );
    assert_eq!(
        store
            .get_session_domain_timeline_limited(&source, 0, 20)
            .expect("input timeline")
            .iter()
            .filter(|event| event.event_json.contains("session.input."))
            .count(),
        6,
        "accepted, classified and queued must be atomic timeline evidence"
    );

    let claimed = store
        .claim_session_runtime_outbox("worker-a", 200, 1_000, 10, &[])
        .expect("claim session heads");
    assert_eq!(claimed.len(), 2, "one head per Session may be claimed");
    assert!(
        claimed.iter().all(|item| item.session_id != rejected),
        "terminal policy decisions must never enter the runnable claim set"
    );
    assert!(claimed.iter().any(|item| item.input_id == first.input_id));
    assert!(claimed
        .iter()
        .any(|item| item.input_id == peer_record.input_id));
    assert!(
        !claimed.iter().any(|item| item.input_id == second.input_id),
        "same-Session second input must remain behind the active head"
    );

    let first_claim = claimed
        .iter()
        .find(|item| item.input_id == first.input_id)
        .expect("source head claimed");
    let token = first_claim
        .claim_token
        .as_deref()
        .expect("claim token issued");
    let running = store
        .mark_session_runtime_outbox_running(
            &first_claim.request_id,
            "worker-a",
            1,
            token,
            first_claim.revision,
            201,
        )
        .expect("mark running");
    assert!(store
        .ack_session_runtime_outbox(
            &running.request_id,
            "worker-a",
            1,
            "stale-token",
            running.revision,
            SessionRuntimeInputStatus::Completed,
            1,
            202,
        )
        .is_err());
    let renewed = store
        .renew_session_runtime_outbox_lease(
            &running.request_id,
            "worker-a",
            1,
            token,
            running.revision,
            202,
            1_000,
        )
        .expect("renew running lease");
    let completed = store
        .ack_session_runtime_outbox(
            &renewed.request_id,
            "worker-a",
            1,
            token,
            renewed.revision,
            SessionRuntimeInputStatus::Completed,
            7,
            203,
        )
        .expect("ack completed input");
    assert_eq!(completed.status, SessionRuntimeInputStatus::Completed);
    assert_eq!(completed.runtime_commit_cursor, Some(7));
    assert_eq!(completed.terminal_at_ms, Some(203));

    let next = store
        .claim_session_runtime_outbox("worker-b", 204, 1_000, 10, &[])
        .expect("claim released Session head");
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].input_id, second.input_id);
    let requeued = store
        .requeue_claimed_session_runtime_outbox(
            &next[0].request_id,
            "worker-b",
            1,
            next[0].claim_token.as_deref().expect("claim token"),
            next[0].revision,
            InputRoutingDecision::StartNewTurn,
            None,
            Some(r#"{"classifier":"target-lost.v1"}"#),
            "target turn is no longer active",
            205,
        )
        .expect("owner-fenced requeue");
    assert_eq!(requeued.status, SessionRuntimeInputStatus::Reclassified);
    assert_eq!(requeued.decision, InputRoutingDecision::StartNewTurn);
    let reclaimed = store
        .claim_session_runtime_outbox("worker-c", 206, 1_000, 10, &[])
        .expect("reclaim reclassified input");
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].input_id, second.input_id);

    let peer_claim = claimed
        .iter()
        .find(|item| item.input_id == peer_record.input_id)
        .expect("peer head claimed");
    let cancelled = store
        .cancel_session_runtime_outbox(
            &peer_claim.input_id,
            1,
            peer_claim.revision,
            "operator",
            "cancel peer test input",
            207,
        )
        .expect("cancel by input id");
    assert_eq!(cancelled.status, SessionRuntimeInputStatus::Cancelled);
    let peer_second = runtime_request(
        &format!("{peer}-2"),
        1,
        InputRoutingDecision::EnqueueNextStep,
        None,
        208,
    );
    let peer_second = append_runtime_input(&store, &peer, &peer_second);
    let peer_second = store
        .reclassify_session_runtime_outbox(
            &peer_second.input_id,
            1,
            peer_second.revision,
            InputRoutingDecision::StartNewTurn,
            None,
            Some(r#"{"classifier":"operator.v1"}"#),
            "operator",
            "explicit reroute",
            209,
        )
        .expect("reclassify queued input");
    assert_eq!(peer_second.status, SessionRuntimeInputStatus::Reclassified);

    let generation_before_branch = store
        .get_session_input_admission(&source)
        .expect("source admission")
        .expect("source exists")
        .generation;
    assert_eq!(
        store
            .copy_session_messages_at_cutoff(&source, &branch, 1)
            .expect("copy immutable branch cutoff"),
        1
    );
    let branch_messages = store.get_all_messages(&branch).expect("branch messages");
    assert_eq!(branch_messages.len(), 1);
    assert_eq!(branch_messages[0].sequence, 0);
    assert!(branch_messages[0]
        .stable_message_id
        .starts_with(&format!("branch:{branch}:")));
    assert!(store
        .copy_session_messages_at_cutoff(&source, &branch, 1)
        .is_err());
    assert_eq!(
        store
            .get_session_input_admission(&source)
            .expect("source admission")
            .expect("source exists")
            .generation,
        generation_before_branch,
        "branch copy must never advance source generation"
    );

    let closed = store
        .close_session_input_admission(
            &source,
            generation_before_branch,
            "operator",
            "close test source",
            210,
        )
        .expect("close admission and expire owned work");
    assert!(!closed.open);
    assert_eq!(closed.generation, generation_before_branch + 1);
    let expired = store
        .get_session_runtime_outbox(&reclaimed[0].request_id)
        .expect("expired lookup")
        .expect("expired input exists");
    assert_eq!(expired.status, SessionRuntimeInputStatus::Expired);
    assert!(store
        .mark_session_runtime_outbox_running(
            &reclaimed[0].request_id,
            "worker-c",
            generation_before_branch,
            reclaimed[0].claim_token.as_deref().expect("claim token"),
            reclaimed[0].revision,
            211,
        )
        .is_err());
    let health = store
        .session_runtime_outbox_health()
        .expect("runtime input health");
    assert!(health.completed >= 1);
    assert!(health.rejected_duplicate >= 1);
    assert!(health.rejected_policy >= 1);
    assert!(health.cancelled >= 1);
    assert!(health.expired >= 1);
    assert!(health.reclassified >= 1);

    for id in [&source, &peer, &branch, &rejected] {
        store.delete_session(id).expect("delete isolated session");
    }
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_batched_execution_history_limits_turn_roots_after_filtering_related_inputs() {
    let fixture = PostgresTestScope::new();
    let store = real_store(&fixture);
    let session_id = unique_id("execution-root-recovery");
    store
        .create_session(&session(&session_id))
        .expect("create isolated session");
    append_runtime_input(
        &store,
        &session_id,
        &runtime_request(
            "pg-root-recovery",
            1,
            InputRoutingDecision::StartNewTurn,
            None,
            100,
        ),
    );
    for index in 0..3 {
        append_runtime_input(
            &store,
            &session_id,
            &runtime_request(
                &format!("pg-root-supplement-{index}"),
                1,
                InputRoutingDecision::SupplementCurrentTurn,
                Some("turn-pg-root-recovery"),
                101 + index,
            ),
        );
    }
    append_runtime_input(
        &store,
        &session_id,
        &runtime_request(
            "pg-root-rejected",
            1,
            InputRoutingDecision::RejectPolicy,
            None,
            110,
        ),
    );

    let roots = store
        .session_runtime_outbox_for_sessions(std::slice::from_ref(&session_id), 1)
        .expect("load root execution history");

    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].decision, InputRoutingDecision::StartNewTurn);
    assert_eq!(roots[0].request_id, "request-pg-root-recovery");
    store
        .delete_session(&session_id)
        .expect("delete isolated session");
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_runtime_failure_retry_and_terminal_statuses_are_real() {
    let fixture = PostgresTestScope::new();
    let store = real_store(&fixture);
    let session_id = unique_id("durable-failure");
    store
        .create_session(&session(&session_id))
        .expect("create isolated session");
    let request = runtime_request(&session_id, 1, InputRoutingDecision::StartNewTurn, None, 10);
    let explicit_message = SessionMessage {
        stable_message_id: request.message_id.clone(),
        session_id: session_id.clone(),
        sequence: 0,
        role: "user".to_string(),
        content_json: r#"[{"type":"text","text":"failure path"}]"#.to_string(),
        blocks_count: 1,
        tool_use_id: None,
        tool_name: None,
        token_usage_json: None,
        created_at_ms: 10,
    };
    let queued = store
        .append_message_with_runtime_outbox(&explicit_message, &request)
        .expect("append explicit message and durable input atomically");
    assert_eq!(queued.status, SessionRuntimeInputStatus::Queued);
    let claimed = store
        .claim_session_runtime_outbox("failure-worker", 20, 100, 1, &[])
        .expect("claim failure input")
        .pop()
        .expect("failure input claimed");
    let running = store
        .mark_session_runtime_outbox_running(
            &claimed.request_id,
            "failure-worker",
            1,
            claimed.claim_token.as_deref().expect("claim token"),
            claimed.revision,
            21,
        )
        .expect("mark failure input running");
    let queued = store
        .fail_session_runtime_outbox(
            &running.request_id,
            "failure-worker",
            1,
            running.claim_token.as_deref().expect("claim token"),
            running.revision,
            OutboxFailureClass::Retryable,
            "temporary dependency failure",
            30,
            3,
            22,
        )
        .expect("schedule retry");
    assert_eq!(queued.status, SessionRuntimeInputStatus::Queued);
    assert!(queued.claim_token.is_none());
    assert!(store
        .claim_session_runtime_outbox("early-worker", 29, 100, 1, &[])
        .expect("early claim")
        .is_empty());
    let claimed = store
        .claim_session_runtime_outbox("blocked-worker", 30, 100, 1, &[])
        .expect("claim retry")
        .pop()
        .expect("retry claimed");
    let blocked = store
        .fail_session_runtime_outbox(
            &claimed.request_id,
            "blocked-worker",
            1,
            claimed.claim_token.as_deref().expect("claim token"),
            claimed.revision,
            OutboxFailureClass::AuthorizationBlocked,
            "approval required",
            31,
            3,
            31,
        )
        .expect("block authorization failure");
    assert_eq!(blocked.status, SessionRuntimeInputStatus::Blocked);
    assert!(blocked.terminal_at_ms.is_none());
    let queued = store
        .retry_blocked_session_runtime_outbox(
            &blocked.request_id,
            1,
            blocked.revision,
            "operator",
            "approval granted",
            32,
        )
        .expect("release blocked input");
    let claimed = store
        .claim_session_runtime_outbox("permanent-worker", 33, 100, 1, &[])
        .expect("claim released input")
        .pop()
        .expect("released input claimed");
    assert_eq!(claimed.request_id, queued.request_id);
    let failed = store
        .fail_session_runtime_outbox(
            &claimed.request_id,
            "permanent-worker",
            1,
            claimed.claim_token.as_deref().expect("claim token"),
            claimed.revision,
            OutboxFailureClass::Permanent,
            "permanent runtime failure",
            34,
            3,
            34,
        )
        .expect("record permanent failure");
    assert_eq!(failed.status, SessionRuntimeInputStatus::Failed);
    assert_eq!(failed.terminal_at_ms, Some(34));
    assert!(store
        .retry_blocked_session_runtime_outbox(
            &failed.request_id,
            1,
            failed.revision,
            "operator",
            "must not retry terminal failure",
            35,
        )
        .is_err());
    store
        .delete_session(&session_id)
        .expect("delete isolated session");
}

#[test]
fn current_session_baseline_retains_input_and_target_turn_fences() {
    let ddl = SESSION_MIGRATIONS[0].statements.join("\n");
    for required in [
        "idx_session_runtime_outbox_input_id",
        "idx_session_runtime_outbox_target_turn",
        "session_generation",
        "input_generation",
        "claim_fence_epoch",
        "application_receipt_json",
        "idx_session_runtime_outbox_session_head",
    ] {
        assert!(
            ddl.contains(required),
            "missing current input fence {required}"
        );
    }
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_current_baseline_initialization_is_atomic() {
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let failing = PostgresMigrationSpec {
        id: "session.atomic-negative",
        domain: SESSION_DOMAIN,
        version: 1,
        description: "atomic baseline failure fixture",
        statements: &[
            "CREATE TABLE baseline_fault_marker(identity TEXT)",
            "SELECT 1/0",
        ],
    };
    assert!(executor
        .apply_migrations(SESSION_DOMAIN, &[failing])
        .is_err());
    let mut connection = executor.checkout_online_read().unwrap();
    assert!(!connection
        .query_one(
            "SELECT to_regclass('baseline_fault_marker') IS NOT NULL",
            &[]
        )
        .unwrap()
        .get::<_, bool>(0));
    assert_eq!(
        connection
            .query_one(
                "SELECT COUNT(*) FROM cowd_schema_migrations WHERE domain='session'",
                &[]
            )
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    drop(connection);
    let store = real_store(&fixture);
    store.create_session(&session("baseline-restart")).unwrap();
    drop(store);
    assert!(real_store(&fixture)
        .get_session("baseline-restart")
        .unwrap()
        .is_some());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_concurrent_store_startup_serializes_preflight_and_migrations() {
    let fixture = PostgresTestScope::new();
    let fixture = Arc::new(fixture);
    let worker_count = 8;
    let gate = Arc::new(Barrier::new(worker_count));
    let workers = (0..worker_count)
        .map(|_| {
            let gate = Arc::clone(&gate);
            let fixture = Arc::clone(&fixture);
            std::thread::spawn(move || {
                let executor = fixture.reconnect();
                gate.wait();
                PostgresSessionStore::new(executor).expect("concurrent scoped store startup")
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().expect("startup worker does not panic");
    }
    let store = real_store(&fixture);
    let mut connection = store
        .executor
        .checkout_background()
        .expect("isolated PostgreSQL verification connection");
    let obsolete_tables: i64 = connection
        .query_one(
            "SELECT COUNT(*)
                   FROM information_schema.tables
                  WHERE table_schema=current_schema()
                    AND table_name IN (
                        'session_mission_outbox',
                        'session_mission_outbox_history'
                    )",
            &[],
        )
        .expect("query obsolete Session Mission tables")
        .get(0);
    assert_eq!(obsolete_tables, 0);
    let route_hint_columns: i64 = connection
        .query_one(
            "SELECT COUNT(*)
                   FROM information_schema.columns
                  WHERE table_schema=current_schema()
                    AND table_name='session_runtime_outbox'
                    AND column_name='task_route_hint_json'",
            &[],
        )
        .expect("query durable Task route hint column")
        .get(0);
    assert_eq!(route_hint_columns, 1);
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_fixture_namespaces_are_disjoint_and_drop_only_their_own_data() {
    let first = PostgresTestScope::new();
    let second = PostgresTestScope::new();
    let first_executor = first.reconnect();
    let second_executor = second.reconnect();
    let first_schema: String = first_executor
        .checkout_critical()
        .unwrap()
        .query_one("SELECT current_schema()", &[])
        .unwrap()
        .get(0);
    let second_schema: String = second_executor
        .checkout_critical()
        .unwrap()
        .query_one("SELECT current_schema()", &[])
        .unwrap()
        .get(0);
    assert_ne!(first_schema, second_schema);
    assert_ne!(first_schema, "public");
    assert_ne!(second_schema, "public");
    first_executor
        .checkout_critical()
        .unwrap()
        .batch_execute(
            "CREATE TABLE isolation_probe(value INTEGER); INSERT INTO isolation_probe VALUES (1)",
        )
        .unwrap();
    second_executor
        .checkout_critical()
        .unwrap()
        .batch_execute(
            "CREATE TABLE isolation_probe(value INTEGER); INSERT INTO isolation_probe VALUES (2)",
        )
        .unwrap();
    let reopened = first.reconnect();
    let value: i32 = reopened
        .checkout_critical()
        .unwrap()
        .query_one("SELECT value FROM isolation_probe", &[])
        .unwrap()
        .get(0);
    assert_eq!(value, 1);
    drop(first);
    let mut reader = second_executor.checkout_critical().unwrap();
    let exists: bool = reader
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)",
            &[&first_schema],
        )
        .unwrap()
        .get(0);
    assert!(!exists);
    let value: i32 = reader
        .query_one("SELECT value FROM isolation_probe", &[])
        .unwrap()
        .get(0);
    assert_eq!(
        value, 2,
        "dropping another fixture must preserve this fixture's data"
    );
}
