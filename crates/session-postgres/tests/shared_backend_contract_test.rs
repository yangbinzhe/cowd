#![allow(clippy::expect_used)]

#[path = "../../session/test-support/backend_contract.rs"]
mod backend_contract;

use std::sync::{Mutex, MutexGuard, OnceLock};

use backend_contract::BackendContractFixture;
use session::SessionStoreBackend;
use session_postgres::PostgresSessionStore;
use storage::{PostgresConnectionConfig, StaticSecretRefResolver};

fn postgres_test_guard() -> MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct PostgresFixture {
    url: String,
    store: PostgresSessionStore,
}

impl PostgresFixture {
    fn connect(url: &str) -> PostgresSessionStore {
        let resolver = StaticSecretRefResolver::new([("contract.pg".to_string(), url.to_string())]);
        PostgresSessionStore::connect(
            PostgresConnectionConfig::new(
                "session-postgres-backend-contract",
                "contract.pg",
                "cowd-session-postgres-backend-contract",
            ),
            &resolver,
        )
        .expect("connect isolated PostgreSQL contract store")
    }

    fn new() -> Self {
        let url = std::env::var("COWD_TEST_POSTGRES_URL")
            .expect("COWD_TEST_POSTGRES_URL must identify an isolated PostgreSQL database");
        let store = Self::connect(&url);
        let mut connection = store
            .executor()
            .checkout_runtime()
            .expect("checkout PostgreSQL contract connection");
        connection
            .batch_execute(
                "TRUNCATE TABLE
                    session_branch_activations,
                    session_lifecycle_intents,
                    session_runtime_outbox_history,
                    session_mission_outbox_history,
                    session_runtime_outbox,
                    session_mission_outbox,
                    session_event_checkpoints,
                    session_snapshots,
                    session_events,
                    session_messages,
                    session_memory_associations,
                    session_recovery_manifest,
                    session_records
                 CASCADE",
            )
            .expect("clear isolated PostgreSQL contract store");
        drop(connection);
        Self { url, store }
    }
}

impl BackendContractFixture for PostgresFixture {
    fn backend(&self) -> &dyn SessionStoreBackend {
        &self.store
    }

    fn shared_backend(&self) -> std::sync::Arc<dyn SessionStoreBackend> {
        std::sync::Arc::new(self.store.clone())
    }

    fn reopen(&mut self) {
        self.store = Self::connect(&self.url);
    }
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_input_generation_and_claim_fence_contract() {
    let _guard = postgres_test_guard();
    backend_contract::input_generation_and_claim_fence(&mut PostgresFixture::new());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_terminal_input_cursor_cas_contract() {
    let _guard = postgres_test_guard();
    backend_contract::terminal_input_cursor_cas(&mut PostgresFixture::new());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_lifecycle_contract() {
    let _guard = postgres_test_guard();
    backend_contract::lifecycle_recovery_and_single_tombstone(&mut PostgresFixture::new());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_branch_contract() {
    let _guard = postgres_test_guard();
    backend_contract::branch_activation_and_idempotent_cutoff(&mut PostgresFixture::new());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_domain_event_idempotency_and_kind_query_contract() {
    let _guard = postgres_test_guard();
    backend_contract::domain_event_idempotency_and_kind_query(&mut PostgresFixture::new());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_application_execution_32_way_semantic_idempotency_contract() {
    let _guard = postgres_test_guard();
    backend_contract::application_execution_32_way_semantic_idempotency(&mut PostgresFixture::new());
}
