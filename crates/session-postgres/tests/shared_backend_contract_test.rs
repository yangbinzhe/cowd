#![allow(clippy::expect_used)]

#[path = "../../session/test-support/backend_contract.rs"]
mod backend_contract;

#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

use backend_contract::BackendContractFixture;
use session::SessionStoreBackend;
use session_postgres::PostgresSessionStore;

struct PostgresFixture {
    store: PostgresSessionStore,
    scope: PostgresTestScope,
}

impl PostgresFixture {
    fn new() -> Self {
        let scope = PostgresTestScope::new();
        let store = PostgresSessionStore::new(scope.reconnect()).expect("owned backend fixture");
        Self { store, scope }
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
        self.store =
            PostgresSessionStore::new(self.scope.reconnect()).expect("reopen same owned namespace");
    }
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_input_generation_and_claim_fence_contract() {
    backend_contract::input_generation_and_claim_fence(&mut PostgresFixture::new());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_input_application_receipt_contract() {
    backend_contract::input_application_receipt_is_atomic_and_recoverable(
        &mut PostgresFixture::new(),
    );
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_terminal_input_cursor_cas_contract() {
    backend_contract::terminal_input_cursor_cas(&mut PostgresFixture::new());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_lifecycle_contract() {
    backend_contract::lifecycle_recovery_and_single_tombstone(&mut PostgresFixture::new());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_branch_contract() {
    backend_contract::branch_activation_and_idempotent_cutoff(&mut PostgresFixture::new());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_domain_event_idempotency_and_kind_query_contract() {
    backend_contract::domain_event_idempotency_and_kind_query(&mut PostgresFixture::new());
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_application_execution_32_way_semantic_idempotency_contract() {
    backend_contract::application_execution_32_way_semantic_idempotency(&mut PostgresFixture::new());
}
