#![allow(clippy::expect_used)]

#[path = "../test-support/backend_contract.rs"]
mod backend_contract;

use backend_contract::BackendContractFixture;
use session::{SessionStoreBackend, SqliteSessionStore};

struct SqliteFixture {
    _directory: tempfile::TempDir,
    path: std::path::PathBuf,
    store: SqliteSessionStore,
}

impl SqliteFixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("create SQLite contract directory");
        let path = directory.path().join("session.db");
        let store = SqliteSessionStore::open(&path).expect("open SQLite contract store");
        Self {
            _directory: directory,
            path,
            store,
        }
    }
}

impl BackendContractFixture for SqliteFixture {
    fn backend(&self) -> &dyn SessionStoreBackend {
        &self.store
    }

    fn shared_backend(&self) -> std::sync::Arc<dyn SessionStoreBackend> {
        std::sync::Arc::new(self.store.clone())
    }

    fn reopen(&mut self) {
        self.store = SqliteSessionStore::open(&self.path).expect("reopen SQLite contract store");
    }
}

#[test]
fn sqlite_input_generation_and_claim_fence_contract() {
    backend_contract::input_generation_and_claim_fence(&mut SqliteFixture::new());
}

#[test]
fn sqlite_lifecycle_contract() {
    backend_contract::lifecycle_recovery_and_single_tombstone(&mut SqliteFixture::new());
}

#[test]
fn sqlite_branch_contract() {
    backend_contract::branch_activation_and_idempotent_cutoff(&mut SqliteFixture::new());
}

#[test]
fn sqlite_domain_event_idempotency_and_kind_query_contract() {
    backend_contract::domain_event_idempotency_and_kind_query(&mut SqliteFixture::new());
}

#[test]
fn sqlite_application_execution_32_way_semantic_idempotency_contract() {
    backend_contract::application_execution_32_way_semantic_idempotency(&mut SqliteFixture::new());
}
