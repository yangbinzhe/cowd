//! Integration tests for [`cowd_memory::project_scope`].
//!
//! Tests project registration, idempotent re-registration, active-project
//! switching, and the always-available global store.

use cowd_memory::{ProjectScopeManager, store::MemoryStore};

/// Helper: create a temporary directory and a `ProjectScopeManager` using
/// `memory.db` inside it.
fn setup() -> (tempfile::TempDir, ProjectScopeManager) {
    let tmp = tempfile::TempDir::new().unwrap();
    let db_path = tmp.path().join("memory.db");
    let manager = ProjectScopeManager::new(db_path).unwrap();
    (tmp, manager)
}

/// Helper: create a dummy project directory inside `tmp` for registration.
fn dummy_project_dir(tmp: &tempfile::TempDir, name: &str) -> std::path::PathBuf {
    let dir = tmp.path().join(name);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// =========================================================================
// Test 1: register + switch + verify current project
// =========================================================================

#[test]
fn test_register_project_returns_id() {
    let (tmp, manager) = setup();
    let proj_dir = dummy_project_dir(&tmp, "my_project");
    let project_id = manager
        .register_project(&proj_dir)
        .expect("should register project");
    assert!(!project_id.is_empty(), "project ID must not be empty");

    let _store = manager
        .switch_project(&project_id)
        .expect("should switch to registered project");

    let current = manager
        .current_project()
        .expect("should have an active project");
    assert_eq!(current.project_id, project_id);
    assert_eq!(current.path, proj_dir.canonicalize().unwrap());
}

// =========================================================================
// Test 2: registering the same path twice returns the same project ID
// =========================================================================

#[test]
fn test_register_same_path_idempotent() {
    let (tmp, manager) = setup();
    let proj_dir = dummy_project_dir(&tmp, "idem_project");

    let id1 = manager.register_project(&proj_dir).unwrap();
    let id2 = manager.register_project(&proj_dir).unwrap();
    assert_eq!(id1, id2, "same path must yield same project ID");
}

// =========================================================================
// Test 3: switching projects changes the active project
// =========================================================================

#[test]
fn test_switch_project_changes_active() {
    let (tmp, manager) = setup();

    let proj_a = dummy_project_dir(&tmp, "project_a");
    let proj_b = dummy_project_dir(&tmp, "project_b");

    let id_a = manager.register_project(&proj_a).unwrap();
    let id_b = manager.register_project(&proj_b).unwrap();

    manager.switch_project(&id_a).unwrap();
    assert_eq!(manager.current_project().unwrap().project_id, id_a);

    manager.switch_project(&id_b).unwrap();
    assert_eq!(manager.current_project().unwrap().project_id, id_b);
}

// =========================================================================
// Test 4: global store is always accessible
// =========================================================================

#[tokio::test]
async fn test_global_store_always_available() {
    let (_tmp, manager) = setup();

    // Global store works from the start.
    let global = manager.global_store();
    let result = global.list_all().await;
    assert!(result.is_ok(), "global store should work: {result:?}");

    // Register & switch to another project; global store still works.
    let proj_dir = dummy_project_dir(&_tmp, "some_project");
    let pid = manager.register_project(&proj_dir).unwrap();
    manager.switch_project(&pid).unwrap();

    let global2 = manager.global_store();
    let result2 = global2.list_all().await;
    assert!(
        result2.is_ok(),
        "global store should still work after project switch: {result2:?}"
    );
}
