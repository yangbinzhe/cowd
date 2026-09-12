//! PostgreSQL-only persistence conformance for Matrix aggregates.

use matrix_core::{MatrixEntity, MatrixEntityInput, MatrixSourceKey};
use matrix_repository::{MatrixStore, PostgresMatrixRepository};
#[path = "../../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_matrix_survives_adapter_reconstruction() {
    let fixture = PostgresTestScope::new();
    let scoped = fixture.reconnect();
    let first = PostgresMatrixRepository::new(scoped.clone()).expect("Matrix PostgreSQL store");
    let entity = MatrixEntity::from_input(MatrixEntityInput {
        entity_id: Some("matrix-pg-conformance".to_string()),
        entity_type: "framework".to_string(),
        canonical_key: "cowd-pg-conformance".to_string(),
        display_name: Some("COWD PG conformance".to_string()),
        source_keys: vec![MatrixSourceKey {
            source_system: "conformance".to_string(),
            source_key: "matrix-v1".to_string(),
            source_ref: Some("conformance://matrix/v1".to_string()),
        }],
        attributes: serde_json::json!({"schema": 1}),
        confidence: Some(1.0),
    });
    first.upsert_entity(&entity).expect("insert Matrix entity");
    drop(first);
    let reopened = PostgresMatrixRepository::new(scoped).expect("reopen Matrix PostgreSQL store");
    assert_eq!(
        reopened
            .get_entity("matrix-pg-conformance")
            .expect("read entity")
            .unwrap()
            .canonical_key,
        "cowd-pg-conformance"
    );
    drop(reopened);
}
