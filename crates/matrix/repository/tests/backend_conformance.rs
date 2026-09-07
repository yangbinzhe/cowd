//! PostgreSQL-only persistence conformance for Matrix aggregates.

use matrix_core::{MatrixEntity, MatrixEntityInput, MatrixSourceKey};
use matrix_repository::{MatrixStore, PostgresMatrixRepository};
use storage::{PostgresConnectionConfig, PostgresExecutor, StaticSecretRefResolver};

fn isolated_executor() -> (PostgresExecutor, PostgresExecutor, String) {
    let url = std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
    let resolver = StaticSecretRefResolver::new([("matrix.conformance.pg".to_string(), url)]);
    let base = PostgresExecutor::connect(
        PostgresConnectionConfig::new(
            "matrix-backend-conformance",
            "matrix.conformance.pg",
            "cowd-matrix-backend-conformance",
        ),
        &resolver,
    )
    .expect("PostgreSQL executor");
    let schema = format!("cowdmatrix_{}", uuid::Uuid::new_v4().simple());
    base.checkout_critical()
        .expect("connection")
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .expect("create schema");
    let scoped = base.scoped_namespace(&schema).expect("scoped executor");
    (base, scoped, schema)
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn postgres_matrix_survives_adapter_reconstruction() {
    let (base, scoped, schema) = isolated_executor();
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
    base.checkout_critical()
        .expect("cleanup connection")
        .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .expect("drop schema");
}
