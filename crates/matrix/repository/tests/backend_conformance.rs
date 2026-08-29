//! Real dual-backend digest conformance for Matrix aggregates and revisions.

use matrix_core::{MatrixEntity, MatrixEntityInput, MatrixSourceKey};
use matrix_repository::{
    copy_quiesced_matrix_store, MatrixSqliteRepository, PostgresMatrixRepository,
};
use storage::{PostgresConnectionConfig, StaticSecretRefResolver};

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn sqlite_and_postgres_matrix_snapshots_have_equal_digests() {
    let sqlite = MatrixSqliteRepository::in_memory().expect("SQLite Matrix store");
    sqlite
        .upsert_entity(&MatrixEntity::from_input(MatrixEntityInput {
            entity_id: Some("matrix-backend-conformance".to_string()),
            entity_type: "framework".to_string(),
            canonical_key: "cowd-backend-conformance".to_string(),
            display_name: Some("COWD backend conformance".to_string()),
            source_keys: vec![MatrixSourceKey {
                source_system: "conformance".to_string(),
                source_key: "matrix-v1".to_string(),
                source_ref: Some("conformance://matrix/v1".to_string()),
            }],
            attributes: serde_json::json!({"schema": 1}),
            confidence: Some(1.0),
        }))
        .expect("seed SQLite Matrix entity");

    let url = std::env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
    let resolver = StaticSecretRefResolver::new([("matrix.conformance.pg".to_string(), url)]);
    let postgres = PostgresMatrixRepository::connect(
        PostgresConnectionConfig::new(
            "matrix-backend-conformance",
            "matrix.conformance.pg",
            "cowd-matrix-backend-conformance",
        ),
        &resolver,
    )
    .expect("PostgreSQL Matrix store");
    let root = tempfile::tempdir().expect("manifest directory");
    let manifest =
        copy_quiesced_matrix_store(&sqlite, &postgres, root.path().join("matrix-cutover.json"))
            .expect("quiesced Matrix copy remains digest exact");

    assert_eq!(manifest.source_digest, manifest.target_digest);
    assert_eq!(manifest.record_count, 1);
}
