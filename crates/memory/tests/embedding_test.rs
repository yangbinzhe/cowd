//! Integration tests for embedding retry logic.
//!
//! T6: Verifies that embed_batch retries on transient HTTP failures
//! with exponential backoff.

use memory::config::VectorConfig;
use memory::EmbeddingClient;

#[tokio::test]
async fn embedding_retries_on_transient_http_failure() {
    // Connect to localhost:1 which will be refused — triggering retries.
    let config = VectorConfig {
        enabled: true,
        model: "test".into(),
        api_url: "http://localhost:1/v1/embeddings".into(),
        timeout_secs: 2,
        ..Default::default()
    };
    let client = EmbeddingClient::new(config);
    let start = std::time::Instant::now();
    let result = client.embed(&["hello"]).await;
    // Should fail after exhausting retries
    assert!(
        result.is_err(),
        "embedding should fail against non-existent server"
    );
    let elapsed_ms = start.elapsed().as_millis();
    // With 3 retries and backoff (500ms, 1000ms), total delay is ~1500ms
    assert!(
        elapsed_ms > 1000,
        "should have retried with backoff delay (elapsed={}ms)",
        elapsed_ms
    );
}
