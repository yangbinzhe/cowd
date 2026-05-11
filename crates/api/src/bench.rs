#[cfg(test)]
mod perf {
    use std::time::Instant;

    use crate::build_http_client_or_default;

    #[test]
    fn http_client_creation_is_fast() {
        let start = Instant::now();
        let client = build_http_client_or_default();
        let elapsed = start.elapsed();
        assert!(elapsed.as_millis() < 2000, "http client creation too slow: {:?}", elapsed);
        let _ = client;
    }

    #[test]
    fn json_parsing_performance() {
        use crate::types::{InputContentBlock, InputMessage};

        let start = Instant::now();
        for _ in 0..1000 {
            let msg = InputMessage {
                role: "user".to_string(),
                content: vec![InputContentBlock::Text { text: "benchmark test message content".to_string() }],
            };
            let json = serde_json::to_string(&msg).unwrap();
            let _parsed: InputMessage = serde_json::from_str(&json).unwrap();
        }
        let elapsed = start.elapsed();
        assert!(elapsed.as_millis() < 200, "json roundtrip too slow: {:?}", elapsed);
    }
}
