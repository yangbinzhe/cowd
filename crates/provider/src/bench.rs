#[cfg(test)]
mod perf {
    use std::time::Instant;

    use crate::build_http_client;

    #[test]
    fn http_client_creation_is_fast() {
        let start = Instant::now();
        let client = build_http_client().expect("HTTP client");
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_millis() < 2000,
            "http client creation too slow: {:?}",
            elapsed
        );
        let _ = client;
    }

    #[test]
    fn json_parsing_performance() {
        use crate::types::{InputContentBlock, InputMessage};

        let mut last_parsed = None;
        let start = Instant::now();
        for _ in 0..1000 {
            let msg = InputMessage {
                role: "user".to_string(),
                content: vec![InputContentBlock::Text {
                    text: "benchmark test message content".to_string(),
                }],
            };
            let json = serde_json::to_string(&msg).unwrap();
            last_parsed = Some(serde_json::from_str::<InputMessage>(&json).unwrap());
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_millis() < 200,
            "json roundtrip too slow: {:?}",
            elapsed
        );

        // Check the wire contract independently of the serializer and timed loop.
        let parsed: InputMessage = serde_json::from_str(
            r#"{"role":"user","content":[{"type":"text","text":"benchmark test message content"}]}"#,
        )
        .unwrap();
        assert_eq!(last_parsed.as_ref(), Some(&parsed));
        assert_eq!(parsed.role, "user");
        assert_eq!(
            parsed.content,
            vec![InputContentBlock::Text {
                text: "benchmark test message content".to_string(),
            }]
        );
    }
}
