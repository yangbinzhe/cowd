//! Compression stages for the memory system.
//!
//! Callers use the stages through their existing compaction owners:
//! - **Stage 1** (`micro`): lossless folding of duplicate durable evidence receipts.
//! - **Stage 2** (`session`): token-threshold triggered medium compression
//!   (structured summary + L2/L3 memory extraction).
//! - **Stage 3** (`deep`): extreme-pressure heavy compression (iterative
//!   summary update + all-content distillation).
//!
//! Supporting modules handle token-budget tracking (`budget`), circuit-breaker
//! / recursion guards (`guard`), and real-time context monitoring (`monitor`).

pub mod budget;
pub mod deep;
pub mod guard;
pub mod llm_summarizer;
pub mod micro;
pub mod monitor;
pub mod quality;
pub mod session;
pub mod token_estimation;

use crate::error::MemoryError;

/// Result alias for compression operations.
pub type Result<T> = std::result::Result<T, MemoryError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Message, MessageRole};

    fn msg(role: MessageRole, content: &str) -> Message {
        Message {
            turn_index: 0,
            role,
            content: content.to_string(),
            tool_use_id: None,
            tool_name: None,
            pinned: false,
        }
    }

    fn tool_msg(tool_name: &str, content: &str) -> Message {
        Message {
            turn_index: 0,
            role: MessageRole::Tool,
            content: content.to_string(),
            tool_use_id: Some("call_1".to_string()),
            tool_name: Some(tool_name.to_string()),
            pinned: false,
        }
    }

    #[test]
    fn micro_preserves_initial_exchange_across_later_compaction() {
        let compactor = micro::MicroCompactor::new();
        let mut msgs = vec![
            msg(MessageRole::User, "Initial: set up the project"),
            msg(MessageRole::Assistant, "Created project with Cargo.toml"),
        ];

        compactor.compact(&mut msgs);

        // Add more conversation
        msgs.push(msg(MessageRole::User, "Add logging support"));
        msgs.push(msg(
            MessageRole::Assistant,
            "Added log4rs with rolling file appender",
        ));

        compactor.compact(&mut msgs);

        let all_content: String = msgs.iter().map(|m| m.content.as_str()).collect();
        assert!(
            all_content.contains("Cargo.toml"),
            "micro-compaction must preserve the first exchange without a durable raw reference"
        );
    }

    #[test]
    fn micro_preserves_authentication_details_across_later_compaction() {
        let compactor = micro::MicroCompactor::new();
        let mut msgs = vec![
            msg(MessageRole::User, "Implement auth module"),
            msg(
                MessageRole::Assistant,
                "Added JWT-based authentication with refresh tokens",
            ),
        ];

        compactor.compact(&mut msgs);

        // Second exchange
        msgs.push(msg(MessageRole::User, "Add rate limiting"));
        msgs.push(msg(
            MessageRole::Assistant,
            "Added token bucket rate limiter",
        ));

        compactor.compact(&mut msgs);

        let all_content: String = msgs.iter().map(|m| m.content.as_str()).collect();
        assert!(
            all_content.contains("JWT"),
            "micro-compaction must preserve authentication details across later exchanges"
        );
    }

    // ===================================================================
    // V2: tool output remains lossless until canonical raw durability exists.
    // ===================================================================

    #[test]
    fn test_terminal_output_without_durable_ref_is_preserved() {
        use crate::compression::micro::MicroCompactor;
        let compactor = MicroCompactor::new();

        let terminal_output = concat!(
            r#"{"exit_code": 1, "output": "test failed at line 42"}"#,
            "\n",
            "error: assertion failed\n",
            "     at src/main.rs:42\n",
        );
        let mut msgs = vec![
            msg(MessageRole::Assistant, "Running tests..."),
            tool_msg("terminal", terminal_output),
            msg(MessageRole::Assistant, "Tests failed, fixing"),
        ];

        compactor.compact(&mut msgs);

        let tool_content = &msgs[1].content;
        assert_eq!(tool_content, terminal_output);
    }

    #[test]
    fn test_read_file_output_summarized_with_path_context() {
        use crate::compression::micro::MicroCompactor;
        let compactor = MicroCompactor::new();

        let read_content = "fn main() {\n    let x = 42;\n    println!(\"{}\", x);\n}\n";
        let mut msgs = vec![
            msg(MessageRole::Assistant, "Reading src/main.rs..."),
            tool_msg("read_file", read_content),
            msg(MessageRole::Assistant, "Found the main function"),
        ];

        compactor.compact(&mut msgs);

        let tool_content = &msgs[1].content;
        assert!(
            tool_content.len() <= read_content.len(),
            "Read file output should at minimum not grow"
        );
    }

    #[test]
    fn test_unknown_tool_without_durable_ref_is_preserved() {
        use crate::compression::micro::MicroCompactor;
        let compactor = MicroCompactor::new();

        let long_content = "result data: ".to_string() + &"x".repeat(3000);
        let mut msgs = vec![
            msg(MessageRole::Assistant, "Running custom transform..."),
            tool_msg("custom_tool", &long_content),
            msg(MessageRole::Assistant, "Done"),
        ];

        compactor.compact(&mut msgs);

        let tool_content = &msgs[1].content;
        assert_eq!(tool_content, &long_content);
    }
}
