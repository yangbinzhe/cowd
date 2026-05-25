// ── TUI Event System ────────────────────────────────────────────
// All events flowing from the background turn runner to the TUI render loop.
// One-to-one mapping with server.rs SSE events. Phase 0 infrastructure.
#![allow(dead_code)]

use std::sync::mpsc;

/// Events emitted by StreamingTurnRunner and consumed by the TUI event loop.
#[derive(Debug, Clone)]
pub enum TuiEvent {
    // ── Streaming content ──
    /// Incremental text delta from the model's response
    TextDelta { text: String },
    /// Incremental thinking/reasoning delta (extended thinking models)
    ThinkingDelta { thinking: String },
    /// Thinking block complete — close the thinking panel
    ThinkingComplete,

    // ── Tool lifecycle ──
    /// A tool call has been initiated
    ToolStart { id: String, name: String, preview: String },
    /// Progress update for a running tool
    ToolProgress { id: String, name: String, progress: String },
    /// Tool execution completed
    ToolComplete { id: String, name: String, summary: String, exit_code: Option<i32> },

    // ── Statistics ──
    /// Cumulative token usage
    TokenUsage { input: u64, output: u64, cache_create: u64, cache_read: u64 },

    // ── Turn lifecycle ──
    /// A new turn has started (user input being processed)
    TurnStarted,
    /// Turn completed successfully with summary data
    TurnComplete { assistant_text: String, iterations: u32 },
    /// Turn failed with an error
    TurnError { error: String },

    // ── System ──
    /// Auto-compaction notification
    CompactionNotice { removed_count: usize },
}

/// Channel sender/receiver type aliases for the TUI event pipeline.
pub type TuiEventSender = mpsc::SyncSender<TuiEvent>;
pub type TuiEventReceiver = mpsc::Receiver<TuiEvent>;

/// Create a bounded TUI event channel.
/// Buffer size 2048 provides headroom for bursty streaming events
/// without dropping, while bounded to prevent runaway memory.
#[must_use]
pub fn tui_event_channel() -> (TuiEventSender, TuiEventReceiver) {
    mpsc::sync_channel::<TuiEvent>(2048)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_channel_send_recv() {
        let (tx, rx) = tui_event_channel();
        tx.send(TuiEvent::TextDelta { text: "hello".into() }).unwrap();
        let event = rx.recv().unwrap();
        assert!(matches!(event, TuiEvent::TextDelta { text } if text == "hello"));
    }

    #[test]
    fn event_clone_roundtrip() {
        let event = TuiEvent::ToolStart {
            id: "t1".into(),
            name: "bash".into(),
            preview: "ls".into(),
        };
        let cloned = event.clone();
        assert!(matches!(cloned, TuiEvent::ToolStart { id, name, preview }
            if id == "t1" && name == "bash" && preview == "ls"));
    }

    #[test]
    fn channel_backpressure_no_panic() {
        let (tx, _rx) = tui_event_channel();
        for i in 0..2048 {
            let _ = tx.try_send(TuiEvent::TextDelta { text: format!("msg{i}") });
        }
        // After channel fills, try_send returns Err — should not panic
        let result = tx.try_send(TuiEvent::TextDelta { text: "overflow".into() });
        assert!(result.is_err());
    }

    #[test]
    fn all_event_variants_debug() {
        // Ensure all variants produce valid Debug output
        let events = vec![
            TuiEvent::TextDelta { text: "t".into() },
            TuiEvent::ThinkingDelta { thinking: "t".into() },
            TuiEvent::ThinkingComplete,
            TuiEvent::ToolStart { id: "i".into(), name: "n".into(), preview: "p".into() },
            TuiEvent::ToolProgress { id: "i".into(), name: "n".into(), progress: "p".into() },
            TuiEvent::ToolComplete { id: "i".into(), name: "n".into(), summary: "s".into(), exit_code: Some(0) },
            TuiEvent::TokenUsage { input: 1, output: 2, cache_create: 3, cache_read: 4 },
            TuiEvent::TurnStarted,
            TuiEvent::TurnComplete { assistant_text: "ok".into(), iterations: 1 },
            TuiEvent::TurnError { error: "e".into() },
            TuiEvent::CompactionNotice { removed_count: 5 },
        ];
        for event in &events {
            let _ = format!("{event:?}");
        }
    }

    #[test]
    fn context_window_event_updates_app() {
        use crate::tui::app::App;
        let mut app = App::new("test", "test-session");
        assert_eq!(app.context_window, 0);
        app.apply_event(TuiEvent::ContextWindow(200_000));
        assert_eq!(app.context_window, 200_000);
    }

    #[test]
    fn token_usage_event_updates_all_counters() {
        use crate::tui::app::App;
        let mut app = App::new("test", "test-session");
        app.apply_event(TuiEvent::TurnStarted);
        app.apply_event(TuiEvent::TokenUsage {
            input: 100, output: 50, cache_create: 10, cache_read: 5,
        });
        assert_eq!(app.input_tokens, 100);
        assert_eq!(app.output_tokens, 50);
        assert_eq!(app.token_count, 165);
        assert_eq!(app.turn_input_tokens, 100);
    }
}
