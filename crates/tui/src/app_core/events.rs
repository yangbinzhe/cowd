#![allow(dead_code)]

use std::sync::mpsc;

use crate::protocol::CowdEvent;

/// Channel sender/receiver type aliases for the event pipeline.
pub type CowdEventSender = mpsc::SyncSender<CowdEvent>;
pub type CowdEventReceiver = mpsc::Receiver<CowdEvent>;

/// Create a bounded event channel.
/// Buffer size 2048 provides headroom for bursty streaming events
/// without dropping, while bounded to prevent runaway memory.
#[must_use]
pub fn cowd_event_channel() -> (CowdEventSender, CowdEventReceiver) {
    mpsc::sync_channel::<CowdEvent>(256)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_channel_send_recv() {
        let (tx, rx) = cowd_event_channel();
        tx.send(CowdEvent::TextDelta {
            text: "hello".into(),
        })
        .unwrap();
        let event = rx.recv().unwrap();
        assert!(matches!(event, CowdEvent::TextDelta { text } if text == "hello"));
    }

    #[test]
    fn event_clone_roundtrip() {
        let event = CowdEvent::ToolStart {
            id: "t1".into(),
            name: "bash".into(),
            preview: "ls".into(),
        };
        let cloned = event.clone();
        assert!(matches!(cloned, CowdEvent::ToolStart { id, name, preview }
            if id == "t1" && name == "bash" && preview == "ls"));
    }

    #[test]
    fn channel_backpressure_no_panic() {
        let (tx, _rx) = cowd_event_channel();
        for i in 0..256 {
            let _ = tx.try_send(CowdEvent::TextDelta {
                text: format!("msg{i}"),
            });
        }
        // After channel fills, try_send returns Err — should not panic
        let result = tx.try_send(CowdEvent::TextDelta {
            text: "overflow".into(),
        });
        assert!(result.is_err());
    }

    #[test]
    fn all_event_variants_debug() {
        // Ensure all variants produce valid Debug output
        let events = vec![
            CowdEvent::TextDelta { text: "t".into() },
            CowdEvent::ThinkingDelta {
                thinking: "t".into(),
            },
            CowdEvent::ThinkingComplete,
            CowdEvent::ToolStart {
                id: "i".into(),
                name: "n".into(),
                preview: "p".into(),
            },
            CowdEvent::ToolProgress {
                id: "i".into(),
                name: "n".into(),
                progress: "p".into(),
            },
            CowdEvent::ToolComplete {
                id: "i".into(),
                name: "n".into(),
                summary: "s".into(),
                exit_code: Some(0),
            },
            CowdEvent::TokenUsage {
                input: 1,
                output: 2,
                cache_create: 3,
                cache_read: 4,
            },
            CowdEvent::TurnStarted,
            CowdEvent::TurnComplete {
                assistant_text: "ok".into(),
                iterations: 1,
            },
            CowdEvent::ResourcesCommitted {
                ids: vec!["res-a".into()],
            },
            CowdEvent::ExecutionGraphSummary {
                summary: crate::RuntimeExecutionGraphSummary {
                    graph_id: Some("graph".into()),
                    board_id: Some("board".into()),
                    status: "completed".into(),
                    agent_tasks: 1,
                    memory_candidates: 1,
                    conflicts: 0,
                    completion_rate: Some(1.0),
                    synthesis_lift: Some(1.1),
                    complementarity_score: Some(0.5),
                },
            },
            CowdEvent::TurnError { error: "e".into() },
            CowdEvent::CompactionNotice { removed_count: 5 },
        ];
        for event in &events {
            let _ = format!("{event:?}");
        }
    }

    #[test]
    fn context_window_event_updates_app() {
        use crate::app::App;
        let mut app = App::new("test", "test-session");
        assert_eq!(app.context_window, 0);
        app.apply_event(CowdEvent::ContextWindow(200_000));
        assert_eq!(app.context_window, 200_000);
    }

    #[test]
    fn token_usage_event_updates_all_counters() {
        use crate::app::App;
        let mut app = App::new("test", "test-session");
        app.apply_event(CowdEvent::TurnStarted);
        app.apply_event(CowdEvent::TokenUsage {
            input: 100,
            output: 50,
            cache_create: 10,
            cache_read: 5,
        });
        assert_eq!(app.input_tokens, 100);
        assert_eq!(app.output_tokens, 50);
        assert_eq!(app.token_count, 165);
        assert_eq!(app.turn_input_tokens, 100);
    }
}
