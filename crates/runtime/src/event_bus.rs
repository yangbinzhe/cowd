use std::sync::mpsc;

use crate::error::CowdError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeEvent {
    TextDelta { text: String, turn_id: String },
    ThinkingDelta { text: String },
    ThinkingComplete,
    ToolStart { id: String, name: String, preview: String },
    ToolProgress { id: String, name: String, progress: String },
    ToolComplete { id: String, name: String, summary: String, exit_code: Option<i32> },
    TurnComplete { turn_id: String, summary: String },
    TurnError { turn_id: String, error: String },
    TokenUsage { input_tokens: u64, output_tokens: u64 },
    Shutdown,
}

pub struct EventBus {
    tx: mpsc::SyncSender<RuntimeEvent>,
}

impl EventBus {
    pub fn new(capacity: usize) -> (Self, EventReceiver) {
        let (tx, rx) = mpsc::sync_channel(capacity);
        (Self { tx }, EventReceiver { rx })
    }

    pub fn publish(&self, event: RuntimeEvent) -> Result<(), CowdError> {
        self.tx.try_send(event).map_err(|e| {
            CowdError::other(format!("event bus closed or full: {e}"))
        })
    }
}

pub struct EventReceiver {
    rx: mpsc::Receiver<RuntimeEvent>,
}

impl EventReceiver {
    pub fn recv(&self) -> Result<RuntimeEvent, CowdError> {
        self.rx.recv().map_err(|e| {
            CowdError::other(format!("event bus receive error: {e}"))
        })
    }

    pub fn try_recv_all(&self) -> Vec<RuntimeEvent> {
        let mut events = Vec::new();
        while let Ok(event) = self.rx.try_recv() {
            events.push(event);
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_publish_subscribe() {
        let (bus, rx) = EventBus::new(256);
        bus.publish(RuntimeEvent::TextDelta {
            text: "hello".into(),
            turn_id: "t1".into(),
        })
        .unwrap();
        let event = rx.recv().unwrap();
        assert_eq!(
            event,
            RuntimeEvent::TextDelta {
                text: "hello".into(),
                turn_id: "t1".into(),
            }
        );
    }

    #[test]
    fn event_backpressure() {
        let (bus, rx) = EventBus::new(2);
        bus.publish(RuntimeEvent::Shutdown).unwrap();
        bus.publish(RuntimeEvent::Shutdown).unwrap();
        assert!(bus.publish(RuntimeEvent::Shutdown).is_err());
        let _ = rx.try_recv_all();
    }

    #[test]
    fn event_try_recv_all_batches() {
        let (bus, rx) = EventBus::new(256);
        bus.publish(RuntimeEvent::ThinkingDelta {
            text: "a".into(),
        })
        .unwrap();
        bus.publish(RuntimeEvent::ThinkingComplete).unwrap();
        bus.publish(RuntimeEvent::Shutdown).unwrap();
        let events = rx.try_recv_all();
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn event_tool_lifecycle() {
        let (bus, rx) = EventBus::new(256);
        bus.publish(RuntimeEvent::ToolStart {
            id: "t1".into(),
            name: "bash".into(),
            preview: "ls".into(),
        })
        .unwrap();
        bus.publish(RuntimeEvent::ToolProgress {
            id: "t1".into(),
            name: "bash".into(),
            progress: "running...".into(),
        })
        .unwrap();
        bus.publish(RuntimeEvent::ToolComplete {
            id: "t1".into(),
            name: "bash".into(),
            summary: "ok".into(),
            exit_code: Some(0),
        })
        .unwrap();
        let events = rx.try_recv_all();
        assert_eq!(events.len(), 3);
        assert!(matches!(events[0], RuntimeEvent::ToolStart { .. }));
        assert!(matches!(events[2], RuntimeEvent::ToolComplete { .. }));
    }

    #[test]
    fn event_drop_receiver_stops_publish() {
        let (bus, rx) = EventBus::new(256);
        drop(rx);
        assert!(bus.publish(RuntimeEvent::Shutdown).is_err());
    }
}
