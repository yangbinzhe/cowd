// M9: EventBus — publish/subscribe decoupling for module communication.
// Derived from opencode's bus/ module pattern.

use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub enum Event {
    SessionCreated { id: String },
    SessionDeleted { id: String },
    TextDelta { content: String },

    TurnCompleted { tokens: u32, model: String },
    ToolExecuted { name: String, duration_ms: u64 },
    MemoryExtracted { count: usize },
    ApprovalRequested { tool: String },
    ThinkingDelta { content: String },
    ToolStart { id: String, name: String, preview: String },
    ToolProgress { id: String, name: String, progress: String },
    ToolComplete { id: String, name: String, summary: String, exit_code: Option<i32> },
    SignatureDelta { signature: String },
}

#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<Event>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.tx.subscribe()
    }

    pub fn emit(&self, event: Event) {
        let _ = self.tx.send(event);
    }

    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn m9_subscribe_and_receive_event() {
        let bus = EventBus::new(10);
        let mut rx = bus.subscribe();
        bus.emit(Event::TurnCompleted { tokens: 100, model: "test".into() });
        let event = rx.try_recv().expect("should receive event");
        match event {
            Event::TurnCompleted { tokens, .. } => assert_eq!(tokens, 100),
            _ => panic!("wrong event type"),
        }
    }

    #[test]
    fn m9_multiple_subscribers_receive_event() {
        let bus = EventBus::new(10);
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();
        bus.emit(Event::SessionCreated { id: "s1".into() });
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }
}
