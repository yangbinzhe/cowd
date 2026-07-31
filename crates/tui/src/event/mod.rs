// ── Event System v2 — TUI-internal routing ──────────────────────
// Core types: EventPriority, ComponentId, RoutedEvent.
// Re-exports EventBus from dispatcher module.
// Separate from tui::events (CowdEvent background→TUI protocol).
#![allow(dead_code)]

use std::cmp::Ordering;

pub mod dispatcher;
pub use dispatcher::EventBus;

/// Priority level for TUI-internal events.
///
/// Ordering: High > Normal > Low.
/// Used by `EventBus::drain()` via `BinaryHeap` to return highest-priority events first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventPriority {
    High,
    Normal,
    Low,
}

impl Ord for EventPriority {
    fn cmp(&self, other: &Self) -> Ordering {
        self.discriminant().cmp(&other.discriminant())
    }
}

impl PartialOrd for EventPriority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl EventPriority {
    fn discriminant(&self) -> u8 {
        match self {
            EventPriority::High => 2,
            EventPriority::Normal => 1,
            EventPriority::Low => 0,
        }
    }
}

/// Component identifier — a string newtype for routing events to specific TUI components.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComponentId(pub String);

impl ComponentId {
    /// Broadcast target: all components receive this event.
    pub fn broadcast() -> Self {
        ComponentId("__broadcast__".to_string())
    }
}

/// A TUI-internal event with routing metadata and priority.
///
/// Wraps a raw `crossterm::event::Event` together with a target `ComponentId`
/// and an `EventPriority`. The `Ord` implementation ensures that within a
/// `BinaryHeap`, higher-priority events are popped first, with FIFO
/// tie-breaking for equal priorities.
///
/// Manual `PartialEq` impl — compares `target`, `event`, and `priority`
/// (omits the internal `seq` counter).
#[derive(Debug, Clone, Eq)]
pub struct RoutedEvent {
    /// Target component identifier.
    pub target: ComponentId,
    /// The raw crossterm event.
    pub event: crossterm::event::Event,
    /// Priority for ordering (High first).
    pub priority: EventPriority,
    /// Sequential counter for FIFO ordering within the same priority.
    seq: u64,
}

/// Priority ordering: High > Normal > Low.
/// Tie-break: earlier sequence number (FIFO) compares as *greater* so the max-heap
/// pops it first.
impl Ord for RoutedEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for RoutedEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RoutedEvent {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target && self.event == other.event && self.priority == other.priority
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CowdEvent;
    use crossterm::event::Event;

    // ── priority_ordering ─────────────────────────────────────
    // Ensures EventBus::drain() returns events in High > Normal > Low order
    // regardless of insertion order.
    #[test]
    fn priority_ordering() {
        let bus = EventBus::new();

        bus.send(Event::Resize(100, 100), EventPriority::Low);
        bus.send(Event::Resize(100, 100), EventPriority::High);
        bus.send(Event::Resize(100, 100), EventPriority::Normal);

        let drained = bus.drain();
        assert_eq!(drained.len(), 3);
        assert_eq!(drained[0].priority, EventPriority::High);
        assert_eq!(drained[1].priority, EventPriority::Normal);
        assert_eq!(drained[2].priority, EventPriority::Low);
    }

    // ── targeted_routing ──────────────────────────────────────
    // Verifies send_targeted() correctly assigns the ComponentId
    // and that the event is preserved through drain().
    #[test]
    fn targeted_routing() {
        let bus = EventBus::new();
        let target = ComponentId("command_palette".to_string());
        let event = Event::Resize(80, 24);

        bus.send_targeted(target.clone(), event.clone(), EventPriority::High);

        let drained = bus.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].target, target);
        assert_eq!(drained[0].event, event);
        assert_eq!(drained[0].priority, EventPriority::High);
    }

    // ── existing_events_tests_still_pass ──────────────────────
    // Verifies the CowdEvent channel (cowd_event_channel /
    // CowdEventSender / CowdEventReceiver) still works after the EventBus
    // module is introduced.
    #[test]
    fn existing_events_tests_still_pass() {
        let (tx, mut rx) = crate::events::cowd_event_channel();
        tx.send(CowdEvent::ReasoningSummaryDelta {
            summary: "channel-event".into(),
        })
        .unwrap();
        let event = rx.try_recv().unwrap();
        assert!(
            matches!(event, CowdEvent::ReasoningSummaryDelta { summary } if summary == "channel-event")
        );
    }

    // ── drain_clears_queue ────────────────────────────────────
    // After drain() the queue should be empty.
    #[test]
    fn drain_clears_queue() {
        let bus = EventBus::new();

        bus.send(Event::Resize(100, 100), EventPriority::Normal);

        let first = bus.drain();
        assert_eq!(first.len(), 1);

        let second = bus.drain();
        assert!(second.is_empty());
    }

    // ── fifo_tiebreak_same_priority ───────────────────────────
    // Events with the same priority should drain in FIFO order.
    #[test]
    fn fifo_tiebreak_same_priority() {
        let bus = EventBus::new();

        bus.send(Event::Resize(1, 1), EventPriority::Normal);
        bus.send(Event::Resize(2, 2), EventPriority::Normal);
        bus.send(Event::Resize(3, 3), EventPriority::Normal);

        let drained = bus.drain();
        assert_eq!(drained.len(), 3);
        // FIFO: insertion order preserved
        assert_eq!(drained[0].event, Event::Resize(1, 1));
        assert_eq!(drained[1].event, Event::Resize(2, 2));
        assert_eq!(drained[2].event, Event::Resize(3, 3));
    }
}
