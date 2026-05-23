// ── EventBus — TUI-internal priority event dispatcher ─────────
// Types only: EventBus, its helper types, and the internal channel.
// No routing logic — that lives in Task 12's Dispatcher.
#![allow(dead_code)]

use super::{ComponentId, EventPriority, RoutedEvent};
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::mpsc;
use std::sync::Mutex;

/// TUI-internal event bus with priority ordering.
///
/// Components fire events into the bus via `send()` / `send_targeted()`.
/// The event loop (or dispatcher) calls `drain()` to retrieve all
/// pending events sorted by priority (High first) with FIFO tie-breaking.
///
/// ## Thread safety
/// - `send()` / `send_targeted()` are lock-free (unbounded mpsc sender).
/// - `drain()` acquires a short-lived mutex on the receiver side.
/// - `EventBus` is `Send + Sync`.
///
/// ## Design notes
/// - Uses `std::sync::mpsc::channel()` (unbounded) for fire-and-forget sends.
/// - Priority ordering is applied at drain time via `BinaryHeap`.
/// - Not related to `tui::events::TuiEvent` — this is an orthogonal channel
///   for TUI-internal component-to-dispatcher messages.
pub struct EventBus {
    /// Clonable sender for fire-and-forget event submission.
    sender: mpsc::Sender<RoutedEvent>,
    /// Receiver wrapped in Mutex for interior-mutability during drain.
    receiver: Mutex<mpsc::Receiver<RoutedEvent>>,
    /// Monotonically increasing sequence counter for FIFO ordering.
    next_seq: AtomicU64,
}

impl EventBus {
    /// Create a new empty event bus.
    #[must_use]
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        EventBus {
            sender: tx,
            receiver: Mutex::new(rx),
            next_seq: AtomicU64::new(0),
        }
    }

    /// Return a clonable sender handle for out-of-band event injection.
    #[must_use]
    pub fn sender(&self) -> mpsc::Sender<RoutedEvent> {
        self.sender.clone()
    }

    /// Fire-and-forget broadcast event to all components.
    ///
    /// The event target is set to `ComponentId::broadcast()`. The dispatcher
    /// (see Task 12) will forward it to every registered component.
    ///
    /// This call never blocks (unbounded mpsc channel).
    pub fn send(&self, event: crossterm::event::Event, priority: EventPriority) {
        self.send_targeted(ComponentId::broadcast(), event, priority);
    }

    /// Fire-and-forget event addressed to a specific component.
    ///
    /// This call never blocks (unbounded mpsc channel).
    pub fn send_targeted(
        &self,
        target: ComponentId,
        event: crossterm::event::Event,
        priority: EventPriority,
    ) {
        let seq = self.next_seq.fetch_add(1, AtomicOrdering::Relaxed);
        let routed = RoutedEvent {
            target,
            event,
            priority,
            seq,
        };
        // Ignore send error: if the receiver is dropped there's nothing to do.
        let _ = self.sender.send(routed);
    }

    /// Drain all pending events in priority order (High first).
    ///
    /// Collects every event currently buffered in the mpsc channel,
    /// orders them by `(priority descending, seq ascending)`, and
    /// returns them as a `Vec`. The internal queue is cleared.
    ///
    /// Returns an empty vec if no events are pending.
    #[must_use]
    pub fn drain(&self) -> Vec<RoutedEvent> {
        let mut heap = BinaryHeap::new();
        // Lock receiver and drain all buffered messages.
        let receiver = self.receiver.lock().expect("EventBus receiver lock poisoned");
        while let Ok(event) = receiver.try_recv() {
            heap.push(event);
        }
        // BinaryHeap is a max-heap, so pop() returns the greatest element first.
        let mut result = Vec::with_capacity(heap.len());
        while let Some(event) = heap.pop() {
            result.push(event);
        }
        result
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}
