// ── EventBus + EventDispatcher — TUI-internal event system ──
// EventBus: priority-ordered channel for component-to-dispatcher events.
// EventDispatcher: drains the EventBus and routes events to components.
#![allow(dead_code)]

use super::{ComponentId, EventPriority, RoutedEvent};
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
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
/// - Not related to `tui::events::CowdEvent` — this is an orthogonal channel
///   for TUI-internal component-to-dispatcher messages.
pub struct EventBus {
    /// Clonable sender for fire-and-forget event submission.
    sender: mpsc::Sender<RoutedEvent>,
    /// Receiver wrapped in Mutex for interior-mutability during drain.
    receiver: Mutex<mpsc::Receiver<RoutedEvent>>,
    /// Monotonically increasing sequence counter for FIFO ordering.
    next_seq: AtomicU64,
    state_changed: AtomicBool,
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
            state_changed: AtomicBool::new(false),
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

    /// A typed internal notification for App/projection changes.  It replaces
    /// the historical fake terminal resize event, so a real resize can never
    /// be confused with a data-change signal.
    pub fn notify_state_changed(&self) {
        self.state_changed.store(true, AtomicOrdering::Release);
    }

    #[must_use]
    pub fn take_state_changed(&self) -> bool {
        self.state_changed.swap(false, AtomicOrdering::AcqRel)
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
        let receiver = match self.receiver.lock() {
            Ok(receiver) => receiver,
            Err(poisoned) => poisoned.into_inner(),
        };
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

// ── EventDispatcher ──────────────────────────────────────────

use crate::components::Component;

/// Registry-backed event dispatcher.
///
/// Components register themselves by `ComponentId` (from this module).
/// On each `dispatch()` call, all pending events in the `EventBus` are
/// drained in priority order and routed to the appropriate handler(s).
pub struct EventDispatcher {
    /// Component registry keyed by `ComponentId` inner `String`.
    components: std::collections::HashMap<String, Box<dyn Component>>,
}

impl EventDispatcher {
    /// Create an empty dispatcher.
    #[must_use]
    pub fn new() -> Self {
        EventDispatcher {
            components: std::collections::HashMap::new(),
        }
    }

    /// Register a component with the given identifier.
    ///
    /// The `id` is used to match `RoutedEvent::target` when routing.
    pub fn register(&mut self, id: ComponentId, component: Box<dyn Component>) {
        self.components.insert(id.0, component);
    }

    /// Drain the event bus and dispatch every event in priority order.
    ///
    /// Events are drained via `bus.drain()` (priority-ordered), then
    /// routed to the appropriate component(s).
    pub fn dispatch(&mut self, bus: &EventBus) {
        let events = bus.drain();
        for event in events {
            self.dispatch_event(event);
        }
    }

    /// Route a single event to its target component.
    ///
    /// - If the target is the broadcast sentinel (`__broadcast__`), the
    ///   event goes to all focusable components.
    /// - If the target is a registered component, the event is delivered
    ///   there. If the handler returns `NotConsumed`, the event falls
    ///   through to broadcast (all focusable components).
    /// - If the target is unknown, the event broadcasts to all focusable
    ///   components as a fallback.
    fn dispatch_event(&mut self, event: RoutedEvent) {
        if event.target.0 == "__broadcast__" {
            self.broadcast(event.event);
            return;
        }

        if let Some(component) = self.components.get_mut(&event.target.0) {
            let result = component.handle_event(&event.event);
            if result.is_not_consumed() {
                self.broadcast(event.event);
            }
        } else {
            self.broadcast(event.event);
        }
    }

    /// Send an event to every registered focusable component.
    fn broadcast(&mut self, event: crossterm::event::Event) {
        for component in self.components.values_mut() {
            if component.focusable() {
                component.handle_event(&event);
            }
        }
    }
}

impl Default for EventDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod dispatcher_tests {
    use super::*;
    use crate::components::{Component, EventResult};
    use crate::event::EventPriority;
    use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Default)]
    struct SpyLog {
        events: Vec<(String, Event)>,
    }

    struct SpyComponent {
        id: &'static str,
        log: Rc<RefCell<SpyLog>>,
        focused: bool,
        handler: Option<Box<dyn FnMut(&Event) -> EventResult>>,
    }

    impl SpyComponent {
        fn new(id: &'static str, log: Rc<RefCell<SpyLog>>) -> Self {
            SpyComponent {
                id,
                log,
                focused: true,
                handler: None,
            }
        }

        fn with_focus(mut self, focused: bool) -> Self {
            self.focused = focused;
            self
        }

        fn with_handler(mut self, handler: impl FnMut(&Event) -> EventResult + 'static) -> Self {
            self.handler = Some(Box::new(handler));
            self
        }
    }

    impl Component for SpyComponent {
        fn render(
            &mut self,
            _ctx: &mut crate::components::RenderContext,
            _area: ratatui::layout::Rect,
        ) {
        }

        fn handle_event(&mut self, event: &Event) -> EventResult {
            self.log
                .borrow_mut()
                .events
                .push((self.id.to_string(), event.clone()));
            if let Some(ref mut handler) = self.handler {
                handler(event)
            } else {
                EventResult::Consumed
            }
        }

        fn focusable(&self) -> bool {
            self.focused
        }
        fn id(&self) -> &str {
            self.id
        }
    }

    // ── helpers ────────────────────────────────────────────────

    fn key_event(code: KeyCode) -> Event {
        Event::Key(KeyEvent::new(code, KeyModifiers::NONE))
    }

    fn make_spy(
        dispatcher: &mut EventDispatcher,
        id: &'static str,
        log: Rc<RefCell<SpyLog>>,
    ) -> ComponentId {
        let cid = ComponentId(id.to_string());
        dispatcher.register(cid.clone(), Box::new(SpyComponent::new(id, log)));
        cid
    }

    fn received(log: &SpyLog) -> Vec<&str> {
        log.events.iter().map(|(id, _)| id.as_str()).collect()
    }

    fn received_by<'a>(log: &'a SpyLog, component_id: &str) -> Vec<&'a Event> {
        log.events
            .iter()
            .filter(|(id, _)| id == component_id)
            .map(|(_, e)| e)
            .collect()
    }

    // ── priority_order_dispatch ────────────────────────────────

    #[test]
    fn priority_order_dispatch() {
        let bus = EventBus::new();
        let mut dispatcher = EventDispatcher::new();
        let log = Rc::new(RefCell::new(SpyLog::default()));

        make_spy(&mut dispatcher, "spy", Rc::clone(&log));

        bus.send(key_event(KeyCode::Char('l')), EventPriority::Low);
        bus.send(key_event(KeyCode::Char('n')), EventPriority::Normal);
        bus.send(key_event(KeyCode::Char('h')), EventPriority::High);

        dispatcher.dispatch(&bus);

        // All three events broadcast to spy. Priority order is verified by
        // EventBus::drain() (tested in mod.rs). Dispatcher processes them in
        // drain order, so spy receives h → n → l.
        assert_eq!(
            log.borrow().events.len(),
            3,
            "spy should receive all 3 broadcast events"
        );
        assert!(bus.drain().is_empty(), "bus should be empty after dispatch");
    }

    // ── routed_to_correct_component ────────────────────────────

    #[test]
    fn routed_to_correct_component() {
        let bus = EventBus::new();
        let mut dispatcher = EventDispatcher::new();
        let log = Rc::new(RefCell::new(SpyLog::default()));

        let spy_a_id = make_spy(&mut dispatcher, "component_a", Rc::clone(&log));
        let _spy_b_id = make_spy(&mut dispatcher, "component_b", Rc::clone(&log));

        let ev = key_event(KeyCode::Enter);
        bus.send_targeted(spy_a_id, ev.clone(), EventPriority::Normal);

        dispatcher.dispatch(&bus);

        // Only component_a consumed the targeted event; component_b was not
        // reached because the event was Consumed (no broadcast fallback).
        assert_eq!(received(&log.borrow()), vec!["component_a"]);
        assert!(bus.drain().is_empty());
    }

    // ── broadcast_fallback_on_not_consumed ─────────────────────

    #[test]
    fn broadcast_fallback_on_not_consumed() {
        let bus = EventBus::new();
        let mut dispatcher = EventDispatcher::new();
        let log = Rc::new(RefCell::new(SpyLog::default()));

        let not_consumed = SpyComponent::new("comp_a", Rc::clone(&log))
            .with_handler(|_e| EventResult::NotConsumed);
        let target_id = ComponentId("comp_a".to_string());
        dispatcher.register(target_id.clone(), Box::new(not_consumed));

        make_spy(&mut dispatcher, "comp_b", Rc::clone(&log));

        bus.send_targeted(target_id, key_event(KeyCode::Tab), EventPriority::Normal);
        dispatcher.dispatch(&bus);

        // comp_a receives first (targeted), then both via broadcast.
        // HashMap iteration order is non-deterministic, so exact order varies.
        let log = log.borrow();
        assert_eq!(
            received_by(&log, "comp_a").len(),
            2,
            "comp_a: targeted + broadcast"
        );
        assert_eq!(
            received_by(&log, "comp_b").len(),
            1,
            "comp_b: broadcast only"
        );
        assert_eq!(
            log.events.first().map(|(id, _)| id.as_str()),
            Some("comp_a"),
            "targeted dispatch first"
        );
        assert_eq!(log.events.len(), 3);
    }

    // ── broadcast_fallback_unknown_target ──────────────────────

    #[test]
    fn broadcast_fallback_unknown_target() {
        let bus = EventBus::new();
        let mut dispatcher = EventDispatcher::new();
        let log = Rc::new(RefCell::new(SpyLog::default()));

        make_spy(&mut dispatcher, "existing", Rc::clone(&log));

        let unknown = ComponentId("nonexistent".to_string());
        bus.send_targeted(unknown, key_event(KeyCode::F(1)), EventPriority::Normal);

        dispatcher.dispatch(&bus);

        assert_eq!(
            received(&log.borrow()),
            vec!["existing"],
            "unknown target → broadcast to existing"
        );
        assert!(bus.drain().is_empty());
    }

    // ── empty_queue_noop ───────────────────────────────────────

    #[test]
    fn empty_queue_noop() {
        let bus = EventBus::new();
        let mut dispatcher = EventDispatcher::new();
        let log = Rc::new(RefCell::new(SpyLog::default()));

        make_spy(&mut dispatcher, "noop_spy", Rc::clone(&log));

        dispatcher.dispatch(&bus);

        assert!(
            log.borrow().events.is_empty(),
            "empty queue → no events dispatched"
        );
    }

    // ── broadcast_skips_non_focusable ──────────────────────────

    #[test]
    fn broadcast_skips_non_focusable() {
        let bus = EventBus::new();
        let mut dispatcher = EventDispatcher::new();
        let log = Rc::new(RefCell::new(SpyLog::default()));

        let focused = SpyComponent::new("focused", Rc::clone(&log)).with_focus(true);
        let unfocused = SpyComponent::new("unfocused", Rc::clone(&log)).with_focus(false);

        dispatcher.register(ComponentId("focused".to_string()), Box::new(focused));
        dispatcher.register(ComponentId("unfocused".to_string()), Box::new(unfocused));

        bus.send(key_event(KeyCode::Esc), EventPriority::Normal);
        dispatcher.dispatch(&bus);

        assert_eq!(
            received(&log.borrow()),
            vec!["focused"],
            "unfocused skipped"
        );
    }

    // ── drain_post_dispatch_empty ──────────────────────────────

    #[test]
    fn drain_post_dispatch_empty() {
        let bus = EventBus::new();
        let mut dispatcher = EventDispatcher::new();
        let log = Rc::new(RefCell::new(SpyLog::default()));

        make_spy(&mut dispatcher, "drain_spy", Rc::clone(&log));

        bus.send(key_event(KeyCode::Char('x')), EventPriority::High);
        bus.send(key_event(KeyCode::Char('y')), EventPriority::Low);

        dispatcher.dispatch(&bus);

        let remaining = bus.drain();
        assert!(
            remaining.is_empty(),
            "EventBus should be empty after dispatch"
        );
        assert_eq!(log.borrow().events.len(), 2, "both events dispatched");
    }
}
