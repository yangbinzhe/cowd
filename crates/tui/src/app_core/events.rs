//! Async-friendly bounded TUI event transport.
//!
//! Producers never block a Tokio worker on a synchronous channel.  Lossy
//! display deltas are allowed to apply backpressure, while terminal/recovery
//! facts are retained in a tiny reliable side queue so a full render queue
//! cannot hide completion from the user.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::protocol::CowdEvent;

const EVENT_CHANNEL_CAPACITY: usize = 256;
/// Recovery facts must remain bounded as well. Projection deltas are
/// cursor-based and can be coalesced; a later snapshot is authoritative when
/// an older delta has been compacted away.
const RELIABLE_QUEUE_CAPACITY: usize = 1024;

struct SequencedEvent {
    sequence: u64,
    event: CowdEvent,
}

#[derive(Clone)]
pub struct CowdEventSender {
    inner: mpsc::Sender<SequencedEvent>,
    reliable: Arc<Mutex<VecDeque<SequencedEvent>>>,
    next_sequence: Arc<AtomicU64>,
}

pub struct CowdEventReceiver {
    inner: mpsc::Receiver<SequencedEvent>,
    reliable: Arc<Mutex<VecDeque<SequencedEvent>>>,
    pending_primary: Option<SequencedEvent>,
}

impl CowdEventSender {
    /// Non-blocking delivery.  When normal rendering is saturated, terminal
    /// and projection recovery facts are retained for the consumer; transient
    /// prose/progress can be fetched again from the canonical projection.
    pub fn send(&self, event: CowdEvent) -> Result<(), mpsc::error::TrySendError<CowdEvent>> {
        let queued = SequencedEvent {
            sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
            event,
        };
        match self.inner.try_send(queued) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(queued)) if is_reliable_event(&queued.event) => {
                let mut queue = self
                    .reliable
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                retain_reliable_event(&mut queue, queued)
                    .map_err(|queued| mpsc::error::TrySendError::Full(queued.event))
            }
            Err(mpsc::error::TrySendError::Full(queued)) => {
                Err(mpsc::error::TrySendError::Full(queued.event))
            }
            Err(mpsc::error::TrySendError::Closed(queued)) => {
                Err(mpsc::error::TrySendError::Closed(queued.event))
            }
        }
    }

    pub fn try_send(&self, event: CowdEvent) -> Result<(), mpsc::error::TrySendError<CowdEvent>> {
        self.send(event)
    }

    /// Backpressured delivery for durable projection streams. The producer
    /// must not advance its cursor until the UI has accepted the envelope.
    pub async fn send_wait(
        &self,
        event: CowdEvent,
    ) -> Result<(), mpsc::error::SendError<CowdEvent>> {
        let queued = SequencedEvent {
            sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
            event,
        };
        self.inner
            .send(queued)
            .await
            .map_err(|error| mpsc::error::SendError(error.0.event))
    }
}

impl CowdEventReceiver {
    pub fn try_recv(&mut self) -> Result<CowdEvent, mpsc::error::TryRecvError> {
        if self.pending_primary.is_none() {
            match self.inner.try_recv() {
                Ok(event) => self.pending_primary = Some(event),
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return self
                        .reliable
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .pop_front()
                        .map(|event| event.event)
                        .ok_or(mpsc::error::TryRecvError::Disconnected);
                }
            }
        }
        let mut reliable = self
            .reliable
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match (
            self.pending_primary.as_ref().map(|event| event.sequence),
            reliable.front().map(|event| event.sequence),
        ) {
            (Some(primary), Some(side)) if side < primary => {
                Ok(reliable.pop_front().expect("front exists").event)
            }
            (Some(_), _) => Ok(self.pending_primary.take().expect("primary exists").event),
            (None, Some(_)) => Ok(reliable.pop_front().expect("front exists").event),
            (None, None) => Err(mpsc::error::TryRecvError::Empty),
        }
    }
}

fn is_reliable_event(event: &CowdEvent) -> bool {
    if let CowdEvent::SessionScoped { event, .. } = event {
        return is_reliable_event(event);
    }
    matches!(
        event,
        CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::UserMessageCommitted { .. }
                | crate::protocol::GatewaySessionEvent::ExecutionPhase { .. }
                | crate::protocol::GatewaySessionEvent::TerminalCommitted { .. }
                | crate::protocol::GatewaySessionEvent::TurnError { .. },
        } | CowdEvent::SessionHistoryPage { .. }
            | CowdEvent::SessionHistoryCatchupPage { .. }
            | CowdEvent::SessionHistoryHydrated { .. }
            | CowdEvent::SessionHistoryOlderPage { .. }
            | CowdEvent::SessionHistoryOlderFailed { .. }
            | CowdEvent::SessionHistoryHydrationFailed { .. }
            | CowdEvent::SessionStreamConnection { .. }
            | CowdEvent::ExecutionProjectionConnection { .. }
            | CowdEvent::MissionProjectionSnapshot { .. }
            | CowdEvent::TurnError { .. }
            | CowdEvent::ResourceUploaded { .. }
            | CowdEvent::ResourceUploadFailed { .. }
            | CowdEvent::ApprovalRequested { .. }
            | CowdEvent::ExecutionProjectionDelta { .. }
            | CowdEvent::ExecutionProjectionLoaded { .. }
            | CowdEvent::ExecutionProjectionLive { .. }
            | CowdEvent::ExecutionProjectionRefreshFailed { .. }
            | CowdEvent::ExecutionProjectionAccessRevoked { .. }
            | CowdEvent::ExecutionGraphSummary { .. }
            | CowdEvent::AppTui { .. }
    )
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct EventScope<'a> {
    session_id: Option<&'a str>,
    authority_generation: Option<u64>,
}

fn event_scope(event: &CowdEvent) -> (EventScope<'_>, &CowdEvent) {
    match event {
        CowdEvent::SessionScoped {
            session_id,
            authority_generation,
            event,
        } => {
            let (nested, event) = event_scope(event);
            (
                EventScope {
                    session_id: nested.session_id.or(Some(session_id.as_str())),
                    authority_generation: nested
                        .authority_generation
                        .or(Some(*authority_generation)),
                },
                event,
            )
        }
        event => (EventScope::default(), event),
    }
}

fn same_event_scope(left: EventScope<'_>, right: EventScope<'_>) -> bool {
    left == right
}

fn is_reconstructible_reliable_event(event: &CowdEvent) -> bool {
    let (_, event) = event_scope(event);
    matches!(
        event,
        CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::ExecutionPhase { .. }
        } | CowdEvent::SessionStreamConnection { .. }
            | CowdEvent::ExecutionProjectionConnection { .. }
            | CowdEvent::MissionProjectionSnapshot { .. }
            | CowdEvent::ExecutionProjectionDelta { .. }
            | CowdEvent::ExecutionProjectionRefreshFailed { .. }
            | CowdEvent::ExecutionProjectionAccessRevoked { .. }
            | CowdEvent::ExecutionGraphSummary { .. }
            | CowdEvent::AppTui { .. }
    )
}

fn is_durable_terminal(event: &CowdEvent) -> bool {
    let (_, event) = event_scope(event);
    matches!(
        event,
        CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TerminalCommitted { .. }
        }
    )
}

fn retain_reliable_event(
    queue: &mut VecDeque<SequencedEvent>,
    event: SequencedEvent,
) -> Result<(), SequencedEvent> {
    let (event_session, event_inner) = event_scope(&event.event);
    match event_inner {
        CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::ExecutionPhase { correlation, .. },
        } => {
            if let Some(index) = queue.iter().position(|queued| {
                let (queued_session, queued_inner) = event_scope(&queued.event);
                matches!(
                    queued_inner,
                    CowdEvent::GatewaySession {
                        event: crate::protocol::GatewaySessionEvent::ExecutionPhase {
                            correlation: queued_correlation,
                            ..
                        }
                    } if same_event_scope(queued_session, event_session)
                        && queued_correlation.session_id == correlation.session_id
                        && queued_correlation.execution_id == correlation.execution_id
                        && queued_correlation.turn_id == correlation.turn_id
                )
            }) {
                queue.remove(index);
            }
        }
        CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TerminalCommitted { correlation, .. },
        } => {
            if let Some(terminal_id) = correlation.terminal_id.as_deref() {
                if let Some(index) = queue.iter().position(|queued| {
                    let (queued_session, queued_inner) = event_scope(&queued.event);
                    matches!(
                        queued_inner,
                        CowdEvent::GatewaySession {
                            event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
                                correlation: queued_correlation,
                                ..
                            }
                        } if same_event_scope(queued_session, event_session)
                            && queued_correlation.terminal_id.as_deref() == Some(terminal_id)
                    )
                }) {
                    queue.remove(index);
                }
            }
        }
        CowdEvent::SessionStreamConnection { session_id, .. } => {
            if let Some(index) = queue.iter().position(|queued| {
                let (queued_scope, queued_inner) = event_scope(&queued.event);
                matches!(
                    queued_inner,
                    CowdEvent::SessionStreamConnection {
                        session_id: queued_session_id,
                        ..
                    } if same_event_scope(queued_scope, event_session)
                        && queued_session_id == session_id
                )
            }) {
                queue.remove(index);
            }
        }
        CowdEvent::ExecutionProjectionConnection {
            generation,
            execution_id,
            ..
        } => {
            if let Some(index) = queue.iter().position(|queued| {
                matches!(
                    &queued.event,
                    CowdEvent::ExecutionProjectionConnection {
                        generation: queued_generation,
                        execution_id: queued_execution_id,
                        ..
                    } if queued_generation == generation
                        && queued_execution_id == execution_id
                )
            }) {
                queue.remove(index);
            }
        }
        CowdEvent::MissionProjectionSnapshot { mission_id, .. } => {
            if let Some(index) = queue.iter().position(|queued| {
                matches!(
                    &queued.event,
                    CowdEvent::MissionProjectionSnapshot {
                        mission_id: queued_mission_id,
                        ..
                    } if queued_mission_id == mission_id
                )
            }) {
                queue.remove(index);
            }
        }
        CowdEvent::ExecutionProjectionDelta { generation, delta } => {
            if let Some(index) = queue.iter().position(|queued| {
                matches!(&queued.event, CowdEvent::ExecutionProjectionDelta {
                    generation: queued_generation,
                    delta: queued_delta,
                } if queued_generation == generation
                    && queued_delta.execution_id == delta.execution_id)
            }) {
                queue.remove(index);
            }
        }
        CowdEvent::ExecutionProjectionLoaded {
            generation,
            projection,
        } => {
            if let Some(index) = queue.iter().position(|queued| {
                matches!(&queued.event, CowdEvent::ExecutionProjectionLoaded {
                    generation: queued_generation,
                    projection: queued_projection,
                } if queued_generation == generation
                    && queued_projection.execution_id == projection.execution_id)
            }) {
                queue.remove(index);
            }
        }
        CowdEvent::ExecutionProjectionRefreshFailed {
            generation,
            execution_id,
            ..
        } => {
            if let Some(index) = queue.iter().position(|queued| {
                matches!(&queued.event, CowdEvent::ExecutionProjectionRefreshFailed {
                    generation: queued_generation,
                    execution_id: queued_execution_id,
                    ..
                } if queued_generation == generation && queued_execution_id == execution_id)
            }) {
                queue.remove(index);
            }
        }
        CowdEvent::ExecutionProjectionAccessRevoked {
            generation,
            execution_id,
            ..
        } => {
            if let Some(index) = queue.iter().position(|queued| {
                matches!(&queued.event, CowdEvent::ExecutionProjectionAccessRevoked {
                    generation: queued_generation,
                    execution_id: queued_execution_id,
                    ..
                } if queued_generation == generation && queued_execution_id == execution_id)
            }) {
                queue.remove(index);
            }
        }
        CowdEvent::ExecutionGraphSummary { summary } => {
            if let Some(graph_id) = summary.graph_id.as_deref() {
                if let Some(index) = queue.iter().position(|queued| {
                    let (queued_scope, queued_inner) = event_scope(&queued.event);
                    matches!(queued_inner, CowdEvent::ExecutionGraphSummary { summary: queued_summary }
                        if same_event_scope(queued_scope, event_session)
                            && queued_summary.graph_id.as_deref() == Some(graph_id))
                }) {
                    queue.remove(index);
                }
            }
        }
        _ => {}
    }

    if queue.len() >= RELIABLE_QUEUE_CAPACITY {
        // Prefer replacing an update that Gateway can reconstruct from its
        // durable projection. Only if the queue consists solely of terminal
        // facts do we discard the oldest one, retaining the newest outcome.
        if let Some(index) = queue
            .iter()
            .position(|queued| is_reconstructible_reliable_event(&queued.event))
        {
            queue.remove(index);
        } else if let Some(index) = queue
            .iter()
            .position(|queued| !is_durable_terminal(&queued.event))
        {
            queue.remove(index);
        } else {
            // Never silently discard a durable terminal. Session stream
            // producers use `send_wait`, while non-awaiting callers receive
            // explicit backpressure and must recover from the durable cursor.
            return Err(event);
        }
    }
    queue.push_back(event);
    Ok(())
}

/// Create a bounded, async-friendly event channel.
#[must_use]
pub fn cowd_event_channel() -> (CowdEventSender, CowdEventReceiver) {
    let (sender, receiver) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let reliable = Arc::new(Mutex::new(VecDeque::new()));
    let next_sequence = Arc::new(AtomicU64::new(0));
    (
        CowdEventSender {
            inner: sender,
            reliable: Arc::clone(&reliable),
            next_sequence: Arc::clone(&next_sequence),
        },
        CowdEventReceiver {
            inner: receiver,
            reliable,
            pending_primary: None,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn display_delta(value: impl Into<String>) -> CowdEvent {
        CowdEvent::ThinkingDelta {
            thinking: value.into(),
        }
    }

    fn durable_terminal(index: usize) -> CowdEvent {
        CowdEvent::GatewaySession {
            event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
                correlation: crate::protocol::GatewayEventCorrelation {
                    session_id: "session-test".to_string(),
                    execution_id: Some(format!("execution-{index}")),
                    turn_id: Some(format!("turn-{index}")),
                    part_id: Some("assistant_text".to_string()),
                    message_id: Some(format!("assistant-{index}")),
                    terminal_id: Some(format!("terminal-{index}")),
                    commit_cursor: Some(index as u64),
                    replayed: false,
                },
                assistant_text: format!("terminal-{index}"),
                sequence: Some(index),
                iterations: 1,
                token_usage: None,
            },
        }
    }

    #[test]
    fn event_channel_send_recv() {
        let (tx, mut rx) = cowd_event_channel();
        tx.send(display_delta("hello")).expect("send");
        let event = rx.try_recv().expect("event");
        assert!(matches!(event, CowdEvent::ThinkingDelta { thinking } if thinking == "hello"));
    }

    #[test]
    fn saturated_channel_keeps_terminal_fact_without_reordering_older_events() {
        let (tx, mut rx) = cowd_event_channel();
        for index in 0..EVENT_CHANNEL_CAPACITY {
            tx.try_send(display_delta(index.to_string()))
                .expect("capacity");
        }
        tx.send(durable_terminal(0)).expect("terminal retained");
        assert!(
            matches!(
                rx.try_recv().expect("oldest primary event first"),
                CowdEvent::ThinkingDelta { thinking } if thinking == "0"
            ),
            "terminal must not overtake earlier rendering events"
        );
        tx.send(display_delta("after-terminal"))
            .expect("new primary event");
        for _ in 1..EVENT_CHANNEL_CAPACITY {
            let _ = rx.try_recv().expect("remaining primary event");
        }
        assert!(matches!(
            rx.try_recv()
                .expect("reliable terminal after primary queue"),
            CowdEvent::GatewaySession {
                event: crate::protocol::GatewaySessionEvent::TerminalCommitted { .. }
            }
        ));
        assert!(matches!(
            rx.try_recv().expect("newer primary event remains after terminal"),
            CowdEvent::ThinkingDelta { thinking } if thinking == "after-terminal"
        ));
    }

    #[test]
    fn lossy_overflow_returns_immediately() {
        let (tx, _rx) = cowd_event_channel();
        for index in 0..EVENT_CHANNEL_CAPACITY {
            tx.try_send(display_delta(index.to_string()))
                .expect("capacity");
        }
        assert!(tx.try_send(display_delta("overflow")).is_err());
    }

    #[tokio::test]
    async fn durable_stream_delivery_waits_for_capacity_instead_of_dropping() {
        let (tx, mut rx) = cowd_event_channel();
        for index in 0..EVENT_CHANNEL_CAPACITY {
            tx.try_send(display_delta(index.to_string()))
                .expect("capacity");
        }
        let pending = tokio::spawn({
            let tx = tx.clone();
            async move {
                tx.send_wait(CowdEvent::AppTui {
                    panel_id: "fixture".to_string(),
                    event: cowd_app_host::TuiAppEvent::LiveStopped {
                        subscription_id: "fixture.live".to_string(),
                    },
                })
                .await
            }
        });
        tokio::task::yield_now().await;
        assert!(!pending.is_finished());
        let _ = rx.try_recv().expect("free one event slot");
        assert!(pending.await.expect("sender task").is_ok());
    }

    #[test]
    fn saturated_projection_stream_is_coalesced_and_bounded() {
        let (tx, _rx) = cowd_event_channel();
        for index in 0..EVENT_CHANNEL_CAPACITY {
            tx.try_send(display_delta(index.to_string()))
                .expect("capacity");
        }
        for cursor in 1_u64..=10_000 {
            tx.send(CowdEvent::ExecutionProjectionDelta {
                generation: 7,
                delta: harness_contract::projection::ProjectionDelta {
                    schema_version: 1,
                    execution_id: "execution-a".to_string(),
                    base_cursor: cursor.saturating_sub(1),
                    target_cursor: cursor,
                    events: Vec::new(),
                },
            })
            .expect("reliable projection");
        }
        let reliable = tx.reliable.lock().expect("reliable queue");
        assert_eq!(reliable.len(), 1);
        assert!(matches!(
            reliable.front().map(|queued| &queued.event),
            Some(CowdEvent::ExecutionProjectionDelta { generation: 7, delta })
                if delta.target_cursor == 10_000
        ));
    }

    #[test]
    fn reliable_coalescing_never_crosses_session_authority_generation() {
        let (tx, _rx) = cowd_event_channel();
        for index in 0..EVENT_CHANNEL_CAPACITY {
            tx.try_send(display_delta(index.to_string()))
                .expect("capacity");
        }
        let connection = |generation, attempt| CowdEvent::SessionScoped {
            session_id: "session-a".to_string(),
            authority_generation: generation,
            event: Box::new(CowdEvent::SessionStreamConnection {
                session_id: "session-a".to_string(),
                state: crate::protocol::SessionStreamConnectionState::Reconnecting {
                    attempt,
                    after_cursor: Some(u64::from(attempt)),
                },
            }),
        };

        tx.send(connection(1, 1)).expect("generation one");
        tx.send(connection(1, 2))
            .expect("same generation coalesces");
        tx.send(connection(2, 1))
            .expect("new authority generation remains distinct");

        let reliable = tx.reliable.lock().expect("reliable queue");
        assert_eq!(
            reliable.len(),
            2,
            "an old authority event must not replace or absorb the new generation"
        );
        let scopes = reliable
            .iter()
            .map(|queued| event_scope(&queued.event).0.authority_generation)
            .collect::<Vec<_>>();
        assert_eq!(scopes, vec![Some(1), Some(2)]);
        assert!(matches!(
            reliable.front().map(|queued| &queued.event),
            Some(CowdEvent::SessionScoped {
                authority_generation: 1,
                event,
                ..
            }) if matches!(
                event.as_ref(),
                CowdEvent::SessionStreamConnection {
                    state: crate::protocol::SessionStreamConnectionState::Reconnecting {
                        attempt: 2,
                        ..
                    },
                    ..
                }
            )
        ));
    }

    #[test]
    fn reliable_terminal_queue_has_a_hard_cap_and_applies_explicit_backpressure() {
        let (tx, _rx) = cowd_event_channel();
        for index in 0..EVENT_CHANNEL_CAPACITY {
            tx.try_send(display_delta(index.to_string()))
                .expect("capacity");
        }
        for index in 0..RELIABLE_QUEUE_CAPACITY {
            tx.send(durable_terminal(index)).expect("reliable terminal");
        }
        assert!(
            tx.send(durable_terminal(RELIABLE_QUEUE_CAPACITY)).is_err(),
            "a durable terminal must receive backpressure instead of evicting another terminal"
        );
        let reliable = tx.reliable.lock().expect("reliable queue");
        assert_eq!(reliable.len(), RELIABLE_QUEUE_CAPACITY);
        assert!(matches!(
            reliable.back().map(|queued| &queued.event),
            Some(CowdEvent::GatewaySession {
                event: crate::protocol::GatewaySessionEvent::TerminalCommitted {
                    correlation,
                    ..
                }
            }) if correlation.terminal_id.as_deref()
                == Some(format!("terminal-{}", RELIABLE_QUEUE_CAPACITY - 1).as_str())
        ));
    }

    #[test]
    fn saturated_channel_retains_approval_requests() {
        let (tx, mut rx) = cowd_event_channel();
        for index in 0..EVENT_CHANNEL_CAPACITY {
            tx.try_send(display_delta(index.to_string()))
                .expect("capacity");
        }
        tx.send(CowdEvent::ApprovalRequested {
            tool: "write_file".to_string(),
        })
        .expect("approval retained");
        for _ in 0..EVENT_CHANNEL_CAPACITY {
            let _ = rx.try_recv().expect("older primary event");
        }
        assert!(matches!(
            rx.try_recv().expect("reliable approval after older events"),
            CowdEvent::ApprovalRequested { tool } if tool == "write_file"
        ));
    }
}
