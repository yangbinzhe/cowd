//! Async-friendly bounded TUI event transport.
//!
//! Producers never block a Tokio worker on a synchronous channel.  Lossy
//! display deltas are allowed to apply backpressure, while terminal/recovery
//! facts are retained in a tiny reliable side queue so a full render queue
//! cannot hide completion from the user.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::protocol::CowdEvent;

const EVENT_CHANNEL_CAPACITY: usize = 256;
/// Recovery facts must remain bounded as well. Projection deltas are
/// cursor-based and can be coalesced; a later snapshot is authoritative when
/// an older delta has been compacted away.
const RELIABLE_QUEUE_CAPACITY: usize = 1024;

#[derive(Clone)]
pub struct CowdEventSender {
    inner: mpsc::Sender<CowdEvent>,
    reliable: Arc<Mutex<VecDeque<CowdEvent>>>,
}

pub struct CowdEventReceiver {
    inner: mpsc::Receiver<CowdEvent>,
    reliable: Arc<Mutex<VecDeque<CowdEvent>>>,
}

impl CowdEventSender {
    /// Non-blocking delivery.  When normal rendering is saturated, terminal
    /// and projection recovery facts are retained for the consumer; transient
    /// prose/progress can be fetched again from the canonical projection.
    pub fn send(&self, event: CowdEvent) -> Result<(), mpsc::error::TrySendError<CowdEvent>> {
        match self.inner.try_send(event) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(event)) if is_reliable_event(&event) => {
                let mut queue = self
                    .reliable
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                retain_reliable_event(&mut queue, event);
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    pub fn try_send(&self, event: CowdEvent) -> Result<(), mpsc::error::TrySendError<CowdEvent>> {
        self.send(event)
    }
}

impl CowdEventReceiver {
    pub fn try_recv(&mut self) -> Result<CowdEvent, mpsc::error::TryRecvError> {
        if let Some(event) = self
            .reliable
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop_front()
        {
            return Ok(event);
        }
        self.inner.try_recv()
    }
}

fn is_reliable_event(event: &CowdEvent) -> bool {
    matches!(
        event,
        CowdEvent::TurnComplete { .. }
            | CowdEvent::TurnError { .. }
            | CowdEvent::ApprovalRequested { .. }
            | CowdEvent::ExecutionProjectionDelta { .. }
            | CowdEvent::ExecutionGraphSummary { .. }
            | CowdEvent::MfgContract { .. }
            | CowdEvent::MfgSnapshot { .. }
            | CowdEvent::MfgReadFailed { .. }
    )
}

fn retain_reliable_event(queue: &mut VecDeque<CowdEvent>, event: CowdEvent) {
    match &event {
        CowdEvent::ExecutionProjectionDelta { delta } => {
            if let Some(index) = queue.iter().position(|queued| {
                matches!(queued, CowdEvent::ExecutionProjectionDelta { delta: queued_delta }
                    if queued_delta.execution_id == delta.execution_id)
            }) {
                queue[index] = event;
                return;
            }
        }
        CowdEvent::ExecutionGraphSummary { summary } => {
            if let Some(graph_id) = summary.graph_id.as_deref() {
                if let Some(index) = queue.iter().position(|queued| {
                    matches!(queued, CowdEvent::ExecutionGraphSummary { summary: queued_summary }
                        if queued_summary.graph_id.as_deref() == Some(graph_id))
                }) {
                    queue[index] = event;
                    return;
                }
            }
        }
        CowdEvent::MfgContract { .. }
        | CowdEvent::MfgSnapshot { .. }
        | CowdEvent::MfgReadFailed { .. } => {
            if let Some(index) = queue.iter().position(|queued| match (&event, queued) {
                (
                    CowdEvent::MfgContract {
                        generation: event_generation,
                        ..
                    },
                    CowdEvent::MfgContract {
                        generation: queued_generation,
                        ..
                    },
                ) => queued_generation == event_generation,
                (
                    CowdEvent::MfgSnapshot {
                        generation: event_generation,
                        ..
                    },
                    CowdEvent::MfgSnapshot {
                        generation: queued_generation,
                        ..
                    },
                ) => queued_generation == event_generation,
                (
                    CowdEvent::MfgReadFailed {
                        generation: event_generation,
                        section: event_section,
                        ..
                    },
                    CowdEvent::MfgReadFailed {
                        generation: queued_generation,
                        section: queued_section,
                        ..
                    },
                ) => queued_generation == event_generation && queued_section == event_section,
                _ => false,
            }) {
                queue[index] = event;
                return;
            }
        }
        _ => {}
    }

    if queue.len() >= RELIABLE_QUEUE_CAPACITY {
        // Prefer replacing an update that Gateway can reconstruct from its
        // durable projection. Only if the queue consists solely of terminal
        // facts do we discard the oldest one, retaining the newest outcome.
        if let Some(index) = queue.iter().position(|queued| {
            matches!(
                queued,
                CowdEvent::ExecutionProjectionDelta { .. }
                    | CowdEvent::ExecutionGraphSummary { .. }
                    | CowdEvent::MfgContract { .. }
                    | CowdEvent::MfgSnapshot { .. }
                    | CowdEvent::MfgReadFailed { .. }
            )
        }) {
            queue.remove(index);
        } else {
            queue.pop_front();
        }
    }
    queue.push_back(event);
}

/// Create a bounded, async-friendly event channel.
#[must_use]
pub fn cowd_event_channel() -> (CowdEventSender, CowdEventReceiver) {
    let (sender, receiver) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let reliable = Arc::new(Mutex::new(VecDeque::new()));
    (
        CowdEventSender {
            inner: sender,
            reliable: Arc::clone(&reliable),
        },
        CowdEventReceiver {
            inner: receiver,
            reliable,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_channel_send_recv() {
        let (tx, mut rx) = cowd_event_channel();
        tx.send(CowdEvent::TextDelta {
            text: "hello".into(),
        })
        .expect("send");
        let event = rx.try_recv().expect("event");
        assert!(matches!(event, CowdEvent::TextDelta { text } if text == "hello"));
    }

    #[test]
    fn saturated_channel_keeps_terminal_fact_without_blocking() {
        let (tx, mut rx) = cowd_event_channel();
        for index in 0..EVENT_CHANNEL_CAPACITY {
            tx.try_send(CowdEvent::TextDelta {
                text: index.to_string(),
            })
            .expect("capacity");
        }
        tx.send(CowdEvent::TurnComplete {
            assistant_text: "done".into(),
            iterations: 1,
        })
        .expect("terminal retained");
        assert!(matches!(
            rx.try_recv().expect("reliable first"),
            CowdEvent::TurnComplete { .. }
        ));
    }

    #[test]
    fn lossy_overflow_returns_immediately() {
        let (tx, _rx) = cowd_event_channel();
        for index in 0..EVENT_CHANNEL_CAPACITY {
            tx.try_send(CowdEvent::TextDelta {
                text: index.to_string(),
            })
            .expect("capacity");
        }
        assert!(tx
            .try_send(CowdEvent::TextDelta {
                text: "overflow".into(),
            })
            .is_err());
    }

    #[test]
    fn saturated_projection_stream_is_coalesced_and_bounded() {
        let (tx, _rx) = cowd_event_channel();
        for index in 0..EVENT_CHANNEL_CAPACITY {
            tx.try_send(CowdEvent::TextDelta {
                text: index.to_string(),
            })
            .expect("capacity");
        }
        for cursor in 1_u64..=10_000 {
            tx.send(CowdEvent::ExecutionProjectionDelta {
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
            reliable.front(),
            Some(CowdEvent::ExecutionProjectionDelta { delta }) if delta.target_cursor == 10_000
        ));
    }

    #[test]
    fn reliable_terminal_queue_has_a_hard_cap_and_retains_latest_outcome() {
        let (tx, _rx) = cowd_event_channel();
        for index in 0..EVENT_CHANNEL_CAPACITY {
            tx.try_send(CowdEvent::TextDelta {
                text: index.to_string(),
            })
            .expect("capacity");
        }
        for index in 0..(RELIABLE_QUEUE_CAPACITY + 10) {
            tx.send(CowdEvent::TurnComplete {
                assistant_text: format!("terminal-{index}"),
                iterations: 1,
            })
            .expect("reliable terminal");
        }
        let reliable = tx.reliable.lock().expect("reliable queue");
        assert_eq!(reliable.len(), RELIABLE_QUEUE_CAPACITY);
        assert!(matches!(
            reliable.back(),
            Some(CowdEvent::TurnComplete { assistant_text, .. })
                if assistant_text == &format!("terminal-{}", RELIABLE_QUEUE_CAPACITY + 9)
        ));
    }

    #[test]
    fn saturated_channel_retains_approval_requests() {
        let (tx, mut rx) = cowd_event_channel();
        for index in 0..EVENT_CHANNEL_CAPACITY {
            tx.try_send(CowdEvent::TextDelta {
                text: index.to_string(),
            })
            .expect("capacity");
        }
        tx.send(CowdEvent::ApprovalRequested {
            tool: "write_file".to_string(),
        })
        .expect("approval retained");
        assert!(matches!(
            rx.try_recv().expect("reliable approval"),
            CowdEvent::ApprovalRequested { tool } if tool == "write_file"
        ));
    }
}
