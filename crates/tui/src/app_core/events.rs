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
                queue.push_back(event);
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
            | CowdEvent::ExecutionProjectionDelta { .. }
            | CowdEvent::ExecutionGraphSummary { .. }
    )
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
}
