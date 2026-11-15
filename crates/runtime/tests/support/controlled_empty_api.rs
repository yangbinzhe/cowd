use futures::{Stream, StreamExt};
use runtime::execution_core::services::TurnCancellationControl;
use runtime::{ApiClient, ApiRequest, AssistantEvent, RuntimeError};
use std::{pin::Pin, sync::Arc};

#[derive(Clone)]
pub struct ControlledEmptyApi {
    answer: Option<&'static str>,
    control: Option<Arc<TurnCancellationControl>>,
}

impl ControlledEmptyApi {
    pub fn new(answer: Option<&'static str>) -> Self {
        Self {
            answer,
            control: None,
        }
    }
}

impl ApiClient for ControlledEmptyApi {
    fn configure_turn_cancellation(
        &mut self,
        control: Option<Arc<TurnCancellationControl>>,
    ) -> Result<(), RuntimeError> {
        self.control = control;
        Ok(())
    }

    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        let Some(control) = self.control.clone() else {
            return Box::pin(futures::stream::once(async {
                Err(RuntimeError::new("fixture cancellation owner missing"))
            }));
        };
        let step_matches = request.bound_step.as_ref().map(|step| {
            let identity = &step.snapshot().identity;
            identity.execution_id.as_deref().is_some_and(|execution| {
                control.matches_target(&identity.session_id, &identity.turn_id, execution)
            })
        });
        let context_matches = request.provider_evidence_context.as_ref().map(|context| {
            control.matches_target(&context.session_id, &context.turn_id, &context.execution_id)
                && context.graph_owner.as_ref().is_some_and(|owner| {
                    owner.activity.session_id == context.session_id
                        && owner.activity.turn_id == context.turn_id
                        && owner.activity.root_execution_id == context.execution_id
                })
        });
        if step_matches == Some(false)
            || context_matches == Some(false)
            || (step_matches.is_none() && context_matches.is_none())
        {
            return Box::pin(futures::stream::once(async {
                Err(RuntimeError::new(
                    "fixture cancellation owner does not match original request",
                ))
            }));
        }
        let mut events = Vec::new();
        if let Some(answer) = self.answer {
            events.push(AssistantEvent::TextDelta(answer.into()));
        }
        events.push(AssistantEvent::MessageStop);
        Box::pin(futures::stream::iter(events).map(move |event| {
            if control.is_cancelled() {
                Err(RuntimeError::new("fixture provider cancelled"))
            } else {
                Ok(event)
            }
        }))
    }
}

#[derive(Clone)]
struct UnownedApi;
impl ApiClient for UnownedApi {
    fn stream(
        &mut self,
        _: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        Box::pin(futures::stream::empty())
    }
}

#[test]
fn unowned_provider_refuses_turn_cancellation_control() {
    let control = TurnCancellationControl::operational("session", "turn", "execution");
    let error = UnownedApi
        .configure_turn_cancellation(Some(control))
        .expect_err("unowned provider must fail closed");
    assert!(error
        .to_string()
        .contains("physical cancellation source owner"));
}

#[test]
fn cancellation_owner_preserves_target_and_observes_stop() {
    let control = TurnCancellationControl::operational("session", "turn", "execution");
    assert!(control.matches_target("session", "turn", "execution"));
    assert!(!control.matches_target("other", "turn", "execution"));
    assert!(!control.matches_target("session", "other", "execution"));
    assert!(!control.matches_target("session", "turn", "other"));
    assert!(!control.is_cancelled());
    control.signal_operational();
    assert!(control.is_cancelled());
}
