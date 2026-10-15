//! One shape for "the model asked for something that did not run".
//!
//! A tool call can be refused before dispatch for several reasons - the arguments
//! violate the exposed contract, or the tool is outside this request's exposure
//! lease. Each reason used to render its own answer: its own tool-result text, its
//! own durable event, its own frame-level sentence. Four branches, four hand-written
//! copies, and one of them (a wholly-refused frame for an unexposed tool) rendered
//! none of them at all - it returned a bare error, so the model's own frame left no
//! assistant message and no tool result and the model could not tell its call had
//! not run.
//!
//! The obligations do not depend on the reason. Every refused call must be answered
//! in-step, because a call the model cannot see was not run is a call it will retry.
//! Every refused call must be recorded durably, because an operator replaying a run
//! needs to know what was asked for. Both live here so a new refusal branch inherits
//! them instead of having to remember them.

use super::*;

/// One tool call Runtime refused before dispatch, and why.
#[derive(Debug, Clone)]
pub(crate) struct FrameRefusal {
    pub call: ModelToolCall,
    pub reason: String,
}

impl FrameRefusal {
    pub(crate) fn new(call: ModelToolCall, reason: impl Into<String>) -> Self {
        Self {
            call,
            reason: reason.into(),
        }
    }

    /// The tool result the model reads on its next step.
    ///
    /// It names the call, states plainly that nothing ran, and points at the shape
    /// to send. "Nothing ran" is the part a refusal that only reports a validation
    /// error leaves out, and it is the difference between a model that corrects its
    /// arguments and one that concludes the tool is broken.
    pub(crate) fn tool_result(&self) -> ConversationMessage {
        ConversationMessage::tool_result(
            self.call.id.clone(),
            self.call.name.clone(),
            format!(
                "Runtime did not execute this call: {}. \
                 Nothing from it ran and no state changed. \
                 Send the same intended action again in the exact shape above.",
                self.reason
            ),
            true,
        )
    }

    /// The durable payload for `tool.call.rejected_before_dispatch`.
    ///
    /// Not `tool.invocation.failed`: an invocation lifecycle event requires an
    /// active Runtime activity binding, and a call that never dispatched has none by
    /// definition. A distinct kind says what actually happened and survives.
    pub(crate) fn event_payload(&self, invocation_id: &str) -> serde_json::Value {
        serde_json::json!({
            "invocation_id": invocation_id,
            "tool_call_id": self.call.id,
            "tool_name": self.call.name,
            "failure_kind": crate::ToolFailureKind::InputContractRejected.as_str(),
            "reason": self.reason,
            "side_effect_committed": false,
            "dispatched": false,
            // Whether the model was actually told. A refusal nobody reads costs a
            // provider round-trip every time it is ignored, and for a while the only
            // way to tell the two apart was to watch the model's behaviour: one live
            // run was refused, wrote the same plan back three times at 0.98 wording
            // similarity, and the durable log showed the refusal had been recorded
            // correctly the whole time.
            //
            // Both paths set this - the model-step path through
            // `preflight_tool_results`, the tool path through a failed call - so one
            // count answers "did the model get told" across both.
            "model_visible": true,
            "model_visible_preview": self
                .call
                .input
                .chars()
                .take(200)
                .collect::<String>(),
        })
    }

    /// The invocation id this refusal is filed under.
    ///
    /// Content-addressed on the session, the call id and the bytes the model sent, so
    /// re-sending the same frame lands on the same id and an operator can see that
    /// Runtime refused the same thing twice rather than two different things.
    pub(crate) fn invocation_id(&self, session_id: &str) -> String {
        format!(
            "tool-inv-{}",
            crate::tool_invocation::stable_hash(&format!(
                "{}:{}:{}",
                session_id, self.call.id, self.call.input
            ))
        )
    }
}

/// The durable refs one refusal points at.
pub(crate) fn refusal_event_refs(invocation_id: &str, refusal: &FrameRefusal) -> Vec<RuntimeEventRef> {
    vec![
        RuntimeEventRef {
            kind: "tool_invocation".to_string(),
            id: invocation_id.to_string(),
        },
        RuntimeEventRef {
            kind: "tool_call".to_string(),
            id: refusal.call.id.clone(),
        },
    ]
}

/// The sentence a wholly-refused frame reports upward.
///
/// Every call in the frame was refused, so nothing from it ran. The model is told
/// that, told the reason, and told that each refused call already carries a tool
/// result naming the exact shape to send.
pub(crate) fn wholly_refused_frame_reason(refusals: &[FrameRefusal]) -> String {
    let detail = refusals
        .iter()
        .map(|refusal| format!("`{}`: {}", refusal.call.name, refusal.reason))
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "Runtime refused every tool call in that response: {detail}. \
         Each refused call has a tool result naming the exact shape to send. \
         Re-emit the same intended action(s) with the corrected arguments; \
         nothing from the refused frame executed."
    )
}
