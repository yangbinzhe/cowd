use harness_contract::turn::{SessionInputStatus, TurnInputCheckpoint};

use crate::session_input::SessionInputRecord;

#[must_use]
pub fn checkpoint_instruction(
    checkpoint: TurnInputCheckpoint,
    records: &[SessionInputRecord],
) -> Option<String> {
    if records.is_empty() {
        return None;
    }

    let mut lines = vec![
        format!(
            "## Runtime input updates at `{}`",
            checkpoint.as_str()
        ),
        "Additional user/session inputs arrived while this turn was running. Decide whether they supplement the current answer, change the plan, or should be acknowledged as queued work. Do not ignore high-priority corrections.".to_string(),
    ];
    for record in records {
        let marker = match record.status {
            SessionInputStatus::Consumed => "consumed",
            SessionInputStatus::InterruptRequested => "interrupt",
            SessionInputStatus::ControlResolved => "control",
            _ => "input",
        };
        lines.push(format!(
            "- {} `{}` ({:?}): {}",
            marker,
            record.envelope.input_id.as_str(),
            record.decision,
            record.envelope.content
        ));
    }
    Some(lines.join("\n"))
}
