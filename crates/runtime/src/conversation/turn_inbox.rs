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
        let proposal = record.relation_proposal.as_ref().map_or_else(
            || "no lifecycle proposal".to_string(),
            |proposal| {
                format!(
                    "proposal={:?} confidence={} reasons={}",
                    proposal.candidate,
                    proposal.confidence_basis_points,
                    proposal.reasons.join(",")
                )
            },
        );
        lines.push(format!(
            "- {} `{}` ({:?}; {}): {}",
            marker,
            record.envelope.input_id.as_str(),
            record.decision,
            proposal,
            record.envelope.content
        ));
    }
    Some(lines.join("\n"))
}
