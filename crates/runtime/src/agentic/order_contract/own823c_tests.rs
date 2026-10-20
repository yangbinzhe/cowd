use super::*;

#[test]
fn root_delivery_contract_discovers_publication_without_inventing_a_selector() {
    assert!(ARTIFACT_CONTENT_REF_CONTRACT.contains("tool_search"));
    assert!(ARTIFACT_CONTENT_REF_CONTRACT.contains("returned"));
    assert!(ARTIFACT_CONTENT_REF_CONTRACT.contains("file hash is not an artifact selector"));
    assert!(ARTIFACT_CONTENT_REF_CONTRACT.contains("message://current"));
    assert!(ARTIFACT_CONTENT_REF_CONTRACT.contains("does not require creating a Task report"));
}

#[test]
fn review_only_execution_keeps_existing_review_and_retirement_actions() {
    use harness_contract::agent_action::*;
    let actions = worker_closeout_action_ids("execute", TaskPurpose::Exploration);
    for required in [OBJECTIVE_REVIEW_TOOL_ID, TASK_WITHDRAW_TOOL_ID, TASK_SUBMIT_TOOL_ID] {
        assert!(actions.contains(&required));
    }
    assert!(OBJECTIVE_REVIEW_SCOPE_DISCIPLINE.contains("distinct"));
    assert!(OBJECTIVE_REVIEW_SCOPE_DISCIPLINE.contains("purpose=exploration"));
    assert!(OBJECTIVE_REVIEW_SCOPE_DISCIPLINE.contains("not redo"));
}
