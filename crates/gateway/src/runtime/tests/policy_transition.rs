use super::*;

#[test]
fn active_policy_reads_the_runtime_canonical_control_handle() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);
    let initial = runtime::SessionExecutionPolicy::from_profile(
        runtime::AutonomyProfileId::Supervised,
        4,
        runtime::SessionExecutionPolicyOrigin::SessionExplicit,
    );
    service.install_test_session_policy("policy-handle", initial.clone());
    let control = service
        .sessions
        .session("policy-handle")
        .and_then(|session| session.policy_control())
        .expect("aggregate control");
    let next = runtime::SessionExecutionPolicy::from_profile(
        runtime::AutonomyProfileId::Yolo,
        5,
        runtime::SessionExecutionPolicyOrigin::SurfaceCommand,
    );
    control.replace(next.clone()).unwrap();
    assert_eq!(
        service.effective_session_execution_policy("policy-handle"),
        next
    );
}
