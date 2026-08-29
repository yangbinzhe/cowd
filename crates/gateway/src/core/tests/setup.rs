#[test]
fn composition_root_shares_process_lifecycle_owners() {
    let root =
        crate::composition_root::GatewayCompositionRoot::new(std::time::Duration::from_secs(1));
    assert!(std::sync::Arc::ptr_eq(
        &root.active_sessions(),
        &root.active_sessions()
    ));
    assert!(std::sync::Arc::ptr_eq(
        &root.gateway_tasks(),
        &root.gateway_tasks()
    ));
}
