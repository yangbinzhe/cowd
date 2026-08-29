use super::*;

#[test]
fn managed_process_policy_preserves_operator_allocator_override() {
    assert_eq!(gateway_allocator_arena_limit(None), Some("2"));
    assert_eq!(
        gateway_allocator_arena_limit(Some(std::ffi::OsString::from("8"))),
        None
    );
}
