use super::*;

#[test]
fn composition_refactor_preserves_default_cli_model_alias() {
    assert_eq!(DEFAULT_MODEL_ALIAS, "main");
}
