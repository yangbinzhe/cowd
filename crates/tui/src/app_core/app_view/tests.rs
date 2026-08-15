use std::collections::{BTreeMap, BTreeSet};

use cowd_app_protocol::{
    AppComponentKindV1, AppComponentV1, AppStreamFrameV1, AppViewDocumentV1,
    AppViewPatchOperationV1, AppViewPatchV1, Sha256Digest,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{backend::TestBackend, Terminal};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::{
    render_app_view, AppSubscriptionStatus, AppViewInputResult, AppViewState, AppViewStateError,
    AppViewStateLimits, AppViewStreamState,
};

fn fixture(name: &str) -> AppViewDocumentV1 {
    let source = match name {
        "reference" => include_str!("fixtures/reference_app.json"),
        "complex" => include_str!("fixtures/complex_dashboard.json"),
        _ => panic!("unknown fixture"),
    };
    serde_json::from_str(source).expect("frozen fixture must decode")
}

fn key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

fn render(document: AppViewDocumentV1, width: u16, height: u16) -> String {
    let state = AppViewState::new(document).expect("fixture state");
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).expect("test terminal");
    terminal
        .draw(|frame| render_app_view(frame, frame.area(), &state))
        .expect("render");
    let buffer = terminal.backend().buffer();
    let mut output = String::new();
    for y in 0..buffer.area().height {
        for x in 0..buffer.area().width {
            output.push_str(buffer[(x, y)].symbol());
        }
        output.push('\n');
    }
    output
}

fn digest(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

#[test]
fn public_renderer_is_reachable_and_fixtures_cover_every_component_kind() {
    let document = fixture("complex");
    let mut kinds = BTreeSet::new();
    fn collect(component: &AppComponentV1, output: &mut BTreeSet<String>) {
        output.insert(format!("{:?}", component.kind));
        for child in &component.children {
            collect(child, output);
        }
    }
    collect(&document.root, &mut kinds);
    assert_eq!(kinds.len(), 18);

    let output = render(fixture("reference"), 80, 24);
    assert!(output.contains("Protocol fixture is active"));
}

#[test]
fn viewport_goldens_are_stable_for_80_96_and_120_columns() {
    let document = fixture("complex");
    let cases = [
        (
            80,
            24,
            "69b8c98801f69f69008a9f760ed8b628b637f71af981b1cb6732732d67415bd2",
        ),
        (
            96,
            30,
            "9a5d3172c4ee6f8f4b1ffaa0bef1569b294af603c4955eb0538922b41a65cbde",
        ),
        (
            120,
            40,
            "d22a3b0e0179a649e2137f0e623723a7bc9e2a12dd248599d350871608a717b1",
        ),
    ];
    for (width, height, expected) in cases {
        let output = render(document.clone(), width, height);
        assert_eq!(digest(&output), expected, "{width}x{height}\n{output}");
        assert!(!output.contains('\u{fffd}'));
    }
}

#[test]
fn full_documents_and_patches_are_transactional_and_revision_strict() {
    let mut state = AppViewState::new(fixture("reference")).expect("state");
    let patch = AppViewPatchV1 {
        schema_version: 1,
        app_id: state.document().app_id.clone(),
        view_id: state.document().view_id.clone(),
        base_revision: "1".to_owned(),
        revision: "2".to_owned(),
        operations: vec![AppViewPatchOperationV1::Replace {
            path: "/root/children/0/properties/message".to_owned(),
            value: json!("patched"),
        }],
    };
    state.apply_patch(&patch).expect("valid patch");
    assert_eq!(state.document().revision, "2");
    assert_eq!(
        state.document().root.children[0].properties.get("message"),
        Some(&json!("patched"))
    );

    let before = state.document().clone();
    let mut bad_base = patch.clone();
    bad_base.base_revision = "1".to_owned();
    bad_base.revision = "3".to_owned();
    assert_eq!(
        state.apply_patch(&bad_base),
        Err(AppViewStateError::PatchBaseMismatch)
    );
    assert_eq!(state.document(), &before);

    let mut invalid_path = patch.clone();
    invalid_path.base_revision = "2".to_owned();
    invalid_path.revision = "3".to_owned();
    invalid_path.operations = vec![AppViewPatchOperationV1::Remove {
        path: "/root/children/999".to_owned(),
    }];
    assert!(matches!(
        state.apply_patch(&invalid_path),
        Err(AppViewStateError::InvalidPatchPath(_))
    ));
    assert_eq!(state.document(), &before);

    let mut old = fixture("reference");
    old.revision = "1".to_owned();
    assert_eq!(
        state.replace_document(old),
        Err(AppViewStateError::StaleRevision)
    );
    assert_eq!(state.document(), &before);
}

#[test]
fn unknown_patch_roots_and_identity_changes_fail_closed() {
    let mut state = AppViewState::new(fixture("reference")).expect("state");
    let unknown = AppViewPatchV1 {
        schema_version: 1,
        app_id: state.document().app_id.clone(),
        view_id: state.document().view_id.clone(),
        base_revision: "1".to_owned(),
        revision: "2".to_owned(),
        operations: vec![AppViewPatchOperationV1::Add {
            path: "/unknown".to_owned(),
            value: Value::Null,
        }],
    };
    assert!(matches!(
        state.apply_patch(&unknown),
        Err(AppViewStateError::InvalidPatchPath(_))
    ));

    let mut other = fixture("reference");
    other.app_id.0 = "other-app".to_owned();
    other.revision = "2".to_owned();
    assert_eq!(
        state.replace_document(other),
        Err(AppViewStateError::IdentityMismatch)
    );
}

#[test]
fn keyboard_navigation_form_entry_and_confirmation_generate_canonical_actions() {
    let mut form_document = fixture("complex");
    form_document.focus_component_id = Some("form".to_owned());
    let mut form_state = AppViewState::new(form_document).expect("form state");
    assert_eq!(
        form_state
            .handle_key(key(KeyCode::Char('界')))
            .expect("input"),
        AppViewInputResult::StateChanged
    );
    assert_eq!(form_state.form_value("form"), "界");
    assert_eq!(
        form_state
            .handle_key(key(KeyCode::Backspace))
            .expect("backspace"),
        AppViewInputResult::StateChanged
    );
    assert_eq!(form_state.form_value("form"), "");

    let mut action_document = fixture("complex");
    action_document.focus_component_id = Some("actions".to_owned());
    let mut action_state = AppViewState::new(action_document).expect("action state");
    action_state
        .handle_key(key(KeyCode::Down))
        .expect("select action");
    assert_eq!(
        action_state
            .handle_key(key(KeyCode::Enter))
            .expect("request confirm"),
        AppViewInputResult::ConfirmationRequired {
            action_id: "dashboard.apply".to_owned()
        }
    );
    let AppViewInputResult::Action(action) = action_state
        .handle_key(key(KeyCode::Enter))
        .expect("confirmed action")
    else {
        panic!("second enter must emit an action");
    };
    assert!(action.confirmed);
    assert_eq!(action.app_id.0, "reference-app");
    assert_eq!(action.document_revision, "10");
    assert_eq!(action.component_id, "actions");
    assert_eq!(action.action_id, "dashboard.apply");
}

#[test]
fn subscriptions_track_sequence_cursor_reconnect_and_resync_without_network() {
    let document = fixture("reference");
    let mut streams = AppViewStreamState::from_document(&document).expect("stream state");
    let digest = Sha256Digest(
        "sha256:54030ea4f653de5c1e4ebb4fd5cd236df8e5ea51136dd74f3dcd648beb8ca87d".to_owned(),
    );
    streams
        .apply_frame(&AppStreamFrameV1::Open {
            schema_version: 1,
            subscription_id: "reference.events".to_owned(),
            sequence: 0,
            schema_digest: digest.clone(),
        })
        .expect("open");
    streams
        .apply_frame(&AppStreamFrameV1::Checkpoint {
            schema_version: 1,
            subscription_id: "reference.events".to_owned(),
            sequence: 1,
            cursor: "cursor-1".to_owned(),
        })
        .expect("checkpoint");
    let live = streams
        .subscription("reference.events")
        .expect("subscription");
    assert_eq!(live.status, AppSubscriptionStatus::Live);
    assert_eq!(live.cursor.as_deref(), Some("cursor-1"));
    assert_eq!(live.next_sequence, 2);

    assert!(streams
        .apply_frame(&AppStreamFrameV1::Data {
            schema_version: 1,
            subscription_id: "reference.events".to_owned(),
            sequence: 4,
            payload: json!({}),
        })
        .is_err());
    assert_eq!(
        streams
            .subscription("reference.events")
            .expect("subscription")
            .status,
        AppSubscriptionStatus::ResyncRequired
    );
    streams.reconnect("reference.events").expect("reconnect");
    assert_eq!(
        streams
            .subscription("reference.events")
            .expect("subscription")
            .status,
        AppSubscriptionStatus::Reconnecting
    );
    streams
        .apply_frame(&AppStreamFrameV1::Open {
            schema_version: 1,
            subscription_id: "reference.events".to_owned(),
            sequence: 0,
            schema_digest: digest,
        })
        .expect("reopen");
    assert!(matches!(
        streams.apply_frame(&AppStreamFrameV1::Data {
            schema_version: 1,
            subscription_id: "reference.events".to_owned(),
            sequence: 1,
            payload: Value::String("x".repeat(1_100_000)),
        }),
        Err(AppViewStateError::ResourceLimit("stream frame bytes"))
    ));
    assert_eq!(
        streams
            .subscription("reference.events")
            .expect("subscription")
            .next_sequence,
        1
    );
}

#[test]
fn unicode_oversize_depth_and_bounded_state_fail_safely() {
    let output = render(fixture("complex"), 80, 24);
    assert!(!output.contains('\u{fffd}'));

    let mut oversized = fixture("complex");
    oversized.root.children[5]
        .properties
        .insert("markdown".to_owned(), Value::String("x".repeat(1_100_000)));
    assert!(matches!(
        AppViewState::new(oversized),
        Err(AppViewStateError::ResourceLimit("document bytes"))
    ));

    fn nested(depth: usize) -> AppComponentV1 {
        AppComponentV1 {
            component_id: format!("depth-{depth}"),
            kind: AppComponentKindV1::Stack,
            label: None,
            accessibility_label: format!("Depth {depth}"),
            properties: BTreeMap::new(),
            children: if depth == 0 {
                Vec::new()
            } else {
                vec![nested(depth - 1)]
            },
        }
    }
    let mut too_deep = fixture("reference");
    too_deep.root = nested(34);
    assert!(matches!(
        AppViewState::new(too_deep),
        Err(AppViewStateError::InvalidDocument(_))
    ));

    let limits = AppViewStateLimits {
        maximum_components: 1,
        ..AppViewStateLimits::default()
    };
    assert!(matches!(
        AppViewState::with_limits(fixture("reference"), limits),
        Err(AppViewStateError::ResourceLimit("components"))
    ));
}
