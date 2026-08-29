use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::*;

#[test]
fn reducer_fences_stale_effects_after_authority_replacement() {
    let mut state = TuiState::new("model", "session-a");
    state.install_session_authority(7);

    let applied = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&applied);
    CompletedCoreGatewayEffect::new(
        "session-a".to_owned(),
        7,
        Ok(serde_json::json!({"ok": true})),
        Box::new(move |_, _| {
            observed.fetch_add(1, Ordering::SeqCst);
        }),
    )
    .apply_if_current(&mut state);
    assert_eq!(applied.load(Ordering::SeqCst), 1);

    state.install_session_authority(8);
    let observed = Arc::clone(&applied);
    CompletedCoreGatewayEffect::new(
        "session-a".to_owned(),
        7,
        Ok(serde_json::json!({"stale": true})),
        Box::new(move |_, _| {
            observed.fetch_add(1, Ordering::SeqCst);
        }),
    )
    .apply_if_current(&mut state);
    assert_eq!(applied.load(Ordering::SeqCst), 1);
}

#[test]
fn revoke_invalidates_generation_and_clears_session_truth() {
    let mut state = TuiState::new("model", "session-a");
    state.install_session_authority(11);
    assert!(state.accepts_authority("session-a", 11));
    state.revoke_session_authority("capability lost");
    assert!(!state.accepts_authority("session-a", 11));
    assert!(state.session.authorization_revoked);
    assert!(state.session.pending_app_surface_commands.is_empty());
    assert!(state.session.pending_core_gateway_effects.is_empty());
}

#[test]
fn unchanged_session_catalog_skips_materialization_and_redraw() {
    let mut state = TuiState::new("model", "session-a");
    let catalog = serde_json::json!({
        "sessions": [{
            "id": "session-a",
            "title": "Primary",
            "status": "active",
            "updated_at": "2026-08-30T12:00:00Z",
            "message_count": 7
        }]
    });

    assert!(state.apply_gateway_session_catalog(&catalog));
    assert_eq!(state.app.shell.picker_sessions.len(), 1);
    assert_eq!(state.session.session_sidebar.len(), 1);
    let render_version = state.app.timeline.render_version;

    assert!(!state.apply_gateway_session_catalog(&catalog));
    assert_eq!(state.app.timeline.render_version, render_version);

    let changed = serde_json::json!({
        "sessions": [{
            "id": "session-a",
            "title": "Renamed",
            "status": "active",
            "updated_at": "2026-08-30T12:01:00Z",
            "message_count": 8
        }]
    });
    assert!(state.apply_gateway_session_catalog(&changed));
    assert!(state.app.timeline.render_version > render_version);
    assert_eq!(
        state.app.shell.picker_sessions[0].title.as_deref(),
        Some("Renamed")
    );
}

#[test]
fn repeated_full_refresh_avoids_at_least_fifteen_percent_of_materialization() {
    let sessions = (0..128)
        .map(|index| {
            serde_json::json!({
                "id": format!("session-{index}"),
                "title": format!("Session {index}"),
                "status": "idle",
                "updated_at": "2026-08-30T12:00:00Z",
                "message_count": index
            })
        })
        .collect::<Vec<_>>();
    let catalog = serde_json::json!({ "sessions": sessions });
    let mut state = TuiState::new("model", "session-0");
    let refreshes = 20;

    reset_session_catalog_materializations();
    for _ in 0..refreshes {
        state.apply_gateway_session_catalog(&catalog);
    }
    let candidate_materializations = session_catalog_materializations();
    let legacy_materializations = 128 * refreshes;
    let reduction_percent = 100.0 * (legacy_materializations - candidate_materializations) as f64
        / legacy_materializations as f64;

    assert_eq!(candidate_materializations, 128);
    assert!(
        reduction_percent >= 15.0,
        "full-refresh materialization reduction was {reduction_percent:.2}%"
    );
}
