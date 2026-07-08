#![allow(dead_code)]

#[derive(Debug, Clone, Copy)]
struct TuiActionCoverage {
    action_id: &'static str,
    gateway_route: &'static str,
    client_method: &'static str,
    panel_key: &'static str,
    state_dispatch: &'static str,
    receipt_marker: &'static str,
}

const REQUIRED_ACTIONS: &[TuiActionCoverage] = &[
    TuiActionCoverage {
        action_id: "surface.health_check",
        gateway_route: "/api/surfaces/:id/health-check",
        client_method: "surface_health_check",
        panel_key: "h health",
        state_dispatch: "surface_health_check",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "surface.start",
        gateway_route: "/api/surfaces/:id/start",
        client_method: "surface_start",
        panel_key: "s start",
        state_dispatch: "surface_start",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "surface.stop",
        gateway_route: "/api/surfaces/:id/stop",
        client_method: "surface_stop",
        panel_key: "x stop",
        state_dispatch: "surface_stop",
        receipt_marker: "require_confirmation",
    },
    TuiActionCoverage {
        action_id: "surface.restart",
        gateway_route: "/api/surfaces/:id/restart",
        client_method: "surface_restart",
        panel_key: "r restart",
        state_dispatch: "surface_restart",
        receipt_marker: "require_confirmation",
    },
    TuiActionCoverage {
        action_id: "surface.repair",
        gateway_route: "/api/surfaces/:id/repair",
        client_method: "surface_repair",
        panel_key: "R repair",
        state_dispatch: "surface_repair",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "surface.send",
        gateway_route: "/api/surfaces/:id/send",
        client_method: "surface_send",
        panel_key: "m send",
        state_dispatch: "surface_send",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "surface.action",
        gateway_route: "/api/surfaces/:id/action",
        client_method: "surface_action",
        panel_key: "a action",
        state_dispatch: "surface_action",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "surface.inbox",
        gateway_route: "/api/surfaces/:id/inbox",
        client_method: "surface_inbox",
        panel_key: "i inbox",
        state_dispatch: "surface_inbox",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "surface.outbox",
        gateway_route: "/api/surfaces/:id/outbox",
        client_method: "surface_outbox",
        panel_key: "o outbox",
        state_dispatch: "surface_outbox",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "surface.messages",
        gateway_route: "/api/surfaces/:id/messages",
        client_method: "surface_messages",
        panel_key: "g ledger",
        state_dispatch: "surface_messages",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "surface.messages.archive",
        gateway_route: "/api/surfaces/:id/messages/archive",
        client_method: "surface_archive_messages",
        panel_key: "A archive",
        state_dispatch: "surface_archive_messages",
        receipt_marker: "require_confirmation",
    },
    TuiActionCoverage {
        action_id: "surface.messages.purge_archived_events",
        gateway_route: "/api/surfaces/:id/messages/purge-archived-events",
        client_method: "surface_purge_archived_events",
        panel_key: "P purge",
        state_dispatch: "surface_purge_archived_events",
        receipt_marker: "require_confirmation",
    },
    TuiActionCoverage {
        action_id: "surface.deliveries",
        gateway_route: "/api/surfaces/:id/deliveries",
        client_method: "surface_deliveries",
        panel_key: "v deliveries",
        state_dispatch: "surface_deliveries",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "surface.inbox.replay",
        gateway_route: "/api/surfaces/:id/inbox/:message_id/replay",
        client_method: "surface_replay_inbox",
        panel_key: "p replay",
        state_dispatch: "surface_replay_inbox",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "surface.outbox.retry",
        gateway_route: "/api/surfaces/:id/outbox/:delivery_id/retry",
        client_method: "surface_retry_outbox",
        panel_key: "d retry",
        state_dispatch: "surface_retry_outbox",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "surface.outbox.dead_letter",
        gateway_route: "/api/surfaces/:id/outbox/:delivery_id/dead-letter",
        client_method: "surface_dead_letter_outbox",
        panel_key: "D dlq",
        state_dispatch: "surface_dead_letter_outbox",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "skill.validate",
        gateway_route: "/api/skills/:id/actions/validate",
        client_method: "skill_action",
        panel_key: "v validate",
        state_dispatch: "handle_skills_panel_action",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "skill.plan",
        gateway_route: "/api/skills/:id/actions/plan",
        client_method: "skill_action",
        panel_key: "p plan",
        state_dispatch: "handle_skills_panel_action",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "skill.run",
        gateway_route: "/api/skills/:id/actions/run",
        client_method: "skill_action",
        panel_key: "r run",
        state_dispatch: "handle_skills_panel_action",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "agent.input",
        gateway_route: "/api/runtime/agents/:id/input",
        client_method: "runtime_agent_input",
        panel_key: "i input",
        state_dispatch: "runtime_agent_input",
        receipt_marker: "AgentTeamPanel",
    },
    TuiActionCoverage {
        action_id: "agent.interrupt",
        gateway_route: "/api/runtime/agents/:id/interrupt",
        client_method: "runtime_agent_interrupt",
        panel_key: "! interrupt",
        state_dispatch: "runtime_agent_interrupt",
        receipt_marker: "AgentTeamPanel",
    },
    TuiActionCoverage {
        action_id: "agent.shutdown",
        gateway_route: "/api/runtime/agents/:id/shutdown",
        client_method: "runtime_agent_shutdown",
        panel_key: "X shutdown",
        state_dispatch: "runtime_agent_shutdown",
        receipt_marker: "AgentTeamPanel",
    },
    TuiActionCoverage {
        action_id: "mission.session_command.consume",
        gateway_route: "/api/mission/sessions/:id/inbox/:command_id/consume",
        client_method: "consume_mission_session_command",
        panel_key: "c consume",
        state_dispatch: "consume_mission_session_command",
        receipt_marker: "GatewayPanel",
    },
    TuiActionCoverage {
        action_id: "mission.session_command.cancel",
        gateway_route: "/api/mission/sessions/:id/inbox/:command_id/cancel",
        client_method: "cancel_mission_session_command",
        panel_key: "C cancel",
        state_dispatch: "cancel_mission_session_command",
        receipt_marker: "GatewayPanel",
    },
    TuiActionCoverage {
        action_id: "mission.session_command.retry",
        gateway_route: "/api/mission/sessions/:id/inbox/:command_id/retry",
        client_method: "retry_mission_session_command",
        panel_key: "y retry",
        state_dispatch: "retry_mission_session_command",
        receipt_marker: "GatewayPanel",
    },
    TuiActionCoverage {
        action_id: "mission.team.tick",
        gateway_route: "/api/mission/control/stewards/scheduler",
        client_method: "tick_mission_steward_scheduler",
        panel_key: "t steward tick",
        state_dispatch: "tick_mission_steward_scheduler",
        receipt_marker: "GatewayPanel",
    },
    TuiActionCoverage {
        action_id: "harness_eval.latest",
        gateway_route: "/api/harness-eval/reports/latest",
        client_method: "harness_eval_latest_report",
        panel_key: "e eval",
        state_dispatch: "harness_eval_latest_report",
        receipt_marker: "record_harness_eval_latest",
    },
    TuiActionCoverage {
        action_id: "harness_eval.run_smoke",
        gateway_route: "/api/harness-eval/runs",
        client_method: "harness_eval_run_smoke",
        panel_key: "E smoke",
        state_dispatch: "harness_eval_run_smoke",
        receipt_marker: "record_action_result",
    },
    TuiActionCoverage {
        action_id: "evolution.overview",
        gateway_route: "/api/evolution/diagnoses",
        client_method: "evolution_diagnoses",
        panel_key: "v evolution",
        state_dispatch: "evolution_diagnoses",
        receipt_marker: "record_evolution_overview",
    },
];

pub fn action_coverage_summary() -> Vec<&'static str> {
    REQUIRED_ACTIONS
        .iter()
        .map(|action| action.action_id)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const GATEWAY_CLIENT: &str = include_str!("../gateway/gateway_client.rs");
    const SURFACE_PANEL: &str = include_str!("../components/surface_panel.rs");
    const SKILLS_PANEL: &str = include_str!("../components/skills_panel.rs");
    const GATEWAY_PANEL: &str = include_str!("../components/gateway_panel.rs");
    const AGENT_TEAM_PANEL: &str = include_str!("../components/agent_team_panel.rs");
    const STATE: &str = include_str!("state.rs");
    const SURFACE_ROUTES: &str = include_str!("../../../gateway/src/api_routes/surface_routes.rs");
    const SKILL_ROUTES: &str = include_str!("../../../gateway/src/api_routes/skill_routes.rs");
    const RUNTIME_ROUTES: &str = include_str!("../../../gateway/src/api_routes/runtime_routes.rs");
    const MISSION_ROUTES: &str = include_str!("../../../gateway/src/api_routes/mission_routes.rs");
    const HARNESS_EVAL_ROUTES: &str =
        include_str!("../../../gateway/src/api_routes/harness_eval_routes.rs");
    const EVOLUTION_ROUTES: &str =
        include_str!("../../../gateway/src/api_routes/evolution_routes.rs");

    #[test]
    fn action_coverage_has_expected_core_actions() {
        let ids = action_coverage_summary();

        assert!(ids.contains(&"surface.start"));
        assert!(ids.contains(&"surface.stop"));
        assert!(ids.contains(&"surface.outbox.retry"));
        assert!(ids.contains(&"surface.messages.archive"));
        assert!(ids.contains(&"surface.messages.purge_archived_events"));
        assert!(ids.contains(&"skill.validate"));
        assert!(ids.contains(&"skill.run"));
        assert!(ids.contains(&"mission.session_command.consume"));
        assert!(ids.contains(&"agent.interrupt"));
        assert!(ids.contains(&"harness_eval.run_smoke"));
        assert!(ids.contains(&"evolution.overview"));
        assert!(ids.len() >= 23);
    }

    #[test]
    fn action_coverage_links_routes_client_panels_and_state_dispatch() {
        let route_sources = [
            SURFACE_ROUTES,
            SKILL_ROUTES,
            RUNTIME_ROUTES,
            MISSION_ROUTES,
            HARNESS_EVAL_ROUTES,
            EVOLUTION_ROUTES,
        ]
        .join("\n");
        let panel_sources =
            [SURFACE_PANEL, SKILLS_PANEL, GATEWAY_PANEL, AGENT_TEAM_PANEL].join("\n");
        for action in REQUIRED_ACTIONS {
            let route_fragments = action
                .gateway_route
                .split('/')
                .filter(|part| !part.is_empty() && !part.starts_with(':'))
                .collect::<Vec<_>>();
            assert!(
                route_fragments
                    .iter()
                    .all(|fragment| route_sources.contains(fragment))
                    || action.gateway_route.contains("/runtime/agents/")
                    || action.gateway_route.contains("/mission/"),
                "{} route missing: {}",
                action.action_id,
                action.gateway_route
            );
            assert!(
                GATEWAY_CLIENT.contains(action.client_method),
                "{} client method missing: {}",
                action.action_id,
                action.client_method
            );
            assert!(
                panel_sources.contains(action.panel_key)
                    || STATE.contains(action.panel_key)
                    || action.panel_key.starts_with("agent ")
                    || action.panel_key.starts_with("mission "),
                "{} panel key/help missing: {}",
                action.action_id,
                action.panel_key
            );
            assert!(
                STATE.contains(action.state_dispatch),
                "{} state dispatch missing: {}",
                action.action_id,
                action.state_dispatch
            );
            assert!(
                panel_sources.contains(action.receipt_marker)
                    || STATE.contains(action.receipt_marker),
                "{} receipt marker missing: {}",
                action.action_id,
                action.receipt_marker
            );
        }
    }

    #[test]
    fn harness_eval_action_coverage_links_gateway_routes_and_tui_dispatch() {
        let route_sources = HARNESS_EVAL_ROUTES;
        let client_source = GATEWAY_CLIENT;
        let panel_source = GATEWAY_PANEL;
        let state_source = STATE;

        for action in REQUIRED_ACTIONS
            .iter()
            .filter(|action| action.action_id.starts_with("harness_eval."))
        {
            let route_fragments = action
                .gateway_route
                .split('/')
                .filter(|part| !part.is_empty() && !part.starts_with(':'))
                .collect::<Vec<_>>();
            assert!(
                route_fragments
                    .iter()
                    .all(|fragment| route_sources.contains(fragment)),
                "{} route missing: {}",
                action.action_id,
                action.gateway_route
            );
            assert!(
                client_source.contains(action.client_method),
                "{} client method missing: {}",
                action.action_id,
                action.client_method
            );
            assert!(
                panel_source.contains(action.panel_key),
                "{} panel key hint missing: {}",
                action.action_id,
                action.panel_key
            );
            assert!(
                state_source.contains(action.state_dispatch),
                "{} state dispatch missing: {}",
                action.action_id,
                action.state_dispatch
            );
            assert!(
                panel_source.contains(action.receipt_marker),
                "{} receipt marker missing: {}",
                action.action_id,
                action.receipt_marker
            );
        }
    }
}
