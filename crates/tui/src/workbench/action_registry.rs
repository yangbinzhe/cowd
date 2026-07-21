use crate::keybind::types::Action;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionRisk {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone)]
pub struct WorkbenchAction {
    pub id: &'static str,
    pub domain: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    pub risk: ActionRisk,
    pub requires_confirmation: bool,
    pub receipt_target: &'static str,
    pub action: Action,
}

pub fn registered_actions() -> Vec<WorkbenchAction> {
    vec![
        WorkbenchAction {
            id: "workbench.open_runtime",
            domain: "runtime",
            label: "Open Runtime",
            description: "Inspect sessions, turns, agents, approvals, and context pressure",
            risk: ActionRisk::Low,
            requires_confirmation: false,
            receipt_target: "runtime_panel",
            action: Action::Execute("/runtime".into()),
        },
        WorkbenchAction {
            id: "workbench.open_workspace",
            domain: "workspace",
            label: "Open Workspace",
            description: "Browse files, resources, attachments, and changes",
            risk: ActionRisk::Low,
            requires_confirmation: false,
            receipt_target: "workspace_panel",
            action: Action::Execute("/files".into()),
        },
        WorkbenchAction {
            id: "workbench.open_reality",
            domain: "reality",
            label: "Open Reality",
            description: "Inspect context, memory, facts, and governance signals",
            risk: ActionRisk::Low,
            requires_confirmation: false,
            receipt_target: "reality_panel",
            action: Action::Execute("/memory".into()),
        },
        WorkbenchAction {
            id: "workbench.open_surfaces",
            domain: "surface",
            label: "Open Surfaces",
            description: "Manage surface health, inbox, outbox, deliveries, and actions",
            risk: ActionRisk::Low,
            requires_confirmation: false,
            receipt_target: "surface_panel",
            action: Action::Execute("/surfaces".into()),
        },
        WorkbenchAction {
            id: "workbench.open_apps",
            domain: "surface",
            label: "Open Apps",
            description: "Open a statically-linked application terminal surface",
            risk: ActionRisk::Low,
            requires_confirmation: false,
            receipt_target: "app_surface_host",
            action: Action::Execute("/apps".into()),
        },
        WorkbenchAction {
            id: "workbench.open_config",
            domain: "config",
            label: "Open Config",
            description: "Inspect effective config, providers, models, profiles, and warnings",
            risk: ActionRisk::Low,
            requires_confirmation: false,
            receipt_target: "config_panel",
            action: Action::Execute("/config".into()),
        },
        WorkbenchAction {
            id: "workbench.refresh_config_status",
            domain: "config",
            label: "Refresh Config Status",
            description:
                "Refresh effective config, providers, models, and Gateway hot-reload status",
            risk: ActionRisk::Low,
            requires_confirmation: false,
            receipt_target: "config_panel",
            action: Action::RefreshConfigStatus,
        },
        WorkbenchAction {
            id: "workbench.review_approvals",
            domain: "runtime",
            label: "Review Approvals",
            description: "Open pending approval and permission cockpit",
            risk: ActionRisk::Low,
            requires_confirmation: false,
            receipt_target: "approval_panel",
            action: Action::Execute("/approvals".into()),
        },
        WorkbenchAction {
            id: "workbench.gateway_diagnostics",
            domain: "diagnostics",
            label: "Gateway Diagnostics",
            description: "Inspect Gateway health, route manifest, and degraded reasons",
            risk: ActionRisk::Low,
            requires_confirmation: false,
            receipt_target: "gateway_panel",
            action: Action::Execute("/gateway".into()),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actions_have_operational_metadata() {
        for action in registered_actions() {
            assert!(!action.id.is_empty());
            assert!(!action.domain.is_empty());
            assert!(!action.receipt_target.is_empty());
        }
        assert!(registered_actions()
            .iter()
            .any(|action| action.id == "workbench.open_config"));
        assert!(registered_actions()
            .iter()
            .any(|action| action.id == "workbench.open_apps"));
    }
}
