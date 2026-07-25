#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkbenchDomain {
    Process,
    Workspace,
    Runtime,
    Reality,
    SurfaceApp,
    Config,
    Diagnostics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PanelDefinition {
    pub id: &'static str,
    pub label: &'static str,
    pub compact_label: &'static str,
    pub domain: WorkbenchDomain,
    pub sidebar_index: Option<usize>,
    pub aliases: &'static [&'static str],
}

pub const PANEL_RUNTIME: PanelDefinition = PanelDefinition {
    id: "runtime",
    label: "Runtime",
    compact_label: "Run",
    domain: WorkbenchDomain::Runtime,
    sidebar_index: Some(0),
    aliases: &["runtime"],
};

pub const PANEL_TOOLS: PanelDefinition = PanelDefinition {
    id: "tools",
    label: "Tools",
    compact_label: "Tool",
    domain: WorkbenchDomain::Runtime,
    sidebar_index: Some(1),
    aliases: &["tools", "toolops", "tool-ops"],
};

pub const PANEL_CHANGES: PanelDefinition = PanelDefinition {
    id: "changes",
    label: "Changes",
    compact_label: "Chg",
    domain: WorkbenchDomain::Workspace,
    sidebar_index: Some(2),
    aliases: &["changes", "diffs"],
};

pub const PANEL_GOALS: PanelDefinition = PanelDefinition {
    id: "goals",
    label: "Goals",
    compact_label: "Goal",
    domain: WorkbenchDomain::Runtime,
    sidebar_index: Some(3),
    aliases: &["goals", "tasks"],
};

pub const PANEL_APPROVALS: PanelDefinition = PanelDefinition {
    id: "approvals",
    label: "Approvals",
    compact_label: "Appr",
    domain: WorkbenchDomain::Runtime,
    sidebar_index: Some(4),
    aliases: &["approvals", "approve"],
};

pub const PANEL_TODO: PanelDefinition = PanelDefinition {
    id: "todo",
    label: "Todo",
    compact_label: "Todo",
    domain: WorkbenchDomain::Process,
    sidebar_index: Some(5),
    aliases: &["todo"],
};

pub const PANEL_FILES: PanelDefinition = PanelDefinition {
    id: "files",
    label: "Files",
    compact_label: "File",
    domain: WorkbenchDomain::Workspace,
    sidebar_index: Some(6),
    aliases: &["files", "workspace"],
};

pub const PANEL_SESSIONS: PanelDefinition = PanelDefinition {
    id: "sessions",
    label: "Sessions",
    compact_label: "Sess",
    domain: WorkbenchDomain::Runtime,
    sidebar_index: Some(7),
    aliases: &["sessions", "session", "resume"],
};

pub const PANEL_SURFACES: PanelDefinition = PanelDefinition {
    id: "surfaces",
    label: "Surfaces",
    compact_label: "Surf",
    domain: WorkbenchDomain::SurfaceApp,
    sidebar_index: Some(8),
    aliases: &["surfaces", "surface"],
};

pub const PANEL_GATEWAY: PanelDefinition = PanelDefinition {
    id: "gateway",
    label: "Gateway",
    compact_label: "Gate",
    domain: WorkbenchDomain::Diagnostics,
    sidebar_index: Some(10),
    aliases: &["gateway", "diagnostics", "doctor"],
};

pub const PANEL_APPS: PanelDefinition = PanelDefinition {
    id: "apps",
    label: "Apps",
    compact_label: "Apps",
    domain: WorkbenchDomain::SurfaceApp,
    sidebar_index: Some(9),
    aliases: &["apps", "app"],
};

pub const PANEL_CONFIG: PanelDefinition = PanelDefinition {
    id: "config",
    label: "Config",
    compact_label: "Cfg",
    domain: WorkbenchDomain::Config,
    sidebar_index: None,
    aliases: &["config", "settings", "providers", "model"],
};

pub const PANEL_REALITY: PanelDefinition = PanelDefinition {
    id: "reality",
    label: "Reality",
    compact_label: "Real",
    domain: WorkbenchDomain::Reality,
    sidebar_index: None,
    aliases: &["reality", "memory", "matrix", "facts", "fact-flow"],
};

pub const PANELS: &[PanelDefinition] = &[
    PANEL_RUNTIME,
    PANEL_TOOLS,
    PANEL_CHANGES,
    PANEL_GOALS,
    PANEL_APPROVALS,
    PANEL_TODO,
    PANEL_FILES,
    PANEL_SESSIONS,
    PANEL_SURFACES,
    PANEL_APPS,
    PANEL_GATEWAY,
    PANEL_CONFIG,
    PANEL_REALITY,
];

pub fn sidebar_panels() -> Vec<PanelDefinition> {
    let mut panels = PANELS
        .iter()
        .copied()
        .filter(|panel| panel.sidebar_index.is_some())
        .collect::<Vec<_>>();
    panels.sort_by_key(|panel| panel.sidebar_index.unwrap_or(usize::MAX));
    panels
}

pub fn sidebar_count() -> usize {
    sidebar_panels().len()
}

pub fn sidebar_labels(width: u16) -> Vec<&'static str> {
    sidebar_panels()
        .iter()
        .map(|panel| {
            if width < 96 {
                panel.compact_label
            } else {
                panel.label
            }
        })
        .collect()
}

pub fn find_by_alias(alias: &str) -> Option<PanelDefinition> {
    let normalized = alias.trim().trim_start_matches('/').to_ascii_lowercase();
    PANELS.iter().copied().find(|panel| {
        panel.id == normalized
            || panel
                .aliases
                .iter()
                .any(|candidate| *candidate == normalized)
    })
}

pub fn label_for_index(index: usize) -> &'static str {
    sidebar_panels()
        .into_iter()
        .find(|panel| panel.sidebar_index == Some(index))
        .map(|panel| panel.label)
        .unwrap_or("Panel")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sidebar_registry_has_stable_order() {
        let labels = sidebar_labels(120);
        assert_eq!(
            labels,
            vec![
                "Runtime",
                "Tools",
                "Changes",
                "Goals",
                "Approvals",
                "Todo",
                "Files",
                "Sessions",
                "Surfaces",
                "Apps",
                "Gateway"
            ]
        );
    }

    #[test]
    fn aliases_resolve_domains() {
        assert_eq!(find_by_alias("workspace").unwrap().id, "files");
        assert_eq!(
            find_by_alias("/config").unwrap().domain,
            WorkbenchDomain::Config
        );
        assert_eq!(
            find_by_alias("matrix").unwrap().domain,
            WorkbenchDomain::Reality
        );
        assert_eq!(find_by_alias("/apps").unwrap().id, "apps");
    }
}
