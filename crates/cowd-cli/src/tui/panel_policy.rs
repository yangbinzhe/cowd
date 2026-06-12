//! Sidebar panel loading policy.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SidebarPanel {
    Runtime,
    Context,
    Changes,
    Goals,
    Approvals,
    Todo,
    Diff,
    Files,
    Sessions,
    Memory,
    Skills,
    Gateway,
}

impl SidebarPanel {
    #[must_use]
    pub fn from_tab(tab: usize) -> Option<Self> {
        match tab {
            0 => Some(Self::Runtime),
            1 => Some(Self::Context),
            2 => Some(Self::Changes),
            3 => Some(Self::Goals),
            4 => Some(Self::Approvals),
            5 => Some(Self::Todo),
            6 => Some(Self::Diff),
            7 => Some(Self::Files),
            8 => Some(Self::Sessions),
            9 => Some(Self::Memory),
            10 => Some(Self::Skills),
            11 => Some(Self::Gateway),
            _ => None,
        }
    }

    #[must_use]
    pub const fn is_hot_status_panel(self) -> bool {
        matches!(
            self,
            Self::Runtime
                | Self::Context
                | Self::Changes
                | Self::Goals
                | Self::Approvals
                | Self::Todo
        )
    }

    #[must_use]
    pub const fn requires_active_sync(self) -> bool {
        !self.is_hot_status_panel()
    }
}

#[must_use]
pub fn should_sync_panel(active: SidebarPanel, target: SidebarPanel) -> bool {
    target.is_hot_status_panel() || active == target
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_sidebar_tabs_to_panels() {
        assert_eq!(SidebarPanel::from_tab(0), Some(SidebarPanel::Runtime));
        assert_eq!(SidebarPanel::from_tab(9), Some(SidebarPanel::Memory));
        assert_eq!(SidebarPanel::from_tab(11), Some(SidebarPanel::Gateway));
        assert_eq!(SidebarPanel::from_tab(12), None);
    }

    #[test]
    fn hot_status_panels_sync_even_when_inactive() {
        assert!(should_sync_panel(
            SidebarPanel::Runtime,
            SidebarPanel::Approvals
        ));
        assert!(should_sync_panel(SidebarPanel::Files, SidebarPanel::Todo));
    }

    #[test]
    fn heavy_panels_sync_only_when_active() {
        assert!(should_sync_panel(
            SidebarPanel::Memory,
            SidebarPanel::Memory
        ));
        assert!(!should_sync_panel(
            SidebarPanel::Runtime,
            SidebarPanel::Memory
        ));
        assert!(!should_sync_panel(
            SidebarPanel::Runtime,
            SidebarPanel::Files
        ));
    }
}
