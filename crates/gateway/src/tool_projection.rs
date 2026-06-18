pub(crate) struct TuiToolRegistryAdapter {
    _inner: tools::GlobalToolRegistry,
}

impl TuiToolRegistryAdapter {
    pub(crate) fn new(inner: tools::GlobalToolRegistry) -> Self {
        Self { _inner: inner }
    }
}

impl tui::app::ToolRegistry for TuiToolRegistryAdapter {
    fn enable_tool(&self, name: &str) {
        // GlobalToolRegistry is read-only at this layer; TUI enabled/disabled
        // state is tracked by panel entries. Validation happens when commands run.
        let _ = name;
    }

    fn disable_tool(&self, name: &str) {
        let _ = name;
    }
}
