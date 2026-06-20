use std::path::PathBuf;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ServiceContext {
    pub(crate) trace_id: Option<String>,
    pub(crate) actor: Option<String>,
    pub(crate) workspace: Option<PathBuf>,
    pub(crate) session_id: Option<String>,
    pub(crate) permission_mode: Option<String>,
    pub(crate) capability: Option<String>,
}

impl ServiceContext {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_workspace(mut self, workspace: impl Into<PathBuf>) -> Self {
        self.workspace = Some(workspace.into());
        self
    }

    pub(crate) fn with_session(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }
}
