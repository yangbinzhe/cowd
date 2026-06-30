use crate::{project_scope::MemoryScope, types::MemoryEntry};

#[derive(Debug, Clone, Default)]
pub struct RecallFence {
    pub session_id: Option<String>,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub agent_id: Option<String>,
}

impl RecallFence {
    pub fn allows(&self, entry: &MemoryEntry) -> bool {
        match &entry.scope {
            MemoryScope::Global => true,
            MemoryScope::Project(project) => self.project_id.as_ref() == Some(project),
            MemoryScope::Task(task) => self.task_id.as_ref() == Some(task),
            MemoryScope::Session(session) => self.session_id.as_ref() == Some(session),
            MemoryScope::Agent(agent) => self.agent_id.as_ref() == Some(agent),
        }
    }
}
