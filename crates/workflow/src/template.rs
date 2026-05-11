// M7.2: WorkflowTemplate — multi-step pipeline
use serde::{Deserialize,Serialize};
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct WorkflowTemplate { pub name: String, pub steps: Vec<WorkflowStep>, pub context_mode: ContextMode }
#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct WorkflowStep { pub name: String, pub description: String, pub verify: String }
#[derive(Debug,Clone,Serialize,Deserialize)]
pub enum ContextMode { Dev, Research, Review }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn m7_workflow_template_three_steps() {
        let tmpl = WorkflowTemplate {
            name: "feature-dev".into(),
            steps: vec![
                WorkflowStep { name: "analyze".into(), description: "Analyze requirements".into(), verify: "cargo check".into() },
                WorkflowStep { name: "implement".into(), description: "Write code".into(), verify: "cargo test".into() },
                WorkflowStep { name: "review".into(), description: "Code review".into(), verify: "cargo clippy".into() },
                WorkflowStep { name: "ship".into(), description: "Merge and deploy".into(), verify: "git push".into() },
            ],
            context_mode: ContextMode::Dev,
        };
        assert_eq!(tmpl.steps.len(), 4);
        assert_eq!(tmpl.steps[0].name, "analyze");
        assert_eq!(tmpl.steps[3].name, "ship");
    }
}
