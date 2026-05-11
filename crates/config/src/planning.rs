use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};

pub fn ensure_planning_dir(cwd: &Path) -> PathBuf {
    let dir = cwd.join(".planning");
    let _ = std::fs::create_dir_all(&dir);
    dir
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanState {
    pub current_phase: String,
    pub milestones: Vec<String>,
    pub notes: String,
}

impl PlanState {
    pub fn new() -> Self { Self { current_phase: "discovery".into(), milestones: vec![], notes: String::new() } }
}

pub fn save_plan_state(dir: &Path, state: &PlanState) -> Result<(), String> {
    let file = dir.join("state.yaml");
    let yaml = serde_yaml::to_string(state).map_err(|e| format!("{e}"))?;
    std::fs::write(&file, yaml).map_err(|e| format!("{e}"))
}

pub fn load_plan_state(dir: &Path) -> Result<Option<PlanState>, String> {
    let file = dir.join("state.yaml");
    if !file.exists() { return Ok(None); }
    let yaml = std::fs::read_to_string(&file).map_err(|e| format!("{e}"))?;
    serde_yaml::from_str(&yaml).map(Some).map_err(|e| format!("{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t11_dir_created() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ensure_planning_dir(tmp.path());
        assert!(dir.exists());
    }

    #[test]
    fn t11_save_load_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ensure_planning_dir(tmp.path());
        let state = PlanState::new();
        save_plan_state(&dir, &state).unwrap();
        let loaded = load_plan_state(&dir).unwrap().unwrap();
        assert_eq!(loaded.current_phase, "discovery");
    }

    #[test]
    fn t11_empty_dir_load() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ensure_planning_dir(tmp.path());
        let loaded = load_plan_state(&dir).unwrap();
        assert!(loaded.is_none());
    }
}