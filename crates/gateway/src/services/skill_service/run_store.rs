use std::path::{Path, PathBuf};

use skill::SkillRunRecord;

use super::SkillServiceError;

const SKILL_RUNS_DIR: &str = "skill";
const SKILL_RUNS_FILE: &str = "runs.jsonl";

pub(super) fn load_runs(config_home: &Path) -> Result<Vec<SkillRunRecord>, SkillServiceError> {
    let path = runs_path(config_home);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = std::fs::read_to_string(&path)
        .map_err(|error| SkillServiceError::Internal(error.to_string()))?;
    let mut records = Vec::new();
    for (line_index, line) in raw.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let record = serde_json::from_str::<SkillRunRecord>(trimmed).map_err(|error| {
            SkillServiceError::Internal(format!(
                "invalid skill run record at {}:{}: {error}",
                path.display(),
                line_index + 1
            ))
        })?;
        records.push(record);
    }
    records.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
    Ok(records)
}

pub(super) fn find_run(
    config_home: &Path,
    run_id: &str,
) -> Result<SkillRunRecord, SkillServiceError> {
    load_runs(config_home)?
        .into_iter()
        .find(|record| record.run_id == run_id)
        .ok_or_else(|| SkillServiceError::NotFound("skill run not found".to_string()))
}

pub(super) fn append_run(
    config_home: &Path,
    record: &SkillRunRecord,
) -> Result<(), SkillServiceError> {
    let path = runs_path(config_home);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| SkillServiceError::Internal(error.to_string()))?;
    }
    let encoded = serde_json::to_string(record)
        .map_err(|error| SkillServiceError::Internal(error.to_string()))?;
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|error| SkillServiceError::Internal(error.to_string()))?;
    writeln!(file, "{encoded}").map_err(|error| SkillServiceError::Internal(error.to_string()))
}

fn runs_path(config_home: &Path) -> PathBuf {
    config_home.join(SKILL_RUNS_DIR).join(SKILL_RUNS_FILE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use skill::{SkillActionKind, SkillRunStatus};

    #[test]
    fn run_store_round_trips_jsonl_records() {
        let root =
            std::env::temp_dir().join(format!("cowd-skill-run-store-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let record = SkillRunRecord {
            run_id: "skillrun-test".to_string(),
            skill_id: "demo".to_string(),
            action: SkillActionKind::Validate,
            status: SkillRunStatus::Succeeded,
            created_at: "2026-06-28T00:00:00Z".to_string(),
            updated_at: "2026-06-28T00:00:01Z".to_string(),
            session_id: None,
            inspection: None,
            plan: None,
            receipt: None,
            error: None,
        };

        append_run(&root, &record).expect("record should append");
        let loaded = find_run(&root, "skillrun-test").expect("record should load");

        assert_eq!(loaded.skill_id, "demo");
        assert_eq!(loaded.status, SkillRunStatus::Succeeded);
        let _ = std::fs::remove_dir_all(root);
    }
}
