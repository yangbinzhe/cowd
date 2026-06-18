use std::path::{Path, PathBuf};

use skill_service::SkillRegistry;

pub(crate) fn load_tui_skill_summaries(cwd: &Path) -> Vec<tui::SkillSummary> {
    let mut summaries = Vec::new();
    for skill in app_mfg::server_manufacturing_skill_pack() {
        let risk = if skill
            .output_actions
            .iter()
            .any(|action| action.contains("dispatch") || action.contains("escalation"))
        {
            "controlled"
        } else if skill.tools.iter().any(|tool| tool.contains("cross_plane")) {
            "governed"
        } else {
            "review"
        };
        summaries.push(tui::SkillSummary {
            name: skill.skill_id,
            description: skill.role,
            installed: true,
            category: skill.domain.clone(),
            source: "mfg".to_string(),
            status: "ready".to_string(),
            risk: risk.to_string(),
            tags: vec![skill.domain, "mfg".to_string()],
        });
    }

    match SkillRegistry::discover(cwd).list() {
        Ok(skills) => {
            for skill in skills {
                summaries.push(tui::SkillSummary {
                    name: skill.name,
                    description: skill.description.unwrap_or_default(),
                    installed: skill.shadowed_by.is_none(),
                    category: "local".to_string(),
                    source: format!("{:?}", skill.source),
                    status: if skill.shadowed_by.is_some() {
                        "shadowed".to_string()
                    } else {
                        "ready".to_string()
                    },
                    risk: "operator_review".to_string(),
                    tags: skill.tags,
                });
            }
        }
        Err(error) => {
            tracing::warn!(error = %error, "failed to load skill registry for TUI");
        }
    }
    summaries
}

pub(crate) fn load_tui_skill_summaries_for_current_dir() -> Vec<tui::SkillSummary> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    load_tui_skill_summaries(&cwd)
}
