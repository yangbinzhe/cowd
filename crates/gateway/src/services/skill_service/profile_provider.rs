use std::path::Path;

use harness_contract::skill::SkillCapabilityProfile;
use skill::{profile_skill_package, SkillRegistry};

pub(crate) fn runtime_skill_profiles_for_workspace(
    workspace_root: &Path,
) -> Vec<SkillCapabilityProfile> {
    let registry = SkillRegistry::discover(workspace_root);
    let skills = match registry.list() {
        Ok(skills) => skills,
        Err(error) => {
            tracing::debug!(
                %error,
                workspace_root = %workspace_root.display(),
                "runtime skill profile discovery skipped"
            );
            return Vec::new();
        }
    };

    skills
        .into_iter()
        .filter(|skill| skill.shadowed_by.is_none())
        .filter_map(|skill| {
            let root = if skill.path.is_file() {
                skill
                    .path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .to_path_buf()
            } else {
                skill.path.clone()
            };
            match profile_skill_package(&root, &skill.name, None) {
                Ok(profile) => Some(profile),
                Err(error) => {
                    tracing::debug!(
                        %error,
                        skill = %skill.name,
                        path = %root.display(),
                        "runtime skill profile skipped"
                    );
                    None
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::skill::SkillAdapterKind;

    struct TempWorkspace {
        root: std::path::PathBuf,
    }

    impl TempWorkspace {
        fn new(label: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "cowd-runtime-skill-{label}-{}",
                uuid::Uuid::new_v4()
            ));
            std::fs::create_dir_all(&root).expect("temp workspace should be created");
            Self { root }
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn runtime_skill_profile_provider_uses_workspace_registry() {
        let temp = TempWorkspace::new("profile-provider");
        let skill_root = temp
            .root
            .join(".cowd")
            .join("skills")
            .join("release-review");
        std::fs::create_dir_all(&skill_root).expect("skill root should be created");
        std::fs::write(
            skill_root.join("SKILL.md"),
            "---\nname: Release Review\ndescription: Review release plans.\ntags: [release, review]\n---\n\nReview release evidence.",
        )
        .expect("skill should be written");

        let profiles = runtime_skill_profiles_for_workspace(&temp.root);

        let profile = profiles
            .iter()
            .find(|profile| profile.skill_id == "release-review")
            .expect("workspace skill profile should be discovered");
        assert_eq!(profile.name, "Release Review");
        assert!(profile.adapters.contains(&SkillAdapterKind::PromptOnly));
        assert!(profile
            .entrypoints
            .iter()
            .any(|entrypoint| entrypoint.path == "SKILL.md"));
    }
}
