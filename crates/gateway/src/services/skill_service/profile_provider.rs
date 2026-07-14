use std::path::Path;

use harness_contract::skill::{SkillAdapterKind, SkillCapabilityProfile};
use skill::{profile_skill_package, SkillRegistry};

#[derive(Debug, Default)]
pub(crate) struct RuntimeSkillAssets {
    pub profiles: Vec<SkillCapabilityProfile>,
    pub prompt_assets: Vec<runtime::RuntimeSkillPromptAsset>,
}

pub(crate) fn runtime_skill_profiles_for_workspace(
    workspace_root: &Path,
) -> Vec<SkillCapabilityProfile> {
    runtime_skill_assets_for_workspace(workspace_root).profiles
}

/// Gateway owns package discovery and inspection. It hands Runtime a bounded
/// PromptOnly asset set so Runtime can select and inject one asset without
/// acquiring a dependency on the open Skill registry or package filesystem.
pub(crate) fn runtime_skill_assets_for_workspace(workspace_root: &Path) -> RuntimeSkillAssets {
    let registry = SkillRegistry::discover(workspace_root);
    let skills = match registry.list() {
        Ok(skills) => skills,
        Err(error) => {
            tracing::debug!(
                %error,
                workspace_root = %workspace_root.display(),
                "runtime skill profile discovery skipped"
            );
            return RuntimeSkillAssets::default();
        }
    };

    let mut assets = RuntimeSkillAssets::default();
    for skill in skills
        .into_iter()
        .filter(|skill| skill.shadowed_by.is_none())
    {
        let root = if skill.path.is_file() {
            skill
                .path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf()
        } else {
            skill.path.clone()
        };
        let profile = match profile_skill_package(&root, &skill.name, None) {
            Ok(profile) => profile,
            Err(error) => {
                tracing::debug!(
                    %error,
                    skill = %skill.name,
                    path = %root.display(),
                    "runtime skill profile skipped"
                );
                continue;
            }
        };
        if let Some(prompt_asset) = prompt_asset_for_profile(&root, &profile) {
            assets.prompt_assets.push(prompt_asset);
        }
        assets.profiles.push(profile);
    }
    assets
}

fn prompt_asset_for_profile(
    root: &Path,
    profile: &SkillCapabilityProfile,
) -> Option<runtime::RuntimeSkillPromptAsset> {
    const MAX_PROMPT_ASSET_CHARS: usize = 48_000;
    let entrypoint = profile
        .entrypoints
        .iter()
        .find(|entrypoint| entrypoint.adapter == SkillAdapterKind::PromptOnly)?;
    let root = root.canonicalize().ok()?;
    let path = root.join(&entrypoint.path).canonicalize().ok()?;
    if !path.starts_with(&root) || !path.is_file() {
        tracing::warn!(
            skill = %profile.skill_id,
            path = %path.display(),
            "runtime skill prompt asset escaped package root or is not a file"
        );
        return None;
    }
    let content = std::fs::read_to_string(&path).ok()?;
    let content = content
        .chars()
        .take(MAX_PROMPT_ASSET_CHARS)
        .collect::<String>();
    if content.trim().is_empty() {
        return None;
    }
    Some(runtime::RuntimeSkillPromptAsset {
        skill_id: profile.skill_id.clone(),
        version: profile.version.clone(),
        content,
        source_ref: format!("skill://{}/{}", profile.skill_id, entrypoint.path),
    })
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

    #[test]
    fn runtime_skill_assets_include_bounded_prompt_only_instruction() {
        let temp = TempWorkspace::new("prompt-assets");
        let skill_root = temp
            .root
            .join(".cowd")
            .join("skills")
            .join("release-review");
        std::fs::create_dir_all(&skill_root).expect("skill root should be created");
        std::fs::write(
            skill_root.join("SKILL.md"),
            "---\nname: Release Review\ndescription: Review release plans.\n---\n\n# Release\nRequire explicit evidence.",
        )
        .expect("skill should be written");

        let assets = runtime_skill_assets_for_workspace(&temp.root);

        assert!(assets
            .profiles
            .iter()
            .any(|profile| profile.skill_id == "release-review"));
        let asset = assets
            .prompt_assets
            .iter()
            .find(|asset| asset.skill_id == "release-review")
            .expect("workspace prompt-only skill must be represented in the runtime catalog");
        assert!(asset.content.contains("Require explicit evidence."));
    }
}
