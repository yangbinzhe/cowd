//! Local skill activation router.
//!
//! The router is deliberately deterministic for CLI use and tests. Runtime can
//! later add model/embedding signals on top of this without replacing the
//! registry-backed scoring contract.

use crate::skill_registry::{SkillInfo, SkillRegistry};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillActivationCandidate {
    pub name: String,
    pub score: u32,
    pub reasons: Vec<String>,
    pub description: Option<String>,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillActivationResult {
    pub query: String,
    pub selected: Option<SkillActivationCandidate>,
    pub candidates: Vec<SkillActivationCandidate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkillRouterConfig {
    pub limit: usize,
    pub minimum_score: u32,
}

impl Default for SkillRouterConfig {
    fn default() -> Self {
        Self {
            limit: 5,
            minimum_score: 2,
        }
    }
}

pub struct SkillRouter {
    registry: SkillRegistry,
    config: SkillRouterConfig,
}

impl SkillRouter {
    #[must_use]
    pub fn new(registry: SkillRegistry) -> Self {
        Self {
            registry,
            config: SkillRouterConfig::default(),
        }
    }

    #[must_use]
    pub fn with_config(mut self, config: SkillRouterConfig) -> Self {
        self.config = config;
        self
    }

    pub fn suggest(&self, query: &str) -> std::io::Result<SkillActivationResult> {
        let query_tokens = tokenize(query);
        let query_lower = query.to_lowercase();
        let mut candidates = self
            .registry
            .list()?
            .into_iter()
            .filter(|skill| skill.shadowed_by.is_none())
            .filter_map(|skill| score_skill(skill, &query_lower, &query_tokens))
            .filter(|candidate| candidate.score >= self.config.minimum_score)
            .collect::<Vec<_>>();

        candidates.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.name.cmp(&right.name))
        });
        candidates.truncate(self.config.limit);
        let selected = candidates.first().cloned();

        Ok(SkillActivationResult {
            query: query.to_string(),
            selected,
            candidates,
        })
    }
}

fn score_skill(
    skill: SkillInfo,
    query_lower: &str,
    query_tokens: &BTreeSet<String>,
) -> Option<SkillActivationCandidate> {
    let mut score = 0;
    let mut reasons = Vec::new();
    let name_lower = skill.name.to_lowercase();

    if query_lower.contains(&name_lower) {
        score += 10;
        reasons.push("name".to_string());
    }

    let description = skill.description.clone().unwrap_or_default();
    let description_lower = description.to_lowercase();
    let description_matches = token_matches(query_tokens, &description_lower);
    if description_matches > 0 {
        score += description_matches * 3;
        reasons.push(format!("description:{description_matches}"));
    }

    let tag_matches = skill
        .tags
        .iter()
        .filter(|tag| query_tokens.contains(&tag.to_lowercase()))
        .count() as u32;
    if tag_matches > 0 {
        score += tag_matches * 5;
        reasons.push(format!("tags:{tag_matches}"));
    }

    let related_matches = skill
        .related_skills
        .iter()
        .filter(|related| query_tokens.contains(&related.to_lowercase()))
        .count() as u32;
    if related_matches > 0 {
        score += related_matches * 2;
        reasons.push(format!("related:{related_matches}"));
    }

    if score == 0 {
        return None;
    }

    Some(SkillActivationCandidate {
        name: skill.name,
        score,
        reasons,
        description: skill.description,
        path: skill.path.display().to_string(),
    })
}

fn token_matches(query_tokens: &BTreeSet<String>, haystack: &str) -> u32 {
    query_tokens
        .iter()
        .filter(|token| haystack.contains(token.as_str()))
        .count() as u32
}

fn tokenize(input: &str) -> BTreeSet<String> {
    input
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'))
        .map(str::trim)
        .filter(|token| token.len() >= 2)
        .map(str::to_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skill_registry::{SkillRegistryRoot, SkillRegistryRootKind, SkillRegistrySource};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let path = std::env::temp_dir().join(format!("cowd-skill-router-{name}-{millis}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn write(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, contents).unwrap();
    }

    #[test]
    fn router_ranks_by_tags_description_and_name() {
        let root = temp_dir("rank");
        write(
            &root.join("release").join("SKILL.md"),
            "---\nname: release\ndescription: Create changelogs and publish releases\ntags: [git, release]\n---\n# Release\n",
        );
        write(
            &root.join("debug").join("SKILL.md"),
            "---\nname: debug\ndescription: Debug failing tests\ntags: [test]\n---\n# Debug\n",
        );
        let registry = SkillRegistry::with_roots(vec![SkillRegistryRoot {
            source: SkillRegistrySource::ProjectCowd,
            path: root,
            kind: SkillRegistryRootKind::SkillsDir,
        }]);
        let result = SkillRouter::new(registry)
            .suggest("prepare git release changelog")
            .unwrap();

        let selected = result.selected.unwrap();
        assert_eq!(selected.name, "release");
        assert!(selected.score > 0);
        assert!(selected
            .reasons
            .iter()
            .any(|reason| reason.starts_with("tags")));
    }
}
