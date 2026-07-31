//! Enhanced Skill Tools Module
//!
//! Provides 3-layer progressive disclosure for skills:
//! - Layer 1: skills_list - metadata only (name, description, category)
//! - Layer 2: skill_view - full content with linked files
//! - Layer 3: skill_read_file - read specific supporting files
//!
//! This module also provides skill management tools:
//! - skill_create - create new skills
//! - skill_edit - edit existing skills
//! - skill_delete - delete skills
//! - skill_generate - auto-generate skills from task context

use crate::skill_manifest::{
    check_prerequisites, get_config_vars, get_related_skills, get_skill_description,
    get_skill_name, get_tags, matches_platform, parse_skill_file, PrerequisitesCheck,
    SkillConfigVar,
};
use crate::{generate_skill_draft, SkillGenerationContext, SkillGenerationTrigger};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Maximum content length for skill display
const MAX_CONTENT_DISPLAY_LENGTH: usize = 8000;
const MAX_BODY_SUMMARY_LENGTH: usize = 500;

/// Skill list input
#[derive(Debug, Deserialize)]
pub struct SkillListInput {
    /// Filter by category
    pub category: Option<String>,
    /// Filter by tags
    pub tags: Option<Vec<String>>,
    /// Filter by platform
    pub platform: Option<String>,
}

/// Skill view input
#[derive(Debug, Deserialize)]
pub struct SkillViewInput {
    /// Skill name to view
    pub name: String,
    /// Optional file path to view (e.g., "references/api.md")
    pub file_path: Option<String>,
    /// Include linked files metadata
    #[serde(default)]
    pub include_files: bool,
}

/// Skill create input
#[derive(Debug, Deserialize)]
pub struct SkillCreateInput {
    /// Skill name
    pub name: String,
    /// Skill description
    pub description: String,
    /// Category for organization
    pub category: Option<String>,
    /// Tags for the skill
    pub tags: Option<Vec<String>>,
    /// Content/body of the skill
    pub content: Option<String>,
}

/// Skill edit input
#[derive(Debug, Deserialize)]
pub struct SkillEditInput {
    /// Skill name
    pub name: String,
    /// New content (replaces entire body)
    pub content: Option<String>,
    /// New description
    pub description: Option<String>,
    /// Search string for patch
    pub search: Option<String>,
    /// Replacement string for patch
    pub replace: Option<String>,
    /// File path to edit
    pub file_path: Option<String>,
}

/// Skill delete input
#[derive(Debug, Deserialize)]
pub struct SkillDeleteInput {
    /// Skill name
    pub name: String,
    /// Force delete without confirmation
    #[serde(default)]
    pub force: bool,
}

/// Skill generate input
#[derive(Debug, Deserialize)]
pub struct SkillGenerateInput {
    /// Task description for generating skill
    pub task_description: Option<String>,
    /// Tool call count (complexity indicator)
    pub tool_call_count: Option<usize>,
    /// Error count
    pub error_count: Option<usize>,
    /// User corrections count
    pub user_corrections: Option<usize>,
    /// Suggested name for the skill
    pub name: Option<String>,
}

/// Linked files in a skill
#[derive(Debug, Clone, Serialize)]
pub struct SkillLinkedFiles {
    /// Reference documents
    pub references: Vec<String>,
    /// Template files
    pub templates: Vec<String>,
    /// Script files
    pub scripts: Vec<String>,
}

/// Skill prerequisites status
#[derive(Debug, Clone, Serialize)]
pub struct SkillPrerequisitesStatus {
    /// Whether all prerequisites are met
    pub met: bool,
    /// Missing environment variables
    pub missing_env_vars: Vec<String>,
    /// Missing commands
    pub missing_commands: Vec<String>,
}

/// Skill list output (Layer 1 - Progressive Disclosure)
#[derive(Debug, Clone, Serialize)]
pub struct SkillListOutput {
    /// Success status
    pub success: bool,
    /// List of skill metadata
    pub skills: Vec<SkillMeta>,
    /// Available categories
    pub categories: Vec<String>,
    /// Available tags
    pub tags: Vec<String>,
    /// Total skill count
    pub count: usize,
    /// Hint for next step
    pub hint: String,
}

/// Skill metadata (Layer 1)
#[derive(Debug, Clone, Serialize)]
pub struct SkillMeta {
    /// Skill name
    pub name: String,
    /// Skill description
    pub description: String,
    /// Category
    pub category: Option<String>,
    /// Tags
    pub tags: Vec<String>,
    /// Whether prerequisites are met
    pub prerequisites_met: bool,
    /// Source path
    pub source: String,
}

/// Skill view output (Layer 2-3 - Progressive Disclosure)
#[derive(Debug, Clone, Serialize)]
pub struct SkillViewOutput {
    /// Success status
    pub success: bool,
    /// Skill name
    pub name: String,
    /// Skill description
    pub description: String,
    /// Tags
    pub tags: Vec<String>,
    /// Related skills
    pub related_skills: Vec<String>,
    /// Full skill content
    pub content: String,
    /// Body summary (truncated)
    pub body_summary: String,
    /// Skill path
    pub path: String,
    /// Linked files
    pub linked_files: SkillLinkedFiles,
    /// Configuration variables
    pub config_vars: Vec<SkillConfigVar>,
    /// Prerequisites status
    pub prerequisites: SkillPrerequisitesStatus,
    /// Whether setup is needed
    pub setup_needed: bool,
    /// Readiness status
    pub readiness_status: String,
    /// Platform compatibility
    pub platforms: Vec<String>,
}

/// Skill create output
#[derive(Debug, Clone, Serialize)]
pub struct SkillCreateOutput {
    /// Success status
    pub success: bool,
    /// Created skill name
    pub name: String,
    /// Path where skill was created
    pub path: String,
    /// Message
    pub message: String,
}

/// Skill edit output
#[derive(Debug, Clone, Serialize)]
pub struct SkillEditOutput {
    /// Success status
    pub success: bool,
    /// Edited skill name
    pub name: String,
    /// Path to edited skill
    pub path: String,
    /// Message
    pub message: String,
}

/// Skill delete output
#[derive(Debug, Clone, Serialize)]
pub struct SkillDeleteOutput {
    /// Success status
    pub success: bool,
    /// Deleted skill name
    pub name: String,
    /// Message
    pub message: String,
}

/// Skill generate output
#[derive(Debug, Clone, Serialize)]
pub struct SkillGenerateOutput {
    /// Success status
    pub success: bool,
    /// Generated skill name
    pub name: String,
    /// Generated content
    pub content: String,
    /// Path where skill was saved
    pub path: Option<String>,
    /// Message
    pub message: String,
}

/// Skill Manager for discovering and managing skills
pub struct SkillManager {
    /// Skill roots to search
    roots: Vec<PathBuf>,
    /// Current platform
    platform: String,
    /// Available commands in PATH
    available_commands: Vec<String>,
    /// Current environment variables
    env_vars: HashMap<String, String>,
}

impl SkillManager {
    /// Create a new skill manager
    pub fn new(roots: Vec<PathBuf>) -> Self {
        let platform = std::env::consts::OS.to_string();
        let available_commands = Self::discover_available_commands();
        let env_vars = std::env::vars().collect();

        Self {
            roots,
            platform,
            available_commands,
            env_vars,
        }
    }

    /// Discover available commands in PATH
    fn discover_available_commands() -> Vec<String> {
        std::env::var("PATH")
            .ok()
            .map(|p| {
                p.split(':')
                    .filter_map(|dir| {
                        fs::read_dir(dir).ok().map(|entries| {
                            entries
                                .filter_map(|e| e.ok())
                                .filter(|e| e.path().is_file())
                                .filter_map(|e| e.file_name().into_string().ok())
                                .collect::<Vec<String>>()
                        })
                    })
                    .flatten()
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default()
    }

    /// List all skills with metadata (Layer 1)
    pub fn list_skills(&self, input: SkillListInput) -> SkillListOutput {
        let mut all_skills = Vec::new();
        let mut categories: HashMap<String, usize> = HashMap::new();
        let mut all_tags: HashMap<String, usize> = HashMap::new();

        // Discover skills from all roots
        for root in &self.roots {
            if let Ok(entries) = fs::read_dir(root) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        let skill_md = path.join("SKILL.md");
                        if skill_md.exists() {
                            if let Ok(parsed) = parse_skill_file(&skill_md) {
                                let name = get_skill_name(&parsed)
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| {
                                        path.file_name()
                                            .unwrap_or_default()
                                            .to_string_lossy()
                                            .to_string()
                                    });
                                let description = get_skill_description(&parsed)
                                    .unwrap_or_default()
                                    .to_string();

                                // Check platform compatibility
                                if !matches_platform(&parsed, &self.platform) {
                                    continue;
                                }

                                // Check tag filter
                                if let Some(ref tags) = input.tags {
                                    let skill_tags = get_tags(&parsed);
                                    if !tags.iter().any(|t| skill_tags.contains(t)) {
                                        continue;
                                    }
                                }

                                // Check prerequisites
                                let prereqs_met = matches_prerequisites(
                                    &parsed,
                                    &self.env_vars,
                                    &self.available_commands,
                                );

                                let tags = get_tags(&parsed);
                                let category =
                                    path.file_name().map(|n| n.to_string_lossy().to_string());

                                // Update category counts
                                if let Some(ref cat) = category {
                                    *categories.entry(cat.clone()).or_insert(0) += 1;
                                }

                                // Update tag counts
                                for tag in &tags {
                                    *all_tags.entry(tag.clone()).or_insert(0) += 1;
                                }

                                all_skills.push(SkillMeta {
                                    name,
                                    description,
                                    category,
                                    tags,
                                    prerequisites_met: prereqs_met,
                                    source: skill_md.display().to_string(),
                                });
                            }
                        }
                    }
                }
            }
        }

        // Sort by name
        all_skills.sort_by(|a, b| a.name.cmp(&b.name));

        // Apply category filter if provided
        if let Some(ref cat) = input.category {
            all_skills.retain(|s| s.category.as_ref() == Some(cat));
        }

        let count = all_skills.len();

        SkillListOutput {
            success: true,
            skills: all_skills,
            categories: categories.keys().cloned().collect(),
            tags: all_tags.keys().cloned().collect(),
            count,
            hint: "Use skill_view(name) to see full content".to_string(),
        }
    }

    /// View a skill with full content (Layer 2-3)
    pub fn view_skill(&self, input: SkillViewInput) -> SkillViewOutput {
        let name = input.name.clone();

        // Search for the skill
        for root in &self.roots {
            if let Ok(entries) = fs::read_dir(root) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        let skill_md = path.join("SKILL.md");
                        if skill_md.exists() {
                            if let Ok(parsed) = parse_skill_file(&skill_md) {
                                let skill_name = get_skill_name(&parsed)
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| {
                                        path.file_name()
                                            .unwrap_or_default()
                                            .to_string_lossy()
                                            .to_string()
                                    });

                                if skill_name.eq_ignore_ascii_case(&name) {
                                    // Check if viewing a specific file
                                    if let Some(ref file_path) = input.file_path {
                                        let full_path = path.join(file_path);
                                        if let Ok(content) = fs::read_to_string(&full_path) {
                                            return SkillViewOutput {
                                                success: true,
                                                name: skill_name.to_string(),
                                                description: get_skill_description(&parsed)
                                                    .unwrap_or_default()
                                                    .to_string(),
                                                tags: get_tags(&parsed),
                                                related_skills: get_related_skills(&parsed),
                                                content,
                                                body_summary: truncate_content(
                                                    &parsed.body,
                                                    MAX_BODY_SUMMARY_LENGTH,
                                                ),
                                                path: full_path.display().to_string(),
                                                linked_files: discover_linked_files(&path),
                                                config_vars: get_config_vars(&parsed),
                                                prerequisites: get_prerequisites_status(
                                                    &parsed,
                                                    &self.env_vars,
                                                    &self.available_commands,
                                                ),
                                                setup_needed: !matches_prerequisites(
                                                    &parsed,
                                                    &self.env_vars,
                                                    &self.available_commands,
                                                ),
                                                readiness_status: if matches_prerequisites(
                                                    &parsed,
                                                    &self.env_vars,
                                                    &self.available_commands,
                                                ) {
                                                    "ready".to_string()
                                                } else {
                                                    "setup_needed".to_string()
                                                },
                                                platforms: get_platforms(&parsed),
                                            };
                                        }
                                    }

                                    // View full skill content
                                    return SkillViewOutput {
                                        success: true,
                                        name: skill_name.to_string(),
                                        description: get_skill_description(&parsed)
                                            .unwrap_or_default()
                                            .to_string(),
                                        tags: get_tags(&parsed),
                                        related_skills: get_related_skills(&parsed),
                                        content: truncate_content(
                                            &parsed.body,
                                            MAX_CONTENT_DISPLAY_LENGTH,
                                        ),
                                        body_summary: truncate_content(
                                            &parsed.body,
                                            MAX_BODY_SUMMARY_LENGTH,
                                        ),
                                        path: skill_md.display().to_string(),
                                        linked_files: discover_linked_files(&path),
                                        config_vars: get_config_vars(&parsed),
                                        prerequisites: get_prerequisites_status(
                                            &parsed,
                                            &self.env_vars,
                                            &self.available_commands,
                                        ),
                                        setup_needed: !matches_prerequisites(
                                            &parsed,
                                            &self.env_vars,
                                            &self.available_commands,
                                        ),
                                        readiness_status: if matches_prerequisites(
                                            &parsed,
                                            &self.env_vars,
                                            &self.available_commands,
                                        ) {
                                            "ready".to_string()
                                        } else {
                                            "setup_needed".to_string()
                                        },
                                        platforms: get_platforms(&parsed),
                                    };
                                }
                            }
                        }
                    }
                }
            }
        }

        // Skill not found
        SkillViewOutput {
            success: false,
            name,
            description: String::new(),
            tags: Vec::new(),
            related_skills: Vec::new(),
            content: String::new(),
            body_summary: String::new(),
            path: String::new(),
            linked_files: SkillLinkedFiles {
                references: Vec::new(),
                templates: Vec::new(),
                scripts: Vec::new(),
            },
            config_vars: Vec::new(),
            prerequisites: SkillPrerequisitesStatus {
                met: false,
                missing_env_vars: Vec::new(),
                missing_commands: Vec::new(),
            },
            setup_needed: false,
            readiness_status: "not_found".to_string(),
            platforms: Vec::new(),
        }
    }

    /// Create a new skill
    pub fn create_skill(&self, input: SkillCreateInput) -> SkillCreateOutput {
        // Skill names are directory names and invocation identifiers. Keep
        // them portable and prevent path traversal before touching a root.
        if !valid_skill_name(&input.name) {
            return SkillCreateOutput {
                success: false,
                name: input.name,
                path: String::new(),
                message:
                    "Skill name must be 1-64 lowercase letters, digits, or hyphens and start/end with a letter or digit"
                        .to_string(),
            };
        }
        if input.description.trim().is_empty() || input.description.contains(['\r', '\n']) {
            return SkillCreateOutput {
                success: false,
                name: input.name,
                path: String::new(),
                message: "Skill description must be a non-empty single line".to_string(),
            };
        }

        // Build skill content
        let content = input
            .content
            .unwrap_or_else(|| format!("# {}\n\n{}", input.name, input.description));

        // Build SKILL.md
        let mut frontmatter = format!(
            "---\nname: {}\ndescription: {}\n",
            input.name, input.description
        );

        if let Some(tags) = &input.tags {
            frontmatter.push_str("tags: [");
            frontmatter.push_str(&tags.join(", "));
            frontmatter.push_str("]\n");
        }

        frontmatter.push_str("---\n\n");
        frontmatter.push_str(&content);

        // Determine install root
        let install_root = self.roots.first().cloned().unwrap_or_else(|| {
            std::env::var("HOME")
                .ok()
                .map(PathBuf::from)
                .map(|h| h.join(".cowd/skills"))
                .unwrap_or_else(|| PathBuf::from("~/.cowd/skills"))
        });

        // Create skill directory
        let skill_dir = install_root.join(&input.name);
        if skill_dir.exists() {
            return SkillCreateOutput {
                success: false,
                name: input.name.clone(),
                path: skill_dir.display().to_string(),
                message: format!(
                    "Skill '{}' already exists at {}",
                    input.name,
                    skill_dir.display()
                ),
            };
        }

        if let Err(e) = fs::create_dir_all(&skill_dir) {
            return SkillCreateOutput {
                success: false,
                name: input.name.clone(),
                path: String::new(),
                message: format!("Failed to create skill directory: {}", e),
            };
        }

        // Write SKILL.md
        let skill_md = skill_dir.join("SKILL.md");
        if let Err(e) = fs::write(&skill_md, &frontmatter) {
            return SkillCreateOutput {
                success: false,
                name: input.name.clone(),
                path: skill_dir.display().to_string(),
                message: format!("Failed to write skill file: {}", e),
            };
        }

        SkillCreateOutput {
            success: true,
            name: input.name.clone(),
            path: skill_md.display().to_string(),
            message: format!("Skill '{}' created successfully", input.name),
        }
    }

    /// Edit an existing skill
    pub fn edit_skill(&self, input: SkillEditInput) -> SkillEditOutput {
        // Find the skill
        for root in &self.roots {
            if let Ok(entries) = fs::read_dir(root) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        let skill_md = path.join("SKILL.md");
                        if skill_md.exists() {
                            if let Ok(parsed) = parse_skill_file(&skill_md) {
                                let skill_name =
                                    get_skill_name(&parsed).unwrap_or_default().to_string();

                                if skill_name.eq_ignore_ascii_case(&input.name) {
                                    // Perform the edit
                                    let mut new_content = parsed.body.clone();

                                    if let Some(ref new_body) = input.content {
                                        new_content = new_body.clone();
                                    }

                                    if let (Some(ref search), Some(ref replace)) =
                                        (&input.search, &input.replace)
                                    {
                                        if !new_content.contains(search) {
                                            return SkillEditOutput {
                                                success: false,
                                                name: input.name.clone(),
                                                path: skill_md.display().to_string(),
                                                message: format!(
                                                    "Search string '{}' not found in skill content",
                                                    search
                                                ),
                                            };
                                        }
                                        new_content = new_content.replace(search, replace);
                                    }

                                    // Rebuild the file
                                    let new_frontmatter = if let Some(ref desc) = input.description
                                    {
                                        format!(
                                            "---\nname: {}\ndescription: {}\n---\n\n",
                                            skill_name, desc
                                        )
                                    } else {
                                        format!(
                                            "---\nname: {}\ndescription: {}\n---\n\n",
                                            skill_name,
                                            get_skill_description(&parsed).unwrap_or_default()
                                        )
                                    };

                                    let full_content =
                                        format!("{}{}", new_frontmatter, new_content);

                                    if let Err(e) = fs::write(&skill_md, &full_content) {
                                        return SkillEditOutput {
                                            success: false,
                                            name: input.name.clone(),
                                            path: skill_md.display().to_string(),
                                            message: format!("Failed to write skill file: {}", e),
                                        };
                                    }

                                    return SkillEditOutput {
                                        success: true,
                                        name: skill_name,
                                        path: skill_md.display().to_string(),
                                        message: "Skill updated successfully".to_string(),
                                    };
                                }
                            }
                        }
                    }
                }
            }
        }

        SkillEditOutput {
            success: false,
            name: input.name.clone(),
            path: String::new(),
            message: format!("Skill '{}' not found", input.name),
        }
    }

    /// Delete a skill
    pub fn delete_skill(&self, input: SkillDeleteInput) -> SkillDeleteOutput {
        let name = input.name.clone();

        for root in &self.roots {
            if let Ok(entries) = fs::read_dir(root) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        let skill_md = path.join("SKILL.md");
                        if skill_md.exists() {
                            if let Ok(parsed) = parse_skill_file(&skill_md) {
                                let skill_name =
                                    get_skill_name(&parsed).unwrap_or_default().to_string();

                                if skill_name.eq_ignore_ascii_case(&name) {
                                    if !input.force {
                                        return SkillDeleteOutput {
                                            success: false,
                                            name: name.clone(),
                                            message: format!(
                                                "Skill '{}' found at {}. Use force=true to delete.",
                                                name,
                                                path.display()
                                            ),
                                        };
                                    }

                                    if let Err(e) = fs::remove_dir_all(&path) {
                                        return SkillDeleteOutput {
                                            success: false,
                                            name,
                                            message: format!("Failed to delete skill: {}", e),
                                        };
                                    }

                                    return SkillDeleteOutput {
                                        success: true,
                                        name,
                                        message: format!("Skill deleted from {}", path.display()),
                                    };
                                }
                            }
                        }
                    }
                }
            }
        }

        SkillDeleteOutput {
            success: false,
            name,
            message: "Skill not found".to_string(),
        }
    }

    /// Generate a skill from task context with intelligent triggers
    pub fn generate_skill(&self, input: SkillGenerateInput) -> SkillGenerateOutput {
        let task_description = input.task_description.clone().unwrap_or_default();
        let tool_call_count = input.tool_call_count.unwrap_or(0);
        let error_count = input.error_count.unwrap_or(0);
        let user_corrections = input.user_corrections.unwrap_or(0);

        let context = SkillGenerationContext {
            task_description: task_description.clone(),
            tool_call_count,
            error_count,
            user_corrections,
            accepted_plan_refs: Vec::new(),
            test_report_refs: Vec::new(),
            knowledge_refs: Vec::new(),
        };
        let draft = generate_skill_draft(input.name.clone(), context);

        if !draft.should_generate {
            return SkillGenerateOutput {
                success: false,
                name: String::new(),
                content: String::new(),
                path: None,
                message: format!(
                    "Not enough context to generate a skill. Triggers: tool_calls={}, errors={}, corrections={}, description={}",
                    tool_call_count, error_count, user_corrections,
                    if task_description.is_empty() { "empty" } else { "provided" }
                ),
            };
        }
        let trigger_reason = draft
            .triggers
            .first()
            .copied()
            .unwrap_or(SkillGenerationTrigger::ExplicitTaskDescription)
            .as_str();
        let name = draft.name;
        let content = draft.content;

        // Optionally save the skill
        let path = if let Some(install_root) = self.roots.first() {
            let skill_dir = install_root.join(&name);
            let skill_md = skill_dir.join("SKILL.md");

            if !skill_dir.exists() {
                if fs::create_dir_all(&skill_dir).is_ok() {
                    if fs::write(&skill_md, &content).is_ok() {
                        Some(skill_md.display().to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        SkillGenerateOutput {
            success: true,
            name,
            content,
            path: path.clone(),
            message: format!(
                "Skill generated via trigger: {}{}",
                trigger_reason,
                if path.is_some() {
                    " and saved"
                } else {
                    " (not saved - no install root available)"
                }
            ),
        }
    }
}

fn valid_skill_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

#[cfg(test)]
mod management_contract_tests {
    use super::*;

    #[test]
    fn create_rejects_path_traversal_before_writing() {
        let output = SkillManager::new(vec![std::env::temp_dir()]).create_skill(SkillCreateInput {
            name: "../outside".to_string(),
            description: "invalid path".to_string(),
            category: None,
            tags: None,
            content: None,
        });

        assert!(!output.success);
        assert!(output.path.is_empty());
    }
}

// Helper functions

pub(crate) fn truncate_content(content: &str, max_len: usize) -> String {
    if content.len() <= max_len {
        content.to_string()
    } else {
        format!("{}...[truncated]", &content[..max_len])
    }
}

fn matches_prerequisites(
    parsed: &crate::skill_manifest::ParsedSkill,
    env_vars: &HashMap<String, String>,
    commands: &[String],
) -> bool {
    matches!(
        check_prerequisites(parsed, env_vars, commands),
        PrerequisitesCheck::Met
    )
}

fn get_prerequisites_status(
    parsed: &crate::skill_manifest::ParsedSkill,
    env_vars: &HashMap<String, String>,
    commands: &[String],
) -> SkillPrerequisitesStatus {
    match check_prerequisites(parsed, env_vars, commands) {
        PrerequisitesCheck::Met => SkillPrerequisitesStatus {
            met: true,
            missing_env_vars: Vec::new(),
            missing_commands: Vec::new(),
        },
        PrerequisitesCheck::Missing {
            env_vars: missing_env,
            commands: missing_cmds,
        } => SkillPrerequisitesStatus {
            met: false,
            missing_env_vars: missing_env,
            missing_commands: missing_cmds,
        },
    }
}

fn discover_linked_files(skill_dir: &Path) -> SkillLinkedFiles {
    let mut references = Vec::new();
    let mut templates = Vec::new();
    let mut scripts = Vec::new();

    if let Ok(entries) = fs::read_dir(skill_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.starts_with("references/")
                        || path.to_string_lossy().contains("/references/")
                    {
                        references.push(name.to_string());
                    } else if name.starts_with("templates/")
                        || path.to_string_lossy().contains("/templates/")
                    {
                        templates.push(name.to_string());
                    } else if name.ends_with(".sh")
                        || name.ends_with(".py")
                        || name.ends_with(".js")
                    {
                        scripts.push(name.to_string());
                    }
                }
            }
        }
    }

    SkillLinkedFiles {
        references,
        templates,
        scripts,
    }
}

fn get_platforms(parsed: &crate::skill_manifest::ParsedSkill) -> Vec<String> {
    if let Some(manifest) = &parsed.manifest {
        if let Some(platforms) = &manifest.platforms {
            return platforms
                .iter()
                .map(|p| format!("{:?}", p).to_lowercase())
                .collect();
        }
    }
    Vec::new()
}

mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_truncate_content() {
        let short = "Hello, world!";
        assert_eq!(truncate_content(short, 20), short);

        let long = "a".repeat(100);
        assert!(truncate_content(&long, 50).contains("[truncated]"));
    }
}
