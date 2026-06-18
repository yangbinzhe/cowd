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
        // Validate name
        if input.name.is_empty() {
            return SkillCreateOutput {
                success: false,
                name: String::new(),
                path: String::new(),
                message: "Skill name cannot be empty".to_string(),
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

        // Determine generation triggers with priority
        let triggers = analyze_generation_triggers(
            &task_description,
            tool_call_count,
            error_count,
            user_corrections,
        );

        if !triggers.should_generate {
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

        // Log the trigger that caused generation
        let trigger_reason = triggers.primary_reason();

        // Generate skill content based on context
        let name = input
            .name
            .clone()
            .unwrap_or_else(|| generate_skill_name(&task_description));

        let content = generate_skill_content(&name, &task_description, &triggers);

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

/// Analysis result for skill generation triggers
#[derive(Debug, Clone)]
struct GenerationTriggers {
    /// Whether generation should proceed
    should_generate: bool,
    /// Tool call count trigger
    tool_call_trigger: bool,
    /// Error count trigger
    error_trigger: bool,
    /// User correction trigger
    correction_trigger: bool,
    /// Task description trigger
    description_trigger: bool,
    /// Context complexity score (0-100)
    complexity_score: usize,
}

impl GenerationTriggers {
    /// Get the primary reason for generation
    fn primary_reason(&self) -> &'static str {
        if self.description_trigger {
            "task_description"
        } else if self.correction_trigger {
            "user_corrections"
        } else if self.error_trigger {
            "error_count"
        } else if self.tool_call_trigger {
            "tool_call_count"
        } else {
            "complexity"
        }
    }
}

/// Analyze whether skill generation should be triggered
fn analyze_generation_triggers(
    task_description: &str,
    tool_call_count: usize,
    error_count: usize,
    user_corrections: usize,
) -> GenerationTriggers {
    // Explicit triggers
    let tool_call_trigger = tool_call_count >= 10; // High complexity threshold
    let error_trigger = error_count >= 2; // Repeated errors suggest need for automation
    let correction_trigger = user_corrections >= 1; // User corrections indicate manual process
    let description_trigger = !task_description.is_empty();

    // Calculate complexity score based on multiple factors
    let mut complexity_score = 0;

    // Tool call complexity (max 30 points)
    complexity_score += (tool_call_count.min(30) * 30) / 30;

    // Error pattern (max 20 points)
    complexity_score += (error_count.min(4) * 20) / 4;

    // User correction rate (max 20 points)
    complexity_score += (user_corrections.min(4) * 20) / 4;

    // Description richness (max 30 points)
    if !task_description.is_empty() {
        let word_count = task_description.split_whitespace().count();
        complexity_score += (word_count.min(30) * 30) / 30;
    }

    // Determine if generation should proceed
    let should_generate = tool_call_trigger
        || error_trigger
        || correction_trigger
        || description_trigger
        || complexity_score >= 40;

    GenerationTriggers {
        should_generate,
        tool_call_trigger,
        error_trigger,
        correction_trigger,
        description_trigger,
        complexity_score,
    }
}

/// Generate comprehensive skill content from context
fn generate_skill_content(
    name: &str,
    task_description: &str,
    triggers: &GenerationTriggers,
) -> String {
    // Parse task description for key information
    let use_cases = extract_use_cases(task_description);
    let procedures = extract_procedures(task_description);
    let prerequisites = extract_prerequisites(task_description);

    // Build skill content
    let mut content = format!(
        r#"# {}

{}

## Trigger Analysis
- Complexity Score: {}/100
- Tool Calls: {}
- Errors: {}
- User Corrections: {}

## When to Use
{}
"#,
        name,
        if task_description.is_empty() {
            "Auto-generated skill from task context."
        } else {
            task_description
        },
        triggers.complexity_score,
        if triggers.tool_call_trigger {
            "high"
        } else {
            "normal"
        },
        if triggers.error_trigger {
            "repeated"
        } else {
            "none"
        },
        if triggers.correction_trigger {
            "detected"
        } else {
            "none"
        },
        use_cases,
    );

    // Add prerequisites if detected
    if !prerequisites.is_empty() {
        content.push_str("## Prerequisites\n");
        for prereq in prerequisites {
            content.push_str(&format!("- {}\n", prereq));
        }
        content.push('\n');
    }

    // Add procedures
    content.push_str("## Procedures\n");
    if procedures.is_empty() {
        content.push_str("1. Analyze the task requirements\n");
        content.push_str("2. Plan the approach\n");
        content.push_str("3. Execute the steps\n");
        content.push_str("4. Verify the results\n");
    } else {
        for (i, proc) in procedures.iter().enumerate() {
            content.push_str(&format!("{}. {}\n", i + 1, proc));
        }
    }
    content.push('\n');

    // Add tips based on triggers
    content.push_str("## Tips\n");
    if triggers.error_trigger {
        content.push_str("- Common pitfalls have been addressed in procedures\n");
    }
    if triggers.correction_trigger {
        content.push_str("- This workflow was refined based on user feedback\n");
    }
    content.push_str("- Always verify results after completion\n");
    content.push_str("- Check prerequisites before starting\n");

    content
}

/// Extract potential use cases from task description
fn extract_use_cases(description: &str) -> String {
    if description.is_empty() {
        return "When you need to automate this type of task".to_string();
    }

    let keywords = vec![
        "deploy",
        "build",
        "test",
        "create",
        "manage",
        "monitor",
        "backup",
        "restore",
        "analyze",
        "generate",
        "process",
        "convert",
        "migrate",
        "configure",
        "install",
        "setup",
    ];

    let desc_lower = description.to_lowercase();
    let words: Vec<&str> = desc_lower.split_whitespace().collect();
    let mut matched = Vec::new();

    for keyword in keywords {
        if words.iter().any(|w| w.contains(keyword)) {
            matched.push(keyword);
        }
    }

    if matched.is_empty() {
        format!(
            "When working with: {}",
            description
                .split_whitespace()
                .take(5)
                .collect::<Vec<_>>()
                .join(" ")
        )
    } else {
        format!(
            "When you need to {} (detected from context)",
            matched.join(", ")
        )
    }
}

/// Extract procedures from task description
fn extract_procedures(description: &str) -> Vec<String> {
    // Look for numbered items or step indicators in description
    let mut procedures = Vec::new();

    // Check for explicit steps
    for line in description.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with(|c: char| c.is_numeric() && trimmed.contains('.')) {
            procedures.push(
                trimmed
                    .chars()
                    .skip_while(|c| c.is_numeric() || *c == '.' || c.is_whitespace())
                    .collect(),
            );
        }
    }

    procedures
}

/// Extract prerequisites from task description
fn extract_prerequisites(description: &str) -> Vec<String> {
    let prereq_keywords = vec!["require", "need", "must have", "prerequisite"];

    let mut prerequisites = Vec::new();
    let lower = description.to_lowercase();

    for keyword in prereq_keywords {
        if lower.contains(keyword) {
            // Extract the sentence containing the keyword
            for sentence in description.split(|c: char| c == '.' || c == ';') {
                if sentence.to_lowercase().contains(keyword) {
                    let trimmed = sentence.trim();
                    if !trimmed.is_empty() && trimmed.len() < 100 {
                        prerequisites.push(trimmed.to_string());
                    }
                }
            }
        }
    }

    // Limit to 3 most relevant prerequisites
    prerequisites.truncate(3);
    prerequisites
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

pub(crate) fn generate_skill_name(description: &str) -> String {
    let words: Vec<&str> = description.split_whitespace().take(3).collect();
    let base = if words.is_empty() {
        "generated-skill".to_string()
    } else {
        words.join("-").to_lowercase()
    };

    // Add timestamp to avoid conflicts
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    format!("{}-{}", base, timestamp % 10000)
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

    #[test]
    fn test_generate_skill_name() {
        let name = generate_skill_name("Deploy to Kubernetes cluster");
        assert!(name.contains("deploy"));
        assert!(name.contains("kubernetes"));
    }
}
