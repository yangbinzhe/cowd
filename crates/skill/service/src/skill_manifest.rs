//! Enhanced Skill Manifest Module
//!
//! Provides rich SKILL.md manifest parsing with support for:
//! - Standard fields (name, description)
//! - Version, author, license metadata
//! - Platform restrictions (macos, linux, windows)
//! - Tags and related skills
//! - Condition-based activation rules
//! - Configuration variables
//! - Prerequisites (env vars, commands)
//!
//! Manifest format example:
//! ```markdown
//! ---
//! name: my-skill
//! description: A description of the skill
//! version: 1.0.0
//! author: Hermes Agent
//! license: MIT
//! platforms: [macos, linux]
//! tags: [coding, devops]
//! related_skills: [lora, peft]
//! conditions:
//!   requires_toolsets: [mcp]
//!   fallback_for_tools: [other-skill]
//! config:
//!   - key: api.key
//!     description: API key for service
//!     default: "default-value"
//! prerequisites:
//!   env_vars: [OPENAI_API_KEY]
//!   commands: [curl, jq]
//! ---
//! # Skill Instructions
//! ...
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

/// Supported platform types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    Macos,
    Linux,
    Windows,
}

/// Condition-based activation rules
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillConditions {
    /// Hide this skill when these tools are available
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback_for_tools: Option<Vec<String>>,

    /// Only show this skill when these toolsets are available
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requires_toolsets: Option<Vec<String>>,

    /// Hide this skill when these toolsets are available
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback_for_toolsets: Option<Vec<String>>,

    /// Only show this skill when these tools are available
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requires_tools: Option<Vec<String>>,
}

/// Configuration variable declaration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillConfigVar {
    /// Configuration key (supports dot notation like `api.key`)
    pub key: String,
    /// Human-readable description
    pub description: String,
    /// Default value (optional)
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

/// Prerequisites for skill execution
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillPrerequisites {
    /// Required environment variables
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env_vars: Option<Vec<String>>,

    /// Required commands in PATH
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commands: Option<Vec<String>>,
}

/// Common Agent Skills metadata used by upstream skill packages.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillPackageMetadata {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requires: Option<SkillPackageRequires>,
}

/// External runtime requirements declared as `metadata.requires`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillPackageRequires {
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bins: Option<Vec<String>>,
}

/// Hermes-specific metadata
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillHermesMetadata {
    /// Tags for categorization and search
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,

    /// Related skills for suggestions
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_skills: Option<Vec<String>>,

    /// Configuration variables
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<Vec<SkillConfigVar>>,

    /// Conditions for activation
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<SkillConditions>,
}

/// Enhanced Skill Manifest
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillManifest {
    /// Skill name (required, ≤64 characters)
    pub name: String,

    /// Description (required, ≤1024 characters)
    pub description: String,

    /// Version string (semver recommended)
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

    /// Author name
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,

    /// License identifier (e.g., MIT, Apache-2.0)
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,

    /// Platform restrictions
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platforms: Option<Vec<Platform>>,

    /// Tags for categorization
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,

    /// Related skills
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_skills: Option<Vec<String>>,

    /// Condition-based activation rules
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<SkillConditions>,

    /// Configuration variables
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<Vec<SkillConfigVar>>,

    /// Prerequisites
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prerequisites: Option<SkillPrerequisites>,

    /// Upstream Agent Skills compatible dependency declaration.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<SkillPackageMetadata>,

    /// Hermes-specific metadata
    #[serde(rename = "hermes")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hermes_metadata: Option<SkillHermesMetadata>,
}

/// Legacy frontmatter format (name and description only)
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacyFrontmatter {
    pub name: Option<String>,
    pub description: Option<String>,
}

/// Complete parsed skill including manifest and body
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSkill {
    /// Parsed manifest (enhanced format)
    pub manifest: Option<SkillManifest>,
    /// Legacy frontmatter (for backward compatibility)
    pub legacy: LegacyFrontmatter,
    /// Skill body content (after frontmatter)
    pub body: String,
    /// Original file path
    pub source_path: Option<PathBuf>,
}

/// Parse error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum SkillParseError {
    #[error("Invalid YAML frontmatter: {0}")]
    InvalidYaml(String),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Field too long: {field} ({length} > {max})")]
    FieldTooLong {
        field: &'static str,
        length: usize,
        max: usize,
    },

    #[error("Invalid platform: {0}")]
    InvalidPlatform(String),

    #[error("IO error: {0}")]
    Io(String),
}

/// Maximum field lengths
const MAX_NAME_LENGTH: usize = 64;
const MAX_DESCRIPTION_LENGTH: usize = 1024;
const MAX_FRONTMATTER_BYTES: usize = 64 * 1024;

/// Parse a SKILL.md content string into a ParsedSkill
pub fn parse_skill_content(contents: &str) -> Result<ParsedSkill, SkillParseError> {
    // Try enhanced YAML parsing first
    let (manifest, legacy, body) = if let Some((yaml_str, body)) = extract_frontmatter(contents) {
        // Try YAML parsing
        match serde_yaml::from_str::<serde_yaml::Value>(&yaml_str) {
            Ok(yaml) => {
                let manifest = parse_enhanced_manifest(&yaml)?;
                let legacy =
                    LegacyFrontmatter {
                        name: manifest.as_ref().map(|m| m.name.clone()).or_else(|| {
                            yaml.get("name").and_then(|v| v.as_str()).map(String::from)
                        }),
                        description: manifest.as_ref().map(|m| m.description.clone()).or_else(
                            || {
                                yaml.get("description")
                                    .and_then(|v| v.as_str())
                                    .map(String::from)
                            },
                        ),
                    };
                (manifest, legacy, body.to_string())
            }
            Err(_e) => {
                // Fall back to legacy parsing
                let legacy = parse_legacy_frontmatter(&yaml_str);
                (None, legacy, body.to_string())
            }
        }
    } else {
        (None, LegacyFrontmatter::default(), contents.to_string())
    };

    Ok(ParsedSkill {
        manifest,
        legacy,
        body,
        source_path: None,
    })
}

/// Parse skill from a file path
pub fn parse_skill_file(path: &Path) -> Result<ParsedSkill, SkillParseError> {
    let contents = fs::read_to_string(path).map_err(|e| SkillParseError::Io(e.to_string()))?;
    let mut parsed = parse_skill_content(&contents)?;
    parsed.source_path = Some(path.to_path_buf());
    Ok(parsed)
}

/// Parse only the bounded YAML frontmatter needed by the discovery catalog.
///
/// Skill instructions can be large. Discovery must not read the complete body
/// before Runtime has selected the Skill for a turn.
pub fn parse_skill_file_header(path: &Path) -> Result<ParsedSkill, SkillParseError> {
    let file = fs::File::open(path).map_err(|error| SkillParseError::Io(error.to_string()))?;
    let mut lines = BufReader::new(file).lines();
    let Some(first) = lines.next() else {
        return Ok(empty_parsed_skill(path));
    };
    let first = first.map_err(|error| SkillParseError::Io(error.to_string()))?;
    if first.trim() != "---" {
        return Ok(empty_parsed_skill(path));
    }

    let mut header = String::from("---\n");
    let mut closed = false;
    for line in lines {
        let line = line.map_err(|error| SkillParseError::Io(error.to_string()))?;
        if header.len().saturating_add(line.len()).saturating_add(1) > MAX_FRONTMATTER_BYTES {
            return Err(SkillParseError::FieldTooLong {
                field: "frontmatter",
                length: header.len().saturating_add(line.len()).saturating_add(1),
                max: MAX_FRONTMATTER_BYTES,
            });
        }
        header.push_str(&line);
        header.push('\n');
        if line.trim() == "---" {
            closed = true;
            break;
        }
    }
    if !closed {
        return Err(SkillParseError::InvalidYaml(
            "unterminated YAML frontmatter".to_string(),
        ));
    }

    let mut parsed = parse_skill_content(&header)?;
    parsed.body.clear();
    parsed.source_path = Some(path.to_path_buf());
    Ok(parsed)
}

fn empty_parsed_skill(path: &Path) -> ParsedSkill {
    ParsedSkill {
        manifest: None,
        legacy: LegacyFrontmatter::default(),
        body: String::new(),
        source_path: Some(path.to_path_buf()),
    }
}

/// Extract YAML frontmatter from markdown content
fn extract_frontmatter(content: &str) -> Option<(String, String)> {
    let mut lines = content.lines();

    // Check for opening ---
    let first = lines.next()?;
    if first.trim() != "---" {
        return None;
    }

    let mut yaml_lines = Vec::new();
    let mut body_start = None;

    for (i, line) in lines.enumerate() {
        if line.trim() == "---" {
            body_start = Some(i + 1);
            break;
        }
        yaml_lines.push(line);
    }

    let yaml_str = yaml_lines.join("\n");
    let body = if let Some(start) = body_start {
        content
            .lines()
            .skip(start)
            .collect::<Vec<_>>()
            .join("\n")
            .trim_start_matches('\n')
            .to_string()
    } else {
        String::new()
    };

    Some((yaml_str, body.trim().to_string()))
}

/// Parse enhanced manifest from YAML value
fn parse_enhanced_manifest(
    yaml: &serde_yaml::Value,
) -> Result<Option<SkillManifest>, SkillParseError> {
    let obj = match yaml {
        serde_yaml::Value::Mapping(m) => m,
        _ => return Ok(None),
    };

    // Check for any enhanced fields
    let has_enhanced = obj.contains_key(serde_yaml::Value::String("hermes".into()))
        || obj.contains_key(serde_yaml::Value::String("conditions".into()))
        || obj.contains_key(serde_yaml::Value::String("config".into()))
        || obj.contains_key(serde_yaml::Value::String("prerequisites".into()))
        || obj.contains_key(serde_yaml::Value::String("metadata".into()))
        || obj.contains_key(serde_yaml::Value::String("platforms".into()))
        || obj.contains_key(serde_yaml::Value::String("version".into()))
        || obj.contains_key(serde_yaml::Value::String("tags".into()))
        || obj.contains_key(serde_yaml::Value::String("related_skills".into()));

    if !has_enhanced {
        return Ok(None);
    }

    // Parse required fields
    let name = get_required_string(obj, "name")?;
    let description = get_required_string(obj, "description")?;

    // Validate field lengths
    validate_field_length("name", &name, MAX_NAME_LENGTH)?;
    validate_field_length("description", &description, MAX_DESCRIPTION_LENGTH)?;

    // Parse optional fields
    let version = get_optional_string(obj, "version");
    let author = get_optional_string(obj, "author");
    let license = get_optional_string(obj, "license");

    // Parse platforms
    let platforms = obj
        .get(serde_yaml::Value::String("platforms".into()))
        .and_then(|v| v.as_sequence())
        .map(|seq| {
            seq.iter()
                .map(|p| {
                    let s = p
                        .as_str()
                        .ok_or_else(|| SkillParseError::InvalidPlatform(format!("{:?}", p)))?;
                    match s.to_lowercase().as_str() {
                        "macos" | "darwin" => Ok(Platform::Macos),
                        "linux" => Ok(Platform::Linux),
                        "windows" | "win32" => Ok(Platform::Windows),
                        _ => Err(SkillParseError::InvalidPlatform(s.to_string())),
                    }
                })
                .collect()
        })
        .transpose()?;

    let tags = get_string_list(obj, "tags");
    let related_skills = get_string_list(obj, "related_skills");

    // Parse conditions
    let conditions = obj
        .get(serde_yaml::Value::String("conditions".into()))
        .and_then(|v| v.as_mapping())
        .map(|m| SkillConditions {
            fallback_for_tools: get_string_list_from_map(m, "fallback_for_tools"),
            requires_toolsets: get_string_list_from_map(m, "requires_toolsets"),
            fallback_for_toolsets: get_string_list_from_map(m, "fallback_for_toolsets"),
            requires_tools: get_string_list_from_map(m, "requires_tools"),
        });

    // Parse config
    let config = obj
        .get(serde_yaml::Value::String("config".into()))
        .and_then(|v| v.as_sequence())
        .map(|seq| {
            seq.iter()
                .filter_map(|item| {
                    let m = item.as_mapping()?;
                    let key = get_required_string_from_map(m, "key")?;
                    let description = get_required_string_from_map(m, "description")?;
                    let default = get_optional_string_from_map(m, "default");
                    Some(SkillConfigVar {
                        key,
                        description,
                        default,
                    })
                })
                .collect()
        });

    // Parse prerequisites
    let prerequisites = obj
        .get(serde_yaml::Value::String("prerequisites".into()))
        .and_then(|v| v.as_mapping())
        .map(|m| SkillPrerequisites {
            env_vars: get_string_list_from_map(m, "env_vars"),
            commands: get_string_list_from_map(m, "commands"),
        });

    // Parse hermes metadata
    let hermes_metadata = obj
        .get(serde_yaml::Value::String("hermes".into()))
        .and_then(|v| v.as_mapping())
        .map(|m| SkillHermesMetadata {
            tags: get_string_list_from_map(m, "tags"),
            related_skills: get_string_list_from_map(m, "related_skills"),
            config: m
                .get(serde_yaml::Value::String("config".into()))
                .and_then(|v| v.as_sequence())
                .map(|seq| {
                    seq.iter()
                        .filter_map(|item| {
                            let m = item.as_mapping()?;
                            let key = get_required_string_from_map(m, "key")?;
                            let description = get_required_string_from_map(m, "description")?;
                            let default = get_optional_string_from_map(m, "default");
                            Some(SkillConfigVar {
                                key,
                                description,
                                default,
                            })
                        })
                        .collect()
                }),
            conditions: m
                .get(serde_yaml::Value::String("conditions".into()))
                .and_then(|v| v.as_mapping())
                .map(|m| SkillConditions {
                    fallback_for_tools: get_string_list_from_map(m, "fallback_for_tools"),
                    requires_toolsets: get_string_list_from_map(m, "requires_toolsets"),
                    fallback_for_toolsets: get_string_list_from_map(m, "fallback_for_toolsets"),
                    requires_tools: get_string_list_from_map(m, "requires_tools"),
                }),
        });

    let metadata = obj
        .get(serde_yaml::Value::String("metadata".into()))
        .and_then(|value| value.as_mapping())
        .map(|metadata| SkillPackageMetadata {
            requires: metadata
                .get(serde_yaml::Value::String("requires".into()))
                .and_then(|value| value.as_mapping())
                .map(|requires| SkillPackageRequires {
                    bins: get_string_list_from_map(requires, "bins"),
                }),
        });

    Ok(Some(SkillManifest {
        name,
        description,
        version,
        author,
        license,
        platforms,
        tags,
        related_skills,
        conditions,
        config,
        prerequisites,
        metadata,
        hermes_metadata,
    }))
}

/// Parse legacy frontmatter (name and description only)
fn parse_legacy_frontmatter(yaml_str: &str) -> LegacyFrontmatter {
    let mut name = None;
    let mut description = None;

    for line in yaml_str.lines() {
        let trimmed = line.trim();
        if let Some(value) = trimmed.strip_prefix("name:") {
            name = Some(unquote_value(value.trim()));
        } else if let Some(value) = trimmed.strip_prefix("description:") {
            description = Some(unquote_value(value.trim()));
        }
    }

    LegacyFrontmatter { name, description }
}

/// Unquote a YAML value
fn unquote_value(value: &str) -> String {
    value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
        .unwrap_or(value)
        .trim()
        .to_string()
}

/// Get a required string field from YAML mapping
fn get_required_string(obj: &serde_yaml::Mapping, key: &str) -> Result<String, SkillParseError> {
    match obj.get(serde_yaml::Value::String(key.into())) {
        Some(serde_yaml::Value::Null) | None => Err(SkillParseError::MissingField(key.to_string())),
        Some(v) => Ok(v
            .as_str()
            .map(String::from)
            .unwrap_or_else(|| format!("{:?}", v))),
    }
}

/// Get an optional string field from YAML mapping
fn get_optional_string(obj: &serde_yaml::Mapping, key: &str) -> Option<String> {
    obj.get(serde_yaml::Value::String(key.into()))
        .and_then(|v| v.as_str())
        .map(String::from)
}

/// Get a string list from YAML mapping
fn get_string_list(obj: &serde_yaml::Mapping, key: &str) -> Option<Vec<String>> {
    obj.get(serde_yaml::Value::String(key.into()))
        .and_then(|v| v.as_sequence())
        .map(|seq| {
            seq.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .collect()
        })
}

/// Get a required string from a nested mapping
fn get_required_string_from_map(obj: &serde_yaml::Mapping, key: &str) -> Option<String> {
    obj.get(serde_yaml::Value::String(key.into()))
        .and_then(|v| v.as_str())
        .map(String::from)
}

/// Get an optional string from a nested mapping
fn get_optional_string_from_map(obj: &serde_yaml::Mapping, key: &str) -> Option<String> {
    obj.get(serde_yaml::Value::String(key.into()))
        .and_then(|v| v.as_str())
        .map(String::from)
}

/// Get a string list from a nested mapping
fn get_string_list_from_map(obj: &serde_yaml::Mapping, key: &str) -> Option<Vec<String>> {
    obj.get(serde_yaml::Value::String(key.into()))
        .and_then(|v| v.as_sequence())
        .map(|seq| {
            seq.iter()
                .filter_map(|s| s.as_str().map(String::from))
                .collect()
        })
}

/// Validate field length
fn validate_field_length(
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), SkillParseError> {
    let len = value.chars().count();
    if len > max {
        return Err(SkillParseError::FieldTooLong {
            field,
            length: len,
            max,
        });
    }
    Ok(())
}

/// Get the effective name (from manifest or legacy)
pub fn get_skill_name(parsed: &ParsedSkill) -> Option<&str> {
    parsed
        .manifest
        .as_ref()
        .map(|m| m.name.as_str())
        .or(parsed.legacy.name.as_deref())
}

/// Get the effective description
pub fn get_skill_description(parsed: &ParsedSkill) -> Option<&str> {
    parsed
        .manifest
        .as_ref()
        .map(|m| m.description.as_str())
        .or(parsed.legacy.description.as_deref())
}

/// Check if skill matches current platform
pub fn matches_platform(parsed: &ParsedSkill, current_platform: &str) -> bool {
    let platforms = match &parsed.manifest {
        Some(m) => m.platforms.as_ref(),
        None => return true, // No restriction
    };

    let Some(platforms) = platforms else {
        return true; // No restriction
    };

    if platforms.is_empty() {
        return true; // Empty list means all platforms
    }

    let current = match current_platform {
        "darwin" | "macos" => "macos",
        "linux" => "linux",
        "windows" | "win32" => "windows",
        _ => current_platform,
    };

    platforms.iter().any(|p| match p {
        Platform::Macos => current == "macos",
        Platform::Linux => current == "linux",
        Platform::Windows => current == "windows",
    })
}

/// Check if prerequisites are met
pub fn check_prerequisites(
    parsed: &ParsedSkill,
    env_vars: &HashMap<String, String>,
    available_commands: &[String],
) -> PrerequisitesCheck {
    let manifest = match &parsed.manifest {
        Some(manifest) => manifest,
        None => return PrerequisitesCheck::Met,
    };

    let mut missing_env_vars = Vec::new();
    let mut missing_commands = Vec::new();

    // Check env vars
    if let Some(required) = manifest
        .prerequisites
        .as_ref()
        .and_then(|prerequisites| prerequisites.env_vars.as_ref())
    {
        for var in required {
            if !env_vars.contains_key(var) {
                missing_env_vars.push(var.clone());
            }
        }
    }

    // Check commands
    let commands = manifest
        .prerequisites
        .as_ref()
        .and_then(|prerequisites| prerequisites.commands.as_ref())
        .into_iter()
        .flatten()
        .chain(
            manifest
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.requires.as_ref())
                .and_then(|requires| requires.bins.as_ref())
                .into_iter()
                .flatten(),
        );
    for cmd in commands {
        if !available_commands.iter().any(|available| available == cmd) {
            missing_commands.push(cmd.clone());
        }
    }
    missing_commands.sort();
    missing_commands.dedup();

    if missing_env_vars.is_empty() && missing_commands.is_empty() {
        PrerequisitesCheck::Met
    } else {
        PrerequisitesCheck::Missing {
            env_vars: missing_env_vars,
            commands: missing_commands,
        }
    }
}

/// Prerequisites check result
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrerequisitesCheck {
    /// All prerequisites are met
    Met,
    /// Some prerequisites are missing
    Missing {
        env_vars: Vec<String>,
        commands: Vec<String>,
    },
}

/// Get all configuration variables for a skill
pub fn get_config_vars(parsed: &ParsedSkill) -> Vec<SkillConfigVar> {
    // Prefer hermes.config, fall back to config
    if let Some(manifest) = &parsed.manifest {
        if let Some(hermes) = &manifest.hermes_metadata {
            if let Some(config) = &hermes.config {
                return config.clone();
            }
        }
        if let Some(config) = &manifest.config {
            return config.clone();
        }
    }
    Vec::new()
}

/// Get tags for a skill
pub fn get_tags(parsed: &ParsedSkill) -> Vec<String> {
    if let Some(manifest) = &parsed.manifest {
        if let Some(tags) = &manifest.tags {
            return tags.clone();
        }
        if let Some(hermes) = &manifest.hermes_metadata {
            if let Some(tags) = &hermes.tags {
                return tags.clone();
            }
        }
    }
    Vec::new()
}

/// Get related skills
pub fn get_related_skills(parsed: &ParsedSkill) -> Vec<String> {
    if let Some(manifest) = &parsed.manifest {
        if let Some(related) = &manifest.related_skills {
            return related.clone();
        }
        if let Some(hermes) = &manifest.hermes_metadata {
            if let Some(related) = &hermes.related_skills {
                return related.clone();
            }
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_parse_basic_frontmatter() {
        let content = r#"---
name: test-skill
description: A test skill
---

# Test Skill
This is the body content.
"#;

        let parsed = parse_skill_content(content).unwrap();
        assert_eq!(get_skill_name(&parsed), Some("test-skill"));
        assert_eq!(get_skill_description(&parsed), Some("A test skill"));
        assert!(parsed.body.contains("Test Skill"));
    }

    #[test]
    fn test_parse_enhanced_frontmatter() {
        let content = r#"---
name: advanced-skill
description: An advanced skill with full metadata
version: 1.0.0
author: Test Author
license: MIT
platforms: [macos, linux]
tags: [coding, devops]
related_skills: [basic-skill, expert-skill]
hermes:
  tags: [advanced, expert]
  related_skills: [premium-skill]
  config:
    - key: api.key
      description: API key
      default: "default"
---

# Advanced Skill
"#;

        let parsed = parse_skill_content(content).unwrap();
        let manifest = parsed.manifest.as_ref().unwrap();
        assert_eq!(manifest.name, "advanced-skill");
        assert_eq!(manifest.version, Some("1.0.0".to_string()));
        assert!(parsed.manifest.is_some());
        let tags = get_tags(&parsed);
        // get_tags returns manifest.tags or hermes.tags, not both
        // manifest.tags is [coding, devops], hermes.tags is [advanced, expert]
        assert!(tags.contains(&"coding".to_string()));
        assert!(tags.contains(&"devops".to_string()));
    }

    #[test]
    fn test_parse_no_frontmatter() {
        let content = "# No Frontmatter\n\nJust plain content.";
        let parsed = parse_skill_content(content).unwrap();
        assert!(parsed.manifest.is_none());
        assert!(parsed.legacy.name.is_none());
        assert!(parsed.body.contains("No Frontmatter"));
    }

    #[test]
    fn test_platform_matching() {
        let content = r#"---
name: platform-skill
description: Platform specific
platforms: [macos, linux]
---

Body
"#;

        let parsed = parse_skill_content(content).unwrap();
        assert!(matches_platform(&parsed, "darwin"));
        assert!(matches_platform(&parsed, "linux"));
        assert!(!matches_platform(&parsed, "windows"));
    }

    #[test]
    fn official_metadata_bins_are_checked_as_command_prerequisites() {
        let content = r#"---
name: lark-base
description: Official Lark Base skill
metadata:
  requires:
    bins: ["lark-cli"]
  cliHelp: "lark-cli base --help"
---

Use lark-cli base commands.
"#;
        let parsed = parse_skill_content(content).unwrap();

        assert_eq!(
            check_prerequisites(&parsed, &HashMap::new(), &[]),
            PrerequisitesCheck::Missing {
                env_vars: Vec::new(),
                commands: vec!["lark-cli".to_string()],
            }
        );
        assert_eq!(
            check_prerequisites(&parsed, &HashMap::new(), &["lark-cli".to_string()]),
            PrerequisitesCheck::Met
        );
    }

    #[test]
    fn catalog_header_parser_does_not_reside_large_skill_body() {
        let directory =
            std::env::temp_dir().join(format!("cowd-skill-header-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("skill directory");
        let path = directory.join("SKILL.md");
        let mut file = std::fs::File::create(&path).expect("skill file");
        file.write_all(
            b"---\nname: large-skill\ndescription: Header only catalog\nversion: 2.1.0\n---\n",
        )
        .expect("frontmatter");
        file.write_all(&vec![b'x'; 2 * 1024 * 1024])
            .expect("large body");
        drop(file);

        let parsed = parse_skill_file_header(&path).expect("bounded header parse");

        assert_eq!(get_skill_name(&parsed), Some("large-skill"));
        assert_eq!(get_skill_description(&parsed), Some("Header only catalog"));
        assert_eq!(
            parsed
                .manifest
                .as_ref()
                .and_then(|value| value.version.as_deref()),
            Some("2.1.0")
        );
        assert!(
            parsed.body.is_empty(),
            "catalog discovery must not retain the Markdown body"
        );
        std::fs::remove_dir_all(directory).expect("cleanup");
    }
}
