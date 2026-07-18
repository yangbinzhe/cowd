use std::cmp::Reverse;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use glob::Pattern;
use regex::RegexBuilder;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use walkdir::WalkDir;

use crate::path_policy::WorkspacePathPolicy;

/// Maximum file size that can be read (10 MB).
const MAX_READ_SIZE: u64 = 10 * 1024 * 1024;

/// Default line window used when callers omit an explicit read limit.
const DEFAULT_READ_LINE_LIMIT: usize = 1_000;

const TRUNCATED_READ_GUIDANCE: &str = "This is a bounded window, not the whole file. Do not continue with consecutive read_file offsets to scan a large file. Use grep_search (or grep_many for independent patterns) to locate the relevant symbol or logic, then read only the matching region.";

/// Maximum file size that can be written (10 MB).
const MAX_WRITE_SIZE: usize = 10 * 1024 * 1024;

/// Check whether a file appears to contain binary content by examining
/// the first chunk for NUL bytes.
fn is_binary_file(path: &Path) -> io::Result<bool> {
    use std::io::Read;
    let mut file = fs::File::open(path)?;
    let mut buffer = [0u8; 8192];
    let bytes_read = file.read(&mut buffer)?;
    Ok(buffer[..bytes_read].contains(&0))
}

/// Text payload returned by file-reading operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TextFilePayload {
    #[serde(rename = "filePath")]
    pub file_path: String,
    pub content: String,
    #[serde(rename = "numLines")]
    pub num_lines: usize,
    #[serde(rename = "startLine")]
    pub start_line: usize,
    #[serde(rename = "totalLines")]
    pub total_lines: usize,
}

/// Output envelope for the `read_file` tool.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadFileOutput {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guidance: Option<String>,
    pub file: TextFilePayload,
}

/// Structured patch hunk emitted by write and edit operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StructuredPatchHunk {
    #[serde(rename = "oldStart")]
    pub old_start: usize,
    #[serde(rename = "oldLines")]
    pub old_lines: usize,
    #[serde(rename = "newStart")]
    pub new_start: usize,
    #[serde(rename = "newLines")]
    pub new_lines: usize,
    pub lines: Vec<String>,
}

/// Output envelope for full-file write operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteFileOutput {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "filePath")]
    pub file_path: String,
    pub content: String,
    #[serde(rename = "structuredPatch")]
    pub structured_patch: Vec<StructuredPatchHunk>,
    #[serde(rename = "originalFile")]
    pub original_file: Option<String>,
    #[serde(rename = "gitDiff")]
    pub git_diff: Option<serde_json::Value>,
}

/// Output envelope for targeted string-replacement edits.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EditFileOutput {
    #[serde(rename = "filePath")]
    pub file_path: String,
    #[serde(rename = "oldString")]
    pub old_string: String,
    #[serde(rename = "newString")]
    pub new_string: String,
    #[serde(rename = "originalFile")]
    pub original_file: String,
    #[serde(rename = "structuredPatch")]
    pub structured_patch: Vec<StructuredPatchHunk>,
    #[serde(rename = "userModified")]
    pub user_modified: bool,
    #[serde(rename = "replaceAll")]
    pub replace_all: bool,
    #[serde(rename = "gitDiff")]
    pub git_diff: Option<serde_json::Value>,
}

/// Result of a glob-based filename search.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GlobSearchOutput {
    #[serde(rename = "durationMs")]
    pub duration_ms: u128,
    #[serde(rename = "numFiles")]
    pub num_files: usize,
    pub filenames: Vec<String>,
    pub truncated: bool,
}

/// Parameters accepted by the grep-style search tool.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GrepSearchInput {
    pub pattern: Option<String>,
    pub path: Option<String>,
    pub glob: Option<String>,
    #[serde(rename = "output_mode")]
    pub output_mode: Option<String>,
    #[serde(rename = "-B")]
    pub before: Option<usize>,
    #[serde(rename = "-A")]
    pub after: Option<usize>,
    #[serde(rename = "-C")]
    pub context_short: Option<usize>,
    pub context: Option<usize>,
    #[serde(
        rename = "-n",
        default,
        deserialize_with = "deserialize_optional_boolish"
    )]
    pub line_numbers: Option<bool>,
    #[serde(
        rename = "-i",
        default,
        deserialize_with = "deserialize_optional_boolish"
    )]
    pub case_insensitive: Option<bool>,
    #[serde(rename = "type")]
    pub file_type: Option<String>,
    pub head_limit: Option<usize>,
    pub offset: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_optional_boolish")]
    pub multiline: Option<bool>,
}

fn deserialize_optional_boolish<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Bool(value)) => Ok(Some(value)),
        Some(serde_json::Value::String(value)) if value.eq_ignore_ascii_case("true") => {
            Ok(Some(true))
        }
        Some(serde_json::Value::String(value)) if value.eq_ignore_ascii_case("false") => {
            Ok(Some(false))
        }
        Some(value) => Err(D::Error::custom(format!(
            "expected a boolean or the string true/false, got {value}"
        ))),
    }
}

/// Result payload returned by the grep-style search tool.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GrepSearchOutput {
    pub mode: Option<String>,
    #[serde(rename = "numFiles")]
    pub num_files: usize,
    pub filenames: Vec<String>,
    pub content: Option<String>,
    #[serde(rename = "numLines")]
    pub num_lines: Option<usize>,
    #[serde(rename = "numMatches")]
    pub num_matches: Option<usize>,
    #[serde(rename = "appliedLimit")]
    pub applied_limit: Option<usize>,
    #[serde(rename = "appliedOffset")]
    pub applied_offset: Option<usize>,
}

/// Reads a text file and returns a line-windowed payload.
pub fn read_file(
    policy: &WorkspacePathPolicy,
    path: &str,
    offset: Option<usize>,
    limit: Option<usize>,
) -> io::Result<ReadFileOutput> {
    let absolute_path = policy.resolve(path)?;

    // Check file size before reading
    let metadata = fs::metadata(&absolute_path)?;
    if metadata.len() > MAX_READ_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "file is too large ({} bytes, max {} bytes)",
                metadata.len(),
                MAX_READ_SIZE
            ),
        ));
    }

    // Detect binary files
    if is_binary_file(&absolute_path)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file appears to be binary",
        ));
    }

    let content = fs::read_to_string(&absolute_path)?;
    let lines: Vec<&str> = content.lines().collect();
    let start_index = offset.unwrap_or(0).min(lines.len());
    let line_limit = limit.unwrap_or(DEFAULT_READ_LINE_LIMIT);
    let end_index = start_index.saturating_add(line_limit).min(lines.len());
    let selected = lines[start_index..end_index].join("\n");
    let truncated = end_index < lines.len();

    Ok(ReadFileOutput {
        kind: String::from("text"),
        truncated,
        guidance: truncated.then(|| TRUNCATED_READ_GUIDANCE.to_string()),
        file: TextFilePayload {
            file_path: absolute_path.to_string_lossy().into_owned(),
            content: selected,
            num_lines: end_index.saturating_sub(start_index),
            start_line: start_index.saturating_add(1),
            total_lines: lines.len(),
        },
    })
}

/// Replaces a file's contents and returns patch metadata.
pub fn write_file(
    policy: &WorkspacePathPolicy,
    path: &str,
    content: &str,
) -> io::Result<WriteFileOutput> {
    if content.len() > MAX_WRITE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "content is too large ({} bytes, max {} bytes)",
                content.len(),
                MAX_WRITE_SIZE
            ),
        ));
    }

    let absolute_path = policy.resolve(path)?;
    let original_file = fs::read_to_string(&absolute_path).ok();
    if let Some(parent) = absolute_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&absolute_path, content)?;

    Ok(WriteFileOutput {
        kind: if original_file.is_some() {
            String::from("update")
        } else {
            String::from("create")
        },
        file_path: absolute_path.to_string_lossy().into_owned(),
        content: content.to_owned(),
        structured_patch: make_patch(original_file.as_deref().unwrap_or(""), content),
        original_file,
        git_diff: None,
    })
}

/// Performs an in-file string replacement and returns patch metadata.
pub fn edit_file(
    policy: &WorkspacePathPolicy,
    path: &str,
    old_string: &str,
    new_string: &str,
    replace_all: bool,
) -> io::Result<EditFileOutput> {
    let absolute_path = policy.resolve(path)?;
    let original_file = fs::read_to_string(&absolute_path)?;
    if old_string == new_string {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "old_string and new_string must differ",
        ));
    }
    if !original_file.contains(old_string) {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "old_string not found in file",
        ));
    }

    let updated = if replace_all {
        original_file.replace(old_string, new_string)
    } else {
        original_file.replacen(old_string, new_string, 1)
    };
    fs::write(&absolute_path, &updated)?;

    Ok(EditFileOutput {
        file_path: absolute_path.to_string_lossy().into_owned(),
        old_string: old_string.to_owned(),
        new_string: new_string.to_owned(),
        original_file: original_file.clone(),
        structured_patch: make_patch(&original_file, &updated),
        user_modified: false,
        replace_all,
        git_diff: None,
    })
}

/// Expands a glob pattern and returns matching filenames.
pub fn glob_search(
    policy: &WorkspacePathPolicy,
    pattern: &str,
    path: Option<&str>,
) -> io::Result<GlobSearchOutput> {
    let started = Instant::now();
    let base_dir = path
        .map(|path| policy.resolve(path))
        .transpose()?
        .unwrap_or_else(|| policy.workspace_root().to_path_buf());
    let search_pattern = if Path::new(pattern).is_absolute() {
        policy.resolve_glob_pattern(pattern)?
    } else {
        policy.resolve_glob_pattern(&base_dir.join(pattern).to_string_lossy())?
    };

    // The `glob` crate does not support brace expansion ({a,b,c}).
    // Expand braces into multiple patterns so patterns like
    // `Assets/**/*.{cs,uxml,uss}` work correctly.
    let expanded = expand_braces(&search_pattern.to_string_lossy());

    let mut seen = std::collections::HashSet::new();
    let mut matches = Vec::new();
    for pat in &expanded {
        let entries = glob::glob(pat)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        for entry in entries.flatten() {
            if entry.is_file() {
                if let Ok(resolved) = policy.ensure_resolved_path(&entry) {
                    if seen.insert(resolved.clone()) {
                        matches.push(resolved);
                    }
                }
            }
        }
    }

    matches.sort_by_key(|path| {
        fs::metadata(path)
            .and_then(|metadata| metadata.modified())
            .ok()
            .map(Reverse)
    });

    let truncated = matches.len() > 100;
    let filenames = matches
        .into_iter()
        .take(100)
        .map(|path| path.to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    Ok(GlobSearchOutput {
        duration_ms: started.elapsed().as_millis(),
        num_files: filenames.len(),
        filenames,
        truncated,
    })
}

/// Runs a regex search over workspace files with optional context lines.
pub fn grep_search(
    policy: &WorkspacePathPolicy,
    input: &GrepSearchInput,
) -> io::Result<GrepSearchOutput> {
    let pattern = match &input.pattern {
        Some(p) if !p.trim().is_empty() => p.as_str(),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "pattern is required — please provide a regex pattern to search for",
            ));
        }
    };

    let base_path = input
        .path
        .as_deref()
        .map(|path| policy.resolve(path))
        .transpose()?
        .unwrap_or_else(|| policy.workspace_root().to_path_buf());

    let regex = RegexBuilder::new(pattern)
        .case_insensitive(input.case_insensitive.unwrap_or(false))
        .dot_matches_new_line(input.multiline.unwrap_or(false))
        .build()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;

    let glob_filter = input
        .glob
        .as_deref()
        .map(Pattern::new)
        .transpose()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    let file_type = input.file_type.as_deref();
    let output_mode = input
        .output_mode
        .clone()
        .unwrap_or_else(|| String::from("content"));
    let context = input.context.or(input.context_short).unwrap_or(0);

    let mut filenames = Vec::new();
    let mut content_lines = Vec::new();
    let mut total_matches = 0usize;

    for file_path in collect_search_files(policy, &base_path)? {
        if !matches_optional_filters(&file_path, glob_filter.as_ref(), file_type) {
            continue;
        }

        if std::fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0) > MAX_READ_SIZE {
            continue;
        }

        let Ok(file_contents) = fs::read_to_string(&file_path) else {
            continue;
        };

        if output_mode == "count" {
            let count = regex.find_iter(&file_contents).count();
            if count > 0 {
                filenames.push(file_path.to_string_lossy().into_owned());
                total_matches += count;
            }
            continue;
        }

        let lines: Vec<&str> = file_contents.lines().collect();
        let mut matched_lines = Vec::new();
        for (index, line) in lines.iter().enumerate() {
            if regex.is_match(line) {
                total_matches += 1;
                matched_lines.push(index);
            }
        }

        if matched_lines.is_empty() {
            continue;
        }

        filenames.push(file_path.to_string_lossy().into_owned());
        if output_mode == "content" {
            for index in matched_lines {
                let start = index.saturating_sub(input.before.unwrap_or(context));
                let end = (index + input.after.unwrap_or(context) + 1).min(lines.len());
                for (current, line) in lines.iter().enumerate().take(end).skip(start) {
                    let prefix = if input.line_numbers.unwrap_or(true) {
                        format!("{}:{}:", file_path.to_string_lossy(), current + 1)
                    } else {
                        format!("{}:", file_path.to_string_lossy())
                    };
                    content_lines.push(format!("{prefix}{line}"));
                }
            }
        }
    }

    let (filenames, applied_limit, applied_offset) =
        apply_limit(filenames, input.head_limit, input.offset);
    let content_output = if output_mode == "content" {
        let (lines, limit, offset) = apply_limit(content_lines, input.head_limit, input.offset);
        return Ok(GrepSearchOutput {
            mode: Some(output_mode),
            num_files: filenames.len(),
            filenames,
            num_lines: Some(lines.len()),
            content: Some(lines.join("\n")),
            num_matches: None,
            applied_limit: limit,
            applied_offset: offset,
        });
    } else {
        None
    };

    Ok(GrepSearchOutput {
        mode: Some(output_mode.clone()),
        num_files: filenames.len(),
        filenames,
        content: content_output,
        num_lines: None,
        num_matches: (output_mode == "count").then_some(total_matches),
        applied_limit,
        applied_offset,
    })
}

fn collect_search_files(
    policy: &WorkspacePathPolicy,
    base_path: &Path,
) -> io::Result<Vec<PathBuf>> {
    if base_path.is_file() {
        return Ok(vec![policy.ensure_resolved_path(base_path)?]);
    }

    let skip_dirs = [
        "target",
        "node_modules",
        ".git",
        ".cowd",
        ".cargo",
        ".gitnexus",
    ];
    let mut files = Vec::new();
    for entry in WalkDir::new(base_path)
        .max_depth(20)
        .into_iter()
        .filter_entry(|e| !skip_dirs.iter().any(|d| e.file_name().to_str() == Some(d)))
    {
        let entry = entry.map_err(|error| io::Error::other(error.to_string()))?;
        if entry.file_type().is_file() {
            if let Ok(resolved) = policy.ensure_resolved_path(entry.path()) {
                files.push(resolved);
            }
        }
    }
    Ok(files)
}

fn matches_optional_filters(
    path: &Path,
    glob_filter: Option<&Pattern>,
    file_type: Option<&str>,
) -> bool {
    if let Some(glob_filter) = glob_filter {
        let path_string = path.to_string_lossy();
        if !glob_filter.matches(&path_string) && !glob_filter.matches_path(path) {
            return false;
        }
    }

    if let Some(file_type) = file_type {
        let extension = path.extension().and_then(|extension| extension.to_str());
        if extension != Some(file_type) {
            return false;
        }
    }

    true
}

fn apply_limit<T>(
    items: Vec<T>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> (Vec<T>, Option<usize>, Option<usize>) {
    let offset_value = offset.unwrap_or(0);
    let mut items = items.into_iter().skip(offset_value).collect::<Vec<_>>();
    let explicit_limit = limit.unwrap_or(250);
    if explicit_limit == 0 {
        return (items, None, (offset_value > 0).then_some(offset_value));
    }

    let truncated = items.len() > explicit_limit;
    items.truncate(explicit_limit);
    (
        items,
        truncated.then_some(explicit_limit),
        (offset_value > 0).then_some(offset_value),
    )
}

fn make_patch(original: &str, updated: &str) -> Vec<StructuredPatchHunk> {
    let mut lines = Vec::new();
    for line in original.lines() {
        lines.push(format!("-{line}"));
    }
    for line in updated.lines() {
        lines.push(format!("+{line}"));
    }

    vec![StructuredPatchHunk {
        old_start: 1,
        old_lines: original.lines().count(),
        new_start: 1,
        new_lines: updated.lines().count(),
        lines,
    }]
}

/// Expand shell-style brace groups in a glob pattern.
///
/// Handles one level of braces: `foo.{a,b,c}` → `["foo.a", "foo.b", "foo.c"]`.
/// Nested braces are not expanded (uncommon in practice).
/// Patterns without braces pass through unchanged.
fn expand_braces(pattern: &str) -> Vec<String> {
    let Some(open) = pattern.find('{') else {
        return vec![pattern.to_owned()];
    };
    let Some(close) = pattern[open..].find('}').map(|i| open + i) else {
        // Unmatched brace — treat as literal.
        return vec![pattern.to_owned()];
    };
    let prefix = &pattern[..open];
    let suffix = &pattern[close + 1..];
    let alternatives = &pattern[open + 1..close];
    alternatives
        .split(',')
        .flat_map(|alt| expand_braces(&format!("{prefix}{alt}{suffix}")))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::path_policy::WorkspacePathPolicy;

    use super::{
        edit_file, expand_braces, glob_search, grep_search, read_file, write_file, GrepSearchInput,
        DEFAULT_READ_LINE_LIMIT, MAX_WRITE_SIZE,
    };

    fn temp_path(name: &str) -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        std::env::temp_dir().join(format!("cowd-native-{name}-{unique}"))
    }

    fn policy_for(path: &std::path::Path) -> WorkspacePathPolicy {
        WorkspacePathPolicy::new(path.parent().expect("temporary path parent"))
    }

    #[test]
    fn reads_and_writes_files() {
        let path = temp_path("read-write.txt");
        let policy = policy_for(&path);
        let write_output = write_file(&policy, path.to_string_lossy().as_ref(), "one\ntwo\nthree")
            .expect("write should succeed");
        assert_eq!(write_output.kind, "create");

        let read_output = read_file(&policy, path.to_string_lossy().as_ref(), Some(1), Some(1))
            .expect("read should succeed");
        assert_eq!(read_output.file.content, "two");
    }

    #[test]
    fn bounds_implicit_reads_but_reports_full_line_count() {
        let path = temp_path("bounded-read.txt");
        let policy = policy_for(&path);
        let content = (0..1_250)
            .map(|line| format!("line-{line}"))
            .collect::<Vec<_>>()
            .join("\n");
        write_file(&policy, path.to_string_lossy().as_ref(), &content)
            .expect("write should succeed");

        let output = read_file(&policy, path.to_string_lossy().as_ref(), None, None)
            .expect("read should succeed");

        assert_eq!(output.file.num_lines, DEFAULT_READ_LINE_LIMIT);
        assert_eq!(output.file.total_lines, 1_250);
        assert_eq!(output.file.start_line, 1);
        assert!(output.file.content.ends_with("line-999"));
        assert!(output.truncated);
        assert!(output
            .guidance
            .as_deref()
            .is_some_and(|guidance| guidance.contains("grep_search")));
    }

    #[test]
    fn edits_file_contents() {
        let path = temp_path("edit.txt");
        let policy = policy_for(&path);
        write_file(&policy, path.to_string_lossy().as_ref(), "alpha beta alpha")
            .expect("initial write should succeed");
        let output = edit_file(
            &policy,
            path.to_string_lossy().as_ref(),
            "alpha",
            "omega",
            true,
        )
        .expect("edit should succeed");
        assert!(output.replace_all);
    }

    #[test]
    fn rejects_binary_files() {
        let path = temp_path("binary-test.bin");
        let policy = policy_for(&path);
        std::fs::write(&path, b"\x00\x01\x02\x03binary content").expect("write should succeed");
        let result = read_file(&policy, path.to_string_lossy().as_ref(), None, None);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("binary"));
    }

    #[test]
    fn rejects_oversized_writes() {
        let path = temp_path("oversize-write.txt");
        let policy = policy_for(&path);
        let huge = "x".repeat(MAX_WRITE_SIZE + 1);
        let result = write_file(&policy, path.to_string_lossy().as_ref(), &huge);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("too large"));
    }

    #[test]
    fn globs_and_greps_directory() {
        let dir = temp_path("search-dir");
        std::fs::create_dir_all(&dir).expect("directory should be created");
        let policy = WorkspacePathPolicy::new(&dir);
        let file = dir.join("demo.rs");
        write_file(
            &policy,
            file.to_string_lossy().as_ref(),
            "fn main() {\n println!(\"hello\");\n}\n",
        )
        .expect("file write should succeed");

        let globbed = glob_search(&policy, "**/*.rs", Some(dir.to_string_lossy().as_ref()))
            .expect("glob should succeed");
        assert_eq!(globbed.num_files, 1);

        let grep_output = grep_search(
            &policy,
            &GrepSearchInput {
                pattern: Some(String::from("hello")),
                path: Some(dir.to_string_lossy().into_owned()),
                glob: Some(String::from("**/*.rs")),
                output_mode: Some(String::from("content")),
                before: None,
                after: None,
                context_short: None,
                context: None,
                line_numbers: Some(true),
                case_insensitive: Some(false),
                file_type: None,
                head_limit: Some(10),
                offset: Some(0),
                multiline: Some(false),
            },
        )
        .expect("grep should succeed");
        assert!(grep_output.content.unwrap_or_default().contains("hello"));
    }

    #[test]
    fn grep_defaults_to_matching_content() {
        let dir = temp_path("grep-default-content");
        std::fs::create_dir_all(&dir).expect("directory should be created");
        let policy = WorkspacePathPolicy::new(&dir);
        let file = dir.join("config.rs");
        write_file(
            &policy,
            file.to_string_lossy().as_ref(),
            "fn parse_label(value: &str) {\n    match value {\n        _ => {}\n    }\n}\n",
        )
        .expect("file write should succeed");

        let output = grep_search(
            &policy,
            &GrepSearchInput {
                pattern: Some(String::from("parse_label")),
                path: Some(file.to_string_lossy().into_owned()),
                glob: None,
                output_mode: None,
                before: None,
                after: None,
                context_short: None,
                context: None,
                line_numbers: None,
                case_insensitive: None,
                file_type: None,
                head_limit: None,
                offset: None,
                multiline: None,
            },
        )
        .expect("grep should succeed");

        assert_eq!(output.mode.as_deref(), Some("content"));
        assert!(output
            .content
            .as_deref()
            .is_some_and(|content| content.contains(":1:fn parse_label")));
    }

    #[test]
    fn grep_accepts_boolean_strings_from_model_tool_calls() {
        let parsed: GrepSearchInput = serde_json::from_value(serde_json::json!({
            "pattern": "label",
            "-n": "false",
            "-i": "TRUE",
            "multiline": "true"
        }))
        .expect("common model boolean strings should be normalized");

        assert_eq!(parsed.line_numbers, Some(false));
        assert_eq!(parsed.case_insensitive, Some(true));
        assert_eq!(parsed.multiline, Some(true));
        assert!(
            serde_json::from_value::<GrepSearchInput>(serde_json::json!({
                "pattern": "label",
                "-i": "yes"
            }))
            .is_err()
        );
    }

    #[test]
    fn expand_braces_no_braces() {
        assert_eq!(expand_braces("*.rs"), vec!["*.rs"]);
    }

    #[test]
    fn expand_braces_single_group() {
        let mut result = expand_braces("Assets/**/*.{cs,uxml,uss}");
        result.sort();
        assert_eq!(
            result,
            vec!["Assets/**/*.cs", "Assets/**/*.uss", "Assets/**/*.uxml",]
        );
    }

    #[test]
    fn expand_braces_nested() {
        let mut result = expand_braces("src/{a,b}.{rs,toml}");
        result.sort();
        assert_eq!(
            result,
            vec!["src/a.rs", "src/a.toml", "src/b.rs", "src/b.toml"]
        );
    }

    #[test]
    fn expand_braces_unmatched() {
        assert_eq!(expand_braces("foo.{bar"), vec!["foo.{bar"]);
    }

    #[test]
    fn glob_search_with_braces_finds_files() {
        let dir = temp_path("glob-braces");
        std::fs::create_dir_all(&dir).unwrap();
        let policy = WorkspacePathPolicy::new(&dir);
        std::fs::write(dir.join("a.rs"), "fn main() {}").unwrap();
        std::fs::write(dir.join("b.toml"), "[package]").unwrap();
        std::fs::write(dir.join("c.txt"), "hello").unwrap();

        let result = glob_search(&policy, "*.{rs,toml}", Some(dir.to_str().unwrap()))
            .expect("glob should succeed");
        assert_eq!(
            result.num_files, 2,
            "should match .rs and .toml but not .txt"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
