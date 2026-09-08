use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

use crate::path_policy::WorkspacePathPolicy;

/// Maximum file size that can be read (10 MB).
const MAX_READ_SIZE: u64 = 10 * 1024 * 1024;

/// Filesystem searches are bounded before they enter the blocking ToolHost
/// adapter. A Runtime waiter timeout alone cannot interrupt a synchronous
/// directory walk, so broad model requests must return a truthful partial
/// result instead of holding a required Agent branch indefinitely.
const MAX_SEARCH_ENTRIES: usize = 100_000;
const MAX_SEARCH_DEPTH: usize = 20;
const MAX_SEARCH_DURATION_MS: u128 = 10_000;
const MAX_SEARCH_RESULTS: usize = 100;

/// Default line window used when callers omit an explicit read limit.
const DEFAULT_READ_LINE_LIMIT: usize = 1_000;

const TRUNCATED_READ_GUIDANCE: &str = "This is a bounded window, not the whole file. For ordinary analysis, use grep_search (or grep_many) to locate relevant logic instead of scanning consecutive offsets. If the task explicitly requires whole-file or EOF coverage, call read_file again with complete=true; complete mode is still protected by the file-size safety ceiling.";

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
    #[serde(rename = "byteLength")]
    pub byte_length: u64,
    pub sha256: String,
    #[serde(rename = "endsWithNewline")]
    pub ends_with_newline: bool,
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
    #[serde(rename = "basePath")]
    pub base_path: String,
    pub pattern: String,
    #[serde(rename = "scanComplete")]
    pub scan_complete: bool,
    #[serde(rename = "durationMs")]
    pub duration_ms: u128,
    #[serde(rename = "numFiles")]
    pub num_files: usize,
    pub filenames: Vec<String>,
    pub truncated: bool,
    #[serde(rename = "continuationCursor", skip_serializing_if = "Option::is_none")]
    pub continuation_cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub omissions: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_request: Option<serde_json::Value>,
    pub coverage: serde_json::Value,
}

/// Explicit overrides for project search rules. Additional patterns exclude
/// matches; include_ignored also admits hidden/project-ignored paths.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchOptions {
    #[serde(default)]
    pub include_ignored: bool,
    #[serde(default)]
    pub ignore_patterns: Vec<String>,
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
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(flatten)]
    pub search_options: SearchOptions,
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
    #[serde(rename = "basePath")]
    pub base_path: String,
    #[serde(rename = "scanComplete")]
    pub scan_complete: bool,
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
    #[serde(rename = "continuationCursor", skip_serializing_if = "Option::is_none")]
    pub continuation_cursor: Option<String>,
    #[serde(default)]
    pub omissions: Vec<String>,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_request: Option<serde_json::Value>,
    pub coverage: serde_json::Value,
}

/// Read exact bytes from one opened file, including CRLF and trailing newlines.
/// The caller supplies the revision it observed; a changing source is not published.
pub fn snapshot_file(
    policy: &WorkspacePathPolicy,
    path: &str,
    expected_sha256: &str,
) -> io::Result<Vec<u8>> {
    use std::io::Read;
    let resolved = policy.resolve(path)?;
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(&resolved)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "source must be a regular file",
        ));
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        policy.ensure_resolved_path(&PathBuf::from(format!(
            "/proc/self/fd/{}",
            file.as_raw_fd()
        )))?;
    }
    let mut bytes = Vec::new();
    file.take(MAX_READ_SIZE + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_READ_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "source exceeds file snapshot resource limit",
        ));
    }
    if format!("{:x}", Sha256::digest(&bytes)) != expected_sha256.trim_start_matches("sha256:") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "source_version_conflict",
        ));
    }
    Ok(bytes)
}

/// Pin each parent component so replacing a path with a symlink cannot
/// redirect publication between validation and the atomic link operation.
#[cfg(target_os = "linux")]
fn publication_parent(
    policy: &WorkspacePathPolicy,
    parent: &Path,
) -> io::Result<(fs::File, PathBuf)> {
    use std::os::{fd::AsRawFd, unix::fs::OpenOptionsExt};
    let open_dir = |path: &Path| {
        fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(path)
    };
    let mut directory = open_dir(policy.workspace_root())?;
    let relative = parent.strip_prefix(policy.workspace_root()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            "publication parent outside workspace",
        )
    })?;
    for component in relative.components() {
        let std::path::Component::Normal(name) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid publication parent component",
            ));
        };
        let pinned = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
        policy.ensure_resolved_path(&pinned)?;
        let next = pinned.join(name);
        match fs::create_dir(&next) {
            Ok(()) => directory.sync_all()?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        directory = open_dir(&next)?;
    }
    let pinned = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
    policy.ensure_resolved_path(&pinned)?;
    Ok((directory, pinned))
}

#[cfg(not(target_os = "linux"))]
fn publication_parent(
    policy: &WorkspacePathPolicy,
    parent: &Path,
) -> io::Result<(fs::File, PathBuf)> {
    fs::create_dir_all(parent)?;
    let resolved = policy.ensure_resolved_path(parent)?;
    Ok((fs::File::open(&resolved)?, resolved))
}

/// Publish fully written bytes without replacing an existing different file.
/// Hard-link creation is atomic and does not overwrite a racing writer.
pub fn materialize_file(
    policy: &WorkspacePathPolicy,
    path: &str,
    bytes: &[u8],
) -> io::Result<(PathBuf, bool)> {
    use std::io::Write;
    if bytes.len() > MAX_WRITE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "content exceeds file write resource limit",
        ));
    }
    let resolved = policy.resolve(path)?;
    let hash = format!("{:x}", Sha256::digest(bytes));
    if resolved.exists() {
        snapshot_file(policy, path, &hash)?;
        return Ok((resolved, false));
    }
    let parent = resolved
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "destination has no parent"))?;
    let (parent_handle, pinned_parent) = publication_parent(policy, parent)?;
    let destination = pinned_parent.join(resolved.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "destination has no filename")
    })?);
    static NEXT_TEMP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let (temp, mut file) = loop {
        let sequence = NEXT_TEMP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let temp = pinned_parent.join(format!(".cowd-publish-{}-{sequence}", std::process::id()));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
        {
            Ok(file) => break (temp, file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        match fs::hard_link(&temp, &destination) {
            Ok(()) => {
                parent_handle.sync_all()?;
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                snapshot_file(policy, path, &hash).map(|_| false)
            }
            Err(error) => Err(error),
        }
    })();
    let _ = fs::remove_file(&temp);
    let created = result?;
    // A renamed parent cannot produce a successful receipt for a stale path.
    snapshot_file(policy, path, &hash)?;
    Ok((resolved, created))
}

/// Reads a text file and returns a line-windowed payload.
pub fn read_file(
    policy: &WorkspacePathPolicy,
    path: &str,
    offset: Option<usize>,
    limit: Option<usize>,
    complete: bool,
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
    // Provider tool emitters often retain an inherited pagination field while
    // upgrading a read to complete coverage. `complete=true` is unambiguous
    // and read-only, so it safely takes precedence instead of consuming a
    // recovery turn on an otherwise harmless redundant offset or limit.
    let start_index = if complete {
        0
    } else {
        offset.unwrap_or(0).min(lines.len())
    };
    let line_limit = if complete {
        lines.len()
    } else {
        limit.unwrap_or(DEFAULT_READ_LINE_LIMIT)
    };
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
            byte_length: metadata.len(),
            sha256: format!("{:x}", Sha256::digest(content.as_bytes())),
            ends_with_newline: content.ends_with('\n'),
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
    let original_file = match fs::read_to_string(&absolute_path) {
        Ok(content) => Some(content),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
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
#[path = "search.rs"]
mod search;
pub(crate) use search::glob_search_page;
pub use search::{glob_search, glob_search_with_options, grep_search};

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

    #[cfg(target_os = "linux")]
    #[test]
    fn publication_pinned_parent_cannot_be_redirected_by_symlink_replacement() {
        let temp = temp_path("pinned-publication");
        std::fs::create_dir_all(&temp).unwrap();
        let outside = temp_path("outside-publication");
        std::fs::create_dir_all(&outside).unwrap();
        let policy = WorkspacePathPolicy::new(temp.as_path());
        let parent = temp.as_path().join("output");
        let (_handle, pinned) = super::publication_parent(&policy, &parent).unwrap();
        std::fs::rename(&parent, temp.as_path().join("saved-output")).unwrap();
        std::os::unix::fs::symlink(outside.as_path(), &parent).unwrap();
        std::fs::write(pinned.join("candidate"), b"body").unwrap();
        std::fs::hard_link(pinned.join("candidate"), pinned.join("published")).unwrap();
        assert!(!outside.as_path().join("candidate").exists());
        assert!(!outside.as_path().join("published").exists());
        assert_eq!(
            std::fs::read(temp.as_path().join("saved-output/published")).unwrap(),
            b"body"
        );
        assert!(super::materialize_file(&policy, "output/new", b"body").is_err());
        assert!(!outside.as_path().join("new").exists());
        std::fs::remove_dir_all(&temp).unwrap();
        std::fs::remove_dir_all(&outside).unwrap();
    }

    #[test]
    fn publication_preserves_exact_bytes_and_rejects_changed_sources() {
        use sha2::{Digest, Sha256};
        let root = temp_path("publication");
        std::fs::create_dir_all(&root).unwrap();
        let policy = WorkspacePathPolicy::new(&root);
        let bytes = "<html>中文\r\n正文</html>\r\n".as_bytes();
        let hash = format!("{:x}", Sha256::digest(bytes));
        std::fs::write(root.join("source.html"), bytes).unwrap();
        let snapshot = super::snapshot_file(&policy, "source.html", &hash).unwrap();
        assert_eq!(snapshot, bytes);
        super::materialize_file(&policy, "export.html", &snapshot).unwrap();
        super::materialize_file(&policy, "export.html", &snapshot).unwrap();
        assert_eq!(std::fs::read(root.join("export.html")).unwrap(), bytes);
        assert!(super::materialize_file(&policy, "export.html", b"different").is_err());
        std::fs::write(root.join("source.html"), b"new revision").unwrap();
        assert!(super::snapshot_file(&policy, "source.html", &hash).is_err());
        assert!(super::snapshot_file(&policy, "../outside", &hash).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publication_concurrent_exports_never_replace_the_winner() {
        let root = temp_path("publication-race");
        std::fs::create_dir_all(&root).unwrap();
        let policy = WorkspacePathPolicy::new(&root);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let joins: Vec<_> = [b"first".to_vec(), b"second".to_vec()]
            .into_iter()
            .map(|bytes| {
                let policy = policy.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    super::materialize_file(&policy, "result", &bytes).map(|_| bytes)
                })
            })
            .collect();
        let results: Vec<_> = joins.into_iter().map(|join| join.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let winner = results.into_iter().find_map(Result::ok).unwrap();
        assert_eq!(std::fs::read(root.join("result")).unwrap(), winner);
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reads_and_writes_files() {
        let path = temp_path("read-write.txt");
        let policy = policy_for(&path);
        let write_output = write_file(&policy, path.to_string_lossy().as_ref(), "one\ntwo\nthree")
            .expect("write should succeed");
        assert_eq!(write_output.kind, "create");

        let read_output = read_file(
            &policy,
            path.to_string_lossy().as_ref(),
            Some(1),
            Some(1),
            false,
        )
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

        let output = read_file(&policy, path.to_string_lossy().as_ref(), None, None, false)
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
    fn explicit_complete_read_returns_the_whole_file_without_pagination() {
        let path = temp_path("complete-read.txt");
        let policy = policy_for(&path);
        let content = (0..1_250)
            .map(|line| format!("line-{line}"))
            .collect::<Vec<_>>()
            .join("\n");
        write_file(&policy, path.to_string_lossy().as_ref(), &content)
            .expect("write should succeed");

        let output = read_file(&policy, path.to_string_lossy().as_ref(), None, None, true)
            .expect("explicit complete read should succeed");

        assert_eq!(output.file.num_lines, 1_250);
        assert_eq!(output.file.total_lines, 1_250);
        assert!(output.file.content.ends_with("line-1249"));
        assert!(!output.truncated);
        assert!(output.guidance.is_none());
        let complete_with_redundant_pagination = read_file(
            &policy,
            path.to_string_lossy().as_ref(),
            Some(1),
            Some(1),
            true,
        )
        .expect("complete coverage takes precedence over redundant pagination");
        assert_eq!(complete_with_redundant_pagination.file.num_lines, 1_250);
        assert_eq!(complete_with_redundant_pagination.file.start_line, 1);
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
        let result = read_file(&policy, path.to_string_lossy().as_ref(), None, None, false);
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

        let globbed = glob_search(
            &policy,
            "**/*.rs",
            Some(dir.to_string_lossy().as_ref()),
            None,
        )
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
                cursor: None,
                search_options: Default::default(),
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
                cursor: None,
                search_options: Default::default(),
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

        let result = glob_search(&policy, "*.{rs,toml}", Some(dir.to_str().unwrap()), None)
            .expect("glob should succeed");
        assert_eq!(
            result.num_files, 2,
            "should match .rs and .toml but not .txt"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn glob_pages_cover_braces_and_prefix_directories_without_duplicates() {
        let directory = temp_path("glob-stable-pages");
        std::fs::create_dir_all(&directory).unwrap();
        let policy = WorkspacePathPolicy::new(&directory);
        std::fs::create_dir(directory.join("a")).unwrap();
        let mut expected = std::collections::BTreeSet::new();
        for index in 0..251 {
            let path = if index % 2 == 0 {
                directory.join(format!("a/{index:04}.rs"))
            } else {
                directory.join(format!("a.{index:04}.toml"))
            };
            std::fs::write(&path, "data").unwrap();
            expected.insert(path.to_string_lossy().into_owned());
        }
        let mut cursor = None;
        let mut actual = std::collections::BTreeSet::new();
        let mut page_count = 0;
        loop {
            let page = glob_search(&policy, "**/*.{rs,toml}", None, cursor.as_deref()).unwrap();
            page_count += 1;
            assert!(page_count <= 4, "cursor must make progress");
            assert!(page.num_files <= super::MAX_SEARCH_RESULTS);
            for path in page.filenames {
                assert!(
                    actual.insert(path),
                    "each match must be delivered only once"
                );
            }
            match page.continuation_cursor {
                Some(next) => {
                    assert!(!page.scan_complete);
                    assert!(glob_search(&policy, "**/*.txt", None, Some(&next)).is_err());
                    assert_ne!(cursor.as_ref(), Some(&next));
                    cursor = Some(next);
                }
                None => {
                    assert!(page.scan_complete);
                    break;
                }
            }
        }
        assert_eq!(actual, expected);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn explicit_build_directory_is_searchable_and_exact_full_page_can_finish() {
        let directory = temp_path("glob-explicit-build");
        std::fs::create_dir_all(&directory).unwrap();
        let build = directory.join("target");
        std::fs::create_dir(&build).unwrap();
        for index in 0..super::MAX_SEARCH_RESULTS {
            std::fs::write(build.join(format!("{index:04}.txt")), "x").unwrap();
        }
        let policy = WorkspacePathPolicy::new(&directory);
        let page = glob_search(&policy, "*.txt", Some("target"), None).unwrap();
        assert_eq!(page.num_files, super::MAX_SEARCH_RESULTS);
        let end = glob_search(
            &policy,
            "*.txt",
            Some("target"),
            page.continuation_cursor.as_deref(),
        )
        .unwrap();
        assert!(end.scan_complete);
        assert_eq!(end.num_files, 0);
        assert!(end.continuation_cursor.is_none());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn default_directory_noise_filter_does_not_hide_same_named_files() {
        let directory = temp_path("glob-directory-filter");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("target"), "ordinary project file").unwrap();
        let policy = WorkspacePathPolicy::new(&directory);
        let page = glob_search(&policy, "**/*", None, None).unwrap();
        assert_eq!(page.num_files, 1);
        assert!(page.scan_complete);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn broad_glob_reports_incomplete_when_depth_bound_hides_descendants() {
        let dir = temp_path("glob-depth-bound");
        std::fs::create_dir_all(&dir).expect("directory should be created");
        let policy = WorkspacePathPolicy::new(&dir);
        let mut nested = dir.clone();
        for index in 0..(super::MAX_SEARCH_DEPTH + 2) {
            nested.push(format!("level-{index}"));
            std::fs::create_dir_all(&nested).expect("nested directory should be created");
        }
        std::fs::write(nested.join("hidden.rs"), "fn hidden() {}").expect("file should be created");

        let result = glob_search(&policy, "**/*", Some(dir.to_str().unwrap()), None)
            .expect("bounded glob should succeed");
        assert!(!result.scan_complete, "depth truncation must be explicit");
        assert_eq!(
            result.num_files, 0,
            "the file is intentionally beyond the bound"
        );
        assert!(
            result.continuation_cursor.is_none(),
            "depth omissions cannot be repaired by repeating a cursor"
        );
        assert!(result.omissions.contains(&"depth_limit".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
