//! Workspace discovery is a live view of the lease's filesystem, never the
//! process cwd or an independently cached repository registry.
use super::*;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct WorkspaceSnapshotInput {
    include_git: Option<bool>,
    include_files: Option<bool>,
    roots: Option<Vec<String>>,
    max_files: Option<usize>,
    query: Option<String>,
    cursor: Option<String>,
    page_size: Option<usize>,
    #[serde(flatten)]
    options: SearchOptions,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryCursor {
    version: u8,
    binding: String,
    root_index: usize,
    after: PathBuf,
    omissions: Vec<String>,
}

fn request(name: &str, input: Value) -> Value {
    json!({"name": name, "input": input})
}
fn omit(omissions: &mut Vec<String>, reason: String) {
    if !omissions.contains(&reason) {
        omissions.push(reason);
    }
}

fn existing_file(policy: &WorkspacePathPolicy, path: &Path) -> bool {
    policy
        .ensure_resolved_path(path)
        .is_ok_and(|path| path.is_file())
}

fn directory(policy: &WorkspacePathPolicy, path: &Path, input: &WorkspaceSnapshotInput) -> Value {
    let markers = [
        "Cargo.toml",
        "package.json",
        "pyproject.toml",
        "go.mod",
        "pom.xml",
        "CMakeLists.txt",
    ]
    .into_iter()
    .filter(|name| existing_file(policy, &path.join(name)))
    .map(|name| path.join(name))
    .collect::<Vec<_>>();
    // A .git file can point outside the lease (worktrees/submodules). Identify
    // the marker without following its gitdir or reading another repository.
    let git_marker = path.join(".git");
    let has_git = policy
        .ensure_resolved_path(&git_marker)
        .is_ok_and(|path| path.exists());
    let documents = [
        "README.md",
        "README",
        "README.rst",
        "AGENTS.md",
        "docs/README.md",
        "docs/index.md",
    ]
    .into_iter()
    .map(|name| path.join(name))
    .filter(|path| existing_file(policy, path))
    .map(|path| json!({"path": path, "read_request": request("read_file", json!({"path": path}))}))
    .collect::<Vec<_>>();
    let head = if input.include_git.unwrap_or(true) && has_git && git_marker.is_dir() {
        policy
            .ensure_resolved_path(&git_marker.join("HEAD"))
            .ok()
            .and_then(|path| {
                let metadata = std::fs::metadata(&path).ok()?;
                if !metadata.is_file() || metadata.len() > 1024 {
                    return None;
                }
                std::fs::read_to_string(path)
                    .ok()
                    .map(|text| text.trim().to_owned())
            })
    } else {
        None
    };
    let search_input = json!({"pattern": "**/*", "path": path, "include_ignored": input.options.include_ignored,
        "ignore_patterns": input.options.ignore_patterns});
    json!({
        "source_kind": "workspace_directory", "ref": path, "name": path.file_name().map(|name| name.to_string_lossy()),
        "root": path, "scope": policy.workspace_root(), "information_status": "observed_path",
        "repository_kind": if has_git { "git" } else if !markers.is_empty() { "project_manifest" } else { "directory" },
        "manifest_paths": markers, "documents": documents,
        "git": if input.include_git.unwrap_or(true) { Some(json!({"marker_present": has_git, "head_file": head, "status": "not_collected"})) } else { None },
        "read_request": request("workspace_snapshot", json!({"roots": [path], "include_files": false,
            "include_git": input.include_git.unwrap_or(true), "include_ignored": input.options.include_ignored, "ignore_patterns": input.options.ignore_patterns})),
        "files_request": request("glob_search", search_input),
        "content_search_request": input.query.as_ref().filter(|q| !q.is_empty()).map(|query| request("grep_search", json!({"pattern": regex::escape(query), "path": path,
            "include_ignored": input.options.include_ignored, "ignore_patterns": input.options.ignore_patterns})))
    })
}

pub(super) fn run_workspace_snapshot(
    lease: &ToolHostLease,
    input: WorkspaceSnapshotInput,
) -> Result<String, String> {
    let policy = lease.path_policy();
    let roots = input.roots.clone().unwrap_or_else(|| vec![".".into()]);
    if roots.is_empty() || roots.len() > 32 {
        return Err("workspace_snapshot requires 1..32 roots".into());
    }
    let mut roots = roots
        .iter()
        .map(|root| policy.resolve(root).map_err(io_to_string))
        .collect::<Result<Vec<_>, _>>()?;
    roots.sort();
    roots.dedup();
    for root in &roots {
        if !root.is_dir() {
            return Err(format!(
                "workspace root is not a readable directory: {}",
                root.display()
            ));
        }
    }
    let binding = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                policy.workspace_root(),
                &roots,
                &input.options,
                &input.query
            ))
            .map_err(|e| e.to_string())?
        )
    );
    let resume = input
        .cursor
        .as_ref()
        .map(|value| {
            serde_json::from_str::<DirectoryCursor>(value)
                .map_err(|_| "invalid workspace directory cursor".to_string())
        })
        .transpose()?;
    if resume.as_ref().is_some_and(|cursor| {
        cursor.version != 1
            || cursor.binding != binding
            || cursor.root_index >= roots.len()
            || cursor.after.parent() != Some(roots[cursor.root_index].as_path())
    }) {
        return Err("workspace cursor does not match authorized roots, query or options".into());
    }
    let mut omissions = resume
        .as_ref()
        .map(|cursor| cursor.omissions.clone())
        .unwrap_or_default();
    let page_size = input.page_size.unwrap_or(32).clamp(1, 100);
    let mut entries = Vec::new();
    let mut next_cursor = None;
    let started = Instant::now();
    let mut visited = 0;
    let query = input.query.as_deref().unwrap_or("").to_lowercase();
    'roots: for (index, root) in roots
        .iter()
        .enumerate()
        .skip(resume.as_ref().map_or(0, |cursor| cursor.root_index))
    {
        let mut overrides = ignore::overrides::OverrideBuilder::new(root);
        for pattern in &input.options.ignore_patterns {
            if pattern.is_empty() || pattern.starts_with('!') {
                return Err("ignore_patterns must be nonempty exclusion globs".into());
            }
            overrides
                .add(&format!("!{pattern}"))
                .map_err(|e| e.to_string())?;
        }
        let mut walker = ignore::WalkBuilder::new(root);
        walker
            .standard_filters(!input.options.include_ignored)
            .parents(false)
            .git_global(false)
            .require_git(false)
            .follow_links(false)
            .max_depth(Some(1))
            .sort_by_file_name(|a, b| a.cmp(b))
            .overrides(overrides.build().map_err(|e| e.to_string())?);
        for entry in walker.build() {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    omit(
                        &mut omissions,
                        format!("directory_read_error:{}", root.display()),
                    );
                    continue;
                }
            };
            if entry.depth() == 0 {
                continue;
            }
            if resume.as_ref().is_some_and(|cursor| {
                cursor.root_index == index && entry.path() <= cursor.after.as_path()
            }) {
                continue;
            }
            if entry.error().is_some() {
                omit(
                    &mut omissions,
                    format!("ignore_rule_error:{}", root.display()),
                );
            }
            visited += 1;
            if entry.file_type().is_some_and(|kind| kind.is_symlink()) {
                omit(
                    &mut omissions,
                    format!("symlink_not_followed:{}", entry.path().display()),
                );
            } else if entry.file_type().is_some_and(|kind| kind.is_dir()) {
                match policy.ensure_resolved_path(entry.path()) {
                    Ok(path) => {
                        let value = directory(policy, &path, &input);
                        let searchable = format!(
                            "{} {} {}",
                            path.display(),
                            value["manifest_paths"],
                            value["documents"]
                        )
                        .to_lowercase();
                        if query.is_empty() || searchable.contains(&query) {
                            entries.push(value);
                        }
                    }
                    Err(_) => omit(
                        &mut omissions,
                        format!("path_not_readable:{}", entry.path().display()),
                    ),
                }
            }
            if entries.len() >= page_size
                || visited >= 10000
                || started.elapsed() >= Duration::from_secs(5)
            {
                next_cursor = Some(
                    serde_json::to_string(&DirectoryCursor {
                        version: 1,
                        binding: binding.clone(),
                        root_index: index,
                        after: entry.path().into(),
                        omissions: omissions.clone(),
                    })
                    .map_err(|e| e.to_string())?,
                );
                break 'roots;
            }
        }
    }
    let root_entries = roots
        .iter()
        .map(|root| directory(policy, root, &input))
        .collect::<Vec<_>>();
    let include_files = input.include_files.unwrap_or(true);
    let max_files = input.max_files.unwrap_or(500).clamp(1, 5000);
    let mut files = Vec::new();
    let mut next_requests = Vec::new();
    let mut scan_complete = include_files;
    // Directory continuation pages do not replay the original file pages.
    if include_files && resume.is_none() {
        for root in &roots {
            if files.len() >= max_files {
                scan_complete = false;
                next_requests.push(request("glob_search", json!({"pattern": "**/*", "path": root, "include_ignored": input.options.include_ignored, "ignore_patterns": input.options.ignore_patterns})));
                continue;
            }
            let page = crate::file_ops::glob_search_page(
                policy,
                "**/*",
                Some(&root.to_string_lossy()),
                None,
                &input.options,
                max_files - files.len(),
            )
            .map_err(io_to_string)?;
            scan_complete &= page.scan_complete;
            files.extend(page.filenames);
            for reason in page.omissions {
                omit(&mut omissions, format!("{reason}:{}", root.display()));
            }
            if let Some(next) = page.next_request {
                next_requests.push(request("glob_search", next));
            }
        }
    } else {
        scan_complete = false;
    }
    if let Some(cursor) = &next_cursor {
        let mut next = serde_json::to_value(&input).map_err(|e| e.to_string())?;
        next.as_object_mut()
            .expect("object input")
            .retain(|_, value| !value.is_null());
        next["cursor"] = json!(cursor);
        next["include_files"] = json!(false);
        next_requests.insert(0, request("workspace_snapshot", next));
    }
    to_pretty_json(
        json!({"type": "workspace_snapshot", "cwd": policy.workspace_root(), "resolvedRoots": roots,
        "git": if input.include_git.unwrap_or(true) { Some(root_entries.iter().map(|entry| json!({"root": entry["root"], "git": entry["git"]})).collect::<Vec<_>>()) } else { None },
        "files": if include_files && resume.is_none() { Some(files) } else { None }, "maxFiles": max_files, "scanComplete": scan_complete,
        "roots": root_entries, "directories": entries, "directoryScanComplete": next_cursor.is_none() && omissions.is_empty(),
        "next_cursor": next_cursor, "next_requests": next_requests, "omissions": omissions,
        "coverage": {"consistency": "live_filesystem", "directory_depth": 1, "nested_directories": "follow read_request",
            "query_scope": "directory_paths_and_document_names", "include_ignored": input.options.include_ignored, "ignore_patterns": input.options.ignore_patterns}}),
    )
}
