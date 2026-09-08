//! Bounded, resumable discovery over a live filesystem. Project ignore rules
//! shape the search scope; no filename blacklist can silently hide a project.
use super::*;
use glob::Pattern;
use regex::RegexBuilder;
use std::time::Instant;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchCursor {
    version: u8,
    root: PathBuf,
    query_hash: String,
    after: PathBuf,
    #[serde(default)]
    file_hash: Option<String>,
    #[serde(default)]
    row: usize,
    #[serde(default)]
    omissions: Vec<String>,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
fn digest(value: &impl Serialize) -> io::Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).map_err(io::Error::other)?)
    ))
}
fn decode(cursor: Option<&str>, root: &Path, query_hash: &str) -> io::Result<Option<SearchCursor>> {
    let cursor = cursor
        .map(|value| {
            serde_json::from_str::<SearchCursor>(value)
                .map_err(|_| invalid("invalid search cursor; copy continuationCursor"))
        })
        .transpose()?;
    if cursor.as_ref().is_some_and(|value| {
        value.version != 2
            || value.root != root
            || value.query_hash != query_hash
            || !value.after.starts_with(root)
    }) {
        return Err(invalid(
            "search cursor does not match root, query or options; start a new search",
        ));
    }
    Ok(cursor)
}
fn encode(
    root: &Path,
    query_hash: &str,
    after: PathBuf,
    file_hash: Option<String>,
    row: usize,
    omissions: &[String],
) -> io::Result<String> {
    serde_json::to_string(&SearchCursor {
        version: 2,
        root: root.into(),
        query_hash: query_hash.into(),
        after,
        file_hash,
        row,
        omissions: omissions.to_vec(),
    })
    .map_err(io::Error::other)
}
fn omit(omissions: &mut Vec<String>, reason: &str) {
    if !omissions.iter().any(|item| item == reason) {
        omissions.push(reason.into());
    }
}
fn walker(
    policy: &WorkspacePathPolicy,
    root: &Path,
    explicit_root: bool,
    options: &SearchOptions,
    resume: Option<&SearchCursor>,
    omissions: &mut Vec<String>,
) -> io::Result<ignore::Walk> {
    let mut overrides = ignore::overrides::OverrideBuilder::new(root);
    for pattern in &options.ignore_patterns {
        if pattern.is_empty() || pattern.starts_with('!') {
            return Err(invalid("ignore_patterns must be nonempty exclusion globs; use include_ignored to override project rules"));
        }
        overrides
            .add(&format!("!{pattern}"))
            .map_err(|e| invalid(e.to_string()))?;
    }
    let after = resume.map(|cursor| cursor.after.clone());
    let inclusive = resume.is_some_and(|cursor| cursor.file_hash.is_some());
    let mut builder = ignore::WalkBuilder::new(root);
    // Pattern-derived subroots retain project rules, but never read ignore
    // files from ancestors outside the authorized workspace. An explicitly
    // selected subdirectory is an intentional narrowing/override of ancestors.
    if !options.include_ignored && !explicit_root {
        let mut ancestors = root
            .parent()
            .into_iter()
            .flat_map(|parent| parent.ancestors())
            .take_while(|parent| parent.starts_with(policy.workspace_root()))
            .collect::<Vec<_>>();
        ancestors.reverse();
        for ancestor in ancestors {
            builder.current_dir(ancestor);
            for name in [".gitignore", ".ignore", ".git/info/exclude"] {
                let path = ancestor.join(name);
                match path.try_exists() {
                    Ok(true) => {
                        if builder.add_ignore(&path).is_some() {
                            omit(omissions, "ignore_rules_invalid_or_unavailable");
                        }
                    }
                    Ok(false) => {}
                    Err(_) => omit(omissions, "ignore_rules_unavailable"),
                }
            }
        }
    }
    builder.current_dir(root);
    builder
        .standard_filters(!options.include_ignored)
        .parents(false)
        .git_global(false)
        .require_git(false)
        .follow_links(false)
        .max_depth(Some(MAX_SEARCH_DEPTH))
        .sort_by_file_name(|a, b| a.cmp(b))
        .overrides(overrides.build().map_err(|e| invalid(e.to_string()))?)
        .filter_entry(move |entry| {
            after.as_ref().is_none_or(|after| {
                entry.path() > after.as_path()
                    || (inclusive && entry.path() == after.as_path())
                    || (entry.file_type().is_some_and(|kind| kind.is_dir())
                        && after.starts_with(entry.path()))
            })
        });
    Ok(builder.build())
}
fn non_glob_root(pattern: &Path, fallback: &Path) -> PathBuf {
    let mut root = PathBuf::new();
    for component in pattern.components() {
        if component
            .as_os_str()
            .to_string_lossy()
            .chars()
            .any(|ch| matches!(ch, '*' | '?' | '[' | '{'))
        {
            break;
        }
        root.push(component);
    }
    if root.is_file() {
        root.pop();
    }
    if root.as_os_str().is_empty() {
        fallback.into()
    } else {
        root
    }
}

pub fn glob_search(
    policy: &WorkspacePathPolicy,
    pattern: &str,
    path: Option<&str>,
    cursor: Option<&str>,
) -> io::Result<GlobSearchOutput> {
    glob_search_with_options(policy, pattern, path, cursor, &SearchOptions::default())
}
pub fn glob_search_with_options(
    policy: &WorkspacePathPolicy,
    pattern: &str,
    path: Option<&str>,
    cursor: Option<&str>,
    options: &SearchOptions,
) -> io::Result<GlobSearchOutput> {
    glob_search_page(policy, pattern, path, cursor, options, MAX_SEARCH_RESULTS)
}

pub(crate) fn glob_search_page(
    policy: &WorkspacePathPolicy,
    pattern: &str,
    path: Option<&str>,
    cursor: Option<&str>,
    options: &SearchOptions,
    page_size: usize,
) -> io::Result<GlobSearchOutput> {
    let page_size = page_size.clamp(1, 5000);
    let started = Instant::now();
    let base = path
        .map(|path| policy.resolve(path))
        .transpose()?
        .unwrap_or_else(|| policy.workspace_root().into());
    let expression = policy.resolve_glob_pattern(&if Path::new(pattern).is_absolute() {
        pattern.to_string()
    } else {
        base.join(pattern).to_string_lossy().into_owned()
    })?;
    let expanded = expand_braces(&expression.to_string_lossy());
    let matchers = expanded
        .iter()
        .map(|expression| Pattern::new(expression).map_err(|e| invalid(e.to_string())))
        .collect::<io::Result<Vec<_>>>()?;
    let mut root = non_glob_root(Path::new(&expanded[0]), &base);
    for expression in &expanded[1..] {
        let other = non_glob_root(Path::new(expression), &base);
        while !other.starts_with(&root) {
            if !root.pop() {
                return Err(invalid("glob roots have no common ancestor"));
            }
        }
    }
    // Validate even a missing literal prefix. Never traverse an outside root.
    root = policy.resolve(&root.to_string_lossy())?;
    let query_hash = digest(&("glob", &expression, options))?;
    let resume = decode(cursor, &root, &query_hash)?;
    if resume
        .as_ref()
        .is_some_and(|value| value.file_hash.is_some())
    {
        return Err(invalid("glob cursor cannot carry a content position"));
    }
    let mut omissions = resume
        .as_ref()
        .map(|value| value.omissions.clone())
        .unwrap_or_default();
    let mut filenames = Vec::new();
    let mut visited = 0;
    let mut last = None;
    let mut truncated = false;
    for entry in walker(
        policy,
        &root,
        root == base && base != policy.workspace_root() && path.is_some(),
        options,
        resume.as_ref(),
        &mut omissions,
    )? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                omit(&mut omissions, "directory_read_error");
                continue;
            }
        };
        if entry.error().is_some() {
            omit(&mut omissions, "ignore_rules_invalid_or_unavailable");
        }
        if resume
            .as_ref()
            .is_some_and(|cursor| entry.path() <= cursor.after.as_path())
        {
            continue;
        }
        if visited >= MAX_SEARCH_ENTRIES
            || (visited > 0 && started.elapsed().as_millis() >= MAX_SEARCH_DURATION_MS)
        {
            truncated = true;
            break;
        }
        visited += 1;
        last = Some(entry.path().to_path_buf());
        let Some(kind) = entry.file_type() else {
            omit(&mut omissions, "file_type_unavailable");
            continue;
        };
        if kind.is_symlink() {
            omit(&mut omissions, "symlink_not_followed");
            continue;
        }
        if entry.depth() >= MAX_SEARCH_DEPTH && kind.is_dir() {
            omit(&mut omissions, "depth_limit");
            continue;
        }
        if !kind.is_file()
            || !matchers
                .iter()
                .any(|pattern| pattern.matches_path(entry.path()))
        {
            continue;
        }
        match policy.ensure_resolved_path(entry.path()) {
            Ok(path) => filenames.push(path.to_string_lossy().into_owned()),
            Err(_) => omit(&mut omissions, "path_not_readable"),
        }
        if filenames.len() == page_size {
            truncated = true;
            break;
        }
    }
    let continuation_cursor = if truncated {
        last.map(|after| encode(&root, &query_hash, after, None, 0, &omissions))
            .transpose()?
    } else {
        None
    };
    let next_request = continuation_cursor.as_ref().map(|cursor| serde_json::json!({"pattern": pattern, "path": base, "cursor": cursor, "include_ignored": options.include_ignored, "ignore_patterns": options.ignore_patterns}));
    Ok(GlobSearchOutput {
        base_path: base.to_string_lossy().into_owned(),
        pattern: pattern.into(),
        scan_complete: !truncated && omissions.is_empty(),
        duration_ms: started.elapsed().as_millis(),
        num_files: filenames.len(),
        filenames,
        truncated,
        continuation_cursor,
        omissions,
        next_request,
        coverage: serde_json::json!({"consistency": "live_filesystem", "include_ignored": options.include_ignored, "ignore_patterns": options.ignore_patterns}),
    })
}

fn matching_windows(
    contents: &str,
    lines: &[&str],
    regex: &regex::Regex,
    input: &GrepSearchInput,
) -> (Vec<(usize, usize)>, usize) {
    let context = input.context.or(input.context_short).unwrap_or(0);
    let before = input.before.unwrap_or(context);
    let after = input.after.unwrap_or(context);
    let mut windows: Vec<(usize, usize)> = Vec::new();
    let mut matches = 0;
    let mut add = |first: usize, last: usize, count: usize| {
        matches += count;
        let start = first.saturating_sub(before);
        let end = last
            .saturating_add(after)
            .saturating_add(1)
            .min(lines.len());
        if let Some(previous) = windows.last_mut().filter(|previous| start <= previous.1) {
            previous.1 = previous.1.max(end);
        } else if start < end {
            windows.push((start, end));
        }
    };
    if input.multiline.unwrap_or(false) {
        let mut offsets = vec![0usize];
        offsets.extend(contents.match_indices('\n').map(|(offset, _)| offset + 1));
        for matched in regex.find_iter(contents) {
            let first = offsets
                .partition_point(|offset| *offset <= matched.start())
                .saturating_sub(1);
            let last = offsets
                .partition_point(|offset| {
                    *offset <= matched.end().saturating_sub(1).max(matched.start())
                })
                .saturating_sub(1);
            add(first, last, 1);
        }
    } else {
        for (index, line) in lines.iter().enumerate() {
            let count = regex.find_iter(line).count();
            if count > 0 {
                add(index, index, count);
            }
        }
    }
    (windows, matches)
}

pub fn grep_search(
    policy: &WorkspacePathPolicy,
    input: &GrepSearchInput,
) -> io::Result<GrepSearchOutput> {
    let pattern = input
        .pattern
        .as_deref()
        .filter(|pattern| !pattern.trim().is_empty())
        .ok_or_else(|| invalid("pattern is required"))?;
    let base = input
        .path
        .as_deref()
        .map(|path| policy.resolve(path))
        .transpose()?
        .unwrap_or_else(|| policy.workspace_root().into());
    let mode = input.output_mode.as_deref().unwrap_or("content");
    if !matches!(mode, "content" | "files_with_matches" | "count") {
        return Err(invalid(
            "output_mode must be content, files_with_matches or count",
        ));
    }
    if input.head_limit == Some(0) {
        return Err(invalid(
            "head_limit must be positive; use next_request for more results",
        ));
    }
    if input.cursor.is_some() && input.offset.unwrap_or(0) != 0 {
        return Err(invalid(
            "offset cannot be combined with cursor; use next_request",
        ));
    }
    let limit = input.head_limit.unwrap_or(250).min(10_000);
    let regex = RegexBuilder::new(pattern)
        .case_insensitive(input.case_insensitive.unwrap_or(false))
        .multi_line(true)
        .dot_matches_new_line(input.multiline.unwrap_or(false))
        .build()
        .map_err(|e| invalid(e.to_string()))?;
    let filter = input
        .glob
        .as_deref()
        .map(|glob| Pattern::new(glob).map_err(|e| invalid(e.to_string())))
        .transpose()?;
    let mut canonical = input.clone();
    canonical.path = Some(base.to_string_lossy().into_owned());
    canonical.cursor = None;
    canonical.offset = None;
    let query_hash = digest(&("grep", &canonical))?;
    let resume = decode(input.cursor.as_deref(), &base, &query_hash)?;
    let mut remaining_skip = resume
        .as_ref()
        .filter(|value| value.file_hash.is_none())
        .map_or(input.offset.unwrap_or(0), |value| value.row);
    let mut omissions = resume
        .as_ref()
        .map(|value| value.omissions.clone())
        .unwrap_or_default();
    let mut filenames = Vec::new();
    let mut output = Vec::new();
    let mut matches = 0usize;
    let mut visited = 0;
    let mut bytes_out = 0;
    let mut last = None;
    let mut next_cursor = None;
    let started = Instant::now();
    let mut resumed_file = resume
        .as_ref()
        .is_none_or(|value| value.file_hash.is_none());
    for entry in walker(
        policy,
        &base,
        base != policy.workspace_root() && input.path.is_some(),
        &input.search_options,
        resume.as_ref(),
        &mut omissions,
    )? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => {
                omit(&mut omissions, "directory_read_error");
                continue;
            }
        };
        if entry.error().is_some() {
            omit(&mut omissions, "ignore_rules_invalid_or_unavailable");
        }
        if resume.as_ref().is_some_and(|cursor| {
            entry.path() < cursor.after.as_path()
                || (cursor.file_hash.is_none() && entry.path() == cursor.after.as_path())
        }) {
            continue;
        }
        if visited >= MAX_SEARCH_ENTRIES
            || (visited > 0 && started.elapsed().as_millis() >= MAX_SEARCH_DURATION_MS)
        {
            next_cursor = last
                .map(|path| encode(&base, &query_hash, path, None, remaining_skip, &omissions))
                .transpose()?;
            break;
        }
        visited += 1;
        last = Some(entry.path().to_path_buf());
        let Some(kind) = entry.file_type() else {
            omit(&mut omissions, "file_type_unavailable");
            continue;
        };
        if kind.is_symlink() {
            omit(&mut omissions, "symlink_not_followed");
            continue;
        }
        if entry.depth() >= MAX_SEARCH_DEPTH && kind.is_dir() {
            omit(&mut omissions, "depth_limit");
            continue;
        }
        if !kind.is_file() {
            continue;
        }
        let path = match policy.ensure_resolved_path(entry.path()) {
            Ok(path) => path,
            Err(_) => {
                omit(&mut omissions, "path_not_readable");
                continue;
            }
        };
        if filter.as_ref().is_some_and(|glob| {
            !glob.matches_path(&path)
                && !glob.matches_path(path.strip_prefix(&base).unwrap_or(&path))
        }) || input
            .file_type
            .as_deref()
            .is_some_and(|kind| path.extension().and_then(|value| value.to_str()) != Some(kind))
        {
            continue;
        }
        if fs::metadata(&path)
            .map(|metadata| metadata.len() > MAX_READ_SIZE)
            .unwrap_or(true)
        {
            omit(&mut omissions, "file_too_large_or_unavailable");
            continue;
        }
        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(_) => {
                omit(&mut omissions, "file_not_readable_as_text");
                continue;
            }
        };
        let hash = format!("{:x}", Sha256::digest(contents.as_bytes()));
        let file_resume = resume
            .as_ref()
            .filter(|value| value.file_hash.is_some() && value.after == entry.path());
        let row_offset = if let Some(cursor) = file_resume {
            if cursor.file_hash.as_deref() != Some(hash.as_str()) {
                return Err(invalid(
                    "source_version_conflict: resumed grep file changed",
                ));
            }
            resumed_file = true;
            cursor.row
        } else {
            0
        };
        let lines = contents.lines().collect::<Vec<_>>();
        let (windows, match_count) = matching_windows(&contents, &lines, &regex, input);
        if match_count == 0 {
            continue;
        }
        if mode != "content" {
            if remaining_skip > 0 {
                remaining_skip -= 1;
                continue;
            }
            filenames.push(path.to_string_lossy().into_owned());
            matches += match_count;
            if filenames.len() == limit {
                next_cursor = Some(encode(
                    &base,
                    &query_hash,
                    entry.path().into(),
                    None,
                    0,
                    &omissions,
                )?);
                break;
            }
            continue;
        }
        let row_count = windows
            .iter()
            .map(|(start, end)| end - start)
            .sum::<usize>();
        if row_offset > row_count {
            return Err(invalid("grep cursor row is out of range"));
        }
        for (row, index) in windows
            .into_iter()
            .flat_map(|(start, end)| start..end)
            .enumerate()
            .skip(row_offset)
        {
            if remaining_skip > 0 {
                remaining_skip -= 1;
                continue;
            }
            let prefix = if input.line_numbers.unwrap_or(true) {
                format!("{}:{}:", path.display(), index + 1)
            } else {
                format!("{}:", path.display())
            };
            let value = format!("{prefix}{}", lines[index]);
            if output.len() == limit
                || (!output.is_empty() && bytes_out + value.len() > MAX_READ_SIZE as usize)
            {
                next_cursor = Some(encode(
                    &base,
                    &query_hash,
                    entry.path().into(),
                    Some(hash.clone()),
                    row,
                    &omissions,
                )?);
                break;
            }
            if filenames
                .last()
                .is_none_or(|file| file != &path.to_string_lossy())
            {
                filenames.push(path.to_string_lossy().into_owned());
            }
            bytes_out += value.len();
            output.push(value);
        }
        if next_cursor.is_some() {
            break;
        }
        if output.len() == limit {
            next_cursor = Some(encode(
                &base,
                &query_hash,
                entry.path().into(),
                None,
                0,
                &omissions,
            )?);
            break;
        }
    }
    if !resumed_file {
        return Err(invalid(
            "source_version_conflict: resumed grep file is no longer readable in this search",
        ));
    }
    let next_request = next_cursor.as_ref().map(|cursor| {
        let mut next = canonical.clone();
        next.cursor = Some(cursor.clone());
        let mut value = serde_json::to_value(next).unwrap_or_default();
        if let Some(object) = value.as_object_mut() {
            object.retain(|_, value| !value.is_null());
        }
        value
    });
    let truncated = next_cursor.is_some();
    Ok(GrepSearchOutput {
        base_path: base.to_string_lossy().into_owned(),
        scan_complete: !truncated && omissions.is_empty(),
        mode: Some(mode.into()),
        num_files: filenames.len(),
        filenames,
        content: (mode == "content").then(|| output.join("\n")),
        num_lines: (mode == "content").then_some(output.len()),
        num_matches: (mode == "count").then_some(matches),
        applied_limit: truncated.then_some(limit),
        applied_offset: input.offset.filter(|offset| *offset > 0),
        continuation_cursor: next_cursor,
        omissions,
        truncated,
        next_request,
        coverage: serde_json::json!({"consistency": "live_filesystem", "resumed_file_hash_checked": resume.as_ref().is_some_and(|cursor| cursor.file_hash.is_some()),
            "include_ignored": input.search_options.include_ignored, "ignore_patterns": input.search_options.ignore_patterns}),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!("cowd-search-{id}"));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
        fn policy(&self) -> WorkspacePathPolicy {
            WorkspacePathPolicy::new(&self.0)
        }
        fn write(&self, path: &str, content: &str) {
            let path = self.0.join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, content).unwrap();
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn request(value: serde_json::Value) -> GrepSearchInput {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn grep_pages_preserve_single_file_unicode_context_and_source_fences() {
        let fixture = Fixture::new();
        let policy = fixture.policy();
        let body = (0..800)
            .map(|index| {
                if index % 3 == 0 {
                    format!("needle 中文 {index}\n")
                } else {
                    format!("context {index}\n")
                }
            })
            .collect::<String>();
        fixture.write("report.txt", &body);
        let reference = grep_search(
            &policy,
            &request(serde_json::json!({"pattern":"needle", "context":1,"head_limit":10000})),
        )
        .unwrap();
        let expected = reference
            .content
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect::<Vec<_>>();
        assert!(reference.scan_complete);
        let mut input =
            request(serde_json::json!({"pattern":"needle", "context":1,"head_limit":7}));
        let mut actual = Vec::new();
        let mut pages = 0;
        loop {
            let page = grep_search(&policy, &input).unwrap();
            pages += 1;
            assert!(pages < 150);
            assert!(page.num_lines.unwrap() <= 7);
            actual.extend(page.content.unwrap().lines().map(str::to_string));
            let Some(next) = page.next_request else {
                assert!(page.scan_complete);
                break;
            };
            assert!(!page.scan_complete);
            input = request(next);
            if pages == 1 {
                let mut changed_query = input.clone();
                changed_query.pattern = Some("context".into());
                assert!(grep_search(&policy, &changed_query).is_err());
                let mut changed_options = input.clone();
                changed_options.search_options.include_ignored = true;
                assert!(grep_search(&policy, &changed_options).is_err());
                fixture.write("report.txt", &format!("changed\n{body}"));
                assert!(grep_search(&policy, &input)
                    .unwrap_err()
                    .to_string()
                    .contains("source_version_conflict"));
                fixture.write("report.txt", &body);
            }
        }
        assert_eq!(actual, expected);
        assert_eq!(
            actual
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            actual.len(),
            "overlapping contexts are not repeated"
        );
    }

    #[test]
    fn grep_directory_pages_follow_component_order_and_multiline_is_real() {
        let fixture = Fixture::new();
        let policy = fixture.policy();
        let mut expected = std::collections::BTreeSet::new();
        for index in 0..131 {
            let relative = if index % 2 == 0 {
                format!("a/{index:04}.rs")
            } else {
                format!("a.{index:04}.rs")
            };
            fixture.write(&relative, "needle\nneedle\n");
            expected.insert(fixture.0.join(relative).to_string_lossy().into_owned());
        }
        let mut input =
            request(serde_json::json!({"pattern":"needle", "output_mode":"count", "head_limit":9}));
        let mut actual = std::collections::BTreeSet::new();
        let mut matches = 0;
        let mut pages = 0;
        loop {
            let page = grep_search(&policy, &input).unwrap();
            pages += 1;
            assert!(pages < 20);
            matches += page.num_matches.unwrap();
            for file in page.filenames {
                assert!(actual.insert(file));
            }
            match page.next_request {
                Some(next) => input = request(next),
                None => {
                    assert!(page.scan_complete);
                    break;
                }
            }
        }
        assert_eq!(actual, expected);
        assert_eq!(matches, 262);
        fixture.write("multi.txt", "before\nstart\nend\nafter\n");
        let multi = request(
            serde_json::json!({"pattern":"start\\nend", "path":"multi.txt", "multiline":true}),
        );
        assert_eq!(grep_search(&policy, &multi).unwrap().num_lines, Some(2));
        let mut single = multi;
        single.multiline = Some(false);
        assert_eq!(grep_search(&policy, &single).unwrap().num_lines, Some(0));
    }

    #[test]
    fn project_ignore_rules_negation_and_explicit_overrides_are_respected() {
        let fixture = Fixture::new();
        let policy = fixture.policy();
        fixture.write(".gitignore", "build/\n*.tmp\n!keep.tmp\nnested/skip.rs\n");
        fixture.write("visible.rs", "needle");
        fixture.write("drop.tmp", "needle");
        fixture.write("keep.tmp", "needle");
        fixture.write("build/generated.rs", "needle");
        fixture.write("nested/skip.rs", "needle");
        fixture.write("nested/.ignore", "private.rs\n");
        fixture.write("nested/private.rs", "needle");
        fixture.write("nested/note.rs", "needle");
        let page = glob_search(&policy, "**/*", None, None).unwrap();
        assert_eq!(page.num_files, 3);
        assert!(page.filenames.iter().any(|name| name.ends_with("keep.tmp")));
        let nested = glob_search(&policy, "nested/*.rs", None, None).unwrap();
        assert_eq!(nested.num_files, 1);
        assert!(nested.filenames[0].ends_with("note.rs"));
        let explicit = glob_search(&policy, "*.rs", Some("build"), None).unwrap();
        assert_eq!(explicit.num_files, 1);
        let all = glob_search_with_options(
            &policy,
            "**/*.rs",
            None,
            None,
            &SearchOptions {
                include_ignored: true,
                ignore_patterns: vec!["**/private.rs".into()],
            },
        )
        .unwrap();
        assert_eq!(all.num_files, 4);
        assert!(!all
            .filenames
            .iter()
            .any(|name| name.ends_with("private.rs")));
        let grep = grep_search(
            &policy,
            &request(serde_json::json!({"pattern":"needle","output_mode":"files_with_matches"})),
        )
        .unwrap();
        assert_eq!(grep.num_files, 3);
    }

    #[test]
    fn search_next_requests_pass_real_tool_schema_and_do_not_reuse_stale_cache() {
        let fixture = Fixture::new();
        fixture.write("one.txt", "needle\nneedle\nneedle\n");
        let host = crate::ToolHost::builtin("search-contract", &fixture.0);
        let lease = host.pin_snapshot();
        let input = serde_json::json!({"pattern":"needle","head_limit":1});
        let output: serde_json::Value = serde_json::from_str(
            &crate::executor::execute_with_lease(&lease, "grep_search", &input).unwrap(),
        )
        .unwrap();
        lease
            .validate_input("grep_search", &output["next_request"])
            .unwrap();
        lease
            .validate_input(
                "grep_many",
                &serde_json::json!({"searches":[output["next_request"].clone()]}),
            )
            .unwrap();
        fixture.write("one.txt", "changed source\n");
        assert!(crate::executor::execute_with_lease(
            &lease,
            "grep_search",
            &output["next_request"]
        )
        .is_err());
        let fresh: serde_json::Value = serde_json::from_str(
            &crate::executor::execute_with_lease(&lease, "grep_search", &input).unwrap(),
        )
        .unwrap();
        assert_eq!(fresh["numLines"], 0);
        for index in 0..101 {
            fixture.write(&format!("file-{index}.rs"), "x");
        }
        let first: serde_json::Value = serde_json::from_str(
            &crate::executor::execute_with_lease(
                &lease,
                "glob_search",
                &serde_json::json!({"pattern":"*.rs"}),
            )
            .unwrap(),
        )
        .unwrap();
        lease
            .validate_input("glob_search", &first["next_request"])
            .unwrap();
        lease
            .validate_input(
                "glob_many",
                &serde_json::json!({"patterns":[first["next_request"].clone()]}),
            )
            .unwrap();
        fixture.write(".gitignore", "*.rs\n");
        let fresh: serde_json::Value = serde_json::from_str(
            &crate::executor::execute_with_lease(
                &lease,
                "glob_search",
                &serde_json::json!({"pattern":"*.rs"}),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(fresh["numFiles"], 0);
    }
}
