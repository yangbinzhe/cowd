//! Independent, pre-admission numeric oracle for the revised K02 facets.
//! This module deliberately does not call tool or production reduction code.
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

pub const VERSION: &str = "k02-numeric-v4.2";
pub const SOURCES: [&str; 6] = [
    "crates/tools/src/lib.rs",
    "crates/runtime/src/lib.rs",
    "crates/memory/src/lib.rs",
    "crates/gateway/src/main.rs",
    "crates/provider/src/lib.rs",
    "crates/harness-eval/src/lib.rs",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub files: Vec<(String, String)>,
    pub contents: Vec<(String, String)>,
    pub file_count: usize,
    pub total_lines: usize,
    pub top: BTreeMap<String, usize>,
    pub facts: [usize; 7],
}
/// The caller chooses a fresh private file outside the evaluated workspace.
/// The file and its directory are synced before the objective is dispatched.
pub fn persist_preregistration(
    path: &Path,
    root: &Path,
    snapshot: &Snapshot,
) -> Result<(), String> {
    use std::io::Write;
    let parent = path
        .parent()
        .ok_or("preregistration parent missing")?
        .canonicalize()
        .map_err(|e| e.to_string())?;
    let workspace = root.canonicalize().map_err(|e| e.to_string())?;
    if parent.starts_with(&workspace) {
        return Err("preregistration must be outside workspace".into());
    }
    let value = serde_json::json!({"version": VERSION, "workspace": workspace, "files": snapshot.files, "contents": snapshot.contents,
        "file_count": snapshot.file_count, "total_lines": snapshot.total_lines, "top": snapshot.top, "batch_facts": snapshot.facts});
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(|e| e.to_string())?;
    file.write_all(&serde_json::to_vec(&value).map_err(|e| e.to_string())?)
        .and_then(|_| file.sync_all())
        .map_err(|e| e.to_string())?;
    fs::File::open(parent)
        .and_then(|d| d.sync_all())
        .map_err(|e| e.to_string())?;
    Ok(())
}
fn lines(s: &str) -> usize {
    s.lines().count()
}
fn read(root: &Path, relative: &str) -> Result<String, String> {
    let p = root
        .join(relative)
        .canonicalize()
        .map_err(|e| e.to_string())?;
    if !p.starts_with(root) {
        return Err(format!("input escapes workspace: {relative}"));
    }
    fs::read_to_string(p).map_err(|e| format!("{relative}: {e}"))
}
fn walk(
    root: &Path,
    path: &Path,
    stack: &mut BTreeSet<PathBuf>,
    out: &mut Vec<(String, String)>,
) -> Result<(), String> {
    let canonical = path.canonicalize().map_err(|e| e.to_string())?;
    if !canonical.starts_with(root) {
        return Ok(());
    }
    let meta = fs::metadata(&canonical).map_err(|e| e.to_string())?;
    if meta.is_dir() {
        if !stack.insert(canonical.clone()) {
            return Err(format!("directory symlink cycle: {}", path.display()));
        }
        let mut entries = fs::read_dir(path)
            .map_err(|e| e.to_string())?
            .map(|x| x.map(|v| v.path()).map_err(|e| e.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        entries.sort();
        for entry in entries {
            walk(root, &entry, stack, out)?;
        }
        stack.remove(&canonical);
    } else if meta.is_file() && path.extension().is_some_and(|x| x == "rs") {
        let rel = path
            .strip_prefix(root)
            .map_err(|e| e.to_string())?
            .to_str()
            .ok_or("non UTF-8 path")?
            .replace('\\', "/");
        let bytes = fs::read(&canonical).map_err(|e| e.to_string())?;
        let text = String::from_utf8(bytes).map_err(|e| format!("{rel}: {e}"))?;
        out.push((rel, text));
    }
    Ok(())
}
// Count source declarations, not names expanded by a grouped use. Only
// unindented declarations count: this is explicit in the revised prompt.
fn declarations(s: &str, prefixes: &[&str]) -> usize {
    s.lines()
        .filter(|line| prefixes.iter().any(|prefix| line.starts_with(prefix)))
        .count()
}
fn members(s: &str) -> Result<usize, String> {
    let mut section = false;
    let mut array = None::<String>;
    for original in s.lines() {
        // Avoid guessing TOML semantics for quoted hashes or multiline strings.
        if original.contains("\"\"\"")
            || (original.contains('#')
                && original
                    .split('#')
                    .next()
                    .unwrap_or("")
                    .matches('"')
                    .count()
                    % 2
                    != 0)
        {
            return Err("unsupported workspace manifest string".into());
        }
        let line = original.split('#').next().unwrap_or("").trim();
        if array.is_none() && line.starts_with('[') {
            section = line == "[workspace]";
            continue;
        }
        if section && array.is_none() {
            if let Some((key, rhs)) = line.split_once('=') {
                if key.trim() == "members" {
                    let rhs = rhs
                        .trim()
                        .strip_prefix('[')
                        .ok_or("invalid members array")?;
                    array = Some(rhs.to_string());
                }
            }
        } else if let Some(body) = array.as_mut() {
            body.push_str(line);
        }
        if let Some(body) = &array {
            if let Some((items, tail)) = body.split_once(']') {
                if !tail.trim().is_empty() {
                    return Err("unsupported members suffix".into());
                }
                let mut count = 0;
                for item in items.split(',').map(str::trim).filter(|v| !v.is_empty()) {
                    let item = item
                        .strip_prefix('"')
                        .and_then(|v| v.strip_suffix('"'))
                        .ok_or("unsupported member literal")?;
                    if item.is_empty() || item.contains(['*', '?', '[', ']', '\\', '"']) {
                        return Err("unsupported workspace member expansion".into());
                    }
                    count += 1;
                }
                return Ok(count);
            }
        }
    }
    Err("workspace members array missing".into())
}
fn crate_name(
    root: &Path,
    relative: &str,
    manifests: &mut BTreeMap<String, String>,
) -> Result<Option<String>, String> {
    if !relative.starts_with("crates/") {
        return Ok(None);
    }
    let mut dir = Path::new(relative).parent();
    while let Some(d) = dir {
        if !d.starts_with("crates") || d == Path::new("crates") {
            break;
        }
        let manifest = root.join(d).join("Cargo.toml");
        if manifest.is_file() {
            let canon = manifest.canonicalize().map_err(|e| e.to_string())?;
            if !canon.starts_with(root) {
                return Err("crate manifest escapes workspace".into());
            }
            let text = fs::read_to_string(canon).map_err(|e| e.to_string())?;
            let manifest_path = manifest
                .strip_prefix(root)
                .map_err(|e| e.to_string())?
                .to_str()
                .ok_or("non UTF-8 manifest path")?
                .replace('\\', "/");
            manifests.insert(manifest_path, text.clone());
            let mut package = false;
            for line in text.lines() {
                let line = line.split('#').next().unwrap_or("").trim();
                if line.starts_with('[') {
                    package = line == "[package]";
                } else if package {
                    if let Some((key, value)) = line.split_once('=') {
                        if key.trim() == "name" {
                            return Ok(Some(value.trim().trim_matches('"').to_string()));
                        }
                    }
                }
            }
            return Err(format!("package name missing: {}", manifest.display()));
        }
        dir = d.parent();
    }
    Ok(None)
}
impl Snapshot {
    pub fn capture(root: &Path) -> Result<Self, String> {
        let root = root.canonicalize().map_err(|e| e.to_string())?;
        let mut content = Vec::new();
        walk(&root, &root, &mut BTreeSet::new(), &mut content)?;
        content.sort_by(|a, b| a.0.cmp(&b.0));
        let mut files = Vec::new();
        let mut total_lines = 0;
        let mut counts = BTreeMap::<String, usize>::new();
        let mut manifests = BTreeMap::new();
        for (path, text) in &content {
            total_lines += lines(text);
            if let Some(name) = crate_name(&root, path, &mut manifests)? {
                *counts.entry(name).or_default() += 1;
            }
            files.push((
                path.clone(),
                format!("{:x}", Sha256::digest(text.as_bytes())),
            ));
        }
        let max = counts
            .values()
            .copied()
            .max()
            .ok_or("no crate Rust files")?;
        let top = counts
            .into_iter()
            .filter(|(_, count)| *count == max)
            .collect();
        let source = SOURCES
            .map(|p| read(&root, p))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let cargo = read(&root, "Cargo.toml")?;
        let facts = [
            members(&cargo)?,
            declarations(&source[0], &["pub mod "]),
            declarations(&source[1], &["pub mod ", "pub use "]),
            declarations(&source[2], &["mod ", "pub mod ", "pub(crate) mod "]),
            lines(&source[3]),
            declarations(&source[4], &["pub use "]),
            lines(&source[5]),
        ];
        // Include the root manifest identity in the frozen source manifest.
        files.push((
            "Cargo.toml".into(),
            format!("{:x}", Sha256::digest(cargo.as_bytes())),
        ));
        let file_count = content.len();
        let mut contents = content;
        contents.push(("Cargo.toml".into(), cargo));
        for (path, text) in manifests {
            files.push((
                path.clone(),
                format!("{:x}", Sha256::digest(text.as_bytes())),
            ));
            contents.push((path, text));
        }
        files.sort();
        contents.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(Self {
            files,
            contents,
            file_count,
            total_lines,
            top,
            facts,
        })
    }
}

// Only complete, unambiguous answers are accepted. The structured form is a
// JSON object with exactly the documented fields. The equivalent text form is
// one `key: value` (or Markdown table row) per fact. Duplicate, unknown or
// contradictory fields, prose and trailing material fail closed.
#[derive(serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ComputeAnswer {
    total_lines: usize,
    rs_files: usize,
    top_crate: String,
    top_crate_files: usize,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchAnswer {
    #[serde(rename = "Cargo.toml")]
    cargo: usize,
    #[serde(rename = "crates/tools/src/lib.rs")]
    tools: usize,
    #[serde(rename = "crates/runtime/src/lib.rs")]
    runtime: usize,
    #[serde(rename = "crates/memory/src/lib.rs")]
    memory: usize,
    #[serde(rename = "crates/gateway/src/main.rs")]
    gateway: usize,
    #[serde(rename = "crates/provider/src/lib.rs")]
    provider: usize,
    #[serde(rename = "crates/harness-eval/src/lib.rs")]
    harness: usize,
}
fn unquote(s: &str) -> Option<&str> {
    let s = s.trim();
    if let Some(rest) = s
        .strip_prefix("```json\n")
        .or_else(|| s.strip_prefix("```\n"))
    {
        Some(rest.strip_suffix("```")?.trim())
    } else {
        Some(s)
    }
}
fn decimal(s: &str) -> Option<usize> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if s.contains(',') {
        let mut groups = s.split(',');
        let first = groups.next()?;
        if first.is_empty() || first.len() > 3 || !first.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        if groups.any(|g| g.len() != 3 || !g.bytes().all(|b| b.is_ascii_digit())) {
            return None;
        }
    } else if !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.replace(',', "").parse().ok()
}
fn text_fields(answer: &str, compute: bool) -> Option<BTreeMap<String, String>> {
    let mut fields = BTreeMap::new();
    for raw in answer.lines() {
        let raw = raw.trim();
        let raw = raw.strip_prefix("- ").or_else(|| raw.strip_prefix("* ")).unwrap_or(raw);
        if raw.is_empty() {
            continue;
        }
        let (key, value) = if raw.starts_with('|') && raw.ends_with('|') {
            let mut cells = raw[1..raw.len() - 1].split('|');
            let key = cells.next()?.trim();
            let value = cells.next()?.trim();
            if cells.next().is_some() {
                return None;
            }
            let separator = |cell: &str| {
                let cell = cell.trim().trim_matches(':');
                cell.len() >= 3 && cell.bytes().all(|b| b == b'-')
            };
            let header = matches!(key, "字段" | "指标" | "项目" | "路径" | "key" | "field" | "path")
                && matches!(value, "值" | "数值" | "结果" | "value" | "count");
            if (separator(key) && separator(value)) || header {
                continue;
            }
            (key, value)
        } else {
            raw.split_once(':').or_else(|| raw.split_once('：'))?
        };
        let key = key.trim().trim_matches('`').trim_matches('*');
        let key = if compute {
            match key {
                "总行数" | "lines" | "total_lines" => "total_lines",
                "文件数" | "文件数量" | "files" | "rs_files" => "rs_files",
                "最多crate" | "最多的crate" | "top crate" | "top_crate" => "top_crate",
                "最多crate文件数" | "top crate files" | "top_crate_files" => "top_crate_files",
                _ => return None,
            }
        } else if key == "Cargo.toml" || SOURCES.contains(&key) {
            key
        } else {
            return None;
        };
        if fields
            .insert(key.to_owned(), value.trim().trim_matches('`').trim_matches('*').to_owned())
            .is_some()
        {
            return None;
        }
    }
    Some(fields)
}
fn text_compute(answer: &str) -> Option<ComputeAnswer> {
    let mut f = text_fields(answer, true)?;
    let result = ComputeAnswer {
        total_lines: decimal(&f.remove("total_lines")?)?,
        rs_files: decimal(&f.remove("rs_files")?)?,
        top_crate: f.remove("top_crate")?,
        top_crate_files: decimal(&f.remove("top_crate_files")?)?,
    };
    f.is_empty().then_some(result)
}
fn text_batch(answer: &str) -> Option<BatchAnswer> {
    let mut f = text_fields(answer, false)?;
    let result = BatchAnswer {
        cargo: decimal(&f.remove("Cargo.toml")?)?,
        tools: decimal(&f.remove(SOURCES[0])?)?,
        runtime: decimal(&f.remove(SOURCES[1])?)?,
        memory: decimal(&f.remove(SOURCES[2])?)?,
        gateway: decimal(&f.remove(SOURCES[3])?)?,
        provider: decimal(&f.remove(SOURCES[4])?)?,
        harness: decimal(&f.remove(SOURCES[5])?)?,
    };
    f.is_empty().then_some(result)
}
// Accept a single fenced JSON answer with surrounding explanation. Other
// fenced candidates and explicit conflicting path/value claims remain errors.
fn explained_batch(answer: &str, facts: &[usize; 7]) -> Option<BatchAnswer> {
    let start = answer.find("```json\n").or_else(|| answer.find("```\n"))?;
    let open = if answer[start..].starts_with("```json\n") { 8 } else { 4 };
    let body_start = start + open;
    let close = body_start + answer[body_start..].find("```")?;
    let before = &answer[..start];
    let after = &answer[close + 3..];
    if before.contains("```") || after.contains("```") { return None; }
    let value: BatchAnswer = serde_json::from_str(answer[body_start..close].trim()).ok()?;
    let keys = ["Cargo.toml", SOURCES[0], SOURCES[1], SOURCES[2], SOURCES[3], SOURCES[4], SOURCES[5]];
    for line in before.lines().chain(after.lines()) {
        // Additional JSON keys or explicit named values are candidate answers,
        // not harmless prose. A matching claim may accompany the JSON.
        if keys.iter().any(|key| line.contains(&format!("\"{key}\""))) { return None; }
        let trimmed = line.trim().trim_start_matches("- ").trim_start_matches("* ");
        for (key, expected) in keys.iter().zip(facts) {
            if trimmed.starts_with('|') {
                let mut cells = trimmed.trim_matches('|').split('|');
                let first = cells.next()?.trim().trim_matches('`').trim_matches('*');
                if first != *key { continue; }
                let claim = cells.next()?.trim();
                if let Some((_, rest)) = claim.split_once("**") {
                    let (number, _) = rest.split_once("**")?;
                    if decimal(number)? != *expected { return None; }
                } else if let Some(number) = decimal(claim.trim_matches('`')) {
                    if number != *expected { return None; }
                }
            } else {
                if let Some(position) = trimmed.find(key) {
                    let rest = trimmed[position + key.len()..].trim_start_matches('`').trim();
                    let claim = [":", "：", "=", "是", "为", "结果为", "实际为", "数量为", "数量是", "计数为", "等于", "is "]
                        .iter().find_map(|prefix| rest.strip_prefix(prefix));
                    if let Some(claim) = claim {
                        let claim = claim.trim().trim_start_matches('*').trim_start_matches('`');
                        let token: String = claim.chars().take_while(|c| c.is_ascii_digit() || *c == ',').collect();
                        if !token.is_empty() {
                            let trailing_comma = token.ends_with(',');
                            let number = token.trim_end_matches(',');
                            if decimal(number)? != *expected { return None; }
                            let rest = claim[token.len()..].trim_start_matches('*').trim_start_matches('`').trim();
                            let alternative = ["或者", "或", "or ", "/", "，", ","]
                                .iter().find_map(|prefix| rest.strip_prefix(prefix))
                                .or_else(|| trailing_comma.then_some(rest));
                            if let Some(alternative) = alternative {
                                let alternative = alternative.trim().trim_start_matches('*').trim_start_matches('`');
                                let digits: String = alternative.chars().take_while(|c| c.is_ascii_digit() || *c == ',').collect();
                                if !digits.is_empty() && decimal(digits.trim_end_matches(','))? != *expected { return None; }
                            }
                        } else if claim.starts_with('-') { return None; }
                    }
                }
            }
        }
    }
    Some(value)
}

pub fn batch_answer(snapshot: &Snapshot, answer: &str) -> bool {
    let trimmed = answer.trim();
    let result = if trimmed.contains("```") {
        explained_batch(trimmed, &snapshot.facts)
    } else {
        let Some(answer) = unquote(trimmed) else { return false; };
        if answer.starts_with('{') {
            serde_json::from_str::<BatchAnswer>(answer).ok()
        } else {
            text_batch(answer)
        }
    };
    result.is_some_and(|a| {
        [
            a.cargo, a.tools, a.runtime, a.memory, a.gateway, a.provider, a.harness,
        ] == snapshot.facts
    })
}
pub fn compute_answer(snapshot: &Snapshot, answer: &str) -> bool {
    let Some(answer) = unquote(answer) else {
        return false;
    };
    let result = if answer.starts_with('{') {
        serde_json::from_str::<ComputeAnswer>(answer).ok()
    } else {
        text_compute(answer)
    };
    result.is_some_and(|a| {
        a.total_lines == snapshot.total_lines
            && a.rs_files == snapshot.file_count
            && snapshot
                .top
                .get(&a.top_crate)
                .is_some_and(|n| *n == a.top_crate_files)
    })
}

/// Select only the original bound, successful Runtime completion refs. A
/// preview, invocation input, or a foreign Session is never a receipt.
pub fn compute_refs(
    timeline: &serde_json::Value,
    session: &str,
    turn: &str,
    root: &str,
) -> Vec<String> {
    timeline["events"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|event| {
            let p = &event["payload"];
            let b = &p["_runtime_activity_binding"];
            let call = p["tool_call_id"].as_str().filter(|s| !s.is_empty())?;
            p["invocation_id"].as_str().filter(|s| !s.is_empty())?;
            let reference = p["full_output_ref"]
                .as_str()
                .filter(|s| s.starts_with("tool://tool-raw-") && !s.contains('?'))?;
            (event["kind"] == "tool.invocation.completed"
                && event["status"] == "completed"
                && event["stream_id"] == format!("session:{session}")
                && p["status"] == "completed"
                && p["is_error"] == false
                && p["tool_name"] == "tool_orchestrate"
                && p["session_id"] == session
                && b["session_id"] == session
                && b["turn_id"] == turn
                && b["root_execution_id"] == root
                && b["tool_contract_id"] == "tool_orchestrate"
                && b["tool_call_id"] == call)
                .then(|| reference.to_string())
        })
        .collect()
}
/// Accept only complete, session-scoped, independently rehashed durable output.
/// A compute step's returned structured result must equal the reported result.
pub fn compute_result_verified(
    snapshot: &Snapshot,
    answer: &str,
    session: &str,
    refs: &[String],
    resolved: &[serde_json::Value],
) -> bool {
    let Some(text) = unquote(answer) else {
        return false;
    };
    let reported = if text.starts_with('{') {
        serde_json::from_str::<ComputeAnswer>(text).ok()
    } else {
        text_compute(text)
    };
    let Some(reported) = reported else {
        return false;
    };
    resolved.iter().any(|item| {
        let Some(reference) = item["ref"].as_str() else {
            return false;
        };
        if !refs.iter().any(|r| r == reference)
            || item["kind"] != "tool"
            || item["available"] != true
            || item["verified"] != true
            || item["session_id"] != session
            || item["artifact"]["snippet_truncated"] != false
        {
            return false;
        }
        let Some(body) = item["artifact"]["snippet"].as_str() else {
            return false;
        };
        if item["artifact"]["bytes"].as_u64() != Some(body.len() as u64)
            || item["artifact"]["sha256"] != format!("sha256:{:x}", Sha256::digest(body.as_bytes()))
        {
            return false;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
            return false;
        };
        value["calls"].as_array().is_some_and(|calls| {
            calls.iter().any(|step| {
                if step["op"] != "compute"
                    || step["tool"] != "execute_code"
                    || step["status"] != "ok"
                    || !step["calls"].as_u64().is_some_and(|n| n > 0)
                {
                    return false;
                }
                let Some(id) = step["stepId"].as_str().filter(|s| !s.is_empty()) else {
                    return false;
                };
                let Some(result) = value["steps"].get(id) else {
                    return false;
                };
                let result_text = result
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| result.to_string());
                let Some(parsed) = serde_json::from_str::<ComputeAnswer>(&result_text).ok() else {
                    return false;
                };
                parsed == reported && compute_answer(snapshot, &result_text)
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample() -> Snapshot {
        Snapshot {
            files: vec![
                ("x.rs".into(), "a".into()),
                ("y.rs".into(), "b".into()),
                ("Cargo.toml".into(), "c".into()),
            ],
            contents: vec![],
            total_lines: 1234,
            file_count: 2,
            top: [("alpha".into(), 2), ("beta".into(), 2)]
                .into_iter()
                .collect(),
            facts: [7, 8, 9, 10, 11, 12, 13],
        }
    }
    #[test]
    fn compute_evidence_requires_bound_complete_hashed_raw_output() {
        let answer = r#"{"total_lines":1234,"rs_files":2,"top_crate":"alpha","top_crate_files":2}"#;
        let output = format!(
            r#"{{"calls":[{{"stepId":"c","op":"compute","tool":"execute_code","status":"ok","calls":1}}],"steps":{{"c":{answer}}}}}"#
        );
        let reference = "tool://tool-raw-call-123";
        let event = serde_json::json!({"kind":"tool.invocation.completed", "status":"completed", "stream_id":"session:s", "payload":{
            "status":"completed", "is_error":false, "tool_name":"tool_orchestrate", "session_id":"s", "tool_call_id":"call", "invocation_id":"inv", "full_output_ref":reference,
            "output_preview":"truncated", "_runtime_activity_binding":{"session_id":"s","turn_id":"t","root_execution_id":"r","tool_contract_id":"tool_orchestrate","tool_call_id":"call"}}});
        let timeline = serde_json::json!({"events":[event]});
        let refs = compute_refs(&timeline, "s", "t", "r");
        assert!(compute_refs(&timeline, "s", "foreign", "r").is_empty());
        let mut resolved = serde_json::json!({"ref":reference,"kind":"tool","available":true,"verified":true,"session_id":"s", "artifact":{
            "snippet":output, "bytes":output.len(), "snippet_truncated":false, "sha256":format!("sha256:{:x}", Sha256::digest(output.as_bytes()))}});
        assert!(compute_result_verified(
            &sample(),
            answer,
            "s",
            &refs,
            &[resolved.clone()]
        ));
        assert!(!compute_result_verified(
            &sample(),
            answer,
            "foreign",
            &refs,
            &[resolved.clone()]
        ));
        assert!(!compute_result_verified(
            &sample(),
            answer,
            "s",
            &["tool://tool-raw-other".into()],
            &[resolved.clone()]
        ));
        assert!(!compute_result_verified(
            &sample(),
            r#"{"total_lines":1234,"rs_files":2,"top_crate":"beta","top_crate_files":2}"#,
            "s",
            &refs,
            &[resolved.clone()]
        ));
        resolved["artifact"]["snippet_truncated"] = serde_json::json!(true);
        assert!(!compute_result_verified(
            &sample(),
            answer,
            "s",
            &refs,
            &[resolved.clone()]
        ));
        resolved["artifact"]["snippet_truncated"] = serde_json::json!(false);
        resolved["artifact"]["sha256"] = serde_json::json!("sha256:wrong");
        assert!(!compute_result_verified(
            &sample(),
            answer,
            "s",
            &refs,
            &[resolved]
        ));
    }
    #[test]
    fn compute_structured_and_equivalent_text_reject_ambiguity() {
        let s = sample();
        assert!(compute_answer(
            &s,
            r#"{"total_lines":1234,"rs_files":2,"top_crate":"alpha","top_crate_files":2}"#
        ));
        assert!(compute_answer(
            &s,
            "总行数: 1,234\n文件数: 2\n最多crate: beta\n最多crate文件数: 2"
        ));
        assert!(compute_answer(
            &s,
            "|lines|1234|\n|files|2|\n|top crate|alpha|\n|top crate files|2|"
        ));
        assert!(compute_answer(&s, "| 指标 | 值 |\n| :--- | ---: |\n| lines | 1234 |\n| files | 2 |\n| top crate | beta |\n| top crate files | 2 |"));
        assert!(compute_answer(&s, "- **总行数**: 1,234\n- **文件数**: 2\n- **最多crate**: alpha\n- **最多crate文件数**: 2"));
        assert!(!compute_answer(
            &s,
            r#"{"total_lines":1234,"rs_files":2,"top_crate":"alpha","top_crate_files":2,"rs_files":3}"#
        ));
        assert!(!compute_answer(
            &s,
            "总行数: 1234\n文件数: 2\n文件数: 3\n最多crate: alpha\n最多crate文件数: 2"
        ));
        assert!(!compute_answer(
            &s,
            "总行数: 1234\n文件数: 2\n最多crate: gamma\n最多crate文件数: 2"
        ));
        assert!(!compute_answer(
            &s,
            "总行数: 1,23\n文件数: 2\n最多crate: alpha\n最多crate文件数: 2"
        ));
        assert!(!compute_answer(
            &s,
            "总行数: 1234\n文件数: 2\n最多crate: alpha\n最多crate文件数: 2\n实际上文件数为3"
        ));
    }
    #[test]
    fn batch_requires_exact_seven_facts_and_rejects_conflicts() {
        let s = sample();
        let paths = [
            "Cargo.toml",
            SOURCES[0],
            SOURCES[1],
            SOURCES[2],
            SOURCES[3],
            SOURCES[4],
            SOURCES[5],
        ];
        let answer = paths
            .iter()
            .zip(s.facts)
            .map(|(p, n)| format!("{p}: {n}"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(batch_answer(&s, &answer));
        let table = format!("| 路径 | 数值 |\n| --- | --- |\n{}", paths.iter().zip(s.facts).map(|(p, n)| format!("| `{p}` | {n} |")).collect::<Vec<_>>().join("\n"));
        assert!(batch_answer(&s, &table));
        assert!(!batch_answer(&s, &format!("{table}\n| Cargo.toml | 999 |")));
        let json = paths
            .iter()
            .zip(s.facts)
            .map(|(p, n)| format!("\"{p}\":{n}"))
            .collect::<Vec<_>>()
            .join(",");
        assert!(batch_answer(&s, &format!("{{{json}}}")));
        assert!(!batch_answer(&s, &answer.replace(": 11", ": 110")));
        assert!(!batch_answer(&s, &answer.replace(SOURCES[3], "other")));
        assert!(!batch_answer(&s, &format!("{answer}\nCargo.toml: 999")));
        assert!(!batch_answer(&s, &format!("{{{json},\"Cargo.toml\":999}}")));
        let explained = format!("说明与证据：\n| 路径 | 事实 | 来源 |\n| `Cargo.toml` | 成员 **7** 个 | 统计 |\n\n```json\n{{{json}}}\n```\n附注：计数依据冻结输入。");
        assert!(batch_answer(&s, &explained));
        assert!(!batch_answer(&s, &explained.replace("**7**", "**8**")));
        assert!(!batch_answer(&s, &format!("{explained}\nCargo.toml: 8")));
        assert!(!batch_answer(&s, &format!("{explained}\n实际上 Cargo.toml 是 8。")));
        assert!(!batch_answer(&s, &format!("{explained}\nCargo.toml 数量为 8。")));
        assert!(!batch_answer(&s, &format!("{explained}\nCargo.toml: 7 或 8")));
        assert!(!batch_answer(&s, &format!("{explained}\nCargo.toml: 7, 8")));
        assert!(!batch_answer(&s, &format!("{explained}\n```json\n{{{json}}}\n```")));
        assert!(!batch_answer(&s, &explained.replace("\"Cargo.toml\":7", "\"Cargo.toml\":8")));
        assert!(!batch_answer(&s, &explained.replace("\"Cargo.toml\":7,", "")));
        assert!(!batch_answer(&s, &format!("```json\n{{{json}}}\n```\nCargo.toml: 8")));
    }
    #[test]
    fn line_endings_and_manifest() {
        assert_eq!(lines(""), 0);
        assert_eq!(lines("a\nb"), 2);
        assert_eq!(lines("a\nb\n"), 2);
        assert_eq!(lines("a\r\nb\r\n"), 2);
        assert_eq!(
            members("[workspace]\nmembers = [\n \"a\",\n \"b\",\n]\n").unwrap(),
            2
        );
    }

    #[test]
    fn snapshot_reads_actual_files_and_detects_changes() {
        let root = std::env::temp_dir().join(format!("k02-oracle-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("Cargo.toml"),
            "[workspace]\nmembers = [\"crates/a\"]\n",
        )
        .unwrap();
        for path in SOURCES {
            let file = root.join(path);
            fs::create_dir_all(file.parent().unwrap()).unwrap();
            fs::write(file, "pub mod sample;\nlast line").unwrap();
        }
        let manifest = root.join("crates/runtime/Cargo.toml");
        fs::write(manifest, "[package]\nname = \"runtime\"\n").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink("/etc/passwd", root.join("escape.rs")).unwrap();
        let before = Snapshot::capture(&root).unwrap();
        assert_eq!(before.files.len(), 8);
        assert_eq!(before.total_lines, 12);
        assert_eq!(before.top.get("runtime"), Some(&1));
        assert_eq!(before.facts[0], 1);
        let prereg = root.with_extension("prereg.json");
        persist_preregistration(&prereg, &root, &before).unwrap();
        assert!(persist_preregistration(&prereg, &root, &before).is_err());
        assert!(persist_preregistration(&root.join("inside.json"), &root, &before).is_err());
        fs::remove_file(prereg).unwrap();
        fs::write(root.join(SOURCES[3]), "changed\n").unwrap();
        assert_ne!(before, Snapshot::capture(&root).unwrap());
        fs::remove_dir_all(root).unwrap();
    }
}
