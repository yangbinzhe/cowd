use std::{
    fs,
    io::Cursor,
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{ServiceEnvelope, WorkspaceService};

const WORKSPACE_IGNORED_DIRS: &[&str] = &[
    ".git",
    "target",
    "node_modules",
    ".venv",
    "venv",
    "__pycache__",
    ".next",
    "dist",
    "build",
];

#[derive(Serialize)]
struct WorkspaceFileItem {
    name: String,
    path: String,
    is_dir: bool,
    #[serde(rename = "type")]
    kind: String,
    size: u64,
    modified_ms: Option<u128>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SessionAttachment {
    pub(crate) ref_id: String,
    pub(crate) kind: String,
    pub(crate) path: String,
    pub(crate) label: String,
    pub(crate) size: u64,
    pub(crate) sha256: String,
    pub(crate) added_at_ms: i64,
}

pub(crate) struct WorkspaceDownload {
    pub(crate) bytes: Vec<u8>,
    pub(crate) file_name: String,
    pub(crate) content_type: &'static str,
}

impl WorkspaceService {
    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.envelope("overview"),
            self.envelope("files"),
            self.envelope("metadata"),
            self.envelope("mutations"),
            self.envelope("attachments"),
        ]
    }

    pub(crate) fn overview(
        &self,
        workspace_root: &Path,
        config_home: &Path,
        profile_id: Option<&str>,
        selected_session: Option<&storage::StorageEndpoint>,
    ) -> serde_json::Value {
        let workspace_canonical = workspace_root.canonicalize().ok();
        let fallback;
        let session = if let Some(endpoint) = selected_session {
            endpoint
        } else {
            fallback = storage::StorageRegistry::default_for_config_home(config_home)
                .endpoint(&storage::StorageDomainId::Session)
                .expect("session endpoint is part of the default Cowd storage inventory")
                .clone();
            &fallback
        };
        serde_json::json!({
            "workspace_root": workspace_root.display().to_string(),
            "workspace_canonical": workspace_canonical.map(|path| path.display().to_string()),
            "profile_id": profile_id,
            "config_home": config_home.display().to_string(),
            "sessions_db": (!session.path.as_os_str().is_empty())
                .then(|| session.path.display().to_string()),
            "session_storage": {
                "logical_id": session.logical_id(),
                "backend": session.backend,
                "scope": session.scope,
                "migration": session.migration,
            },
            "memory_dir": config_home.join("memory").display().to_string(),
        })
    }

    pub(crate) fn workspaces(
        &self,
        workspace_root: &Path,
        profile_id: Option<&str>,
    ) -> serde_json::Value {
        serde_json::json!({
            "workspaces": [{
                "id": "current",
                "name": workspace_root.file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("workspace"),
                "path": workspace_root.display().to_string(),
                "active": true,
                "profile_id": profile_id,
            }]
        })
    }

    pub(crate) fn list_files(
        &self,
        workspace_root: &Path,
        dir: Option<&str>,
        recursive: bool,
        limit: usize,
    ) -> Result<serde_json::Value, String> {
        let root = workspace_root_canonical(workspace_root)?;
        let dir = resolve_existing_workspace_path(workspace_root, dir)?;
        if !dir.is_dir() {
            return Err("path is not a directory".to_string());
        }
        let limit = limit.clamp(1, 10_000);
        let mut truncated = false;
        let mut files = Vec::new();
        if recursive {
            collect_workspace_files_recursive(&root, &dir, &mut files, limit, &mut truncated)?;
        } else {
            files = fs::read_dir(&dir)
                .map_err(|error| error.to_string())?
                .flatten()
                .filter_map(|entry| workspace_file_item(&root, entry.path()))
                .take(limit)
                .collect::<Vec<_>>();
            truncated = fs::read_dir(&dir)
                .map_err(|error| error.to_string())?
                .flatten()
                .filter_map(|entry| workspace_file_item(&root, entry.path()))
                .nth(limit)
                .is_some();
        }
        files.sort_by(|a, b| match (a.is_dir, b.is_dir) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
        });
        Ok(serde_json::json!({
            "workspace_root": workspace_root.display().to_string(),
            "dir": workspace_relative_path(&root, &dir),
            "recursive": recursive,
            "limit": limit,
            "truncated": truncated,
            "ignored": WORKSPACE_IGNORED_DIRS,
            "files": files,
        }))
    }

    pub(crate) fn create_file(
        &self,
        workspace_root: &Path,
        path: &str,
        content: &str,
    ) -> Result<serde_json::Value, String> {
        let target = resolve_new_workspace_file_path(workspace_root, path)?;
        if target.exists() && target.is_dir() {
            return Err("path is a directory".to_string());
        }
        fs::write(&target, content).map_err(|error| error.to_string())?;
        let root = workspace_root_canonical(workspace_root)?;
        Ok(serde_json::json!({
            "path": workspace_relative_path(&root, &target),
            "created": true,
        }))
    }

    pub(crate) fn create_dir(
        &self,
        workspace_root: &Path,
        path: &str,
    ) -> Result<serde_json::Value, String> {
        let target = resolve_new_workspace_file_path(workspace_root, path)?;
        fs::create_dir_all(&target).map_err(|error| error.to_string())?;
        let root = workspace_root_canonical(workspace_root)?;
        Ok(serde_json::json!({
            "path": workspace_relative_path(&root, &target),
            "created": true,
            "type": "dir",
        }))
    }

    pub(crate) fn delete_path(
        &self,
        workspace_root: &Path,
        path: &str,
    ) -> Result<serde_json::Value, String> {
        let target = resolve_existing_workspace_path(workspace_root, Some(path))?;
        if target.is_dir() {
            fs::remove_dir_all(&target)
        } else {
            fs::remove_file(&target)
        }
        .map_err(|error| error.to_string())?;
        Ok(serde_json::json!({
            "deleted": true,
            "path": path,
        }))
    }

    pub(crate) fn metadata(
        &self,
        workspace_root: &Path,
        path: &str,
    ) -> Result<serde_json::Value, String> {
        let root = workspace_root_canonical(workspace_root)?;
        let target = resolve_existing_workspace_path(workspace_root, Some(path))?;
        let item = workspace_file_item(&root, target.clone())
            .ok_or_else(|| "path not found".to_string())?;
        let sha256 = if target.is_file() {
            fs::read(&target).ok().map(|bytes| hash_bytes(&bytes))
        } else {
            None
        };
        Ok(serde_json::json!({
            "item": item,
            "sha256": sha256,
        }))
    }

    pub(crate) fn rename_path(
        &self,
        workspace_root: &Path,
        from: &str,
        to: &str,
    ) -> Result<serde_json::Value, String> {
        let source = resolve_existing_workspace_path(workspace_root, Some(from))?;
        let target = resolve_new_workspace_file_path(workspace_root, to)?;
        if target.exists() {
            return Err("target already exists".to_string());
        }
        fs::rename(&source, &target).map_err(|error| error.to_string())?;
        let root = workspace_root_canonical(workspace_root)?;
        Ok(serde_json::json!({
            "renamed": true,
            "from": from,
            "to": workspace_relative_path(&root, &target),
        }))
    }

    pub(crate) fn upload_file(
        &self,
        workspace_root: &Path,
        dir: &str,
        file_name: &str,
        bytes: &[u8],
        overwrite: bool,
    ) -> Result<serde_json::Value, String> {
        let file_name = safe_upload_name(file_name)?;
        let path = if dir.trim().is_empty() {
            file_name
        } else {
            format!("{}/{}", dir.trim().trim_end_matches('/'), file_name)
        };
        let target = resolve_new_workspace_file_path(workspace_root, &path)?;
        if target.exists() && !overwrite {
            return Err("target already exists".to_string());
        }
        fs::write(&target, bytes).map_err(|error| error.to_string())?;
        let root = workspace_root_canonical(workspace_root)?;
        Ok(serde_json::json!({
            "uploaded": true,
            "path": workspace_relative_path(&root, &target),
            "size": bytes.len(),
            "sha256": hash_bytes(bytes),
        }))
    }

    pub(crate) fn raw_file(&self, workspace_root: &Path, path: &str) -> Result<Vec<u8>, String> {
        let file = resolve_existing_workspace_path(workspace_root, Some(path))?;
        if !file.is_file() {
            return Err("path is not a file".to_string());
        }
        fs::read(&file).map_err(|error| error.to_string())
    }

    pub(crate) fn download_path(
        &self,
        workspace_root: &Path,
        path: &str,
    ) -> Result<WorkspaceDownload, String> {
        let root = workspace_root_canonical(workspace_root)?;
        let target = resolve_existing_workspace_path(workspace_root, Some(path))?;
        if target.is_file() {
            let bytes = fs::read(&target).map_err(|error| error.to_string())?;
            return Ok(WorkspaceDownload {
                bytes,
                file_name: target
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("download.bin")
                    .to_string(),
                content_type: "application/octet-stream",
            });
        }
        if !target.is_dir() {
            return Err("path is not downloadable".to_string());
        }
        let archive_name = target
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("workspace")
            .to_string();
        let mut bytes = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut bytes);
            append_workspace_dir_to_tar(&mut builder, &root, &target, Path::new(&archive_name))?;
            builder.finish().map_err(|error| error.to_string())?;
        }
        Ok(WorkspaceDownload {
            bytes,
            file_name: format!("{archive_name}.tar"),
            content_type: "application/x-tar",
        })
    }

    pub(crate) fn list_attachments(
        &self,
        config_home: &Path,
        session_id: &str,
    ) -> Vec<SessionAttachment> {
        load_session_attachments(config_home, session_id)
    }

    pub(crate) fn add_attachment(
        &self,
        workspace_root: &Path,
        config_home: &Path,
        session_id: &str,
        path: &str,
        kind: String,
        label: Option<String>,
    ) -> Result<SessionAttachment, String> {
        let file = resolve_existing_workspace_path(workspace_root, Some(path))?;
        if !file.is_file() {
            return Err("attachment path is not a file".to_string());
        }
        let bytes = fs::read(&file).map_err(|error| error.to_string())?;
        let root = workspace_root_canonical(workspace_root)?;
        let relative = workspace_relative_path(&root, &file);
        let hash = hash_bytes(&bytes);
        let ref_id = format!(
            "att-{}-{}",
            chrono::Utc::now().timestamp_millis(),
            hash.trim_start_matches("sha256:")
                .chars()
                .take(12)
                .collect::<String>()
        );
        let attachment = SessionAttachment {
            ref_id,
            kind,
            label: label.unwrap_or_else(|| relative.clone()),
            path: relative,
            size: bytes.len() as u64,
            sha256: hash,
            added_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        let mut attachments = load_session_attachments(config_home, session_id);
        attachments.retain(|item| item.path != attachment.path);
        attachments.push(attachment.clone());
        save_session_attachments(config_home, session_id, &attachments)?;
        Ok(attachment)
    }

    pub(crate) fn delete_attachment(
        &self,
        config_home: &Path,
        session_id: &str,
        ref_id: &str,
    ) -> Result<bool, String> {
        let mut attachments = load_session_attachments(config_home, session_id);
        let before = attachments.len();
        attachments.retain(|item| item.ref_id != ref_id);
        let deleted = attachments.len() != before;
        if deleted {
            save_session_attachments(config_home, session_id, &attachments)?;
        }
        Ok(deleted)
    }
}

fn path_has_safe_relative_components(path: &Path) -> bool {
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

fn workspace_root_canonical(workspace_root: &Path) -> Result<PathBuf, String> {
    workspace_root
        .canonicalize()
        .map_err(|e| format!("workspace root is unavailable: {e}"))
}

fn resolve_existing_workspace_path(
    workspace_root: &Path,
    relative: Option<&str>,
) -> Result<PathBuf, String> {
    let root = workspace_root_canonical(workspace_root)?;
    let rel = relative.map(str::trim).unwrap_or("");
    let rel_path = Path::new(rel);
    if !rel.is_empty() && !path_has_safe_relative_components(rel_path) {
        return Err("path must stay inside the workspace".to_string());
    }
    let candidate = if rel.is_empty() {
        root.clone()
    } else {
        root.join(rel_path)
    };
    let resolved = candidate
        .canonicalize()
        .map_err(|e| format!("path not found: {e}"))?;
    if !resolved.starts_with(&root) {
        return Err("path must stay inside the workspace".to_string());
    }
    Ok(resolved)
}

fn resolve_new_workspace_file_path(
    workspace_root: &Path,
    relative: &str,
) -> Result<PathBuf, String> {
    let root = workspace_root_canonical(workspace_root)?;
    let rel = relative.trim();
    if rel.is_empty() {
        return Err("path is required".to_string());
    }
    let rel_path = Path::new(rel);
    if !path_has_safe_relative_components(rel_path) {
        return Err("path must stay inside the workspace".to_string());
    }
    let target = root.join(rel_path);
    let parent = target
        .parent()
        .ok_or_else(|| "file parent is unavailable".to_string())?;
    fs::create_dir_all(parent).map_err(|e| format!("failed to create parent directory: {e}"))?;
    let parent_resolved = parent
        .canonicalize()
        .map_err(|e| format!("file parent is unavailable: {e}"))?;
    if !parent_resolved.starts_with(&root) {
        return Err("path must stay inside the workspace".to_string());
    }
    Ok(target)
}

fn workspace_relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .ok()
        .and_then(|path| path.to_str())
        .unwrap_or("")
        .replace('\\', "/")
}

fn workspace_file_item(root: &Path, path: PathBuf) -> Option<WorkspaceFileItem> {
    let metadata = fs::metadata(&path).ok()?;
    let name = path.file_name()?.to_string_lossy().to_string();
    let is_dir = metadata.is_dir();
    let modified_ms = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis());
    Some(WorkspaceFileItem {
        name,
        path: workspace_relative_path(root, &path),
        is_dir,
        kind: if is_dir { "dir" } else { "file" }.to_string(),
        size: if is_dir { 0 } else { metadata.len() },
        modified_ms,
    })
}

fn collect_workspace_files_recursive(
    root: &Path,
    dir: &Path,
    files: &mut Vec<WorkspaceFileItem>,
    limit: usize,
    truncated: &mut bool,
) -> Result<(), String> {
    if files.len() >= limit {
        *truncated = true;
        return Ok(());
    }

    let mut entries = fs::read_dir(dir)
        .map_err(|error| error.to_string())?
        .flatten()
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        if files.len() >= limit {
            *truncated = true;
            return Ok(());
        }

        let name = entry.file_name().to_string_lossy().to_string();
        let path = entry.path();
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() && WORKSPACE_IGNORED_DIRS.contains(&name.as_str()) {
            continue;
        }

        if let Some(item) = workspace_file_item(root, path.clone()) {
            files.push(item);
        }
        if metadata.is_dir() {
            collect_workspace_files_recursive(root, &path, files, limit, truncated)?;
        }
    }
    Ok(())
}

fn append_workspace_dir_to_tar<W: std::io::Write>(
    builder: &mut tar::Builder<W>,
    root: &Path,
    source: &Path,
    archive_path: &Path,
) -> Result<(), String> {
    if !source.starts_with(root) {
        return Err("path must stay inside the workspace".to_string());
    }
    let metadata = fs::symlink_metadata(source).map_err(|error| error.to_string())?;
    if metadata.file_type().is_symlink() {
        return Ok(());
    }
    if metadata.is_dir() {
        builder
            .append_dir(archive_path, source)
            .map_err(|error| error.to_string())?;
        let mut entries = fs::read_dir(source)
            .map_err(|error| error.to_string())?
            .flatten()
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let child = entry.path();
            let child_name = entry.file_name();
            append_workspace_dir_to_tar(builder, root, &child, &archive_path.join(child_name))?;
        }
        return Ok(());
    }
    if metadata.is_file() {
        let bytes = fs::read(source).map_err(|error| error.to_string())?;
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, archive_path, Cursor::new(bytes))
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn hash_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

fn safe_upload_name(name: &str) -> Result<String, String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err("file name is required".to_string());
    }
    let path = Path::new(trimmed);
    if !path_has_safe_relative_components(path) || path.components().count() != 1 {
        return Err("uploaded file name must be a safe file name".to_string());
    }
    Ok(trimmed.to_string())
}

fn attachment_store_path(config_home: &Path, session_id: &str) -> PathBuf {
    config_home
        .join("session_attachments")
        .join(format!("{session_id}.json"))
}

fn load_session_attachments(config_home: &Path, session_id: &str) -> Vec<SessionAttachment> {
    let path = attachment_store_path(config_home, session_id);
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Vec<SessionAttachment>>(&raw).ok())
        .unwrap_or_default()
}

fn save_session_attachments(
    config_home: &Path,
    session_id: &str,
    attachments: &[SessionAttachment],
) -> Result<(), String> {
    let path = attachment_store_path(config_home, session_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let rendered = serde_json::to_string_pretty(attachments).map_err(|error| error.to_string())?;
    fs::write(path, rendered).map_err(|error| error.to_string())
}
