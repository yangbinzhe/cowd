use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, RwLock};

use serde::Serialize;
use surface::{
    builtin_surfaces, normalize_surface_id, SurfaceActionRequest, SurfaceDescriptor, SurfaceError,
    SurfaceFrame, SurfaceManifest, SurfaceOperationResult, SurfaceRegistrySnapshot,
    SurfaceSendRequest, SURFACE_MANIFEST_FILE,
};

#[derive(Debug, Clone)]
pub(crate) struct SurfaceHost {
    registry: Arc<RwLock<BTreeMap<String, SurfaceDescriptor>>>,
    roots: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceDiscoveryReport {
    pub(crate) roots: Vec<String>,
    pub(crate) discovered: usize,
    pub(crate) failures: Vec<SurfaceDiscoveryFailure>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SurfaceDiscoveryFailure {
    pub(crate) path: String,
    pub(crate) error: String,
}

impl SurfaceHost {
    pub(crate) fn new(roots: Vec<PathBuf>) -> Self {
        let host = Self {
            registry: Arc::new(RwLock::new(BTreeMap::new())),
            roots,
        };
        host.register_builtin_surfaces();
        host
    }

    pub(crate) fn default_for(config_home: &Path) -> Self {
        let install_root = std::env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(Path::to_path_buf))
            .map(|root| root.join("surfaces"));
        let mut roots = Vec::new();
        if let Some(root) = install_root {
            roots.push(root);
        }
        roots.push(config_home.join("surfaces"));
        Self::new(roots)
    }

    pub(crate) fn register_builtin_surfaces(&self) {
        let mut registry = self
            .registry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for manifest in builtin_surfaces().into_values() {
            let descriptor = SurfaceDescriptor::from_manifest(&manifest, "builtin");
            registry.insert(descriptor.id.clone(), descriptor);
        }
    }

    pub(crate) fn discover(&self) -> SurfaceDiscoveryReport {
        let mut failures = Vec::new();
        let mut discovered = 0usize;
        for root in &self.roots {
            if !root.is_dir() {
                continue;
            }
            let Ok(entries) = std::fs::read_dir(root) else {
                failures.push(SurfaceDiscoveryFailure {
                    path: root.display().to_string(),
                    error: "failed to read surface root".to_string(),
                });
                continue;
            };
            for entry in entries.flatten() {
                let manifest_path = entry.path().join(SURFACE_MANIFEST_FILE);
                if !manifest_path.is_file() {
                    continue;
                }
                match SurfaceManifest::load(&manifest_path) {
                    Ok(manifest) => {
                        let mut descriptor = SurfaceDescriptor::from_manifest(
                            &manifest,
                            manifest_path.display().to_string(),
                        );
                        descriptor.diagnostics.push(
                            "surface discovered; sidecar launch is controlled by gateway"
                                .to_string(),
                        );
                        self.registry
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .insert(descriptor.id.clone(), descriptor);
                        discovered += 1;
                    }
                    Err(error) => failures.push(SurfaceDiscoveryFailure {
                        path: manifest_path.display().to_string(),
                        error: error.to_string(),
                    }),
                }
            }
        }
        SurfaceDiscoveryReport {
            roots: self
                .roots
                .iter()
                .map(|root| root.display().to_string())
                .collect(),
            discovered,
            failures,
        }
    }

    pub(crate) fn snapshot(&self) -> SurfaceRegistrySnapshot {
        let mut surfaces = self
            .registry
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        surfaces.sort_by(|left, right| left.id.cmp(&right.id));
        SurfaceRegistrySnapshot::new(surfaces)
    }

    pub(crate) fn get(&self, id: &str) -> Option<SurfaceDescriptor> {
        self.registry
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&normalize_surface_id(id))
            .cloned()
    }

    pub(crate) fn has_external_surface(&self, id: &str) -> bool {
        self.get(id).is_some_and(|surface| surface.entry.is_some())
    }

    pub(crate) async fn send(
        &self,
        request: SurfaceSendRequest,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        let Some(surface) = self.get(&request.surface) else {
            return Ok(SurfaceOperationResult::unavailable(&request.surface));
        };
        if surface.entry.is_none() {
            return Ok(SurfaceOperationResult::unavailable(&request.surface));
        }
        let surface_id = normalize_surface_id(&request.surface);
        let frame = SurfaceFrame::Send {
            id: SurfaceFrame::new_id(),
            surface: surface_id.clone(),
            recipient: request.recipient,
            thread: request.thread,
            text: request.text,
            metadata: request.metadata,
        };
        self.invoke(surface, frame).await
    }

    pub(crate) async fn action(
        &self,
        request: SurfaceActionRequest,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        let Some(surface) = self.get(&request.surface) else {
            return Ok(SurfaceOperationResult::unavailable(&request.surface));
        };
        if surface.entry.is_none() {
            return Ok(SurfaceOperationResult::unavailable(&request.surface));
        }
        let surface_id = normalize_surface_id(&request.surface);
        let frame = SurfaceFrame::Action {
            id: SurfaceFrame::new_id(),
            surface: surface_id,
            action: request.action,
            payload: request.payload,
        };
        self.invoke(surface, frame).await
    }

    async fn invoke(
        &self,
        surface: SurfaceDescriptor,
        frame: SurfaceFrame,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        tokio::task::spawn_blocking(move || invoke_sidecar(surface, frame))
            .await
            .map_err(|error| SurfaceError::Invocation {
                surface: "unknown".to_string(),
                reason: format!("surface task join failed: {error}"),
            })?
    }
}

impl Default for SurfaceHost {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

fn invoke_sidecar(
    surface: SurfaceDescriptor,
    frame: SurfaceFrame,
) -> Result<SurfaceOperationResult, SurfaceError> {
    let surface_id = surface.id.clone();
    let entry = surface
        .entry
        .clone()
        .ok_or_else(|| SurfaceError::Unavailable(surface_id.clone()))?;
    let manifest_path = PathBuf::from(&surface.source);
    let working_dir = manifest_path.parent().map(Path::to_path_buf);
    let mut command_path = PathBuf::from(entry);
    if command_path.is_relative() {
        if let Some(root) = &working_dir {
            command_path = root.join(command_path);
        }
    }

    let mut child = Command::new(&command_path)
        .current_dir(working_dir.as_deref().unwrap_or_else(|| Path::new(".")))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("failed to launch `{}`: {error}", command_path.display()),
        })?;

    let mut stdin = child.stdin.take().ok_or_else(|| SurfaceError::Invocation {
        surface: surface_id.clone(),
        reason: "sidecar stdin is not available".to_string(),
    })?;
    let encoded = frame.encode_jsonl()?;
    stdin
        .write_all(encoded.as_bytes())
        .and_then(|_| stdin.flush())
        .map_err(|error| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("failed to write jsonl request: {error}"),
        })?;
    drop(stdin);

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: "sidecar stdout is not available".to_string(),
        })?;
    let mut reader = std::io::BufReader::new(stdout);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|error| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("failed to read jsonl response: {error}"),
        })?;
    if line.trim().is_empty() {
        return Err(SurfaceError::Invocation {
            surface: surface_id,
            reason: "sidecar returned no jsonl response".to_string(),
        });
    }

    let response = SurfaceFrame::decode_jsonl(&line)?;
    let _ = child.wait();
    Ok(operation_result_from_frame(&surface_id, response))
}

fn operation_result_from_frame(surface: &str, frame: SurfaceFrame) -> SurfaceOperationResult {
    match frame {
        SurfaceFrame::Ok { payload, .. } => SurfaceOperationResult::ok(surface, payload),
        SurfaceFrame::Error { code, message, .. } => {
            SurfaceOperationResult::error(surface, code, message)
        }
        SurfaceFrame::HandshakeOk { capabilities, .. } => SurfaceOperationResult::ok(
            surface,
            serde_json::json!({
                "status": "ok",
                "capabilities": capabilities,
            }),
        ),
        other => SurfaceOperationResult::error(
            surface,
            "surface_unexpected_frame",
            format!("unexpected surface response frame: {other:?}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[cfg(unix)]
    #[tokio::test]
    async fn discovers_and_invokes_stdio_jsonl_sidecar() {
        let root =
            std::env::temp_dir().join(format!("cowd-surface-host-test-{}", uuid::Uuid::new_v4()));
        let surface_dir = root.join("echo");
        fs::create_dir_all(&surface_dir).unwrap();
        let sidecar = surface_dir.join("cowd-surface-echo");
        fs::write(
            &sidecar,
            "#!/usr/bin/env sh\nread _line\nprintf '%s\\n' '{\"type\":\"ok\",\"id\":\"reply\",\"payload\":{\"status\":\"sent\",\"message_id\":\"m-1\"}}'\n",
        )
        .unwrap();
        let mut permissions = fs::metadata(&sidecar).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&sidecar, permissions).unwrap();
        fs::write(
            surface_dir.join(SURFACE_MANIFEST_FILE),
            r#"{
                "schema": "cowd.surface.v1",
                "id": "echo",
                "name": "Echo Surface",
                "version": "1.0.0",
                "kind": "external-integration",
                "entry": "./cowd-surface-echo",
                "transport": "stdio-jsonl",
                "capabilities": ["send_text"],
                "default_enabled": true
            }"#,
        )
        .unwrap();

        let host = SurfaceHost::new(vec![root.clone()]);
        let report = host.discover();
        assert_eq!(report.discovered, 1);
        assert!(host.has_external_surface("echo"));

        let result = host
            .send(SurfaceSendRequest {
                surface: "echo".to_string(),
                recipient: "room-1".to_string(),
                thread: None,
                text: "hello".to_string(),
                metadata: serde_json::Value::Null,
            })
            .await
            .unwrap();
        assert_eq!(result.status, "sent");
        assert_eq!(result.message_id.as_deref(), Some("m-1"));

        let _ = fs::remove_dir_all(root);
    }
}
