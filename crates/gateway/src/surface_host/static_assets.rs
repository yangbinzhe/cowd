use std::path::{Component, Path, PathBuf};

use surface::{SurfaceDescriptor, SurfaceError, SurfaceResource};

use super::{SurfaceHost, SurfaceResourceSummary, SurfaceRouteSummary, SurfaceStaticFile};

pub(crate) const STATIC_ENTRY_CACHE_CONTROL: &str = "no-cache, no-store, must-revalidate";
pub(crate) const STATIC_HASHED_ASSET_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";
pub(crate) const STATIC_ASSET_CACHE_CONTROL: &str = "no-cache, must-revalidate";

pub(crate) fn cache_control_for_static_file(file: &SurfaceStaticFile) -> &'static str {
    if file.spa_fallback
        || file
            .file_path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("html"))
    {
        return STATIC_ENTRY_CACHE_CONTROL;
    }
    if file
        .file_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.rsplit_once('-'))
        .is_some_and(|(_, digest)| {
            digest.len() >= 8
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        })
    {
        return STATIC_HASHED_ASSET_CACHE_CONTROL;
    }
    STATIC_ASSET_CACHE_CONTROL
}

impl SurfaceHost {
    pub(crate) fn routes(&self, id: &str) -> Option<SurfaceRouteSummary> {
        self.get(id).map(|surface| SurfaceRouteSummary {
            surface: surface.id,
            routes: surface.routes,
        })
    }

    pub(crate) fn resources(&self, id: &str) -> Option<SurfaceResourceSummary> {
        self.get(id).map(|surface| SurfaceResourceSummary {
            surface: surface.id,
            resources: surface.resources,
        })
    }

    pub(crate) fn resolve_static(
        &self,
        id: &str,
        requested_path: &str,
    ) -> Result<Option<SurfaceStaticFile>, SurfaceError> {
        if !request_path_is_safe(requested_path) {
            return Ok(None);
        }
        let Some(surface) = self.get(id) else {
            return Ok(None);
        };
        let requested_path = normalize_request_path(requested_path);
        for resource in &surface.resources {
            let mount = normalize_mount(&resource.mount);
            if !path_matches_mount(&requested_path, &mount) {
                continue;
            }
            let relative = strip_mount(&requested_path, &mount);
            let base = resource_base_dir(&surface, resource);
            if let Some(file_path) =
                resolve_resource_file(&base, &relative).filter(|file_path| file_path.is_file())
            {
                return Ok(Some(SurfaceStaticFile {
                    surface: surface.id,
                    mount,
                    requested_path,
                    file_path,
                    spa_fallback: false,
                }));
            }
            if resource.spa {
                let index = resolve_resource_file(&base, "index.html");
                if let Some(index) = index.filter(|index| index.is_file()) {
                    return Ok(Some(SurfaceStaticFile {
                        surface: surface.id,
                        mount,
                        requested_path,
                        file_path: index,
                        spa_fallback: true,
                    }));
                }
            }
        }
        Ok(None)
    }
}

pub(super) fn normalize_request_path(path: &str) -> String {
    let mut cleaned = PathBuf::new();
    for component in Path::new(path.trim_start_matches('/')).components() {
        match component {
            Component::Normal(part) => cleaned.push(part),
            Component::CurDir => {}
            Component::RootDir | Component::ParentDir | Component::Prefix(_) => {}
        }
    }
    let normalized = cleaned.to_string_lossy().replace('\\', "/");
    if normalized.is_empty() {
        "/".to_string()
    } else {
        format!("/{normalized}")
    }
}

fn request_path_is_safe(path: &str) -> bool {
    Path::new(path.trim_start_matches('/'))
        .components()
        .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

fn normalize_mount(mount: &str) -> String {
    let normalized = normalize_request_path(mount);
    if normalized == "/" {
        normalized
    } else {
        normalized.trim_end_matches('/').to_string()
    }
}

fn path_matches_mount(path: &str, mount: &str) -> bool {
    if mount == "/" {
        return true;
    }
    path == mount
        || path
            .strip_prefix(mount)
            .is_some_and(|tail| tail.starts_with('/'))
}

fn strip_mount(path: &str, mount: &str) -> String {
    if mount == "/" {
        return path.trim_start_matches('/').to_string();
    }
    path.strip_prefix(mount)
        .unwrap_or(path)
        .trim_start_matches('/')
        .to_string()
}

fn resource_base_dir(surface: &SurfaceDescriptor, resource: &SurfaceResource) -> PathBuf {
    let declared = PathBuf::from(&resource.dir);
    if declared.is_absolute() {
        return declared;
    }
    let source_path = PathBuf::from(&surface.source);
    let root = source_path.parent().unwrap_or_else(|| Path::new("."));
    root.join(declared)
}

fn resolve_resource_file(base: &Path, relative: &str) -> Option<PathBuf> {
    let base = base.canonicalize().ok()?;
    let mut candidate = base.clone();
    for component in Path::new(relative).components() {
        match component {
            Component::Normal(part) => candidate.push(part),
            Component::CurDir => {}
            Component::RootDir | Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    let canonical = candidate.canonicalize().ok()?;
    if canonical.starts_with(&base) {
        Some(canonical)
    } else {
        None
    }
}
