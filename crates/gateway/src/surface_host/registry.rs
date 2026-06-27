use std::path::Path;

use surface::{
    builtin_surfaces, normalize_surface_id, SurfaceDescriptor, SurfaceLifecycle, SurfaceManifest,
    SurfaceRegistrySnapshot, SurfaceResource, SurfaceResourceKind, SurfaceRuntimeSnapshot,
    SurfaceRuntimeStatus, SURFACE_MANIFEST_FILE,
};

use super::{SurfaceDiscoveryFailure, SurfaceDiscoveryReport, SurfaceHost, SurfaceHostHealth};

impl SurfaceHost {
    pub(crate) fn register_builtin_surfaces(&self) {
        let mut registry = self
            .registry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut runtime = self
            .runtime
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for manifest in builtin_surfaces().into_values() {
            let descriptor = SurfaceDescriptor::from_manifest(&manifest, "builtin");
            runtime.insert(
                descriptor.id.clone(),
                SurfaceRuntimeSnapshot::builtin(&descriptor.id),
            );
            registry.insert(descriptor.id.clone(), descriptor);
        }
    }

    pub(crate) fn register_webui_static_resource(&self, dir: &Path) {
        let mut registry = self
            .registry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(webui) = registry.get_mut("webui") {
            webui.resources.retain(|resource| resource.mount != "/");
            webui.resources.push(SurfaceResource {
                kind: SurfaceResourceKind::Static,
                mount: "/".to_string(),
                dir: dir.display().to_string(),
                spa: true,
            });
            webui
                .diagnostics
                .push("webui static assets registered as builtin surface resource".to_string());
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
                        self.runtime
                            .write()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .entry(descriptor.id.clone())
                            .or_insert_with(|| {
                                SurfaceRuntimeSnapshot::discovered(
                                    &descriptor.id,
                                    descriptor.lifecycle,
                                )
                            });
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

    pub(crate) fn runtime_snapshot(&self, id: &str) -> Option<SurfaceRuntimeSnapshot> {
        self.runtime
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&normalize_surface_id(id))
            .cloned()
    }

    pub(crate) fn runtime_snapshots(&self) -> Vec<SurfaceRuntimeSnapshot> {
        let mut snapshots = self
            .runtime
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        snapshots.sort_by(|left, right| left.surface.cmp(&right.surface));
        snapshots
    }

    pub(crate) fn health(&self) -> SurfaceHostHealth {
        let snapshot = self.snapshot();
        let runtime = self.runtime_snapshots();
        let external_surface_count = snapshot
            .surfaces
            .iter()
            .filter(|surface| surface.entry.is_some())
            .count();
        let route_count = snapshot
            .surfaces
            .iter()
            .map(|surface| surface.routes.len())
            .sum();
        let resource_count = snapshot
            .surfaces
            .iter()
            .map(|surface| surface.resources.len())
            .sum();
        let ready_count = runtime
            .iter()
            .filter(|surface| {
                matches!(
                    surface.status,
                    SurfaceRuntimeStatus::Ready | SurfaceRuntimeStatus::Builtin
                )
            })
            .count();
        let degraded_count = runtime
            .iter()
            .filter(|surface| surface.status == SurfaceRuntimeStatus::Degraded)
            .count();
        let failed_count = runtime
            .iter()
            .filter(|surface| {
                matches!(
                    surface.status,
                    SurfaceRuntimeStatus::Failed | SurfaceRuntimeStatus::Unavailable
                )
            })
            .count();
        let circuit_open_count = runtime
            .iter()
            .filter(|surface| surface.status == SurfaceRuntimeStatus::CircuitOpen)
            .count();
        let status = if failed_count > 0 || circuit_open_count > 0 {
            "degraded"
        } else if degraded_count > 0 {
            "warning"
        } else {
            "ready"
        }
        .to_string();
        SurfaceHostHealth {
            status,
            surface_count: snapshot.surfaces.len(),
            external_surface_count,
            route_count,
            resource_count,
            ready_count,
            degraded_count,
            failed_count,
            circuit_open_count,
            roots: self
                .roots
                .iter()
                .map(|root| root.display().to_string())
                .collect(),
        }
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

    pub(super) fn config_for(&self, id: &str) -> Option<serde_json::Value> {
        self.configs
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&normalize_surface_id(id))
            .cloned()
    }

    pub(super) fn runtime_for_discovered(
        &self,
        id: &str,
        lifecycle: SurfaceLifecycle,
    ) -> SurfaceRuntimeSnapshot {
        self.runtime_snapshot(id)
            .unwrap_or_else(|| SurfaceRuntimeSnapshot::discovered(id, lifecycle))
    }
}
