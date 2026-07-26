use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

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
        self.set_webui_static_resource(Some(dir));
    }

    pub(crate) fn set_webui_static_resource(&self, dir: Option<&Path>) {
        let mut registry = self
            .registry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(webui) = registry.get_mut("webui") {
            webui.resources.retain(|resource| resource.mount != "/");
            if let Some(dir) = dir {
                webui.resources.push(SurfaceResource {
                    kind: SurfaceResourceKind::Static,
                    mount: "/".to_string(),
                    dir: dir.display().to_string(),
                    spa: true,
                });
                webui
                    .diagnostics
                    .push("webui static assets registered as builtin surface resource".to_string());
            } else {
                webui.diagnostics.push(
                    "webui static assets unregistered from builtin surface resource".to_string(),
                );
            }
        }
    }

    pub(crate) fn set_configs(&self, configs: BTreeMap<String, serde_json::Value>) {
        let normalized = configs
            .into_iter()
            .map(|(id, value)| (normalize_surface_id(&id), value))
            .collect();
        *self
            .configs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = normalized;
    }

    pub(crate) fn discover(&self) -> SurfaceDiscoveryReport {
        let (descriptors, failures) = self.scan_manifest_descriptors();
        let mut discovered = 0usize;
        for descriptor in descriptors {
            self.upsert_discovered_descriptor(descriptor);
            discovered += 1;
        }
        SurfaceDiscoveryReport {
            roots: self.root_strings(),
            discovered,
            removed: Vec::new(),
            failures,
        }
    }

    pub(crate) async fn reload_manifests(&self) -> SurfaceDiscoveryReport {
        let (descriptors, failures) = self.scan_manifest_descriptors();
        let mut discovered_ids = BTreeSet::new();
        for descriptor in descriptors {
            discovered_ids.insert(descriptor.id.clone());
            self.upsert_discovered_descriptor(descriptor);
        }

        let stale = self.stale_manifest_surface_ids(&discovered_ids);
        for surface in &stale {
            let _ = self.stop_surface(surface).await;
        }
        self.remove_manifest_surfaces(&stale);

        SurfaceDiscoveryReport {
            roots: self.root_strings(),
            discovered: discovered_ids.len(),
            removed: stale,
            failures,
        }
    }

    fn scan_manifest_descriptors(&self) -> (Vec<SurfaceDescriptor>, Vec<SurfaceDiscoveryFailure>) {
        let mut failures = Vec::new();
        let mut descriptors = Vec::new();
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
                        descriptors.push(descriptor);
                    }
                    Err(error) => failures.push(SurfaceDiscoveryFailure {
                        path: manifest_path.display().to_string(),
                        error: error.to_string(),
                    }),
                }
            }
        }
        (descriptors, failures)
    }

    fn upsert_discovered_descriptor(&self, descriptor: SurfaceDescriptor) {
        self.runtime
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(descriptor.id.clone())
            .or_insert_with(|| {
                SurfaceRuntimeSnapshot::discovered(&descriptor.id, descriptor.lifecycle)
            });
        self.registry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(descriptor.id.clone(), descriptor);
    }

    fn stale_manifest_surface_ids(&self, discovered_ids: &BTreeSet<String>) -> Vec<String> {
        let mut stale = self
            .registry
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(id, descriptor)| {
                descriptor.source != "builtin"
                    && !discovered_ids.contains(*id)
                    && self.source_is_under_manifest_root(&descriptor.source)
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        stale.sort();
        stale
    }

    fn remove_manifest_surfaces(&self, ids: &[String]) {
        if ids.is_empty() {
            return;
        }
        let mut registry = self
            .registry
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut runtime = self
            .runtime
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for id in ids {
            registry.remove(id);
            runtime.remove(id);
        }
    }

    fn source_is_under_manifest_root(&self, source: &str) -> bool {
        let path = PathBuf::from(source);
        self.roots.iter().any(|root| path.starts_with(root))
    }

    fn root_strings(&self) -> Vec<String> {
        self.roots
            .iter()
            .map(|root| root.display().to_string())
            .collect()
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
            .filter(|surface| surface.is_executable())
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
            task_ownership: self.gateway_tasks.health(),
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
        self.get(id).is_some_and(|surface| surface.is_executable())
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
