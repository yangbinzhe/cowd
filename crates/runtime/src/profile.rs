//! Profile isolation for multi-user / multi-project setups.
//!
//! Each profile has its own independent storage for:
//! - Configuration (`config.yaml`)
//! - Memory / sessions (`memory/`)
//! - Permissions (`permissions.yaml`)
//!
//! Profiles are stored under `~/.cowd/profiles/{id}/`.
//! A "default" profile is always present.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

const ACTIVE_PROFILE_FILE: &str = "active_profile";
const PROFILE_META_FILE: &str = "profile.json";

/// Metadata for a single profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileMeta {
    pub id: String,
    pub name: String,
    pub created_at: String,
    pub is_active: bool,
}

/// A fully resolved profile with its directory paths.
#[derive(Debug, Clone)]
pub struct Profile {
    pub id: String,
    pub name: String,
    /// Root directory: `~/.cowd/profiles/{id}/`
    pub base_dir: PathBuf,
}

impl Profile {
    /// Path to the profile's config file.
    pub fn config_path(&self) -> PathBuf {
        self.base_dir.join("config.yaml")
    }

    /// Path to the profile's memory directory.
    pub fn memory_dir(&self) -> PathBuf {
        self.base_dir.join("memory")
    }

    /// Path to the profile's sessions database.
    pub fn sessions_db(&self) -> PathBuf {
        self.memory_dir().join("sessions.db")
    }

    /// Path to the profile's sessions JSONL directory.
    pub fn sessions_jsonl_dir(&self) -> PathBuf {
        self.memory_dir().join("sessions")
    }

    /// Path to the profile's permissions file.
    pub fn permissions_path(&self) -> PathBuf {
        self.base_dir.join("permissions.yaml")
    }

    /// Ensure all required subdirectories exist.
    pub fn ensure_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.base_dir)?;
        std::fs::create_dir_all(self.memory_dir())?;
        std::fs::create_dir_all(self.sessions_jsonl_dir())?;
        Ok(())
    }
}

/// Manages multiple profiles with independent configuration and storage.
pub struct ProfileManager {
    /// Root directory: `~/.cowd/profiles/`
    profiles_dir: PathBuf,
    /// Currently active profile ID.
    active_profile: RwLock<String>,
}

impl ProfileManager {
    /// Create a new ProfileManager rooted at `~/.cowd/profiles/`.
    pub fn new(home_dir: &Path) -> Self {
        Self::new_with_profiles_dir(home_dir.join(".cowd").join("profiles"))
    }

    /// Create a manager rooted at an already-resolved config home.
    ///
    /// This is the preferred constructor for daemons/tests that already use
    /// `COWD_CONFIG_HOME`, because `new(home)` assumes a user home and appends
    /// `.cowd/profiles`.
    pub fn from_config_home(config_home: &Path) -> Self {
        Self::new_with_profiles_dir(config_home.join("profiles"))
    }

    /// Create a manager rooted at an explicit profiles directory.
    pub fn new_with_profiles_dir(profiles_dir: PathBuf) -> Self {
        let active = read_active_profile_id(&profiles_dir).unwrap_or_else(|| "default".to_string());
        Self {
            profiles_dir,
            active_profile: RwLock::new(active),
        }
    }

    /// Ensure the profiles directory and default profile exist.
    pub fn initialize(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.profiles_dir)?;
        let default = self.get_profile("default");
        if default.is_none() {
            self.create_profile("default")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        }
        if read_active_profile_id(&self.profiles_dir).is_none() {
            self.persist_active_profile("default")
                .map_err(std::io::Error::other)?;
        }
        Ok(())
    }

    /// Create a new profile with the given name.
    pub fn create_profile(&self, name: &str) -> Result<Profile, String> {
        let id = sanitize_profile_id(name);
        let profile = Profile {
            id: id.clone(),
            name: name.to_string(),
            base_dir: self.profiles_dir.join(&id),
        };

        if profile.base_dir.exists() {
            return Err(format!("Profile '{}' already exists", id));
        }

        profile
            .ensure_dirs()
            .map_err(|e| format!("Failed to create profile dirs: {}", e))?;

        // Write a default config.yaml
        let config_content = format!(
            "# Profile: {}\n# Created: {}\n",
            name,
            chrono::Utc::now().to_rfc3339()
        );
        std::fs::write(profile.config_path(), config_content)
            .map_err(|e| format!("Failed to write config: {}", e))?;

        // Write a default permissions.yaml
        let perms_content =
            "# Default permissions for this profile\npermission_mode: workspace_write\n";
        std::fs::write(profile.permissions_path(), perms_content)
            .map_err(|e| format!("Failed to write permissions: {}", e))?;

        let meta = ProfileMeta {
            id: id.clone(),
            name: name.to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            is_active: false,
        };
        write_profile_meta(&profile, &meta)?;

        Ok(profile)
    }

    /// List all available profiles.
    pub fn list_profiles(&self) -> Vec<ProfileMeta> {
        let active = self
            .active_profile
            .read()
            .unwrap_or_else(|poisoned| {
                tracing::warn!("profile manager RwLock poisoned; recovering");
                poisoned.into_inner()
            })
            .clone();
        let mut result = Vec::new();

        if let Ok(entries) = std::fs::read_dir(&self.profiles_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    let id = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("")
                        .to_string();

                    let created_at = std::fs::metadata(&path)
                        .and_then(|m| m.created())
                        .map(|t| {
                            let dur = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                            chrono::DateTime::from_timestamp(dur.as_secs() as i64, 0)
                                .map(|dt| dt.to_rfc3339())
                                .unwrap_or_default()
                        })
                        .unwrap_or_default();

                    let is_active = id == active;
                    let profile = Profile {
                        id: id.clone(),
                        name: id.clone(),
                        base_dir: path,
                    };
                    let mut meta = read_profile_meta(&profile).unwrap_or(ProfileMeta {
                        id: id.clone(),
                        name: id.clone(),
                        created_at,
                        is_active,
                    });
                    meta.id = id;
                    meta.is_active = is_active;
                    result.push(meta);
                }
            }
        }

        result.sort_by(|a, b| {
            // Default always first, then alphabetical
            match (a.id == "default", b.id == "default") {
                (true, _) => std::cmp::Ordering::Less,
                (_, true) => std::cmp::Ordering::Greater,
                _ => a.id.cmp(&b.id),
            }
        });

        result
    }

    /// Get a specific profile by name.
    pub fn get_profile(&self, name: &str) -> Option<Profile> {
        let id = sanitize_profile_id(name);
        let base_dir = self.profiles_dir.join(&id);
        if base_dir.exists() {
            let profile = Profile {
                id,
                name: name.to_string(),
                base_dir,
            };
            let name = read_profile_meta(&profile)
                .map(|meta| meta.name)
                .unwrap_or_else(|| profile.name.clone());
            Some(Profile { name, ..profile })
        } else {
            None
        }
    }

    /// Get the currently active profile.
    pub fn active_profile(&self) -> Profile {
        let active = self
            .active_profile
            .read()
            .unwrap_or_else(|poisoned| {
                tracing::warn!("profile manager RwLock poisoned; recovering");
                poisoned.into_inner()
            })
            .clone();
        self.get_profile(&active).unwrap_or_else(|| {
            // Fallback to default if active is somehow invalid
            self.get_profile("default")
                .expect("default profile must exist")
        })
    }

    /// Switch the active profile.
    pub fn switch_profile(&self, name: &str) -> Result<(), String> {
        let profile = self
            .get_profile(name)
            .ok_or_else(|| format!("Profile '{}' not found", name))?;

        let mut active = self.active_profile.write().unwrap_or_else(|poisoned| {
            tracing::warn!("profile manager RwLock poisoned; recovering");
            poisoned.into_inner()
        });
        *active = profile.id.clone();
        self.persist_active_profile(&profile.id)?;
        Ok(())
    }

    /// Delete a profile (cannot delete the default or active profile).
    pub fn delete_profile(&self, name: &str) -> Result<(), String> {
        let id = sanitize_profile_id(name);

        if id == "default" {
            return Err("Cannot delete the default profile".to_string());
        }

        let active = self
            .active_profile
            .read()
            .unwrap_or_else(|poisoned| {
                tracing::warn!("profile manager RwLock poisoned; recovering");
                poisoned.into_inner()
            })
            .clone();
        if id == active {
            return Err(
                "Cannot delete the active profile. Switch to another profile first.".to_string(),
            );
        }

        let base_dir = self.profiles_dir.join(&id);
        if !base_dir.exists() {
            return Err(format!("Profile '{}' not found", id));
        }

        std::fs::remove_dir_all(&base_dir)
            .map_err(|e| format!("Failed to delete profile: {}", e))?;

        Ok(())
    }

    /// Convenience: initialize the default profile from the user's home directory.
    pub fn init_default(home: &std::path::Path) -> Result<String, String> {
        let mgr = Self::new(home);
        mgr.initialize().map_err(|e| format!("profile init: {e}"))?;
        Ok(mgr.active_id().to_string())
    }

    /// Return the currently active profile ID.
    pub fn active_id(&self) -> String {
        self.active_profile
            .read()
            .unwrap_or_else(|poisoned| {
                tracing::warn!("profile manager RwLock poisoned; recovering");
                poisoned.into_inner()
            })
            .clone()
    }

    /// Return the underlying profiles directory.
    pub fn profiles_dir(&self) -> &Path {
        &self.profiles_dir
    }

    fn persist_active_profile(&self, id: &str) -> Result<(), String> {
        fs::create_dir_all(&self.profiles_dir)
            .map_err(|e| format!("Failed to create profiles dir: {e}"))?;
        fs::write(self.profiles_dir.join(ACTIVE_PROFILE_FILE), id)
            .map_err(|e| format!("Failed to persist active profile: {e}"))
    }
}

fn sanitize_profile_id(name: &str) -> String {
    let mut id = String::new();
    let mut last_was_sep = false;
    for ch in name.trim().to_lowercase().chars() {
        if ch.is_ascii_alphanumeric() {
            id.push(ch);
            last_was_sep = false;
        } else if matches!(ch, '-' | '_' | ' ') && !last_was_sep {
            id.push('_');
            last_was_sep = true;
        }
    }
    let id = id.trim_matches('_').to_string();
    if id.is_empty() {
        "profile".to_string()
    } else {
        id
    }
}

fn read_active_profile_id(profiles_dir: &Path) -> Option<String> {
    fs::read_to_string(profiles_dir.join(ACTIVE_PROFILE_FILE))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_profile_meta(profile: &Profile) -> Option<ProfileMeta> {
    let raw = fs::read_to_string(profile.base_dir.join(PROFILE_META_FILE)).ok()?;
    serde_json::from_str(&raw).ok()
}

fn write_profile_meta(profile: &Profile, meta: &ProfileMeta) -> Result<(), String> {
    let raw = serde_json::to_string_pretty(meta)
        .map_err(|e| format!("Failed to serialize profile metadata: {e}"))?;
    fs::write(profile.base_dir.join(PROFILE_META_FILE), raw)
        .map_err(|e| format!("Failed to write profile metadata: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_list_profiles() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = ProfileManager::new(tmp.path());
        mgr.initialize().unwrap();

        // Default should exist
        let profiles = mgr.list_profiles();
        assert!(profiles.iter().any(|p| p.id == "default"));

        // Create another profile
        let p = mgr.create_profile("work").unwrap();
        assert!(p.base_dir.exists());
        assert!(p.config_path().exists());
        assert!(p.permissions_path().exists());

        let profiles = mgr.list_profiles();
        assert_eq!(profiles.len(), 2);
    }

    #[test]
    fn switch_profile() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = ProfileManager::new(tmp.path());
        mgr.initialize().unwrap();
        mgr.create_profile("personal").unwrap();

        assert_eq!(mgr.active_profile().id, "default");

        mgr.switch_profile("personal").unwrap();
        assert_eq!(mgr.active_profile().id, "personal");
    }

    #[test]
    fn active_profile_persists_across_managers() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = ProfileManager::from_config_home(tmp.path());
        mgr.initialize().unwrap();
        mgr.create_profile("Enterprise Ops").unwrap();
        mgr.switch_profile("enterprise_ops").unwrap();

        let reopened = ProfileManager::from_config_home(tmp.path());
        reopened.initialize().unwrap();

        assert_eq!(reopened.active_id(), "enterprise_ops");
        let profiles = reopened.list_profiles();
        assert!(profiles
            .iter()
            .any(|p| p.id == "enterprise_ops" && p.name == "Enterprise Ops" && p.is_active));
    }

    #[test]
    fn cannot_delete_default_or_active() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = ProfileManager::new(tmp.path());
        mgr.initialize().unwrap();
        mgr.create_profile("work").unwrap();

        assert!(mgr.delete_profile("default").is_err());
        // work is not the active profile, so it should be deletable
        assert!(mgr.delete_profile("work").is_ok());
    }

    #[test]
    fn delete_non_active_profile() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = ProfileManager::new(tmp.path());
        mgr.initialize().unwrap();
        mgr.create_profile("temp").unwrap();

        // Default is active, temp can be deleted
        assert!(mgr.delete_profile("temp").is_ok());
        assert!(mgr.get_profile("temp").is_none());
    }
}
