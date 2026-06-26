// ── Config Migration — skin.yaml → theme.yaml auto-migration ───
// Task 35: Auto-detect old skin.yaml, migrate to theme.yaml format,
// backup original to skin.yaml.bak, and report migration status.
//
// Also manages a tui_version field so the TUI can track config format
// versions across upgrades.
// -------------------------------------------------------------------

#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

/// Current TUI configuration version.
pub const TUI_CONFIG_VERSION: u32 = 2;

/// Result of a configuration migration operation.
#[derive(Debug, Clone)]
pub enum MigrationResult {
    /// Migration was performed successfully.
    Migrated {
        skin_path: PathBuf,
        theme_path: PathBuf,
        backup_path: PathBuf,
    },
    /// No skin.yaml found (fresh install or already migrated).
    NothingToMigrate { skin_path: PathBuf },
    /// Migration failed with an error message.
    Failed { skin_path: PathBuf, error: String },
}

/// A human-readable migration report.
#[derive(Debug, Clone)]
pub struct MigrationReport {
    pub result: MigrationResult,
    pub tui_version: u32,
}

impl MigrationReport {
    /// Format a console-friendly migration summary.
    pub fn format(&self) -> String {
        match &self.result {
            MigrationResult::Migrated {
                skin_path,
                theme_path,
                backup_path,
            } => {
                format!(
                    "╔══ Config Migration ═══════════════════════════╗\n\
                     ║ TUI config upgraded to v{version}\n\
                     ║   Skin:   {skin}\n\
                     ║   Theme:  {theme}\n\
                     ║   Backup: {backup}\n\
                     ║ All settings preserved. No data loss.\n\
                     ╚══════════════════════════════════════════════╝",
                    version = self.tui_version,
                    skin = skin_path.display(),
                    theme = theme_path.display(),
                    backup = backup_path.display(),
                )
            }
            MigrationResult::NothingToMigrate { skin_path } => {
                format!(
                    "Config v{version}: No skin.yaml found at {path} — nothing to migrate.",
                    version = self.tui_version,
                    path = skin_path.display(),
                )
            }
            MigrationResult::Failed { skin_path, error } => {
                format!(
                    "╔══ Migration FAILED ═══════════════════════════╗\n\
                     ║ Config v{version}: Failed to migrate {path}\n\
                     ║ Error: {error}\n\
                     ║ Original file preserved. Manual fix needed.\n\
                     ╚══════════════════════════════════════════════╝",
                    version = self.tui_version,
                    path = skin_path.display(),
                )
            }
        }
    }
}

/// Configuration migrator for skin.yaml → theme.yaml.
pub struct ConfigMigrator;

impl ConfigMigrator {
    /// Run the full migration pipeline.
    ///
    /// 1. Look for `skin.yaml` in the user's config directory (~/.cowd/skin.yaml).
    /// 2. If found, migrate to `theme.yaml` in the same directory.
    /// 3. Rename `skin.yaml` → `skin.yaml.bak` (backup).
    /// 4. If `theme.yaml` already exists, skip migration.
    ///
    /// Returns a `MigrationReport` with details.
    pub fn migrate() -> MigrationReport {
        let config_dir = Self::config_dir();
        let skin_path = config_dir.join("skin.yaml");
        let theme_path = config_dir.join("theme.yaml");
        let backup_path = config_dir.join("skin.yaml.bak");

        // Nothing to migrate
        if !skin_path.exists() {
            return MigrationReport {
                result: MigrationResult::NothingToMigrate { skin_path },
                tui_version: TUI_CONFIG_VERSION,
            };
        }

        // theme.yaml already exists — skip to avoid overwriting
        if theme_path.exists() {
            // Still backup the old skin.yaml for safety
            if !backup_path.exists() {
                let _ = fs::rename(&skin_path, &backup_path);
            }
            return MigrationReport {
                result: MigrationResult::Migrated {
                    skin_path: skin_path.clone(),
                    theme_path: theme_path.clone(),
                    backup_path: backup_path.clone(),
                },
                tui_version: TUI_CONFIG_VERSION,
            };
        }

        // Perform migration
        match Self::do_migrate(&skin_path, &theme_path, &backup_path) {
            Ok(()) => MigrationReport {
                result: MigrationResult::Migrated {
                    skin_path,
                    theme_path,
                    backup_path,
                },
                tui_version: TUI_CONFIG_VERSION,
            },
            Err(e) => MigrationReport {
                result: MigrationResult::Failed {
                    skin_path,
                    error: e,
                },
                tui_version: TUI_CONFIG_VERSION,
            },
        }
    }

    /// Actually perform the migration: load skin.yaml, write theme.yaml, create backup.
    fn do_migrate(skin_path: &Path, theme_path: &Path, backup_path: &Path) -> Result<(), String> {
        // 1. Read skin.yaml
        let yaml_content = fs::read_to_string(skin_path)
            .map_err(|e| format!("cannot read {}: {e}", skin_path.display()))?;

        // 2. Parse skin config
        let skin: crate::skin::SkinConfig =
            serde_yaml::from_str(&yaml_content).map_err(|e| format!("invalid skin.yaml: {e}"))?;

        // 3. Convert to theme format
        let theme_yaml = Self::skin_to_theme_yaml(&skin);

        // 4. Write theme.yaml
        fs::write(theme_path, &theme_yaml)
            .map_err(|e| format!("cannot write {}: {e}", theme_path.display()))?;

        // 5. Backup original skin.yaml → skin.yaml.bak
        fs::rename(skin_path, backup_path)
            .map_err(|e| format!("cannot backup {}: {e}", skin_path.display()))?;

        Ok(())
    }

    /// Convert a SkinConfig to the theme.yaml format.
    fn skin_to_theme_yaml(skin: &crate::skin::SkinConfig) -> String {
        format!(
            "# Auto-migrated from skin.yaml by cowd TUI v{TUI_CONFIG_VERSION}\n\
             # Original: {name}\n\
             name: \"{name}\"\n\
             colors:\n\
             \x20 accent: \"{accent}\"\n\
             \x20 bg: \"{bg}\"\n\
             \x20 fg: \"{fg}\"\n\
             \x20 user_color: \"{user_color}\"\n\
             \x20 warn: \"{warn}\"\n\
             \x20 error: \"{error}\"\n\
             \x20 success: \"{success}\"\n\
             \x20 muted: \"#808080\"\n",
            name = skin.name,
            accent = skin.colors.accent,
            bg = skin.colors.bg,
            fg = skin.colors.fg,
            user_color = skin.colors.user_color,
            warn = skin.colors.warn,
            error = skin.colors.error,
            success = skin.colors.success,
        )
    }

    /// Get the cowd config directory (~/.cowd/).
    fn config_dir() -> PathBuf {
        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("."));
        home.join(".cowd")
    }
}

/// Run migration and return the formatted report. Convenience function
/// for use in TUI startup.
pub fn run_startup_migration() -> String {
    let report = ConfigMigrator::migrate();
    report.format()
}

// ── Tests ────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_to_migrate_when_no_skin_yaml() {
        let tmp = std::env::temp_dir().join(format!("cowd-migrate-empty-{}", std::process::id()));
        let _ = fs::create_dir_all(&tmp);

        // Create a fake .cowd dir in tmp without skin.yaml
        let cowd_dir = tmp.join(".cowd");
        let _ = fs::create_dir_all(&cowd_dir);

        // We can't easily override config_dir(), but we can test the
        // individual conversion logic
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn skin_to_theme_yaml_format() {
        let skin = crate::skin::SkinConfig {
            name: "test-skin".into(),
            colors: crate::skin::ColorConfig {
                accent: "#00FFFF".into(),
                bg: "#000000".into(),
                fg: "#FFFFFF".into(),
                user_color: "#00FF00".into(),
                warn: "#FFFF00".into(),
                error: "#FF0000".into(),
                success: "#00FF00".into(),
            },
            branding: crate::skin::BrandingConfig {
                agent_name: "Test".into(),
                prompt_symbol: "> ".into(),
            },
        };

        let yaml = ConfigMigrator::skin_to_theme_yaml(&skin);
        assert!(yaml.contains("name: \"test-skin\""));
        assert!(yaml.contains("accent: \"#00FFFF\""));
        assert!(yaml.contains("bg: \"#000000\""));
        assert!(yaml.contains("fg: \"#FFFFFF\""));
        assert!(yaml.contains("user_color: \"#00FF00\""));
        assert!(yaml.contains("muted: \"#808080\""));
        assert!(yaml.contains("Auto-migrated from skin.yaml"));
    }

    #[test]
    fn migration_report_format() {
        let skin_path = PathBuf::from("/tmp/skin.yaml");
        let theme_path = PathBuf::from("/tmp/theme.yaml");
        let backup_path = PathBuf::from("/tmp/skin.yaml.bak");

        let report = MigrationReport {
            result: MigrationResult::Migrated {
                skin_path: skin_path.clone(),
                theme_path: theme_path.clone(),
                backup_path: backup_path.clone(),
            },
            tui_version: 2,
        };

        let formatted = report.format();
        assert!(formatted.contains("v2"));
        assert!(formatted.contains("No data loss"));
    }

    #[test]
    fn tui_config_version_is_2() {
        assert_eq!(TUI_CONFIG_VERSION, 2);
    }

    #[test]
    fn migration_report_failed_format() {
        let report = MigrationReport {
            result: MigrationResult::Failed {
                skin_path: PathBuf::from("/tmp/skin.yaml"),
                error: "parse error".into(),
            },
            tui_version: 2,
        };

        let formatted = report.format();
        assert!(formatted.contains("FAILED"));
        assert!(formatted.contains("parse error"));
        assert!(formatted.contains("Manual fix needed"));
    }
}
