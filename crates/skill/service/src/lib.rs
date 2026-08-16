#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

pub mod generation;
pub mod inspect;
pub mod package_lifecycle;
pub mod run;
pub mod skill_manifest;
pub mod skill_registry;
pub mod skill_router;
pub mod skill_security;
pub mod skill_tools;

pub use generation::{generate_skill_draft, SkillGenerationContext, SkillGenerationTrigger};
pub use inspect::{
    inspect_skill_package, profile_skill_catalog_entry, profile_skill_package, stable_skill_id,
};
pub use package_lifecycle::{
    default_managed_skill_store_root, list_managed_skill_entries, ManagedSkillActivePointerV1,
    ManagedSkillEntryV1, ManagedSkillStore, SkillInstallPlanV1, SkillInstallReceiptV1,
    SkillLifecycle, SkillLifecycleError, SkillLifecycleStatusV1, SkillPackageClassV1,
    SkillPackageFileV1, SkillSourceIdentityV1, SkillSourceKindV1, MAX_SKILL_ARCHIVE_BYTES,
    MAX_SKILL_DEPTH, MAX_SKILL_EXTRACTED_BYTES, MAX_SKILL_FILES, MAX_SKILL_FILE_BYTES,
    SKILL_STORE_SCHEMA_VERSION,
};
pub use run::{
    SkillActionKind, SkillRunEvidence, SkillRunPlan, SkillRunReceipt, SkillRunRecord,
    SkillRunStatus,
};
pub use skill_manifest::{
    check_prerequisites, get_config_vars, get_related_skills, get_skill_description,
    get_skill_name, get_tags, matches_platform, parse_skill_content, parse_skill_file,
    parse_skill_file_header, Platform, PrerequisitesCheck, SkillConditions, SkillConfigVar,
    SkillHermesMetadata, SkillManifest, SkillPrerequisites,
};
pub use skill_registry::{
    discover_skill_registry_roots, SkillInfo, SkillRegistry, SkillRegistryRoot,
    SkillRegistryRootKind, SkillRegistryScope, SkillRegistrySource,
};
pub use skill_router::{
    SkillActivationCandidate, SkillActivationResult, SkillRouter, SkillRouterConfig,
};
pub use skill_security::{
    scan_skill_content, scan_skill_file, FindingCategory, SecurityFinding, SecurityScanResult,
    SecurityStatus, Severity,
};
pub use skill_tools::{
    SkillCreateInput, SkillCreateOutput, SkillDeleteInput, SkillDeleteOutput, SkillEditInput,
    SkillEditOutput, SkillGenerateInput, SkillGenerateOutput, SkillLinkedFiles, SkillListInput,
    SkillListOutput, SkillManager, SkillMeta, SkillPrerequisitesStatus, SkillViewInput,
    SkillViewOutput,
};
