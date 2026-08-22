//! Durable, file-backed Team Template Definition storage and resolution.
//!
//! The module is deliberately isolated until the runtime composition root
//! adopts the V1 Definition projection.  Storage roots are supplied by the
//! caller; this domain never derives a config-home path on its own.

mod bootstrap;
mod import_export;
mod resolver;
mod storage_layout;
mod store;
mod validation;

pub use import_export::{DraftTeamTemplateImport, TeamTemplateExport};
pub use resolver::{
    ExactAgentRevisionResolver, ResolvedTeamTemplate, TeamTemplateDefinitionResolver,
};
pub use storage_layout::RegisteredTeamTemplateLayout;
pub use store::{
    ScopedTeamTemplateLayout, StoredTeamTemplateRevision, TeamDefaultPointer,
    TeamDefinitionStoreError, TeamReleaseAssignment, TeamTemplateDefinitionStore,
    TeamTemplateStorageLayout,
};

pub(crate) use bootstrap::{bootstrap_builtin_teams, BuiltinTeamTrust};
pub(crate) use validation::build_revision;
