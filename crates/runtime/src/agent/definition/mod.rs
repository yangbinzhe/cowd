//! Durable, file-backed Agent Definition storage and resolution.
//!
//! This module is intentionally self-contained until the runtime composition
//! root adopts it.  Callers provide a [`DefinitionStorageLayout`] so this
//! domain never derives paths from a configuration home by itself.

mod bootstrap;
mod import_export;
mod resolver;
mod storage_layout;
mod store;
mod validation;

pub use import_export::{
    AgentDefinitionExport, DraftAgentDefinitionImport, ExplicitTomlAgentImport,
};
pub use resolver::{AgentDefinitionResolver, ResolvedAgentDefinition};
pub use storage_layout::RegisteredAgentDefinitionLayout;
pub use store::{
    AgentDefinitionStore, DefinitionStorageLayout, DefinitionStoreError, ScopedDefinitionLayout,
    StoredAgentDefinitionRevision,
};

pub(crate) use bootstrap::{bootstrap_builtin_agents, BuiltinAgentTrust};
