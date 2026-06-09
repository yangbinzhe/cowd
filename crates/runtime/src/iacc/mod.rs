//! IACC structured operations cognition contracts.
//!
//! The IACC layer is an optional structured-data cognition substrate on top of
//! Cowd runtime. It stores operational facts, attention items, and bounded
//! evidence packets without replacing source systems, connector resources, or
//! memory runtime.

mod analysis;
mod attention;
mod change;
mod evidence;
mod fact;
mod incident;
mod metric;
mod source;
mod store;

pub use analysis::{
    IaccAttributionCandidate, IaccImpactPath, IaccOperationalAnalysis, IaccRecommendedAction,
};
pub use attention::{IaccAttentionItem, IaccSeverity};
pub use change::IaccChangeEvent;
pub use evidence::{IaccEvidencePacket, IaccEvidenceSourceRef};
pub use fact::{IaccFact, IaccFactInput};
pub use incident::IaccIncident;
pub use metric::{IaccMetricDefinition, IaccMetricState, IaccMetricStatus};
pub use source::{IaccSourceKind, IaccSourceSnapshot};
pub use store::{
    IaccHealth, IaccMetricRecomputeResult, IaccStore, IaccStoreError, IACC_SCHEMA_VERSION,
};

#[must_use]
pub fn iacc_reference(kind: &str, id: &str) -> String {
    format!("iacc:{kind}:{id}")
}
