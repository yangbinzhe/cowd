mod budget;
mod graph;
mod materializer;
mod metrics;
mod recovery;
mod registry;
mod session;

pub use budget::{HotMemoryBudget, HotStateConfig, HotStateMemoryConfig};
pub use graph::HotExecutionGraphRegistry;
pub use materializer::{DerivedMaterialization, DerivedMaterializer, DerivedMaterializerHealth};
pub use metrics::{HotStateMetrics, HotStateMetricsSnapshot};
pub use recovery::RecoveryPermit;
pub use registry::{HotResidencyRegistry, HotResidencySnapshot, HotResidentClass};
pub use session::{HotSessionRegistry, HotSessionSnapshot};

use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotStateHealth {
    pub budget: HotMemoryBudget,
    pub metrics: HotStateMetricsSnapshot,
    pub materializer: DerivedMaterializerHealth,
    pub shard_count: usize,
    pub pressure_high: bool,
}

/// Process-local owner of active Runtime state. Durable stores remain the
/// recovery authority; this plane is the scheduling and query authority while
/// the process is alive.
#[derive(Clone)]
pub struct RuntimeHotStatePlane {
    budget: Arc<RwLock<HotMemoryBudget>>,
    residency: Arc<HotResidencyRegistry>,
    graphs: Arc<HotExecutionGraphRegistry>,
    sessions: Arc<HotSessionRegistry>,
    materializer: Arc<DerivedMaterializer>,
    metrics: Arc<HotStateMetrics>,
    shard_count: usize,
}

impl RuntimeHotStatePlane {
    #[must_use]
    pub fn new(config: HotStateConfig) -> Self {
        let budget = Arc::new(RwLock::new(HotMemoryBudget::resolve(&config.memory)));
        let metrics = Arc::new(HotStateMetrics::default());
        let residency = Arc::new(HotResidencyRegistry::new(
            Arc::clone(&budget),
            Arc::clone(&metrics),
        ));
        let shards = config.resolved_shards();
        let graphs = Arc::new(HotExecutionGraphRegistry::new(
            shards,
            Arc::clone(&residency),
            Arc::clone(&metrics),
        ));
        let sessions = Arc::new(HotSessionRegistry::new(
            shards,
            Arc::clone(&residency),
            Arc::clone(&metrics),
        ));
        let materializer = Arc::new(DerivedMaterializer::new(
            config.materializer_queue_capacity,
            Arc::clone(&metrics),
            Arc::clone(&residency),
        ));
        Self {
            budget,
            residency,
            graphs,
            sessions,
            materializer,
            metrics,
            shard_count: shards,
        }
    }

    #[must_use]
    pub fn budget(&self) -> HotMemoryBudget {
        self.budget
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn reconfigure(&self, config: &HotStateConfig) -> Result<HotMemoryBudget, String> {
        config.validate()?;
        let resolved = HotMemoryBudget::resolve(&config.memory);
        *self
            .budget
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = resolved.clone();
        Ok(resolved)
    }

    #[must_use]
    pub fn residency(&self) -> &Arc<HotResidencyRegistry> {
        &self.residency
    }

    #[must_use]
    pub fn graphs(&self) -> &Arc<HotExecutionGraphRegistry> {
        &self.graphs
    }

    #[must_use]
    pub fn sessions(&self) -> &Arc<HotSessionRegistry> {
        &self.sessions
    }

    #[must_use]
    pub fn materializer(&self) -> &Arc<DerivedMaterializer> {
        &self.materializer
    }

    #[must_use]
    pub fn metrics(&self) -> &Arc<HotStateMetrics> {
        &self.metrics
    }

    #[must_use]
    pub const fn shard_count(&self) -> usize {
        self.shard_count
    }

    #[must_use]
    pub fn health(&self) -> HotStateHealth {
        HotStateHealth {
            budget: self.budget(),
            metrics: self.metrics.snapshot(),
            materializer: self.materializer.health(),
            shard_count: self.shard_count,
            pressure_high: self.residency.pressure_high(),
        }
    }
}

impl Default for RuntimeHotStatePlane {
    fn default() -> Self {
        Self::new(HotStateConfig::default())
    }
}
