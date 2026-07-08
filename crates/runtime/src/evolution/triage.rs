use std::collections::BTreeMap;

use super::signal::EvolutionSignal;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvolutionTriageCluster {
    pub key: String,
    pub signals: Vec<EvolutionSignal>,
}

#[derive(Debug, Clone, Default)]
pub struct EvolutionTriageService;

impl EvolutionTriageService {
    #[must_use]
    pub fn cluster(signals: &[EvolutionSignal]) -> Vec<EvolutionTriageCluster> {
        let mut grouped = BTreeMap::<String, Vec<EvolutionSignal>>::new();
        for signal in signals {
            grouped
                .entry(signal.aggregate_key())
                .or_default()
                .push(signal.clone());
        }
        grouped
            .into_iter()
            .map(|(key, signals)| EvolutionTriageCluster { key, signals })
            .collect()
    }
}
