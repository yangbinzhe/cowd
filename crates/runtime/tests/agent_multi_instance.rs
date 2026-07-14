#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;

use harness_contract::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionSelector,
};
use runtime::{AgentBindingRequest, RuntimeServices};

#[test]
fn one_definition_compiles_eight_isolated_instances_with_a_shared_revision_digest() {
    let services = RuntimeServices::in_memory().expect("runtime");
    let definition = AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/explore").unwrap();
    let snapshots = (1..=8)
        .map(|slot| {
            let mut request = AgentBindingRequest::new(
                definition.clone(),
                RevisionSelector::LatestApprovedStable,
                format!("instance:researcher:{slot}"),
                format!("session:research:{slot}"),
                format!("task:research:{slot}"),
            );
            request.role_slot_id = Some(format!("researcher:{slot}"));
            request.team_id = Some("team:parallel-research".to_string());
            request.granted_capabilities = vec![AgentCapability::Read, AgentCapability::Search];
            request.fact_boundaries = vec!["observed".to_string()];
            services
                .compile_agent_binding(request)
                .expect("bounded instance binding")
                .snapshot
        })
        .collect::<Vec<_>>();
    assert_eq!(snapshots.len(), 8);
    assert_eq!(
        snapshots
            .iter()
            .map(|binding| binding.definition_ref.definition_id.as_str().to_string())
            .collect::<BTreeSet<_>>()
            .len(),
        1
    );
    assert_eq!(
        snapshots
            .iter()
            .map(|binding| binding.definition_digest.clone())
            .collect::<BTreeSet<_>>()
            .len(),
        1
    );
    assert_eq!(
        snapshots
            .iter()
            .map(|binding| binding.binding_id.clone())
            .collect::<BTreeSet<_>>()
            .len(),
        8
    );
    assert_eq!(
        snapshots
            .iter()
            .map(|binding| binding.instance.instance_id.clone())
            .collect::<BTreeSet<_>>()
            .len(),
        8
    );
    assert_eq!(
        snapshots
            .iter()
            .map(|binding| binding.data_lease.task_id.clone())
            .collect::<BTreeSet<_>>()
            .len(),
        8
    );
}
