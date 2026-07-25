//! Embedded, release-owned builtin Agent Definition bootstrap.
//!
//! Builtins are materialized into the explicitly selected installation scope
//! so the normal immutable Store and Resolver paths are exercised. Their
//! expected content digests stay in the executable: modifying files in the
//! builtin root cannot change a runnable builtin Definition because the
//! registry re-checks this manifest on every resolution.

use std::collections::BTreeMap;

use harness_contract::agent::{
    AgentCapability, AgentCapabilityContract, AgentCognitivePolicy, AgentDefinitionId,
    AgentDefinitionManifest, AgentDefinitionRevisionRef, AgentEvaluationContract,
    AgentExecutorPolicy, AgentModelPolicy, AgentOutputContract, CognitiveReadScope,
    CognitiveWriteMode, DefaultPointer, DefinitionScope, ReleaseAssignment,
    ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionLifecycle,
    ValidationError,
};
use sha2::{Digest, Sha256};

use super::store::{AgentDefinitionStore, DefinitionStorageLayout, DefinitionStoreError};

const RELEASE_ATTESTATION: &str = "embedded-release/cowd-runtime-v1";

#[derive(Debug, Clone, Default)]
pub(crate) struct BuiltinAgentTrust {
    digests: BTreeMap<String, String>,
}

impl BuiltinAgentTrust {
    pub(crate) fn verify(
        &self,
        revision_ref: &AgentDefinitionRevisionRef,
        content_digest: &str,
    ) -> Result<(), DefinitionStoreError> {
        let expected = self
            .digests
            .get(&revision_key(revision_ref))
            .ok_or_else(|| {
                DefinitionStoreError::UnresolvablePointer(
                    revision_ref.definition_id.clone(),
                    "builtin definition is not part of the verified release bundle".to_string(),
                )
            })?;
        if expected == content_digest {
            Ok(())
        } else {
            Err(DefinitionStoreError::DigestMismatch {
                subject: format!(
                    "builtin release digest for {} revision {}",
                    revision_ref.definition_id.as_str(),
                    revision_ref.revision
                ),
                expected: expected.clone(),
                actual: content_digest.to_string(),
            })
        }
    }
}

pub(crate) fn bootstrap_builtin_agents<L>(
    store: &AgentDefinitionStore<L>,
) -> Result<BuiltinAgentTrust, DefinitionStoreError>
where
    L: DefinitionStorageLayout,
{
    let mut trust = BuiltinAgentTrust::default();
    for builtin in builtin_agents().map_err(DefinitionStoreError::Contract)? {
        let stored = store.store_revision(builtin.manifest, builtin.instructions)?;
        store.record_release_assignment(&ReleaseAssignment {
            scope: DefinitionScope::Builtin,
            revision_ref: stored.revision.revision_ref.clone(),
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::ReleaseAuthorityAttestation {
                attestation_ref: RELEASE_ATTESTATION.to_string(),
            },
            content_digest: stored.revision.content_digest.clone(),
        })?;
        // Builtins are release-owned, but callers may still select a Definition
        // through the standard DefaultPointer contract. Materialize that pointer
        // during bootstrap so every selection path resolves the same verified
        // stable release instead of depending on an implicit fallback.
        store.set_default_pointer(&DefaultPointer::latest(
            DefinitionScope::Builtin,
            stored.revision.revision_ref.definition_id.clone(),
            ReleaseAuthorization::ReleaseAuthorityAttestation {
                attestation_ref: RELEASE_ATTESTATION.to_string(),
            },
        ))?;
        trust.digests.insert(
            revision_key(&stored.revision.revision_ref),
            stored.revision.content_digest,
        );
    }
    Ok(trust)
}

fn revision_key(revision_ref: &AgentDefinitionRevisionRef) -> String {
    format!(
        "{}@{}",
        revision_ref.definition_id.as_str(),
        revision_ref.revision
    )
}

struct BuiltinAgent {
    manifest: AgentDefinitionManifest,
    instructions: &'static str,
}

fn builtin_agents() -> Result<Vec<BuiltinAgent>, ValidationError> {
    Ok(vec![
        builtin(
            "direct",
            "Direct",
            "Answers bounded questions from supplied context without unnecessary coordination.",
            "# Direct\n\nResolve bounded questions using the supplied context. State uncertainty instead of inventing evidence.\n",
            vec![AgentCapability::Read],
            vec![CognitiveReadScope::Session],
        )?,
        builtin(
            "explore",
            "Explore",
            "Collects and compares evidence before preparing a grounded synthesis.",
            "# Explore\n\nAcquire relevant evidence, compare alternatives, and return citations with uncertainty.\n",
            vec![AgentCapability::Read, AgentCapability::Search],
            vec![
                CognitiveReadScope::Session,
                CognitiveReadScope::Project,
                CognitiveReadScope::WorkspaceKnowledge,
            ],
        )?,
        builtin(
            "execute",
            "Execute",
            "Plans, changes, verifies, and reports bounded implementation work.",
            "# Execute\n\nPlan before mutation, use the granted tools only, verify changes, and return reviewable evidence.\n",
            vec![
                AgentCapability::Read,
                AgentCapability::Search,
                AgentCapability::Write,
                AgentCapability::Test,
            ],
            vec![
                CognitiveReadScope::Session,
                CognitiveReadScope::Team,
                CognitiveReadScope::Project,
                CognitiveReadScope::DefinitionLineage,
            ],
        )?,
    ])
}

fn builtin(
    local_id: &str,
    name: &str,
    description: &str,
    instructions: &'static str,
    capabilities: Vec<AgentCapability>,
    read_scopes: Vec<CognitiveReadScope>,
) -> Result<BuiltinAgent, ValidationError> {
    let definition_id =
        AgentDefinitionId::new(DefinitionScope::Builtin, format!("cowd/{local_id}"))?;
    let instructions_digest = format!("{:x}", Sha256::digest(instructions.as_bytes()));
    Ok(BuiltinAgent {
        manifest: AgentDefinitionManifest {
            api_version: "cowd.agent/v1".to_string(),
            definition_id,
            revision: 1,
            name: name.to_string(),
            description: description.to_string(),
            lifecycle: RevisionLifecycle::Published,
            executor: AgentExecutorPolicy::CowdNative,
            model_policy: AgentModelPolicy {
                profile: "default".to_string(),
                allowed_models: Vec::new(),
                fallback_allowed: true,
            },
            cognitive_policy: AgentCognitivePolicy {
                context_profile: "default".to_string(),
                read_scopes,
                write_mode: CognitiveWriteMode::CandidateOnly,
                team_working_state_visible: true,
            },
            capability_contract: AgentCapabilityContract {
                approval_required_for: capabilities
                    .contains(&AgentCapability::Write)
                    .then_some(AgentCapability::Write)
                    .into_iter()
                    .collect(),
                capability_ceiling: capabilities,
                skill_refs: Vec::new(),
            },
            output_contract: AgentOutputContract::reviewable(),
            evaluation: AgentEvaluationContract::single_release_gate(
                format!("builtin/{local_id}/baseline"),
                "evidence",
            ),
            instructions_digest,
        },
        instructions,
    })
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::agent::definition::AgentDefinitionResolver;
    use crate::agent::definition::ScopedDefinitionLayout;
    use harness_contract::agent::RevisionSelector;

    #[test]
    fn embedded_builtin_definitions_are_runnable_and_tampering_is_rejected() {
        let temp = TempDir::new().expect("temporary root");
        let store = AgentDefinitionStore::new(ScopedDefinitionLayout::new(
            temp.path().join("builtin"),
            temp.path().join("user"),
            temp.path().join("workspace"),
        ));
        let trust = bootstrap_builtin_agents(&store).expect("bootstrap");
        let direct =
            AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/direct").expect("definition id");
        let resolved = AgentDefinitionResolver::new(&store)
            .resolve(&direct, RevisionSelector::LatestApprovedStable)
            .expect("resolved direct");
        let resolved_default = AgentDefinitionResolver::new(&store)
            .resolve(&direct, RevisionSelector::DefaultPointer)
            .expect("resolved direct default pointer");
        assert_eq!(
            resolved_default.revision.revision_ref,
            resolved.revision.revision_ref
        );
        trust
            .verify(
                &resolved.revision.revision_ref,
                &resolved.revision.content_digest,
            )
            .expect("embedded digest");

        let manifest = temp
            .path()
            .join("builtin/agents/cowd/direct/revisions/1/agent.yaml");
        std::fs::write(&manifest, "api_version: cowd.agent/v1\n").expect("tamper manifest");
        assert!(AgentDefinitionResolver::new(&store)
            .resolve(&direct, RevisionSelector::LatestApprovedStable)
            .is_err());
    }
}
