//! Durable, versioned Team Template contracts.
//!
//! `TeamTemplateDefinitionId` is intentionally separate from the legacy
//! executable template kinds. Runtime resolves only versioned template
//! revisions through the Team Definition registry.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::agent::definition::{
    validate_digest, validate_qualified_id, validate_reference, validate_revision,
};
use crate::agent::{
    AgentCapability, AgentDefinitionId, DefinitionScope, RevisionLifecycle, RevisionSelector,
    ValidationError,
};

use super::binding::RoleBehaviorFacet;
use crate::evaluation::EvaluationContract;

/// A scope-qualified durable Team Template identifier, for example
/// `workspace/cowd/implementation-review`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TeamTemplateDefinitionId(String);

impl TeamTemplateDefinitionId {
    pub fn new(scope: DefinitionScope, local_id: impl AsRef<str>) -> Result<Self, ValidationError> {
        Self::try_from(format!("{}/{}", scope.as_str(), local_id.as_ref()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    #[allow(
        clippy::unreachable,
        reason = "construction validates the qualified scope prefix before storing the opaque id"
    )]
    pub fn scope(&self) -> DefinitionScope {
        match self.0.split('/').next() {
            Some("builtin") => DefinitionScope::Builtin,
            Some("user") => DefinitionScope::User,
            Some("workspace") => DefinitionScope::Workspace,
            _ => unreachable!("validated TeamTemplateDefinitionId has a valid scope"),
        }
    }
}

impl TryFrom<String> for TeamTemplateDefinitionId {
    type Error = ValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_qualified_id("team_template_definition_id", &value)?;
        Ok(Self(value))
    }
}

impl TryFrom<&str> for TeamTemplateDefinitionId {
    type Error = ValidationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TeamTemplateRevisionRef {
    pub template_id: TeamTemplateDefinitionId,
    pub revision: u64,
}

impl TeamTemplateRevisionRef {
    pub fn new(
        template_id: TeamTemplateDefinitionId,
        revision: u64,
    ) -> Result<Self, ValidationError> {
        validate_revision("revision", revision)?;
        Ok(Self {
            template_id,
            revision,
        })
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_revision("revision", self.revision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RoleCardinalityPolicy {
    Fixed {
        count: u16,
    },
    Range {
        min: u16,
        max: u16,
    },
    /// Runtime may choose a count within this envelope from the validated
    /// focus plan and currently available resource capacity.  `target` is
    /// the preferred count when no stronger request or resource constraint
    /// applies; it is not a browser or protocol-level fanout default.
    Adaptive {
        min: u16,
        target: u16,
        max: u16,
    },
}

impl RoleCardinalityPolicy {
    pub fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Self::Fixed { count } if *count > 0 => Ok(()),
            Self::Range { min, max } if *min > 0 && min <= max => Ok(()),
            Self::Adaptive { min, target, max } if *min > 0 && min <= target && target <= max => {
                Ok(())
            }
            _ => Err(ValidationError::InvalidContract {
                message:
                    "role cardinality must have a positive minimum no greater than its maximum"
                        .to_string(),
            }),
        }
    }

    #[must_use]
    pub const fn min(&self) -> u16 {
        match self {
            Self::Fixed { count } => *count,
            Self::Range { min, .. } => *min,
            Self::Adaptive { min, .. } => *min,
        }
    }

    #[must_use]
    pub const fn max(&self) -> u16 {
        match self {
            Self::Fixed { count } => *count,
            Self::Range { max, .. } => *max,
            Self::Adaptive { max, .. } => *max,
        }
    }

    /// Preferred cardinality before a concrete focus plan or Runtime
    /// resource ceiling narrows the admissible envelope.
    #[must_use]
    pub const fn preferred(&self) -> u16 {
        match self {
            Self::Fixed { count } => *count,
            Self::Range { max, .. } => *max,
            Self::Adaptive { target, .. } => *target,
        }
    }

    #[must_use]
    pub const fn permits(&self, count: u16) -> bool {
        count >= self.min() && count <= self.max()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RolePartitionPolicy {
    Single,
    ByFocus { partition_key: String },
    Explicit { partitions: Vec<String> },
}

impl RolePartitionPolicy {
    fn validate(&self, cardinality: &RoleCardinalityPolicy) -> Result<(), ValidationError> {
        match self {
            Self::Single if cardinality.max() == 1 => Ok(()),
            Self::Single => Err(ValidationError::InvalidContract {
                message: "single partition policy requires role cardinality to be exactly one"
                    .to_string(),
            }),
            Self::ByFocus { partition_key } => {
                validate_reference("partition.partition_key", partition_key)?;
                if cardinality.max() == 1 {
                    return Err(ValidationError::InvalidContract {
                        message: "focus partition policy requires capacity for more than one role instance"
                            .to_string(),
                    });
                }
                Ok(())
            }
            Self::Explicit { partitions } => {
                if partitions.is_empty() {
                    return Err(ValidationError::MissingField {
                        field: "partition.partitions".to_string(),
                    });
                }
                let mut unique = BTreeSet::new();
                for partition in partitions {
                    validate_reference("partition.partitions", partition)?;
                    if !unique.insert(partition) {
                        return Err(ValidationError::DuplicateValue {
                            field: "partition.partitions".to_string(),
                            value: partition.clone(),
                        });
                    }
                }
                let count = u16::try_from(partitions.len()).map_err(|_| {
                    ValidationError::InvalidContract {
                        message: "partition count exceeds u16 role cardinality capacity"
                            .to_string(),
                    }
                })?;
                if !cardinality.permits(count) {
                    return Err(ValidationError::InvalidContract {
                        message: "explicit partition count is outside the role cardinality range"
                            .to_string(),
                    });
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRoleTaskContract {
    pub contract_ref: String,
    pub acceptance: Vec<String>,
}

impl TeamRoleTaskContract {
    fn validate(&self) -> Result<(), ValidationError> {
        validate_reference("role.task_contract.contract_ref", &self.contract_ref)?;
        if self.acceptance.is_empty() {
            return Err(ValidationError::MissingField {
                field: "role.task_contract.acceptance".to_string(),
            });
        }
        validate_unique_non_empty("role.task_contract.acceptance", &self.acceptance)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRoleDefinition {
    pub role_id: String,
    /// Human-facing display name for this role (e.g. "供应链专家").
    /// Display-only: never participates in behavior, permissions or acceptance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub responsibility: String,
    pub agent_definition_id: AgentDefinitionId,
    pub agent_selector: RevisionSelector,
    pub cardinality: RoleCardinalityPolicy,
    pub partition: RolePartitionPolicy,
    /// Immutable, typed execution behavior declared by the Template author.
    ///
    /// Runtime freezes these facets into `TeamBindingSnapshot` and never
    /// derives them from a role id, localized display string, graph position,
    /// result field name, or mutable template default.  A published role must
    /// make its behavior explicit so custom Teams remain flexible without
    /// hidden runtime heuristics.
    pub behavior: Vec<RoleBehaviorFacet>,
    pub grant_ceiling: Vec<AgentCapability>,
    pub task_contract: TeamRoleTaskContract,
}

/// One role's human-facing display name, declared by a template author or
/// proposed by the model. Never used for behavior decisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleDisplayName {
    pub role_id: String,
    pub display_name: String,
}

/// Optional human-facing display metadata for a Team Template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamTemplateDisplay {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_display_name: Option<String>,
    #[serde(default)]
    pub role_display_names: Vec<RoleDisplayName>,
}

impl TeamRoleDefinition {
    fn validate(&self) -> Result<(), ValidationError> {
        validate_role_id(&self.role_id)?;
        validate_reference("role.responsibility", &self.responsibility)?;
        self.agent_selector.validate()?;
        if !matches!(
            self.agent_selector,
            RevisionSelector::ExactApprovedRevision { .. }
        ) {
            return Err(ValidationError::InvalidContract {
                message: "team role agent selector must pin an exact approved agent revision"
                    .to_string(),
            });
        }
        self.cardinality.validate()?;
        self.partition.validate(&self.cardinality)?;
        if self.behavior.is_empty() {
            return Err(ValidationError::MissingField {
                field: "role.behavior".to_string(),
            });
        }
        let mut behavior_kinds = BTreeSet::new();
        for facet in &self.behavior {
            facet
                .validate()
                .map_err(|message| ValidationError::InvalidContract {
                    message: format!("role.behavior: {message}"),
                })?;
            if !behavior_kinds.insert(facet.kind_key()) {
                return Err(ValidationError::DuplicateValue {
                    field: "role.behavior".to_string(),
                    value: facet.kind_key().to_string(),
                });
            }
        }
        if self.grant_ceiling.is_empty() {
            return Err(ValidationError::MissingField {
                field: "role.grant_ceiling".to_string(),
            });
        }
        validate_unique_capabilities("role.grant_ceiling", &self.grant_ceiling)?;
        self.task_contract.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamRoleDependency {
    pub from_role_id: String,
    pub to_role_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamTopologyContract {
    pub protocol_ref: String,
    pub require_synthesis: bool,
    pub require_review: bool,
}

impl TeamTopologyContract {
    fn validate(&self) -> Result<(), ValidationError> {
        validate_reference("topology.protocol_ref", &self.protocol_ref)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamResultContract {
    pub required_fields: Vec<String>,
    pub evidence_required: bool,
    pub synthesis_required: bool,
}

impl TeamResultContract {
    fn validate(&self) -> Result<(), ValidationError> {
        if self.required_fields.is_empty() {
            return Err(ValidationError::MissingField {
                field: "result_contract.required_fields".to_string(),
            });
        }
        validate_unique_non_empty("result_contract.required_fields", &self.required_fields)?;
        if self.evidence_required && !self.required_fields.iter().any(|field| field == "evidence") {
            return Err(ValidationError::InvalidContract {
                message: "evidence-required team result contract must include the evidence field"
                    .to_string(),
            });
        }
        if self.synthesis_required && !self.required_fields.iter().any(|field| field == "summary") {
            return Err(ValidationError::InvalidContract {
                message: "synthesis-required team result contract must include the summary field"
                    .to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamTemplateManifest {
    pub api_version: String,
    pub template_id: TeamTemplateDefinitionId,
    pub revision: u64,
    pub name: String,
    /// Optional display-only metadata (team name + per-role readable names).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display: Option<TeamTemplateDisplay>,
    pub lifecycle: RevisionLifecycle,
    pub topology: TeamTopologyContract,
    pub roles: Vec<TeamRoleDefinition>,
    /// Optional template-published semantic aliases for model proposals.
    /// Runtime never owns a global role-name synonym table: an alias is valid
    /// only when this immutable template revision explicitly declares it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub role_aliases: BTreeMap<String, String>,
    #[serde(default)]
    pub dependencies: Vec<TeamRoleDependency>,
    pub result_contract: TeamResultContract,
    /// Team evaluation uses the same immutable metric language as an Agent,
    /// while its scenarios cover topology, handoff and synthesis behaviour.
    pub evaluation: EvaluationContract,
    pub instructions_digest: String,
}

impl TeamTemplateManifest {
    #[must_use]
    pub fn revision_ref(&self) -> TeamTemplateRevisionRef {
        TeamTemplateRevisionRef {
            template_id: self.template_id.clone(),
            revision: self.revision,
        }
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.api_version != "cowd.team/v1" {
            return Err(ValidationError::InvalidContract {
                message: "team template api_version must be cowd.team/v1".to_string(),
            });
        }
        validate_revision("revision", self.revision)?;
        validate_reference("name", &self.name)?;
        self.topology.validate()?;
        if self.roles.is_empty() {
            return Err(ValidationError::MissingField {
                field: "roles".to_string(),
            });
        }
        let mut role_ids = BTreeSet::new();
        for role in &self.roles {
            role.validate()?;
            if !role_ids.insert(role.role_id.as_str()) {
                return Err(ValidationError::DuplicateValue {
                    field: "roles.role_id".to_string(),
                    value: role.role_id.clone(),
                });
            }
        }
        for (alias, role_id) in &self.role_aliases {
            validate_role_id(alias)?;
            if !role_ids.contains(role_id.as_str()) {
                return Err(ValidationError::InvalidContract {
                    message: format!(
                        "role alias `{alias}` points to unknown template role `{role_id}`"
                    ),
                });
            }
        }
        validate_dependencies(&role_ids, &self.dependencies)?;
        self.result_contract.validate()?;
        self.evaluation.validate()?;
        validate_digest("instructions_digest", &self.instructions_digest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamTemplateRevision {
    pub revision_ref: TeamTemplateRevisionRef,
    pub manifest: TeamTemplateManifest,
    pub content_digest: String,
}

impl TeamTemplateRevision {
    pub fn validate(&self) -> Result<(), ValidationError> {
        self.revision_ref.validate()?;
        self.manifest.validate()?;
        if self.revision_ref != self.manifest.revision_ref() {
            return Err(ValidationError::InvalidContract {
                message: "revision reference must match manifest template_id and revision"
                    .to_string(),
            });
        }
        validate_digest("content_digest", &self.content_digest)
    }
}

fn validate_role_id(value: &str) -> Result<(), ValidationError> {
    if value.is_empty()
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
        })
    {
        return Err(ValidationError::InvalidIdentifier {
            field: "role.role_id".to_string(),
            value: value.to_string(),
            reason: "must use lowercase ascii letters, digits, hyphens, or underscores".to_string(),
        });
    }
    Ok(())
}

fn validate_dependencies(
    role_ids: &BTreeSet<&str>,
    dependencies: &[TeamRoleDependency],
) -> Result<(), ValidationError> {
    let mut outgoing = BTreeMap::<&str, Vec<&str>>::new();
    let mut indegree = role_ids
        .iter()
        .map(|role_id| (*role_id, 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();

    for dependency in dependencies {
        if !role_ids.contains(dependency.from_role_id.as_str()) {
            return Err(ValidationError::InvalidReference {
                field: "dependencies.from_role_id".to_string(),
                value: dependency.from_role_id.clone(),
                reason: "does not name a declared role".to_string(),
            });
        }
        if !role_ids.contains(dependency.to_role_id.as_str()) {
            return Err(ValidationError::InvalidReference {
                field: "dependencies.to_role_id".to_string(),
                value: dependency.to_role_id.clone(),
                reason: "does not name a declared role".to_string(),
            });
        }
        if dependency.from_role_id == dependency.to_role_id {
            return Err(ValidationError::InvalidContract {
                message: "team role dependency cannot reference the same role on both ends"
                    .to_string(),
            });
        }
        if !seen.insert((&dependency.from_role_id, &dependency.to_role_id)) {
            return Err(ValidationError::DuplicateValue {
                field: "dependencies".to_string(),
                value: format!("{}->{}", dependency.from_role_id, dependency.to_role_id),
            });
        }
        outgoing
            .entry(dependency.from_role_id.as_str())
            .or_default()
            .push(dependency.to_role_id.as_str());
        let target = indegree
            .get_mut(dependency.to_role_id.as_str())
            .ok_or_else(|| ValidationError::InvalidContract {
                message: "team dependency target disappeared during validation".to_string(),
            })?;
        *target += 1;
    }

    let mut frontier = indegree
        .iter()
        .filter_map(|(role_id, count)| (*count == 0).then_some(*role_id))
        .collect::<Vec<_>>();
    let mut visited = 0usize;
    while let Some(role_id) = frontier.pop() {
        visited += 1;
        for target in outgoing.get(role_id).into_iter().flatten() {
            let count =
                indegree
                    .get_mut(target)
                    .ok_or_else(|| ValidationError::InvalidContract {
                        message: "team dependency edge points to an undeclared role".to_string(),
                    })?;
            *count -= 1;
            if *count == 0 {
                frontier.push(target);
            }
        }
    }
    if visited != role_ids.len() {
        return Err(ValidationError::InvalidContract {
            message: "team template role dependencies contain a cycle".to_string(),
        });
    }
    Ok(())
}

fn validate_unique_non_empty(field: &str, values: &[String]) -> Result<(), ValidationError> {
    let mut seen = BTreeSet::new();
    for value in values {
        validate_reference(field, value)?;
        if !seen.insert(value) {
            return Err(ValidationError::DuplicateValue {
                field: field.to_string(),
                value: value.clone(),
            });
        }
    }
    Ok(())
}

fn validate_unique_capabilities(
    field: &str,
    capabilities: &[AgentCapability],
) -> Result<(), ValidationError> {
    let mut seen = BTreeSet::new();
    for capability in capabilities {
        if !seen.insert(*capability as u8) {
            return Err(ValidationError::DuplicateValue {
                field: field.to_string(),
                value: format!("{capability:?}"),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn role(role_id: &str) -> TeamRoleDefinition {
        TeamRoleDefinition {
            role_id: role_id.to_string(),
            display_name: None,
            responsibility: format!("{role_id} responsibility"),
            agent_definition_id: AgentDefinitionId::try_from("workspace/cowd/reviewer").unwrap(),
            agent_selector: RevisionSelector::ExactApprovedRevision { revision: 2 },
            cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
            partition: RolePartitionPolicy::Single,
            behavior: vec![RoleBehaviorFacet::TerminalCandidate { required: true }],
            grant_ceiling: vec![AgentCapability::Read, AgentCapability::Search],
            task_contract: TeamRoleTaskContract {
                contract_ref: format!("task/{role_id}"),
                acceptance: vec!["evidence-backed output".to_string()],
            },
        }
    }

    fn manifest() -> TeamTemplateManifest {
        TeamTemplateManifest {
            api_version: "cowd.team/v1".to_string(),
            template_id: TeamTemplateDefinitionId::try_from("workspace/cowd/implementation-review")
                .unwrap(),
            revision: 1,
            name: "Implementation Review".to_string(),
            display: None,
            lifecycle: RevisionLifecycle::Published,
            topology: TeamTopologyContract {
                protocol_ref: "review_fix@1".to_string(),
                require_synthesis: true,
                require_review: true,
            },
            roles: vec![role("implementer"), role("reviewer")],
            role_aliases: BTreeMap::new(),
            dependencies: vec![TeamRoleDependency {
                from_role_id: "implementer".to_string(),
                to_role_id: "reviewer".to_string(),
            }],
            result_contract: TeamResultContract {
                required_fields: vec!["summary".to_string(), "evidence".to_string()],
                evidence_required: true,
                synthesis_required: true,
            },
            evaluation: EvaluationContract::single_release_gate(
                "team/implementation-review",
                "team_interoperability",
            ),
            instructions_digest: digest('a'),
        }
    }

    #[test]
    fn durable_team_template_revision_validates_with_qualified_revision_identity() {
        let manifest = manifest();
        let revision = TeamTemplateRevision {
            revision_ref: manifest.revision_ref(),
            manifest,
            content_digest: digest('b'),
        };
        revision.validate().unwrap();
        assert_eq!(
            revision.revision_ref.template_id.as_str(),
            "workspace/cowd/implementation-review"
        );
    }

    #[test]
    fn role_behavior_is_required_and_cannot_be_implicitly_derived() {
        let mut manifest = manifest();
        manifest.roles[0].behavior.clear();
        let error = manifest
            .validate()
            .expect_err("published Team role must declare its own behavior");
        assert!(matches!(
            error,
            ValidationError::MissingField { field } if field == "role.behavior"
        ));
    }

    #[test]
    fn team_roles_require_exact_agent_revision_pins() {
        let mut invalid = manifest();
        invalid.roles[0].agent_selector = RevisionSelector::LatestApprovedStable;
        assert!(matches!(
            invalid.validate(),
            Err(ValidationError::InvalidContract { .. })
        ));
    }

    #[test]
    fn explicit_partitions_must_fit_role_cardinality() {
        let mut invalid = role("researcher");
        invalid.cardinality = RoleCardinalityPolicy::Fixed { count: 2 };
        invalid.partition = RolePartitionPolicy::Explicit {
            partitions: vec![
                "security".to_string(),
                "performance".to_string(),
                "ux".to_string(),
            ],
        };
        assert!(matches!(
            invalid.validate(),
            Err(ValidationError::InvalidContract { .. })
        ));
    }

    #[test]
    fn dependency_cycles_are_rejected() {
        let mut invalid = manifest();
        invalid.dependencies.push(TeamRoleDependency {
            from_role_id: "reviewer".to_string(),
            to_role_id: "implementer".to_string(),
        });
        assert!(matches!(
            invalid.validate(),
            Err(ValidationError::InvalidContract { .. })
        ));
    }
}
