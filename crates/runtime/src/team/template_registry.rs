//! Versioned Team template availability and structural validation.
//!
//! The registry is a pure compiler input. It owns no mutable state, scheduler,
//! or fallback role loop: unavailable protocols fail before graph registration.

use harness_contract::team::{TeamRoleSpec, TeamTemplateAvailability, TeamTemplateId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamRoleDependency {
    pub from_role_id: String,
    pub to_role_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamTemplateSpec {
    pub template_id: TeamTemplateId,
    pub protocol_id: &'static str,
    pub version: u32,
    pub availability: TeamTemplateAvailability,
    pub requires_review: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeamTemplateValidationError {
    DirectOnly(TeamTemplateId),
    Unavailable {
        template_id: TeamTemplateId,
        available_in: &'static str,
    },
    InsufficientRoles(TeamTemplateId),
    DuplicateRole(String),
    UnknownDependencyRole(String),
    SelfDependency(String),
}

impl std::fmt::Display for TeamTemplateValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DirectOnly(id) => {
                write!(formatter, "{} must compile as a direct graph", id.as_str())
            }
            Self::Unavailable {
                template_id,
                available_in,
            } => {
                write!(
                    formatter,
                    "{} is unavailable until {available_in}",
                    template_id.as_str()
                )
            }
            Self::InsufficientRoles(id) => {
                write!(formatter, "{} requires at least two roles", id.as_str())
            }
            Self::DuplicateRole(role) => write!(formatter, "duplicate team role: {role}"),
            Self::UnknownDependencyRole(role) => {
                write!(formatter, "dependency references unknown role: {role}")
            }
            Self::SelfDependency(role) => write!(formatter, "role cannot depend on itself: {role}"),
        }
    }
}

impl std::error::Error for TeamTemplateValidationError {}

#[derive(Debug, Default)]
pub struct TeamTemplateRegistry;

impl TeamTemplateRegistry {
    #[must_use]
    pub fn spec(template_id: TeamTemplateId) -> TeamTemplateSpec {
        use TeamTemplateAvailability::{Available, Unavailable};
        let (protocol_id, availability, requires_review) = match template_id {
            TeamTemplateId::SingleExecutor => ("direct@1", Available, false),
            TeamTemplateId::ExecuteReview => ("execute_review@1", Available, true),
            TeamTemplateId::FanoutResearchSynthesis => {
                ("fanout_research_synthesis@1", Available, false)
            }
            TeamTemplateId::DebateConsensus => ("debate@1", Unavailable, true),
            TeamTemplateId::ImplementationReviewFix => ("review_fix@1", Unavailable, true),
            TeamTemplateId::IncidentResponse => ("incident@1", Unavailable, true),
            TeamTemplateId::LongRunningProject => ("mission_schedule@1", Unavailable, true),
        };
        TeamTemplateSpec {
            template_id,
            protocol_id,
            version: 1,
            availability,
            requires_review,
        }
    }

    #[must_use]
    pub fn all() -> Vec<TeamTemplateSpec> {
        vec![
            TeamTemplateId::SingleExecutor,
            TeamTemplateId::ExecuteReview,
            TeamTemplateId::FanoutResearchSynthesis,
            TeamTemplateId::DebateConsensus,
            TeamTemplateId::ImplementationReviewFix,
            TeamTemplateId::IncidentResponse,
            TeamTemplateId::LongRunningProject,
        ]
        .into_iter()
        .map(Self::spec)
        .collect()
    }

    pub fn validate(
        template_id: TeamTemplateId,
        roles: &[TeamRoleSpec],
        dependencies: &[TeamRoleDependency],
    ) -> Result<TeamTemplateSpec, TeamTemplateValidationError> {
        let spec = Self::spec(template_id);
        if template_id == TeamTemplateId::SingleExecutor {
            return Err(TeamTemplateValidationError::DirectOnly(template_id));
        }
        if spec.availability == TeamTemplateAvailability::Unavailable {
            let available_in = match template_id {
                TeamTemplateId::LongRunningProject => "V8",
                _ => "V6",
            };
            return Err(TeamTemplateValidationError::Unavailable {
                template_id,
                available_in,
            });
        }
        if roles.len() < 2 {
            return Err(TeamTemplateValidationError::InsufficientRoles(template_id));
        }
        let mut role_ids = std::collections::BTreeSet::new();
        for role in roles {
            if !role_ids.insert(role.role_id.as_str()) {
                return Err(TeamTemplateValidationError::DuplicateRole(
                    role.role_id.clone(),
                ));
            }
        }
        for dependency in dependencies {
            if dependency.from_role_id == dependency.to_role_id {
                return Err(TeamTemplateValidationError::SelfDependency(
                    dependency.from_role_id.clone(),
                ));
            }
            for role_id in [&dependency.from_role_id, &dependency.to_role_id] {
                if !role_ids.contains(role_id.as_str()) {
                    return Err(TeamTemplateValidationError::UnknownDependencyRole(
                        role_id.clone(),
                    ));
                }
            }
        }
        Ok(spec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn role(id: &str) -> TeamRoleSpec {
        TeamRoleSpec {
            role_id: id.into(),
            responsibility: id.into(),
            required_capabilities: Vec::new(),
            allowed_tools: Vec::new(),
            acceptance: Vec::new(),
            evidence_duties: Vec::new(),
        }
    }

    #[test]
    fn all_seven_templates_have_stable_availability() {
        let specs = TeamTemplateRegistry::all();
        assert_eq!(specs.len(), 7);
        assert_eq!(
            specs
                .iter()
                .filter(|spec| spec.availability == TeamTemplateAvailability::Available)
                .count(),
            3
        );
    }

    #[test]
    fn available_templates_validate_and_future_templates_fail_before_graph_creation() {
        let roles = vec![role("executor"), role("reviewer")];
        assert!(TeamTemplateRegistry::validate(
            TeamTemplateId::ExecuteReview,
            &roles,
            &[TeamRoleDependency {
                from_role_id: "executor".into(),
                to_role_id: "reviewer".into()
            }],
        )
        .is_ok());
        assert!(matches!(
            TeamTemplateRegistry::validate(TeamTemplateId::DebateConsensus, &roles, &[]),
            Err(TeamTemplateValidationError::Unavailable {
                available_in: "V6",
                ..
            })
        ));
    }
}
