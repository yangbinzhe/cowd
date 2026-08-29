//! Neutral capability ownership metadata shared by architecture participants.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleRole {
    Authority,
    Coordinator,
    Worker,
    Projector,
    Adapter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityScope {
    Local,
    ExternalPort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriterKind {
    Canonical,
    Coordinating,
    Effect,
    Projection,
    ReadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CapabilityRoleBinding {
    pub capability_id: &'static str,
    pub state_authority_id: &'static str,
    pub authority_scope: AuthorityScope,
    pub role: LifecycleRole,
    pub writer_kind: WriterKind,
    pub consumer_ids: &'static [&'static str],
}

impl CapabilityRoleBinding {
    #[must_use]
    pub const fn local(
        capability_id: &'static str,
        state_authority_id: &'static str,
        role: LifecycleRole,
        writer_kind: WriterKind,
    ) -> Self {
        Self {
            capability_id,
            state_authority_id,
            authority_scope: AuthorityScope::Local,
            role,
            writer_kind,
            consumer_ids: &[],
        }
    }

    #[must_use]
    pub const fn external(
        capability_id: &'static str,
        state_authority_id: &'static str,
        role: LifecycleRole,
    ) -> Self {
        Self {
            capability_id,
            state_authority_id,
            authority_scope: AuthorityScope::ExternalPort,
            role,
            writer_kind: WriterKind::ReadOnly,
            consumer_ids: &[],
        }
    }

    pub fn validate(self) -> Result<(), &'static str> {
        if self.capability_id.is_empty() || self.state_authority_id.is_empty() {
            return Err("capability and state authority IDs must be non-empty");
        }
        if self.authority_scope == AuthorityScope::ExternalPort
            && self.role == LifecycleRole::Authority
        {
            return Err("an external port cannot claim local authority");
        }
        if self.role == LifecycleRole::Authority && self.writer_kind != WriterKind::Canonical {
            return Err("Authority must be the canonical writer");
        }
        if self.role != LifecycleRole::Authority && self.writer_kind == WriterKind::Canonical {
            return Err("only Authority may be a canonical writer");
        }
        if matches!(self.role, LifecycleRole::Projector | LifecycleRole::Adapter)
            && !matches!(
                self.writer_kind,
                WriterKind::Projection | WriterKind::ReadOnly
            )
        {
            return Err("Projector and Adapter cannot mutate canonical state");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_authority_and_canonical_worker_fail_closed() {
        let external = CapabilityRoleBinding {
            role: LifecycleRole::Authority,
            ..CapabilityRoleBinding::external(
                "surface.read",
                "surface.catalog",
                LifecycleRole::Adapter,
            )
        };
        assert!(external.validate().is_err());
        let worker = CapabilityRoleBinding::local(
            "runtime.turn.worker",
            "runtime.turn",
            LifecycleRole::Worker,
            WriterKind::Canonical,
        );
        assert!(worker.validate().is_err());
    }
}
