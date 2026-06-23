use serde::Deserialize;

use super::{service_envelope, MissionService, ServiceEnvelope};

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct StartMissionSessionHttpRequest {
    pub(crate) title: String,
    #[serde(default)]
    pub(crate) session_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AttachMissionTeamHttpRequest {
    pub(crate) team_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AttachMissionAgentHttpRequest {
    pub(crate) agent_id: String,
}

impl MissionService {
    pub(crate) fn new() -> Self {
        Self {
            label: "mission",
            owner: "0.9.368 Mission Runtime service boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }

    pub(crate) fn projection_contract(&self) -> ServiceEnvelope {
        self.envelope("projection")
    }

    pub(crate) fn session_control_contract(&self) -> ServiceEnvelope {
        self.envelope("session_control")
    }

    pub(crate) fn approval_projection_contract(&self) -> ServiceEnvelope {
        self.envelope("approval_projection")
    }

    pub(crate) fn relation_projection_contract(&self) -> ServiceEnvelope {
        self.envelope("relation_projection")
    }

    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.projection_contract(),
            self.session_control_contract(),
            self.approval_projection_contract(),
            self.relation_projection_contract(),
        ]
    }

    pub(crate) fn projection(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.projection_contract(),
            "mission": runtime::global_mission_runtime().projection(),
        })
    }

    pub(crate) fn approvals(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.approval_projection_contract(),
            "approvals": runtime::global_approval_queue().projection(),
        })
    }

    pub(crate) fn relations(&self) -> serde_json::Value {
        serde_json::json!({
            "envelope": self.relation_projection_contract(),
            "relations": runtime::global_session_relation_graph().projection(),
        })
    }

    pub(crate) fn start_session(
        &self,
        request: StartMissionSessionHttpRequest,
    ) -> Result<serde_json::Value, String> {
        let session = runtime::global_mission_runtime().start_session(
            runtime::StartMissionSessionRequest {
                title: request.title,
                session_id: request.session_id,
            },
        )?;
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "session": session,
            "mission": runtime::global_mission_runtime().projection(),
        }))
    }

    pub(crate) fn switch_session(&self, session_id: &str) -> Result<serde_json::Value, String> {
        self.command_value(runtime::global_mission_runtime().switch_session(session_id)?)
    }

    pub(crate) fn background_session(&self, session_id: &str) -> Result<serde_json::Value, String> {
        self.command_value(runtime::global_mission_runtime().background_session(session_id)?)
    }

    pub(crate) fn pause_session(&self, session_id: &str) -> Result<serde_json::Value, String> {
        self.command_value(runtime::global_mission_runtime().pause_session(session_id)?)
    }

    pub(crate) fn close_session(&self, session_id: &str) -> Result<serde_json::Value, String> {
        self.command_value(runtime::global_mission_runtime().close_session(session_id)?)
    }

    pub(crate) fn attach_team(
        &self,
        session_id: &str,
        request: AttachMissionTeamHttpRequest,
    ) -> Result<serde_json::Value, String> {
        self.command_value(
            runtime::global_mission_runtime().attach_team(session_id, request.team_id)?,
        )
    }

    pub(crate) fn attach_agent(
        &self,
        session_id: &str,
        request: AttachMissionAgentHttpRequest,
    ) -> Result<serde_json::Value, String> {
        self.command_value(
            runtime::global_mission_runtime().attach_agent(session_id, request.agent_id)?,
        )
    }

    fn command_value(
        &self,
        receipt: runtime::MissionCommandReceipt,
    ) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "envelope": self.session_control_contract(),
            "ok": true,
            "receipt": receipt,
            "mission": runtime::global_mission_runtime().projection(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mission_service_projects_runtime_control_surfaces() {
        let service = MissionService::new();
        let session_id = format!("mission-service-test-{}", uuid::Uuid::new_v4());
        let started = service
            .start_session(StartMissionSessionHttpRequest {
                title: "verify mission service".to_string(),
                session_id: Some(session_id.clone()),
            })
            .expect("start session");

        assert_eq!(started["ok"], true);
        assert_eq!(started["envelope"]["service"], "mission");
        assert_eq!(
            started["mission"]["active_session_id"].as_str(),
            Some(session_id.as_str())
        );

        let background = service
            .background_session(&session_id)
            .expect("background session");
        assert_eq!(background["receipt"]["status"], "accepted");
        assert_eq!(service.projection()["mission"]["kind"], "mission.runtime");
        assert_eq!(
            service.approvals()["approvals"]["kind"],
            "runtime.global_approvals"
        );
        assert_eq!(
            service.relations()["relations"]["kind"],
            "runtime.session_relations"
        );
    }
}
