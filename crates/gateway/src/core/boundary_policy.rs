use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GatewayResponsibility {
    RuntimeHost,
    HttpApi,
    SseStream,
    WebuiStaticAssets,
    HealthProjection,
    CommandProjection,
    McpHostedAdapter,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayBoundaryPolicy {
    pub owns: Vec<GatewayResponsibility>,
    pub forbidden_route_dependencies: Vec<String>,
}

impl GatewayBoundaryPolicy {
    #[must_use]
    pub fn runtime_entrypoint() -> Self {
        Self {
            owns: vec![
                GatewayResponsibility::RuntimeHost,
                GatewayResponsibility::HttpApi,
                GatewayResponsibility::SseStream,
                GatewayResponsibility::WebuiStaticAssets,
                GatewayResponsibility::HealthProjection,
                GatewayResponsibility::CommandProjection,
                GatewayResponsibility::McpHostedAdapter,
            ],
            forbidden_route_dependencies: [
                "SessionKernel",
                "UnifiedSessionStore",
                "SmartApprovalGate",
                "ContextRuntimeKernel",
                "MatrixStore::open",
                "MfgStore::open",
                "rusqlite",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_policy_declares_single_runtime_entrypoint() {
        let policy = GatewayBoundaryPolicy::runtime_entrypoint();
        assert!(policy.owns.contains(&GatewayResponsibility::RuntimeHost));
        assert!(policy.owns.contains(&GatewayResponsibility::HttpApi));
        assert!(policy
            .forbidden_route_dependencies
            .contains(&"SessionKernel".to_string()));
    }
}
