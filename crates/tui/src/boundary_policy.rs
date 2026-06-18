use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TuiBackendAccess {
    GatewayHttp,
    GatewaySse,
    GatewayCommandProjection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TuiBoundaryPolicy {
    pub allowed_backend_access: Vec<TuiBackendAccess>,
    pub forbidden_direct_dependencies: Vec<String>,
}

impl TuiBoundaryPolicy {
    #[must_use]
    pub fn gateway_client_only() -> Self {
        Self {
            allowed_backend_access: vec![
                TuiBackendAccess::GatewayHttp,
                TuiBackendAccess::GatewaySse,
                TuiBackendAccess::GatewayCommandProjection,
            ],
            forbidden_direct_dependencies: [
                "runtime", "app_mfg", "matrix", "storage", "tools", "memory", "rusqlite",
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
    fn tui_policy_only_allows_gateway_backend_access() {
        let policy = TuiBoundaryPolicy::gateway_client_only();
        assert!(policy
            .allowed_backend_access
            .contains(&TuiBackendAccess::GatewayHttp));
        assert!(policy
            .forbidden_direct_dependencies
            .contains(&"runtime".to_string()));
        assert!(policy
            .forbidden_direct_dependencies
            .contains(&"rusqlite".to_string()));
    }
}
