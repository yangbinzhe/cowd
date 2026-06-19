use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ServiceEnvelope {
    pub(crate) service: &'static str,
    pub(crate) operation: &'static str,
    pub(crate) status: &'static str,
    pub(crate) owner: &'static str,
    pub(crate) boundary_status: &'static str,
}

pub(crate) fn service_envelope(
    service: &'static str,
    owner: &'static str,
    operation: &'static str,
) -> ServiceEnvelope {
    ServiceEnvelope {
        service,
        operation,
        status: "service_boundary_ready",
        owner,
        boundary_status: "0618_final_boundary",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ServiceReceipt {
    pub(crate) service: &'static str,
    pub(crate) operation: &'static str,
    pub(crate) outcome: &'static str,
    pub(crate) trace_id: Option<String>,
}

impl ServiceReceipt {
    pub(crate) fn completed(
        service: &'static str,
        operation: &'static str,
        trace_id: Option<String>,
    ) -> Self {
        Self {
            service,
            operation,
            outcome: "completed",
            trace_id,
        }
    }
}
