use super::*;

impl MfgService {
    pub(crate) fn report_delivery_payload(
        &self,
        report: &MfgCockpitReportSnapshot,
        request: &MfgCockpitReportDeliveryRequest,
    ) -> app_mfg::MfgCockpitReportDeliveryPayload {
        app_mfg::MfgCockpitReportDeliveryPayload::from_report(
            report,
            app_mfg::MfgCockpitReportDeliveryPayloadRequest {
                channel: request.channel.clone(),
                template_id: request.template_id.clone(),
                target_ref: request
                    .target_ref
                    .clone()
                    .or_else(|| report.delivery_ref.clone()),
                requested_capability: request.requested_capability.clone(),
            },
        )
    }

    pub(crate) fn report_delivery_action(
        &self,
        report: &MfgCockpitReportSnapshot,
        request: &MfgCockpitReportDeliveryRequest,
        delivery_payload: &app_mfg::MfgCockpitReportDeliveryPayload,
    ) -> CrossPlaneAction {
        let actor_principal = request
            .actor_principal
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(report.owner_ref.as_str());
        let requested_capability = request
            .requested_capability
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(delivery_payload.requested_capability.as_str());
        let mut action = CrossPlaneAction::new(actor_principal, requested_capability);
        action.actor_identity_ref = request.actor_identity_ref.clone();
        action.source_channel = Some(
            request
                .source_channel
                .clone()
                .unwrap_or_else(|| "mfg.report".to_string()),
        );
        action.session_id = Some(report.report_id.clone());
        action.provider_account = request.provider_account.clone();
        action.target_ref = request
            .target_ref
            .clone()
            .or_else(|| delivery_payload.target_ref.clone())
            .or_else(|| report.delivery_ref.clone());
        action.resource_ref = request
            .resource_ref
            .clone()
            .or_else(|| Some(delivery_payload.resource_ref.clone()));
        action.risk = CrossPlaneRisk::Low;
        action.data_classification = DataClassification::Internal;
        action.identity_trust = IdentityTrust::Unknown;
        action
    }

    pub(crate) fn report_delivery_receipt_matches(
        &self,
        receipt: &CrossPlaneExecutionReceipt,
        report: &MfgCockpitReportSnapshot,
    ) -> bool {
        receipt.action.session_id.as_deref() == Some(report.report_id.as_str())
    }

    pub(crate) fn attach_report_delivery_receipt(
        &self,
        config_home: impl AsRef<Path>,
        report: &MfgCockpitReportSnapshot,
        receipt: &CrossPlaneExecutionReceipt,
    ) -> Result<MfgCockpitReportSnapshot, MfgRepositoryError> {
        self.attach_cockpit_report_delivery(
            config_home,
            &report.report_id,
            MfgCockpitReportDeliveryReceipt::new(
                report.report_id.clone(),
                receipt.id.clone(),
                receipt.status.clone(),
                receipt.dispatch_status.clone(),
                receipt.audit_record_id.clone(),
            ),
        )
    }
}
