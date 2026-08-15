use std::collections::BTreeMap;

use cowd_app_protocol::{
    app_tui_view_patch_schema_digest_v1, AppStreamFrameV1, AppViewDocumentV1, ProtocolValidate,
    MAX_STREAM_FRAME_BYTES,
};

use super::AppViewStateError;

const MAXIMUM_SUBSCRIPTIONS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppSubscriptionStatus {
    Connecting,
    Live,
    Reconnecting,
    ResyncRequired,
    Ended,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppSubscriptionState {
    pub status: AppSubscriptionStatus,
    pub next_sequence: u64,
    pub cursor: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct AppViewStreamState {
    subscriptions: BTreeMap<String, AppSubscriptionState>,
}

impl AppViewStreamState {
    pub fn from_document(document: &AppViewDocumentV1) -> Result<Self, AppViewStateError> {
        if document.subscriptions.len() > MAXIMUM_SUBSCRIPTIONS {
            return Err(AppViewStateError::ResourceLimit("subscriptions"));
        }
        let mut subscriptions = BTreeMap::new();
        for descriptor in &document.subscriptions {
            if subscriptions
                .insert(
                    descriptor.subscription_id.clone(),
                    AppSubscriptionState {
                        status: AppSubscriptionStatus::Connecting,
                        next_sequence: 0,
                        cursor: descriptor.cursor.clone(),
                        last_error: None,
                    },
                )
                .is_some()
            {
                return Err(AppViewStateError::InvalidDocument(format!(
                    "duplicate subscription id {}",
                    descriptor.subscription_id
                )));
            }
        }
        Ok(Self { subscriptions })
    }

    #[must_use]
    pub fn subscription(&self, subscription_id: &str) -> Option<&AppSubscriptionState> {
        self.subscriptions.get(subscription_id)
    }

    pub fn reconnect(&mut self, subscription_id: &str) -> Result<(), AppViewStateError> {
        let state = self
            .subscriptions
            .get_mut(subscription_id)
            .ok_or_else(|| AppViewStateError::InvalidDocument("unknown subscription".to_owned()))?;
        state.status = AppSubscriptionStatus::Reconnecting;
        state.next_sequence = 0;
        state.last_error = None;
        Ok(())
    }

    pub fn require_resync(&mut self, subscription_id: &str) -> Result<(), AppViewStateError> {
        let state = self
            .subscriptions
            .get_mut(subscription_id)
            .ok_or_else(|| AppViewStateError::InvalidDocument("unknown subscription".to_owned()))?;
        state.status = AppSubscriptionStatus::ResyncRequired;
        Ok(())
    }

    pub fn apply_frame(&mut self, frame: &AppStreamFrameV1) -> Result<(), AppViewStateError> {
        let encoded_length = serde_json::to_vec(frame)
            .map_err(|error| AppViewStateError::InvalidDocument(error.to_string()))?
            .len();
        if u64::try_from(encoded_length).unwrap_or(u64::MAX) > MAX_STREAM_FRAME_BYTES {
            return Err(AppViewStateError::ResourceLimit("stream frame bytes"));
        }
        frame
            .validate()
            .map_err(|error| AppViewStateError::InvalidDocument(error.to_string()))?;
        let subscription_id = frame.subscription_id();
        let state = self
            .subscriptions
            .get_mut(subscription_id)
            .ok_or_else(|| AppViewStateError::InvalidDocument("unknown subscription".to_owned()))?;
        let sequence = frame.sequence();
        if matches!(frame, AppStreamFrameV1::Open { .. }) {
            let AppStreamFrameV1::Open { schema_digest, .. } = frame else {
                unreachable!();
            };
            let expected_schema = app_tui_view_patch_schema_digest_v1()
                .map_err(|error| AppViewStateError::InvalidDocument(error.to_string()))?;
            if schema_digest != &expected_schema {
                state.status = AppSubscriptionStatus::ResyncRequired;
                return Err(AppViewStateError::InvalidDocument(
                    "stream schema digest does not match the signed TUI patch contract".to_owned(),
                ));
            }
            if state.status != AppSubscriptionStatus::Connecting
                && state.status != AppSubscriptionStatus::Reconnecting
                && state.status != AppSubscriptionStatus::ResyncRequired
            {
                state.status = AppSubscriptionStatus::ResyncRequired;
                return Err(AppViewStateError::InvalidDocument(
                    "unexpected stream open frame".to_owned(),
                ));
            }
            state.status = AppSubscriptionStatus::Live;
            state.next_sequence = 1;
            state.last_error = None;
            return Ok(());
        }
        if state.status != AppSubscriptionStatus::Live || sequence != state.next_sequence {
            state.status = AppSubscriptionStatus::ResyncRequired;
            return Err(AppViewStateError::InvalidDocument(format!(
                "stream sequence mismatch: expected {}, received {sequence}",
                state.next_sequence
            )));
        }
        state.next_sequence = state.next_sequence.saturating_add(1);
        match frame {
            AppStreamFrameV1::Checkpoint { cursor, .. } => state.cursor = Some(cursor.clone()),
            AppStreamFrameV1::Error { error, .. } => {
                state.status = AppSubscriptionStatus::Error;
                state.last_error = Some(error.message.clone());
            }
            AppStreamFrameV1::End { .. } => state.status = AppSubscriptionStatus::Ended,
            AppStreamFrameV1::Data { .. } => {}
            AppStreamFrameV1::Open { .. } => {
                return Err(AppViewStateError::InvalidDocument(
                    "unexpected stream open frame".to_owned(),
                ));
            }
        }
        Ok(())
    }
}
