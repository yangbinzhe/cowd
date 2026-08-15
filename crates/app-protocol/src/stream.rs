use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    require_bounded, require_schema, AppErrorDetailV1, ProtocolValidate, ProtocolValidationError,
    Sha256Digest,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AppStreamFrameV1 {
    Open {
        schema_version: u16,
        subscription_id: String,
        sequence: u64,
        schema_digest: Sha256Digest,
    },
    Data {
        schema_version: u16,
        subscription_id: String,
        sequence: u64,
        payload: Value,
    },
    Checkpoint {
        schema_version: u16,
        subscription_id: String,
        sequence: u64,
        cursor: String,
    },
    Error {
        schema_version: u16,
        subscription_id: String,
        sequence: u64,
        error: AppErrorDetailV1,
    },
    End {
        schema_version: u16,
        subscription_id: String,
        sequence: u64,
        reason: StreamEndReasonV1,
    },
}

impl AppStreamFrameV1 {
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        match self {
            Self::Open { sequence, .. }
            | Self::Data { sequence, .. }
            | Self::Checkpoint { sequence, .. }
            | Self::Error { sequence, .. }
            | Self::End { sequence, .. } => *sequence,
        }
    }

    #[must_use]
    pub fn subscription_id(&self) -> &str {
        match self {
            Self::Open {
                subscription_id, ..
            }
            | Self::Data {
                subscription_id, ..
            }
            | Self::Checkpoint {
                subscription_id, ..
            }
            | Self::Error {
                subscription_id, ..
            }
            | Self::End {
                subscription_id, ..
            } => subscription_id,
        }
    }
}

impl ProtocolValidate for AppStreamFrameV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        let schema_version = match self {
            Self::Open { schema_version, .. }
            | Self::Data { schema_version, .. }
            | Self::Checkpoint { schema_version, .. }
            | Self::Error { schema_version, .. }
            | Self::End { schema_version, .. } => *schema_version,
        };
        require_schema("AppStreamFrameV1", schema_version)?;
        require_bounded("subscription_id", self.subscription_id(), 256)?;
        if let Self::Open {
            sequence,
            schema_digest,
            ..
        } = self
        {
            if *sequence != 0 {
                return Err(ProtocolValidationError::InvalidField {
                    field: "stream.open.sequence",
                    reason: "must be zero".to_owned(),
                });
            }
            schema_digest.validate_value("stream.schema_digest")?;
        }
        if let Self::Checkpoint { cursor, .. } = self {
            require_bounded("stream.cursor", cursor, 4096)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamEndReasonV1 {
    Completed,
    Cancelled,
    DeadlineExceeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppStreamAckV1 {
    pub schema_version: u16,
    pub subscription_id: String,
    pub maximum_contiguous_sequence: u64,
    pub cursor: String,
}

impl ProtocolValidate for AppStreamAckV1 {
    fn validate(&self) -> Result<(), ProtocolValidationError> {
        require_schema("AppStreamAckV1", self.schema_version)?;
        require_bounded("subscription_id", &self.subscription_id, 256)?;
        require_bounded("cursor", &self.cursor, 4096)
    }
}
