use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ServiceError {
    InvalidInput(String),
    NotFound(String),
    PermissionDenied(String),
    Conflict(String),
    Unavailable(String),
    Degraded(String),
    Internal(String),
}

impl ServiceError {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::InvalidInput(_) => "invalid_input",
            Self::NotFound(_) => "not_found",
            Self::PermissionDenied(_) => "permission_denied",
            Self::Conflict(_) => "conflict",
            Self::Unavailable(_) => "unavailable",
            Self::Degraded(_) => "degraded",
            Self::Internal(_) => "internal",
        }
    }

    pub(crate) fn message(&self) -> &str {
        match self {
            Self::InvalidInput(message)
            | Self::NotFound(message)
            | Self::PermissionDenied(message)
            | Self::Conflict(message)
            | Self::Unavailable(message)
            | Self::Degraded(message)
            | Self::Internal(message) => message,
        }
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind(), self.message())
    }
}

impl std::error::Error for ServiceError {}
