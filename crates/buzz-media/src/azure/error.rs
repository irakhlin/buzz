//! Azure Blob Storage error type.

use reqwest::StatusCode;

/// A failed Azure Blob Storage operation.
#[derive(Debug, thiserror::Error)]
pub enum AzureError {
    /// The service answered with a non-success status.
    #[error("azure blob storage returned {status}{}: {message}", code.as_deref().map(|c| format!(" ({c})")).unwrap_or_default())]
    Status {
        /// HTTP status code.
        status: u16,
        /// `x-ms-error-code` response header, when present.
        code: Option<String>,
        /// Bounded response-body excerpt or a short description.
        message: String,
    },
    /// The request did not complete (connect, timeout, or body transfer).
    #[error("azure blob storage transport error: {0}")]
    Transport(String),
    /// Acquiring or applying credentials failed.
    #[error("azure blob storage authorization failed: {0}")]
    Auth(String),
    /// The client was configured incorrectly.
    #[error("azure blob storage configuration error: {0}")]
    Config(String),
    /// A body exceeded the caller's size bound and was not buffered.
    #[error("azure blob is {size} bytes, over the {max}-byte limit")]
    TooLarge {
        /// Observed or declared size.
        size: u64,
        /// Caller's limit.
        max: u64,
    },
    /// A response did not match the documented shape.
    #[error("azure blob storage returned an unexpected response: {0}")]
    Parse(String),
}

impl AzureError {
    /// The HTTP status of a service error.
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Status { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// The `x-ms-error-code` of a service error.
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Status { code, .. } => code.as_deref(),
            _ => None,
        }
    }

    /// The blob, version, or container does not exist.
    pub fn is_not_found(&self) -> bool {
        self.status() == Some(StatusCode::NOT_FOUND.as_u16())
    }

    /// A conditional write lost: `If-Match` / `If-None-Match` failed (412) or
    /// a create-only write found an existing blob (409 `BlobAlreadyExists`).
    pub fn is_precondition_failed(&self) -> bool {
        match self.status() {
            Some(412) => true,
            Some(409) => self.code() == Some("BlobAlreadyExists"),
            _ => false,
        }
    }

    /// Whether a write that returned this error may still have been applied
    /// server-side: a transport failure, or a 5xx where the service may have
    /// committed before failing to answer.
    pub fn outcome_unknown(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Status { status, .. } => *status >= 500,
            _ => false,
        }
    }

    /// Whether an idempotent request may be retried.
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Status { status, .. } => matches!(status, 408 | 429 | 500 | 502 | 503 | 504),
            _ => false,
        }
    }

    pub(crate) fn transport(error: reqwest::Error) -> Self {
        Self::Transport(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(status: u16, code: Option<&str>) -> AzureError {
        AzureError::Status {
            status,
            code: code.map(str::to_owned),
            message: String::new(),
        }
    }

    #[test]
    fn classifies_conditional_write_and_unknown_outcomes() {
        assert!(status(412, Some("ConditionNotMet")).is_precondition_failed());
        assert!(status(409, Some("BlobAlreadyExists")).is_precondition_failed());
        assert!(!status(409, Some("LeaseIdMissing")).is_precondition_failed());
        assert!(status(404, Some("BlobNotFound")).is_not_found());
        assert!(status(503, None).outcome_unknown());
        assert!(AzureError::Transport("reset".into()).outcome_unknown());
        assert!(!status(403, None).outcome_unknown());
        assert!(status(429, None).is_retryable());
        assert!(!status(412, None).is_retryable());
        assert_eq!(
            status(409, Some("BlobAlreadyExists")).to_string(),
            "azure blob storage returned 409 (BlobAlreadyExists): "
        );
    }
}
