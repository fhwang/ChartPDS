//! Error types for the sources/adapter layer.

use thiserror::Error;

/// Errors returned by source adapters.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The access token is expired or revoked; the user must re-authorize.
    #[error("re-authorization required: {reason}")]
    ReauthRequired {
        /// Human-readable explanation.
        reason: String,
    },

    /// No credentials are configured for this adapter; the user must connect
    /// it before syncing. Distinct from [`Error::ReauthRequired`], which means
    /// a previously-connected adapter's token expired.
    #[error("no credentials configured: {reason}")]
    NoCredentials {
        /// Human-readable explanation.
        reason: String,
    },

    /// A transient failure (network, rate-limit, server error). Safe to retry.
    #[error("transient error: {reason}")]
    Transient {
        /// Human-readable explanation.
        reason: String,
    },

    /// The API response couldn't be parsed or didn't match the expected shape.
    #[error("parse error: {reason}")]
    Parse {
        /// Human-readable explanation.
        reason: String,
    },

    /// Archive operation failed.
    #[error("archive operation failed")]
    Archive(#[source] crate::archive::Error),

    /// Database operation failed.
    #[error("database operation failed")]
    Database(#[source] sqlx::Error),
}

impl Error {
    /// Machine-readable reason code for structured reporting (MCP results and
    /// `source_state.last_error_reason`).
    #[must_use]
    pub fn reason_code(&self) -> &'static str {
        match self {
            Error::ReauthRequired { .. } => "reauth_required",
            Error::NoCredentials { .. } => "no_credentials",
            Error::Transient { .. } => "transient",
            Error::Parse { .. } => "parse_error",
            Error::Archive(_) => "archive_error",
            Error::Database(_) => "database_error",
        }
    }
}

impl From<crate::archive::Error> for Error {
    fn from(err: crate::archive::Error) -> Self {
        Self::Archive(err)
    }
}

impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        Self::Database(err)
    }
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reason_code_maps_each_variant() {
        assert_eq!(
            Error::ReauthRequired { reason: "x".into() }.reason_code(),
            "reauth_required"
        );
        assert_eq!(
            Error::NoCredentials { reason: "x".into() }.reason_code(),
            "no_credentials"
        );
        assert_eq!(
            Error::Transient { reason: "x".into() }.reason_code(),
            "transient"
        );
        assert_eq!(
            Error::Parse { reason: "x".into() }.reason_code(),
            "parse_error"
        );
    }
}
