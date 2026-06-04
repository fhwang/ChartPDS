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
