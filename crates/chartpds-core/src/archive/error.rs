//! Archive error types.

use thiserror::Error;

/// Errors returned by the archive module.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The requested blob does not exist in the archive.
    #[error("blob not found: {key}")]
    NotFound {
        /// Hex key of the missing blob.
        key: String,
    },

    /// The storage backend returned an error not specific to a blob.
    #[error("storage backend error")]
    Backend(#[source] object_store::Error),
}

impl From<object_store::Error> for Error {
    fn from(err: object_store::Error) -> Self {
        Self::Backend(err)
    }
}

/// Convenience type alias used throughout the archive module.
pub type Result<T> = std::result::Result<T, Error>;
