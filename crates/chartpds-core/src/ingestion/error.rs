//! Ingestion error types.

use thiserror::Error;

/// Errors returned by the ingestion module.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The input is not valid XML.
    #[error("invalid XML")]
    Xml(#[source] roxmltree::Error),

    /// The input parses as XML but is not a `ChartPDS`-recognised CCDA.
    #[error("not a CCDA document: {reason}")]
    NotCcda {
        /// Human-readable explanation of why the input was rejected.
        reason: String,
    },

    /// Archive operation failed.
    #[error("archive operation failed")]
    Archive(#[source] crate::archive::Error),

    /// Database operation failed.
    #[error("database operation failed")]
    Database(#[source] sqlx::Error),

    /// An adapter (Fitbit/Oura) failed to replay an archived blob during a
    /// rebuild.
    #[error("adapter replay failed")]
    Adapter(#[source] crate::sources::Error),
}

impl From<crate::sources::Error> for Error {
    fn from(err: crate::sources::Error) -> Self {
        Self::Adapter(err)
    }
}

impl From<roxmltree::Error> for Error {
    fn from(err: roxmltree::Error) -> Self {
        Self::Xml(err)
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

/// Convenience type alias used throughout the ingestion module.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_reason_for_not_ccda() {
        let err = Error::NotCcda {
            reason: "missing templateId".to_owned(),
        };
        assert!(err.to_string().contains("missing templateId"));
    }

    #[test]
    fn from_roxmltree_error_yields_xml_variant() {
        let err: Error = roxmltree::Document::parse("<broken").unwrap_err().into();
        assert!(matches!(err, Error::Xml(_)));
    }
}
