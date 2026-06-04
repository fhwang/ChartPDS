//! Document-kind enumeration.
//!
//! Each variant represents a document format `ChartPDS` knows how to ingest.
//! Per-kind parsing logic lives in [`ingestion`](super::super::ingestion);
//! this module only enumerates the kinds and exposes their metadata.

use thiserror::Error;

/// A document-kind known to `ChartPDS`.
///
/// Adding a new kind: extend this enum and add the corresponding parser in
/// `ingestion/`. The enum is `#[non_exhaustive]` so downstream `match` arms
/// will continue to compile (with a wildcard arm) when new kinds land.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Kind {
    /// CCDA — Consolidated Clinical Document Architecture (XML).
    Ccda,
}

impl Kind {
    /// File extension used when archiving this kind (no leading dot).
    #[must_use]
    pub fn extension(self) -> &'static str {
        match self {
            Self::Ccda => "xml",
        }
    }

    /// MIME content type used when transmitting or labelling this kind.
    #[must_use]
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Ccda => "application/xml",
        }
    }

    /// Parse a `Kind` from its canonical lowercase string form.
    ///
    /// # Errors
    ///
    /// Returns [`KindParseError`] if `s` is not a recognised kind.
    #[expect(
        clippy::should_implement_trait,
        reason = "Intentionally an inherent method rather than `std::str::FromStr` — \
                  defer the trait impl until a caller wants `\"ccda\".parse::<Kind>()`. \
                  When that day comes, `impl FromStr for Kind` then delete this attribute; \
                  the lint will then stop firing and `expect` will force the cleanup."
    )]
    pub fn from_str(s: &str) -> Result<Self, KindParseError> {
        match s {
            "ccda" => Ok(Self::Ccda),
            other => Err(KindParseError::Unknown {
                input: other.to_owned(),
            }),
        }
    }
}

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Ccda => "ccda",
        };
        f.write_str(s)
    }
}

/// Errors returned by [`Kind::from_str`].
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum KindParseError {
    /// The input is not a known kind.
    #[error("unknown kind: {input:?}")]
    Unknown {
        /// The offending input.
        input: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ccda_is_a_known_kind() {
        assert_eq!(Kind::from_str("ccda"), Ok(Kind::Ccda));
    }

    #[test]
    fn unknown_string_is_rejected() {
        assert!(Kind::from_str("docx").is_err());
        assert!(Kind::from_str("").is_err());
        assert!(Kind::from_str("CCDA").is_err()); // Case-sensitive.
    }

    #[test]
    fn kind_display_round_trips_through_from_str() {
        let k = Kind::Ccda;
        let s = format!("{k}");
        let parsed = Kind::from_str(&s).expect("round-trip");
        assert_eq!(k, parsed);
    }

    #[test]
    fn ccda_metadata_is_stable() {
        assert_eq!(Kind::Ccda.extension(), "xml");
        assert_eq!(Kind::Ccda.content_type(), "application/xml");
    }

    #[test]
    fn parse_error_displays_the_offending_input() {
        let err = Kind::from_str("nope").expect_err("invalid input");
        assert!(err.to_string().contains("nope"));
    }
}
