//! Extraction error types.

use thiserror::Error;

/// Errors from PDF text extraction or the LLM extraction call.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The PDF parsed but produced no extractable text (likely a scan).
    #[error("PDF has no extractable text layer; OCR is unsupported")]
    NoTextLayer,

    /// The bytes could not be parsed as a PDF.
    #[error("failed to parse PDF: {reason}")]
    Pdf {
        /// Human-readable parse failure description.
        reason: String,
    },

    /// The extraction API request failed (network, auth, refusal, ...).
    #[error("extraction API request failed: {reason}")]
    Api {
        /// Human-readable failure description.
        reason: String,
    },

    /// The extraction API responded, but the payload was not usable.
    #[error("extraction response invalid: {reason}")]
    InvalidResponse {
        /// Human-readable description of the malformation.
        reason: String,
    },
}
