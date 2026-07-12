//! The frozen extraction artifact: verified LLM output, archived as JSON.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Coding system URI for ICD-10-CM — the only system v1 extracts.
pub const ICD10_CM_SYSTEM: &str = "http://hl7.org/fhir/sid/icd-10-cm";

/// One verified coding: a code the document provably quotes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtractedCoding {
    /// Coding system URI (always [`ICD10_CM_SYSTEM`] in v1).
    pub system: String,
    /// The code exactly as written in the document.
    pub code: String,
    /// The diagnosis text the document pairs with the code.
    pub display: String,
    /// Verbatim text span containing the code; verified as a substring of
    /// the extracted document text (whitespace-normalized).
    pub quote: String,
    /// Verbatim section heading the quote appeared under, if any.
    pub section_label: Option<String>,
}

/// Identity of the extractor that produced an artifact, for auditability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtractorInfo {
    /// Claude model id (e.g. `"claude-opus-4-8"`).
    pub model: String,
    /// Version of the extraction prompt.
    pub prompt_version: u32,
}

/// The archived extraction artifact for one narrative PDF.
///
/// Contains only claims that passed mechanical verification against the
/// document text. Replayed verbatim on rebuild — never regenerated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtractionArtifact {
    /// SHA-256 hex of the PDF blob this artifact describes.
    pub document: String,
    /// The document's calendar date (ISO-8601), if verified.
    pub document_date: Option<String>,
    /// Verbatim text span supporting `document_date`.
    pub document_date_quote: Option<String>,
    /// Short human-readable label (extractor-authored, not verified).
    pub title: Option<String>,
    /// Verified codings.
    pub codings: Vec<ExtractedCoding>,
    /// Who produced this artifact.
    pub extractor: ExtractorInfo,
    /// When extraction ran (RFC 3339).
    #[serde(with = "time::serde::rfc3339")]
    pub extracted_at: OffsetDateTime,
}

/// Un-verified LLM output, as parsed from the structured-output response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RawExtraction {
    /// Claimed document date (ISO-8601), if any.
    pub document_date: Option<String>,
    /// Claimed verbatim span containing the date.
    pub document_date_quote: Option<String>,
    /// Proposed title.
    pub title: Option<String>,
    /// Proposed codings, pre-verification.
    pub codings: Vec<RawCoding>,
}

/// One un-verified proposed coding.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RawCoding {
    /// Proposed code.
    pub code: String,
    /// Proposed display text.
    pub display: String,
    /// Claimed verbatim span containing the code.
    pub quote: String,
    /// Claimed section heading.
    pub section_label: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn artifact_round_trips_through_json() {
        let a = ExtractionArtifact {
            document: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_owned(),
            document_date: Some("2026-04-21".to_owned()),
            document_date_quote: Some("Order Date: 04/21/2026".to_owned()),
            title: Some("GI Pathology Report".to_owned()),
            codings: vec![ExtractedCoding {
                system: ICD10_CM_SYSTEM.to_owned(),
                code: "R10.9".to_owned(),
                display: "Abdominal pain, unspecified".to_owned(),
                quote: "Abdominal pain, unspecified - R10.9".to_owned(),
                section_label: Some("Pre-Op Diagnosis/Indications".to_owned()),
            }],
            extractor: ExtractorInfo {
                model: "claude-opus-4-8".to_owned(),
                prompt_version: 1,
            },
            extracted_at: datetime!(2026-07-06 12:00:00 UTC),
        };
        let bytes = serde_json::to_vec(&a).expect("serialize");
        let back: ExtractionArtifact = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(a, back);
    }
}
