//! Journal-entry claim model and verification.
//!
//! Journal codings are *inferred*: the LLM maps colloquial prose ("my left
//! shoulder has been aching") onto an ICD-10-CM code that appears nowhere in
//! the source text. The clinical-PDF rule "code must appear in its quote"
//! is therefore structurally impossible here and is replaced by a vocabulary
//! check against the vendored ICD-10-CM table, via
//! [`canonical_icd10cm`](super::icd10::canonical_icd10cm) — which both
//! validates and rewrites the code to its canonical dotted form, so a code
//! the LLM wrote as `"m25512"` and one it wrote as `"M25.512"` persist as
//! the identical `coding_code`, keeping one observation series per code
//! rather than silently forking it. The quote-grounds-in-text rule stays
//! mandatory, and a claimed numeric severity must literally appear in the
//! grounding quote — otherwise the severity (not the coding) is dropped.
//! Index rows produced from these claims carry `derivation = 'inferred'`.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::artifact::{ExtractorInfo, ICD10_CM_SYSTEM};
use super::icd10::canonical_icd10cm;
use super::verify::{contains_anchored, date_candidates, normalize_ws};

/// Un-verified journal LLM output, as parsed from the structured response.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RawJournalExtraction {
    /// Claimed entry date (ISO-8601), if the entry text states one.
    pub entry_date: Option<String>,
    /// Claimed verbatim span containing the date.
    pub entry_date_quote: Option<String>,
    /// Proposed title.
    pub title: Option<String>,
    /// Proposed codings, pre-verification.
    pub codings: Vec<RawJournalCoding>,
}

/// One un-verified proposed journal coding.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RawJournalCoding {
    /// Proposed ICD-10-CM code (inferred — not expected in the text).
    pub code: String,
    /// Standard ICD-10-CM description for the code.
    pub display: String,
    /// Claimed verbatim span describing the complaint.
    pub quote: String,
    /// Author-stated numeric severity, only when explicitly written.
    pub severity: Option<f64>,
}

/// One verified journal coding: quote grounded, code in the vocabulary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JournalCoding {
    /// Coding system URI (always [`ICD10_CM_SYSTEM`]).
    pub system: String,
    /// The inferred ICD-10-CM code, canonicalized (uppercase, dotted form —
    /// see [`canonical_icd10cm`](super::icd10::canonical_icd10cm)) so that
    /// case- or dot-variant renderings of the same code from the LLM never
    /// split one code into multiple `(system, code)` keys in the index.
    pub code: String,
    /// Standard ICD-10-CM description.
    pub display: String,
    /// Verbatim entry span the inference was grounded in.
    pub quote: String,
    /// Author-stated severity; verified to appear in the quote.
    pub severity: Option<f64>,
}

/// Journal extraction output that survived verification.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedJournalExtraction {
    /// Verified entry date (ISO-8601), if claimed and provable.
    pub entry_date: Option<String>,
    /// The verbatim span supporting the date.
    pub entry_date_quote: Option<String>,
    /// Title (passed through unverified — presentational only).
    pub title: Option<String>,
    /// Codings that verified.
    pub codings: Vec<JournalCoding>,
    /// Codes dropped for failing the ICD-10-CM table check — surfaced
    /// separately so ingestion can feed them back to the model for one
    /// corrective retry.
    pub invalid_codes: Vec<String>,
    /// Human-readable reasons for every dropped claim.
    pub rejected: Vec<String>,
}

/// The frozen extraction artifact for one journal entry. Replayed verbatim
/// on rebuild — never regenerated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JournalExtractionArtifact {
    /// SHA-256 hex of the journal blob this artifact describes.
    pub document: String,
    /// The entry's calendar date (ISO-8601). Required: undated entries are
    /// rejected at ingest, so every artifact carries a date.
    pub entry_date: String,
    /// Short human-readable label (extractor-authored, not verified).
    pub title: Option<String>,
    /// Verified codings.
    pub codings: Vec<JournalCoding>,
    /// Who produced this artifact.
    pub extractor: ExtractorInfo,
    /// When extraction ran (RFC 3339).
    #[serde(with = "time::serde::rfc3339")]
    pub extracted_at: OffsetDateTime,
}

/// True when `severity` is literally stated in the (normalized) quote,
/// not flanked by other digits (a 6 inside "26" does not count).
fn severity_stated(norm_quote: &str, severity: f64) -> bool {
    contains_anchored(norm_quote, &format!("{severity}"))
}

/// Verify raw journal LLM output against the entry text.
///
/// Rules: quote must ground in the text (whitespace-normalized substring);
/// the code must be a real ICD-10-CM code per the vendored table (the
/// code-in-quote rule is waived — journal codes are inferred) and is stored
/// in its canonical dotted form, not verbatim as the LLM wrote it; a claimed
/// severity must appear in the quote, else the severity alone is dropped;
/// a claimed entry date is verified exactly like a clinical document date.
#[must_use]
pub fn verify_journal_extraction(
    text: &str,
    raw: RawJournalExtraction,
) -> VerifiedJournalExtraction {
    let norm_text = normalize_ws(text);
    let mut rejected = Vec::new();
    let mut invalid_codes = Vec::new();

    let mut codings = Vec::new();
    for c in raw.codings {
        let norm_quote = normalize_ws(&c.quote);
        if c.code.trim().is_empty() {
            rejected.push("coding with empty code".to_owned());
        } else if norm_quote.is_empty() {
            rejected.push(format!("coding {}: empty quote", c.code));
        } else if !norm_text.contains(&norm_quote) {
            rejected.push(format!(
                "coding {}: quote not found in entry text: {:?}",
                c.code, c.quote
            ));
        } else if let Some(canonical) = canonical_icd10cm(&c.code) {
            let severity = match c.severity {
                Some(s) if severity_stated(&norm_quote, s) => Some(s),
                Some(s) => {
                    rejected.push(format!(
                        "coding {}: severity {s} not stated in quote {:?} — kept the coding, dropped the severity",
                        c.code, c.quote
                    ));
                    None
                }
                None => None,
            };
            codings.push(JournalCoding {
                system: ICD10_CM_SYSTEM.to_owned(),
                code: canonical,
                display: c.display,
                quote: c.quote,
                severity,
            });
        } else {
            rejected.push(format!(
                "coding {}: not a valid ICD-10-CM code (vendored FY2026 table)",
                c.code
            ));
            invalid_codes.push(c.code);
        }
    }

    let (entry_date, entry_date_quote) = match (raw.entry_date, raw.entry_date_quote) {
        (Some(date), Some(quote)) => {
            let norm_quote = normalize_ws(&quote);
            match date_candidates(&date) {
                Some(cands)
                    if norm_text.contains(&norm_quote)
                        && cands.iter().any(|c| contains_anchored(&norm_quote, c)) =>
                {
                    (Some(date), Some(quote))
                }
                _ => {
                    rejected.push(format!(
                        "entry_date {date}: quote missing from text or date not in quote: {quote:?}"
                    ));
                    (None, None)
                }
            }
        }
        (Some(date), None) => {
            rejected.push(format!("entry_date {date}: no supporting quote"));
            (None, None)
        }
        (None, _) => (None, None),
    };

    VerifiedJournalExtraction {
        entry_date,
        entry_date_quote,
        title: raw.title,
        codings,
        invalid_codes,
        rejected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extraction::artifact::{ExtractorInfo, ICD10_CM_SYSTEM};

    const ENTRY: &str = "Left shoulder aching again after climbing.\n\
        Pain was maybe a 6 today. Slept badly, kept waking up.\n";

    fn raw_coding(code: &str, quote: &str, severity: Option<f64>) -> RawJournalCoding {
        RawJournalCoding {
            code: code.to_owned(),
            display: "Pain in left shoulder".to_owned(),
            quote: quote.to_owned(),
            severity,
        }
    }

    fn raw(codings: Vec<RawJournalCoding>) -> RawJournalExtraction {
        RawJournalExtraction {
            entry_date: None,
            entry_date_quote: None,
            title: Some("Journal — shoulder ache".to_owned()),
            codings,
        }
    }

    #[test]
    fn accepts_inferred_coding_whose_quote_grounds_and_code_is_real() {
        // The code M25.512 appears nowhere in ENTRY — that is the point:
        // inferred codings waive the code-in-quote rule.
        let v = verify_journal_extraction(
            ENTRY,
            raw(vec![raw_coding(
                "M25.512",
                "Left shoulder aching again after climbing. Pain was maybe a 6 today.",
                Some(6.0),
            )]),
        );
        assert_eq!(v.codings.len(), 1);
        assert_eq!(v.codings[0].system, ICD10_CM_SYSTEM);
        assert_eq!(v.codings[0].code, "M25.512");
        assert_eq!(v.codings[0].severity, Some(6.0));
        assert!(v.rejected.is_empty());
        assert!(v.invalid_codes.is_empty());
    }

    #[test]
    fn stores_the_canonical_code_when_the_llm_wrote_it_dotless_or_lowercase() {
        let v = verify_journal_extraction(
            ENTRY,
            raw(vec![raw_coding(
                "m25512",
                "Left shoulder aching again after climbing.",
                None,
            )]),
        );
        assert_eq!(v.codings.len(), 1);
        assert_eq!(
            v.codings[0].code, "M25.512",
            "dotless lowercase input canonicalizes to the dotted uppercase form"
        );
        assert!(v.rejected.is_empty());
    }

    #[test]
    fn rejects_quote_not_in_entry() {
        let v = verify_journal_extraction(
            ENTRY,
            raw(vec![raw_coding("M25.512", "My knee hurts", None)]),
        );
        assert!(v.codings.is_empty());
        assert_eq!(v.rejected.len(), 1);
        assert!(v.rejected[0].contains("quote not found"));
    }

    #[test]
    fn rejects_invalid_code_and_reports_it_for_retry_feedback() {
        let v = verify_journal_extraction(
            ENTRY,
            raw(vec![raw_coding(
                "M25.5129",
                "Left shoulder aching again after climbing.",
                None,
            )]),
        );
        assert!(v.codings.is_empty());
        assert_eq!(v.invalid_codes, vec!["M25.5129".to_owned()]);
        assert_eq!(v.rejected.len(), 1);
        assert!(v.rejected[0].contains("not a valid ICD-10-CM code"));
    }

    #[test]
    fn drops_severity_not_stated_in_quote_but_keeps_the_coding() {
        let v = verify_journal_extraction(
            ENTRY,
            raw(vec![raw_coding(
                "M25.512",
                "Left shoulder aching again after climbing.",
                Some(6.0), // "6" is in the entry but NOT in this quote
            )]),
        );
        assert_eq!(v.codings.len(), 1);
        assert_eq!(v.codings[0].severity, None, "unstated severity dropped");
        assert_eq!(v.rejected.len(), 1);
        assert!(v.rejected[0].contains("severity"));
    }

    #[test]
    fn severity_must_not_match_inside_a_longer_number() {
        let text = "Rated it 26 out of 100 on the clinic form.";
        let v = verify_journal_extraction(
            text,
            RawJournalExtraction {
                entry_date: None,
                entry_date_quote: None,
                title: None,
                codings: vec![raw_coding(
                    "R52",
                    "Rated it 26 out of 100 on the clinic form.",
                    Some(6.0),
                )],
            },
        );
        assert_eq!(v.codings[0].severity, None, "6 inside 26 must not count");
    }

    #[test]
    fn verifies_entry_date_like_document_date() {
        let text = "July 26, 2026. Shoulder felt fine.";
        let v = verify_journal_extraction(
            text,
            RawJournalExtraction {
                entry_date: Some("2026-07-26".to_owned()),
                entry_date_quote: Some("July 26, 2026".to_owned()),
                title: None,
                codings: vec![],
            },
        );
        assert_eq!(v.entry_date.as_deref(), Some("2026-07-26"));

        let bad = verify_journal_extraction(
            text,
            RawJournalExtraction {
                entry_date: Some("2026-07-27".to_owned()),
                entry_date_quote: Some("July 26, 2026".to_owned()),
                title: None,
                codings: vec![],
            },
        );
        assert_eq!(bad.entry_date, None);
        assert_eq!(bad.rejected.len(), 1);
    }

    #[test]
    fn artifact_round_trips_through_json() {
        let a = JournalExtractionArtifact {
            document: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_owned(),
            entry_date: "2026-07-26".to_owned(),
            title: Some("Journal — shoulder ache".to_owned()),
            codings: vec![JournalCoding {
                system: ICD10_CM_SYSTEM.to_owned(),
                code: "M25.512".to_owned(),
                display: "Pain in left shoulder".to_owned(),
                quote: "Left shoulder aching again".to_owned(),
                severity: Some(6.0),
            }],
            extractor: ExtractorInfo {
                model: "claude-opus-4-8".to_owned(),
                prompt_version: 1,
            },
            extracted_at: time::macros::datetime!(2026-07-26 12:00:00 UTC),
        };
        let bytes = serde_json::to_vec(&a).expect("serialize");
        let back: JournalExtractionArtifact = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(a, back);
    }
}
