//! Mechanical verification of LLM extraction claims against document text.
//!
//! Pure functions, no async, no database, no network. An LLM claim only
//! reaches the archived artifact if it is provable against the text: every
//! quote must be a substring (whitespace-normalized — portal PDFs contain
//! non-breaking spaces), a coding's code must appear inside its quote, and
//! a claimed date must appear literally inside its quote.

use super::artifact::{ExtractedCoding, RawExtraction, ICD10_CM_SYSTEM};

/// Extraction output that survived verification, plus what was dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedExtraction {
    /// Verified document date (ISO-8601).
    pub document_date: Option<String>,
    /// The verbatim span supporting the date.
    pub document_date_quote: Option<String>,
    /// Title (passed through unverified — presentational only).
    pub title: Option<String>,
    /// Codings whose quote and code both verified.
    pub codings: Vec<ExtractedCoding>,
    /// Human-readable reasons for every dropped claim.
    pub rejected: Vec<String>,
}

/// Collapse every run of Unicode whitespace (including NBSP) to one space.
pub(crate) fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// True when `needle` occurs in `haystack` NOT flanked by an ASCII digit on
/// either side — so a candidate date can't match inside a longer number
/// (e.g. `1/15/2026` inside `11/15/2026`).
fn contains_anchored(haystack: &str, needle: &str) -> bool {
    let mut start = 0;
    while let Some(pos) = haystack[start..].find(needle) {
        let abs = start + pos;
        let end = abs + needle.len();
        let left_ok = abs == 0
            || !haystack[..abs]
                .chars()
                .next_back()
                .is_some_and(|c| c.is_ascii_digit());
        let right_ok = end == haystack.len()
            || !haystack[end..]
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_digit());
        if left_ok && right_ok {
            return true;
        }
        start = abs + 1;
    }
    false
}

/// Render the plausible in-document spellings of an ISO date `YYYY-MM-DD`.
fn date_candidates(iso: &str) -> Option<Vec<String>> {
    const MONTHS: [&str; 12] = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    let mut parts = iso.splitn(3, '-');
    let year: i32 = parts.next()?.parse().ok()?;
    let month: u32 = parts.next()?.parse().ok()?;
    let day: u32 = parts.next()?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let name = MONTHS[(month - 1) as usize];
    let abbr = &name[..3];
    Some(vec![
        iso.to_owned(),
        format!("{month:02}/{day:02}/{year}"),
        format!("{month}/{day}/{year}"),
        format!("{name} {day}, {year}"),
        format!("{abbr} {day}, {year}"),
        format!("{day} {name} {year}"),
    ])
}

/// Verify raw LLM output against the extracted document text.
///
/// Claims that fail verification are dropped and reported in
/// [`VerifiedExtraction::rejected`]; they never reach the artifact.
#[must_use]
pub fn verify_extraction(text: &str, raw: RawExtraction) -> VerifiedExtraction {
    let norm_text = normalize_ws(text);
    let mut rejected = Vec::new();

    let mut codings = Vec::new();
    for c in raw.codings {
        let norm_quote = normalize_ws(&c.quote);
        if c.code.trim().is_empty() {
            rejected.push("coding with empty code".to_owned());
        } else if norm_quote.is_empty() {
            rejected.push(format!("coding {}: empty quote", c.code));
        } else if !norm_text.contains(&norm_quote) {
            rejected.push(format!(
                "coding {}: quote not found in document text: {:?}",
                c.code, c.quote
            ));
        } else if !norm_quote.contains(&c.code) {
            rejected.push(format!(
                "coding {}: code does not appear in its quote {:?}",
                c.code, c.quote
            ));
        } else {
            codings.push(ExtractedCoding {
                system: ICD10_CM_SYSTEM.to_owned(),
                code: c.code,
                display: c.display,
                quote: c.quote,
                section_label: c.section_label,
            });
        }
    }

    let (document_date, document_date_quote) = match (raw.document_date, raw.document_date_quote) {
        (Some(date), Some(quote)) => {
            let norm_quote = normalize_ws(&quote);
            let candidates = date_candidates(&date);
            match candidates {
                Some(cands)
                    if norm_text.contains(&norm_quote)
                        && cands.iter().any(|c| contains_anchored(&norm_quote, c)) =>
                {
                    (Some(date), Some(quote))
                }
                _ => {
                    rejected.push(format!(
                        "document_date {date}: quote missing from text or date not in quote: {quote:?}"
                    ));
                    (None, None)
                }
            }
        }
        (Some(date), None) => {
            rejected.push(format!("document_date {date}: no supporting quote"));
            (None, None)
        }
        (None, _) => (None, None),
    };

    VerifiedExtraction {
        document_date,
        document_date_quote,
        title: raw.title,
        codings,
        rejected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extraction::artifact::RawCoding;

    // NBSP after the colon, mirroring real portal-printout extraction output.
    const TEXT: &str = "Order Date:\u{a0}04/21/2026\n\
        Pre-Op Diagnosis/Indications: Abdominal pain,\n unspecified - R10.9\n\
        DIAGNOSIS: NEGATIVE FOR DYSPLASIA";

    fn raw(codings: Vec<RawCoding>, date: Option<&str>, date_quote: Option<&str>) -> RawExtraction {
        RawExtraction {
            document_date: date.map(str::to_owned),
            document_date_quote: date_quote.map(str::to_owned),
            title: Some("GI Pathology Report".to_owned()),
            codings,
        }
    }

    fn coding(code: &str, quote: &str) -> RawCoding {
        RawCoding {
            code: code.to_owned(),
            display: "Abdominal pain, unspecified".to_owned(),
            quote: quote.to_owned(),
            section_label: Some("Pre-Op Diagnosis/Indications".to_owned()),
        }
    }

    #[test]
    fn accepts_quote_across_whitespace_differences() {
        // The quote uses a single space where the text has a newline, and the
        // date quote uses a plain space where the text has an NBSP.
        let v = verify_extraction(
            TEXT,
            raw(
                vec![coding("R10.9", "Abdominal pain, unspecified - R10.9")],
                Some("2026-04-21"),
                Some("Order Date: 04/21/2026"),
            ),
        );
        assert_eq!(v.codings.len(), 1);
        assert_eq!(v.codings[0].system, ICD10_CM_SYSTEM);
        assert_eq!(v.document_date.as_deref(), Some("2026-04-21"));
        assert!(v.rejected.is_empty());
    }

    #[test]
    fn rejects_quote_not_in_text() {
        let v = verify_extraction(
            TEXT,
            raw(vec![coding("K62.5", "Hemorrhage - K62.5")], None, None),
        );
        assert!(v.codings.is_empty());
        assert_eq!(v.rejected.len(), 1);
        assert!(v.rejected[0].contains("K62.5"));
    }

    #[test]
    fn rejects_code_missing_from_its_quote() {
        let v = verify_extraction(
            TEXT,
            raw(
                vec![coding("Z12.11", "Abdominal pain, unspecified - R10.9")],
                None,
                None,
            ),
        );
        assert!(v.codings.is_empty());
        assert_eq!(v.rejected.len(), 1);
    }

    #[test]
    fn rejects_date_whose_quote_lacks_the_date() {
        let v = verify_extraction(
            TEXT,
            raw(vec![], Some("2026-05-01"), Some("Order Date: 04/21/2026")),
        );
        assert_eq!(v.document_date, None);
        assert_eq!(v.rejected.len(), 1);
    }

    #[test]
    fn rejects_date_without_quote() {
        let v = verify_extraction(TEXT, raw(vec![], Some("2026-04-21"), None));
        assert_eq!(v.document_date, None);
        assert_eq!(v.rejected.len(), 1);
    }

    #[test]
    fn rejects_date_that_only_matches_inside_a_longer_number() {
        // Quote contains 11/15/2026 (November); claim is January 15 whose
        // unpadded candidate "1/15/2026" is a substring of "11/15/2026".
        let text = "Order Date: 11/15/2026";
        let v = verify_extraction(
            text,
            raw(vec![], Some("2026-01-15"), Some("Order Date: 11/15/2026")),
        );
        assert_eq!(v.document_date, None);
        assert_eq!(v.rejected.len(), 1);
    }

    #[test]
    fn accepts_unpadded_date_at_a_digit_boundary() {
        let text = "Order Date: 4/21/2026 final";
        let v = verify_extraction(
            text,
            raw(vec![], Some("2026-04-21"), Some("Order Date: 4/21/2026")),
        );
        assert_eq!(v.document_date.as_deref(), Some("2026-04-21"));
        assert!(v.rejected.is_empty());
    }

    #[test]
    fn rejects_empty_code() {
        let v = verify_extraction(TEXT, raw(vec![coding("", "Abdominal")], None, None));
        assert!(v.codings.is_empty());
        assert_eq!(v.rejected.len(), 1);
    }
}
