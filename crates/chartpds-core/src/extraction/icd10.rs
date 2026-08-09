//! ICD-10-CM code validity and canonicalization: a vendored table of every
//! valid code.
//!
//! Source: the NCHS/CMS ICD-10-CM FY2026 "order file" (public domain),
//! stripped to the code column — dotless, uppercase, one code per line,
//! including non-billable category codes (journal extraction deliberately
//! prefers coarse codes, and a category like `M25` is a real code).
//! Vintage: FY2026. Annual staleness is acceptable for validity checking;
//! to refresh, re-run the acquisition step in the implementation plan and
//! replace the data file.

use std::sync::OnceLock;

const RAW: &str = include_str!("../../data/icd10cm_codes_fy2026.txt");

/// The sorted code table, parsed once on first use.
fn codes() -> &'static [&'static str] {
    static CODES: OnceLock<Vec<&'static str>> = OnceLock::new();
    CODES.get_or_init(|| {
        let mut v: Vec<&'static str> = RAW.lines().filter(|l| !l.is_empty()).collect();
        v.sort_unstable();
        v
    })
}

/// Trim, upper-case, and validate the *shape* of a code candidate, returning
/// its dotless form on success.
///
/// A shape is accepted only when it is dotless, or has exactly one dot
/// immediately after the third character (`"M25.512"`, not `"M2.5512"` or
/// `"M25.5.12"`) — the conventional ICD-10-CM category/subcategory split.
/// Case-insensitive; leading and trailing whitespace is ignored. Does not
/// consult the code table — callers combine this with a `codes()` lookup.
fn dotless_shape(code: &str) -> Option<String> {
    let trimmed = code.trim();
    if trimmed.is_empty() {
        return None;
    }
    let upper: String = trimmed.chars().map(|c| c.to_ascii_uppercase()).collect();
    match upper.find('.') {
        None => Some(upper),
        Some(pos) if pos == 3 && upper.matches('.').count() == 1 => {
            Some(upper.chars().filter(|c| *c != '.').collect())
        }
        Some(_) => None,
    }
}

/// True when `code` is a real ICD-10-CM code per the vendored FY2026 table.
///
/// Case-insensitive; the conventional dot after the third character is
/// optional (`"M25.512"` and `"M25512"` are the same code) but if present
/// must sit immediately after the third character, and only one dot is
/// permitted — `"M2.5512"` and `"M25.5.12"` are rejected as malformed
/// shapes, not merely unrecognized codes. Leading and trailing whitespace is
/// ignored.
#[must_use]
pub fn is_valid_icd10cm(code: &str) -> bool {
    dotless_shape(code)
        .is_some_and(|normalized| codes().binary_search(&normalized.as_str()).is_ok())
}

/// Canonicalize a real ICD-10-CM code to its standard dotted rendering, or
/// `None` when the shape is malformed or the code is not in the vendored
/// table.
///
/// Canonical form: uppercase, with a dot inserted immediately after the
/// third character when the code is longer than three characters (e.g.
/// `"m25512"` → `"M25.512"`, `"R109"` → `"R10.9"`, `"M25"` → `"M25"`
/// unchanged). Accepts the same input shapes as [`is_valid_icd10cm`] — dotted
/// or dotless, case-insensitive, trimmed — so callers can canonicalize
/// whatever an LLM proposed without validating it first.
#[must_use]
pub fn canonical_icd10cm(code: &str) -> Option<String> {
    let normalized = dotless_shape(code)?;
    if codes().binary_search(&normalized.as_str()).is_err() {
        return None;
    }
    if normalized.len() > 3 {
        Some(format!("{}.{}", &normalized[..3], &normalized[3..]))
    } else {
        Some(normalized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_real_codes_with_or_without_dot_any_case() {
        assert!(is_valid_icd10cm("M25.512"));
        assert!(is_valid_icd10cm("M25512"));
        assert!(is_valid_icd10cm("m25.512"));
        assert!(is_valid_icd10cm("R10.9"));
        assert!(is_valid_icd10cm(" R10.9 "));
        // Non-billable category code — deliberately valid (coarse coding).
        assert!(is_valid_icd10cm("M25"));
    }

    #[test]
    fn rejects_hallucinated_and_malformed_codes() {
        assert!(!is_valid_icd10cm("Q99.9999"));
        assert!(!is_valid_icd10cm("M25.5129"));
        assert!(!is_valid_icd10cm(""));
        assert!(!is_valid_icd10cm("NOTACODE"));
        assert!(!is_valid_icd10cm("123.45"));
    }

    #[test]
    fn rejects_dot_in_the_wrong_place_or_more_than_one_dot() {
        // Dot not immediately after the third character.
        assert!(!is_valid_icd10cm("M2.5512"));
        // Two dots.
        assert!(!is_valid_icd10cm("M25.5.12"));
    }

    #[test]
    fn canonicalizes_real_codes_to_the_dotted_form() {
        assert_eq!(canonical_icd10cm("m25512"), Some("M25.512".to_owned()));
        assert_eq!(canonical_icd10cm("M25.512"), Some("M25.512".to_owned()));
        assert_eq!(canonical_icd10cm("R109"), Some("R10.9".to_owned()));
        assert_eq!(canonical_icd10cm(" r10.9 "), Some("R10.9".to_owned()));
        // Three-character category code has no dot to insert.
        assert_eq!(canonical_icd10cm("m25"), Some("M25".to_owned()));
    }

    #[test]
    fn canonicalization_rejects_malformed_shapes_and_unknown_codes() {
        assert_eq!(canonical_icd10cm("M2.5512"), None);
        assert_eq!(canonical_icd10cm("M25.5.12"), None);
        assert_eq!(canonical_icd10cm("Q99.9999"), None);
        assert_eq!(canonical_icd10cm(""), None);
    }
}
