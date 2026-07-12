//! Deterministic PDF text extraction (no LLM, no network).

use super::error::Error;

/// Extract the embedded text layer from PDF bytes.
///
/// Purely mechanical: the same bytes always produce the same text, so
/// `rebuild_index` can re-derive it from the archive alone.
///
/// # Errors
///
/// Returns [`Error::Pdf`] if the bytes are not parseable as a PDF, and
/// [`Error::NoTextLayer`] if parsing succeeds but yields no text (a scanned
/// image; OCR is out of scope).
pub fn extract_pdf_text(bytes: &[u8]) -> Result<String, Error> {
    let text = pdf_extract::extract_text_from_mem(bytes).map_err(|err| Error::Pdf {
        reason: err.to_string(),
    })?;
    if text.chars().all(char::is_whitespace) {
        return Err(Error::NoTextLayer);
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &[u8] = include_bytes!("fixtures/synthetic_pathology.pdf");

    #[test]
    fn extracts_text_with_codes_from_fixture() {
        let text = extract_pdf_text(FIXTURE).expect("extract");
        for needle in ["R10.9", "Z12.11", "K64.8", "DIAGNOSIS", "04/21/2026"] {
            assert!(
                text.contains(needle),
                "missing {needle:?} in extracted text"
            );
        }
    }

    #[test]
    fn non_pdf_bytes_return_pdf_error() {
        let err = extract_pdf_text(b"not a pdf").expect_err("should fail");
        assert!(matches!(err, Error::Pdf { .. }));
    }
}
