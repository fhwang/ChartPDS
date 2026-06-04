//! Parse a CCDA document's raw XML into a `roxmltree::Document`.

use crate::ingestion::Result;

/// Parse the given XML string into a `roxmltree::Document`.
///
/// This is a thin wrapper that converts `roxmltree::Error` into
/// `ingestion::Error::Xml` via the existing `From` impl. The returned
/// document borrows from the input string; callers retain ownership.
///
/// # Errors
///
/// Returns [`crate::ingestion::Error::Xml`] if the bytes do not parse
/// as well-formed XML.
pub(crate) fn parse_xml(xml: &str) -> Result<roxmltree::Document<'_>> {
    Ok(roxmltree::Document::parse(xml)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = include_str!("fixtures/valid_minimal.xml");
    const BROKEN: &str = "<not-valid";

    #[test]
    fn parse_xml_accepts_valid_minimal_ccda() {
        let doc = parse_xml(VALID).expect("valid xml parses");
        assert_eq!(doc.root_element().tag_name().name(), "ClinicalDocument");
    }

    #[test]
    fn parse_xml_rejects_broken_xml() {
        let err = parse_xml(BROKEN).expect_err("broken xml fails");
        assert!(matches!(err, crate::ingestion::Error::Xml(_)));
    }
}
