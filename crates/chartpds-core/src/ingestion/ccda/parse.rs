//! Parse a CCDA document's raw XML into a `roxmltree::Document`.

use super::time::parse_hl7_timestamp;
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

/// Extract the document's authored date from `ClinicalDocument/effectiveTime`.
///
/// Returns the UTC calendar date as `YYYY-MM-DD`, or `None` when the element
/// is absent, has no `value`, or the value does not parse as an HL7 timestamp.
/// Used to populate `source_documents.document_date`.
pub(crate) fn extract_document_date(doc: &roxmltree::Document<'_>) -> Option<String> {
    let value = doc
        .root_element()
        .children()
        .find(|n| n.is_element() && n.tag_name().name() == "effectiveTime")?
        .attribute("value")?;
    let ts = parse_hl7_timestamp(value).ok()?;
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    ts.to_offset(time::UtcOffset::UTC).date().format(&fmt).ok()
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

    #[test]
    fn extract_document_date_returns_utc_date_from_valid_minimal() {
        let doc = parse_xml(VALID).expect("valid xml parses");
        let date = extract_document_date(&doc);
        assert_eq!(date.as_deref(), Some("2026-01-01"));
    }

    #[test]
    fn extract_document_date_returns_none_when_effective_time_absent() {
        let xml = r#"<ClinicalDocument xmlns="urn:hl7-org:v3"><code/></ClinicalDocument>"#;
        let doc = parse_xml(xml).expect("parses");
        assert!(extract_document_date(&doc).is_none());
    }

    #[test]
    fn extract_document_date_returns_none_when_value_attribute_absent() {
        let xml = r#"<ClinicalDocument xmlns="urn:hl7-org:v3"><effectiveTime/></ClinicalDocument>"#;
        let doc = parse_xml(xml).expect("parses");
        assert!(extract_document_date(&doc).is_none());
    }

    #[test]
    fn extract_document_date_returns_none_when_value_unparseable() {
        let xml = r#"<ClinicalDocument xmlns="urn:hl7-org:v3"><effectiveTime value="not-a-date"/></ClinicalDocument>"#;
        let doc = parse_xml(xml).expect("parses");
        assert!(extract_document_date(&doc).is_none());
    }
}
