//! Cheap structural check that a parsed XML document looks like a CCDA.

use crate::ingestion::{Error, Result};

const HL7_V3_NAMESPACE: &str = "urn:hl7-org:v3";

/// Cheap structural check: does this look plausibly like a CCDA?
///
/// Validates three minimal invariants:
/// - Root element is `<ClinicalDocument>`.
/// - Root element is in the HL7 v3 namespace.
/// - Root has at least one `<templateId>` child.
///
/// This is intentionally permissive: it does NOT validate template IDs
/// against the C-CDA OID space, nor does it inspect the body structure.
/// A document with one bogus `<templateId>` passes; the actual extraction
/// path will then yield zero observations or fail more specifically.
///
/// # Errors
///
/// Returns [`Error::NotCcda`] with a human-readable `reason` describing
/// which invariant failed.
pub(crate) fn self_check(doc: &roxmltree::Document<'_>) -> Result<()> {
    let root = doc.root_element();

    if root.tag_name().name() != "ClinicalDocument" {
        return Err(Error::NotCcda {
            reason: format!(
                "root element is <{}>, expected <ClinicalDocument>",
                root.tag_name().name()
            ),
        });
    }

    if root.tag_name().namespace() != Some(HL7_V3_NAMESPACE) {
        return Err(Error::NotCcda {
            reason: format!(
                "root element is in namespace {:?}, expected {HL7_V3_NAMESPACE:?}",
                root.tag_name().namespace()
            ),
        });
    }

    let has_template_id = root
        .children()
        .any(|c| c.is_element() && c.tag_name().name() == "templateId");
    if !has_template_id {
        return Err(Error::NotCcda {
            reason: "root element has no <templateId> child".to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::ccda::parse::parse_xml;

    const VALID: &str = include_str!("fixtures/valid_minimal.xml");
    const NOT_CCDA: &str = include_str!("fixtures/not_ccda.xml");

    #[test]
    fn self_check_accepts_valid_minimal_ccda() {
        let doc = parse_xml(VALID).expect("parse");
        self_check(&doc).expect("self-check passes");
    }

    #[test]
    fn self_check_rejects_non_ccda_root() {
        let doc = parse_xml(NOT_CCDA).expect("parse");
        let err = self_check(&doc).expect_err("self-check fails");
        assert!(matches!(err, Error::NotCcda { .. }));
    }

    #[test]
    fn self_check_rejects_clinical_document_without_template_id() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3">
</ClinicalDocument>"#;
        let doc = parse_xml(xml).expect("parse");
        let err = self_check(&doc).expect_err("self-check fails (no templateId)");
        match err {
            Error::NotCcda { reason } => {
                assert!(reason.contains("templateId"), "reason: {reason}");
            }
            other => panic!("expected NotCcda, got {other:?}"),
        }
    }

    #[test]
    fn self_check_rejects_clinical_document_in_wrong_namespace() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="http://example.com/different">
  <templateId root="2.16.840.1.113883.10.20.22.1.1"/>
</ClinicalDocument>"#;
        let doc = parse_xml(xml).expect("parse");
        let err = self_check(&doc).expect_err("self-check fails (wrong ns)");
        match err {
            Error::NotCcda { reason } => {
                assert!(reason.contains("namespace"), "reason: {reason}");
            }
            other => panic!("expected NotCcda, got {other:?}"),
        }
    }
}
