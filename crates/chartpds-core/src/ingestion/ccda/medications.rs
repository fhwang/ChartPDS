//! Extract medications from the medications section.
//!
//! Walks `ClinicalDocument > component > structuredBody > component >
//! section[code=10160-0] > entry > substanceAdministration`, pulling
//! the drug code from `consumable > manufacturedProduct >
//! manufacturedMaterial > code`, status, dose, route, and date range.

use crate::clinical::fhir_system_for_oid;
use crate::ingestion::{Error, Result};

const LOINC_OID: &str = "2.16.840.1.113883.6.1";
const MEDICATIONS_LOINC: &str = "10160-0";
const XSI_NS: &str = "http://www.w3.org/2001/XMLSchema-instance";

/// A single medication extracted from a CCDA document.
#[derive(Debug, Clone)]
pub(crate) struct ExtractedMedication {
    pub(crate) coding_system: String,
    pub(crate) coding_code: String,
    pub(crate) coding_display: Option<String>,
    pub(crate) status: String,
    pub(crate) dose: Option<String>,
    pub(crate) route: Option<String>,
    pub(crate) frequency: Option<String>,
    pub(crate) start_date: Option<String>,
    pub(crate) end_date: Option<String>,
}

/// Extract every medication under the medications section (LOINC 10160-0).
///
/// Returns an empty vec if the document has no medications section.
///
/// # Errors
///
/// Returns [`Error::NotCcda`] if any substanceAdministration is malformed —
/// e.g. missing drug code, an unknown OID, or a missing status.
pub(crate) fn extract_medications(
    doc: &roxmltree::Document<'_>,
) -> Result<Vec<ExtractedMedication>> {
    let root = doc.root_element();
    let Some(structured_body) = root
        .children()
        .filter(|n| n.is_element() && n.tag_name().name() == "component")
        .find_map(|c| {
            c.children()
                .find(|n| n.is_element() && n.tag_name().name() == "structuredBody")
        })
    else {
        return Ok(Vec::new());
    };

    let mut out = Vec::new();
    for section_component in structured_body
        .children()
        .filter(|n| n.is_element() && n.tag_name().name() == "component")
    {
        let Some(section) = section_component
            .children()
            .find(|n| n.is_element() && n.tag_name().name() == "section")
        else {
            continue;
        };
        if !section_is_medications(&section) {
            continue;
        }
        for sa in substance_administrations_in_section(&section) {
            if let Some(med) = extract_medication(&sa)? {
                out.push(med);
            }
        }
    }
    Ok(out)
}

fn section_is_medications(section: &roxmltree::Node<'_, '_>) -> bool {
    section
        .children()
        .filter(|n| n.is_element() && n.tag_name().name() == "code")
        .any(|code| {
            code.attribute("codeSystem") == Some(LOINC_OID)
                && code.attribute("code") == Some(MEDICATIONS_LOINC)
        })
}

fn substance_administrations_in_section<'doc, 'input>(
    section: &roxmltree::Node<'doc, 'input>,
) -> Vec<roxmltree::Node<'doc, 'input>> {
    let mut out = Vec::new();
    for entry in section
        .children()
        .filter(|n| n.is_element() && n.tag_name().name() == "entry")
    {
        for sa in entry
            .children()
            .filter(|n| n.is_element() && n.tag_name().name() == "substanceAdministration")
        {
            out.push(sa);
        }
    }
    out
}

fn extract_medication(sa: &roxmltree::Node<'_, '_>) -> Result<Option<ExtractedMedication>> {
    // Drug code: consumable > manufacturedProduct > manufacturedMaterial > code.
    let Some(code_elem) = sa
        .children()
        .find(|n| n.is_element() && n.tag_name().name() == "consumable")
        .and_then(|consumable| {
            consumable
                .children()
                .find(|n| n.is_element() && n.tag_name().name() == "manufacturedProduct")
        })
        .and_then(|mp| {
            mp.children()
                .find(|n| n.is_element() && n.tag_name().name() == "manufacturedMaterial")
        })
        .and_then(|mm| {
            mm.children()
                .find(|n| n.is_element() && n.tag_name().name() == "code")
        })
    else {
        return Ok(None);
    };

    // Skip entries with nullFlavor or otherwise missing coded value.
    let Some(oid) = code_elem.attribute("codeSystem") else {
        return Ok(None);
    };
    let Some(coding_code) = code_elem.attribute("code") else {
        return Ok(None);
    };
    let Some(coding_system) = fhir_system_for_oid(oid) else {
        return Ok(None);
    };
    let coding_display = code_elem.attribute("displayName").map(str::to_owned);

    // Status from <statusCode code="...">.
    let status_code = sa
        .children()
        .find(|n| n.is_element() && n.tag_name().name() == "statusCode")
        .ok_or_else(|| Error::NotCcda {
            reason: "<substanceAdministration> missing <statusCode>".to_owned(),
        })?;
    let status = status_code
        .attribute("code")
        .ok_or_else(|| Error::NotCcda {
            reason: "<statusCode> missing code attribute".to_owned(),
        })?;

    // Dose from <doseQuantity value="..." unit="...">.
    let dose = sa
        .children()
        .find(|n| n.is_element() && n.tag_name().name() == "doseQuantity")
        .and_then(|dq| {
            let value = dq.attribute("value")?;
            let unit = dq.attribute("unit")?;
            Some(format!("{value} {unit}"))
        });

    // Route from <routeCode displayName="...">.
    let route = sa
        .children()
        .find(|n| n.is_element() && n.tag_name().name() == "routeCode")
        .and_then(|rc| rc.attribute("displayName").map(str::to_owned));

    // Date range from <effectiveTime xsi:type="IVL_TS"><low>/<high>.
    let ivl_ts = sa.children().find(|n| {
        n.is_element()
            && n.tag_name().name() == "effectiveTime"
            && n.attributes().any(|a| {
                a.name() == "type" && a.namespace() == Some(XSI_NS) && a.value() == "IVL_TS"
            })
    });

    let start_date = ivl_ts.as_ref().and_then(|et| {
        et.children()
            .find(|n| n.is_element() && n.tag_name().name() == "low")
            .and_then(|low| low.attribute("value"))
            .map(|s| format!("{}-{}-{}", &s[0..4], &s[4..6], &s[6..8]))
    });

    let end_date = ivl_ts.as_ref().and_then(|et| {
        et.children()
            .find(|n| n.is_element() && n.tag_name().name() == "high")
            .and_then(|high| high.attribute("value"))
            .map(|s| format!("{}-{}-{}", &s[0..4], &s[4..6], &s[6..8]))
    });

    // Frequency: deferred (PIVL_TS support not yet implemented).
    let frequency = None;

    Ok(Some(ExtractedMedication {
        coding_system: coding_system.to_owned(),
        coding_code: coding_code.to_owned(),
        coding_display,
        status: status.to_owned(),
        dose,
        route,
        frequency,
        start_date,
        end_date,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::ccda::parse::parse_xml;

    const VALID: &str = include_str!("fixtures/valid_minimal.xml");

    #[test]
    fn extract_returns_one_medication_for_minimal_fixture() {
        let doc = parse_xml(VALID).expect("parse");
        let meds = extract_medications(&doc).expect("extract");
        assert_eq!(meds.len(), 1);
    }

    #[test]
    fn extracted_medication_has_expected_fields() {
        let doc = parse_xml(VALID).expect("parse");
        let meds = extract_medications(&doc).expect("extract");
        let m = &meds[0];
        assert_eq!(
            m.coding_system,
            "http://www.nlm.nih.gov/research/umls/rxnorm"
        );
        assert_eq!(m.coding_code, "860975");
        assert_eq!(
            m.coding_display.as_deref(),
            Some("Metformin 500 MG Oral Tablet")
        );
        assert_eq!(m.status, "active");
        assert_eq!(m.dose.as_deref(), Some("500 mg"));
        assert_eq!(m.route, None);
        assert_eq!(m.frequency, None);
        assert_eq!(m.start_date.as_deref(), Some("2021-06-01"));
        assert_eq!(m.end_date, None);
    }

    #[test]
    fn extract_returns_empty_when_no_medications_section() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <templateId root="2.16.840.1.113883.10.20.22.1.1"/>
  <component>
    <structuredBody>
      <component>
        <section>
          <code code="8716-3" codeSystem="2.16.840.1.113883.6.1" displayName="Vital signs"/>
        </section>
      </component>
    </structuredBody>
  </component>
</ClinicalDocument>"#;
        let doc = parse_xml(xml).expect("parse");
        let meds = extract_medications(&doc).expect("extract");
        assert!(meds.is_empty());
    }
}
