//! Extract problems from the problem list section.
//!
//! Walks `ClinicalDocument > component > structuredBody > component >
//! section[code=11450-4] > entry > act > entryRelationship > observation`,
//! pulling the diagnosis code from `<value xsi:type="CD">` (not `<code>`),
//! status from `<statusCode>`, and onset date from `<effectiveTime><low>`.

use crate::clinical::fhir_system_for_oid;
use crate::ingestion::{Error, Result};

const LOINC_OID: &str = "2.16.840.1.113883.6.1";
const PROBLEM_LIST_LOINC: &str = "11450-4";

/// A single problem extracted from a CCDA document.
#[derive(Debug, Clone)]
pub(crate) struct ExtractedProblem {
    pub(crate) coding_system: String,
    pub(crate) coding_code: String,
    pub(crate) coding_display: Option<String>,
    pub(crate) status: String,
    pub(crate) onset_date: Option<String>,
}

/// Extract every problem under the problem list section (LOINC 11450-4).
///
/// Returns an empty vec if the document has no problem list section.
///
/// # Errors
///
/// Returns [`Error::NotCcda`] if any problem observation is malformed —
/// e.g. missing a `<value>` element, an unknown OID, or a missing status.
pub(crate) fn extract_problems(doc: &roxmltree::Document<'_>) -> Result<Vec<ExtractedProblem>> {
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
        if !section_is_problem_list(&section) {
            continue;
        }
        for observation in observations_in_section(&section) {
            if let Some(problem) = extract_problem(&observation)? {
                out.push(problem);
            }
        }
    }
    Ok(out)
}

fn section_is_problem_list(section: &roxmltree::Node<'_, '_>) -> bool {
    section
        .children()
        .filter(|n| n.is_element() && n.tag_name().name() == "code")
        .any(|code| {
            code.attribute("codeSystem") == Some(LOINC_OID)
                && code.attribute("code") == Some(PROBLEM_LIST_LOINC)
        })
}

/// Walk entry > act > entryRelationship > observation.
fn observations_in_section<'doc, 'input>(
    section: &roxmltree::Node<'doc, 'input>,
) -> Vec<roxmltree::Node<'doc, 'input>> {
    let mut out = Vec::new();
    for entry in section
        .children()
        .filter(|n| n.is_element() && n.tag_name().name() == "entry")
    {
        for act in entry
            .children()
            .filter(|n| n.is_element() && n.tag_name().name() == "act")
        {
            for er in act
                .children()
                .filter(|n| n.is_element() && n.tag_name().name() == "entryRelationship")
            {
                for observation in er
                    .children()
                    .filter(|n| n.is_element() && n.tag_name().name() == "observation")
                {
                    out.push(observation);
                }
            }
        }
    }
    out
}

fn extract_problem(observation: &roxmltree::Node<'_, '_>) -> Result<Option<ExtractedProblem>> {
    // Diagnosis code comes from <value xsi:type="CD">, NOT from <code>.
    let Some(value) = observation.children().find(|n| {
        if !n.is_element() || n.tag_name().name() != "value" {
            return false;
        }
        n.attributes().any(|a| {
            a.name() == "type"
                && a.namespace() == Some("http://www.w3.org/2001/XMLSchema-instance")
                && a.value() == "CD"
        })
    }) else {
        return Ok(None);
    };

    // Skip entries with nullFlavor or otherwise missing coded value —
    // these are valid CCDA but carry no usable diagnosis data.
    let Some(oid) = value.attribute("codeSystem") else {
        return Ok(None);
    };
    let Some(coding_code) = value.attribute("code") else {
        return Ok(None);
    };
    let Some(coding_system) = fhir_system_for_oid(oid) else {
        return Ok(None);
    };
    let coding_display = value.attribute("displayName").map(str::to_owned);

    // Status from <statusCode code="...">.
    let status_code = observation
        .children()
        .find(|n| n.is_element() && n.tag_name().name() == "statusCode")
        .ok_or_else(|| Error::NotCcda {
            reason: "<observation> missing <statusCode>".to_owned(),
        })?;
    let status = status_code
        .attribute("code")
        .ok_or_else(|| Error::NotCcda {
            reason: "<statusCode> missing code attribute".to_owned(),
        })?;

    // Onset date from <effectiveTime><low value="YYYYMMDD">.
    let onset_date = observation
        .children()
        .find(|n| n.is_element() && n.tag_name().name() == "effectiveTime")
        .and_then(|et| {
            et.children()
                .find(|n| n.is_element() && n.tag_name().name() == "low")
        })
        .and_then(|low| low.attribute("value"))
        .map(|s| format!("{}-{}-{}", &s[0..4], &s[4..6], &s[6..8]));

    Ok(Some(ExtractedProblem {
        coding_system: coding_system.to_owned(),
        coding_code: coding_code.to_owned(),
        coding_display,
        status: status.to_owned(),
        onset_date,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::ccda::parse::parse_xml;

    const VALID: &str = include_str!("fixtures/valid_minimal.xml");

    #[test]
    fn extract_returns_one_problem_for_minimal_fixture() {
        let doc = parse_xml(VALID).expect("parse");
        let problems = extract_problems(&doc).expect("extract");
        assert_eq!(problems.len(), 1);
    }

    #[test]
    fn extracted_problem_has_expected_fields() {
        let doc = parse_xml(VALID).expect("parse");
        let problems = extract_problems(&doc).expect("extract");
        let p = &problems[0];
        assert_eq!(p.coding_system, "http://snomed.info/sct");
        assert_eq!(p.coding_code, "44054006");
        assert_eq!(
            p.coding_display.as_deref(),
            Some("Type 2 diabetes mellitus")
        );
        assert_eq!(p.status, "completed");
        assert_eq!(p.onset_date.as_deref(), Some("2020-03-15"));
    }

    #[test]
    fn extract_skips_null_flavor_problems() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <templateId root="2.16.840.1.113883.10.20.22.1.1"/>
  <component>
    <structuredBody>
      <component>
        <section>
          <code code="11450-4" codeSystem="2.16.840.1.113883.6.1" displayName="Problem list"/>
          <entry>
            <act classCode="ACT" moodCode="EVN">
              <entryRelationship typeCode="SUBJ">
                <observation classCode="OBS" moodCode="EVN">
                  <statusCode code="completed"/>
                  <value xsi:type="CD" nullFlavor="OTH"/>
                </observation>
              </entryRelationship>
              <entryRelationship typeCode="SUBJ">
                <observation classCode="OBS" moodCode="EVN">
                  <statusCode code="completed"/>
                  <effectiveTime><low value="20200315"/></effectiveTime>
                  <value xsi:type="CD" code="44054006" codeSystem="2.16.840.1.113883.6.96"
                         displayName="Type 2 diabetes mellitus"/>
                </observation>
              </entryRelationship>
            </act>
          </entry>
        </section>
      </component>
    </structuredBody>
  </component>
</ClinicalDocument>"#;
        let doc = parse_xml(xml).expect("parse");
        let problems = extract_problems(&doc).expect("should not error on nullFlavor");
        // Only the coded problem should be extracted; the nullFlavor one is skipped.
        assert_eq!(problems.len(), 1);
        assert_eq!(problems[0].coding_code, "44054006");
    }

    #[test]
    fn extract_returns_empty_when_no_problems_section() {
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
        let problems = extract_problems(&doc).expect("extract");
        assert!(problems.is_empty());
    }
}
