//! Extract observations from the vital signs section.
//!
//! Walks `ClinicalDocument > component > structuredBody > component >
//! section[code=8716-3] > entry > organizer > component > observation`,
//! pulling code, effective time, and value from each `<observation>`.

use crate::clinical::fhir_system_for_oid;
use crate::ingestion::ccda::time::parse_hl7_timestamp;
use crate::ingestion::{Error, Result};
use time::OffsetDateTime;

const LOINC_OID: &str = "2.16.840.1.113883.6.1";
const VITAL_SIGNS_LOINC: &str = "8716-3";

/// A single observation extracted from a CCDA document.
///
/// Field shape mirrors `index::observations::NewObservation` so the ingester
/// can hand off rows without further transformation.
#[derive(Debug, Clone)]
pub(crate) struct ExtractedObservation {
    pub(crate) coding_system: String,
    pub(crate) coding_code: String,
    pub(crate) coding_display: Option<String>,
    pub(crate) effective_start: OffsetDateTime,
    pub(crate) effective_end: Option<OffsetDateTime>,
    pub(crate) value_quantity: Option<f64>,
    pub(crate) value_string: Option<String>,
    pub(crate) value_unit: Option<String>,
}

/// Extract every observation under the vital signs section.
///
/// Returns an empty vec if the document has no vital signs section.
///
/// # Errors
///
/// Returns [`Error::NotCcda`] if any observation it finds is malformed —
/// e.g. missing a `<code>` element, an unparseable `effectiveTime`, or an
/// unknown OID in the code system.
pub(crate) fn extract_observations(
    doc: &roxmltree::Document<'_>,
) -> Result<Vec<ExtractedObservation>> {
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
        if !section_is_vital_signs(&section) {
            continue;
        }
        for observation in observations_in_section(&section) {
            if let Some(obs) = extract_observation(&observation)? {
                out.push(obs);
            }
        }
    }
    Ok(out)
}

fn section_is_vital_signs(section: &roxmltree::Node<'_, '_>) -> bool {
    section
        .children()
        .filter(|n| n.is_element() && n.tag_name().name() == "code")
        .any(|code| {
            code.attribute("codeSystem") == Some(LOINC_OID)
                && code.attribute("code") == Some(VITAL_SIGNS_LOINC)
        })
}

fn observations_in_section<'doc, 'input>(
    section: &roxmltree::Node<'doc, 'input>,
) -> Vec<roxmltree::Node<'doc, 'input>> {
    let mut out = Vec::new();
    for entry in section
        .children()
        .filter(|n| n.is_element() && n.tag_name().name() == "entry")
    {
        for organizer in entry
            .children()
            .filter(|n| n.is_element() && n.tag_name().name() == "organizer")
        {
            for component in organizer
                .children()
                .filter(|n| n.is_element() && n.tag_name().name() == "component")
            {
                for observation in component
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

fn extract_observation(
    observation: &roxmltree::Node<'_, '_>,
) -> Result<Option<ExtractedObservation>> {
    let Some(code) = observation
        .children()
        .find(|n| n.is_element() && n.tag_name().name() == "code")
    else {
        return Ok(None);
    };
    // Skip entries with nullFlavor or otherwise missing coded value.
    let Some(oid) = code.attribute("codeSystem") else {
        return Ok(None);
    };
    let Some(coding_code) = code.attribute("code") else {
        return Ok(None);
    };
    let Some(coding_system) = fhir_system_for_oid(oid) else {
        return Ok(None);
    };
    let coding_display = code.attribute("displayName").map(str::to_owned);

    let effective_time = observation
        .children()
        .find(|n| n.is_element() && n.tag_name().name() == "effectiveTime")
        .ok_or_else(|| Error::NotCcda {
            reason: "<observation> missing <effectiveTime>".to_owned(),
        })?;
    let effective_start_str = effective_time
        .attribute("value")
        .ok_or_else(|| Error::NotCcda {
            reason: "<effectiveTime> missing value attribute".to_owned(),
        })?;
    let effective_start = parse_hl7_timestamp(effective_start_str)?;

    let value_node = observation
        .children()
        .find(|n| n.is_element() && n.tag_name().name() == "value");
    let (value_quantity, value_string, value_unit) = match value_node {
        Some(v) => extract_value(&v)?,
        None => (None, None, None),
    };

    Ok(Some(ExtractedObservation {
        coding_system: coding_system.to_owned(),
        coding_code: coding_code.to_owned(),
        coding_display,
        effective_start,
        // TODO: parse <low>/<high> children of <effectiveTime> for interval
        // measurements. Today every observation is a point in time.
        effective_end: None,
        value_quantity,
        value_string,
        value_unit,
    }))
}

fn extract_value(
    value: &roxmltree::Node<'_, '_>,
) -> Result<(Option<f64>, Option<String>, Option<String>)> {
    let xsi_type = value
        .attributes()
        .find(|a| {
            a.name() == "type" && a.namespace() == Some("http://www.w3.org/2001/XMLSchema-instance")
        })
        .map(|a| a.value());
    match xsi_type {
        Some("PQ") => {
            let v_str = value.attribute("value").ok_or_else(|| Error::NotCcda {
                reason: "<value xsi:type=PQ> missing value attribute".to_owned(),
            })?;
            let v: f64 = v_str.parse().map_err(|err| Error::NotCcda {
                reason: format!("<value xsi:type=PQ> value not numeric: {err}"),
            })?;
            let unit = value.attribute("unit").map(str::to_owned);
            Ok((Some(v), None, unit))
        }
        Some("ST") | None => {
            let text = value.text().map(str::to_owned);
            Ok((None, text, None))
        }
        Some(other) => Err(Error::NotCcda {
            reason: format!("unsupported value xsi:type {other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::ccda::parse::parse_xml;
    use time::macros::datetime;

    const VALID: &str = include_str!("fixtures/valid_minimal.xml");

    #[test]
    fn extract_returns_two_observations_for_minimal_fixture() {
        let doc = parse_xml(VALID).expect("parse");
        let obs = extract_observations(&doc).expect("extract");
        assert_eq!(obs.len(), 2);
    }

    #[test]
    fn extracted_body_weight_observation_has_expected_fields() {
        let doc = parse_xml(VALID).expect("parse");
        let obs = extract_observations(&doc).expect("extract");
        let weight = obs
            .iter()
            .find(|o| o.coding_code == "29463-7")
            .expect("body weight observation present");
        assert_eq!(weight.coding_system, "http://loinc.org");
        assert_eq!(weight.coding_display.as_deref(), Some("Body Weight"));
        assert_eq!(weight.effective_start, datetime!(2026-01-01 12:00:00 UTC));
        assert_eq!(weight.value_quantity, Some(72.5));
        assert_eq!(weight.value_unit.as_deref(), Some("kg"));
        assert_eq!(weight.value_string, None);
    }

    #[test]
    fn extracted_body_height_observation_has_expected_fields() {
        let doc = parse_xml(VALID).expect("parse");
        let obs = extract_observations(&doc).expect("extract");
        let height = obs
            .iter()
            .find(|o| o.coding_code == "8302-2")
            .expect("body height observation present");
        assert_eq!(height.coding_system, "http://loinc.org");
        assert_eq!(height.value_quantity, Some(175.0));
        assert_eq!(height.value_unit.as_deref(), Some("cm"));
    }

    #[test]
    fn extract_returns_empty_when_no_vital_signs_section() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3">
  <templateId root="2.16.840.1.113883.10.20.22.1.1"/>
  <component>
    <structuredBody>
      <component>
        <section>
          <code code="42349-1" codeSystem="2.16.840.1.113883.6.1" displayName="Reason for visit"/>
        </section>
      </component>
    </structuredBody>
  </component>
</ClinicalDocument>"#;
        let doc = parse_xml(xml).expect("parse");
        let obs = extract_observations(&doc).expect("extract");
        assert!(obs.is_empty());
    }

    #[test]
    fn extract_st_value_observation_populates_value_string() {
        // ST is the "string" value type — used for categorical observations
        // like sleep stage names. The extractor should pull the element's
        // text into value_string and leave value_quantity / value_unit None.
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <templateId root="2.16.840.1.113883.10.20.22.1.1"/>
  <component>
    <structuredBody>
      <component>
        <section>
          <code code="8716-3" codeSystem="2.16.840.1.113883.6.1" displayName="Vital signs"/>
          <entry>
            <organizer classCode="CLUSTER" moodCode="EVN">
              <component>
                <observation classCode="OBS" moodCode="EVN">
                  <code code="custom-mood" codeSystem="2.16.840.1.113883.6.1" displayName="Patient mood"/>
                  <effectiveTime value="20260101120000+0000"/>
                  <value xsi:type="ST">good</value>
                </observation>
              </component>
            </organizer>
          </entry>
        </section>
      </component>
    </structuredBody>
  </component>
</ClinicalDocument>"#;
        let doc = parse_xml(xml).expect("parse");
        let obs = extract_observations(&doc).expect("extract");
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].value_string.as_deref(), Some("good"));
        assert_eq!(obs[0].value_quantity, None);
        assert_eq!(obs[0].value_unit, None);
    }
}
