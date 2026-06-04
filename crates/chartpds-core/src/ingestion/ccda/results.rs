//! Extract observations from the results section (lab draws).
//!
//! Walks `ClinicalDocument > component > structuredBody > component >
//! section[code=30954-2] > entry > organizer > component > observation`
//! (with a fallback to `entry > observation` for documents that omit the
//! battery organizer), pulling code, effective time, and value from each
//! `<observation>`.
//!
//! These rows land in the same `observations` table as vital signs — a lab
//! result (`HbA1c`, LDL-C, glucose, …) is just an observation with a lab LOINC
//! code. We therefore reuse [`ExtractedObservation`] from the vitals module.
//!
//! The results section differs from vitals in two ways this module handles:
//!
//! - `effectiveTime` values routinely omit the timezone offset (the 14-char
//!   `YYYYMMDDHHmmss` form), which [`parse_hl7_timestamp`] now accepts.
//! - Lab `<value>` elements use a wider set of `xsi:type`s than vitals:
//!   `PQ` (numeric), `ST` (text), `CD` (coded) and `IVL_PQ` (interval). The
//!   value extraction here tolerates all four and never fails ingestion on an
//!   unrecognized type — a lab section must not be rejected wholesale because
//!   one result carries an exotic value shape.

use crate::clinical::fhir_system_for_oid;
use crate::ingestion::ccda::time::parse_hl7_timestamp;
use crate::ingestion::ccda::vitals::ExtractedObservation;
use crate::ingestion::{Error, Result};

const LOINC_OID: &str = "2.16.840.1.113883.6.1";
const RESULTS_LOINC: &str = "30954-2";
const XSI_NS: &str = "http://www.w3.org/2001/XMLSchema-instance";

/// Extract every observation under the results section (LOINC 30954-2).
///
/// Returns an empty vec if the document has no results section.
///
/// # Errors
///
/// Returns [`Error::NotCcda`] if a result observation is structurally
/// malformed — e.g. an `<effectiveTime>` missing its `value` attribute, or a
/// numeric `<value>` whose `value` attribute isn't parseable. A result with a
/// value type we don't model is kept (code + time, no value), not an error.
pub(crate) fn extract_results(doc: &roxmltree::Document<'_>) -> Result<Vec<ExtractedObservation>> {
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
        if !section_is_results(&section) {
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

fn section_is_results(section: &roxmltree::Node<'_, '_>) -> bool {
    section
        .children()
        .filter(|n| n.is_element() && n.tag_name().name() == "code")
        .any(|code| {
            code.attribute("codeSystem") == Some(LOINC_OID)
                && code.attribute("code") == Some(RESULTS_LOINC)
        })
}

/// Collect the result `<observation>` nodes in a results section.
///
/// Lab results are grouped under a `<organizer>` battery, one `<observation>`
/// per `<component>`. Some documents place a result `<observation>` directly
/// under `<entry>` with no organizer, so we walk both shapes. Either way we
/// only take direct-child observations — nested `entryRelationship`
/// observations (lab narrative notes, encounters) are deliberately skipped.
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
        // Fallback: result observation hung directly off the entry.
        for observation in entry
            .children()
            .filter(|n| n.is_element() && n.tag_name().name() == "observation")
        {
            out.push(observation);
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
            reason: "result <observation> missing <effectiveTime>".to_owned(),
        })?;
    let effective_start_str = effective_time
        .attribute("value")
        .ok_or_else(|| Error::NotCcda {
            reason: "result <effectiveTime> missing value attribute".to_owned(),
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
        effective_end: None,
        value_quantity,
        value_string,
        value_unit,
    }))
}

/// Extract the value triple from a lab `<value>` element.
///
/// Handles the four `xsi:type`s seen in real lab feeds:
/// - `PQ` — physical quantity: numeric value + unit. See [`extract_pq_value`]
///   for the nested-`<translation>` fallback that real EHR exports require.
/// - `IVL_PQ` — interval: a single-bound interval becomes that bound's
///   numeric value + unit; a two-bound interval is recorded textually as
///   `"low-high unit"` since collapsing a range to one number would lie.
/// - `CD` — coded value: the code's `displayName` (else `code`) as a string.
/// - `ST` (or untyped) — free text.
///
/// Any other type is tolerated as a valueless observation (`None, None,
/// None`): the draw is still recorded by code + time.
///
/// # Errors
///
/// Returns [`Error::NotCcda`] only when a `PQ`'s own `value` attribute is
/// present but non-numeric — a structurally broken quantity.
fn extract_value(
    value: &roxmltree::Node<'_, '_>,
) -> Result<(Option<f64>, Option<String>, Option<String>)> {
    let xsi_type = value
        .attributes()
        .find(|a| a.name() == "type" && a.namespace() == Some(XSI_NS))
        .map(|a| a.value());
    match xsi_type {
        Some("PQ") => extract_pq_value(value),
        Some("IVL_PQ") => Ok(extract_interval_value(value)),
        Some("CD") => {
            let text = value
                .attribute("displayName")
                .or_else(|| value.attribute("code"))
                .map(str::to_owned);
            Ok((None, text, None))
        }
        Some("ST") | None => {
            let text = value.text().map(str::to_owned);
            Ok((None, text, None))
        }
        Some(_) => Ok((None, None, None)),
    }
}

/// Extract the value triple from a `PQ` (physical quantity) `<value>`.
///
/// The straightforward case is a numeric `value` attribute with a `unit`
/// (e.g. `HbA1c` `<value xsi:type="PQ" value="5.5" unit="%"/>`).
///
/// Real EHR exports (Epic) also emit a `nullFlavor` PQ whose number is buried
/// in a nested `<translation value="63">`, with the unit only spelled out in
/// an `<originalText>` (e.g. `mg/dL (calc)`). This is how every calculated
/// LDL-C in the archive is encoded, so without the fallback LDL-C would stay
/// dark. We therefore: use the direct `value` if present, else fall back to a
/// `<translation>`'s `value`, taking the unit from the PQ's own `unit`
/// attribute, then the translation's `unit`, then any `<originalText>`.
///
/// A PQ with no recoverable number anywhere records a valueless draw rather
/// than failing the document.
///
/// # Errors
///
/// Returns [`Error::NotCcda`] only when the PQ's own `value` attribute is
/// present but does not parse as a number.
fn extract_pq_value(
    value: &roxmltree::Node<'_, '_>,
) -> Result<(Option<f64>, Option<String>, Option<String>)> {
    let translation = value
        .children()
        .find(|n| n.is_element() && n.tag_name().name() == "translation");
    let unit = value
        .attribute("unit")
        .map(str::to_owned)
        .or_else(|| {
            translation
                .and_then(|t| t.attribute("unit"))
                .map(str::to_owned)
        })
        .or_else(|| original_text(value));

    // Prefer the PQ's own value attribute.
    if let Some(v_str) = value.attribute("value") {
        let v: f64 = v_str.parse().map_err(|err| Error::NotCcda {
            reason: format!("<value xsi:type=PQ> value not numeric: {err}"),
        })?;
        return Ok((Some(v), None, unit));
    }

    // Fall back to a nested <translation value="...">. A non-numeric
    // translation is treated as no value rather than an error — it is only a
    // best-effort recovery of a nullFlavor PQ.
    if let Some(v) = translation
        .and_then(|t| t.attribute("value"))
        .and_then(|s| s.parse::<f64>().ok())
    {
        return Ok((Some(v), None, unit));
    }

    Ok((None, None, unit))
}

/// The text of the first descendant `<originalText>` element, trimmed.
///
/// `<originalText>` may sit directly under the `<value>` or under a nested
/// `<translation>`; a descendant walk finds it either way. Returns `None`
/// when absent or when it only references external narrative (`<reference>`).
fn original_text(value: &roxmltree::Node<'_, '_>) -> Option<String> {
    value
        .descendants()
        .find(|n| n.is_element() && n.tag_name().name() == "originalText")
        .and_then(|n| n.text())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

/// Reduce an `IVL_PQ` interval to a value triple.
///
/// A single bound (`<low>` or `<high>` alone, or both equal) is a definite
/// quantity. Two distinct bounds are a true range, kept as a `"low-high unit"`
/// string rather than fabricating a midpoint that downstream trending would
/// read as a measured value.
fn extract_interval_value(
    value: &roxmltree::Node<'_, '_>,
) -> (Option<f64>, Option<String>, Option<String>) {
    let bound = |name: &str| -> Option<(f64, Option<String>)> {
        value
            .children()
            .find(|n| n.is_element() && n.tag_name().name() == name)
            .and_then(|n| {
                let v: f64 = n.attribute("value")?.parse().ok()?;
                Some((v, n.attribute("unit").map(str::to_owned)))
            })
    };
    let low = bound("low");
    let high = bound("high");
    match (low, high) {
        (Some((l, unit)), Some((h, _))) if (l - h).abs() < f64::EPSILON => (Some(l), None, unit),
        (Some((l, unit)), Some((h, _))) => {
            let display = match &unit {
                Some(u) => format!("{l}-{h} {u}"),
                None => format!("{l}-{h}"),
            };
            (None, Some(display), None)
        }
        (Some((l, unit)), None) => (Some(l), None, unit),
        (None, Some((h, unit))) => (Some(h), None, unit),
        (None, None) => (None, None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingestion::ccda::parse::parse_xml;
    use time::macros::datetime;

    const VALID: &str = include_str!("fixtures/valid_minimal.xml");

    #[test]
    fn extract_finds_hba1c_in_minimal_fixture() {
        let doc = parse_xml(VALID).expect("parse");
        let results = extract_results(&doc).expect("extract");
        let a1c = results
            .iter()
            .find(|o| o.coding_code == "4548-4")
            .expect("HbA1c result present");
        assert_eq!(a1c.coding_system, "http://loinc.org");
        assert_eq!(a1c.coding_display.as_deref(), Some("HEMOGLOBIN A1c"));
        // 14-char effectiveTime with no offset, treated as UTC.
        assert_eq!(a1c.effective_start, datetime!(2025-08-06 04:02:00 UTC));
        assert_eq!(a1c.value_quantity, Some(5.5));
        assert_eq!(a1c.value_unit.as_deref(), Some("%"));
        assert_eq!(a1c.value_string, None);
    }

    #[test]
    fn extract_finds_ldl_in_minimal_fixture() {
        let doc = parse_xml(VALID).expect("parse");
        let results = extract_results(&doc).expect("extract");
        let ldl = results
            .iter()
            .find(|o| o.coding_code == "13457-7")
            .expect("LDL-C result present");
        assert_eq!(ldl.value_quantity, Some(99.0));
        assert_eq!(ldl.value_unit.as_deref(), Some("mg/dL"));
    }

    #[test]
    fn extract_returns_empty_when_no_results_section() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3">
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
        let results = extract_results(&doc).expect("extract");
        assert!(results.is_empty());
    }

    #[test]
    fn coded_value_is_kept_as_string_not_an_error() {
        // A CD-typed lab result (e.g. a qualitative finding) must not fail
        // ingestion; its displayName is recorded as the value string.
        let xml = results_section_with_value(
            r#"<value xsi:type="CD" code="10828004" codeSystem="2.16.840.1.113883.6.96" displayName="Positive"/>"#,
        );
        let doc = parse_xml(&xml).expect("parse");
        let results = extract_results(&doc).expect("extract");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value_string.as_deref(), Some("Positive"));
        assert_eq!(results[0].value_quantity, None);
    }

    #[test]
    fn interval_with_single_bound_is_a_quantity() {
        let xml = results_section_with_value(
            r#"<value xsi:type="IVL_PQ"><low value="60" unit="mL/min"/></value>"#,
        );
        let doc = parse_xml(&xml).expect("parse");
        let results = extract_results(&doc).expect("extract");
        assert_eq!(results[0].value_quantity, Some(60.0));
        assert_eq!(results[0].value_unit.as_deref(), Some("mL/min"));
    }

    #[test]
    fn interval_with_two_bounds_is_recorded_textually() {
        let xml = results_section_with_value(
            r#"<value xsi:type="IVL_PQ"><low value="70" unit="mg/dL"/><high value="99" unit="mg/dL"/></value>"#,
        );
        let doc = parse_xml(&xml).expect("parse");
        let results = extract_results(&doc).expect("extract");
        assert_eq!(results[0].value_quantity, None);
        assert_eq!(results[0].value_string.as_deref(), Some("70-99 mg/dL"));
    }

    #[test]
    fn nullflavor_pq_recovers_value_from_nested_translation() {
        // How every calculated LDL-C in the real archive is encoded: the
        // parent PQ is nullFlavor and the number lives in <translation>, with
        // the unit only in <originalText>. Without recovery, LDL-C stays dark.
        let xml = results_section_with_value(
            r#"<value xsi:type="PQ" nullFlavor="OTH"><translation value="63" nullFlavor="OTH"><originalText>mg/dL (calc)</originalText></translation></value>"#,
        );
        let doc = parse_xml(&xml).expect("parse");
        let results = extract_results(&doc).expect("extract");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value_quantity, Some(63.0));
        assert_eq!(results[0].value_unit.as_deref(), Some("mg/dL (calc)"));
    }

    #[test]
    fn nullflavor_pq_with_no_recoverable_value_is_valueless() {
        let xml = results_section_with_value(r#"<value xsi:type="PQ" nullFlavor="UNK"/>"#);
        let doc = parse_xml(&xml).expect("parse");
        let results = extract_results(&doc).expect("extract");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value_quantity, None);
        assert_eq!(results[0].value_string, None);
    }

    #[test]
    fn unknown_value_type_is_tolerated_as_valueless() {
        let xml = results_section_with_value(r#"<value xsi:type="ED">opaque</value>"#);
        let doc = parse_xml(&xml).expect("parse");
        let results = extract_results(&doc).expect("extract");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].coding_code, "4548-4");
        assert_eq!(results[0].value_quantity, None);
        assert_eq!(results[0].value_string, None);
    }

    /// Build a minimal CCDA carrying one result observation with the given
    /// raw `<value>` markup.
    fn results_section_with_value(value_markup: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <templateId root="2.16.840.1.113883.10.20.22.1.1"/>
  <component>
    <structuredBody>
      <component>
        <section>
          <code code="30954-2" codeSystem="2.16.840.1.113883.6.1" displayName="Results"/>
          <entry>
            <organizer classCode="BATTERY" moodCode="EVN">
              <code nullFlavor="UNK"/>
              <component>
                <observation classCode="OBS" moodCode="EVN">
                  <code code="4548-4" codeSystem="2.16.840.1.113883.6.1" displayName="HEMOGLOBIN A1c"/>
                  <effectiveTime value="20250806040200"/>
                  {value_markup}
                </observation>
              </component>
            </organizer>
          </entry>
        </section>
      </component>
    </structuredBody>
  </component>
</ClinicalDocument>"#
        )
    }
}
