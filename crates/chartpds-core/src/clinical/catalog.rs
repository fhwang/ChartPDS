//! Static catalog describing the codings `ChartPDS` itself mints.
//!
//! Standard codings (LOINC, …) are self-describing to a client that knows the
//! vocabulary and are deliberately omitted. The sleep-stage entry is derived
//! from [`AasmSleepStage`] so the catalog cannot drift from the encoder.

use crate::clinical::{AasmSleepStage, AASM_SLEEP_STAGE_CODE, AASM_SLEEP_STAGE_SYSTEM};

/// One allowed value of a minted coding's value vocabulary.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct CodingValue {
    /// Numeric value stored in `value_quantity`.
    pub value_quantity: f64,
    /// String value stored in `value_string`.
    pub value_string: &'static str,
    /// Human-facing label for this value.
    pub label: &'static str,
}

/// A self-description of one ChartPDS-minted coding.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct CodingDefinition {
    /// FHIR coding system URI.
    pub coding_system: &'static str,
    /// Coding code within the system.
    pub coding_code: &'static str,
    /// What the coding measures.
    pub description: &'static str,
    /// How to read `value_quantity`.
    pub value_quantity_meaning: &'static str,
    /// How to read `value_string`.
    pub value_string_meaning: &'static str,
    /// The value vocabulary.
    pub values: Vec<CodingValue>,
    /// Practical hints for driving the aggregator/history tools.
    pub hints: Vec<&'static str>,
}

/// All ChartPDS-minted coding definitions (LOINC and other standard codings
/// are excluded as self-describing).
#[must_use]
pub fn minted_coding_definitions() -> Vec<CodingDefinition> {
    vec![sleep_stage_definition()]
}

fn sleep_stage_definition() -> CodingDefinition {
    let values = AasmSleepStage::ALL
        .iter()
        .map(|s| CodingValue {
            value_quantity: f64::from(s.discriminant()),
            value_string: s.as_str(),
            label: s.label(),
        })
        .collect();
    CodingDefinition {
        coding_system: AASM_SLEEP_STAGE_SYSTEM,
        coding_code: AASM_SLEEP_STAGE_CODE,
        description: "Per-epoch (5-minute) AASM sleep stage. One observation per 5-min epoch; effective_start/end bound the epoch.",
        value_quantity_meaning: "AASM stage discriminant. Monotonic: 0 = wake, >=1 = asleep.",
        value_string_meaning: "Stage name matching the discriminant.",
        values,
        hints: vec!["For 'asleep' totals use value_range {min: 1, max: 4}; wake is 0."],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_exactly_the_sleep_stage_coding() {
        let defs = minted_coding_definitions();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].coding_code, AASM_SLEEP_STAGE_CODE);
        assert_eq!(defs[0].coding_system, AASM_SLEEP_STAGE_SYSTEM);
    }

    #[test]
    fn sleep_stage_values_match_enum() {
        let def = sleep_stage_definition();
        assert_eq!(def.values.len(), AasmSleepStage::ALL.len());
        for (v, stage) in def.values.iter().zip(AasmSleepStage::ALL.iter()) {
            // Use bit-level equality to avoid clippy::float_cmp: discriminants
            // are small integers that are exactly representable in f64, so
            // bitwise equality is correct.
            let expected_qty = f64::from(stage.discriminant());
            assert!(
                v.value_quantity.to_bits() == expected_qty.to_bits(),
                "value_quantity mismatch for {:?}: {} != {}",
                stage,
                v.value_quantity,
                expected_qty,
            );
            assert_eq!(v.value_string, stage.as_str());
            assert_eq!(v.label, stage.label());
        }
    }
}
