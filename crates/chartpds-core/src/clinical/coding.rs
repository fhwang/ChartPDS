//! FHIR coding-system URIs and OID-to-URI translation.
//!
//! CCDA documents identify code systems by OID. The records layer
//! translates to FHIR canonical URIs at the boundary so everything
//! above speaks one vocabulary.

/// LOINC system URI.
pub const SYSTEM_LOINC: &str = "http://loinc.org";

/// `RxNorm` system URI.
pub const SYSTEM_RXNORM: &str = "http://www.nlm.nih.gov/research/umls/rxnorm";

/// ICD-10-CM system URI.
pub const SYSTEM_ICD10: &str = "http://hl7.org/fhir/sid/icd-10-cm";

/// SNOMED CT system URI.
pub const SYSTEM_SNOMED: &str = "http://snomed.info/sct";

/// Translate a CCDA-style OID into its canonical FHIR system URI.
///
/// Returns `None` for OIDs that are not in the known translation table.
/// New mappings should be added here as adapters or document parsers
/// require them.
#[must_use]
pub fn fhir_system_for_oid(oid: &str) -> Option<&'static str> {
    match oid {
        "2.16.840.1.113883.6.1" => Some(SYSTEM_LOINC),
        "2.16.840.1.113883.6.88" => Some(SYSTEM_RXNORM),
        "2.16.840.1.113883.6.90" => Some(SYSTEM_ICD10),
        "2.16.840.1.113883.6.96" => Some(SYSTEM_SNOMED),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_constants_are_canonical_fhir_uris() {
        assert_eq!(SYSTEM_LOINC, "http://loinc.org");
        assert_eq!(SYSTEM_RXNORM, "http://www.nlm.nih.gov/research/umls/rxnorm");
        assert_eq!(SYSTEM_ICD10, "http://hl7.org/fhir/sid/icd-10-cm");
        assert_eq!(SYSTEM_SNOMED, "http://snomed.info/sct");
    }

    #[test]
    fn fhir_system_for_oid_maps_loinc() {
        assert_eq!(
            fhir_system_for_oid("2.16.840.1.113883.6.1"),
            Some(SYSTEM_LOINC)
        );
    }

    #[test]
    fn fhir_system_for_oid_maps_rxnorm() {
        assert_eq!(
            fhir_system_for_oid("2.16.840.1.113883.6.88"),
            Some(SYSTEM_RXNORM)
        );
    }

    #[test]
    fn fhir_system_for_oid_maps_icd10() {
        assert_eq!(
            fhir_system_for_oid("2.16.840.1.113883.6.90"),
            Some(SYSTEM_ICD10)
        );
    }

    #[test]
    fn fhir_system_for_oid_maps_snomed() {
        assert_eq!(
            fhir_system_for_oid("2.16.840.1.113883.6.96"),
            Some(SYSTEM_SNOMED)
        );
    }

    #[test]
    fn fhir_system_for_oid_returns_none_for_unknown() {
        assert_eq!(fhir_system_for_oid("0.0.0.0"), None);
        assert_eq!(fhir_system_for_oid(""), None);
        assert_eq!(fhir_system_for_oid("2.16.840.1.113883.6.999"), None);
    }
}
