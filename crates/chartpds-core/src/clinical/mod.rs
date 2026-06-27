//! Clinical-domain taxonomies and registries.
//!
//! Pure data + lookup. No I/O. Used by both [`ingestion`](super::ingestion)
//! (which annotates extracted observations) and [`queries`](super::queries)
//! (which translates user-facing terms into stored representations).

mod aasm;
mod catalog;
mod coding;
mod kind_registry;

pub use aasm::{AasmSleepStage, AASM_SLEEP_STAGE_CODE, AASM_SLEEP_STAGE_SYSTEM};
pub use catalog::{minted_coding_definitions, CodingDefinition, CodingValue};
pub use coding::{
    fhir_system_for_oid, LOINC_SLEEP_DURATION, LOINC_WASO, SYSTEM_ICD10, SYSTEM_LOINC,
    SYSTEM_RXNORM, SYSTEM_SNOMED,
};
pub use kind_registry::{Kind, KindParseError};
