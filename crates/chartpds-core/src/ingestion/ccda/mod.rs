//! CCDA-specific parsing and extraction.
//!
//! Submodules build up the pipeline:
//! - `parse` — XML parsing (roxmltree wrapper).
//! - `self_check` — confirms the document is a CCDA.
//! - `time` — HL7 v3 timestamp parsing.
//! - `vitals` — extracts observations from the vital signs section.
//! - `problems` — extracts problems from the problem list section.
//! - `medications` — extracts medications from the medications section.
//! - `results` — extracts observations from the lab results section.

pub(crate) mod medications;
pub(crate) mod parse;
pub(crate) mod problems;
pub(crate) mod results;
pub(crate) mod self_check;
pub(crate) mod time;
pub(crate) mod vitals;

pub(crate) use medications::extract_medications;
pub(crate) use problems::extract_problems;
pub(crate) use results::extract_results;
pub(crate) use vitals::extract_observations;
