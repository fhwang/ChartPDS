//! Narrative-document extraction: deterministic PDF text + one-time,
//! mechanically verified LLM extraction of explicitly quoted codings.
//!
//! Design invariant: the LLM runs once, at ingest. Its verified output is
//! frozen as an archived artifact; `rebuild_index` replays the artifact and
//! never calls a model.

mod artifact;
mod error;
mod llm;
mod pdf;
mod verify;

pub use artifact::{
    ExtractedCoding, ExtractionArtifact, ExtractorInfo, RawCoding, RawExtraction, ICD10_CM_SYSTEM,
};
pub use error::Error;
pub use llm::{ClaudeExtractor, LlmExtractor, EXTRACTION_MODEL, PROMPT_VERSION};
pub use pdf::extract_pdf_text;
pub use verify::{verify_extraction, VerifiedExtraction};
