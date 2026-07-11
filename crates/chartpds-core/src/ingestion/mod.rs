//! CCDA parsing + observation extraction + archive-to-index pipeline, plus
//! narrative-PDF ingestion (archive → text → verified LLM extraction).
//!
//! Public surface is the [`ingest`] function (the CCDA orchestrator),
//! [`ingest_narrative_pdf`] (the narrative-PDF orchestrator), and [`Error`].
//! Everything else is internal — the parser, self-check, and per-section
//! extractors live in [`ccda`](self::ccda).

mod ccda;
mod error;
mod ingest;
mod narrative;
mod rebuild;

pub use error::{Error, Result};
pub use ingest::ingest;
pub use narrative::{
    ingest_narrative_pdf, NarrativeIngestOutcome, NarrativeIngestParams, NARRATIVE_EXTRACTION_KIND,
    NARRATIVE_PDF_KIND,
};
pub use rebuild::{rebuild_index, RebuildResult};
