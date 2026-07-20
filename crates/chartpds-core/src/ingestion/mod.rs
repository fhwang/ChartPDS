//! CCDA parsing + observation extraction + archive-to-index pipeline, plus
//! narrative-PDF ingestion (archive → text → verified LLM extraction).
//!
//! Public surface is the [`ingest`] function (the CCDA orchestrator),
//! [`ingest_narrative_pdf`] (the narrative-PDF orchestrator), and [`Error`].
//! Everything else is internal — the parser, self-check, and per-section
//! extractors live in [`ccda`](self::ccda).
//!
//! [`ingest`] is the canonical CCDA write path: archive blob + manifest →
//! parse → extract → one `source_documents` row plus one row per extracted
//! item. It deliberately runs without a transaction: if the process dies
//! mid-ingest, the archived bytes are durable — re-run from the archive.
//! Four CCDA sections are extracted today: vital signs and lab results
//! (both stored as observations — a lab draw is just an observation with a
//! lab LOINC code), problems (diagnoses), and medications (prescriptions).

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
