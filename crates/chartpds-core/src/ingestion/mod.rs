//! CCDA parsing + observation extraction + archive-to-index pipeline, plus
//! narrative-PDF ingestion (archive → text → verified LLM extraction) and
//! journal-entry ingestion (free colloquial `.md` text → deterministic
//! entry-date resolution → verified LLM inference of ICD-10-CM codings,
//! indexed as `derivation = 'inferred'` observations rather than problems).
//!
//! Public surface is the [`ingest`] function (the CCDA orchestrator),
//! [`ingest_narrative_pdf`] (the narrative-PDF orchestrator),
//! [`ingest_journal`] (the journal-entry orchestrator), and [`Error`].
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
mod journal;
mod narrative;
mod rebuild;

pub use error::{Error, Result};
pub use ingest::ingest;
pub use journal::{ingest_journal, JournalIngestOutcome, JOURNAL_EXTRACTION_KIND, JOURNAL_KIND};
pub use narrative::{
    ingest_narrative_pdf, NarrativeIngestOutcome, NarrativeIngestParams, NARRATIVE_EXTRACTION_KIND,
    NARRATIVE_PDF_KIND,
};
pub use rebuild::{rebuild_index, RebuildResult};
