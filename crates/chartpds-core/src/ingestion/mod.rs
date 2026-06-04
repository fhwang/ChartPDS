//! CCDA parsing + observation extraction + archive-to-index pipeline.
//!
//! Public surface is the [`ingest`] function (the orchestrator) and
//! [`Error`]. Everything else is internal — the parser, self-check,
//! and per-section extractors live in [`ccda`](self::ccda).

mod ccda;
mod error;
mod ingest;
mod rebuild;

pub use error::{Error, Result};
pub use ingest::ingest;
pub use rebuild::{rebuild_index, RebuildResult};
