//! `ChartPDS` core library.
//!
//! The module tree below traces the data flow:
//!
//! - [`sources`] fetches from external APIs (Fitbit, Oura, Google Health).
//! - [`archive`] stores raw blobs durably (content-addressed via `object_store`).
//! - [`ingestion`] parses archive blobs into structured form.
//! - [`clinical`] holds domain knowledge (codings, taxonomies, kinds).
//! - [`extraction`] turns narrative PDFs into text + verified codings.
//! - [`index`] is the `SQLite` projection used for queries.
//! - [`queries`] exposes analytical reads over the index.
//! - [`sync`] orchestrates the loop on a schedule.
//! - [`notifications`] dispatches out-of-band events on failures.
//!
//! Data lives in three storage tiers, in decreasing order of sanctity:
//!
//! - **archive** (`$DIR/archive/`) — bytes that arrived from outside, raw
//!   and untouched. The system of record; never GC'd.
//! - **derived** (`$DIR/derived/`) — machine-generated derivations that are
//!   expensive to recreate (LLM extraction output is non-deterministic and
//!   costs money, so it is persisted, not recomputed on rebuild). Same
//!   content-addressed store type as the archive; versioned via each
//!   artifact's `extractor` `{model, prompt_version}`.
//! - **index** (`$DIR/chartpds.db`) — the disposable `SQLite` projection,
//!   rebuilt from the two blob stores by [`ingestion::rebuild_index`].

pub mod archive;
pub mod clinical;
pub mod extraction;
pub mod index;
pub mod ingestion;
pub mod notifications;
pub mod queries;
pub mod sources;
pub mod sync;
