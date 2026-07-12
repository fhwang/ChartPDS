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

pub mod archive;
pub mod clinical;
pub mod extraction;
pub mod index;
pub mod ingestion;
pub mod notifications;
pub mod queries;
pub mod sources;
pub mod sync;
