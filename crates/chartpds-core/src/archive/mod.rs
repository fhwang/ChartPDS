//! Content-addressed blob archive.
//!
//! Wraps [`object_store`] with a domain layer that identifies blobs by the
//! SHA-256 hash of their content. The hash is the storage path; equal content
//! always produces equal keys (deduplication is automatic). All source types
//! share one flat directory; the bytes stay the raw, untouched payload
//! (CCDA XML, Fitbit/Oura JSON, clinical PDFs).
//!
//! Each blob is paired with a `<hash>.meta.json` sidecar [`Manifest`] that
//! makes the store self-describing — the `SQLite` index can be fully
//! reconstructed from the blobs alone. Write with
//! [`Archive::put_with_manifest`] so the sidecar is never missing; bare
//! [`Archive::put`] exists for tests. [`Archive::list_keys`] filters to
//! 64-char hex names, so sidecars are never mistaken for blobs.
//!
//! The manifest `time` (`archived_at`) is the immutable instant the bytes
//! first entered the archive. Rebuilds copy it *through* — it is not the
//! projection-build time and must never be re-stamped to "now".

mod error;
mod key;
mod manifest;
mod store;

pub use error::{Error, Result};
pub use key::{compute_blob_key, BlobKey, KeyParseError};
pub use manifest::Manifest;
pub use store::Archive;
