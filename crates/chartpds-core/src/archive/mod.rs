//! Content-addressed blob archive.
//!
//! Wraps [`object_store`] with a domain layer that identifies blobs by the
//! SHA-256 hash of their content. The hash is the storage path; equal content
//! always produces equal keys (deduplication is automatic).

mod error;
mod key;
mod manifest;
mod store;

pub use error::{Error, Result};
pub use key::{compute_blob_key, BlobKey, KeyParseError};
pub use manifest::Manifest;
pub use store::Archive;
