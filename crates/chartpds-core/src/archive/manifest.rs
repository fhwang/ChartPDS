//! Sidecar manifest describing an archived blob.
//!
//! Each blob `<hash>` is paired with a `<hash>.meta.json` sidecar that records
//! the provenance and type of the bytes. The format follows the
//! [`CloudEvents` v1.0](https://github.com/cloudevents/spec) context-attribute
//! model in *binary content mode*: the blob stays the raw, untouched payload
//! (CCDA XML, Fitbit/Oura JSON) while this manifest carries the attributes
//! needed to interpret and replay it.
//!
//! Keeping all provenance beside the bytes makes the archive self-describing:
//! the `SQLite` index can be fully reconstructed from the archive alone (see
//! [`crate::ingestion::rebuild_index`]).

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// The `CloudEvents` spec version this manifest conforms to.
const SPEC_VERSION: &str = "1.0";

/// Provenance manifest for an archived blob, modelled on `CloudEvents` v1.0.
///
/// Field names mirror `CloudEvents` context attributes (`type`, `time`,
/// `subject`, ...) on the wire; internal field names are domain-friendly
/// `snake_case` with `#[serde(rename)]` bridging the two.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// `CloudEvents` `specversion` — always [`SPEC_VERSION`].
    #[serde(rename = "specversion")]
    pub spec_version: String,

    /// `CloudEvents` `id`. Set to the blob's content hash by
    /// [`crate::archive::Archive::put_with_manifest`]; `source` + `id` is unique.
    pub id: String,

    /// `CloudEvents` `source` — the producing adapter (`"ccda"`, `"fitbit"`,
    /// `"oura"`). Mirrors `source_documents.source`.
    pub source: String,

    /// `CloudEvents` `type` — the document kind (`"ccda"`,
    /// `"fitbit-intraday-hr-day"`, `"oura-sleep-session"`). Drives rebuild
    /// dispatch. Mirrors `source_documents.kind`.
    #[serde(rename = "type")]
    pub kind: String,

    /// `CloudEvents` `datacontenttype` — media type of the blob
    /// (`"application/xml"` for CCDA, `"application/json"` for adapters).
    #[serde(rename = "datacontenttype")]
    pub data_content_type: String,

    /// `CloudEvents` `subject` — the per-document subject within the producer's
    /// context. Carries the replay "date" (Fitbit day, Oura sleep day) so no
    /// adapter needs a custom blob envelope.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub subject: Option<String>,

    /// `CloudEvents` `time` — when the bytes first entered the archive. Immutable;
    /// copied through on rebuild. Becomes `source_documents.archived_at`.
    #[serde(rename = "time", with = "time::serde::rfc3339")]
    pub archived_at: OffsetDateTime,

    /// Extension attribute: the original upload filename, where one exists.
    #[serde(
        rename = "originalfilename",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub original_filename: Option<String>,
}

impl Manifest {
    /// Build a manifest for a blob about to be archived.
    ///
    /// `id` is left empty here and assigned the content hash by
    /// [`crate::archive::Archive::put_with_manifest`], so callers never need to
    /// hash the content themselves.
    #[must_use]
    pub fn new(
        source: impl Into<String>,
        kind: impl Into<String>,
        data_content_type: impl Into<String>,
        subject: Option<String>,
        archived_at: OffsetDateTime,
        original_filename: Option<String>,
    ) -> Self {
        Self {
            spec_version: SPEC_VERSION.to_owned(),
            id: String::new(),
            source: source.into(),
            kind: kind.into(),
            data_content_type: data_content_type.into(),
            subject,
            archived_at,
            original_filename,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn manifest_serializes_with_cloudevents_attribute_names() {
        let m = Manifest::new(
            "fitbit",
            "fitbit-intraday-hr-day",
            "application/json",
            Some("2026-01-15".to_owned()),
            datetime!(2026-01-15 03:12:00 UTC),
            Some("fitbit-hr-2026-01-15.json".to_owned()),
        );
        let json: serde_json::Value = serde_json::to_value(&m).expect("serialize");

        assert_eq!(json["specversion"], "1.0");
        assert_eq!(json["type"], "fitbit-intraday-hr-day");
        assert_eq!(json["datacontenttype"], "application/json");
        assert_eq!(json["subject"], "2026-01-15");
        assert_eq!(json["time"], "2026-01-15T03:12:00Z");
        assert_eq!(json["originalfilename"], "fitbit-hr-2026-01-15.json");
    }

    #[test]
    fn manifest_round_trips_through_json() {
        let m = Manifest::new(
            "oura",
            "oura-sleep-session",
            "application/json",
            Some("2026-01-15".to_owned()),
            datetime!(2026-01-15 03:12:00 UTC),
            None,
        );
        let bytes = serde_json::to_vec(&m).expect("serialize");
        let back: Manifest = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(m, back);
    }

    #[test]
    fn optional_fields_omitted_when_absent() {
        let m = Manifest::new(
            "ccda",
            "ccda",
            "application/xml",
            None,
            datetime!(2026-01-15 03:12:00 UTC),
            None,
        );
        let json: serde_json::Value = serde_json::to_value(&m).expect("serialize");
        assert!(json.get("subject").is_none());
        assert!(json.get("originalfilename").is_none());
    }
}
