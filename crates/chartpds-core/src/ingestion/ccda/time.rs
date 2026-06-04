//! Parse HL7 v3 timestamp strings.
//!
//! HL7 v3 TS format: `YYYYMMDDHHmmss±zzzz` (full) or `YYYYMMDD` (date only).
//! No separators between components. Time-zone offset is signed 4 digits
//! without a colon (`+0000`, `-0500`).

use crate::ingestion::{Error, Result};
use time::OffsetDateTime;

/// Parse an HL7 v3 timestamp.
///
/// Accepts two forms:
/// - Full: `YYYYMMDDHHmmss±zzzz` (e.g. `20260101120000+0000`).
/// - Date only: `YYYYMMDD` (e.g. `20260101`). Treated as UTC midnight.
///
/// # Errors
///
/// Returns [`Error::NotCcda`] if the input doesn't match either form.
pub(crate) fn parse_hl7_timestamp(s: &str) -> Result<OffsetDateTime> {
    match s.len() {
        8 => parse_date_only(s),
        19 => parse_full(s),
        other => Err(Error::NotCcda {
            reason: format!("malformed HL7 timestamp {s:?}: expected 8 or 19 chars, got {other}"),
        }),
    }
}

fn parse_date_only(s: &str) -> Result<OffsetDateTime> {
    let format = time::macros::format_description!("[year][month][day]");
    let date = time::Date::parse(s, &format).map_err(|err| Error::NotCcda {
        reason: format!("malformed HL7 date {s:?}: {err}"),
    })?;
    Ok(date.midnight().assume_utc())
}

fn parse_full(s: &str) -> Result<OffsetDateTime> {
    let format = time::macros::format_description!(
        "[year][month][day][hour][minute][second][offset_hour sign:mandatory][offset_minute]"
    );
    OffsetDateTime::parse(s, &format).map_err(|err| Error::NotCcda {
        reason: format!("malformed HL7 timestamp {s:?}: {err}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn parse_full_timestamp_with_tz() {
        let ts = parse_hl7_timestamp("20260101120000+0000").expect("parse");
        assert_eq!(ts, datetime!(2026-01-01 12:00:00 UTC));
    }

    #[test]
    fn parse_full_timestamp_with_positive_offset() {
        let ts = parse_hl7_timestamp("20260101120000+0500").expect("parse");
        assert_eq!(ts, datetime!(2026-01-01 12:00:00 +05:00));
    }

    #[test]
    fn parse_full_timestamp_with_negative_offset() {
        let ts = parse_hl7_timestamp("20260101120000-0800").expect("parse");
        assert_eq!(ts, datetime!(2026-01-01 12:00:00 -08:00));
    }

    #[test]
    fn parse_date_only_assumes_utc_midnight() {
        let ts = parse_hl7_timestamp("20260101").expect("parse");
        assert_eq!(ts, datetime!(2026-01-01 0:00:00 UTC));
    }

    #[test]
    fn parse_rejects_empty_string() {
        assert!(parse_hl7_timestamp("").is_err());
    }

    #[test]
    fn parse_rejects_short_string() {
        assert!(parse_hl7_timestamp("2026").is_err());
    }

    #[test]
    fn parse_rejects_malformed_offset() {
        assert!(parse_hl7_timestamp("20260101120000ZZZZ").is_err());
    }
}
