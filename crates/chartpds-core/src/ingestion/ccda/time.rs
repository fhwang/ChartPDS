//! Parse HL7 v3 timestamp strings.
//!
//! HL7 v3 TS format: `YYYYMMDDHHmmss±zzzz` (full), `YYYYMMDDHHmmss`
//! (datetime, no offset) or `YYYYMMDD` (date only). No separators between
//! components. Time-zone offset is signed 4 digits without a colon
//! (`+0000`, `-0500`).

use crate::ingestion::{Error, Result};
use time::{OffsetDateTime, PrimitiveDateTime};

/// Parse an HL7 v3 timestamp.
///
/// Accepts three forms:
/// - Full: `YYYYMMDDHHmmss±zzzz` (e.g. `20260101120000+0000`).
/// - Datetime without offset: `YYYYMMDDHHmmss` (e.g. `20250806040200`).
///   Treated as UTC, since HL7 leaves an absent offset implementation-defined
///   and the lab feeds in this archive emit UTC instants. Lab `effectiveTime`
///   values routinely omit the offset, unlike the vital-signs feed.
/// - Date only: `YYYYMMDD` (e.g. `20260101`). Treated as UTC midnight.
///
/// # Errors
///
/// Returns [`Error::NotCcda`] if the input doesn't match any of these forms.
pub(crate) fn parse_hl7_timestamp(s: &str) -> Result<OffsetDateTime> {
    match s.len() {
        8 => parse_date_only(s),
        14 => parse_datetime_no_offset(s),
        19 => parse_full(s),
        other => Err(Error::NotCcda {
            reason: format!(
                "malformed HL7 timestamp {s:?}: expected 8, 14 or 19 chars, got {other}"
            ),
        }),
    }
}

fn parse_datetime_no_offset(s: &str) -> Result<OffsetDateTime> {
    let format = time::macros::format_description!("[year][month][day][hour][minute][second]");
    let dt = PrimitiveDateTime::parse(s, &format).map_err(|err| Error::NotCcda {
        reason: format!("malformed HL7 datetime {s:?}: {err}"),
    })?;
    Ok(dt.assume_utc())
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
    fn parse_datetime_without_offset_assumes_utc() {
        // Lab effectiveTime values omit the timezone offset; HL7 leaves an
        // absent offset implementation-defined and we treat it as UTC.
        let ts = parse_hl7_timestamp("20250806040200").expect("parse");
        assert_eq!(ts, datetime!(2025-08-06 04:02:00 UTC));
    }

    #[test]
    fn parse_rejects_malformed_datetime_no_offset() {
        // Right length (14) but not a valid datetime.
        assert!(parse_hl7_timestamp("2025XX06040200").is_err());
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
