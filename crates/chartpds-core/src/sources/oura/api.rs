//! Oura v2 sleep API client.
//!
//! Fetches sleep sessions from the Oura personal API. Each session
//! includes a `sleep_phase_5_min` string that encodes per-epoch
//! (5-minute) sleep stages.

use crate::sources;

/// Oura v2 sleep endpoint.
const SLEEP_URL: &str = "https://api.ouraring.com/v2/usercollection/sleep";

/// A single sleep session from the Oura API.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OuraSleepSession {
    /// Oura-assigned session identifier.
    pub id: String,
    /// Sleep date in `YYYY-MM-DD` format (Oura's "sleep day", usually
    /// the morning after bedtime).
    pub day: String,
    /// ISO 8601 timestamp when the user went to bed.
    pub bedtime_start: String,
    /// ISO 8601 timestamp when the user got out of bed.
    pub bedtime_end: String,
    /// Session type (`"long_sleep"`, `"late_nap"`, etc.).
    #[serde(rename = "type")]
    pub session_type: String,
    /// Per-epoch (5-minute) sleep-stage string. Each char encodes one
    /// epoch: `'1'` = deep, `'2'` = light, `'3'` = REM, `'4'` = awake.
    pub sleep_phase_5_min: String,
    /// Total sleep duration in seconds.
    pub total_sleep_duration: Option<i64>,
    /// REM sleep duration in seconds.
    pub rem_sleep_duration: Option<i64>,
    /// Deep sleep duration in seconds.
    pub deep_sleep_duration: Option<i64>,
    /// Light sleep duration in seconds.
    pub light_sleep_duration: Option<i64>,
}

/// Result of fetching sleep sessions for a date range.
#[derive(Debug, Clone)]
pub struct OuraSleepFetchResult {
    /// Parsed sleep sessions.
    pub sessions: Vec<OuraSleepSession>,
    /// Raw JSON response for archiving.
    pub raw: serde_json::Value,
}

/// Oura v2 sleep endpoint response shape.
#[derive(serde::Deserialize)]
struct SleepResponse {
    data: Vec<OuraSleepSession>,
}

/// Fetch sleep sessions for a date range from the Oura v2 API.
///
/// # Arguments
///
/// * `client` - HTTP client to use for the request.
/// * `access_token` - Oura personal access token (PAT).
/// * `start_date` - Inclusive start date (`YYYY-MM-DD`).
/// * `end_date` - Inclusive end date (`YYYY-MM-DD`).
///
/// # Errors
///
/// - [`sources::Error::ReauthRequired`] on HTTP 401 or 403.
/// - [`sources::Error::Transient`] on HTTP 429 or 5xx, or network errors.
/// - [`sources::Error::Parse`] on unexpected response shapes.
pub async fn fetch_sleep(
    client: &reqwest::Client,
    access_token: &str,
    start_date: &str,
    end_date: &str,
) -> sources::Result<OuraSleepFetchResult> {
    let response = client
        .get(SLEEP_URL)
        .bearer_auth(access_token)
        .query(&[("start_date", start_date), ("end_date", end_date)])
        .send()
        .await
        .map_err(|err| sources::Error::Transient {
            reason: format!("oura sleep fetch network error: {err}"),
        })?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(classify_http_error(status.as_u16(), &body));
    }

    let raw: serde_json::Value = response.json().await.map_err(|err| sources::Error::Parse {
        reason: format!("oura sleep response JSON error: {err}"),
    })?;

    let parsed: SleepResponse =
        serde_json::from_value(raw.clone()).map_err(|err| sources::Error::Parse {
            reason: format!("oura sleep response parse error: {err}"),
        })?;

    Ok(OuraSleepFetchResult {
        sessions: parsed.data,
        raw,
    })
}

/// Parse a raw JSON value into sleep sessions.
///
/// Used by tests to validate parsing without making HTTP calls.
///
/// # Errors
///
/// Returns [`sources::Error::Parse`] if the JSON doesn't match the
/// expected response shape.
pub fn parse_sleep_response(raw: &serde_json::Value) -> sources::Result<Vec<OuraSleepSession>> {
    let parsed: SleepResponse =
        serde_json::from_value(raw.clone()).map_err(|err| sources::Error::Parse {
            reason: format!("oura sleep response parse error: {err}"),
        })?;
    Ok(parsed.data)
}

/// Classify an HTTP error from the Oura sleep endpoint.
fn classify_http_error(status: u16, body: &str) -> sources::Error {
    if status == 401 || status == 403 {
        return sources::Error::ReauthRequired {
            reason: format!("oura sleep endpoint returned {status}: {body}"),
        };
    }
    if status == 429 || status >= 500 {
        return sources::Error::Transient {
            reason: format!("oura sleep endpoint returned {status}: {body}"),
        };
    }
    sources::Error::Parse {
        reason: format!("oura sleep endpoint returned {status}: {body}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sleep_response_with_sessions() {
        let json = serde_json::json!({
            "data": [
                {
                    "id": "abc123",
                    "day": "2026-01-15",
                    "bedtime_start": "2026-01-14T22:30:00-05:00",
                    "bedtime_end": "2026-01-15T06:30:00-05:00",
                    "type": "long_sleep",
                    "sleep_phase_5_min": "4421133244",
                    "total_sleep_duration": 28800,
                    "rem_sleep_duration": 7200,
                    "deep_sleep_duration": 5400,
                    "light_sleep_duration": 12600
                }
            ]
        });

        let sessions = parse_sleep_response(&json).expect("parse");
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, "abc123");
        assert_eq!(sessions[0].day, "2026-01-15");
        assert_eq!(sessions[0].session_type, "long_sleep");
        assert_eq!(sessions[0].sleep_phase_5_min, "4421133244");
        assert_eq!(sessions[0].total_sleep_duration, Some(28800));
    }

    #[test]
    fn parse_sleep_response_empty_data() {
        let json = serde_json::json!({ "data": [] });
        let sessions = parse_sleep_response(&json).expect("parse");
        assert!(sessions.is_empty());
    }

    #[test]
    fn parse_sleep_response_with_null_durations() {
        let json = serde_json::json!({
            "data": [
                {
                    "id": "xyz",
                    "day": "2026-01-15",
                    "bedtime_start": "2026-01-14T23:00:00Z",
                    "bedtime_end": "2026-01-15T07:00:00Z",
                    "type": "late_nap",
                    "sleep_phase_5_min": "42",
                    "total_sleep_duration": null,
                    "rem_sleep_duration": null,
                    "deep_sleep_duration": null,
                    "light_sleep_duration": null
                }
            ]
        });

        let sessions = parse_sleep_response(&json).expect("parse");
        assert_eq!(sessions.len(), 1);
        assert!(sessions[0].total_sleep_duration.is_none());
        assert!(sessions[0].rem_sleep_duration.is_none());
    }

    #[test]
    fn classify_401_as_reauth_required() {
        let err = classify_http_error(401, "unauthorized");
        assert!(matches!(err, sources::Error::ReauthRequired { .. }));
    }

    #[test]
    fn classify_403_as_reauth_required() {
        let err = classify_http_error(403, "forbidden");
        assert!(matches!(err, sources::Error::ReauthRequired { .. }));
    }

    #[test]
    fn classify_429_as_transient() {
        let err = classify_http_error(429, "too many requests");
        assert!(matches!(err, sources::Error::Transient { .. }));
    }

    #[test]
    fn classify_500_as_transient() {
        let err = classify_http_error(500, "internal server error");
        assert!(matches!(err, sources::Error::Transient { .. }));
    }

    #[test]
    fn classify_400_as_parse() {
        let err = classify_http_error(400, "bad request");
        assert!(matches!(err, sources::Error::Parse { .. }));
    }
}
