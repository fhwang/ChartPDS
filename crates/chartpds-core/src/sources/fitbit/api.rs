//! Google Health heart-rate API client.
//!
//! Fetches intraday heart-rate data from the Google Health v4 endpoint,
//! handling pagination and error classification.

use crate::sources;

/// A single heart-rate sample from Google Health.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HeartRateSample {
    /// ISO 8601 timestamp of the sample (e.g. `"2026-01-01T08:30:00.000Z"`).
    pub physical_time: String,
    /// Heart rate in beats per minute.
    pub beats_per_minute: i64,
}

/// Result of fetching one day's intraday heart-rate data.
#[derive(Debug, Clone)]
pub struct IntradayResult {
    /// Parsed heart-rate samples.
    pub samples: Vec<HeartRateSample>,
    /// Raw JSON pages accumulated for archiving.
    pub raw_pages: Vec<serde_json::Value>,
}

// ── Google Health response structures ────────────────────────────────

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DataPointsResponse {
    data_points: Option<Vec<DataPoint>>,
    next_page_token: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DataPoint {
    heart_rate: HeartRateData,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct HeartRateData {
    sample_time: SampleTime,
    beats_per_minute: serde_json::Value, // can be string or number
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct SampleTime {
    physical_time: String,
}

// ── Constants ────────────────────────────────────────────────────────

const BASE_URL: &str = "https://health.googleapis.com/v4/users/me/dataTypes/heart-rate/dataPoints";

// ── Public helpers ───────────────────────────────────────────────────

/// Parse a single page of the data-points response.
///
/// Returns the extracted samples and an optional next-page token.
///
/// # Errors
///
/// Returns [`sources::Error::Parse`] if the JSON doesn't match the expected
/// response shape or contains non-numeric `beatsPerMinute` values.
pub(crate) fn parse_data_points_page(
    json: &serde_json::Value,
) -> sources::Result<(Vec<HeartRateSample>, Option<String>)> {
    let page: DataPointsResponse =
        serde_json::from_value(json.clone()).map_err(|err| sources::Error::Parse {
            reason: format!("data-points response parse error: {err}"),
        })?;

    let mut samples = Vec::new();
    if let Some(data_points) = page.data_points {
        for dp in data_points {
            let bpm = coerce_bpm(&dp.heart_rate.beats_per_minute)?;
            samples.push(HeartRateSample {
                physical_time: dp.heart_rate.sample_time.physical_time,
                beats_per_minute: bpm,
            });
        }
    }

    Ok((samples, page.next_page_token))
}

/// Fetch one day's intraday heart-rate data, paging through all results.
///
/// # Errors
///
/// - [`sources::Error::ReauthRequired`] on HTTP 401.
/// - [`sources::Error::Transient`] on HTTP 429 or 5xx, or network errors.
/// - [`sources::Error::Parse`] on unexpected response shapes.
pub(crate) async fn fetch_intraday_heart_rate(
    client: &reqwest::Client,
    access_token: &str,
    date: &str,
) -> sources::Result<IntradayResult> {
    let next_date = next_day(date)?;
    let filter = format!(
        "heart_rate.sample_time.physical_time >= \"{date}T00:00:00Z\" \
         AND heart_rate.sample_time.physical_time < \"{next_date}T00:00:00Z\""
    );

    let mut all_samples = Vec::new();
    let mut raw_pages = Vec::new();
    let mut page_token: Option<String> = None;

    loop {
        let mut request = client
            .get(BASE_URL)
            .bearer_auth(access_token)
            .query(&[("filter", &filter), ("pageSize", &"10000".to_owned())]);

        if let Some(ref token) = page_token {
            request = request.query(&[("pageToken", token)]);
        }

        let response = request
            .send()
            .await
            .map_err(|err| sources::Error::Transient {
                reason: format!("heart-rate fetch network error: {err}"),
            })?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(classify_http_error(status.as_u16(), &body));
        }

        let page_json: serde_json::Value =
            response.json().await.map_err(|err| sources::Error::Parse {
                reason: format!("heart-rate response JSON error: {err}"),
            })?;

        let (samples, next_token) = parse_data_points_page(&page_json)?;
        all_samples.extend(samples);
        raw_pages.push(page_json);

        match next_token {
            Some(t) if !t.is_empty() => page_token = Some(t),
            _ => break,
        }
    }

    Ok(IntradayResult {
        samples: all_samples,
        raw_pages,
    })
}

// ── Private helpers ──────────────────────────────────────────────────

/// Increment a `YYYY-MM-DD` date by one day.
fn next_day(date: &str) -> sources::Result<String> {
    let format = time::macros::format_description!("[year]-[month]-[day]");
    let d = time::Date::parse(date, &format).map_err(|err| sources::Error::Parse {
        reason: format!("invalid date {date:?}: {err}"),
    })?;
    let next = d.next_day().ok_or_else(|| sources::Error::Parse {
        reason: format!("cannot compute next day for {date:?}"),
    })?;
    // Format manually to avoid requiring the `formatting` feature.
    let (y, m, day) = (next.year(), next.month() as u8, next.day());
    Ok(format!("{y:04}-{m:02}-{day:02}"))
}

/// Coerce a `beatsPerMinute` JSON value (string or number) to `i64`.
fn coerce_bpm(value: &serde_json::Value) -> sources::Result<i64> {
    if let Some(n) = value.as_i64() {
        return Ok(n);
    }
    if let Some(s) = value.as_str() {
        return s.parse::<i64>().map_err(|err| sources::Error::Parse {
            reason: format!("beatsPerMinute string not numeric: {err}"),
        });
    }
    // Handle float numbers (e.g. 72.0)
    if let Some(f) = value.as_f64() {
        #[allow(
            clippy::cast_possible_truncation,
            reason = "BPM is always a small integer"
        )]
        return Ok(f as i64);
    }
    Err(sources::Error::Parse {
        reason: format!("beatsPerMinute is neither string nor number: {value}"),
    })
}

/// Classify an HTTP error from the data-points endpoint.
fn classify_http_error(status: u16, body: &str) -> sources::Error {
    if status == 401 {
        return sources::Error::ReauthRequired {
            reason: format!("heart-rate endpoint returned {status}: {body}"),
        };
    }
    if status == 429 || status >= 500 {
        return sources::Error::Transient {
            reason: format!("heart-rate endpoint returned {status}: {body}"),
        };
    }
    sources::Error::Parse {
        reason: format!("heart-rate endpoint returned {status}: {body}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_page_response() {
        let json: serde_json::Value = serde_json::json!({
            "dataPoints": [
                {
                    "heartRate": {
                        "sampleTime": { "physicalTime": "2026-01-01T08:30:00.000Z" },
                        "beatsPerMinute": 72
                    }
                },
                {
                    "heartRate": {
                        "sampleTime": { "physicalTime": "2026-01-01T08:31:00.000Z" },
                        "beatsPerMinute": 75
                    }
                }
            ]
        });

        let (samples, next_token) = parse_data_points_page(&json).expect("parse");
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].physical_time, "2026-01-01T08:30:00.000Z");
        assert_eq!(samples[0].beats_per_minute, 72);
        assert_eq!(samples[1].physical_time, "2026-01-01T08:31:00.000Z");
        assert_eq!(samples[1].beats_per_minute, 75);
        assert!(next_token.is_none());
    }

    #[test]
    fn parse_empty_response() {
        let json: serde_json::Value = serde_json::json!({});

        let (samples, next_token) = parse_data_points_page(&json).expect("parse");
        assert!(samples.is_empty());
        assert!(next_token.is_none());
    }

    #[test]
    fn beats_per_minute_handles_string_and_number() {
        let json: serde_json::Value = serde_json::json!({
            "dataPoints": [
                {
                    "heartRate": {
                        "sampleTime": { "physicalTime": "2026-01-01T08:30:00.000Z" },
                        "beatsPerMinute": "72"
                    }
                },
                {
                    "heartRate": {
                        "sampleTime": { "physicalTime": "2026-01-01T08:31:00.000Z" },
                        "beatsPerMinute": 85
                    }
                }
            ]
        });

        let (samples, _) = parse_data_points_page(&json).expect("parse");
        assert_eq!(samples[0].beats_per_minute, 72);
        assert_eq!(samples[1].beats_per_minute, 85);
    }

    #[test]
    fn next_day_increments_date() {
        assert_eq!(next_day("2026-01-01").unwrap(), "2026-01-02");
        assert_eq!(next_day("2026-01-31").unwrap(), "2026-02-01");
        assert_eq!(next_day("2026-12-31").unwrap(), "2027-01-01");
    }

    #[test]
    fn next_day_rejects_invalid_date() {
        assert!(next_day("not-a-date").is_err());
    }

    #[test]
    fn classify_401_as_reauth_required() {
        let err = classify_http_error(401, "unauthorized");
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
    fn classify_403_as_parse() {
        let err = classify_http_error(403, "forbidden");
        assert!(matches!(err, sources::Error::Parse { .. }));
    }
}
