//! Holdout regression tests for the `observation_stats` tool surface.
//!
//! PROTECTED: these tests are part of the holdout suite. A failure here is a
//! real regression in the product contract — fix `crates/**`, never edit this
//! file or its fixture to make it pass. Changes under `holdout/` require a
//! human-signed bless commit (see the design spec and `holdout.lock`).
//!
//! Locks the `ChartPDS` issue #21 capability: descriptive statistics over one
//! coding's observations. Two contracts:
//!
//! 1. The statistics themselves are exact and hand-checkable: sample sd
//!    (n−1 denominator), R type-7 percentiles (linear interpolation), and
//!    threshold counts where `n_below` is strictly-less.
//! 2. `start_time_of_day` is minutes since local NOON in the *request*
//!    timezone, honoring that zone's real UTC offset for the observation's
//!    date. This encodes a bug caught during development: 02:16Z in
//!    `America/New_York` in January is 21:16 EST (556 minutes past noon) —
//!    an implementation that applies the EDT offset year-round reports
//!    22:16 (616), and one that ignores the timezone reports 856 (UTC).

use chartpds_holdout::{fixture, Harness};

/// LOINC code for body weight.
const BODY_WEIGHT: &str = "29463-7";

/// The fixture's three weights (380 / 400 / 420) produce exact descriptive
/// statistics, and a threshold of 400 counts 380 as below (strictly-less)
/// and 400/420 as at-or-above.
#[tokio::test]
async fn value_stats_and_threshold_counts_are_exact() {
    let server = Harness::start().await;
    server
        .ingest_ccda(&fixture("observation_stats_vitals.xml"))
        .await;

    let result = server
        .call_tool(
            "observation_stats",
            serde_json::json!({
                "coding": { "system": "http://loinc.org", "code": BODY_WEIGHT },
                "start": "2026-01-01T00:00:00Z",
                "end":   "2026-02-01T00:00:00Z",
                "thresholds": [400.0]
            }),
        )
        .await;

    assert_eq!(result["count"], 3, "all three weights aggregate: {result}");
    assert_eq!(result["mean"], 400.0);
    assert_eq!(result["sd"], 20.0, "sample sd uses n-1: {result}");
    assert_eq!(result["min"], 380.0);
    assert_eq!(result["max"], 420.0);
    assert_eq!(result["p25"], 390.0, "type-7 interpolated p25: {result}");
    assert_eq!(result["p50"], 400.0);
    assert_eq!(result["p75"], 410.0, "type-7 interpolated p75: {result}");
    assert_eq!(result["confidence"], "confirmed");

    let thresholds = result["thresholds"].as_array().expect("thresholds array");
    assert_eq!(thresholds.len(), 1, "one threshold requested: {result}");
    assert_eq!(thresholds[0]["threshold"], 400.0);
    assert_eq!(
        thresholds[0]["n_below"], 1,
        "n_below is strictly-less, so 400 itself does not count: {result}"
    );
    assert_eq!(thresholds[0]["n_at_or_above"], 2);
}

/// `start_time_of_day` converts to the request timezone with that date's
/// real UTC offset: 02:16Z on a January date is 21:16 EST in
/// `America/New_York` — 556 minutes past local noon, not 616 (EDT applied
/// year-round) and not 856 (timezone ignored).
#[tokio::test]
async fn start_time_of_day_honors_request_timezone_and_dst() {
    let server = Harness::start().await;
    server
        .ingest_ccda(&fixture("observation_stats_vitals.xml"))
        .await;

    let args = |timezone: Option<&str>| {
        let mut args = serde_json::json!({
            "coding": { "system": "http://loinc.org", "code": BODY_WEIGHT },
            "start": "2026-01-01T00:00:00Z",
            "end":   "2026-02-01T00:00:00Z",
            "field": "start_time_of_day"
        });
        if let Some(tz) = timezone {
            args["timezone"] = serde_json::json!(tz);
        }
        args
    };

    // All three observations are at 02:16Z, so mean == every sample.
    let new_york = server
        .call_tool("observation_stats", args(Some("America/New_York")))
        .await;
    assert_eq!(new_york["count"], 3);
    assert_eq!(
        new_york["mean"], 556.0,
        "02:16Z in January New York is 21:16 EST = 556 min past local noon: {new_york}"
    );

    // Contrast: without a timezone the same instants are UTC 02:16 = 856
    // minutes past noon — proving the timezone parameter changes the result.
    let utc = server.call_tool("observation_stats", args(None)).await;
    assert_eq!(
        utc["mean"], 856.0,
        "default timezone is UTC, 02:16 = 856 min past noon: {utc}"
    );
}
