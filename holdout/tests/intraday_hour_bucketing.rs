//! Holdout regression test for intra-day (hour) + timezone bucketing.
//!
//! PROTECTED: part of the holdout suite. A failure here is a real regression in
//! the product contract — fix `crates/**`, never edit this file or its fixtures
//! to make it pass. Changes under `holdout/` require a human-signed bless commit.
//!
//! Locks the `ChartPDS` issue #4 capability: `observation_table` with a
//! `duration_in_range` column, `bucket:"hour"` + an IANA `timezone` must
//! bucket wake epochs by LOCAL clock hour (carrying the zone offset), not by
//! UTC hour. The fixture is one Oura night (22:00–02:00 EDT, so four local
//! hours of sleep-stage data) crossing UTC midnight, with wake epochs at
//! NY-local 23:00 (2026-06-26) and 01:00 (2026-06-27) — which are 03:00Z and
//! 05:00Z on 2026-06-27. An hour with sleep data but no wake minutes reads
//! as an explicit 0.0, not a dropped row.

use chartpds_holdout::Harness;

const AASM_SYSTEM: &str = "https://chartpds.fhwang.net/coding/aasm/sleep-stage";
const AASM_CODE: &str = "aasm-sleep-stage";

/// Flatten a table result to `(bucket_key, wake_minutes)` pairs.
fn hour_rows(table: &serde_json::Value) -> Vec<(String, f64)> {
    table["rows"]
        .as_array()
        .expect("rows array")
        .iter()
        .map(|b| {
            (
                b["bucket_key"]
                    .as_str()
                    .expect("bucket_key str")
                    .to_string(),
                b["values"][0].as_f64().expect("wake minutes"),
            )
        })
        .collect()
}

#[tokio::test]
async fn hour_bucketing_uses_local_wall_clock_with_timezone() {
    let server = Harness::start().await;
    server.seed_archive_from_fixtures("oura_sleep_night");

    let rebuild = server
        .call_tool("index_rebuild", serde_json::Value::Null)
        .await;
    assert_eq!(
        rebuild["oura_ingested"], 1,
        "the Oura night must replay: {rebuild}"
    );

    // Local (America/New_York): wake epochs fall in the 23:00 (06-26) and
    // 01:00 (06-27) LOCAL hours, each carrying the -04:00 EDT offset.
    let local = server
        .call_tool(
            "observation_table",
            serde_json::json!({
                "columns": [{
                    "coding": { "system": AASM_SYSTEM, "code": AASM_CODE },
                    "aggregate": "duration_in_range",
                    "value_min": 0.0, "value_max": 0.0
                }],
                "start": "2026-06-26T00:00:00-04:00",
                "end":   "2026-06-28T00:00:00-04:00",
                "bucket": "hour",
                "timezone": "America/New_York"
            }),
        )
        .await;
    assert_eq!(
        hour_rows(&local),
        vec![
            ("2026-06-26T22:00:00-04:00".to_string(), 0.0),
            ("2026-06-26T23:00:00-04:00".to_string(), 5.0),
            ("2026-06-27T00:00:00-04:00".to_string(), 0.0),
            ("2026-06-27T01:00:00-04:00".to_string(), 5.0),
        ],
        "hour+timezone must bucket by local wall-clock hour, with each \
         5-minute wake epoch in its local hour and explicit 0.0 for slept \
         hours: {local}"
    );

    // Contrast: without timezone the same epochs bucket by UTC hour (03:00Z,
    // 05:00Z) — proving the timezone parameter actually changes the result.
    let utc = server
        .call_tool(
            "observation_table",
            serde_json::json!({
                "columns": [{
                    "coding": { "system": AASM_SYSTEM, "code": AASM_CODE },
                    "aggregate": "duration_in_range",
                    "value_min": 0.0, "value_max": 0.0
                }],
                "start": "2026-06-26T00:00:00Z",
                "end":   "2026-06-28T00:00:00Z",
                "bucket": "hour"
            }),
        )
        .await;
    assert_eq!(
        hour_rows(&utc),
        vec![
            ("2026-06-27T02:00:00Z".to_string(), 0.0),
            ("2026-06-27T03:00:00Z".to_string(), 5.0),
            ("2026-06-27T04:00:00Z".to_string(), 0.0),
            ("2026-06-27T05:00:00Z".to_string(), 5.0),
        ],
        "hour without timezone must bucket by UTC hour: {utc}"
    );
}
