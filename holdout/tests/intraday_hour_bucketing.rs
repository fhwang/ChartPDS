//! Holdout regression test for intra-day (hour) + timezone bucketing.
//!
//! PROTECTED: part of the holdout suite. A failure here is a real regression in
//! the product contract — fix `crates/**`, never edit this file or its fixtures
//! to make it pass. Changes under `holdout/` require a human-signed bless commit.
//!
//! Locks the `ChartPDS` issue #4 capability: `observation_duration_in_range` with
//! `bucket:"hour"` + an IANA `timezone` must bucket wake epochs by LOCAL clock
//! hour (carrying the zone offset), not by UTC hour. The fixture is one Oura
//! night crossing UTC midnight, with wake epochs at NY-local 23:00 (2026-06-26)
//! and 01:00 (2026-06-27) — which are 03:00Z and 05:00Z on 2026-06-27.

use chartpds_holdout::Harness;

const AASM_SYSTEM: &str = "https://chartpds.fhwang.net/coding/aasm/sleep-stage";
const AASM_CODE: &str = "aasm-sleep-stage";

fn bucket_starts(per_bucket: &serde_json::Value) -> Vec<String> {
    per_bucket
        .as_array()
        .expect("per_bucket array")
        .iter()
        .map(|b| {
            b["bucket_start"]
                .as_str()
                .expect("bucket_start str")
                .to_string()
        })
        .collect()
}

#[tokio::test]
async fn hour_bucketing_uses_local_wall_clock_with_timezone() {
    let server = Harness::start().await;
    server.seed_archive_from_fixtures("oura_sleep_night");

    let rebuild = server
        .call_tool("rebuild_index", serde_json::Value::Null)
        .await;
    assert_eq!(
        rebuild["oura_ingested"], 1,
        "the Oura night must replay: {rebuild}"
    );

    // Local (America/New_York): wake epochs fall in the 23:00 (06-26) and
    // 01:00 (06-27) LOCAL hours, each carrying the -04:00 EDT offset.
    let local = server
        .call_tool(
            "observation_duration_in_range",
            serde_json::json!({
                "coding": { "system": AASM_SYSTEM, "code": AASM_CODE },
                "start": "2026-06-26T00:00:00-04:00",
                "end":   "2026-06-28T00:00:00-04:00",
                "value_min": 0.0, "value_max": 0.0,
                "bucket": "hour",
                "timezone": "America/New_York"
            }),
        )
        .await;
    let local_starts = bucket_starts(&local["per_bucket"]);
    assert_eq!(
        local_starts,
        vec![
            "2026-06-26T23:00:00-04:00".to_string(),
            "2026-06-27T01:00:00-04:00".to_string(),
        ],
        "hour+timezone must bucket by local wall-clock hour: {local}"
    );
    for b in local["per_bucket"].as_array().unwrap() {
        assert_eq!(
            b["total_minutes"], 5.0,
            "each wake epoch is 5 minutes: {local}"
        );
    }

    // Contrast: without timezone the same epochs bucket by UTC hour (03:00Z,
    // 05:00Z) — proving the timezone parameter actually changes the result.
    let utc = server
        .call_tool(
            "observation_duration_in_range",
            serde_json::json!({
                "coding": { "system": AASM_SYSTEM, "code": AASM_CODE },
                "start": "2026-06-26T00:00:00Z",
                "end":   "2026-06-28T00:00:00Z",
                "value_min": 0.0, "value_max": 0.0,
                "bucket": "hour"
            }),
        )
        .await;
    assert_eq!(
        bucket_starts(&utc["per_bucket"]),
        vec![
            "2026-06-27T03:00:00Z".to_string(),
            "2026-06-27T05:00:00Z".to_string(),
        ],
        "hour without timezone must bucket by UTC hour: {utc}"
    );
}
