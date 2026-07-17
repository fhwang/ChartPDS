//! Holdout regression test for episode-based bucketing (issue #27, cap. 1).
//!
//! PROTECTED: part of the holdout suite. A failure here is a real regression in
//! the product contract — fix `crates/**`, never edit this file or its fixtures
//! to make it pass. Changes under `holdout/` require a human-signed bless commit.
//!
//! Locks the contract that `observation_duration_in_range` with
//! `bucket:"episode"` answers "minutes of deep sleep per sleep period" in one
//! call, with one row per sleep period:
//!
//! 1. A sleep period crossing a calendar-day boundary contributes to exactly
//!    ONE bucket (never split across two), keyed by the RFC 3339 UTC instant
//!    the episode began.
//! 2. `bucket:"day"` on the same data splits the midnight-crossing night —
//!    proving episode mode is a genuinely different attribution, not a
//!    relabeling of day buckets.
//!
//! Fixture: two Oura nights. Night 1 (2026-06-26 23:40Z → 06-27 00:20Z,
//! epochs "21122112") has 20 deep (`1` = AASM N3) minutes, 10 on each side of
//! UTC midnight. Night 2 (2026-06-27 23:00Z → 23:30Z, "223122") has 5.

use chartpds_holdout::Harness;

const AASM_SYSTEM: &str = "https://chartpds.fhwang.net/coding/aasm/sleep-stage";
const AASM_CODE: &str = "aasm-sleep-stage";

#[tokio::test]
async fn deep_sleep_per_sleep_period_is_one_bucket_per_episode() {
    let server = Harness::start().await;
    server.seed_archive_from_fixtures("episode_sleep_nights");

    let rebuild = server
        .call_tool("rebuild_index", serde_json::Value::Null)
        .await;
    assert_eq!(
        rebuild["oura_ingested"], 2,
        "both Oura nights must replay: {rebuild}"
    );

    // "Minutes of deep sleep per sleep period" as a single tool call: one
    // result row per sleep period, keyed by when the episode began.
    let episodes = server
        .call_tool(
            "observation_duration_in_range",
            serde_json::json!({
                "coding": { "system": AASM_SYSTEM, "code": AASM_CODE },
                "start": "2026-06-26T00:00:00Z",
                "end":   "2026-06-29T00:00:00Z",
                "value_min": 3.0, "value_max": 3.0, // AASM N3 (deep)
                "bucket": "episode"
            }),
        )
        .await;
    let rows = episodes["per_bucket"].as_array().expect("per_bucket array");
    assert_eq!(
        rows.len(),
        2,
        "exactly one bucket per sleep period: {episodes}"
    );
    assert_eq!(rows[0]["bucket_start"], "2026-06-26T23:40:00Z");
    assert_eq!(
        rows[0]["total_minutes"], 20.0,
        "the midnight-crossing night's 20 deep minutes stay whole in one \
         bucket: {episodes}"
    );
    assert_eq!(rows[1]["bucket_start"], "2026-06-27T23:00:00Z");
    assert_eq!(rows[1]["total_minutes"], 5.0);

    // Contrast: UTC day bucketing splits night 1's deep minutes across both
    // calendar days (10 + 10, with night 2's 5 joining 06-27) — the exact
    // failure mode episode bucketing exists to avoid.
    let days = server
        .call_tool(
            "observation_duration_in_range",
            serde_json::json!({
                "coding": { "system": AASM_SYSTEM, "code": AASM_CODE },
                "start": "2026-06-26T00:00:00Z",
                "end":   "2026-06-29T00:00:00Z",
                "value_min": 3.0, "value_max": 3.0,
                "bucket": "day"
            }),
        )
        .await;
    let day_rows = days["per_bucket"].as_array().expect("per_bucket array");
    assert_eq!(day_rows.len(), 2, "{days}");
    assert_eq!(day_rows[0]["bucket_start"], "2026-06-26");
    assert_eq!(day_rows[0]["total_minutes"], 10.0);
    assert_eq!(day_rows[1]["bucket_start"], "2026-06-27");
    assert_eq!(day_rows[1]["total_minutes"], 15.0);
}
