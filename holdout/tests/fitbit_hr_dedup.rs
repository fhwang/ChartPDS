//! Holdout regression test for Fitbit heart-rate cross-document duplication.
//!
//! PROTECTED: part of the holdout suite. A failure here is a real regression in
//! the product contract — fix `crates/**`, never edit this file or its fixtures
//! to make it pass. Changes under `holdout/` require a human-signed bless commit
//! (see the design spec and `holdout.lock`).
//!
//! The bug (diagnosed externally via the MCP surface): every Fitbit sync
//! archived a fresh blob for an overlapping day, and `ChartPDS` keyed dedup only
//! on the blob's content hash. As intraday HR grew through the day each pull's
//! bytes differed, so the hash differed, so a new `source_document` was inserted
//! and the whole day's observations were re-inserted — once per overlapping
//! pull. Aggregates over LOINC 8867-4 inflated by the duplicate-document count
//! (observed up to 41x), making the weekly MVPA metric uncomputable.
//!
//! Reproduction through the black-box surface: plant two overlapping Fitbit HR
//! blobs for the same day (a 2-sample pull archived earlier, a 3-sample pull
//! archived later) into the archive, then `rebuild_index` to replay them. The
//! day must collapse to the newest pull's samples — not the sum across both
//! documents.

use chartpds_holdout::Harness;

/// LOINC code for heart rate.
const HEART_RATE: &str = "8867-4";

/// Replaying two overlapping pulls of one Fitbit day yields exactly the newest
/// pull's observations — not a duplicated copy per source document.
#[tokio::test]
async fn fitbit_reingested_day_does_not_duplicate_heart_rate() {
    let server = Harness::start().await;

    // Two overlapping pulls of 2026-06-22 land as two distinct archive blobs.
    server.seed_archive_from_fixtures("fitbit_hr_dup");

    // Replay the archive into the index.
    let rebuild = server
        .call_tool("rebuild_index", serde_json::Value::Null)
        .await;
    assert_eq!(
        rebuild["fitbit_ingested"], 2,
        "both Fitbit blobs should be replayed: {rebuild}"
    );

    // get_observation_history must return only the newest pull's three samples,
    // each at a distinct timestamp — not 2 + 3 = 5 rows with repeated timestamps.
    let history = server
        .call_tool(
            "get_observation_history",
            serde_json::json!({
                "codings": [{ "system": "http://loinc.org", "code": HEART_RATE }]
            }),
        )
        .await;
    let rows = history.as_array().expect("history array");
    assert_eq!(
        rows.len(),
        3,
        "re-pulled day must not duplicate observations across documents: {history}"
    );
    // effective_start serializes as a JSON array ([year, ordinal, hour, ...]);
    // compare the raw values, not as strings.
    let mut starts: Vec<String> = rows
        .iter()
        .map(|r| r["effective_start"].to_string())
        .collect();
    starts.sort_unstable();
    starts.dedup();
    assert_eq!(
        starts.len(),
        3,
        "every observation must have a distinct effective_start: {history}"
    );

    // observation_counts — the tool the bug report showed reporting millions of
    // phantom samples — must report the true count for 8867-4.
    let counts = server
        .call_tool("observation_counts", serde_json::Value::Null)
        .await;
    let entry = counts
        .as_array()
        .expect("counts array")
        .iter()
        .find(|c| c["coding_code"] == HEART_RATE)
        .unwrap_or_else(|| panic!("no 8867-4 entry in counts: {counts}"));
    assert_eq!(
        entry["count"], 3,
        "observation_counts must not inflate by the duplicate-document factor: {counts}"
    );
}
