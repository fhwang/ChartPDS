//! Holdout regression test for aligned multi-coding tables (issue #27, cap. 2).
//!
//! PROTECTED: part of the holdout suite. A failure here is a real regression in
//! the product contract — fix `crates/**`, never edit this file or its fixture
//! to make it pass. Changes under `holdout/` require a human-signed bless commit.
//!
//! Locks the `observation_table` contract: a SINGLE call with several codings
//! returns one row per bucket with one value per coding, aligned by the store —
//! and a bucket where a coding has no data reads as an explicit `null` in that
//! coding's position, never a dropped row, a shifted column, or a fabricated 0.
//!
//! Fixture: weight on Jan 15–18 (380/400/420/440), heart rate on Jan 15–17
//! only (60/80/70). Jan 18's row must be `[440.0, null]`.

use chartpds_holdout::{fixture, Harness};

#[tokio::test]
async fn one_row_per_day_with_explicit_null_for_missing_coding() {
    let server = Harness::start().await;
    server
        .ingest_ccda(&fixture("relationship_vitals.xml"))
        .await;

    let table = server
        .call_tool(
            "observation_table",
            serde_json::json!({
                "columns": [
                    { "coding": { "system": "http://loinc.org", "code": "29463-7" } },
                    { "coding": { "system": "http://loinc.org", "code": "8867-4" } }
                ],
                "start": "2026-01-01T00:00:00Z",
                "end":   "2026-02-01T00:00:00Z",
                "bucket": "day"
            }),
        )
        .await;

    // The response names the columns in request order so values map
    // positionally without client-side bookkeeping.
    assert_eq!(table["columns"][0]["code"], "29463-7", "{table}");
    assert_eq!(table["columns"][0]["aggregate"], "mean");
    assert_eq!(table["columns"][1]["code"], "8867-4");

    let rows = table["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 4, "one row per day with any data: {table}");

    assert_eq!(rows[0]["bucket_key"], "2026-01-15");
    assert_eq!(rows[0]["values"], serde_json::json!([380.0, 60.0]));
    assert_eq!(rows[1]["bucket_key"], "2026-01-16");
    assert_eq!(rows[1]["values"], serde_json::json!([400.0, 80.0]));
    assert_eq!(rows[2]["bucket_key"], "2026-01-17");
    assert_eq!(rows[2]["values"], serde_json::json!([420.0, 70.0]));

    // Jan 18 has weight but no heart rate: the row still appears, with an
    // explicit null (not 0, not a missing element) in the heart-rate slot.
    assert_eq!(rows[3]["bucket_key"], "2026-01-18");
    assert_eq!(rows[3]["values"], serde_json::json!([440.0, null]));
}
