//! Holdout regression test: recent/unsettled Fitbit data must be reported as
//! `provisional`, never silently as settled fact.
//!
//! PROTECTED: part of the holdout suite. A failure here is a real regression in
//! the product contract — fix `crates/**`, never edit this file or its fixtures
//! to make it pass. Changes under `holdout/` require a human-signed bless commit
//! (see the design spec and `holdout.lock`).
//!
//! Gap being guarded (issue #15): `ChartPDS` computes per-day confidence but did
//! not surface it, so a consumer querying recent data could not tell an
//! incomplete day from a settled one. After `index_rebuild` (no live frontier),
//! every Fitbit day is unsettled — `observation_history` must tag its rows
//! `confidence: "provisional"`.

use chartpds_holdout::Harness;

/// LOINC code for heart rate.
const HEART_RATE: &str = "8867-4";

/// Replaying a Fitbit blob leaves every day unsettled (no live freshness
/// frontier after `index_rebuild`), so `observation_history` must report
/// `confidence: "provisional"` for every row — never silently `"confirmed"`.
#[tokio::test]
async fn fitbit_history_reports_provisional_confidence() {
    let server = Harness::start().await;

    server.seed_archive_from_fixtures("fitbit_confidence");
    let rebuild = server
        .call_tool("index_rebuild", serde_json::Value::Null)
        .await;
    assert!(
        rebuild["fitbit_ingested"].as_i64().unwrap_or(0) >= 1,
        "fitbit blob should replay: {rebuild}"
    );

    let history = server
        .call_tool(
            "observation_history",
            serde_json::json!({
                "codings": [{ "system": "http://loinc.org", "code": HEART_RATE }]
            }),
        )
        .await;
    let rows = history["items"].as_array().expect("items array");
    assert!(!rows.is_empty(), "expected some HR rows: {history}");
    for row in rows {
        assert_eq!(
            row["confidence"], "provisional",
            "recent Fitbit data must be reported provisional: {row}"
        );
    }
}
