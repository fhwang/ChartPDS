//! Holdout regression test for Spearman rank correlation in
//! `observation_relationship` (issue #27 follow-up).
//!
//! PROTECTED: part of the holdout suite. A failure here is a real regression in
//! the product contract — fix `crates/**`, never edit this file or its fixture
//! to make it pass. Changes under `holdout/` require a human-signed bless commit.
//!
//! Locks that `spearman_r` is genuinely rank-based, not an alias of
//! `pearson_r`. The fixture's weight (380/400/420/440) and heart rate
//! (60/61/63/80) rise together monotonically but not linearly: the ranks
//! agree exactly, so Spearman ρ must be 1.0 exactly, while Pearson on the
//! raw values is 620/√(2000·266) ≈ 0.85. Both must be reported in the
//! same response — the client compares them to detect outliers or curved
//! relationships without a second call.

use chartpds_holdout::{fixture, Harness};

#[tokio::test]
async fn spearman_is_rank_based_and_reported_alongside_pearson() {
    let server = Harness::start().await;
    server.ingest_ccda(&fixture("spearman_vitals.xml")).await;

    let result = server
        .call_tool(
            "observation_relationship",
            serde_json::json!({
                "x": { "coding": { "system": "http://loinc.org", "code": "29463-7" } },
                "y": { "coding": { "system": "http://loinc.org", "code": "8867-4" } },
                "start": "2026-01-01T00:00:00Z",
                "end":   "2026-02-01T00:00:00Z",
                "bucket": "day"
            }),
        )
        .await;

    assert_eq!(result["n_pairs"], 4, "{result}");
    assert_eq!(
        result["spearman_r"], 1.0,
        "monotonic series: ranks agree exactly, ρ must be 1.0: {result}"
    );
    let pearson = result["pearson_r"]
        .as_f64()
        .unwrap_or_else(|| panic!("pearson_r must be a number: {result}"));
    assert!(
        pearson > 0.8 && pearson < 0.9,
        "raw Pearson is ~0.85 here; spearman_r equal to it (or to 1.0 \
         alongside a pearson_r of 1.0) means the rank transform is gone: \
         {result}"
    );
}
