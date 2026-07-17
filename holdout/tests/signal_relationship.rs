//! Holdout regression test for two-signal relationships (issue #27, cap. 3).
//!
//! PROTECTED: part of the holdout suite. A failure here is a real regression in
//! the product contract — fix `crates/**`, never edit this file or its fixture
//! to make it pass. Changes under `holdout/` require a human-signed bless commit.
//!
//! Locks the `observation_relationship` contract with exact, hand-checkable
//! numbers:
//!
//! 1. Same-day pairing over weight (380/400/420) vs. heart rate (60/80/70):
//!    deviations (−20,0,20) × (−10,10,0) → Pearson r = 200/√(800·200) = 0.5
//!    exactly, and `n_pairs` = 3 because Jan 18 (weight, no heart rate) is
//!    excluded — the sample size must reflect only complete pairs.
//! 2. `lag: 1` pairs each x with the FOLLOWING day's y — (380,80), (400,70) —
//!    r = −1 exactly with `n_pairs` = 2 (unmatched edge days dropped).
//! 3. A `threshold` on x returns y statistics per group, with the
//!    strictly-below split: x = 400 itself lands in `x_at_or_above`.

use chartpds_holdout::{fixture, Harness};

fn args() -> serde_json::Value {
    serde_json::json!({
        "x": { "coding": { "system": "http://loinc.org", "code": "29463-7" } },
        "y": { "coding": { "system": "http://loinc.org", "code": "8867-4" } },
        "start": "2026-01-01T00:00:00Z",
        "end":   "2026-02-01T00:00:00Z",
        "bucket": "day"
    })
}

#[tokio::test]
async fn same_day_pairs_give_exact_pearson_r_and_exclude_incomplete_days() {
    let server = Harness::start().await;
    server
        .ingest_ccda(&fixture("relationship_vitals.xml"))
        .await;

    let result = server.call_tool("observation_relationship", args()).await;
    assert_eq!(
        result["n_pairs"], 3,
        "Jan 18 has no heart rate; the pair count must reflect only \
         complete pairs: {result}"
    );
    assert_eq!(result["pearson_r"], 0.5, "hand-checkable r: {result}");
    assert_eq!(result["x_mean"], 400.0);
    assert_eq!(result["y_mean"], 70.0);
}

#[tokio::test]
async fn lag_pairs_x_with_the_following_bucket_y() {
    let server = Harness::start().await;
    server
        .ingest_ccda(&fixture("relationship_vitals.xml"))
        .await;

    let mut lagged = args();
    lagged["lag"] = serde_json::json!(1);
    let result = server.call_tool("observation_relationship", lagged).await;
    // Pairs: (x Jan 15, y Jan 16) = (380, 80) and (x Jan 16, y Jan 17) =
    // (400, 70). Jan 17/18 x-values have no next-day y → dropped.
    assert_eq!(result["n_pairs"], 2, "{result}");
    assert_eq!(result["pearson_r"], -1.0, "{result}");
}

#[tokio::test]
async fn threshold_groups_split_strictly_below_vs_at_or_above() {
    let server = Harness::start().await;
    server
        .ingest_ccda(&fixture("relationship_vitals.xml"))
        .await;

    let mut with_threshold = args();
    with_threshold["threshold"] = serde_json::json!(400.0);
    let result = server
        .call_tool("observation_relationship", with_threshold)
        .await;
    // x strictly below 400: only (380, 60). x = 400 itself is at-or-above,
    // grouped with (420, 70).
    assert_eq!(result["groups"]["x_below"]["count"], 1, "{result}");
    assert_eq!(result["groups"]["x_below"]["mean"], 60.0);
    assert_eq!(result["groups"]["x_at_or_above"]["count"], 2);
    assert_eq!(result["groups"]["x_at_or_above"]["mean"], 75.0);
    assert_eq!(result["groups"]["x_at_or_above"]["min"], 70.0);
    assert_eq!(result["groups"]["x_at_or_above"]["max"], 80.0);
}
