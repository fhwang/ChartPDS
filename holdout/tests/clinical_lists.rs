//! Holdout regression tests for the clinical-list and observation tool surface.
//!
//! PROTECTED: these tests are part of the holdout suite. A failure here is a
//! real regression in the product contract — fix `crates/**`, never edit this
//! file or its fixture to make it pass. Changes under `holdout/` require a
//! human-signed bless commit (see the design spec and `holdout.lock`).

use chartpds_holdout::{fixture, Harness};

/// After ingesting a CCDA with one problem, `list_problems` reports exactly that
/// problem, deduped to one entry with its provenance.
#[tokio::test]
async fn list_problems_reports_ingested_diabetes() {
    let server = Harness::start().await;
    server.ingest_ccda(&fixture("diabetes_minimal.xml")).await;

    let result = server
        .call_tool("list_problems", serde_json::Value::Null)
        .await;

    assert!(
        result["latest_document_date"].is_string(),
        "expected a latest_document_date, got: {result}"
    );
    let items = result["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1, "expected exactly one problem: {result}");
    assert_eq!(items[0]["coding_code"], "44054006");
    assert_eq!(items[0]["document_count"], 1);
}

/// `list_medications` reports the ingested medication, deduped per code.
#[tokio::test]
async fn list_medications_reports_ingested_metformin() {
    let server = Harness::start().await;
    server.ingest_ccda(&fixture("diabetes_minimal.xml")).await;

    let result = server
        .call_tool("list_medications", serde_json::Value::Null)
        .await;

    let items = result["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1, "expected exactly one medication: {result}");
    assert_eq!(items[0]["coding_code"], "860975");
    assert_eq!(items[0]["document_count"], 1);
}

/// A vital sign round-trips through ingest and `latest_observation_by_code`
/// with its value intact.
#[tokio::test]
async fn latest_observation_round_trips_body_weight() {
    let server = Harness::start().await;
    server.ingest_ccda(&fixture("diabetes_minimal.xml")).await;

    let result = server
        .call_tool(
            "latest_observation_by_code",
            serde_json::json!({ "code": "29463-7" }),
        )
        .await;

    assert_eq!(result["coding_code"], "29463-7");
    assert_eq!(result["value_quantity"], 72.5);
}

/// A lab result is queryable by its LOINC coding through
/// `get_observation_history`.
#[tokio::test]
async fn observation_history_returns_lab_result() {
    let server = Harness::start().await;
    server.ingest_ccda(&fixture("diabetes_minimal.xml")).await;

    let result = server
        .call_tool(
            "get_observation_history",
            serde_json::json!({
                "codings": [{ "system": "http://loinc.org", "code": "4548-4" }]
            }),
        )
        .await;

    let rows = result.as_array().expect("history array");
    assert_eq!(rows.len(), 1, "expected one HbA1c observation: {result}");
    assert_eq!(rows[0]["coding_code"], "4548-4");
    assert_eq!(rows[0]["value_quantity"], 5.5);
}
