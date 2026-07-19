//! Holdout regression tests: the *live* LLM extraction path, exercised
//! black-box against a loopback mock Messages-API server (the harness
//! refuses non-loopback base URLs, so these tests can never reach the real
//! API).
//!
//! PROTECTED: part of the holdout suite. A failure here is a real regression in
//! the product contract — fix `crates/**`, never edit this file or its fixtures
//! to make it pass. Changes under `holdout/` require a human-signed bless commit
//! (see the design spec and `holdout.lock`).
//!
//! Contracts being guarded:
//!
//! 1. **The LLM is called exactly once, at ingest — and never again.** A live
//!    ingest makes one API call; `index_rebuild` makes zero, proven by
//!    shutting the mock server down before rebuilding: if rebuild ever
//!    regressed into calling a model, it would hit a dead port instead of
//!    silently spending money against the real API.
//! 2. **Unverifiable LLM claims never reach the index.** A coding whose
//!    quote is not a verbatim substring of the document text is dropped and
//!    reported in `rejected` — the hallucination guard.
//! 3. **A sustained LLM outage fails the ingest cleanly.** Transient failures
//!    are retried in-band (three attempts, bounded — never unbounded
//!    hammering); a still-failing API then fails the `record_ingest` call
//!    with NOTHING persisted — no partial text-only state to reconcile later,
//!    nothing for rebuild to resurrect — and the identical ingest succeeds
//!    once the API recovers.
//!
//! The scripted "LLM" response is derived from the blessed frozen-artifact
//! fixture, so its quotes verify against the synthetic PDF by construction.

use chartpds_holdout::mock_llm::{MockLlm, MockLlmResponse};
use chartpds_holdout::Harness;

/// SHA-256 of the synthetic pathology PDF blob in `fixtures/narrative_pdf/`.
const PDF_HASH: &str = "c749a54e55e6f009146acd05dd0ea0adf273797bb37b205944376251e82074b0";

/// SHA-256 of the frozen extraction artifact blob for that PDF.
const ARTIFACT_HASH: &str = "85b7837f95774fd0d7000da50992a8385effbb067be718b4f36370fc97a45d4d";

/// Absolute path of a file in `fixtures/narrative_pdf/`.
fn fixture_path(name: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("narrative_pdf")
        .join(name)
}

/// Build the raw-extraction JSON a live LLM call would return, from the
/// blessed artifact fixture: same date/title/codings (whose quotes verify
/// against the synthetic PDF by construction), minus the artifact-only
/// fields (`document`, `system`, `extractor`, `extracted_at`).
fn scripted_extraction() -> serde_json::Value {
    let artifact: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(fixture_path(ARTIFACT_HASH)).expect("read artifact fixture"),
    )
    .expect("artifact fixture is JSON");
    let codings: Vec<serde_json::Value> = artifact["codings"]
        .as_array()
        .expect("codings array")
        .iter()
        .map(|c| {
            serde_json::json!({
                "code": c["code"],
                "display": c["display"],
                "quote": c["quote"],
                "section_label": c["section_label"],
            })
        })
        .collect();
    serde_json::json!({
        "document_date": artifact["document_date"],
        "document_date_quote": artifact["document_date_quote"],
        "title": artifact["title"],
        "codings": codings,
    })
}

/// Ingest the synthetic PDF through `record_ingest` and return the outcome.
async fn ingest_fixture_pdf(server: &Harness) -> serde_json::Value {
    server
        .call_tool(
            "record_ingest",
            serde_json::json!({
                "kind": "clinical-pdf",
                "source": "holdout",
                "file_path": fixture_path(PDF_HASH).to_str().expect("utf-8 path"),
            }),
        )
        .await
}

/// One LLM call at ingest applies a verified extraction end-to-end; rebuild
/// then reproduces the same state with the mock server DEAD — zero further
/// LLM calls, ever.
#[tokio::test]
async fn live_extraction_calls_llm_once_and_rebuild_never_calls_again() {
    let mock = MockLlm::start(vec![MockLlmResponse::Extraction(scripted_extraction())]).await;
    let server = Harness::start_with_llm(&mock.base_url()).await;

    let outcome = ingest_fixture_pdf(&server).await;
    assert_eq!(
        outcome["extraction_status"], "applied",
        "live extraction must apply: {outcome}"
    );
    assert_eq!(outcome["codings_indexed"], 3, "all codings: {outcome}");
    assert_eq!(
        outcome["rejected"].as_array().map(Vec::len),
        Some(0),
        "no false rejections: {outcome}"
    );
    assert_eq!(outcome["document_date"], "2026-04-21");
    assert_eq!(mock.request_count(), 1, "exactly one LLM call at ingest");

    // Kill the LLM endpoint, then rebuild: the index must be reproduced
    // entirely from the archive + derived store, with no model call.
    mock.shutdown();
    let rebuild = server
        .call_tool("index_rebuild", serde_json::Value::Null)
        .await;
    assert_eq!(rebuild["narratives_ingested"], 1, "{rebuild}");
    assert_eq!(rebuild["extractions_applied"], 1, "{rebuild}");
    assert_eq!(rebuild["blobs_skipped"], 0, "{rebuild}");
    assert_eq!(
        mock.request_count(),
        1,
        "rebuild must never call the LLM (mock saw a second request)"
    );

    // The verified codings survived the rebuild.
    let problems = server
        .call_tool("problem_list", serde_json::Value::Null)
        .await;
    let items = problems["items"].as_array().expect("items");
    for code in ["R10.9", "Z12.11", "K64.8"] {
        assert!(
            items.iter().any(|p| p["coding_code"] == code),
            "code {code} missing after rebuild: {problems}"
        );
    }
}

/// A coding whose quote is not verbatim in the document is rejected and
/// never indexed, while verifiable codings from the same response apply.
#[tokio::test]
async fn hallucinated_coding_is_rejected_and_never_indexed() {
    let mut extraction = scripted_extraction();
    extraction["codings"]
        .as_array_mut()
        .expect("codings")
        .push(serde_json::json!({
            "code": "E11.9",
            "display": "Type 2 diabetes mellitus without complications",
            "quote": "Type 2 diabetes mellitus without complications - E11.9",
            "section_label": null,
        }));
    let mock = MockLlm::start(vec![MockLlmResponse::Extraction(extraction)]).await;
    let server = Harness::start_with_llm(&mock.base_url()).await;

    let outcome = ingest_fixture_pdf(&server).await;
    assert_eq!(outcome["extraction_status"], "applied", "{outcome}");
    assert_eq!(
        outcome["codings_indexed"], 3,
        "only the verifiable codings: {outcome}"
    );
    assert_eq!(
        outcome["rejected"].as_array().map(Vec::len),
        Some(1),
        "the hallucination is reported: {outcome}"
    );

    let problems = server
        .call_tool("problem_list", serde_json::Value::Null)
        .await;
    let items = problems["items"].as_array().expect("items");
    assert!(
        !items.iter().any(|p| p["coding_code"] == "E11.9"),
        "hallucinated code must never reach the index: {problems}"
    );
}

/// A sustained LLM outage fails the ingest outright — after bounded in-band
/// retries — leaving no trace: nothing indexed, nothing archived, nothing
/// for rebuild to resurrect. Once the API recovers, re-running the same
/// ingest succeeds cleanly.
#[tokio::test]
async fn llm_outage_fails_ingest_cleanly_and_reingest_recovers() {
    // Three 500s exhaust the ingest's in-band retry budget; the fourth
    // response serves the post-recovery re-ingest.
    let mock = MockLlm::start(vec![
        MockLlmResponse::Status(500, "mock upstream outage"),
        MockLlmResponse::Status(500, "mock upstream outage"),
        MockLlmResponse::Status(500, "mock upstream outage"),
        MockLlmResponse::Extraction(scripted_extraction()),
    ])
    .await;
    let server = Harness::start_with_llm(&mock.base_url()).await;

    let err = server
        .try_call_tool(
            "record_ingest",
            serde_json::json!({
                "kind": "clinical-pdf",
                "source": "holdout",
                "file_path": fixture_path(PDF_HASH).to_str().expect("utf-8 path"),
            }),
        )
        .await
        .expect_err("a sustained LLM outage must fail the ingest");
    assert!(err.contains("500"), "error surfaces the HTTP status: {err}");
    assert_eq!(
        mock.request_count(),
        3,
        "retries are bounded: exactly three attempts, then give up"
    );

    // No residue: the document is neither indexed (searchable) nor archived
    // (nothing for rebuild to replay).
    let hits = server
        .call_tool(
            "narrative_search",
            serde_json::json!({ "query": "dysplasia" }),
        )
        .await;
    assert_eq!(
        hits["items"].as_array().map(Vec::len),
        Some(0),
        "failed ingest must not index text: {hits}"
    );
    let rebuild = server
        .call_tool("index_rebuild", serde_json::Value::Null)
        .await;
    assert_eq!(
        rebuild["narratives_ingested"], 0,
        "failed ingest must not archive the PDF: {rebuild}"
    );

    // Recovery: the identical ingest succeeds once the API is back.
    let outcome = ingest_fixture_pdf(&server).await;
    assert_eq!(outcome["extraction_status"], "applied", "{outcome}");
    assert_eq!(outcome["codings_indexed"], 3, "{outcome}");
    assert_eq!(
        mock.request_count(),
        4,
        "recovery is one clean LLM call (rebuild made none)"
    );
}
