//! Holdout regression tests: journal-entry ingestion — LLM-*inferred*
//! codings, exercised black-box against a loopback mock Messages-API server
//! (the harness refuses non-loopback base URLs, so these tests can never
//! reach the real API).
//!
//! PROTECTED: part of the holdout suite. A failure here is a real regression in
//! the product contract — fix `crates/**`, never edit this file or its fixtures
//! to make it pass. Changes under `holdout/` require a human-signed bless commit
//! (see the design spec and `holdout.lock`).
//!
//! Contracts being guarded:
//!
//! 1. **Journal complaints become inferred observations, and the LLM runs
//!    exactly once.** A journal ingest maps colloquial prose to ICD-10-CM
//!    `observations` rows marked `derivation: "inferred"` — never `problems`
//!    rows — and `index_rebuild` reproduces them from the archive + derived
//!    store with zero further model calls (proven by shutting the mock down
//!    before rebuilding).
//! 2. **Nonexistent ICD-10-CM codes never reach the index, and stored codes
//!    are canonical.** A hallucinated code is rejected against the vendored
//!    vocabulary table with exactly one corrective retry (bounded — never a
//!    loop), and a code the model writes dotless/lowercase is stored in
//!    canonical form so the analytic key space cannot fork ("m25569" and
//!    "M25.569" must be the same series).
//! 3. **A malformed-but-quote-verified LLM entry date fails the ingest with
//!    NOTHING persisted.** Before this was enforced, a date like "2026-7-6"
//!    could pass quote verification, persist the archive blob and a frozen
//!    artifact, and then fail the strict parse — leaving a poison artifact
//!    that made every future `index_rebuild` fail. The ingest must reject it
//!    up front, leave both stores empty, and leave rebuild working.

use chartpds_holdout::mock_llm::{MockLlm, MockLlmResponse};
use chartpds_holdout::Harness;

/// ICD-10-CM coding-system URI used for journal-inferred observations.
const ICD10_CM: &str = "http://hl7.org/fhir/sid/icd-10-cm";

/// Ingest one journal entry inline through `record_ingest` and return the
/// outcome.
async fn ingest_journal(server: &Harness, entry: &str) -> serde_json::Value {
    server
        .call_tool(
            "record_ingest",
            serde_json::json!({
                "kind": "journal",
                "source": "journal",
                "content": entry,
            }),
        )
        .await
}

/// Fetch full observation history for one ICD-10-CM code.
async fn history_for(server: &Harness, code: &str) -> serde_json::Value {
    server
        .call_tool(
            "observation_history",
            serde_json::json!({ "codings": [{ "system": ICD10_CM, "code": code }] }),
        )
        .await
}

/// A dated journal entry becomes an inferred observation (with the author's
/// stated severity), searchable text, and NOT a problem-list entry; rebuild
/// then reproduces the same state with the mock server DEAD — zero further
/// LLM calls, ever.
#[tokio::test]
async fn journal_codings_index_as_inferred_observations_and_rebuild_never_calls_llm() {
    const ENTRY: &str = "# Jul 26, 2026\n\nLeft shoulder aching again after climbing. \
Pain was maybe a 6 today.\n";
    let mock = MockLlm::start(vec![MockLlmResponse::Extraction(serde_json::json!({
        "entry_date": null,
        "entry_date_quote": null,
        "title": "Journal — left shoulder ache",
        "codings": [{
            "code": "M25.512",
            "display": "Pain in left shoulder",
            "quote": "Left shoulder aching again after climbing. Pain was maybe a 6 today.",
            "severity": 6,
        }],
    }))])
    .await;
    let server = Harness::start_with_llm(&mock.base_url()).await;

    let outcome = ingest_journal(&server, ENTRY).await;
    assert_eq!(
        outcome["entry_date"], "2026-07-26",
        "dated markdown header must date the entry deterministically: {outcome}"
    );
    let codings = outcome["codings"].as_array().expect("codings");
    assert_eq!(codings.len(), 1, "{outcome}");
    assert_eq!(codings[0]["code"], "M25.512", "{outcome}");
    assert_eq!(
        codings[0]["severity"], 6.0,
        "author-stated severity: {outcome}"
    );
    assert!(
        codings[0]["quote"].as_str().is_some_and(|q| !q.is_empty()),
        "the outcome must echo the grounding quote for the author to audit: {outcome}"
    );
    assert_eq!(
        outcome["rejected"].as_array().map(Vec::len),
        Some(0),
        "no false rejections: {outcome}"
    );
    assert_eq!(mock.request_count(), 1, "exactly one LLM call at ingest");

    // The complaint is an observation — inferred, with severity as the
    // value — and is NOT on the clinician-asserted problem list.
    let history = history_for(&server, "M25.512").await;
    let items = history["items"].as_array().expect("items");
    assert_eq!(items.len(), 1, "{history}");
    assert_eq!(items[0]["derivation"], "inferred", "{history}");
    assert_eq!(items[0]["value_quantity"], 6.0, "{history}");
    let problems = server
        .call_tool("problem_list", serde_json::Value::Null)
        .await;
    assert!(
        !problems["items"]
            .as_array()
            .expect("items")
            .iter()
            .any(|p| p["coding_code"] == "M25.512"),
        "journal complaints must never enter the problem list: {problems}"
    );

    // The entry text is retrievable, and the catalog reports its coding count.
    let hits = server
        .call_tool(
            "narrative_search",
            serde_json::json!({ "query": "climbing" }),
        )
        .await;
    let hit_items = hits["items"].as_array().expect("items");
    assert_eq!(hit_items.len(), 1, "{hits}");
    assert_eq!(hit_items[0]["kind"], "journal", "{hits}");
    assert_eq!(hit_items[0]["codings_count"], 1, "{hits}");

    // Kill the LLM endpoint, then rebuild: the index must be reproduced
    // entirely from the archive + derived store, with no model call.
    mock.shutdown();
    let rebuild = server
        .call_tool("index_rebuild", serde_json::Value::Null)
        .await;
    assert_eq!(rebuild["journals_ingested"], 1, "{rebuild}");
    assert_eq!(rebuild["journal_extractions_applied"], 1, "{rebuild}");
    assert_eq!(rebuild["blobs_skipped"], 0, "{rebuild}");
    assert_eq!(
        mock.request_count(),
        1,
        "rebuild must never call the LLM (mock saw a second request)"
    );
    let history = history_for(&server, "M25.512").await;
    let items = history["items"].as_array().expect("items");
    assert_eq!(
        items.len(),
        1,
        "observation must survive rebuild: {history}"
    );
    assert_eq!(items[0]["derivation"], "inferred", "{history}");
}

/// A hallucinated (well-formed but nonexistent) ICD-10-CM code is rejected
/// against the vendored table and triggers exactly one corrective retry;
/// the retry's dotless/lowercase code is stored canonicalized, and the
/// hallucinated code never reaches the index.
#[tokio::test]
async fn hallucinated_code_is_retried_once_and_stored_code_is_canonical() {
    const ENTRY: &str = "# Jul 27, 2026\n\nKnee pain flared up on the stairs.\n";
    let quote = "Knee pain flared up on the stairs.";
    let mock = MockLlm::start(vec![
        // First attempt: a code that does not exist in ICD-10-CM.
        MockLlmResponse::Extraction(serde_json::json!({
            "entry_date": null,
            "entry_date_quote": null,
            "title": "Journal — knee pain",
            "codings": [{
                "code": "M25.5691",
                "display": "Pain in unspecified knee",
                "quote": quote,
                "severity": null,
            }],
        })),
        // Corrective retry: a real code, written dotless and lowercase.
        MockLlmResponse::Extraction(serde_json::json!({
            "entry_date": null,
            "entry_date_quote": null,
            "title": "Journal — knee pain",
            "codings": [{
                "code": "m25569",
                "display": "Pain in unspecified knee",
                "quote": quote,
                "severity": null,
            }],
        })),
    ])
    .await;
    let server = Harness::start_with_llm(&mock.base_url()).await;

    let outcome = ingest_journal(&server, ENTRY).await;
    assert_eq!(
        mock.request_count(),
        2,
        "an invalid code earns exactly one corrective retry, never a loop"
    );
    let codings = outcome["codings"].as_array().expect("codings");
    assert_eq!(codings.len(), 1, "{outcome}");
    assert_eq!(
        codings[0]["code"], "M25.569",
        "stored code must be canonical (uppercase, dotted): {outcome}"
    );
    assert!(
        outcome["rejected"]
            .as_array()
            .expect("rejected")
            .iter()
            .any(|r| r.as_str().is_some_and(|s| s.contains("M25.5691"))),
        "the hallucinated code's rejection stays visible: {outcome}"
    );

    // The canonical code is queryable; the hallucinated one never landed.
    let history = history_for(&server, "M25.569").await;
    assert_eq!(
        history["items"].as_array().map(Vec::len),
        Some(1),
        "canonical code queryable: {history}"
    );
    for absent in ["M25.5691", "m25569"] {
        let history = history_for(&server, absent).await;
        assert_eq!(
            history["items"].as_array().map(Vec::len),
            Some(0),
            "non-canonical key {absent} must not exist: {history}"
        );
    }
}

/// A malformed-but-quote-verified LLM entry date ("2026-7-6") fails the
/// ingest up front with an actionable error and NOTHING persisted — no
/// archived blob, no frozen artifact, no index rows — and a subsequent
/// rebuild works on empty stores instead of choking on a poison artifact.
#[tokio::test]
async fn malformed_llm_date_fails_ingest_with_nothing_persisted() {
    const ENTRY: &str = "Left shoulder has been sore since 7/6/2026, no idea why.\n";
    let mock = MockLlm::start(vec![MockLlmResponse::Extraction(serde_json::json!({
        "entry_date": "2026-7-6",
        "entry_date_quote": "7/6/2026",
        "title": null,
        "codings": [],
    }))])
    .await;
    let server = Harness::start_with_llm(&mock.base_url()).await;

    let err = server
        .try_call_tool(
            "record_ingest",
            serde_json::json!({
                "kind": "journal",
                "source": "journal",
                "content": ENTRY,
            }),
        )
        .await
        .expect_err("a non-canonical LLM date must fail the ingest");
    assert!(
        err.contains("YYYY-MM-DD"),
        "error tells the author how to date the entry: {err}"
    );

    // No residue: the text is not searchable, and rebuild sees empty stores
    // (in particular, no frozen artifact that would poison every rebuild).
    let hits = server
        .call_tool("narrative_search", serde_json::json!({ "query": "sore" }))
        .await;
    assert_eq!(
        hits["items"].as_array().map(Vec::len),
        Some(0),
        "failed ingest must not index text: {hits}"
    );
    let rebuild = server
        .call_tool("index_rebuild", serde_json::Value::Null)
        .await;
    assert_eq!(rebuild["blobs_found"], 0, "nothing archived: {rebuild}");
    assert_eq!(rebuild["journals_ingested"], 0, "{rebuild}");
    assert_eq!(rebuild["journal_extractions_applied"], 0, "{rebuild}");
}
