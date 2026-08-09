//! One-shot LLM extraction via the Claude API.
//!
//! Rust has no official Anthropic SDK, so this is a plain `reqwest` call to
//! `POST /v1/messages` with a structured-output JSON schema
//! (`output_config.format`), which guarantees the response text is valid
//! JSON matching [`RawExtraction`] (clinical-PDF ingest) or
//! [`RawJournalExtraction`] (journal-entry ingest). The two prompts and
//! schemas differ, but share the same model, retry, and transport
//! machinery. The model id and prompt versions are pinned here and recorded
//! in every archived artifact.

use std::future::Future;

use super::artifact::RawExtraction;
use super::error::Error;
use super::journal::RawJournalExtraction;

/// Claude model used for extraction. Recorded in every artifact.
pub const EXTRACTION_MODEL: &str = "claude-opus-4-8";

/// Version of the extraction request ([`EXTRACTION_PROMPT`] plus request
/// configuration). Bump when either changes in a way that affects output.
/// v2: enabled adaptive thinking (v1 leaked reasoning into the JSON).
pub const PROMPT_VERSION: u32 = 2;

/// Version of the journal extraction request ([`JOURNAL_PROMPT`] plus
/// request configuration). Bump when either changes in a way that affects
/// output.
pub const JOURNAL_PROMPT_VERSION: u32 = 1;

const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
const API_VERSION: &str = "2023-06-01";

/// Total attempts per extraction. Transient failures (connection errors,
/// HTTP 429/5xx) are retried in-band with short linear backoff: narrative
/// ingestion is interactive, so a brief API blip should heal invisibly,
/// while a sustained outage should fail the ingest quickly so the caller
/// can re-run it once the API recovers.
const MAX_ATTEMPTS: u32 = 3;

/// `POST /v1/messages` endpoint under a base URL (trailing slash tolerated).
fn messages_url(base_url: &str) -> String {
    format!("{}/v1/messages", base_url.trim_end_matches('/'))
}

const EXTRACTION_PROMPT: &str = "You are extracting structured data from a clinical narrative \
document for a personal health record. Work ONLY from the document text below; never use \
outside knowledge to add codes that are not literally present.\n\
\n\
Extract:\n\
1. document_date: the single calendar date this document is about (order/collection date for \
a lab or pathology report; visit date for a note), formatted YYYY-MM-DD, with \
document_date_quote set to an exact text span (copied verbatim from the document) that \
contains that date. If no date is present, use null for both.\n\
2. title: a short human-readable label for the document, \
e.g. \"GI Pathology Report — colon biopsy\".\n\
3. codings: every ICD-10 code that appears VERBATIM in the text. For each: code exactly as \
written; display = the diagnosis text the document pairs with the code; quote = an exact \
text span (copied verbatim, including the code) where it appears; section_label = the \
section heading it appears under (e.g. \"Pre-Op Diagnosis/Indications\"), or null.\n\
\n\
Do not include codes that do not appear in the text. Copy quotes exactly — they are checked \
mechanically against the document, and any quote that is not a verbatim substring is \
discarded.";

const JOURNAL_PROMPT: &str = "You are reading one personal health-journal entry, written by a \
patient in casual, non-medical language. Map the complaints and symptoms the author describes \
onto ICD-10-CM codes so the entry can be found and analyzed alongside clinical records. The \
author is not a clinician: do not diagnose, and prefer coarse, common symptom/complaint codes \
(R-codes; site-specific pain codes such as M25.512 Pain in left shoulder) over specific disease \
codes. Use the same code for the same complaint every time.\n\
\n\
Extract:\n\
1. entry_date: the calendar date the entry is about, formatted YYYY-MM-DD, ONLY if a date \
appears in the entry text itself, with entry_date_quote set to an exact verbatim span \
containing that date. Otherwise null for both.\n\
2. title: a short human-readable label, e.g. \"Journal — left shoulder ache, poor sleep\".\n\
3. codings: one per distinct symptom or complaint the author describes as their own, current \
experience. For each: code = a valid ICD-10-CM code for the complaint; display = the standard \
ICD-10-CM description; quote = an exact verbatim span from the entry describing the complaint; \
severity = a number ONLY when the author explicitly writes a numeric rating (e.g. \"pain was a \
6 today\" -> 6) and that number appears inside the quote, otherwise null. Never turn words like \
\"awful\" into a number.\n\
\n\
Do not code things the author denies, describes in someone else, or mentions only as history. \
Copy quotes exactly — they are checked mechanically against the entry, and any quote that is \
not a verbatim substring is discarded.";

/// Anything that can turn document text into a [`RawExtraction`].
///
/// Behind a trait so ingestion tests can use a canned extractor instead of
/// the network. Uses a native `impl Future` return (no `async_trait`),
/// matching the `sources::Source` convention.
pub trait LlmExtractor {
    /// Extract structured claims from the document text.
    fn extract(&self, text: &str) -> impl Future<Output = Result<RawExtraction, Error>> + Send;
}

/// The production extractor: calls the Claude API.
#[derive(Clone)]
pub struct ClaudeExtractor {
    http: reqwest::Client,
    api_key: String,
    base_url: String,
    /// Base delay between retry attempts (attempt N waits N × this).
    /// Tests shrink it; production keeps the 1s default.
    retry_backoff: std::time::Duration,
}

impl ClaudeExtractor {
    /// Build an extractor from an HTTP client and API key, targeting the
    /// official Anthropic API endpoint.
    #[must_use]
    pub fn new(http: reqwest::Client, api_key: String) -> Self {
        Self {
            http,
            api_key,
            base_url: DEFAULT_BASE_URL.to_owned(),
            retry_backoff: std::time::Duration::from_secs(1),
        }
    }

    /// Override the API base URL (for a proxy or gateway). Follows the
    /// official Anthropic SDKs' convention: the base replaces
    /// `https://api.anthropic.com`, with `/v1/messages` appended.
    #[must_use]
    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url;
        self
    }

    /// Build from the environment: `ANTHROPIC_API_KEY` (required) and
    /// `ANTHROPIC_BASE_URL` (optional endpoint override, same variable the
    /// official Anthropic SDKs honor).
    ///
    /// Returns `None` when the key is unset or empty — the caller degrades
    /// to text-only ingestion.
    #[must_use]
    pub fn from_env(http: reqwest::Client) -> Option<Self> {
        let extractor = match std::env::var("ANTHROPIC_API_KEY") {
            Ok(key) if !key.is_empty() => Self::new(http, key),
            _ => return None,
        };
        match std::env::var("ANTHROPIC_BASE_URL") {
            Ok(base) if !base.is_empty() => Some(extractor.with_base_url(base)),
            _ => Some(extractor),
        }
    }
}

/// The JSON schema the API is constrained to (structured outputs).
fn output_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "document_date": {"type": ["string", "null"]},
            "document_date_quote": {"type": ["string", "null"]},
            "title": {"type": ["string", "null"]},
            "codings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string"},
                        "display": {"type": "string"},
                        "quote": {"type": "string"},
                        "section_label": {"type": ["string", "null"]}
                    },
                    "required": ["code", "display", "quote", "section_label"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["document_date", "document_date_quote", "title", "codings"],
        "additionalProperties": false
    })
}

/// Build the `POST /v1/messages` request body for a document text.
///
/// Adaptive thinking is enabled deliberately: with thinking off, Opus 4.8
/// intermittently leaks its reasoning into the constrained JSON output
/// (observed live: a title ending in "…valid JSON I must produce codings
/// array. Let me correct." with an empty codings array). Thinking blocks
/// give that reasoning somewhere else to go.
fn build_request_body(text: &str) -> serde_json::Value {
    serde_json::json!({
        "model": EXTRACTION_MODEL,
        "max_tokens": 16000,
        "thinking": {"type": "adaptive"},
        "output_config": {"format": {"type": "json_schema", "schema": output_schema()}},
        "messages": [{
            "role": "user",
            "content": format!("{EXTRACTION_PROMPT}\n\n<document>\n{text}\n</document>"),
        }],
    })
}

/// The JSON schema for journal extraction (structured outputs).
fn journal_output_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "entry_date": {"type": ["string", "null"]},
            "entry_date_quote": {"type": ["string", "null"]},
            "title": {"type": ["string", "null"]},
            "codings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string"},
                        "display": {"type": "string"},
                        "quote": {"type": "string"},
                        "severity": {"type": ["number", "null"]}
                    },
                    "required": ["code", "display", "quote", "severity"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["entry_date", "entry_date_quote", "title", "codings"],
        "additionalProperties": false
    })
}

/// Build the `POST /v1/messages` request body for one journal entry.
/// `feedback` carries rejection reasons from a prior attempt (e.g. invalid
/// codes) so the model can correct itself on the single semantic retry.
fn build_journal_request_body(text: &str, feedback: Option<&str>) -> serde_json::Value {
    let mut prompt = JOURNAL_PROMPT.to_owned();
    if let Some(fb) = feedback {
        prompt.push_str("\n\nA previous attempt was rejected for these reasons — correct them:\n");
        prompt.push_str(fb);
    }
    serde_json::json!({
        "model": EXTRACTION_MODEL,
        "max_tokens": 16000,
        "thinking": {"type": "adaptive"},
        "output_config": {"format": {"type": "json_schema", "schema": journal_output_schema()}},
        "messages": [{
            "role": "user",
            "content": format!("{prompt}\n\n<entry>\n{text}\n</entry>"),
        }],
    })
}

/// Parse the Messages API response body into the expected structured type.
fn parse_response_as<T: serde::de::DeserializeOwned>(body: &serde_json::Value) -> Result<T, Error> {
    if let Some(stop) = body.get("stop_reason").and_then(|v| v.as_str()) {
        if stop == "refusal" {
            return Err(Error::Api {
                reason: "model refused the extraction request".to_owned(),
            });
        }
        if stop == "max_tokens" {
            return Err(Error::InvalidResponse {
                reason: "response truncated at max_tokens".to_owned(),
            });
        }
    }
    let text = body
        .get("content")
        .and_then(|c| c.as_array())
        .and_then(|blocks| {
            blocks
                .iter()
                .find(|b| b.get("type").and_then(|t| t.as_str()) == Some("text"))
        })
        .and_then(|b| b.get("text"))
        .and_then(|t| t.as_str())
        .ok_or_else(|| Error::InvalidResponse {
            reason: "no text content block in response".to_owned(),
        })?;
    serde_json::from_str(text).map_err(|err| Error::InvalidResponse {
        reason: format!("structured output did not parse as expected type: {err}"),
    })
}

/// Parse the Messages API response body into a [`RawExtraction`].
fn parse_response(body: &serde_json::Value) -> Result<RawExtraction, Error> {
    parse_response_as(body)
}

impl ClaudeExtractor {
    /// One `POST /v1/messages` attempt, returning the raw response JSON. On
    /// failure the `bool` says whether the failure is transient (connection
    /// error, HTTP 429/5xx) and worth retrying; deterministic failures
    /// (auth, refusal, malformed response) are not.
    async fn attempt(&self, body: &serde_json::Value) -> Result<serde_json::Value, (Error, bool)> {
        let response = self
            .http
            .post(messages_url(&self.base_url))
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", API_VERSION)
            .json(body)
            .send()
            .await
            .map_err(|err| {
                (
                    Error::Api {
                        reason: format!("request failed: {err}"),
                    },
                    true,
                )
            })?;
        let status = response.status();
        if !status.is_success() {
            // Don't parse as JSON here: a non-2xx response (e.g. a 502 from
            // an intermediary proxy) may return an HTML or plain-text body,
            // and trying to parse it as JSON would surface a generic
            // body-read error that loses the actual HTTP status.
            let body_text = response.text().await.unwrap_or_default();
            let transient =
                status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS;
            return Err((
                Error::Api {
                    reason: format!("HTTP {status}: {body_text}"),
                },
                transient,
            ));
        }
        let body: serde_json::Value = response.json().await.map_err(|err| {
            (
                Error::Api {
                    reason: format!("reading response body: {err}"),
                },
                false,
            )
        })?;
        Ok(body)
    }

    /// Run [`Self::attempt`] with the bounded transient-retry loop, returning
    /// the raw response JSON of whichever attempt succeeded.
    async fn request_with_retries(
        &self,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let mut attempt = 1u32;
        loop {
            match self.attempt(body).await {
                Ok(v) => return Ok(v),
                Err((error, transient)) => {
                    if !transient || attempt >= MAX_ATTEMPTS {
                        return Err(error);
                    }
                    tracing::warn!(attempt, error = %error, "transient extraction failure; retrying");
                    tokio::time::sleep(self.retry_backoff * attempt).await;
                    attempt += 1;
                }
            }
        }
    }
}

impl LlmExtractor for ClaudeExtractor {
    async fn extract(&self, text: &str) -> Result<RawExtraction, Error> {
        let body = build_request_body(text);
        let v = self.request_with_retries(&body).await?;
        parse_response(&v)
    }
}

/// Anything that can turn one journal entry into a [`RawJournalExtraction`].
///
/// Separate from [`LlmExtractor`] because the journal prompt, output schema,
/// and feedback-retry contract differ; ingestion tests use canned impls.
pub trait JournalExtractor {
    /// Extract structured claims from one journal entry. `feedback`, when
    /// present, carries rejection reasons from a prior attempt (invalid
    /// codes) for a single corrective retry.
    fn extract_journal(
        &self,
        text: &str,
        feedback: Option<&str>,
    ) -> impl Future<Output = Result<RawJournalExtraction, Error>> + Send;
}

impl JournalExtractor for ClaudeExtractor {
    async fn extract_journal(
        &self,
        text: &str,
        feedback: Option<&str>,
    ) -> Result<RawJournalExtraction, Error> {
        let body = build_journal_request_body(text, feedback);
        let v = self.request_with_retries(&body).await?;
        parse_response_as(&v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_url_appends_path_and_tolerates_trailing_slash() {
        assert_eq!(
            messages_url("https://api.anthropic.com"),
            "https://api.anthropic.com/v1/messages"
        );
        assert_eq!(
            messages_url("http://127.0.0.1:8080/"),
            "http://127.0.0.1:8080/v1/messages"
        );
    }

    #[test]
    fn with_base_url_overrides_the_default() {
        let extractor = ClaudeExtractor::new(reqwest::Client::new(), "k".to_owned())
            .with_base_url("http://127.0.0.1:9999".to_owned());
        assert_eq!(extractor.base_url, "http://127.0.0.1:9999");
    }

    #[test]
    fn request_body_pins_model_and_embeds_document() {
        let body = build_request_body("SAMPLE DOCUMENT TEXT");
        assert_eq!(body["model"], EXTRACTION_MODEL);
        assert_eq!(body["output_config"]["format"]["type"], "json_schema");
        let content = body["messages"][0]["content"].as_str().expect("content");
        assert!(content.contains("SAMPLE DOCUMENT TEXT"));
        assert!(content.contains("ICD-10"));
        // No sampling params (400 on Opus 4.8). Adaptive thinking must be ON:
        // without it the model leaks reasoning into the constrained output.
        assert!(body.get("temperature").is_none());
        assert_eq!(body["thinking"]["type"], "adaptive");
    }

    /// Minimal scripted HTTP server on a std thread: each connection gets
    /// the next scripted `(status, body)` response, then the listener drops
    /// (further connections are refused). Returns the base URL and a counter
    /// of requests served.
    fn scripted_server(
        script: Vec<(u16, String)>,
    ) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::io::{Read, Write};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind scripted server");
        let addr = listener.local_addr().expect("scripted server addr");
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        std::thread::spawn(move || {
            for (status, body) in script {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                // Drain the request (headers + content-length body) before
                // responding, so the client never sees a reset mid-send.
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let header_end = loop {
                    let Ok(n) = stream.read(&mut chunk) else {
                        break 0;
                    };
                    if n == 0 {
                        break 0;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&buf[..header_end]).to_ascii_lowercase();
                let len: usize = headers
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
                while buf.len() < header_end + len {
                    let Ok(n) = stream.read(&mut chunk) else {
                        break;
                    };
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let response = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        (format!("http://{addr}"), hits)
    }

    /// A well-formed Messages-API success body with an empty extraction.
    fn ok_response_body() -> String {
        serde_json::json!({
            "stop_reason": "end_turn",
            "content": [{
                "type": "text",
                "text": r#"{"document_date":null,"document_date_quote":null,"title":null,"codings":[]}"#
            }]
        })
        .to_string()
    }

    fn fast_retry_extractor(base_url: String) -> ClaudeExtractor {
        let mut extractor =
            ClaudeExtractor::new(reqwest::Client::new(), "k".to_owned()).with_base_url(base_url);
        extractor.retry_backoff = std::time::Duration::from_millis(10);
        extractor
    }

    #[tokio::test]
    async fn retries_transient_http_failures_then_succeeds() {
        let (base_url, hits) = scripted_server(vec![
            (500, "internal error".to_owned()),
            (529, "overloaded".to_owned()),
            (200, ok_response_body()),
        ]);
        let raw = fast_retry_extractor(base_url)
            .extract("doc")
            .await
            .expect("third attempt should succeed");
        assert!(raw.codings.is_empty());
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "both transient failures should be retried"
        );
    }

    #[tokio::test]
    async fn gives_up_after_bounded_attempts() {
        // Four scripted failures, but only three attempts should be made.
        let (base_url, hits) = scripted_server(vec![
            (500, "outage".to_owned()),
            (500, "outage".to_owned()),
            (500, "outage".to_owned()),
            (500, "outage".to_owned()),
        ]);
        let err = fast_retry_extractor(base_url)
            .extract("doc")
            .await
            .expect_err("sustained outage should fail");
        assert!(
            err.to_string().contains("500"),
            "final error surfaces the HTTP status: {err}"
        );
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "retry budget is exactly three attempts"
        );
    }

    #[tokio::test]
    async fn does_not_retry_non_transient_client_errors() {
        let (base_url, hits) = scripted_server(vec![
            (401, "unauthorized".to_owned()),
            (200, ok_response_body()),
        ]);
        let err = fast_retry_extractor(base_url)
            .extract("doc")
            .await
            .expect_err("auth failure is deterministic; retrying cannot help");
        assert!(err.to_string().contains("401"), "{err}");
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "client errors must not be retried"
        );
    }

    #[test]
    fn parses_a_successful_response() {
        let body = serde_json::json!({
            "stop_reason": "end_turn",
            "content": [{
                "type": "text",
                "text": r#"{"document_date":"2026-04-21","document_date_quote":"Order Date: 04/21/2026","title":"GI Pathology Report","codings":[{"code":"R10.9","display":"Abdominal pain, unspecified","quote":"Abdominal pain, unspecified - R10.9","section_label":"Pre-Op Diagnosis/Indications"}]}"#
            }]
        });
        let raw = parse_response(&body).expect("parse");
        assert_eq!(raw.document_date.as_deref(), Some("2026-04-21"));
        assert_eq!(raw.codings.len(), 1);
        assert_eq!(raw.codings[0].code, "R10.9");
    }

    #[test]
    fn refusal_maps_to_api_error() {
        let body = serde_json::json!({"stop_reason": "refusal", "content": []});
        let err = parse_response(&body).expect_err("should fail");
        assert!(matches!(err, Error::Api { .. }));
    }

    #[test]
    fn garbage_content_maps_to_invalid_response() {
        let body = serde_json::json!({
            "stop_reason": "end_turn",
            "content": [{"type": "text", "text": "not json"}]
        });
        let err = parse_response(&body).expect_err("should fail");
        assert!(matches!(err, Error::InvalidResponse { .. }));
    }

    #[test]
    fn journal_request_body_pins_model_schema_and_embeds_entry() {
        let body = build_journal_request_body("SAMPLE JOURNAL ENTRY", None);
        assert_eq!(body["model"], EXTRACTION_MODEL);
        assert_eq!(body["output_config"]["format"]["type"], "json_schema");
        assert_eq!(body["thinking"]["type"], "adaptive");
        let content = body["messages"][0]["content"].as_str().expect("content");
        assert!(content.contains("SAMPLE JOURNAL ENTRY"));
        assert!(content.contains("ICD-10-CM"));
        assert!(content.contains("severity"));
        assert!(
            !content.contains("invalid ICD-10-CM codes"),
            "no feedback block"
        );
        // Schema must include the journal-only fields.
        let schema = &body["output_config"]["format"]["schema"];
        assert!(schema["properties"]["entry_date"].is_object());
        assert!(schema["properties"]["codings"]["items"]["properties"]["severity"].is_object());
    }

    #[test]
    fn journal_feedback_is_appended_to_the_prompt() {
        let body =
            build_journal_request_body("ENTRY", Some("M25.5129 is not a valid ICD-10-CM code"));
        let content = body["messages"][0]["content"].as_str().expect("content");
        assert!(content.contains("M25.5129 is not a valid ICD-10-CM code"));
    }

    #[tokio::test]
    async fn extract_journal_parses_a_successful_response() {
        let (base_url, _hits) = scripted_server(vec![(
            200,
            serde_json::json!({
                "stop_reason": "end_turn",
                "content": [{
                    "type": "text",
                    "text": r#"{"entry_date":null,"entry_date_quote":null,"title":"Journal","codings":[{"code":"M25.512","display":"Pain in left shoulder","quote":"shoulder aching","severity":6}]}"#
                }]
            })
            .to_string(),
        )]);
        let raw = fast_retry_extractor(base_url)
            .extract_journal("shoulder aching", None)
            .await
            .expect("extract");
        assert_eq!(raw.codings.len(), 1);
        assert_eq!(raw.codings[0].code, "M25.512");
        assert_eq!(raw.codings[0].severity, Some(6.0));
    }
}
