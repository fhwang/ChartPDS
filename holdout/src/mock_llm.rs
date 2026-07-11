//! A minimal mock Claude Messages API server for holdout tests.
//!
//! Serves scripted responses over a loopback TCP listener so a holdout test
//! can exercise the binary's *live* LLM extraction path — request
//! construction, response parsing, verification, artifact freezing — without
//! any network egress or real API key. The harness only accepts loopback
//! base URLs, so the suite stays hermetic by construction.
//!
//! Hand-rolled on `tokio::net` rather than a mock-server crate to keep the
//! holdout dependency graph minimal (same reason `lib.rs` hand-rolls
//! `TempDir`).

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// One scripted reply. Requests are answered in script order; requests
/// beyond the end of the script get an HTTP 500.
pub enum MockLlmResponse {
    /// HTTP 200 with a Messages-API envelope whose single text content block
    /// is this JSON value serialized — i.e. what structured outputs return.
    Extraction(serde_json::Value),
    /// A raw HTTP status with a plain-text body (e.g. a 500 outage).
    Status(u16, &'static str),
}

/// A running mock LLM server.
///
/// Captures every request body it receives; `shutdown` closes the listener
/// so later connection attempts are refused (the port stays dead).
pub struct MockLlm {
    addr: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<serde_json::Value>>>,
    handle: tokio::task::JoinHandle<()>,
}

impl MockLlm {
    /// Bind a loopback listener and serve the scripted responses.
    ///
    /// # Panics
    ///
    /// Panics if the listener cannot bind.
    pub async fn start(script: Vec<MockLlmResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock LLM listener");
        let addr = listener.local_addr().expect("mock LLM local addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let handle = tokio::spawn(async move {
            let mut script = script.into_iter();
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                if let Some(body) = read_request_body(&mut stream).await {
                    if let Ok(json) = serde_json::from_slice(&body) {
                        captured.lock().expect("requests lock").push(json);
                    }
                }
                let response = match script.next() {
                    Some(MockLlmResponse::Extraction(extraction)) => {
                        let envelope = serde_json::json!({
                            "id": "msg_mock",
                            "type": "message",
                            "role": "assistant",
                            "stop_reason": "end_turn",
                            "content": [{"type": "text", "text": extraction.to_string()}],
                        });
                        http_response(200, "OK", &envelope.to_string())
                    }
                    Some(MockLlmResponse::Status(code, body)) => {
                        http_response(code, "Mock Error", body)
                    }
                    None => http_response(500, "Mock Error", "mock LLM script exhausted"),
                };
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        Self {
            addr,
            requests,
            handle,
        }
    }

    /// Base URL for `ANTHROPIC_BASE_URL` (always loopback).
    #[must_use]
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// How many requests reached the mock so far.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn request_count(&self) -> usize {
        self.requests.lock().expect("requests lock").len()
    }

    /// Stop serving: the listener closes and later connection attempts to
    /// the port are refused. Captured requests remain readable.
    pub fn shutdown(&self) {
        self.handle.abort();
    }
}

impl Drop for MockLlm {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Read one HTTP/1.1 request from the stream and return its body, using the
/// `content-length` header. Returns `None` on malformed input.
async fn read_request_body(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > 65536 {
            return None;
        }
    };
    let headers = String::from_utf8_lossy(&buf[..header_end]).to_ascii_lowercase();
    let content_length: usize = headers
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse().ok())?;
    while buf.len() < header_end + content_length {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Some(buf[header_end..header_end + content_length].to_vec())
}

/// First index of `needle` in `haystack`.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Serialize a complete HTTP/1.1 response with `connection: close`.
fn http_response(status: u16, reason: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}
