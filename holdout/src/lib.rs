//! Black-box test harness for the `ChartPDS` holdout regression suite.
//!
//! The holdout suite drives the real `chartpds-mcp` server binary over stdio,
//! exactly as an MCP client would: it spawns the process against a throwaway
//! `CHARTPDS_DATA_DIR`, completes the MCP initialize handshake, and calls the
//! product-surface tools (`ingest_record`, `list_problems`, …), asserting on
//! the JSON they return.
//!
//! Binding to the tool surface — not `chartpds-core`'s churning Rust API — is
//! deliberate: it is the stable product contract, so fast internal refactors do
//! not legitimately break these tests. See
//! `docs/superpowers/specs/2026-06-27-holdout-regression-suite-design.md`.
//!
//! These tests are protected: see `holdout.lock`, `.github/allowed_signers`,
//! and the `holdout` CI workflow. Do not edit anything under `holdout/` to make
//! a failing test pass — a failure is a real regression. Fix the code instead.

use std::path::PathBuf;

use rmcp::model::{CallToolRequestParams, RawContent};
use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
use rmcp::{serve_client, service::RunningService, RoleClient};
use serde_json::Value;

/// A running `chartpds-mcp` server, driven over stdio as a child process.
///
/// Holds the temp data directory open for the server's lifetime; dropping the
/// `Harness` closes the stdio transport (the server sees EOF and exits) and
/// removes the data directory.
pub struct Harness {
    // Declared before `_data_dir` so it drops first: closing the stdio transport
    // lets the server exit before its data directory is removed.
    client: RunningService<RoleClient, ()>,
    // Removed on drop, once the server has exited.
    _data_dir: TempDir,
}

impl Harness {
    /// Spawn the `chartpds-mcp` binary against a fresh temporary data directory
    /// and complete the MCP initialize handshake.
    ///
    /// The server is started with no source credentials and the sync daemon
    /// disabled, so the harness exercises only the local ingest/query path.
    ///
    /// # Panics
    ///
    /// Panics if the `chartpds-mcp` binary cannot be located or spawned, or if
    /// the initialize handshake fails.
    pub async fn start() -> Self {
        let data_dir = TempDir::new().expect("create temp data dir");
        let data_path = data_dir.path().to_owned();
        let transport = TokioChildProcess::new(
            tokio::process::Command::new(server_binary()).configure(|cmd| {
                cmd.env("CHARTPDS_DATA_DIR", &data_path);
                // Disable the background sync daemon; the holdout suite drives
                // ingestion explicitly.
                cmd.env("CHARTPDS_SYNC_INTERVAL_SECS", "0");
                cmd.env_remove("GOOGLE_HEALTH_CLIENT_ID");
                cmd.env_remove("GOOGLE_HEALTH_CLIENT_SECRET");
                cmd.env_remove("OURA_PERSONAL_ACCESS_TOKEN");
            }),
        )
        .expect("spawn chartpds-mcp");
        let client = serve_client((), transport)
            .await
            .expect("MCP initialize handshake");
        Self {
            _data_dir: data_dir,
            client,
        }
    }

    /// Call an MCP tool by name with JSON `args` and return the parsed JSON the
    /// tool emitted as its text content.
    ///
    /// Pass [`Value::Null`] for tools that take no arguments; otherwise `args`
    /// must be a JSON object.
    ///
    /// # Panics
    ///
    /// Panics if `args` is neither an object nor null, if the tool call errors,
    /// if it returns no text content, or if that text is not valid JSON.
    pub async fn call_tool(&self, name: &'static str, args: Value) -> Value {
        let arguments = match args {
            Value::Object(map) => Some(map),
            Value::Null => None,
            other => panic!("tool arguments must be a JSON object or null, got: {other}"),
        };
        let mut param = CallToolRequestParams::new(name);
        param.arguments = arguments;
        let result = self
            .client
            .call_tool(param)
            .await
            .unwrap_or_else(|err| panic!("tool call {name} failed: {err}"));
        let text = result
            .content
            .iter()
            .find_map(|content| match &content.raw {
                RawContent::Text(text) => Some(text.text.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("tool {name} returned no text content"));
        serde_json::from_str(&text)
            .unwrap_or_else(|err| panic!("tool {name} text content is not JSON: {err}"))
    }

    /// Ingest an inline CCDA document via the `ingest_record` tool.
    ///
    /// # Panics
    ///
    /// Panics if ingestion fails (see [`Harness::call_tool`]).
    pub async fn ingest_ccda(&self, xml: &str) {
        self.call_tool(
            "ingest_record",
            serde_json::json!({
                "content": xml,
                "kind": "ccda",
                "source": "holdout",
            }),
        )
        .await;
    }
}

/// Read a fixture from the holdout crate's `fixtures/` directory.
///
/// # Panics
///
/// Panics if the fixture file cannot be read.
#[must_use]
pub fn fixture(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read fixture {}: {err}", path.display()))
}

/// Locate the built `chartpds-mcp` binary.
///
/// Honors a `CHARTPDS_MCP_BIN` override; otherwise discovers the binary next to
/// the test executable (`target/<profile>/chartpds-mcp`), which is where
/// `cargo test --workspace` builds it.
fn server_binary() -> PathBuf {
    if let Ok(path) = std::env::var("CHARTPDS_MCP_BIN") {
        return PathBuf::from(path);
    }
    let test_exe = std::env::current_exe().expect("locate current test executable");
    // test_exe = .../target/<profile>/deps/<test-bin>
    // binary   = .../target/<profile>/chartpds-mcp
    let profile_dir = test_exe
        .parent()
        .and_then(std::path::Path::parent)
        .expect("target profile directory");
    let bin = profile_dir.join(format!("chartpds-mcp{}", std::env::consts::EXE_SUFFIX));
    assert!(
        bin.exists(),
        "chartpds-mcp binary not found at {}; run the holdout suite via \
         `cargo test --workspace` (or `just check`) so the binary is built, or \
         set CHARTPDS_MCP_BIN to its path",
        bin.display()
    );
    bin
}

/// A throwaway directory, removed on drop.
///
/// A tiny, dependency-free stand-in for the `tempfile` crate. Keeping it out of
/// the crate's dependency graph matters here: `tempfile` pulls `getrandom` (and
/// its WASI shims) into the non-dev graph, which trips the workspace's
/// `cargo deny` duplicate-version ban. The holdout suite should carry the
/// smallest dependency footprint we can manage.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    /// Create a uniquely-named directory under the system temp dir.
    fn new() -> std::io::Result<Self> {
        use std::sync::atomic::{AtomicU32, Ordering};
        static COUNTER: AtomicU32 = AtomicU32::new(0);

        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let name = format!(
            "chartpds-holdout-{}-{nanos}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let path = std::env::temp_dir().join(name);
        std::fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
