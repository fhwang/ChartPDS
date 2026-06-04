//! The MCP server struct + tool handlers.

use chartpds_core::archive::Archive;
use chartpds_core::sources::oauth::OAuthConfig;
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Content, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler,
};
use sqlx::SqlitePool;
use time::format_description::well_known::Rfc3339;

/// Arguments for the `latest_observation_by_code` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct LatestObservationByCodeArgs {
    /// LOINC code to look up (e.g. `"29463-7"` for body weight).
    pub(crate) code: String,
}

/// Arguments for the `observations_in_range` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct ObservationsInRangeArgs {
    /// LOINC code to filter on.
    pub(crate) code: String,
    /// Inclusive start of the time window (RFC 3339).
    pub(crate) start: String,
    /// Exclusive end of the time window (RFC 3339).
    pub(crate) end: String,
}

/// Arguments for the `observation_counts` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct ObservationCountsArgs {}

/// Arguments for the `ingest_record` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct IngestRecordArgs {
    /// Absolute path to a CCDA file on disk. The server reads it directly.
    /// Provide either `file_path` or `content`, not both.
    pub(crate) file_path: Option<String>,
    /// The full CCDA XML content as a string (for small inline payloads).
    /// Provide either `file_path` or `content`, not both.
    pub(crate) content: Option<String>,
    /// Document kind. Currently only `"ccda"` is supported.
    pub(crate) kind: String,
    /// Source identifier (e.g. `"manual-upload"`, `"fitbit"`).
    pub(crate) source: String,
    /// Original filename, if known. Inferred from `file_path` if not provided.
    pub(crate) original_filename: Option<String>,
}

/// Arguments for the `connect_source` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct ConnectSourceArgs {
    /// Source name: `"fitbit"` or `"oura"`.
    pub(crate) source: String,
    /// Personal access token (required for `"oura"`; ignored for `"fitbit"`).
    pub(crate) token: Option<String>,
}

/// Arguments for the `sync_source` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct SyncSourceArgs {
    /// Source to sync. If omitted, syncs all configured sources.
    pub(crate) source: Option<String>,
    /// Number of recent days to sync (defaults to 8).
    pub(crate) window_days: Option<i64>,
}

/// Arguments for the `list_notifications` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct ListNotificationsArgs {
    /// Max number of notifications to return (default 20).
    pub(crate) limit: Option<i64>,
}

/// `ChartPDS` MCP server.
///
/// Holds the shared `SqlitePool`, the blob [`Archive`], and an `rmcp`
/// `ToolRouter`. Each tool is an `async fn` on this impl annotated with
/// `#[tool(description = "...")]`.
#[derive(Clone)]
pub(crate) struct ChartPdsServer {
    pool: SqlitePool,
    archive: Archive,
    oauth_config: Option<OAuthConfig>,
    http_client: reqwest::Client,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl ChartPdsServer {
    pub(crate) fn new(
        pool: SqlitePool,
        archive: Archive,
        oauth_config: Option<OAuthConfig>,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            pool,
            archive,
            oauth_config,
            http_client,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        description = "Get the most-recent observation for a given LOINC code. Returns null if no observation matches."
    )]
    async fn latest_observation_by_code(
        &self,
        Parameters(args): Parameters<LatestObservationByCodeArgs>,
    ) -> Result<CallToolResult, McpError> {
        let observation = chartpds_core::queries::latest_by_code(&self.pool, &args.code)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;

        let json = serde_json::to_string(&observation)
            .map_err(|err| McpError::internal_error(format!("serializing result: {err}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "List observations matching a LOINC code with effective_start in [start, end). Returns a JSON array."
    )]
    async fn observations_in_range(
        &self,
        Parameters(args): Parameters<ObservationsInRangeArgs>,
    ) -> Result<CallToolResult, McpError> {
        let start = time::OffsetDateTime::parse(&args.start, &Rfc3339)
            .map_err(|err| McpError::invalid_params(format!("invalid start: {err}"), None))?;
        let end = time::OffsetDateTime::parse(&args.end, &Rfc3339)
            .map_err(|err| McpError::invalid_params(format!("invalid end: {err}"), None))?;

        let rows = chartpds_core::queries::in_range(&self.pool, &args.code, start, end)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&rows)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Count observations grouped by LOINC code. Returns [{coding_code, count}] ordered alphabetically."
    )]
    async fn observation_counts(
        &self,
        Parameters(_args): Parameters<ObservationCountsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let counts = chartpds_core::queries::counts_per_code(&self.pool)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&counts)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "List all problems (diagnoses) in the index. Returns a JSON array of problems, ordered by onset date."
    )]
    async fn list_problems(
        &self,
        Parameters(_args): Parameters<ObservationCountsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let problems = chartpds_core::queries::list_problems(&self.pool)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&problems)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "List all medications in the index. Returns a JSON array of medications, ordered by start date."
    )]
    async fn list_medications(
        &self,
        Parameters(_args): Parameters<ObservationCountsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let meds = chartpds_core::queries::list_medications(&self.pool)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&meds)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Ingest a CCDA document. Provide either file_path (server reads the file directly) or content (inline XML string). Archives the blob, parses it, and indexes observations, problems, and medications. Returns the source_document id."
    )]
    async fn ingest_record(
        &self,
        Parameters(args): Parameters<IngestRecordArgs>,
    ) -> Result<CallToolResult, McpError> {
        if args.kind != "ccda" {
            return Err(McpError::invalid_params(
                format!(
                    "unsupported kind {:?}; only \"ccda\" is supported",
                    args.kind
                ),
                None,
            ));
        }

        let (content, original_filename) = match (args.file_path, args.content) {
            (Some(path), None) => {
                let data = std::fs::read(&path).map_err(|err| {
                    McpError::invalid_params(format!("could not read file {path:?}: {err}"), None)
                })?;
                let filename = args.original_filename.or_else(|| {
                    std::path::Path::new(&path)
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                });
                (bytes::Bytes::from(data), filename)
            }
            (None, Some(text)) => (bytes::Bytes::from(text), args.original_filename),
            (Some(_), Some(_)) => {
                return Err(McpError::invalid_params(
                    "provide either file_path or content, not both",
                    None,
                ));
            }
            (None, None) => {
                return Err(McpError::invalid_params(
                    "provide either file_path or content",
                    None,
                ));
            }
        };

        let source_document_id = chartpds_core::ingestion::ingest(
            &self.archive,
            &self.pool,
            content,
            &args.kind,
            &args.source,
            original_filename.as_deref(),
            time::OffsetDateTime::now_utc(),
        )
        .await
        .map_err(|err| McpError::internal_error(format!("ingestion failed: {err}"), None))?;

        let result = serde_json::json!({ "source_document_id": source_document_id });
        let json = serde_json::to_string(&result)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Connect a data source. For fitbit: starts OAuth flow and returns an authorization URL to open in a browser. For oura: stores the personal access token (pass it in the 'token' parameter)."
    )]
    async fn connect_source(
        &self,
        Parameters(args): Parameters<ConnectSourceArgs>,
    ) -> Result<CallToolResult, McpError> {
        match args.source.as_str() {
            "fitbit" => {
                let oauth_config = self.oauth_config.as_ref().ok_or_else(|| {
                    McpError::invalid_params(
                        "GOOGLE_HEALTH_CLIENT_ID and GOOGLE_HEALTH_CLIENT_SECRET must be set",
                        None,
                    )
                })?;
                crate::oauth_callback::spawn_callback_listener(
                    self.pool.clone(),
                    self.http_client.clone(),
                    oauth_config.clone(),
                );
                let url = format!(
                    "https://accounts.google.com/o/oauth2/v2/auth\
                     ?client_id={client_id}\
                     &redirect_uri={redirect_uri}\
                     &response_type=code\
                     &scope=https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly\
                     &access_type=offline\
                     &prompt=consent",
                    client_id = oauth_config.client_id,
                    redirect_uri = crate::oauth_callback::REDIRECT_URI,
                );
                Ok(CallToolResult::success(vec![Content::text(format!(
                    "Open this URL in your browser to authorize:\n\n{url}\n\n\
                     After authorizing, the browser will redirect to localhost and \
                     the server will automatically store your credentials."
                ))]))
            }
            "oura" => {
                let token = args.token.ok_or_else(|| {
                    McpError::invalid_params(
                        "token parameter is required for oura (get a PAT from https://cloud.ouraring.com/personal-access-tokens)",
                        None,
                    )
                })?;
                let credentials_json = serde_json::json!({ "access_token": token }).to_string();
                let now_str = time::OffsetDateTime::now_utc()
                    .format(&Rfc3339)
                    .unwrap_or_default();
                chartpds_core::index::upsert_source_credentials(
                    &self.pool,
                    chartpds_core::index::UpsertSourceCredentialsParams {
                        source_name: "oura",
                        credentials_json: &credentials_json,
                        updated_at: &now_str,
                    },
                )
                .await
                .map_err(|err| {
                    McpError::internal_error(format!("storing credentials: {err}"), None)
                })?;
                Ok(CallToolResult::success(vec![Content::text(
                    "Oura credentials stored successfully. You can now call sync_source with source=\"oura\".",
                )]))
            }
            other => Err(McpError::invalid_params(
                format!("unknown source {other:?}; known sources: fitbit, oura"),
                None,
            )),
        }
    }

    #[tool(
        description = "Drop and rebuild the entire index from archived blobs. Re-ingests every CCDA document; skips non-CCDA blobs (e.g. adapter JSON). Run sync_source after to restore adapter data. Returns {blobs_found, documents_ingested, blobs_skipped}."
    )]
    async fn rebuild_index(
        &self,
        Parameters(_args): Parameters<ObservationCountsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let result = chartpds_core::ingestion::rebuild_index(&self.archive, &self.pool)
            .await
            .map_err(|err| McpError::internal_error(format!("rebuild failed: {err}"), None))?;
        let json = serde_json::to_string(&result)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Sync a data source (or all configured sources). Fetches recent data, archives it, and indexes observations. Returns a summary of days synced and samples collected."
    )]
    async fn sync_source(
        &self,
        Parameters(args): Parameters<SyncSourceArgs>,
    ) -> Result<CallToolResult, McpError> {
        let window_days = args.window_days.unwrap_or(8);

        match args.source.as_deref() {
            Some("fitbit") => self.sync_fitbit_inner(window_days).await,
            Some("oura") => self.sync_oura_inner(window_days).await,
            Some(other) => Err(McpError::invalid_params(
                format!("unknown source {other:?}; known sources: fitbit, oura"),
                None,
            )),
            None => {
                // Sync all configured sources, collect results.
                let mut summaries: Vec<String> = Vec::new();
                let mut errors: Vec<String> = Vec::new();

                // Try Fitbit (only if oauth_config is present).
                if self.oauth_config.is_some() {
                    match self.sync_fitbit_inner(window_days).await {
                        Ok(r) => summaries.push(format!("fitbit: {}", Self::result_text(&r))),
                        Err(e) => errors.push(format!("fitbit: {}", e.message)),
                    }
                }

                // Try Oura (only if a token is resolvable).
                if self.resolve_oura_token().await.is_ok() {
                    match self.sync_oura_inner(window_days).await {
                        Ok(r) => summaries.push(format!("oura: {}", Self::result_text(&r))),
                        Err(e) => errors.push(format!("oura: {}", e.message)),
                    }
                }

                if summaries.is_empty() && errors.is_empty() {
                    return Ok(CallToolResult::success(vec![Content::text(
                        "No sources configured. Use connect_source to set up fitbit or oura.",
                    )]));
                }

                let mut parts: Vec<String> = Vec::new();
                if !summaries.is_empty() {
                    parts.push(summaries.join("\n"));
                }
                if !errors.is_empty() {
                    parts.push(format!("Errors:\n{}", errors.join("\n")));
                }
                Ok(CallToolResult::success(vec![Content::text(
                    parts.join("\n\n"),
                )]))
            }
        }
    }

    #[tool(
        description = "List recent notification log entries (auth failures, sync problems). Returns newest first."
    )]
    async fn list_notifications(
        &self,
        Parameters(args): Parameters<ListNotificationsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = args.limit.unwrap_or(20);
        let entries = chartpds_core::queries::list_recent_notifications(&self.pool, limit)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&entries)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }
}

// ── Private helpers ──────────────────────────────────────────────────

impl ChartPdsServer {
    /// Look up the Oura PAT from `source_credentials` (set by
    /// `connect_source`). Falls back to checking the environment for
    /// `OURA_PERSONAL_ACCESS_TOKEN` as a convenience for initial setup.
    async fn resolve_oura_token(&self) -> Result<String, McpError> {
        // Try source_credentials first.
        if let Ok(Some(creds)) =
            chartpds_core::index::get_source_credentials(&self.pool, "oura").await
        {
            let parsed: serde_json::Value =
                serde_json::from_str(&creds.credentials_json).map_err(|err| {
                    McpError::internal_error(format!("parsing oura credentials: {err}"), None)
                })?;
            if let Some(token) = parsed.get("access_token").and_then(|v| v.as_str()) {
                return Ok(token.to_owned());
            }
        }

        // Fall back to environment variable.
        std::env::var("OURA_PERSONAL_ACCESS_TOKEN").map_err(|_| {
            McpError::invalid_params(
                "No Oura PAT found. Call connect_source with source=\"oura\" first or set OURA_PERSONAL_ACCESS_TOKEN.",
                None,
            )
        })
    }

    /// Sync Fitbit heart-rate data for the given window.
    async fn sync_fitbit_inner(&self, window_days: i64) -> Result<CallToolResult, McpError> {
        let oauth_config = self.oauth_config.as_ref().ok_or_else(|| {
            McpError::invalid_params(
                "GOOGLE_HEALTH_CLIENT_ID and GOOGLE_HEALTH_CLIENT_SECRET must be set",
                None,
            )
        })?;

        let result = chartpds_core::sources::fitbit::sync::sync_recent_days(
            &self.archive,
            &self.pool,
            &self.http_client,
            oauth_config,
            window_days,
        )
        .await
        .map_err(|err| McpError::internal_error(format!("sync failed: {err}"), None))?;

        let json = serde_json::json!({
            "days_synced": result.days_synced,
            "total_samples": result.total_samples,
        });
        let text = serde_json::to_string(&json)
            .map_err(|err| McpError::internal_error(format!("serializing result: {err}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    /// Sync Oura sleep data for the given window.
    async fn sync_oura_inner(&self, window_days: i64) -> Result<CallToolResult, McpError> {
        let access_token = self.resolve_oura_token().await?;

        let result = chartpds_core::sources::oura::sync::sync_recent_days(
            &self.archive,
            &self.pool,
            &self.http_client,
            &access_token,
            window_days,
        )
        .await
        .map_err(|err| McpError::internal_error(format!("oura sync failed: {err}"), None))?;

        let json = serde_json::json!({
            "days_synced": result.days_synced,
            "total_samples": result.total_samples,
        });
        let text = serde_json::to_string(&json)
            .map_err(|err| McpError::internal_error(format!("serializing result: {err}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    /// Extract the text content from a [`CallToolResult`] for summary building.
    fn result_text(result: &CallToolResult) -> String {
        result
            .content
            .first()
            .and_then(|c| match &c.raw {
                rmcp::model::RawContent::Text(t) => Some(t.text.clone()),
                _ => None,
            })
            .unwrap_or_default()
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for ChartPdsServer {
    fn get_info(&self) -> ServerInfo {
        // `ServerInfo` (aliased to `InitializeResult`) is `#[non_exhaustive]`
        // in rmcp 1.7, so we build it via `new(...).with_instructions(...)`
        // instead of a struct literal.
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "ChartPDS personal data store. Ingest clinical documents and query observations.",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chartpds_core::archive::BlobKey;
    use chartpds_core::index::{
        insert_observation, insert_source_document, open_pool, InsertObservationParams,
        InsertSourceDocumentParams,
    };
    use object_store::memory::InMemory;
    use std::sync::Arc;
    use time::macros::datetime;
    use time::OffsetDateTime;

    async fn fresh_server_with_empty_db() -> ChartPdsServer {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        ChartPdsServer::new(pool, archive, None, reqwest::Client::new())
    }

    async fn fresh_server_with_one_weight() -> ChartPdsServer {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");

        let key = BlobKey::from_hex_str(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .expect("valid key");
        let doc_id = insert_source_document(
            &pool,
            InsertSourceDocumentParams {
                archive_key: &key,
                kind: "ccda",
                source: "test",
                original_filename: None,
                ingested_at: OffsetDateTime::now_utc(),
            },
        )
        .await
        .expect("doc");
        insert_observation(
            &pool,
            InsertObservationParams {
                source_document_id: doc_id,
                coding_system: "http://loinc.org",
                coding_code: "29463-7",
                coding_display: Some("Body Weight"),
                effective_start: datetime!(2026-01-01 12:00:00 UTC),
                effective_end: None,
                value_quantity: Some(72.5),
                value_string: None,
                value_unit: Some("kg"),
            },
        )
        .await
        .expect("obs");

        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        ChartPdsServer::new(pool, archive, None, reqwest::Client::new())
    }

    #[tokio::test]
    async fn latest_observation_by_code_returns_the_match() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .latest_observation_by_code(Parameters(LatestObservationByCodeArgs {
                code: "29463-7".to_string(),
            }))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["coding_code"], "29463-7");
        assert_eq!(value["value_quantity"], 72.5);
        assert_eq!(value["value_unit"], "kg");
    }

    #[tokio::test]
    async fn latest_observation_by_code_returns_null_when_no_match() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .latest_observation_by_code(Parameters(LatestObservationByCodeArgs {
                code: "no-such-code".to_string(),
            }))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        assert_eq!(text, "null");
    }

    #[tokio::test]
    async fn observations_in_range_returns_match_in_window() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .observations_in_range(Parameters(ObservationsInRangeArgs {
                code: "29463-7".to_string(),
                start: "2025-12-01T00:00:00Z".to_string(),
                end: "2026-02-01T00:00:00Z".to_string(),
            }))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        let arr = value.as_array().expect("expected array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["coding_code"], "29463-7");
        assert_eq!(arr[0]["value_quantity"], 72.5);
    }

    #[tokio::test]
    async fn observations_in_range_returns_empty_outside_window() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .observations_in_range(Parameters(ObservationsInRangeArgs {
                code: "29463-7".to_string(),
                start: "2030-01-01T00:00:00Z".to_string(),
                end: "2030-12-01T00:00:00Z".to_string(),
            }))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        assert_eq!(text, "[]");
    }

    #[tokio::test]
    async fn observation_counts_returns_one_entry() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .observation_counts(Parameters(ObservationCountsArgs {}))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        assert_eq!(text, r#"[{"coding_code":"29463-7","count":1}]"#);
    }

    #[tokio::test]
    async fn ingest_record_returns_source_document_id() {
        let server = fresh_server_with_empty_db().await;
        let ccda =
            include_str!("../../chartpds-core/src/ingestion/ccda/fixtures/valid_minimal.xml");

        let result = server
            .ingest_record(Parameters(IngestRecordArgs {
                file_path: None,
                content: Some(ccda.to_owned()),
                kind: "ccda".to_owned(),
                source: "test".to_owned(),
                original_filename: Some("ccd.xml".to_owned()),
            }))
            .await
            .expect("ingest tool call");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).unwrap();
        assert!(value["source_document_id"].is_number());
    }

    #[tokio::test]
    async fn ingest_then_query_round_trips() {
        let server = fresh_server_with_empty_db().await;
        let ccda =
            include_str!("../../chartpds-core/src/ingestion/ccda/fixtures/valid_minimal.xml");

        // Ingest
        server
            .ingest_record(Parameters(IngestRecordArgs {
                file_path: None,
                content: Some(ccda.to_owned()),
                kind: "ccda".to_owned(),
                source: "test".to_owned(),
                original_filename: None,
            }))
            .await
            .expect("ingest");

        // Query body weight
        let result = server
            .latest_observation_by_code(Parameters(LatestObservationByCodeArgs {
                code: "29463-7".to_owned(),
            }))
            .await
            .expect("query");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(value["coding_code"], "29463-7");
        assert_eq!(value["value_quantity"], 72.5);
    }

    #[tokio::test]
    async fn list_problems_returns_ingested_problems() {
        let server = fresh_server_with_empty_db().await;
        let ccda =
            include_str!("../../chartpds-core/src/ingestion/ccda/fixtures/valid_minimal.xml");

        server
            .ingest_record(Parameters(IngestRecordArgs {
                file_path: None,
                content: Some(ccda.to_owned()),
                kind: "ccda".to_owned(),
                source: "test".to_owned(),
                original_filename: None,
            }))
            .await
            .expect("ingest");

        let result = server
            .list_problems(Parameters(ObservationCountsArgs {}))
            .await
            .expect("list_problems tool call");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        let arr = value.as_array().expect("expected array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["coding_code"], "44054006");
    }

    #[tokio::test]
    async fn list_medications_returns_ingested_medications() {
        let server = fresh_server_with_empty_db().await;
        let ccda =
            include_str!("../../chartpds-core/src/ingestion/ccda/fixtures/valid_minimal.xml");

        server
            .ingest_record(Parameters(IngestRecordArgs {
                file_path: None,
                content: Some(ccda.to_owned()),
                kind: "ccda".to_owned(),
                source: "test".to_owned(),
                original_filename: None,
            }))
            .await
            .expect("ingest");

        let result = server
            .list_medications(Parameters(ObservationCountsArgs {}))
            .await
            .expect("list_medications tool call");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        let arr = value.as_array().expect("expected array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["coding_code"], "860975");
    }

    #[tokio::test]
    async fn list_notifications_returns_seeded_entry() {
        let server = fresh_server_with_empty_db().await;

        // Manually append a notification log entry.
        chartpds_core::index::append_notification_log(
            &server.pool,
            "auth_expired:fitbit",
            "2026-01-15T10:00:00Z",
            "critical",
            "ChartPDS: Fitbit re-authorization required",
            "The Fitbit adapter needs re-authorization.",
        )
        .await
        .expect("append");

        let result = server
            .list_notifications(Parameters(ListNotificationsArgs { limit: Some(10) }))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        let arr = value.as_array().expect("expected array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["condition_id"], "auth_expired:fitbit");
        assert_eq!(arr[0]["severity"], "critical");
    }

    #[tokio::test]
    async fn rebuild_index_re_ingests_archived_ccda() {
        let server = fresh_server_with_empty_db().await;
        let ccda =
            include_str!("../../chartpds-core/src/ingestion/ccda/fixtures/valid_minimal.xml");

        // Ingest a CCDA first (puts it in the archive).
        server
            .ingest_record(Parameters(IngestRecordArgs {
                file_path: None,
                content: Some(ccda.to_owned()),
                kind: "ccda".to_owned(),
                source: "test".to_owned(),
                original_filename: None,
            }))
            .await
            .expect("ingest");

        // Rebuild the index.
        let result = server
            .rebuild_index(Parameters(ObservationCountsArgs {}))
            .await
            .expect("rebuild_index tool call");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["blobs_found"], 1);
        assert_eq!(value["documents_ingested"], 1);
        assert_eq!(value["blobs_skipped"], 0);

        // Observations should still be present after the rebuild.
        let obs_result = server
            .observation_counts(Parameters(ObservationCountsArgs {}))
            .await
            .expect("observation_counts after rebuild");

        let obs_text = match &obs_result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let obs_value: serde_json::Value = serde_json::from_str(obs_text).expect("valid JSON");
        let arr = obs_value.as_array().expect("expected array");
        assert!(!arr.is_empty(), "observations should survive rebuild");
    }

    #[tokio::test]
    async fn connect_source_oura_stores_credentials() {
        let server = fresh_server_with_empty_db().await;

        let result = server
            .connect_source(Parameters(ConnectSourceArgs {
                source: "oura".to_owned(),
                token: Some("test-pat-abc123".to_owned()),
            }))
            .await
            .expect("connect_source oura succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        assert!(text.contains("stored successfully"), "got: {text}");

        // Verify credentials are in the database.
        let creds = chartpds_core::index::get_source_credentials(&server.pool, "oura")
            .await
            .expect("get succeeds")
            .expect("row exists");
        let parsed: serde_json::Value =
            serde_json::from_str(&creds.credentials_json).expect("valid JSON");
        assert_eq!(parsed["access_token"], "test-pat-abc123");
    }

    #[tokio::test]
    async fn connect_source_unknown_returns_error() {
        let server = fresh_server_with_empty_db().await;

        let err = server
            .connect_source(Parameters(ConnectSourceArgs {
                source: "unknown".to_owned(),
                token: None,
            }))
            .await
            .expect_err("should fail for unknown source");

        assert!(
            err.message.contains("unknown source"),
            "got: {}",
            err.message
        );
    }

    #[tokio::test]
    async fn sync_source_unknown_returns_error() {
        let server = fresh_server_with_empty_db().await;

        let err = server
            .sync_source(Parameters(SyncSourceArgs {
                source: Some("unknown".to_owned()),
                window_days: None,
            }))
            .await
            .expect_err("should fail for unknown source");

        assert!(
            err.message.contains("unknown source"),
            "got: {}",
            err.message
        );
    }
}
