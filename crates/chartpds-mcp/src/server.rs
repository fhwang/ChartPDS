//! The MCP server struct + tool handlers.
//!
//! The tool surface is defined entirely by the `#[tool(...)]`-annotated
//! methods in the `#[tool_router]` block below; their `description` strings
//! are the canonical per-tool documentation (MCP clients receive them
//! verbatim), so they are not re-enumerated anywhere else.
//!
//! Adding a tool: define an `async fn` on the `ChartPdsServer` impl inside
//! the `#[tool_router]` block, annotate it with
//! `#[tool(description = "...")]`, take args via `Parameters<YourArgs>`
//! where `YourArgs: Deserialize + JsonSchema`, and return
//! `Result<CallToolResult, McpError>`. Test by constructing the server
//! directly in a `#[tokio::test]` and calling the method — no stdio
//! transport needed.

use chartpds_core::archive::Archive;
use chartpds_core::ingestion::NarrativeIngestParams;
use chartpds_core::sources::oauth::OAuthConfig;
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, Content, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler,
};
use sqlx::SqlitePool;
use time::format_description::well_known::Rfc3339;

/// Arguments for the `observation_latest` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct ObservationLatestArgs {
    /// Coding to look up (e.g. `{system: "http://loinc.org", code:
    /// "29463-7"}` for body weight).
    pub(crate) coding: Coding,
}

/// Arguments for the `observation_history` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct ObservationHistoryArgs {
    /// One or more codings to read. Each is `{system, code}`.
    pub(crate) codings: Vec<Coding>,
    /// Optional inclusive lower bound on `effective_start` (RFC 3339).
    pub(crate) start: Option<String>,
    /// Optional exclusive upper bound on `effective_start` (RFC 3339).
    pub(crate) end: Option<String>,
}

/// Arguments for tools that take none.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct EmptyArgs {}

/// Arguments for the `record_ingest` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct RecordIngestArgs {
    /// Absolute path to a CCDA file on disk. The server reads it directly.
    /// Provide either `file_path` or `content`, not both.
    pub(crate) file_path: Option<String>,
    /// The full CCDA XML content as a string (for small inline payloads).
    /// Provide either `file_path` or `content`, not both. Not usable for
    /// `kind="clinical-pdf"`: binary PDF bytes cannot be passed through this
    /// string parameter — use `file_path` instead.
    pub(crate) content: Option<String>,
    /// Document kind: "ccda" or "clinical-pdf".
    pub(crate) kind: String,
    /// Source identifier (e.g. `"manual-upload"`, `"fitbit"`).
    pub(crate) source: String,
    /// Original filename, if known. Inferred from `file_path` if not provided.
    pub(crate) original_filename: Option<String>,
}

/// Arguments for the `source_connect` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct SourceConnectArgs {
    /// Source name: `"fitbit"` or `"oura"`.
    pub(crate) source: String,
    /// Personal access token (required for `"oura"`; ignored for `"fitbit"`).
    pub(crate) token: Option<String>,
}

/// Arguments for the `source_sync` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct SourceSyncArgs {
    /// Source to sync. If omitted, syncs all configured sources.
    pub(crate) source: Option<String>,
    /// Number of recent days to sync (defaults to 8).
    pub(crate) window_days: Option<i64>,
}

/// Arguments for the `notification_list` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct NotificationListArgs {
    /// Max number of notifications to return (default 20).
    pub(crate) limit: Option<i64>,
}

/// Arguments for the `narrative_search` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct NarrativeSearchArgs {
    /// FTS5 full-text query (e.g. `"biopsy proctitis"`). Omit to list the
    /// full narrative catalog, newest first.
    pub(crate) query: Option<String>,
    /// Maximum results (default 20).
    pub(crate) limit: Option<i64>,
}

/// Arguments for the `narrative_get` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct NarrativeGetArgs {
    /// The narrative's `source_document_id` (from `narrative_search`).
    pub(crate) source_document_id: i64,
}

/// A coding selector: FHIR system URI plus code.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct Coding {
    /// FHIR system URI (e.g. `"http://loinc.org"` or the AASM sleep-stage URI).
    pub(crate) system: String,
    /// Code within the system (e.g. `"8867-4"`).
    pub(crate) code: String,
}

/// Arguments for the `observation_stats` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct ObservationStatsArgs {
    /// Coding to aggregate over.
    pub(crate) coding: Coding,
    /// Inclusive start of the time window (RFC 3339). Omit for an
    /// open-ended (all-time) lower bound.
    pub(crate) start: Option<String>,
    /// Exclusive end of the time window (RFC 3339). Omit to default to now.
    pub(crate) end: Option<String>,
    /// Field to aggregate: `"value"` (default), `"start_time_of_day"`,
    /// `"end_time_of_day"` (minutes since local noon, `[0, 1440)`), or
    /// `"interval_minutes"`.
    pub(crate) field: Option<String>,
    /// Bucketing: `"none"` (default), `"hour"`, `"day"`, `"week"` (ISO,
    /// keyed by Monday), `"month"`, `"day_of_week"` (`mon` … `sun`), or
    /// `"episode"` (per gap-tolerant chain of the `episode` spec coding's
    /// interval observations, e.g. one sleep period, keyed by the episode's
    /// RFC 3339 UTC start instant).
    pub(crate) bucket: Option<String>,
    /// IANA timezone (e.g. `"America/New_York"`) governing bucket
    /// boundaries and time-of-day derivation. Omit for UTC.
    pub(crate) timezone: Option<String>,
    /// Optional thresholds; each reports counts below / at-or-above.
    pub(crate) thresholds: Option<Vec<f64>>,
    /// Episode definition; required when `bucket` is `"episode"`, invalid
    /// otherwise.
    pub(crate) episode: Option<EpisodeArgs>,
}

/// One requested column for the `observation_table` and
/// `observation_relationship` tools.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct TableColumnArgs {
    /// Coding to aggregate over.
    pub(crate) coding: Coding,
    /// Aggregate: `"mean"` (default), `"sum"`, `"min"`, `"max"`, `"count"`,
    /// `"median"`, `"duration_in_range"` (minutes the coding's intervals
    /// spent inside `[value_min, value_max]`), or `"longest_run_in_range"`
    /// (minutes of the longest run of in-range intervals, gap-tolerant via
    /// `gap_seconds`).
    pub(crate) aggregate: Option<String>,
    /// Field the value aggregates operate on: `"value"` (default),
    /// `"start_time_of_day"`, `"end_time_of_day"`, or `"interval_minutes"`.
    /// Ignored by the range aggregates (`"duration_in_range"`,
    /// `"longest_run_in_range"`).
    pub(crate) field: Option<String>,
    /// Minimum `value_quantity` (inclusive). Required for (and only valid
    /// with) `aggregate:"duration_in_range"` or `"longest_run_in_range"`.
    pub(crate) value_min: Option<f64>,
    /// Maximum `value_quantity` (inclusive). Required for (and only valid
    /// with) `aggregate:"duration_in_range"` or `"longest_run_in_range"`.
    pub(crate) value_max: Option<f64>,
    /// Allowed gap, in seconds, between consecutive in-range intervals that
    /// still counts as one continuous run. Only valid with
    /// `aggregate:"longest_run_in_range"`; defaults to 0.
    pub(crate) gap_seconds: Option<i64>,
}

/// Episode definition shared by the stats/table/relationship args: the
/// coding whose interval observations define the episodes (e.g. the AASM
/// sleep-stage coding for sleep periods).
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct EpisodeArgs {
    /// Coding whose interval observations define the episodes.
    pub(crate) coding: Coding,
    /// Allowed gap, in seconds, between consecutive intervals that still
    /// chains them into one episode. Defaults to 0.
    pub(crate) gap_seconds: Option<i64>,
}

/// Arguments for the `observation_table` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct ObservationTableArgs {
    /// Requested columns, in output order.
    pub(crate) columns: Vec<TableColumnArgs>,
    /// Inclusive start of the time window (RFC 3339). Omit for an
    /// open-ended (all-time) lower bound.
    pub(crate) start: Option<String>,
    /// Exclusive end of the time window (RFC 3339). Omit to default to now.
    pub(crate) end: Option<String>,
    /// Bucketing: `"none"`, `"hour"`, `"day"` (default), `"week"` (ISO,
    /// keyed by Monday), `"month"`, `"day_of_week"` (`mon` … `sun`), or
    /// `"episode"` (per gap-tolerant chain of the `episode` spec coding's
    /// interval observations, e.g. one sleep period, keyed by the episode's
    /// RFC 3339 UTC start instant).
    pub(crate) bucket: Option<String>,
    /// IANA timezone (e.g. `"America/New_York"`) governing calendar bucket
    /// boundaries and time-of-day fields. Omit for UTC.
    pub(crate) timezone: Option<String>,
    /// Episode definition; required when `bucket` is `"episode"`, invalid
    /// otherwise.
    pub(crate) episode: Option<EpisodeArgs>,
}

/// Arguments for the `observation_relationship` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct ObservationRelationshipArgs {
    /// The first signal (the "exposure"), e.g. daily activity.
    pub(crate) x: TableColumnArgs,
    /// The second signal (the "outcome"), e.g. nightly sleep.
    pub(crate) y: TableColumnArgs,
    /// Inclusive start of the time window (RFC 3339). Omit for an
    /// open-ended (all-time) lower bound.
    pub(crate) start: Option<String>,
    /// Exclusive end of the time window (RFC 3339). Omit to default to now.
    pub(crate) end: Option<String>,
    /// Bucketing: `"hour"`, `"day"` (default), `"week"` (ISO), `"month"`,
    /// or `"episode"` (per gap-tolerant chain of the `episode` spec
    /// coding's interval observations; episode `i` pairs with episode
    /// `i + lag` by index, not by calendar arithmetic).
    pub(crate) bucket: Option<String>,
    /// IANA timezone governing bucket boundaries. Omit for UTC.
    pub(crate) timezone: Option<String>,
    /// Pair `x` at bucket `t` with `y` at bucket `t + lag` (in buckets).
    /// Defaults to 0 (same bucket); `1` pairs each `x` with the following
    /// bucket's `y`. May be negative.
    pub(crate) lag: Option<i64>,
    /// Optional threshold on `x`: also report `y` summary statistics for
    /// pairs with `x` strictly below vs. at-or-above it.
    pub(crate) threshold: Option<f64>,
    /// Episode definition; required when `bucket` is `"episode"`, invalid
    /// otherwise.
    pub(crate) episode: Option<EpisodeArgs>,
}

/// Validate an inclusive value range: `value_min <= value_max`.
fn validate_range_bounds(value_min: f64, value_max: f64) -> Result<(), McpError> {
    if value_min > value_max {
        return Err(McpError::invalid_params(
            "value_min must be <= value_max".to_string(),
            None,
        ));
    }
    Ok(())
}

/// Validate and default a column's `gap_seconds`. Only valid (and defaulted
/// to `0`) when `allowed` is true; an unexpected value on a non-run
/// aggregate is an error.
fn parse_gap_seconds(gap_seconds: Option<i64>, allowed: bool) -> Result<i64, McpError> {
    if !allowed {
        if gap_seconds.is_some() {
            return Err(McpError::invalid_params(
                "gap_seconds is only valid with aggregate \"longest_run_in_range\"".to_string(),
                None,
            ));
        }
        return Ok(0);
    }
    let gap_seconds = gap_seconds.unwrap_or(0);
    if gap_seconds < 0 {
        return Err(McpError::invalid_params(
            "gap_seconds must be >= 0".to_string(),
            None,
        ));
    }
    Ok(gap_seconds)
}

/// Parse a column's aggregate/field strings into a core [`ColumnSpec`],
/// validating the `duration_in_range` / `longest_run_in_range` value-bound
/// and `gap_seconds` rules.
///
/// [`ColumnSpec`]: chartpds_core::queries::ColumnSpec
fn parse_column(
    args: &TableColumnArgs,
) -> Result<chartpds_core::queries::ColumnSpec<'_>, McpError> {
    let aggregate = match args.aggregate.as_deref() {
        Some("duration_in_range") => {
            let (Some(value_min), Some(value_max)) = (args.value_min, args.value_max) else {
                return Err(McpError::invalid_params(
                    "aggregate \"duration_in_range\" requires value_min and value_max".to_string(),
                    None,
                ));
            };
            validate_range_bounds(value_min, value_max)?;
            parse_gap_seconds(args.gap_seconds, false)?;
            chartpds_core::queries::ColumnAggregate::DurationInRange {
                value_min,
                value_max,
            }
        }
        Some("longest_run_in_range") => {
            let (Some(value_min), Some(value_max)) = (args.value_min, args.value_max) else {
                return Err(McpError::invalid_params(
                    "aggregate \"longest_run_in_range\" requires value_min and value_max"
                        .to_string(),
                    None,
                ));
            };
            validate_range_bounds(value_min, value_max)?;
            let gap_seconds = parse_gap_seconds(args.gap_seconds, true)?;
            chartpds_core::queries::ColumnAggregate::LongestRunInRange {
                value_min,
                value_max,
                gap_seconds,
            }
        }
        other => {
            if args.value_min.is_some() || args.value_max.is_some() {
                return Err(McpError::invalid_params(
                    "value_min/value_max are only valid with aggregate \"duration_in_range\" or \"longest_run_in_range\""
                        .to_string(),
                    None,
                ));
            }
            parse_gap_seconds(args.gap_seconds, false)?;
            match other {
                None | Some("mean") => chartpds_core::queries::ColumnAggregate::Mean,
                Some("sum") => chartpds_core::queries::ColumnAggregate::Sum,
                Some("min") => chartpds_core::queries::ColumnAggregate::Min,
                Some("max") => chartpds_core::queries::ColumnAggregate::Max,
                Some("count") => chartpds_core::queries::ColumnAggregate::Count,
                Some("median") => chartpds_core::queries::ColumnAggregate::Median,
                Some(unknown) => {
                    return Err(McpError::invalid_params(
                        format!(
                            "invalid aggregate {unknown:?}; expected \"mean\", \"sum\", \"min\", \"max\", \"count\", \"median\", \"duration_in_range\", or \"longest_run_in_range\""
                        ),
                        None,
                    ))
                }
            }
        }
    };
    let field = match args.field.as_deref() {
        None | Some("value") => chartpds_core::queries::StatsField::Value,
        Some("start_time_of_day") => chartpds_core::queries::StatsField::StartTimeOfDay,
        Some("end_time_of_day") => chartpds_core::queries::StatsField::EndTimeOfDay,
        Some("interval_minutes") => chartpds_core::queries::StatsField::IntervalMinutes,
        Some(other) => {
            return Err(McpError::invalid_params(
                format!(
                    "invalid field {other:?}; expected \"value\", \"start_time_of_day\", \"end_time_of_day\", or \"interval_minutes\""
                ),
                None,
            ))
        }
    };
    Ok(chartpds_core::queries::ColumnSpec {
        coding_system: &args.coding.system,
        coding_code: &args.coding.code,
        aggregate,
        field,
    })
}

/// The sentinel used for an omitted `start`: the earliest representable
/// window bound, making an open lower bound behave like "all time".
const OPEN_START: time::OffsetDateTime = time::macros::datetime!(0001-01-01 0:00 UTC);

/// Parse an optional half-open `[start, end)` window, RFC 3339 strings.
/// `start` defaults to [`OPEN_START`]; `end` defaults to `now`.
fn parse_window(
    start: Option<&str>,
    end: Option<&str>,
    now: time::OffsetDateTime,
) -> Result<(time::OffsetDateTime, time::OffsetDateTime), McpError> {
    let start = match start {
        Some(s) => time::OffsetDateTime::parse(s, &Rfc3339)
            .map_err(|err| McpError::invalid_params(format!("invalid start: {err}"), None))?,
        None => OPEN_START,
    };
    let end = match end {
        Some(s) => time::OffsetDateTime::parse(s, &Rfc3339)
            .map_err(|err| McpError::invalid_params(format!("invalid end: {err}"), None))?,
        None => now,
    };
    Ok((start, end))
}

/// Parse a bucket string against a tool's allowed subset, applying
/// `default` when `s` is omitted. The error lists `allowed` verbatim.
fn parse_bucket(s: Option<&str>, default: &str, allowed: &[&str]) -> Result<String, McpError> {
    let value = s.unwrap_or(default);
    if !allowed.contains(&value) {
        let choices = allowed
            .iter()
            .map(|a| format!("{a:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(McpError::invalid_params(
            format!("invalid bucket {value:?}; expected {choices}"),
            None,
        ));
    }
    Ok(value.to_string())
}

/// Validate and convert an optional [`EpisodeArgs`] into a core
/// [`chartpds_core::queries::EpisodeSpec`], enforcing `gap_seconds >= 0`
/// (default 0) and that an episode object is only present when the bucket
/// is actually `"episode"`.
fn parse_episode(
    args: Option<&EpisodeArgs>,
    bucket_is_episode: bool,
) -> Result<Option<chartpds_core::queries::EpisodeSpec<'_>>, McpError> {
    let Some(spec) = args else {
        return Ok(None);
    };
    if !bucket_is_episode {
        return Err(McpError::invalid_params(
            "episode is only valid with bucket \"episode\"".to_string(),
            None,
        ));
    }
    let gap_seconds = spec.gap_seconds.unwrap_or(0);
    if gap_seconds < 0 {
        return Err(McpError::invalid_params(
            "episode.gap_seconds must be >= 0".to_string(),
            None,
        ));
    }
    Ok(Some(chartpds_core::queries::EpisodeSpec {
        coding_system: &spec.coding.system,
        coding_code: &spec.coding.code,
        gap_seconds,
    }))
}

/// Canonical string for a parsed aggregate, echoed back in responses.
fn aggregate_name(aggregate: chartpds_core::queries::ColumnAggregate) -> &'static str {
    match aggregate {
        chartpds_core::queries::ColumnAggregate::Mean => "mean",
        chartpds_core::queries::ColumnAggregate::Sum => "sum",
        chartpds_core::queries::ColumnAggregate::Min => "min",
        chartpds_core::queries::ColumnAggregate::Max => "max",
        chartpds_core::queries::ColumnAggregate::Count => "count",
        chartpds_core::queries::ColumnAggregate::Median => "median",
        chartpds_core::queries::ColumnAggregate::DurationInRange { .. } => "duration_in_range",
        chartpds_core::queries::ColumnAggregate::LongestRunInRange { .. } => "longest_run_in_range",
    }
}

/// Canonical string for a parsed field, echoed back in responses.
fn field_name(field: chartpds_core::queries::StatsField) -> &'static str {
    match field {
        chartpds_core::queries::StatsField::Value => "value",
        chartpds_core::queries::StatsField::StartTimeOfDay => "start_time_of_day",
        chartpds_core::queries::StatsField::EndTimeOfDay => "end_time_of_day",
        chartpds_core::queries::StatsField::IntervalMinutes => "interval_minutes",
    }
}

/// Map an [`AlignedTableError`] to the MCP error space.
///
/// [`AlignedTableError`]: chartpds_core::queries::AlignedTableError
fn map_table_err(err: &chartpds_core::queries::AlignedTableError) -> McpError {
    match err {
        chartpds_core::queries::AlignedTableError::InvalidTimezone(_)
        | chartpds_core::queries::AlignedTableError::MissingEpisodeSpec => {
            McpError::invalid_params(err.to_string(), None)
        }
        chartpds_core::queries::AlignedTableError::Db(_)
        | chartpds_core::queries::AlignedTableError::Internal(_) => {
            McpError::internal_error(format!("query failed: {err}"), None)
        }
    }
}

/// `ChartPDS` MCP server.
///
/// Holds the shared `SqlitePool`, the source-bytes [`Archive`], the derived
/// store (same content-addressed shape, holds machine-generated derivations
/// such as extraction artifacts), and an `rmcp` `ToolRouter`. Each tool is an
/// `async fn` on this impl annotated with `#[tool(description = "...")]`.
#[derive(Clone)]
pub(crate) struct ChartPdsServer {
    pool: SqlitePool,
    archive: Archive,
    derived: Archive,
    oauth_config: Option<OAuthConfig>,
    http_client: reqwest::Client,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl ChartPdsServer {
    pub(crate) fn new(
        pool: SqlitePool,
        archive: Archive,
        derived: Archive,
        oauth_config: Option<OAuthConfig>,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            pool,
            archive,
            derived,
            oauth_config,
            http_client,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        description = "Get the most-recent observation for a given coding. Works for any coding present in the store — LOINC or a minted coding (e.g. AASM sleep stages). Returns null if no observation matches."
    )]
    async fn observation_latest(
        &self,
        Parameters(args): Parameters<ObservationLatestArgs>,
    ) -> Result<CallToolResult, McpError> {
        let observation = chartpds_core::queries::latest_by_coding(
            &self.pool,
            time::OffsetDateTime::now_utc(),
            &args.coding.system,
            &args.coding.code,
        )
        .await
        .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;

        let json = serde_json::to_string(&observation)
            .map_err(|err| McpError::internal_error(format!("serializing result: {err}"), None))?;

        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Read observation history across one or more codings, with optional open-ended bounds. Args: codings [{system, code}], start? (RFC 3339, inclusive), end? (RFC 3339, exclusive); omit either bound for open-ended, omit both for full history. Returns {items: [...]} ordered by (coding_system, coding_code, effective_start)."
    )]
    async fn observation_history(
        &self,
        Parameters(args): Parameters<ObservationHistoryArgs>,
    ) -> Result<CallToolResult, McpError> {
        let start =
            match args.start.as_deref() {
                Some(s) => Some(time::OffsetDateTime::parse(s, &Rfc3339).map_err(|err| {
                    McpError::invalid_params(format!("invalid start: {err}"), None)
                })?),
                None => None,
            };
        let end = match args.end.as_deref() {
            Some(s) => Some(
                time::OffsetDateTime::parse(s, &Rfc3339)
                    .map_err(|err| McpError::invalid_params(format!("invalid end: {err}"), None))?,
            ),
            None => None,
        };

        let codings: Vec<chartpds_core::queries::CodingKey<'_>> = args
            .codings
            .iter()
            .map(|c| chartpds_core::queries::CodingKey {
                coding_system: &c.system,
                coding_code: &c.code,
            })
            .collect();

        let rows = chartpds_core::queries::observation_history(
            &self.pool,
            time::OffsetDateTime::now_utc(),
            &codings,
            start,
            end,
        )
        .await
        .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&serde_json::json!({ "items": rows }))
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Discover which codings are present in the store. Returns {items: [{coding_system, coding_code, count, first_effective_start, last_effective_start}]} grouped by (system, code), ordered by system then code. Feed {coding_system, coding_code} into the history/aggregator tools. Empty items means an empty store; last_effective_start is the per-coding freshness signal."
    )]
    async fn observation_codings(
        &self,
        Parameters(_args): Parameters<EmptyArgs>,
    ) -> Result<CallToolResult, McpError> {
        let counts = chartpds_core::queries::counts_per_code(&self.pool)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&serde_json::json!({ "items": counts }))
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Describe the non-standard codings ChartPDS mints, including value-encoding semantics a client cannot infer. Standard codings (LOINC) are omitted as self-describing. Returns {items: [{coding_system, coding_code, description, value_quantity_meaning, value_string_meaning, values:[{value_quantity, value_string, label}], hints}]}."
    )]
    async fn coding_definitions(
        &self,
        Parameters(_args): Parameters<EmptyArgs>,
    ) -> Result<CallToolResult, McpError> {
        let defs = chartpds_core::clinical::minted_coding_definitions();
        let json = serde_json::to_string(&serde_json::json!({ "items": defs }))
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Current problems (diagnoses), deduped to one entry per code. Returns {latest_document_date, items:[{coding_system, coding_code, coding_display, status, onset_date, document_count, first_seen, last_seen}]}. NOTE: `status` is the raw source-asserted value and is UNRELIABLE. To judge whether a problem is current, compare its `last_seen` against `latest_document_date` (a code absent from the newest document is likely resolved)."
    )]
    async fn problem_list(
        &self,
        Parameters(_args): Parameters<EmptyArgs>,
    ) -> Result<CallToolResult, McpError> {
        let result = chartpds_core::queries::current_problems(&self.pool)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&result)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Current medications, deduped to one entry per code. Returns {latest_document_date, items:[{coding_system, coding_code, coding_display, status, dose, route, start_date, end_date, document_count, first_seen, last_seen}]}. NOTE: `status` is the raw source-asserted value and is UNRELIABLE. To judge whether a medication is current, compare its `last_seen` against `latest_document_date` and treat a past `end_date` as discontinued."
    )]
    async fn medication_list(
        &self,
        Parameters(_args): Parameters<EmptyArgs>,
    ) -> Result<CallToolResult, McpError> {
        let result = chartpds_core::queries::current_medications(&self.pool)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&result)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Ingest a medical record document. kind=\"ccda\": CCDA XML (observations, problems, medications). kind=\"clinical-pdf\": a narrative clinical PDF (pathology/imaging report, visit note) — archives the PDF, indexes its text for narrative_search, and extracts explicitly-quoted ICD-10 codes into problems via a one-time verified LLM pass. LLM extraction is required: a missing ANTHROPIC_API_KEY or an LLM outage (after brief in-band retries) fails the ingest without persisting anything — fix the configuration or wait out the outage, then re-run. kind=\"clinical-pdf\" requires file_path (binary PDF bytes cannot be passed via the content string parameter). Returns what was extracted, verified, and dropped."
    )]
    async fn record_ingest(
        &self,
        Parameters(args): Parameters<RecordIngestArgs>,
    ) -> Result<CallToolResult, McpError> {
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

        match args.kind.as_str() {
            "ccda" => {
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
                .map_err(|err| {
                    McpError::internal_error(format!("ingestion failed: {err}"), None)
                })?;
                let result = serde_json::json!({ "source_document_id": source_document_id });
                let json = serde_json::to_string(&result)
                    .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
                Ok(CallToolResult::success(vec![Content::text(json)]))
            }
            "clinical-pdf" => {
                let extractor =
                    chartpds_core::extraction::ClaudeExtractor::from_env(self.http_client.clone());
                let outcome = chartpds_core::ingestion::ingest_narrative_pdf(
                    &self.archive,
                    &self.derived,
                    &self.pool,
                    content,
                    NarrativeIngestParams {
                        source: &args.source,
                        original_filename: original_filename.as_deref(),
                        archived_at: time::OffsetDateTime::now_utc(),
                    },
                    extractor.as_ref(),
                )
                .await
                .map_err(|err| {
                    McpError::internal_error(format!("ingestion failed: {err}"), None)
                })?;
                let json = serde_json::to_string(&outcome)
                    .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
                Ok(CallToolResult::success(vec![Content::text(json)]))
            }
            other => Err(McpError::invalid_params(
                format!("unsupported kind {other:?}; supported: \"ccda\", \"clinical-pdf\""),
                None,
            )),
        }
    }

    #[tool(
        description = "Connect a data source. Returns JSON. For fitbit: starts OAuth flow and returns {source: \"fitbit\", status: \"authorization_pending\", authorization_url, message} — open authorization_url in a browser; the server catches the callback and stores credentials automatically. For oura: stores the personal access token (pass it in the 'token' parameter) and returns {source: \"oura\", status: \"connected\", message}."
    )]
    async fn source_connect(
        &self,
        Parameters(args): Parameters<SourceConnectArgs>,
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
                let payload = serde_json::json!({
                    "source": "fitbit",
                    "status": "authorization_pending",
                    "authorization_url": url,
                    "message": "Open the URL in a browser to authorize; the server catches the callback and stores credentials automatically."
                });
                let json = serde_json::to_string(&payload)
                    .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
                Ok(CallToolResult::success(vec![Content::text(json)]))
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
                let payload = serde_json::json!({
                    "source": "oura",
                    "status": "connected",
                    "message": "Oura credentials stored. Call source_sync with source=\"oura\"."
                });
                let json = serde_json::to_string(&payload)
                    .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
                Ok(CallToolResult::success(vec![Content::text(json)]))
            }
            other => Err(McpError::invalid_params(
                format!("unknown source {other:?}; known sources: fitbit, oura"),
                None,
            )),
        }
    }

    #[tool(
        description = "Drop and rebuild the entire index from the archive and the derived store, replaying every source (CCDA, Fitbit, Oura, narrative PDFs + frozen extraction artifacts) via each blob's sidecar manifest. No re-sync needed. Unknown or malformed blobs are skipped. Returns {blobs_found, ccda_ingested, fitbit_ingested, oura_ingested, narratives_ingested, extractions_applied, blobs_skipped}."
    )]
    async fn index_rebuild(
        &self,
        Parameters(_args): Parameters<EmptyArgs>,
    ) -> Result<CallToolResult, McpError> {
        let result =
            chartpds_core::ingestion::rebuild_index(&self.archive, &self.derived, &self.pool)
                .await
                .map_err(|err| McpError::internal_error(format!("rebuild failed: {err}"), None))?;
        let json = serde_json::to_string(&result)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Sync a data source (or all configured sources). Returns {items:[{source, ok, days_synced?, total_samples?, reason?, message?}]}. A sync failure is reported in-band as ok:false with a reason in {reauth_required, no_credentials, transient, parse_error, archive_error, database_error}; the tool call itself still succeeds so the caller can render against stale data. Syncing all sources skips unconfigured ones."
    )]
    async fn source_sync(
        &self,
        Parameters(args): Parameters<SourceSyncArgs>,
    ) -> Result<CallToolResult, McpError> {
        let window_days = args.window_days.unwrap_or(8);

        let results: Vec<serde_json::Value> = match args.source.as_deref() {
            Some("fitbit") => vec![self.sync_fitbit_structured(window_days).await],
            Some("oura") => vec![self.sync_oura_structured(window_days).await],
            Some(other) => {
                return Err(McpError::invalid_params(
                    format!("unknown source {other:?}; known sources: fitbit, oura"),
                    None,
                ))
            }
            None => {
                let mut out = Vec::new();
                if self.oauth_config.is_some() {
                    out.push(self.sync_fitbit_structured(window_days).await);
                }
                if self.resolve_oura_token().await.is_ok() {
                    out.push(self.sync_oura_structured(window_days).await);
                }
                out
            }
        };

        let payload = serde_json::json!({ "items": results });
        let text = serde_json::to_string(&payload)
            .map_err(|err| McpError::internal_error(format!("serializing result: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    #[tool(
        description = "List recent notification log entries (auth failures, sync problems). Returns {items: [...]}, newest first."
    )]
    async fn notification_list(
        &self,
        Parameters(args): Parameters<NotificationListArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = args.limit.unwrap_or(20);
        let entries = chartpds_core::queries::list_recent_notifications(&self.pool, limit)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&serde_json::json!({ "items": entries }))
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Descriptive statistics (count, mean, sample sd, min/max, p25/p50/p75, optional threshold counts) for one coding's observations over a window. Args: coding {system, code}, start/end (RFC 3339, half-open; both optional — start defaults to an open-ended lower bound (all time), end defaults to now), field (\"value\" default | \"start_time_of_day\" | \"end_time_of_day\" | \"interval_minutes\"; time-of-day fields are minutes since local noon in [0,1440) so overnight timings stay linear, e.g. 22:16 -> 616), bucket (\"none\" default | \"hour\" | \"day\" | \"week\" ISO keyed by Monday | \"month\" | \"day_of_week\" mon..sun | \"episode\"), timezone (IANA name, default UTC; governs bucket boundaries and time-of-day), thresholds (numbers; each reports n_below / n_at_or_above, n_below is strictly-less), episode {coding, gap_seconds?} (required for bucket \"episode\", invalid otherwise; episodes are gap-tolerant chains of the episode-spec coding's interval observations, e.g. sleep periods, keyed by their RFC 3339 UTC start instant — a period crossing midnight stays in one bucket). Observations lacking the field are excluded and count reflects rows aggregated. bucket \"none\" returns one flat stats object (all stats null when count 0); otherwise {items:[{bucket_key, ...}]} with empty buckets omitted. sd is the sample sd (null when count < 2). confidence is \"provisional\" if any aggregated observation is provisional, else \"confirmed\"."
    )]
    async fn observation_stats(
        &self,
        Parameters(args): Parameters<ObservationStatsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let (start, end) = parse_window(
            args.start.as_deref(),
            args.end.as_deref(),
            time::OffsetDateTime::now_utc(),
        )?;
        let field = match args.field.as_deref() {
            None | Some("value") => chartpds_core::queries::StatsField::Value,
            Some("start_time_of_day") => chartpds_core::queries::StatsField::StartTimeOfDay,
            Some("end_time_of_day") => chartpds_core::queries::StatsField::EndTimeOfDay,
            Some("interval_minutes") => chartpds_core::queries::StatsField::IntervalMinutes,
            Some(other) => {
                return Err(McpError::invalid_params(
                    format!(
                        "invalid field {other:?}; expected \"value\", \"start_time_of_day\", \"end_time_of_day\", or \"interval_minutes\""
                    ),
                    None,
                ))
            }
        };
        let bucket_str = parse_bucket(
            args.bucket.as_deref(),
            "none",
            &[
                "none",
                "hour",
                "day",
                "week",
                "month",
                "day_of_week",
                "episode",
            ],
        )?;
        let bucket = match bucket_str.as_str() {
            "none" => chartpds_core::queries::StatsBucket::None,
            "hour" => chartpds_core::queries::StatsBucket::Hour,
            "day" => chartpds_core::queries::StatsBucket::Day,
            "week" => chartpds_core::queries::StatsBucket::Week,
            "month" => chartpds_core::queries::StatsBucket::Month,
            "day_of_week" => chartpds_core::queries::StatsBucket::DayOfWeek,
            "episode" => chartpds_core::queries::StatsBucket::Episode,
            _ => unreachable!("parse_bucket validated against the allowed list"),
        };
        let episode = parse_episode(
            args.episode.as_ref(),
            bucket == chartpds_core::queries::StatsBucket::Episode,
        )?;

        let result = chartpds_core::queries::observation_stats(
            &self.pool,
            time::OffsetDateTime::now_utc(),
            chartpds_core::queries::ObservationStatsParams {
                coding_system: &args.coding.system,
                coding_code: &args.coding.code,
                start,
                end,
                field,
                bucket,
                timezone: args.timezone.as_deref(),
                thresholds: args.thresholds.as_deref(),
                episode,
            },
        )
        .await
        .map_err(|err| match err {
            chartpds_core::queries::ObservationStatsError::InvalidTimezone(_)
            | chartpds_core::queries::ObservationStatsError::MissingEpisodeSpec => {
                McpError::invalid_params(err.to_string(), None)
            }
            chartpds_core::queries::ObservationStatsError::Db(_)
            | chartpds_core::queries::ObservationStatsError::Internal(_) => {
                McpError::internal_error(format!("query failed: {err}"), None)
            }
        })?;

        let json = serde_json::to_string(&result)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Aligned multi-coding table: one row per bucket with one value per requested coding — the store does the joining, no client-side re-keying. Args: columns [{coding {system, code}, aggregate (\"mean\" default | \"sum\" | \"min\" | \"max\" | \"count\" | \"median\" | \"duration_in_range\" | \"longest_run_in_range\"), field (\"value\" default | \"start_time_of_day\" | \"end_time_of_day\" | \"interval_minutes\"), value_min/value_max (inclusive; required for duration_in_range and longest_run_in_range), gap_seconds (only valid with longest_run_in_range; allowed gap in seconds before a run breaks, default 0)}], start/end (RFC 3339, half-open; both optional — start defaults to an open-ended lower bound (all time), end defaults to now), bucket (\"none\" | \"hour\" | \"day\" default | \"week\" ISO keyed by Monday | \"month\" | \"day_of_week\" mon..sun | \"episode\"), timezone (IANA, default UTC), episode {coding, gap_seconds?} (required for bucket \"episode\", invalid otherwise; episodes are gap-tolerant chains of that coding's intervals, e.g. sleep periods, keyed by their RFC 3339 UTC start instant — a period crossing midnight stays in one row). duration_in_range reports minutes the coding's intervals spent in range, each interval credited whole to the bucket containing its start; longest_run_in_range reports the longest unbroken in-range run's minutes, the whole run credited to the bucket containing the run's start (a run crossing a bucket boundary, e.g. midnight, stays in one row). Returns {columns:[{system, code, aggregate, field}], rows:[{bucket_key, values:[number|null per column, request order], confidence}]}; bucket_key is null only for bucket \"none\", which yields a single whole-window row ONLY when at least one column has qualifying data in the window (rows: [] if none do); null values mean the coding has no qualifying data in that bucket. Example: one row per day with total sleep (93832-4, mean), WASO (103215-0, mean), and minutes of elevated heart rate (8867-4, duration_in_range value_min 100)."
    )]
    async fn observation_table(
        &self,
        Parameters(args): Parameters<ObservationTableArgs>,
    ) -> Result<CallToolResult, McpError> {
        if args.columns.is_empty() {
            return Err(McpError::invalid_params(
                "columns must not be empty".to_string(),
                None,
            ));
        }
        let (start, end) = parse_window(
            args.start.as_deref(),
            args.end.as_deref(),
            time::OffsetDateTime::now_utc(),
        )?;
        let bucket_str = parse_bucket(
            args.bucket.as_deref(),
            "day",
            &[
                "none",
                "hour",
                "day",
                "week",
                "month",
                "day_of_week",
                "episode",
            ],
        )?;
        let bucket = match bucket_str.as_str() {
            "none" => chartpds_core::queries::TableBucket::None,
            "hour" => chartpds_core::queries::TableBucket::Hour,
            "day" => chartpds_core::queries::TableBucket::Day,
            "week" => chartpds_core::queries::TableBucket::Week,
            "month" => chartpds_core::queries::TableBucket::Month,
            "day_of_week" => chartpds_core::queries::TableBucket::DayOfWeek,
            "episode" => chartpds_core::queries::TableBucket::Episode,
            _ => unreachable!("parse_bucket validated against the allowed list"),
        };
        let episode = parse_episode(
            args.episode.as_ref(),
            bucket == chartpds_core::queries::TableBucket::Episode,
        )?;
        let columns = args
            .columns
            .iter()
            .map(parse_column)
            .collect::<Result<Vec<_>, _>>()?;

        let table = chartpds_core::queries::aligned_table(
            &self.pool,
            time::OffsetDateTime::now_utc(),
            chartpds_core::queries::AlignedTableParams {
                columns: &columns,
                start,
                end,
                bucket,
                episode,
                timezone: args.timezone.as_deref(),
            },
        )
        .await
        .map_err(|err| map_table_err(&err))?;

        let column_echo: Vec<serde_json::Value> = columns
            .iter()
            .map(|c| {
                serde_json::json!({
                    "system": c.coding_system,
                    "code": c.coding_code,
                    "aggregate": aggregate_name(c.aggregate),
                    "field": field_name(c.field),
                })
            })
            .collect();
        let json = serde_json::to_string(&serde_json::json!({
            "columns": column_echo,
            "rows": table.rows,
        }))
        .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "How two coded signals relate over a window, without exporting data. Both signals are reduced to one value per bucket (same column spec as observation_table: coding {system, code}, aggregate \"mean\" default | \"sum\" | \"min\" | \"max\" | \"count\" | \"median\" | \"duration_in_range\" | \"longest_run_in_range\" with value_min/value_max and per-column gap_seconds, field), then paired bucket-by-bucket. Args: x, y (column specs), start/end (RFC 3339, half-open; both optional — start defaults to an open-ended lower bound (all time), end defaults to now), bucket (\"hour\" | \"day\" default | \"week\" | \"month\" | \"episode\"), timezone (IANA), lag (buckets; 1 pairs each x with the FOLLOWING bucket's y — e.g. activity during the day vs. sleep the following night; default 0; may be negative; for bucket \"episode\" this pairs episode i's x with episode i+lag's y by index, not calendar arithmetic), threshold (on x: also returns y statistics for pairs with x strictly below vs. at-or-above), episode {coding, gap_seconds?} (required for bucket \"episode\", invalid otherwise; episodes are gap-tolerant chains of that coding's intervals, e.g. sleep periods, keyed by their RFC 3339 UTC start instant — a period crossing midnight stays in one bucket). Buckets missing either signal are excluded; n_pairs is the kept-pair count. Returns {n_pairs, pearson_r (null under 2 pairs or zero variance), spearman_r (rank-based; robust to outliers and monotonic-but-nonlinear relationships — a gap between it and pearson_r suggests outliers or a curved relationship), x_mean, x_sd, y_mean, y_sd, groups?: {x_below, x_at_or_above: {count, mean, sd, min, max, p50}}}."
    )]
    async fn observation_relationship(
        &self,
        Parameters(args): Parameters<ObservationRelationshipArgs>,
    ) -> Result<CallToolResult, McpError> {
        let (start, end) = parse_window(
            args.start.as_deref(),
            args.end.as_deref(),
            time::OffsetDateTime::now_utc(),
        )?;
        let bucket_str = parse_bucket(
            args.bucket.as_deref(),
            "day",
            &["hour", "day", "week", "month", "episode"],
        )?;
        let bucket = match bucket_str.as_str() {
            "hour" => chartpds_core::queries::RelationshipBucket::Hour,
            "day" => chartpds_core::queries::RelationshipBucket::Day,
            "week" => chartpds_core::queries::RelationshipBucket::Week,
            "month" => chartpds_core::queries::RelationshipBucket::Month,
            "episode" => chartpds_core::queries::RelationshipBucket::Episode,
            _ => unreachable!("parse_bucket validated against the allowed list"),
        };
        let episode = parse_episode(
            args.episode.as_ref(),
            bucket == chartpds_core::queries::RelationshipBucket::Episode,
        )?;
        let x = parse_column(&args.x)?;
        let y = parse_column(&args.y)?;

        let result = chartpds_core::queries::signal_relationship(
            &self.pool,
            time::OffsetDateTime::now_utc(),
            chartpds_core::queries::SignalRelationshipParams {
                x,
                y,
                start,
                end,
                bucket,
                timezone: args.timezone.as_deref(),
                lag_buckets: args.lag.unwrap_or(0),
                x_threshold: args.threshold,
                episode,
            },
        )
        .await
        .map_err(|err| map_table_err(&err))?;

        let json = serde_json::to_string(&result)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Full-text search over narrative clinical documents (FTS5, BM25-ranked). Args: query? (FTS5 syntax; omit to list the whole narrative catalog newest-first), limit? (default 20). Query terms containing punctuation (e.g. the ICD-10 code \"R10.9\") must be double-quoted inside the query string, or FTS5 will fail to parse them. Returns {items: [{source_document_id, title, kind, source, document_date, snippet}]}. Pass source_document_id to narrative_get for the full text."
    )]
    async fn narrative_search(
        &self,
        Parameters(args): Parameters<NarrativeSearchArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = args.limit.unwrap_or(20);
        let hits =
            chartpds_core::queries::search_narratives(&self.pool, args.query.as_deref(), limit)
                .await
                .map_err(|err| match &err {
                    // Caller-caused FTS5 query errors only (bad syntax, or a
                    // column filter like "title:x" against a table whose
                    // only indexed column is "text"); operational errors
                    // (corrupt index, etc.) map to internal_error.
                    sqlx::Error::Database(db)
                        if db.message().contains("fts5: syntax error")
                            || db.message().contains("no such column") =>
                    {
                        McpError::invalid_params(format!("invalid FTS5 query: {err}"), None)
                    }
                    _ => McpError::internal_error(format!("query failed: {err}"), None),
                })?;
        let json = serde_json::to_string(&serde_json::json!({ "items": hits }))
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Fetch one narrative clinical document: metadata, full extracted text, and the verified codings extracted from it (with their section labels). Args: source_document_id (from narrative_search). Returns null if source_document_id doesn't exist or isn't a narrative (clinical-pdf) document."
    )]
    async fn narrative_get(
        &self,
        Parameters(args): Parameters<NarrativeGetArgs>,
    ) -> Result<CallToolResult, McpError> {
        let detail = chartpds_core::queries::get_narrative(&self.pool, args.source_document_id)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&detail)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }
}

// ── Private helpers ──────────────────────────────────────────────────

impl ChartPdsServer {
    /// Look up the Oura PAT from `source_credentials` (set by
    /// `source_connect`). Falls back to checking the environment for
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
                "No Oura PAT found. Call source_connect with source=\"oura\" first or set OURA_PERSONAL_ACCESS_TOKEN.",
                None,
            )
        })
    }

    /// Sync Fitbit and return a per-source structured result object.
    async fn sync_fitbit_structured(&self, window_days: i64) -> serde_json::Value {
        let Some(oauth_config) = self.oauth_config.as_ref() else {
            return serde_json::json!({
                "source": "fitbit",
                "ok": false,
                "reason": "no_credentials",
                "message": "GOOGLE_HEALTH_CLIENT_ID and GOOGLE_HEALTH_CLIENT_SECRET must be set"
            });
        };
        match chartpds_core::sources::fitbit::sync::sync_recent_days(
            &self.archive,
            &self.pool,
            &self.http_client,
            oauth_config,
            window_days,
        )
        .await
        {
            Ok(r) => serde_json::json!({
                "source": "fitbit",
                "ok": true,
                "days_synced": r.days_synced,
                "total_samples": r.total_samples
            }),
            Err(e) => serde_json::json!({
                "source": "fitbit",
                "ok": false,
                "reason": e.reason_code(),
                "message": e.to_string()
            }),
        }
    }

    /// Sync Oura and return a per-source structured result object.
    async fn sync_oura_structured(&self, window_days: i64) -> serde_json::Value {
        let Ok(access_token) = self.resolve_oura_token().await else {
            return serde_json::json!({
                "source": "oura",
                "ok": false,
                "reason": "no_credentials",
                "message": "No Oura PAT found. Call source_connect with source=\"oura\" first or set OURA_PERSONAL_ACCESS_TOKEN."
            });
        };
        match chartpds_core::sources::oura::sync::sync_recent_days(
            &self.archive,
            &self.pool,
            &self.http_client,
            &access_token,
            window_days,
        )
        .await
        {
            Ok(r) => serde_json::json!({
                "source": "oura",
                "ok": true,
                "days_synced": r.days_synced,
                "total_samples": r.total_samples
            }),
            Err(e) => serde_json::json!({
                "source": "oura",
                "ok": false,
                "reason": e.reason_code(),
                "message": e.to_string()
            }),
        }
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for ChartPdsServer {
    fn get_info(&self) -> ServerInfo {
        // `ServerInfo` (aliased to `InitializeResult`) is `#[non_exhaustive]`
        // in rmcp 1.7, so we build it via `new(...).with_instructions(...)`
        // instead of a struct literal.
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "ChartPDS personal data store. Tool families: observation_* (query), \
             coding_definitions, problem_list/medication_list, narrative_* (documents), \
             record_ingest, source_* (adapters), notification_list, index_rebuild.",
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

    const PDF_FIXTURE: &[u8] =
        include_bytes!("../../chartpds-core/src/extraction/fixtures/synthetic_pathology.pdf");

    async fn fresh_server_with_empty_db() -> ChartPdsServer {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        let derived = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        ChartPdsServer::new(pool, archive, derived, None, reqwest::Client::new())
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
                archived_at: OffsetDateTime::now_utc(),
                document_date: None,
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
        let derived = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        ChartPdsServer::new(pool, archive, derived, None, reqwest::Client::new())
    }

    #[tokio::test]
    async fn observation_latest_returns_the_match() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .observation_latest(Parameters(ObservationLatestArgs {
                coding: Coding {
                    system: "http://loinc.org".to_owned(),
                    code: "29463-7".to_owned(),
                },
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
    async fn observation_latest_returns_null_when_no_match() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .observation_latest(Parameters(ObservationLatestArgs {
                coding: Coding {
                    system: "http://loinc.org".to_owned(),
                    code: "no-such-code".to_owned(),
                },
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
    async fn observation_latest_returns_null_when_system_does_not_match() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .observation_latest(Parameters(ObservationLatestArgs {
                coding: Coding {
                    system: "https://example.com/coding/bogus".to_owned(),
                    code: "29463-7".to_owned(),
                },
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
    async fn observation_history_returns_match_for_coding() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .observation_history(Parameters(ObservationHistoryArgs {
                codings: vec![Coding {
                    system: "http://loinc.org".to_owned(),
                    code: "29463-7".to_owned(),
                }],
                start: None,
                end: None,
            }))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let v: serde_json::Value = serde_json::from_str(text).expect("valid json");
        let arr = v["items"].as_array().expect("items array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["coding_code"], "29463-7");
    }

    #[tokio::test]
    async fn observation_history_empty_when_coding_absent() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .observation_history(Parameters(ObservationHistoryArgs {
                codings: vec![Coding {
                    system: "http://loinc.org".to_owned(),
                    code: "no-such-code".to_owned(),
                }],
                start: None,
                end: None,
            }))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let v: serde_json::Value = serde_json::from_str(text).expect("valid json");
        let arr = v["items"].as_array().expect("items array");
        assert!(arr.is_empty());
    }

    #[tokio::test]
    async fn observation_codings_returns_one_entry() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .observation_codings(Parameters(EmptyArgs {}))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let v: serde_json::Value = serde_json::from_str(text).expect("valid json");
        let arr = v["items"].as_array().expect("items array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["coding_system"], "http://loinc.org");
        assert_eq!(arr[0]["coding_code"], "29463-7");
        assert_eq!(arr[0]["count"], 1);
        assert_eq!(arr[0]["first_effective_start"], "2026-01-01T12:00:00Z");
        assert_eq!(arr[0]["last_effective_start"], "2026-01-01T12:00:00Z");
    }

    #[tokio::test]
    async fn coding_definitions_returns_sleep_stage_catalog() {
        let server = fresh_server_with_empty_db().await;
        let result = server
            .coding_definitions(Parameters(EmptyArgs {}))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let v: serde_json::Value = serde_json::from_str(text).expect("valid json");
        let arr = v["items"].as_array().expect("items array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["coding_code"], "aasm-sleep-stage");
        assert_eq!(
            arr[0]["coding_system"],
            "https://chartpds.fhwang.net/coding/aasm/sleep-stage"
        );
        assert_eq!(arr[0]["values"].as_array().expect("values array").len(), 5);
        // Spot-check the encoding a client needs.
        assert_eq!(arr[0]["values"][0]["value_quantity"], 0.0);
        assert_eq!(arr[0]["values"][0]["value_string"], "wake");
        assert_eq!(arr[0]["values"][4]["value_string"], "rem");
    }

    #[tokio::test]
    async fn record_ingest_returns_source_document_id() {
        let server = fresh_server_with_empty_db().await;
        let ccda =
            include_str!("../../chartpds-core/src/ingestion/ccda/fixtures/valid_minimal.xml");

        let result = server
            .record_ingest(Parameters(RecordIngestArgs {
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
            .record_ingest(Parameters(RecordIngestArgs {
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
            .observation_latest(Parameters(ObservationLatestArgs {
                coding: Coding {
                    system: "http://loinc.org".to_owned(),
                    code: "29463-7".to_owned(),
                },
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
    async fn problem_list_returns_ingested_problems() {
        let server = fresh_server_with_empty_db().await;
        let ccda =
            include_str!("../../chartpds-core/src/ingestion/ccda/fixtures/valid_minimal.xml");

        server
            .record_ingest(Parameters(RecordIngestArgs {
                file_path: None,
                content: Some(ccda.to_owned()),
                kind: "ccda".to_owned(),
                source: "test".to_owned(),
                original_filename: None,
            }))
            .await
            .expect("ingest");

        let result = server
            .problem_list(Parameters(EmptyArgs {}))
            .await
            .expect("problem_list tool call");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert!(value["latest_document_date"].is_string());
        let arr = value["items"].as_array().expect("expected items array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["coding_code"], "44054006");
        assert_eq!(arr[0]["document_count"], 1);
    }

    #[tokio::test]
    async fn medication_list_returns_ingested_medications() {
        let server = fresh_server_with_empty_db().await;
        let ccda =
            include_str!("../../chartpds-core/src/ingestion/ccda/fixtures/valid_minimal.xml");

        server
            .record_ingest(Parameters(RecordIngestArgs {
                file_path: None,
                content: Some(ccda.to_owned()),
                kind: "ccda".to_owned(),
                source: "test".to_owned(),
                original_filename: None,
            }))
            .await
            .expect("ingest");

        let result = server
            .medication_list(Parameters(EmptyArgs {}))
            .await
            .expect("medication_list tool call");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert!(value["latest_document_date"].is_string());
        let arr = value["items"].as_array().expect("expected items array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["coding_code"], "860975");
        assert_eq!(arr[0]["document_count"], 1);
    }

    #[tokio::test]
    async fn notification_list_returns_seeded_entry() {
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
            .notification_list(Parameters(NotificationListArgs { limit: Some(10) }))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        let arr = value["items"].as_array().expect("expected items array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["condition_id"], "auth_expired:fitbit");
        assert_eq!(arr[0]["severity"], "critical");
    }

    #[tokio::test]
    async fn index_rebuild_re_ingests_archived_ccda() {
        let server = fresh_server_with_empty_db().await;
        let ccda =
            include_str!("../../chartpds-core/src/ingestion/ccda/fixtures/valid_minimal.xml");

        // Ingest a CCDA first (puts it in the archive).
        server
            .record_ingest(Parameters(RecordIngestArgs {
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
            .index_rebuild(Parameters(EmptyArgs {}))
            .await
            .expect("index_rebuild tool call");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["blobs_found"], 1);
        assert_eq!(value["ccda_ingested"], 1);
        assert_eq!(value["blobs_skipped"], 0);

        // Observations should still be present after the rebuild.
        let obs_result = server
            .observation_codings(Parameters(EmptyArgs {}))
            .await
            .expect("observation_codings after rebuild");

        let obs_text = match &obs_result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let obs_value: serde_json::Value = serde_json::from_str(obs_text).expect("valid JSON");
        let arr = obs_value["items"].as_array().expect("items array");
        assert!(!arr.is_empty(), "observations should survive rebuild");
    }

    async fn fresh_server_with_sleep_epochs() -> ChartPdsServer {
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
                archived_at: OffsetDateTime::now_utc(),
                document_date: None,
            },
        )
        .await
        .expect("doc");

        // Two contiguous 5-min asleep epochs (N2, N3) => a 10-minute run.
        for (start_end, stage) in [
            (
                (
                    datetime!(2026-01-01 22:00:00 UTC),
                    datetime!(2026-01-01 22:05:00 UTC),
                ),
                2.0,
            ),
            (
                (
                    datetime!(2026-01-01 22:05:00 UTC),
                    datetime!(2026-01-01 22:10:00 UTC),
                ),
                3.0,
            ),
        ] {
            insert_observation(
                &pool,
                InsertObservationParams {
                    source_document_id: doc_id,
                    coding_system: "https://chartpds.fhwang.net/coding/aasm/sleep-stage",
                    coding_code: "aasm-sleep-stage",
                    coding_display: Some("Sleep stage"),
                    effective_start: start_end.0,
                    effective_end: Some(start_end.1),
                    value_quantity: Some(stage),
                    value_string: None,
                    value_unit: None,
                },
            )
            .await
            .expect("obs");
        }

        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        let derived = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        ChartPdsServer::new(pool, archive, derived, None, reqwest::Client::new())
    }

    /// Three nightly total-sleep observations: 400, 420, 380 minutes.
    async fn fresh_server_with_nightly_sleep() -> ChartPdsServer {
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
                archived_at: OffsetDateTime::now_utc(),
                document_date: None,
            },
        )
        .await
        .expect("doc");
        for (start, minutes) in [
            (datetime!(2026-01-01 07:00:00 UTC), 400.0),
            (datetime!(2026-01-02 07:00:00 UTC), 420.0),
            (datetime!(2026-01-03 07:00:00 UTC), 380.0),
        ] {
            insert_observation(
                &pool,
                InsertObservationParams {
                    source_document_id: doc_id,
                    coding_system: "http://loinc.org",
                    coding_code: "93832-4",
                    coding_display: Some("Sleep duration"),
                    effective_start: start,
                    effective_end: None,
                    value_quantity: Some(minutes),
                    value_string: None,
                    value_unit: Some("min"),
                },
            )
            .await
            .expect("obs");
        }

        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        let derived = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        ChartPdsServer::new(pool, archive, derived, None, reqwest::Client::new())
    }

    /// Three nights of sleep-stage epoch intervals (22:00–06:00, well
    /// gap-separated) defining three episodes, but only two nightly
    /// total-sleep summaries (93832-4, night 0 and night 1) landing inside
    /// them — night 2 has episode structure but no summary value.
    async fn fresh_server_with_gapped_sleep_episodes() -> ChartPdsServer {
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
                archived_at: OffsetDateTime::now_utc(),
                document_date: None,
            },
        )
        .await
        .expect("doc");

        for d in 0..3 {
            insert_observation(
                &pool,
                InsertObservationParams {
                    source_document_id: doc_id,
                    coding_system: "https://chartpds.fhwang.net/coding/aasm/sleep-stage",
                    coding_code: "aasm-sleep-stage",
                    coding_display: Some("Sleep stage"),
                    effective_start: datetime!(2026-01-01 22:00:00 UTC) + time::Duration::days(d),
                    effective_end: Some(
                        datetime!(2026-01-02 06:00:00 UTC) + time::Duration::days(d),
                    ),
                    value_quantity: Some(2.0),
                    value_string: None,
                    value_unit: None,
                },
            )
            .await
            .expect("obs");
        }
        for (d, minutes) in [(0i64, 400.0), (1i64, 420.0)] {
            insert_observation(
                &pool,
                InsertObservationParams {
                    source_document_id: doc_id,
                    coding_system: "http://loinc.org",
                    coding_code: "93832-4",
                    coding_display: Some("Sleep duration"),
                    effective_start: datetime!(2026-01-01 23:00:00 UTC) + time::Duration::days(d),
                    effective_end: None,
                    value_quantity: Some(minutes),
                    value_string: None,
                    value_unit: Some("min"),
                },
            )
            .await
            .expect("obs");
        }

        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        let derived = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        ChartPdsServer::new(pool, archive, derived, None, reqwest::Client::new())
    }

    fn stats_args() -> ObservationStatsArgs {
        ObservationStatsArgs {
            coding: Coding {
                system: "http://loinc.org".to_string(),
                code: "93832-4".to_string(),
            },
            start: Some("2026-01-01T00:00:00Z".to_string()),
            end: Some("2026-02-01T00:00:00Z".to_string()),
            field: None,
            bucket: None,
            timezone: None,
            thresholds: None,
            episode: None,
        }
    }

    #[tokio::test]
    async fn observation_stats_flat_defaults_to_value_field() {
        let server = fresh_server_with_nightly_sleep().await;
        let result = server
            .observation_stats(Parameters(stats_args()))
            .await
            .expect("tool call succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["count"], 3);
        assert_eq!(value["p50"], 400.0);
        assert_eq!(value["min"], 380.0);
        assert_eq!(value["max"], 420.0);
        assert_eq!(value["confidence"], "confirmed");
        // No thresholds requested → key omitted.
        assert!(value.get("thresholds").is_none());
    }

    #[tokio::test]
    async fn observation_stats_reports_thresholds() {
        let server = fresh_server_with_nightly_sleep().await;
        let mut args = stats_args();
        args.thresholds = Some(vec![400.0]);
        let result = server
            .observation_stats(Parameters(args))
            .await
            .expect("tool call succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["thresholds"][0]["threshold"], 400.0);
        assert_eq!(value["thresholds"][0]["n_below"], 1);
        assert_eq!(value["thresholds"][0]["n_at_or_above"], 2);
    }

    #[tokio::test]
    async fn observation_stats_day_of_week_bucket_shape() {
        let server = fresh_server_with_nightly_sleep().await;
        let mut args = stats_args();
        args.bucket = Some("day_of_week".to_string());
        let result = server
            .observation_stats(Parameters(args))
            .await
            .expect("tool call succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        // Jan 1/2/3 2026 are Thu/Fri/Sat → three buckets, Monday-first order.
        let keys: Vec<&str> = value["items"]
            .as_array()
            .expect("items array")
            .iter()
            .map(|b| b["bucket_key"].as_str().expect("key"))
            .collect();
        assert_eq!(keys, vec!["thu", "fri", "sat"]);
    }

    #[tokio::test]
    async fn observation_stats_defaults_open_window() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .observation_stats(Parameters(ObservationStatsArgs {
                coding: Coding {
                    system: "http://loinc.org".to_owned(),
                    code: "29463-7".to_owned(),
                },
                start: None,
                end: None,
                field: None,
                bucket: None,
                timezone: None,
                thresholds: None,
                episode: None,
            }))
            .await
            .expect("tool call");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let v: serde_json::Value = serde_json::from_str(text).expect("json");
        assert_eq!(v["count"], 1);
    }

    #[tokio::test]
    async fn observation_stats_bucketed_result_uses_items() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .observation_stats(Parameters(ObservationStatsArgs {
                coding: Coding {
                    system: "http://loinc.org".to_owned(),
                    code: "29463-7".to_owned(),
                },
                start: None,
                end: None,
                field: None,
                bucket: Some("day".to_owned()),
                timezone: None,
                thresholds: None,
                episode: None,
            }))
            .await
            .expect("tool call");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let v: serde_json::Value = serde_json::from_str(text).expect("json");
        assert_eq!(v["items"][0]["bucket_key"], "2026-01-01");
    }

    #[tokio::test]
    async fn observation_stats_episode_bucket_requires_episode_object() {
        let server = fresh_server_with_one_weight().await;
        let err = server
            .observation_stats(Parameters(ObservationStatsArgs {
                coding: Coding {
                    system: "http://loinc.org".to_owned(),
                    code: "29463-7".to_owned(),
                },
                start: None,
                end: None,
                field: None,
                bucket: Some("episode".to_owned()),
                timezone: None,
                thresholds: None,
                episode: None,
            }))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("episode"));
    }

    /// Body weight on Jan 1/2/3 and heart rate on Jan 1/2 only.
    async fn fresh_server_with_weight_and_hr() -> ChartPdsServer {
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
                archived_at: OffsetDateTime::now_utc(),
                document_date: None,
            },
        )
        .await
        .expect("doc");
        for (code, start, value) in [
            ("29463-7", datetime!(2026-01-01 08:00:00 UTC), 380.0),
            ("29463-7", datetime!(2026-01-02 08:00:00 UTC), 400.0),
            ("29463-7", datetime!(2026-01-03 08:00:00 UTC), 420.0),
            ("8867-4", datetime!(2026-01-01 08:00:00 UTC), 60.0),
            ("8867-4", datetime!(2026-01-02 08:00:00 UTC), 80.0),
        ] {
            insert_observation(
                &pool,
                InsertObservationParams {
                    source_document_id: doc_id,
                    coding_system: "http://loinc.org",
                    coding_code: code,
                    coding_display: None,
                    effective_start: start,
                    effective_end: None,
                    value_quantity: Some(value),
                    value_string: None,
                    value_unit: None,
                },
            )
            .await
            .expect("obs");
        }

        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        let derived = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        ChartPdsServer::new(pool, archive, derived, None, reqwest::Client::new())
    }

    fn table_column(code: &str) -> TableColumnArgs {
        TableColumnArgs {
            coding: Coding {
                system: "http://loinc.org".to_string(),
                code: code.to_string(),
            },
            aggregate: None,
            field: None,
            value_min: None,
            value_max: None,
            gap_seconds: None,
        }
    }

    fn table_args(columns: Vec<TableColumnArgs>) -> ObservationTableArgs {
        ObservationTableArgs {
            columns,
            start: Some("2026-01-01T00:00:00Z".to_string()),
            end: Some("2026-02-01T00:00:00Z".to_string()),
            bucket: None,
            timezone: None,
            episode: None,
        }
    }

    #[tokio::test]
    async fn observation_table_aligns_day_rows_with_explicit_null() {
        let server = fresh_server_with_weight_and_hr().await;
        let result = server
            .observation_table(Parameters(table_args(vec![
                table_column("29463-7"),
                table_column("8867-4"),
            ])))
            .await
            .expect("tool call succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["columns"][0]["code"], "29463-7");
        assert_eq!(value["columns"][0]["aggregate"], "mean");
        assert_eq!(value["columns"][0]["field"], "value");
        let rows = value["rows"].as_array().expect("rows array");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["bucket_key"], "2026-01-01");
        assert_eq!(rows[0]["values"], serde_json::json!([380.0, 60.0]));
        // Jan 3 has weight but no heart rate: explicit null in position 2.
        assert_eq!(rows[2]["bucket_key"], "2026-01-03");
        assert_eq!(rows[2]["values"], serde_json::json!([420.0, null]));
        assert_eq!(rows[2]["confidence"], "confirmed");
    }

    #[tokio::test]
    async fn observation_table_episode_bucket_end_to_end() {
        let server = fresh_server_with_sleep_epochs().await;
        let mut deep = table_column("aasm-sleep-stage");
        deep.coding.system = "https://chartpds.fhwang.net/coding/aasm/sleep-stage".to_string();
        deep.aggregate = Some("duration_in_range".to_string());
        deep.value_min = Some(3.0);
        deep.value_max = Some(3.0);
        let mut args = table_args(vec![deep]);
        args.bucket = Some("episode".to_string());
        args.episode = Some(EpisodeArgs {
            coding: Coding {
                system: "https://chartpds.fhwang.net/coding/aasm/sleep-stage".to_string(),
                code: "aasm-sleep-stage".to_string(),
            },
            gap_seconds: None,
        });
        let result = server
            .observation_table(Parameters(args))
            .await
            .expect("tool call succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        let rows = value["rows"].as_array().expect("rows array");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["bucket_key"], "2026-01-01T22:00:00Z");
        assert_eq!(rows[0]["values"], serde_json::json!([5.0]));
    }

    #[tokio::test]
    async fn observation_table_rejects_invalid_args() {
        let server = fresh_server_with_weight_and_hr().await;

        // duration_in_range without value bounds.
        let mut bad = table_column("8867-4");
        bad.aggregate = Some("duration_in_range".to_string());
        let err = server
            .observation_table(Parameters(table_args(vec![bad])))
            .await
            .expect_err("missing bounds");
        assert!(err.to_string().contains("value_min"));

        // Value bounds with a non-duration aggregate.
        let mut bad = table_column("8867-4");
        bad.value_min = Some(1.0);
        let err = server
            .observation_table(Parameters(table_args(vec![bad])))
            .await
            .expect_err("bounds with mean");
        assert!(err.to_string().contains("duration_in_range"));

        // Episode bucket without an episode spec.
        let mut args = table_args(vec![table_column("8867-4")]);
        args.bucket = Some("episode".to_string());
        let err = server
            .observation_table(Parameters(args))
            .await
            .expect_err("missing episode spec");
        assert!(err.to_string().contains("episode"));

        // Empty columns.
        let err = server
            .observation_table(Parameters(table_args(vec![])))
            .await
            .expect_err("empty columns");
        assert!(err.to_string().contains("columns"));

        // Unknown aggregate.
        let mut bad = table_column("8867-4");
        bad.aggregate = Some("mode".to_string());
        let err = server
            .observation_table(Parameters(table_args(vec![bad])))
            .await
            .expect_err("unknown aggregate");
        assert!(err.to_string().contains("invalid aggregate"));
    }

    #[tokio::test]
    async fn observation_table_longest_run_in_range_accepted_with_gap_seconds() {
        let server = fresh_server_with_one_weight().await;
        let mut col = table_column("29463-7");
        col.aggregate = Some("longest_run_in_range".to_string());
        col.value_min = Some(0.0);
        col.value_max = Some(1000.0);
        col.gap_seconds = Some(0);
        let result = server
            .observation_table(Parameters(table_args(vec![col])))
            .await
            .expect("longest_run_in_range with valid bounds and gap_seconds succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["columns"][0]["aggregate"], "longest_run_in_range");
        let rows = value["rows"].as_array().expect("rows array");
        assert_eq!(rows.len(), 1);
        // The seeded weight observation has no effective_end, so it never
        // becomes an interval: the bucket exists but the value is null.
        assert_eq!(rows[0]["values"], serde_json::json!([null]));
    }

    #[tokio::test]
    async fn observation_table_longest_run_in_range_requires_value_bounds() {
        let server = fresh_server_with_weight_and_hr().await;
        let mut bad = table_column("8867-4");
        bad.aggregate = Some("longest_run_in_range".to_string());
        let err = server
            .observation_table(Parameters(table_args(vec![bad])))
            .await
            .expect_err("missing bounds");
        assert!(err.to_string().contains("value_min"));
        assert!(err.to_string().contains("longest_run_in_range"));
    }

    #[tokio::test]
    async fn observation_table_gap_seconds_rejected_on_non_longest_run_aggregate() {
        let server = fresh_server_with_weight_and_hr().await;
        let mut bad = table_column("8867-4");
        bad.aggregate = Some("mean".to_string());
        bad.gap_seconds = Some(0);
        let err = server
            .observation_table(Parameters(table_args(vec![bad])))
            .await
            .expect_err("gap_seconds with mean");
        assert!(err.to_string().contains("gap_seconds"));
    }

    #[tokio::test]
    async fn observation_table_negative_gap_seconds_rejected() {
        let server = fresh_server_with_weight_and_hr().await;
        let mut bad = table_column("8867-4");
        bad.aggregate = Some("longest_run_in_range".to_string());
        bad.value_min = Some(0.0);
        bad.value_max = Some(1000.0);
        bad.gap_seconds = Some(-1);
        let err = server
            .observation_table(Parameters(table_args(vec![bad])))
            .await
            .expect_err("negative gap_seconds");
        assert!(err.to_string().contains("gap_seconds"));
    }

    #[tokio::test]
    async fn observation_table_duration_in_range_rejects_inverted_bounds() {
        let server = fresh_server_with_weight_and_hr().await;
        let mut bad = table_column("8867-4");
        bad.aggregate = Some("duration_in_range".to_string());
        bad.value_min = Some(10.0);
        bad.value_max = Some(5.0);
        let err = server
            .observation_table(Parameters(table_args(vec![bad])))
            .await
            .expect_err("value_min > value_max");
        assert!(err.to_string().contains("value_min"));
        assert!(err.to_string().contains("value_max"));
    }

    #[tokio::test]
    async fn observation_table_none_bucket_yields_null_key_row() {
        let server = fresh_server_with_one_weight().await;
        let result = server
            .observation_table(Parameters(ObservationTableArgs {
                columns: vec![TableColumnArgs {
                    coding: Coding {
                        system: "http://loinc.org".to_owned(),
                        code: "29463-7".to_owned(),
                    },
                    aggregate: None,
                    field: None,
                    value_min: None,
                    value_max: None,
                    gap_seconds: None,
                }],
                start: None,
                end: None,
                bucket: Some("none".to_owned()),
                timezone: None,
                episode: None,
            }))
            .await
            .expect("tool call");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let v: serde_json::Value = serde_json::from_str(text).expect("json");
        assert!(v["rows"][0]["bucket_key"].is_null());
        assert_eq!(v["rows"][0]["values"][0], 72.5);
    }

    #[tokio::test]
    async fn observation_table_rejects_longest_run_without_bounds() {
        let server = fresh_server_with_one_weight().await;
        let err = server
            .observation_table(Parameters(ObservationTableArgs {
                columns: vec![TableColumnArgs {
                    coding: Coding {
                        system: "http://loinc.org".to_owned(),
                        code: "29463-7".to_owned(),
                    },
                    aggregate: Some("longest_run_in_range".to_owned()),
                    field: None,
                    value_min: None,
                    value_max: None,
                    gap_seconds: None,
                }],
                start: None,
                end: None,
                bucket: None,
                timezone: None,
                episode: None,
            }))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("value_min"));
    }

    fn relationship_args() -> ObservationRelationshipArgs {
        ObservationRelationshipArgs {
            x: table_column("29463-7"),
            y: table_column("8867-4"),
            start: Some("2026-01-01T00:00:00Z".to_string()),
            end: Some("2026-02-01T00:00:00Z".to_string()),
            bucket: None,
            timezone: None,
            lag: None,
            threshold: None,
            episode: None,
        }
    }

    #[tokio::test]
    async fn observation_relationship_reports_exact_pearson_r() {
        let server = fresh_server_with_weight_and_hr().await;
        let result = server
            .observation_relationship(Parameters(relationship_args()))
            .await
            .expect("tool call succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        // Jan 3 has weight but no HR → excluded. Remaining pairs
        // (380, 60), (400, 80) are perfectly correlated.
        assert_eq!(value["n_pairs"], 2);
        assert_eq!(value["pearson_r"], 1.0);
        // Two pairs, ranks agree → ρ = 1 as well.
        assert_eq!(value["spearman_r"], 1.0);
        assert_eq!(value["x_mean"], 390.0);
        assert_eq!(value["y_mean"], 70.0);
        assert!(value.get("groups").is_none(), "no threshold requested");
    }

    #[tokio::test]
    async fn observation_relationship_lag_shifts_y_forward() {
        let server = fresh_server_with_weight_and_hr().await;
        let mut args = relationship_args();
        args.lag = Some(1);
        args.threshold = Some(400.0);
        let result = server
            .observation_relationship(Parameters(args))
            .await
            .expect("tool call succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        // Only (x Jan 1 = 380, y Jan 2 = 80) pairs; a single pair has no
        // correlation.
        assert_eq!(value["n_pairs"], 1);
        assert_eq!(value["pearson_r"], serde_json::Value::Null);
        assert_eq!(value["spearman_r"], serde_json::Value::Null);
        assert_eq!(value["groups"]["x_below"]["count"], 1);
        assert_eq!(value["groups"]["x_below"]["mean"], 80.0);
        assert_eq!(value["groups"]["x_at_or_above"]["count"], 0);
    }

    #[tokio::test]
    async fn observation_relationship_episode_bucket_requires_episode_spec() {
        let server = fresh_server_with_weight_and_hr().await;
        let mut args = relationship_args();
        args.bucket = Some("episode".to_string());
        let err = server
            .observation_relationship(Parameters(args))
            .await
            .expect_err("missing episode spec");
        assert!(err.to_string().contains("episode"));
    }

    #[tokio::test]
    async fn observation_relationship_episode_bucket_pairs_by_index_with_lag() {
        // Three nights of sleep-stage epoch intervals define three
        // episodes. Only two nights (0, 1) also have a nightly total-sleep
        // summary (93832-4) whose effective_start falls inside its
        // episode. With lag 1, episode i's x pairs with episode i+1's y:
        // (x=night0=400, y=night1=420) is the only pair where both sides
        // have data — night1's x has no night2 y, and night2's x is null.
        let server = fresh_server_with_gapped_sleep_episodes().await;
        let mut args = relationship_args();
        args.x = table_column("93832-4");
        args.y = table_column("93832-4");
        args.bucket = Some("episode".to_string());
        args.lag = Some(1);
        args.episode = Some(EpisodeArgs {
            coding: Coding {
                system: "https://chartpds.fhwang.net/coding/aasm/sleep-stage".to_string(),
                code: "aasm-sleep-stage".to_string(),
            },
            gap_seconds: None,
        });
        let result = server
            .observation_relationship(Parameters(args))
            .await
            .expect("tool call succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["n_pairs"], 1);
        assert_eq!(value["x_mean"], 400.0);
        assert_eq!(value["y_mean"], 420.0);
    }

    #[tokio::test]
    async fn observation_stats_rejects_unknown_field_and_bucket() {
        let server = fresh_server_with_nightly_sleep().await;
        let mut args = stats_args();
        args.field = Some("nope".to_string());
        let err = server
            .observation_stats(Parameters(args))
            .await
            .expect_err("unknown field");
        assert!(err.to_string().contains("invalid field"));

        let mut args = stats_args();
        args.bucket = Some("century".to_string());
        let err = server
            .observation_stats(Parameters(args))
            .await
            .expect_err("unknown bucket");
        assert!(err.to_string().contains("invalid bucket"));
    }

    #[tokio::test]
    async fn source_connect_oura_stores_credentials() {
        let server = fresh_server_with_empty_db().await;

        let result = server
            .source_connect(Parameters(SourceConnectArgs {
                source: "oura".to_owned(),
                token: Some("test-pat-abc123".to_owned()),
            }))
            .await
            .expect("source_connect oura succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["source"], "oura");
        assert_eq!(value["status"], "connected");

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
    async fn source_connect_unknown_returns_error() {
        let server = fresh_server_with_empty_db().await;

        let err = server
            .source_connect(Parameters(SourceConnectArgs {
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
    async fn source_sync_unknown_returns_error() {
        let server = fresh_server_with_empty_db().await;

        let err = server
            .source_sync(Parameters(SourceSyncArgs {
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

    #[tokio::test]
    async fn source_sync_fitbit_without_credentials_reports_no_credentials() {
        let server = fresh_server_with_empty_db().await;
        let result = server
            .source_sync(Parameters(SourceSyncArgs {
                source: Some("fitbit".to_owned()),
                window_days: None,
            }))
            .await
            .expect("tool call succeeds (failure is in-band)");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        let arr = value["items"].as_array().expect("items array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["source"], "fitbit");
        assert_eq!(arr[0]["ok"], false);
        assert_eq!(arr[0]["reason"], "no_credentials");
    }

    #[tokio::test]
    async fn source_sync_oura_without_credentials_reports_no_credentials() {
        std::env::remove_var("OURA_PERSONAL_ACCESS_TOKEN");
        let server = fresh_server_with_empty_db().await;
        let result = server
            .source_sync(Parameters(SourceSyncArgs {
                source: Some("oura".to_owned()),
                window_days: None,
            }))
            .await
            .expect("tool call succeeds (failure is in-band)");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        let arr = value["items"].as_array().expect("items array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["source"], "oura");
        assert_eq!(arr[0]["ok"], false);
        assert_eq!(arr[0]["reason"], "no_credentials");
    }

    #[tokio::test]
    async fn source_sync_all_with_nothing_configured_returns_empty_results() {
        std::env::remove_var("OURA_PERSONAL_ACCESS_TOKEN");
        let server = fresh_server_with_empty_db().await;
        let result = server
            .source_sync(Parameters(SourceSyncArgs {
                source: None,
                window_days: None,
            }))
            .await
            .expect("tool call succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["items"].as_array().expect("array").len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn record_ingest_clinical_pdf_fails_without_key_and_persists_nothing() {
        // Hermetic: never let this test reach the network. `from_env` reads
        // ANTHROPIC_API_KEY, so clear it for this process before ingesting;
        // env mutation is process-global, so this test runs single-threaded
        // (flavor = "current_thread") and is the only test touching this var.
        std::env::remove_var("ANTHROPIC_API_KEY");

        let server = fresh_server_with_empty_db().await;
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("synthetic_pathology.pdf");
        std::fs::write(&path, PDF_FIXTURE).expect("write fixture");

        let err = server
            .record_ingest(Parameters(RecordIngestArgs {
                file_path: Some(path.to_string_lossy().into_owned()),
                content: None,
                kind: "clinical-pdf".to_string(),
                source: "manual-upload".to_string(),
                original_filename: None,
            }))
            .await
            .expect_err("ingest without a key must fail");
        assert!(
            err.to_string().contains("ANTHROPIC_API_KEY"),
            "error must name the missing configuration: {err}"
        );

        // Nothing was persisted: the failed ingest left no searchable text.
        let search = server
            .narrative_search(Parameters(NarrativeSearchArgs {
                query: Some("proctitis OR dysplasia".to_string()),
                limit: None,
            }))
            .await
            .expect("search");
        let text = match &search.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let hits: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(hits["items"].as_array().expect("items array").len(), 0);
    }

    #[tokio::test]
    async fn narrative_search_rejects_invalid_fts5_query() {
        let server = fresh_server_with_empty_db().await;

        let err = server
            .narrative_search(Parameters(NarrativeSearchArgs {
                query: Some("AND AND".to_owned()),
                limit: None,
            }))
            .await
            .expect_err("malformed FTS5 query must be rejected");

        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn narrative_search_rejects_column_filter_on_unindexed_column() {
        // narrative_texts_fts only indexes "text"; a column-filter query
        // like "title:biopsy" is caller error (invalid_params), not an
        // internal failure.
        let server = fresh_server_with_empty_db().await;

        let err = server
            .narrative_search(Parameters(NarrativeSearchArgs {
                query: Some("title:biopsy".to_owned()),
                limit: None,
            }))
            .await
            .expect_err("FTS5 column filter on a non-indexed column must be rejected");

        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn narrative_search_with_valid_query_returns_items_array() {
        let server = fresh_server_with_empty_db().await;

        let result = server
            .narrative_search(Parameters(NarrativeSearchArgs {
                query: Some("proctitis".to_owned()),
                limit: None,
            }))
            .await
            .expect("narrative_search tool call");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert!(value["items"].is_array());
    }

    #[tokio::test]
    async fn record_ingest_rejects_unknown_kind() {
        let server = fresh_server_with_empty_db().await;
        let err = server
            .record_ingest(Parameters(RecordIngestArgs {
                file_path: None,
                content: Some("whatever".to_string()),
                kind: "hl7v2".to_string(),
                source: "test".to_string(),
                original_filename: None,
            }))
            .await
            .expect_err("unknown kind must be rejected");
        assert!(err.to_string().contains("hl7v2"));
    }
}
