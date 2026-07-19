//! Aligned multi-coding tables: one row per bucket, one value per coding.
//!
//! A client asks for several codings over a window with one bucketing
//! granularity (calendar day / week / month, or episode) and gets back a
//! table already joined by bucket — no client-side re-keying. Each column
//! reduces a coding's observations in a bucket to a single value via an
//! aggregate; a bucket the coding never touched reads `null`.

use std::collections::{BTreeMap, BTreeSet};

use jiff::tz::TimeZone;
use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::queries::episodes::{
    detect_episodes, episode_index_for, fetch_all_intervals, runs, utc_instant_key, Episode,
};
use crate::queries::observation_stats::{
    bucket_key, fetch_rows, field_value, percentile, sort_by_hour_instant, ObservationStatsError,
};
use crate::queries::roll_up_bucket_confidence;
use crate::queries::{StatsBucket, StatsField};
use crate::sources::DayConfidence;

/// How a column reduces one bucket's observations to a single value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnAggregate {
    /// Arithmetic mean of the field values.
    Mean,
    /// Sum of the field values.
    Sum,
    /// Smallest field value.
    Min,
    /// Largest field value.
    Max,
    /// Number of observations with the field present.
    Count,
    /// Median (R type-7 linear interpolation) of the field values.
    Median,
    /// Minutes the coding's intervals spent inside an inclusive value range.
    /// `0.0` when the bucket has interval rows but none in range; `null`
    /// when it has no interval rows at all.
    DurationInRange {
        /// Minimum `value_quantity` (inclusive).
        value_min: f64,
        /// Maximum `value_quantity` (inclusive).
        value_max: f64,
    },
    /// Longest unbroken run of the coding's in-range intervals that STARTS
    /// in the bucket, in minutes.
    ///
    /// Runs are chained over the whole window's in-range intervals of this
    /// column's coding (not just the intervals in one bucket) — two
    /// consecutive in-range intervals join the same run while the gap
    /// between them is `<= gap_seconds`, and an out-of-range interval breaks
    /// the chain unless `gap_seconds` spans it (i.e. is at least as long as
    /// the out-of-range interval, bridging the two in-range neighbors on
    /// either side). Each run is then attributed WHOLE to the bucket
    /// containing its start, so a run crossing a bucket boundary (e.g.
    /// midnight for day buckets) stays in one row rather than splitting
    /// across two. A bucket with interval rows of the coding but no run
    /// starting in it reads `0.0`; a bucket with no interval rows at all
    /// reads `null`.
    LongestRunInRange {
        /// Minimum `value_quantity` (inclusive).
        value_min: f64,
        /// Maximum `value_quantity` (inclusive).
        value_max: f64,
        /// Maximum gap in seconds between consecutive in-range intervals
        /// that still counts as continuous.
        gap_seconds: i64,
    },
}

/// One requested column: a coding plus how to reduce it per bucket.
#[derive(Debug, Clone, Copy)]
pub struct ColumnSpec<'a> {
    /// FHIR coding system URI.
    pub coding_system: &'a str,
    /// Coding code within `coding_system`.
    pub coding_code: &'a str,
    /// How to reduce the bucket's observations to one value.
    pub aggregate: ColumnAggregate,
    /// Which per-observation number the value aggregates operate on.
    /// Ignored by the range aggregates ([`ColumnAggregate::DurationInRange`]
    /// and [`ColumnAggregate::LongestRunInRange`]).
    pub field: StatsField,
}

/// How to bucket the table's rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableBucket {
    /// One row over the whole window.
    None,
    /// Per clock hour (local to the request timezone), keyed by the RFC 3339
    /// top-of-hour instant with the local offset (`...Z` for UTC). Row order
    /// is by instant, not by lexicographic string order: those two diverge
    /// across a DST fall-back in a positive-UTC-offset zone (e.g.
    /// Europe/Berlin), where the second occurrence of the repeated wall
    /// hour has the smaller (`+01:00`) offset string but the later instant.
    Hour,
    /// Per local calendar day, keyed `YYYY-MM-DD`.
    Day,
    /// Per ISO week, keyed by the Monday date (`YYYY-MM-DD`).
    Week,
    /// Per calendar month, keyed `YYYY-MM`.
    Month,
    /// Per day of week, keyed `mon` … `sun` (output Monday-first).
    DayOfWeek,
    /// Per detected episode of the episode coding (see [`EpisodeSpec`]),
    /// keyed by the RFC 3339 UTC instant the episode began. Every episode
    /// in the window yields a row; every column's observations are assigned
    /// to the episode containing their `effective_start`.
    Episode,
}

/// The coding whose interval observations define the episodes for
/// [`TableBucket::Episode`].
#[derive(Debug, Clone, Copy)]
pub struct EpisodeSpec<'a> {
    /// FHIR coding system URI.
    pub coding_system: &'a str,
    /// Coding code within `coding_system`.
    pub coding_code: &'a str,
    /// Maximum gap in seconds between consecutive intervals that still
    /// chains them into one episode.
    pub gap_seconds: i64,
}

/// Parameters for [`aligned_table`].
pub struct AlignedTableParams<'a> {
    /// The requested columns, in output order.
    pub columns: &'a [ColumnSpec<'a>],
    /// Start of the half-open window (inclusive) on `effective_start`.
    pub start: OffsetDateTime,
    /// End of the half-open window (exclusive) on `effective_start`.
    pub end: OffsetDateTime,
    /// How to bucket the rows.
    pub bucket: TableBucket,
    /// Episode definition; required when `bucket` is [`TableBucket::Episode`].
    pub episode: Option<EpisodeSpec<'a>>,
    /// IANA timezone for calendar bucket boundaries and time-of-day fields;
    /// `None` = UTC.
    pub timezone: Option<&'a str>,
}

/// One table row: a bucket key plus one value per requested column.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct TableRow {
    /// Bucket key: `YYYY-MM-DD` (day / ISO-week Monday), `YYYY-MM` (month),
    /// `mon` … `sun` (day of week), an RFC 3339 top-of-hour instant (hour),
    /// or an RFC 3339 UTC instant (episode). `null` only for the single row
    /// of [`TableBucket::None`].
    pub bucket_key: Option<String>,
    /// One value per requested column, in request order; `null` when the
    /// coding has no qualifying observations in the bucket.
    pub values: Vec<Option<f64>>,
    /// `Provisional` if any contributing observation's source-day is
    /// provisional, else `Confirmed`.
    pub confidence: DayConfidence,
}

/// Result of [`aligned_table`]: rows in chronological bucket order.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct AlignedTable {
    /// The table rows.
    pub rows: Vec<TableRow>,
}

/// Failure modes of [`aligned_table`].
#[derive(Debug, thiserror::Error)]
pub enum AlignedTableError {
    /// The underlying SQL query failed.
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    /// The supplied `timezone` is not a known IANA zone name.
    #[error("invalid timezone: {0}")]
    InvalidTimezone(String),
    /// `bucket` was episode but no episode spec was supplied.
    #[error("bucket \"episode\" requires an episode spec")]
    MissingEpisodeSpec,
    /// An internal date/time conversion failed unexpectedly.
    #[error("internal datetime error: {0}")]
    Internal(String),
}

/// Build an aligned multi-coding table over `[start, end)`.
///
/// Each requested column's observations are fetched, assigned to buckets
/// (calendar buckets by `effective_start` in the request timezone; episode
/// buckets by containment in an episode of the episode coding), and reduced
/// to one value per bucket by the column's aggregate. Calendar rows appear
/// for every bucket where at least one column has observations; episode
/// rows appear for every detected episode. Missing cells are `None`.
///
/// # Errors
///
/// Returns [`AlignedTableError::Db`] if a query fails,
/// [`AlignedTableError::InvalidTimezone`] for an unknown IANA zone,
/// [`AlignedTableError::MissingEpisodeSpec`] when `bucket` is episode
/// without an episode spec, or [`AlignedTableError::Internal`] if a
/// date/time conversion fails unexpectedly.
pub async fn aligned_table(
    pool: &SqlitePool,
    now: OffsetDateTime,
    params: AlignedTableParams<'_>,
) -> Result<AlignedTable, AlignedTableError> {
    let tz = resolve_timezone(params.timezone)?;
    let episodes = detect_bucket_episodes(pool, &params).await?;

    let mut cells_per_column: Vec<BTreeMap<(u8, String), Cell>> = Vec::new();
    let mut contributions: Vec<(String, String, Option<String>)> = Vec::new();
    for column in params.columns {
        cells_per_column.push(
            column_cells(
                pool,
                column,
                &params,
                episodes.as_deref(),
                &tz,
                &mut contributions,
            )
            .await?,
        );
    }

    // Row keys: every detected episode, or the union of calendar buckets
    // any column touched (BTreeSet keeps them chronological — every
    // calendar key format except `Hour` sorts lexicographically, and the
    // leading `u8` orders `day_of_week` Monday-first). `Hour` keys are
    // re-sorted by parsed instant below, since their lexicographic order
    // does not always match instant order across a DST fall-back in a
    // positive-UTC-offset zone (issue #29 finding 1).
    let mut keys: Vec<(u8, String)> = match &episodes {
        Some(eps) => eps.iter().map(|e| (0, utc_instant_key(e.start))).collect(),
        None => cells_per_column
            .iter()
            .flat_map(|cells| cells.keys().cloned())
            .collect::<BTreeSet<(u8, String)>>()
            .into_iter()
            .collect(),
    };
    if params.bucket == TableBucket::Hour {
        sort_by_hour_instant(&mut keys, |key| key.1.as_str()).map_err(map_stats_err)?;
    }

    let confidence_by_bucket = roll_up_bucket_confidence(pool, now, &contributions).await?;
    Ok(AlignedTable {
        rows: keys
            .into_iter()
            .map(|key| TableRow {
                values: params
                    .columns
                    .iter()
                    .zip(&cells_per_column)
                    .map(|(column, cells)| cells.get(&key).and_then(|c| c.reduce(column.aggregate)))
                    .collect(),
                confidence: confidence_by_bucket
                    .get(&key.1)
                    .copied()
                    .unwrap_or(DayConfidence::Confirmed),
                bucket_key: (params.bucket != TableBucket::None).then_some(key.1),
            })
            .collect(),
    })
}

/// Resolve an optional IANA zone name (`None` = UTC).
///
/// `pub(crate)` so [`crate::queries::signal_relationship`] can resolve the
/// same timezone for hour-bucket lag key-shifting.
pub(crate) fn resolve_timezone(name: Option<&str>) -> Result<TimeZone, AlignedTableError> {
    match name {
        Some(name) => {
            TimeZone::get(name).map_err(|_| AlignedTableError::InvalidTimezone(name.to_string()))
        }
        None => Ok(TimeZone::UTC),
    }
}

/// For [`TableBucket::Episode`], detect the episode coding's episodes;
/// `None` for calendar buckets.
async fn detect_bucket_episodes(
    pool: &SqlitePool,
    params: &AlignedTableParams<'_>,
) -> Result<Option<Vec<Episode>>, AlignedTableError> {
    if params.bucket != TableBucket::Episode {
        return Ok(None);
    }
    let spec = params
        .episode
        .as_ref()
        .ok_or(AlignedTableError::MissingEpisodeSpec)?;
    let rows = fetch_all_intervals(
        pool,
        spec.coding_system,
        spec.coding_code,
        params.start,
        params.end,
    )
    .await?;
    let intervals: Vec<(OffsetDateTime, OffsetDateTime)> =
        rows.iter().map(|r| (r.start, r.end)).collect();
    Ok(Some(detect_episodes(&intervals, spec.gap_seconds)))
}

/// One bucket's accumulated inputs for a single column.
#[derive(Default)]
struct Cell {
    /// Field values (value aggregates only).
    values: Vec<f64>,
    /// Whether any row of the coding landed in the bucket.
    rows_seen: bool,
    /// Whether any interval row landed in the bucket (duration aggregate).
    intervals_seen: bool,
    /// Seconds of in-range intervals (duration aggregate).
    in_range_seconds: i64,
    /// Longest in-range run starting in this bucket, in minutes (longest-run
    /// aggregate). Default `0.0`, distinguished from "no data" by
    /// `intervals_seen`.
    longest_minutes: f64,
}

impl Cell {
    /// Reduce the accumulated inputs to the column's single value.
    fn reduce(&self, aggregate: ColumnAggregate) -> Option<f64> {
        #[allow(
            clippy::cast_precision_loss,
            reason = "realistic observation counts and interval seconds fit f64 without loss"
        )]
        match aggregate {
            ColumnAggregate::DurationInRange { .. } => self
                .intervals_seen
                .then(|| self.in_range_seconds as f64 / 60.0),
            ColumnAggregate::LongestRunInRange { .. } => {
                self.intervals_seen.then_some(self.longest_minutes)
            }
            ColumnAggregate::Count => self.rows_seen.then_some(self.values.len() as f64),
            ColumnAggregate::Sum => (!self.values.is_empty()).then(|| self.values.iter().sum()),
            ColumnAggregate::Mean => (!self.values.is_empty())
                .then(|| self.values.iter().sum::<f64>() / self.values.len() as f64),
            ColumnAggregate::Min => self.values.iter().copied().reduce(f64::min),
            ColumnAggregate::Max => self.values.iter().copied().reduce(f64::max),
            ColumnAggregate::Median => {
                let mut sorted = self.values.clone();
                sorted.sort_by(f64::total_cmp);
                percentile(&sorted, 0.5)
            }
        }
    }
}

/// Fetch one column's observations and accumulate them into per-bucket
/// cells, appending each row's confidence contribution.
async fn column_cells(
    pool: &SqlitePool,
    column: &ColumnSpec<'_>,
    params: &AlignedTableParams<'_>,
    episodes: Option<&[Episode]>,
    tz: &TimeZone,
    contributions: &mut Vec<(String, String, Option<String>)>,
) -> Result<BTreeMap<(u8, String), Cell>, AlignedTableError> {
    let rows = fetch_rows(
        pool,
        column.coding_system,
        column.coding_code,
        params.start,
        params.end,
    )
    .await?;
    let mut cells: BTreeMap<(u8, String), Cell> = BTreeMap::new();
    // Longest-run needs a window-wide pass: the chain is built over ALL of
    // the column's in-range intervals in the window, not per bucket, so
    // in-range intervals are collected here (fetch order) and reduced to
    // runs after the loop below.
    let mut in_range: Vec<(OffsetDateTime, OffsetDateTime)> = Vec::new();
    for row in &rows {
        let label = match episodes {
            Some(eps) => match episode_index_for(eps, row.effective_start) {
                Some(i) => (0, utc_instant_key(eps[i].start)),
                None => continue,
            },
            None => calendar_label(row.effective_start, params.bucket, tz)?,
        };
        contributions.push((
            label.1.clone(),
            row.source.clone(),
            row.document_date.clone(),
        ));
        let cell = cells.entry(label).or_default();
        cell.rows_seen = true;
        match column.aggregate {
            ColumnAggregate::DurationInRange {
                value_min,
                value_max,
            } => {
                if let Some(end) = row.effective_end {
                    cell.intervals_seen = true;
                    if row
                        .value_quantity
                        .is_some_and(|v| v >= value_min && v <= value_max)
                    {
                        cell.in_range_seconds +=
                            end.unix_timestamp() - row.effective_start.unix_timestamp();
                    }
                }
            }
            ColumnAggregate::LongestRunInRange {
                value_min,
                value_max,
                ..
            } => {
                if let Some(end) = row.effective_end {
                    cell.intervals_seen = true;
                    if row
                        .value_quantity
                        .is_some_and(|v| v >= value_min && v <= value_max)
                    {
                        in_range.push((row.effective_start, end));
                    }
                }
            }
            _ => {
                if let Some(value) = field_value(row, column.field, tz).map_err(map_stats_err)? {
                    cell.values.push(value);
                }
            }
        }
    }
    if let ColumnAggregate::LongestRunInRange { gap_seconds, .. } = column.aggregate {
        for run in runs(&in_range, gap_seconds) {
            let label = match episodes {
                Some(eps) => match episode_index_for(eps, run.start) {
                    Some(i) => (0, utc_instant_key(eps[i].start)),
                    None => continue,
                },
                None => calendar_label(run.start, params.bucket, tz)?,
            };
            let cell = cells.entry(label).or_default();
            cell.longest_minutes = cell.longest_minutes.max(run.minutes);
        }
    }
    Ok(cells)
}

/// Calendar bucket label of an instant (none / hour / day / ISO week
/// Monday / month / day of week).
fn calendar_label(
    dt: OffsetDateTime,
    bucket: TableBucket,
    tz: &TimeZone,
) -> Result<(u8, String), AlignedTableError> {
    let stats_bucket = match bucket {
        TableBucket::None => StatsBucket::None,
        TableBucket::Hour => StatsBucket::Hour,
        TableBucket::Day => StatsBucket::Day,
        TableBucket::Week => StatsBucket::Week,
        TableBucket::Month => StatsBucket::Month,
        TableBucket::DayOfWeek => StatsBucket::DayOfWeek,
        TableBucket::Episode => {
            return Err(AlignedTableError::Internal(
                "episode buckets are not calendar buckets".to_string(),
            ))
        }
    };
    bucket_key(dt, stats_bucket, tz).map_err(map_stats_err)
}

/// Map helper errors from the shared `observation_stats` machinery.
fn map_stats_err(err: ObservationStatsError) -> AlignedTableError {
    match err {
        ObservationStatsError::Db(err) => AlignedTableError::Db(err),
        ObservationStatsError::InvalidTimezone(name) => AlignedTableError::InvalidTimezone(name),
        ObservationStatsError::Internal(msg) => AlignedTableError::Internal(msg),
        // `bucket_key`/`field_value` never construct this variant (episode
        // buckets take the `Internal` arm above); mapped for exhaustiveness.
        ObservationStatsError::MissingEpisodeSpec => AlignedTableError::MissingEpisodeSpec,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clinical::{AASM_SLEEP_STAGE_CODE, AASM_SLEEP_STAGE_SYSTEM, SYSTEM_LOINC};
    use crate::queries::test_support::{
        hr_interval, seed_interval_observations, seed_observations, IntervalObsSpec, ObsSpec,
    };
    use time::macros::datetime;

    const NOW: OffsetDateTime = datetime!(2026-07-01 00:00:00 UTC);

    fn value_column(code: &str, aggregate: ColumnAggregate) -> ColumnSpec<'_> {
        ColumnSpec {
            coding_system: SYSTEM_LOINC,
            coding_code: code,
            aggregate,
            field: StatsField::Value,
        }
    }

    fn day_params<'a>(columns: &'a [ColumnSpec<'a>]) -> AlignedTableParams<'a> {
        AlignedTableParams {
            columns,
            start: datetime!(2026-01-01 00:00:00 UTC),
            end: datetime!(2026-02-01 00:00:00 UTC),
            bucket: TableBucket::Day,
            episode: None,
            timezone: None,
        }
    }

    /// Body weight on Jan 1/2/3; heart rate on Jan 1/2 only.
    fn weight_and_hr_specs() -> [ObsSpec; 5] {
        let obs = |code, day, value| ObsSpec {
            coding_code: code,
            coding_display: None,
            effective_start: datetime!(2026-01-01 08:00:00 UTC) + time::Duration::days(day),
            value_quantity: Some(value),
            value_unit: None,
        };
        [
            obs("29463-7", 0, 380.0),
            obs("29463-7", 1, 400.0),
            obs("29463-7", 2, 420.0),
            obs("8867-4", 0, 60.0),
            obs("8867-4", 1, 80.0),
        ]
    }

    #[tokio::test]
    async fn day_rows_align_codings_with_explicit_null() {
        let (pool, _) = seed_observations(&weight_and_hr_specs()).await;
        let columns = [
            value_column("29463-7", ColumnAggregate::Mean),
            value_column("8867-4", ColumnAggregate::Mean),
        ];
        let table = aligned_table(&pool, NOW, day_params(&columns))
            .await
            .expect("query");
        assert_eq!(table.rows.len(), 3);
        assert_eq!(table.rows[0].bucket_key.as_deref(), Some("2026-01-01"));
        assert_eq!(table.rows[0].values, vec![Some(380.0), Some(60.0)]);
        assert_eq!(table.rows[1].bucket_key.as_deref(), Some("2026-01-02"));
        assert_eq!(table.rows[1].values, vec![Some(400.0), Some(80.0)]);
        // Jan 3 has weight but NO heart rate: explicit null, row still present.
        assert_eq!(table.rows[2].bucket_key.as_deref(), Some("2026-01-03"));
        assert_eq!(table.rows[2].values, vec![Some(420.0), None]);
        assert_eq!(table.rows[2].confidence, DayConfidence::Confirmed);
    }

    #[tokio::test]
    async fn aggregates_reduce_one_bucket_exactly() {
        // Three same-day weights: 380 / 400 / 420.
        let specs: Vec<ObsSpec> = [380.0, 400.0, 420.0]
            .iter()
            .enumerate()
            .map(|(i, &v)| ObsSpec {
                coding_code: "29463-7",
                coding_display: None,
                effective_start: datetime!(2026-01-01 08:00:00 UTC)
                    + time::Duration::hours(i64::try_from(i).expect("small index")),
                value_quantity: Some(v),
                value_unit: None,
            })
            .collect();
        let (pool, _) = seed_observations(&specs).await;
        let columns = [
            value_column("29463-7", ColumnAggregate::Mean),
            value_column("29463-7", ColumnAggregate::Sum),
            value_column("29463-7", ColumnAggregate::Min),
            value_column("29463-7", ColumnAggregate::Max),
            value_column("29463-7", ColumnAggregate::Count),
            value_column("29463-7", ColumnAggregate::Median),
        ];
        let table = aligned_table(&pool, NOW, day_params(&columns))
            .await
            .expect("query");
        assert_eq!(table.rows.len(), 1);
        assert_eq!(
            table.rows[0].values,
            vec![
                Some(400.0),
                Some(1200.0),
                Some(380.0),
                Some(420.0),
                Some(3.0),
                Some(400.0),
            ]
        );
    }

    #[tokio::test]
    async fn duration_in_range_column_distinguishes_zero_from_null() {
        // Jan 1: one in-range HR minute (110). Jan 2: one out-of-range HR
        // minute (130) -> 0.0, not null.
        let (pool, _) = seed_interval_observations(&[
            IntervalObsSpec {
                coding_system: SYSTEM_LOINC,
                coding_code: "8867-4",
                effective_start: datetime!(2026-01-01 08:00:00 UTC),
                effective_end: datetime!(2026-01-01 08:01:00 UTC),
                value_quantity: 110.0,
            },
            IntervalObsSpec {
                coding_system: SYSTEM_LOINC,
                coding_code: "8867-4",
                effective_start: datetime!(2026-01-02 08:00:00 UTC),
                effective_end: datetime!(2026-01-02 08:01:00 UTC),
                value_quantity: 130.0,
            },
        ])
        .await;
        let columns = [ColumnSpec {
            coding_system: SYSTEM_LOINC,
            coding_code: "8867-4",
            aggregate: ColumnAggregate::DurationInRange {
                value_min: 101.0,
                value_max: 118.0,
            },
            field: StatsField::Value,
        }];
        let table = aligned_table(&pool, NOW, day_params(&columns))
            .await
            .expect("query");
        assert_eq!(table.rows.len(), 2);
        assert_eq!(table.rows[0].bucket_key.as_deref(), Some("2026-01-01"));
        assert_eq!(table.rows[0].values, vec![Some(1.0)]);
        assert_eq!(table.rows[1].bucket_key.as_deref(), Some("2026-01-02"));
        assert_eq!(table.rows[1].values, vec![Some(0.0)]);
    }

    #[tokio::test]
    async fn episode_rows_align_other_codings_into_sleep_periods() {
        // Two nights of epochs; each night also has a nightly summary
        // observation (total sleep minutes) spanning the same interval.
        let epoch = |start, end, stage| IntervalObsSpec {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            effective_start: start,
            effective_end: end,
            value_quantity: stage,
        };
        let (pool, _) = seed_interval_observations(&[
            // Night A crosses UTC midnight: 5 deep minutes.
            epoch(
                datetime!(2026-06-26 23:50:00 UTC),
                datetime!(2026-06-26 23:55:00 UTC),
                3.0,
            ),
            epoch(
                datetime!(2026-06-26 23:55:00 UTC),
                datetime!(2026-06-27 00:00:00 UTC),
                2.0,
            ),
            // Night A summary: 10 total minutes.
            IntervalObsSpec {
                coding_system: SYSTEM_LOINC,
                coding_code: "93832-4",
                effective_start: datetime!(2026-06-26 23:50:00 UTC),
                effective_end: datetime!(2026-06-27 00:00:00 UTC),
                value_quantity: 10.0,
            },
            // Night B: no deep sleep.
            epoch(
                datetime!(2026-06-27 23:00:00 UTC),
                datetime!(2026-06-27 23:05:00 UTC),
                2.0,
            ),
            IntervalObsSpec {
                coding_system: SYSTEM_LOINC,
                coding_code: "93832-4",
                effective_start: datetime!(2026-06-27 23:00:00 UTC),
                effective_end: datetime!(2026-06-27 23:05:00 UTC),
                value_quantity: 5.0,
            },
        ])
        .await;
        let columns = [
            ColumnSpec {
                coding_system: SYSTEM_LOINC,
                coding_code: "93832-4",
                aggregate: ColumnAggregate::Mean,
                field: StatsField::Value,
            },
            ColumnSpec {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                aggregate: ColumnAggregate::DurationInRange {
                    value_min: 3.0,
                    value_max: 3.0,
                },
                field: StatsField::Value,
            },
        ];
        let table = aligned_table(
            &pool,
            NOW,
            AlignedTableParams {
                columns: &columns,
                start: datetime!(2026-06-26 00:00:00 UTC),
                end: datetime!(2026-06-29 00:00:00 UTC),
                bucket: TableBucket::Episode,
                episode: Some(EpisodeSpec {
                    coding_system: AASM_SLEEP_STAGE_SYSTEM,
                    coding_code: AASM_SLEEP_STAGE_CODE,
                    gap_seconds: 0,
                }),
                timezone: None,
            },
        )
        .await
        .expect("query");
        assert_eq!(table.rows.len(), 2);
        assert_eq!(
            table.rows[0].bucket_key.as_deref(),
            Some("2026-06-26T23:50:00Z")
        );
        assert_eq!(table.rows[0].values, vec![Some(10.0), Some(5.0)]);
        assert_eq!(
            table.rows[1].bucket_key.as_deref(),
            Some("2026-06-27T23:00:00Z")
        );
        assert_eq!(table.rows[1].values, vec![Some(5.0), Some(0.0)]);
    }

    #[tokio::test]
    async fn longest_run_chains_across_gap_and_attributes_to_start_day() {
        // 23:50–00:00 and 00:05–00:15 next day, gap 5 min, all in range:
        // with gap_seconds 300 this is ONE 25-minute run, attributed to Jan 1.
        let (pool, _) = seed_interval_observations(&[
            hr_interval(
                datetime!(2026-01-01 23:50:00 UTC),
                datetime!(2026-01-02 00:00:00 UTC),
                110.0,
            ),
            hr_interval(
                datetime!(2026-01-02 00:05:00 UTC),
                datetime!(2026-01-02 00:15:00 UTC),
                110.0,
            ),
        ])
        .await;
        let columns = [ColumnSpec {
            coding_system: SYSTEM_LOINC,
            coding_code: "8867-4",
            aggregate: ColumnAggregate::LongestRunInRange {
                value_min: 100.0,
                value_max: 120.0,
                gap_seconds: 300,
            },
            field: StatsField::Value,
        }];
        let table = aligned_table(&pool, NOW, day_params(&columns))
            .await
            .expect("query");
        // Jan 1 row: the whole 25-minute run. Jan 2 row: has interval rows but
        // no run STARTING there -> 0.0, not null.
        assert_eq!(table.rows[0].bucket_key.as_deref(), Some("2026-01-01"));
        assert_eq!(table.rows[0].values, vec![Some(25.0)]);
        assert_eq!(table.rows[1].bucket_key.as_deref(), Some("2026-01-02"));
        assert_eq!(table.rows[1].values, vec![Some(0.0)]);
    }

    #[tokio::test]
    async fn longest_run_out_of_range_rows_break_runs() {
        // in-range, out-of-range, in-range back to back: two 10-minute runs, not one.
        let (pool, _) = seed_interval_observations(&[
            hr_interval(
                datetime!(2026-01-01 08:00:00 UTC),
                datetime!(2026-01-01 08:10:00 UTC),
                110.0,
            ),
            hr_interval(
                datetime!(2026-01-01 08:10:00 UTC),
                datetime!(2026-01-01 08:20:00 UTC),
                130.0,
            ),
            hr_interval(
                datetime!(2026-01-01 08:20:00 UTC),
                datetime!(2026-01-01 08:30:00 UTC),
                110.0,
            ),
        ])
        .await;
        let columns = [ColumnSpec {
            coding_system: SYSTEM_LOINC,
            coding_code: "8867-4",
            aggregate: ColumnAggregate::LongestRunInRange {
                value_min: 100.0,
                value_max: 120.0,
                gap_seconds: 0,
            },
            field: StatsField::Value,
        }];
        let table = aligned_table(&pool, NOW, day_params(&columns))
            .await
            .expect("query");
        assert_eq!(table.rows[0].values, vec![Some(10.0)]);
    }

    // --- ported from the retired duration_in_value_range /
    // longest_continuous_in_value_range modules (issue #29 task 6): behaviors
    // not otherwise exercised through the aligned_table column-aggregate path.

    #[tokio::test]
    async fn duration_in_range_respects_value_boundary_inclusivity() {
        // Four 1-minute intervals: exactly value_min, exactly value_max, just
        // below min, just above max. Only the two boundary-exact ones count.
        let (pool, _) = seed_interval_observations(&[
            hr_interval(
                datetime!(2026-01-01 08:00:00 UTC),
                datetime!(2026-01-01 08:01:00 UTC),
                100.0, // == value_min: included
            ),
            hr_interval(
                datetime!(2026-01-01 08:01:00 UTC),
                datetime!(2026-01-01 08:02:00 UTC),
                120.0, // == value_max: included
            ),
            hr_interval(
                datetime!(2026-01-01 08:02:00 UTC),
                datetime!(2026-01-01 08:03:00 UTC),
                99.9, // just below min: excluded
            ),
            hr_interval(
                datetime!(2026-01-01 08:03:00 UTC),
                datetime!(2026-01-01 08:04:00 UTC),
                120.1, // just above max: excluded
            ),
        ])
        .await;
        let columns = [ColumnSpec {
            coding_system: SYSTEM_LOINC,
            coding_code: "8867-4",
            aggregate: ColumnAggregate::DurationInRange {
                value_min: 100.0,
                value_max: 120.0,
            },
            field: StatsField::Value,
        }];
        let table = aligned_table(&pool, NOW, day_params(&columns))
            .await
            .expect("query");
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows[0].values, vec![Some(2.0)]);
    }

    #[tokio::test]
    async fn duration_in_range_hour_bucket_credits_whole_interval_to_start_hour() {
        // A 10-minute interval spanning 07:55Z-08:05Z crosses the UTC hour
        // boundary but is credited whole to the 07:00 bucket (its start),
        // never split 5/5 across 07:00 and 08:00.
        let (pool, _) = seed_interval_observations(&[hr_interval(
            datetime!(2026-01-01 07:55:00 UTC),
            datetime!(2026-01-01 08:05:00 UTC),
            110.0,
        )])
        .await;
        let columns = [ColumnSpec {
            coding_system: SYSTEM_LOINC,
            coding_code: "8867-4",
            aggregate: ColumnAggregate::DurationInRange {
                value_min: 100.0,
                value_max: 120.0,
            },
            field: StatsField::Value,
        }];
        let mut params = day_params(&columns);
        params.bucket = TableBucket::Hour;
        let table = aligned_table(&pool, NOW, params).await.expect("query");
        assert_eq!(table.rows.len(), 1);
        assert_eq!(
            table.rows[0].bucket_key.as_deref(),
            Some("2026-01-01T07:00:00Z")
        );
        assert_eq!(table.rows[0].values, vec![Some(10.0)]);
    }

    #[tokio::test]
    async fn hour_bucket_dst_fall_back_keeps_two_distinct_local_hours() {
        // 2026-11-01 fall-back: 05:30Z is 01:30 EDT (-04:00), 06:30Z is
        // 01:30 EST (-05:00). Wall-clock 01:00 occurs twice, so the two
        // epochs must land in two distinct hour buckets, not merge.
        let (pool, _) = seed_interval_observations(&[
            IntervalObsSpec {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                effective_start: datetime!(2026-11-01 05:30:00 UTC),
                effective_end: datetime!(2026-11-01 05:35:00 UTC),
                value_quantity: 0.0,
            },
            IntervalObsSpec {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                effective_start: datetime!(2026-11-01 06:30:00 UTC),
                effective_end: datetime!(2026-11-01 06:35:00 UTC),
                value_quantity: 0.0,
            },
        ])
        .await;
        let columns = [ColumnSpec {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            aggregate: ColumnAggregate::DurationInRange {
                value_min: 0.0,
                value_max: 0.0,
            },
            field: StatsField::Value,
        }];
        let table = aligned_table(
            &pool,
            NOW,
            AlignedTableParams {
                columns: &columns,
                start: datetime!(2026-11-01 00:00:00 UTC),
                end: datetime!(2026-11-02 00:00:00 UTC),
                bucket: TableBucket::Hour,
                episode: None,
                timezone: Some("America/New_York"),
            },
        )
        .await
        .expect("query");
        assert_eq!(table.rows.len(), 2);
        assert_eq!(
            table.rows[0].bucket_key.as_deref(),
            Some("2026-11-01T01:00:00-04:00")
        );
        assert_eq!(table.rows[0].values, vec![Some(5.0)]);
        assert_eq!(
            table.rows[1].bucket_key.as_deref(),
            Some("2026-11-01T01:00:00-05:00")
        );
        assert_eq!(table.rows[1].values, vec![Some(5.0)]);
    }

    #[tokio::test]
    async fn hour_bucket_dst_fall_back_orders_by_instant_in_positive_offset_zone() {
        // Europe/Berlin falls back at 2026-10-25 03:00 CEST -> 02:00 CET
        // (01:00 UTC). The repeated 02:00 wall hour's two occurrences must
        // come out in instant order: +02:00 (earlier, UTC 00:30) before
        // +01:00 (later, UTC 01:30) — the opposite of lexicographic string
        // order, which would put "+01:00" first (issue #29 finding 1).
        let (pool, _) = seed_interval_observations(&[
            IntervalObsSpec {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                effective_start: datetime!(2026-10-25 00:30:00 UTC),
                effective_end: datetime!(2026-10-25 00:35:00 UTC),
                value_quantity: 0.0,
            },
            IntervalObsSpec {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                effective_start: datetime!(2026-10-25 01:30:00 UTC),
                effective_end: datetime!(2026-10-25 01:35:00 UTC),
                value_quantity: 0.0,
            },
        ])
        .await;
        let columns = [ColumnSpec {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            aggregate: ColumnAggregate::DurationInRange {
                value_min: 0.0,
                value_max: 0.0,
            },
            field: StatsField::Value,
        }];
        let table = aligned_table(
            &pool,
            NOW,
            AlignedTableParams {
                columns: &columns,
                start: datetime!(2026-10-25 00:00:00 UTC),
                end: datetime!(2026-10-25 03:00:00 UTC),
                bucket: TableBucket::Hour,
                episode: None,
                timezone: Some("Europe/Berlin"),
            },
        )
        .await
        .expect("query");
        assert_eq!(table.rows.len(), 2);
        assert_eq!(
            table.rows[0].bucket_key.as_deref(),
            Some("2026-10-25T02:00:00+02:00")
        );
        assert_eq!(table.rows[0].values, vec![Some(5.0)]);
        assert_eq!(
            table.rows[1].bucket_key.as_deref(),
            Some("2026-10-25T02:00:00+01:00")
        );
        assert_eq!(table.rows[1].values, vec![Some(5.0)]);
    }

    #[tokio::test]
    async fn hour_bucket_fractional_offset_zone_regroups_by_local_hour() {
        // Asia/Kolkata is +05:30. Both intervals share the UTC 06:00 hour,
        // but the half-hour offset puts them in different IST local hours.
        let (pool, _) = seed_interval_observations(&[
            IntervalObsSpec {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                effective_start: datetime!(2026-06-27 06:15:00 UTC),
                effective_end: datetime!(2026-06-27 06:20:00 UTC),
                value_quantity: 0.0,
            },
            IntervalObsSpec {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                effective_start: datetime!(2026-06-27 06:45:00 UTC),
                effective_end: datetime!(2026-06-27 06:50:00 UTC),
                value_quantity: 0.0,
            },
        ])
        .await;
        let columns = [ColumnSpec {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            aggregate: ColumnAggregate::DurationInRange {
                value_min: 0.0,
                value_max: 0.0,
            },
            field: StatsField::Value,
        }];
        let table = aligned_table(
            &pool,
            NOW,
            AlignedTableParams {
                columns: &columns,
                start: datetime!(2026-06-27 00:00:00 UTC),
                end: datetime!(2026-06-28 00:00:00 UTC),
                bucket: TableBucket::Hour,
                episode: None,
                timezone: Some("Asia/Kolkata"),
            },
        )
        .await
        .expect("query");
        assert_eq!(table.rows.len(), 2);
        assert_eq!(
            table.rows[0].bucket_key.as_deref(),
            Some("2026-06-27T11:00:00+05:30")
        );
        assert_eq!(table.rows[0].values, vec![Some(5.0)]);
        assert_eq!(
            table.rows[1].bucket_key.as_deref(),
            Some("2026-06-27T12:00:00+05:30")
        );
        assert_eq!(table.rows[1].values, vec![Some(5.0)]);
    }

    #[tokio::test]
    async fn duration_in_range_excludes_rows_outside_window() {
        // Window covers only Jan 1; the Jan 2 row must not contribute even
        // though it's in value range.
        let (pool, _) = seed_interval_observations(&[
            hr_interval(
                datetime!(2026-01-01 08:00:00 UTC),
                datetime!(2026-01-01 08:01:00 UTC),
                110.0,
            ),
            hr_interval(
                datetime!(2026-01-02 08:00:00 UTC),
                datetime!(2026-01-02 08:01:00 UTC),
                110.0,
            ),
        ])
        .await;
        let columns = [ColumnSpec {
            coding_system: SYSTEM_LOINC,
            coding_code: "8867-4",
            aggregate: ColumnAggregate::DurationInRange {
                value_min: 90.0,
                value_max: 140.0,
            },
            field: StatsField::Value,
        }];
        let table = aligned_table(
            &pool,
            NOW,
            AlignedTableParams {
                columns: &columns,
                start: datetime!(2026-01-01 00:00:00 UTC),
                end: datetime!(2026-01-02 00:00:00 UTC),
                bucket: TableBucket::Day,
                episode: None,
                timezone: None,
            },
        )
        .await
        .expect("query");
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows[0].bucket_key.as_deref(), Some("2026-01-01"));
        assert_eq!(table.rows[0].values, vec![Some(1.0)]);
    }

    #[tokio::test]
    async fn duration_in_range_does_not_cross_coding_systems() {
        // An AASM row with value 3 must not be counted as an 8867-4 (HR)
        // row, even though both queries could share a numeric range.
        let (pool, _) = seed_interval_observations(&[IntervalObsSpec {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            effective_start: datetime!(2026-01-01 08:00:00 UTC),
            effective_end: datetime!(2026-01-01 08:05:00 UTC),
            value_quantity: 3.0,
        }])
        .await;
        let columns = [ColumnSpec {
            coding_system: SYSTEM_LOINC,
            coding_code: "8867-4",
            aggregate: ColumnAggregate::DurationInRange {
                value_min: 1.0,
                value_max: 4.0,
            },
            field: StatsField::Value,
        }];
        let table = aligned_table(&pool, NOW, day_params(&columns))
            .await
            .expect("query");
        assert!(table.rows.is_empty());
    }

    #[tokio::test]
    async fn longest_run_per_bucket_picks_the_max_of_two_separate_runs() {
        // Same day: a 10-minute run (two chained 5-min intervals) and a
        // separate 5-minute run later. gap 0 keeps them apart; the bucket
        // reports the longer one, not their sum.
        let (pool, _) = seed_interval_observations(&[
            hr_interval(
                datetime!(2026-01-01 08:00:00 UTC),
                datetime!(2026-01-01 08:05:00 UTC),
                110.0,
            ),
            hr_interval(
                datetime!(2026-01-01 08:05:00 UTC),
                datetime!(2026-01-01 08:10:00 UTC),
                110.0,
            ),
            hr_interval(
                datetime!(2026-01-01 09:00:00 UTC),
                datetime!(2026-01-01 09:05:00 UTC),
                110.0,
            ),
        ])
        .await;
        let columns = [ColumnSpec {
            coding_system: SYSTEM_LOINC,
            coding_code: "8867-4",
            aggregate: ColumnAggregate::LongestRunInRange {
                value_min: 100.0,
                value_max: 120.0,
                gap_seconds: 0,
            },
            field: StatsField::Value,
        }];
        let table = aligned_table(&pool, NOW, day_params(&columns))
            .await
            .expect("query");
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows[0].values, vec![Some(10.0)]);
    }

    #[tokio::test]
    async fn episode_bucket_longest_run_stays_whole_across_midnight_and_zeroes_runless_episode() {
        // Night A: deep run 23:55Z-00:05Z crosses UTC midnight but must land
        // whole in night A's episode row. Night B has no deep sleep at all;
        // its episode reports 0.0, not null (it has AASM rows).
        let epoch = |start, end, stage| IntervalObsSpec {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            effective_start: start,
            effective_end: end,
            value_quantity: stage,
        };
        let (pool, _) = seed_interval_observations(&[
            epoch(
                datetime!(2026-06-26 23:50:00 UTC),
                datetime!(2026-06-26 23:55:00 UTC),
                2.0,
            ),
            epoch(
                datetime!(2026-06-26 23:55:00 UTC),
                datetime!(2026-06-27 00:00:00 UTC),
                3.0,
            ),
            epoch(
                datetime!(2026-06-27 00:00:00 UTC),
                datetime!(2026-06-27 00:05:00 UTC),
                3.0,
            ),
            epoch(
                datetime!(2026-06-27 00:05:00 UTC),
                datetime!(2026-06-27 00:10:00 UTC),
                2.0,
            ),
            epoch(
                datetime!(2026-06-27 23:00:00 UTC),
                datetime!(2026-06-27 23:05:00 UTC),
                2.0,
            ),
        ])
        .await;
        let columns = [ColumnSpec {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            aggregate: ColumnAggregate::LongestRunInRange {
                value_min: 3.0,
                value_max: 3.0,
                gap_seconds: 0,
            },
            field: StatsField::Value,
        }];
        let table = aligned_table(
            &pool,
            NOW,
            AlignedTableParams {
                columns: &columns,
                start: datetime!(2026-06-26 00:00:00 UTC),
                end: datetime!(2026-06-29 00:00:00 UTC),
                bucket: TableBucket::Episode,
                episode: Some(EpisodeSpec {
                    coding_system: AASM_SLEEP_STAGE_SYSTEM,
                    coding_code: AASM_SLEEP_STAGE_CODE,
                    gap_seconds: 0,
                }),
                timezone: None,
            },
        )
        .await
        .expect("query");
        assert_eq!(table.rows.len(), 2);
        assert_eq!(
            table.rows[0].bucket_key.as_deref(),
            Some("2026-06-26T23:50:00Z")
        );
        assert_eq!(table.rows[0].values, vec![Some(10.0)]);
        assert_eq!(
            table.rows[1].bucket_key.as_deref(),
            Some("2026-06-27T23:00:00Z")
        );
        assert_eq!(table.rows[1].values, vec![Some(0.0)]);
    }

    #[tokio::test]
    async fn month_bucket_keys_by_year_month() {
        let (pool, _) = seed_observations(&weight_and_hr_specs()).await;
        let columns = [value_column("29463-7", ColumnAggregate::Mean)];
        let mut params = day_params(&columns);
        params.bucket = TableBucket::Month;
        let table = aligned_table(&pool, NOW, params).await.expect("query");
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows[0].bucket_key.as_deref(), Some("2026-01"));
        assert_eq!(table.rows[0].values, vec![Some(400.0)]);
    }

    #[tokio::test]
    async fn none_bucket_returns_one_whole_window_row_with_null_key() {
        let (pool, _) = seed_observations(&weight_and_hr_specs()).await;
        let columns = [value_column("29463-7", ColumnAggregate::Mean)];
        let mut params = day_params(&columns);
        params.bucket = TableBucket::None;
        let table = aligned_table(&pool, NOW, params).await.expect("query");
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows[0].bucket_key, None);
        assert_eq!(table.rows[0].values, vec![Some(400.0)]); // mean of 380/400/420
    }

    #[tokio::test]
    async fn day_of_week_rows_are_monday_first() {
        // weight_and_hr_specs: Jan 1 2026 = Thursday, Jan 2 = Friday, Jan 3 = Saturday.
        let (pool, _) = seed_observations(&weight_and_hr_specs()).await;
        let columns = [value_column("29463-7", ColumnAggregate::Mean)];
        let mut params = day_params(&columns);
        params.bucket = TableBucket::DayOfWeek;
        let table = aligned_table(&pool, NOW, params).await.expect("query");
        let keys: Vec<_> = table.rows.iter().map(|r| r.bucket_key.clone()).collect();
        assert_eq!(
            keys,
            vec![
                Some("thu".to_string()),
                Some("fri".to_string()),
                Some("sat".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn hour_bucket_keys_by_local_hour() {
        let (pool, _) = seed_observations(&weight_and_hr_specs()).await; // 08:00 UTC each day
        let columns = [value_column("29463-7", ColumnAggregate::Count)];
        let mut params = day_params(&columns);
        params.bucket = TableBucket::Hour;
        let table = aligned_table(&pool, NOW, params).await.expect("query");
        assert_eq!(table.rows.len(), 3);
        assert_eq!(
            table.rows[0].bucket_key.as_deref(),
            Some("2026-01-01T08:00:00Z")
        );
    }

    #[tokio::test]
    async fn episode_bucket_without_spec_is_an_error() {
        let (pool, _) = seed_observations(&[]).await;
        let columns = [value_column("29463-7", ColumnAggregate::Mean)];
        let mut params = day_params(&columns);
        params.bucket = TableBucket::Episode;
        let err = aligned_table(&pool, NOW, params).await.unwrap_err();
        assert!(matches!(err, AlignedTableError::MissingEpisodeSpec));
    }

    #[tokio::test]
    async fn invalid_timezone_is_an_error() {
        let (pool, _) = seed_observations(&[]).await;
        let columns = [value_column("29463-7", ColumnAggregate::Mean)];
        let mut params = day_params(&columns);
        params.timezone = Some("Not/AZone");
        let err = aligned_table(&pool, NOW, params).await.unwrap_err();
        assert!(matches!(err, AlignedTableError::InvalidTimezone(_)));
    }
}
