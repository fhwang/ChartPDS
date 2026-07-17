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
    detect_episodes, episode_index_for, fetch_all_intervals, utc_instant_key, Episode,
};
use crate::queries::observation_stats::{
    bucket_key, fetch_rows, field_value, percentile, ObservationStatsError,
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
    /// Ignored by [`ColumnAggregate::DurationInRange`].
    pub field: StatsField,
}

/// How to bucket the table's rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableBucket {
    /// Per local calendar day, keyed `YYYY-MM-DD`.
    Day,
    /// Per ISO week, keyed by the Monday date (`YYYY-MM-DD`).
    Week,
    /// Per calendar month, keyed `YYYY-MM`.
    Month,
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
    /// or an RFC 3339 UTC instant (episode).
    pub bucket_key: String,
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

    let mut cells_per_column: Vec<BTreeMap<String, Cell>> = Vec::new();
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
    // calendar key format sorts lexicographically).
    let keys: Vec<String> = match &episodes {
        Some(eps) => eps.iter().map(|e| utc_instant_key(e.start)).collect(),
        None => cells_per_column
            .iter()
            .flat_map(|cells| cells.keys().cloned())
            .collect::<BTreeSet<String>>()
            .into_iter()
            .collect(),
    };

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
                    .get(&key)
                    .copied()
                    .unwrap_or(DayConfidence::Confirmed),
                bucket_key: key,
            })
            .collect(),
    })
}

/// Resolve an optional IANA zone name (`None` = UTC).
fn resolve_timezone(name: Option<&str>) -> Result<TimeZone, AlignedTableError> {
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
) -> Result<BTreeMap<String, Cell>, AlignedTableError> {
    let rows = fetch_rows(
        pool,
        column.coding_system,
        column.coding_code,
        params.start,
        params.end,
    )
    .await?;
    let mut cells: BTreeMap<String, Cell> = BTreeMap::new();
    for row in &rows {
        let label = match episodes {
            Some(eps) => match episode_index_for(eps, row.effective_start) {
                Some(i) => utc_instant_key(eps[i].start),
                None => continue,
            },
            None => calendar_label(row.effective_start, params.bucket, tz)?,
        };
        contributions.push((label.clone(), row.source.clone(), row.document_date.clone()));
        let cell = cells.entry(label).or_default();
        cell.rows_seen = true;
        if let ColumnAggregate::DurationInRange {
            value_min,
            value_max,
        } = column.aggregate
        {
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
        } else if let Some(value) = field_value(row, column.field, tz).map_err(map_stats_err)? {
            cell.values.push(value);
        }
    }
    Ok(cells)
}

/// Calendar bucket label of an instant (day / ISO week Monday / month).
fn calendar_label(
    dt: OffsetDateTime,
    bucket: TableBucket,
    tz: &TimeZone,
) -> Result<String, AlignedTableError> {
    let stats_bucket = match bucket {
        TableBucket::Day => StatsBucket::Day,
        TableBucket::Week => StatsBucket::Week,
        TableBucket::Month => StatsBucket::Month,
        TableBucket::Episode => {
            return Err(AlignedTableError::Internal(
                "episode buckets are not calendar buckets".to_string(),
            ))
        }
    };
    let (_, label) = bucket_key(dt, stats_bucket, tz).map_err(map_stats_err)?;
    Ok(label)
}

/// Map helper errors from the shared `observation_stats` machinery.
fn map_stats_err(err: ObservationStatsError) -> AlignedTableError {
    match err {
        ObservationStatsError::Db(err) => AlignedTableError::Db(err),
        ObservationStatsError::InvalidTimezone(name) => AlignedTableError::InvalidTimezone(name),
        ObservationStatsError::Internal(msg) => AlignedTableError::Internal(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clinical::{AASM_SLEEP_STAGE_CODE, AASM_SLEEP_STAGE_SYSTEM, SYSTEM_LOINC};
    use crate::queries::test_support::{
        seed_interval_observations, seed_observations, IntervalObsSpec, ObsSpec,
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
        assert_eq!(table.rows[0].bucket_key, "2026-01-01");
        assert_eq!(table.rows[0].values, vec![Some(380.0), Some(60.0)]);
        assert_eq!(table.rows[1].bucket_key, "2026-01-02");
        assert_eq!(table.rows[1].values, vec![Some(400.0), Some(80.0)]);
        // Jan 3 has weight but NO heart rate: explicit null, row still present.
        assert_eq!(table.rows[2].bucket_key, "2026-01-03");
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
        assert_eq!(table.rows[0].bucket_key, "2026-01-01");
        assert_eq!(table.rows[0].values, vec![Some(1.0)]);
        assert_eq!(table.rows[1].bucket_key, "2026-01-02");
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
        assert_eq!(table.rows[0].bucket_key, "2026-06-26T23:50:00Z");
        assert_eq!(table.rows[0].values, vec![Some(10.0), Some(5.0)]);
        assert_eq!(table.rows[1].bucket_key, "2026-06-27T23:00:00Z");
        assert_eq!(table.rows[1].values, vec![Some(5.0), Some(0.0)]);
    }

    #[tokio::test]
    async fn month_bucket_keys_by_year_month() {
        let (pool, _) = seed_observations(&weight_and_hr_specs()).await;
        let columns = [value_column("29463-7", ColumnAggregate::Mean)];
        let mut params = day_params(&columns);
        params.bucket = TableBucket::Month;
        let table = aligned_table(&pool, NOW, params).await.expect("query");
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows[0].bucket_key, "2026-01");
        assert_eq!(table.rows[0].values, vec![Some(400.0)]);
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
