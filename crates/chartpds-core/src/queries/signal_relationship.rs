//! Two-signal relationship: pair two codings' per-bucket values and
//! quantify how they relate.
//!
//! Both signals are reduced to one value per bucket via the shared
//! aligned-table column machinery, then paired bucket-by-bucket with an
//! optional lag (`x` at bucket `t` against `y` at bucket `t + lag` — e.g.
//! activity during the day vs. sleep the following night). Pairs missing
//! either signal are excluded and `n_pairs` reflects only kept pairs.

use sqlx::SqlitePool;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::queries::aligned_table::{
    aligned_table, resolve_timezone, AlignedTableError, AlignedTableParams, ColumnSpec,
    EpisodeSpec, TableBucket,
};
use crate::queries::observation_stats::{bucket_key, percentile};
use crate::queries::StatsBucket;

/// How to bucket both signals before pairing.
///
/// Calendar buckets (including hour) pair by key arithmetic; episode
/// buckets pair by row index (see [`signal_relationship`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipBucket {
    /// Per clock hour, local to the request timezone.
    Hour,
    /// Per local calendar day.
    Day,
    /// Per ISO week (keyed by Monday).
    Week,
    /// Per calendar month.
    Month,
    /// Per detected episode of an episode coding (see [`EpisodeSpec`]).
    Episode,
}

/// Parameters for [`signal_relationship`].
pub struct SignalRelationshipParams<'a> {
    /// The first signal (the "exposure").
    pub x: ColumnSpec<'a>,
    /// The second signal (the "outcome").
    pub y: ColumnSpec<'a>,
    /// Start of the half-open window (inclusive) on `effective_start`.
    pub start: OffsetDateTime,
    /// End of the half-open window (exclusive) on `effective_start`.
    pub end: OffsetDateTime,
    /// Bucketing granularity for both signals.
    pub bucket: RelationshipBucket,
    /// IANA timezone for bucket boundaries; `None` = UTC.
    pub timezone: Option<&'a str>,
    /// Pair `x` at bucket `t` with `y` at bucket `t + lag_buckets`.
    /// `0` pairs same-bucket values; `1` pairs each `x` with the following
    /// bucket's `y`. May be negative.
    pub lag_buckets: i64,
    /// Optional threshold on `x`: also report `y` summary statistics for
    /// pairs with `x` strictly below vs. at-or-above the threshold.
    pub x_threshold: Option<f64>,
    /// Episode definition; required when `bucket` is
    /// [`RelationshipBucket::Episode`].
    pub episode: Option<EpisodeSpec<'a>>,
}

/// Summary statistics of the `y` values in one threshold group.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct GroupSummary {
    /// Number of pairs in the group.
    pub count: usize,
    /// Mean of the group's `y` values; `null` when empty.
    pub mean: Option<f64>,
    /// Sample standard deviation (n−1); `null` when `count` < 2.
    pub sd: Option<f64>,
    /// Smallest `y` value; `null` when empty.
    pub min: Option<f64>,
    /// Largest `y` value; `null` when empty.
    pub max: Option<f64>,
    /// Median (R type-7); `null` when empty.
    pub p50: Option<f64>,
}

/// Per-group `y` statistics split by the `x` threshold
/// (strictly-below convention, matching `observation_stats` thresholds).
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct RelationshipGroups {
    /// Pairs with `x` strictly below the threshold.
    pub x_below: GroupSummary,
    /// Pairs with `x` at or above the threshold.
    pub x_at_or_above: GroupSummary,
}

/// Result of [`signal_relationship`].
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct SignalRelationship {
    /// Number of buckets where both signals (after lag) had a value.
    pub n_pairs: usize,
    /// Pearson correlation over the paired values; `null` when fewer than
    /// two pairs or either signal has zero variance.
    pub pearson_r: Option<f64>,
    /// Spearman rank correlation over the paired values (Pearson over the
    /// rank-transformed pairs; ties share their average rank). Robust to
    /// outliers and to monotonic-but-nonlinear relationships. `null` when
    /// fewer than two pairs or either signal's ranks have zero variance.
    pub spearman_r: Option<f64>,
    /// Mean of the paired `x` values; `null` when there are no pairs.
    pub x_mean: Option<f64>,
    /// Sample standard deviation of the paired `x` values; `null` when
    /// fewer than two pairs.
    pub x_sd: Option<f64>,
    /// Mean of the paired `y` values; `null` when there are no pairs.
    pub y_mean: Option<f64>,
    /// Sample standard deviation of the paired `y` values; `null` when
    /// fewer than two pairs.
    pub y_sd: Option<f64>,
    /// Threshold group comparison; present only when the request set
    /// `x_threshold`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<RelationshipGroups>,
}

/// Quantify the relationship between two codings over a window.
///
/// Reduces both signals to one value per bucket (see
/// [`aligned_table`]), pairs `x` at bucket `t` with `y` at bucket
/// `t + lag_buckets`, and reports the paired-sample count, Pearson r,
/// Spearman ρ, per-signal mean/sd, and (optionally) `y` statistics
/// grouped by an `x` threshold. Buckets missing either signal are
/// excluded.
///
/// # Errors
///
/// Same failure modes as [`aligned_table`]:
/// [`AlignedTableError::Db`], [`AlignedTableError::InvalidTimezone`], or
/// [`AlignedTableError::Internal`].
pub async fn signal_relationship(
    pool: &SqlitePool,
    now: OffsetDateTime,
    params: SignalRelationshipParams<'_>,
) -> Result<SignalRelationship, AlignedTableError> {
    let table_bucket = match params.bucket {
        RelationshipBucket::Hour => TableBucket::Hour,
        RelationshipBucket::Day => TableBucket::Day,
        RelationshipBucket::Week => TableBucket::Week,
        RelationshipBucket::Month => TableBucket::Month,
        RelationshipBucket::Episode => TableBucket::Episode,
    };
    let columns = [params.x, params.y];
    let table = aligned_table(
        pool,
        now,
        AlignedTableParams {
            columns: &columns,
            start: params.start,
            end: params.end,
            bucket: table_bucket,
            episode: params.episode,
            timezone: params.timezone,
        },
    )
    .await?;

    let mut pairs: Vec<(f64, f64)> = Vec::new();
    if params.bucket == RelationshipBucket::Episode {
        for (i, row) in table.rows.iter().enumerate() {
            let Some(x) = row.values[0] else { continue };
            let Some(j) = i64::try_from(i)
                .ok()
                .and_then(|i| i.checked_add(params.lag_buckets))
                .and_then(|j| usize::try_from(j).ok())
            else {
                continue;
            };
            if let Some(Some(y)) = table.rows.get(j).map(|r| r.values[1]) {
                pairs.push((x, y));
            }
        }
    } else {
        let tz = resolve_timezone(params.timezone)?;
        let mut y_by_bucket = std::collections::BTreeMap::new();
        for row in &table.rows {
            if let Some(y) = row.values[1] {
                y_by_bucket.insert(row.bucket_key.as_deref().unwrap_or(""), y);
            }
        }
        for row in &table.rows {
            let Some(x) = row.values[0] else { continue };
            let y_key = shift_bucket_key(
                row.bucket_key.as_deref().unwrap_or(""),
                params.bucket,
                params.lag_buckets,
                &tz,
            )?;
            if let Some(&y) = y_by_bucket.get(y_key.as_str()) {
                pairs.push((x, y));
            }
        }
    }

    let xs: Vec<f64> = pairs.iter().map(|&(x, _)| x).collect();
    let ys: Vec<f64> = pairs.iter().map(|&(_, y)| y).collect();
    let (x_mean, x_sd) = mean_sd(&xs);
    let (y_mean, y_sd) = mean_sd(&ys);
    let groups = params.x_threshold.map(|threshold| {
        let split = |keep: fn(f64, f64) -> bool| {
            group_summary(
                pairs
                    .iter()
                    .filter(|&&(x, _)| keep(x, threshold))
                    .map(|&(_, y)| y)
                    .collect(),
            )
        };
        RelationshipGroups {
            x_below: split(|x, t| x < t),
            x_at_or_above: split(|x, t| x >= t),
        }
    });

    Ok(SignalRelationship {
        n_pairs: pairs.len(),
        pearson_r: pearson(&pairs),
        spearman_r: spearman(&pairs),
        x_mean,
        x_sd,
        y_mean,
        y_sd,
        groups,
    })
}

/// Mean and sample standard deviation (n−1) of a sample.
fn mean_sd(values: &[f64]) -> (Option<f64>, Option<f64>) {
    if values.is_empty() {
        return (None, None);
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "bucket counts fit f64 without loss"
    )]
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let sd = (values.len() > 1).then(|| {
        let variance = values
            .iter()
            .map(|v| {
                let d = v - mean;
                d * d
            })
            .sum::<f64>()
            / (n - 1.0);
        variance.sqrt()
    });
    (Some(mean), sd)
}

/// Summarize one threshold group's `y` values.
fn group_summary(mut values: Vec<f64>) -> GroupSummary {
    values.sort_by(f64::total_cmp);
    let (mean, sd) = mean_sd(&values);
    GroupSummary {
        count: values.len(),
        mean,
        sd,
        min: values.first().copied(),
        max: values.last().copied(),
        p50: percentile(&values, 0.5),
    }
}

/// Shift a calendar bucket key by `lag` buckets.
///
/// Hour keys are RFC 3339 top-of-hour instants (shifted by `lag` hours and
/// relabeled in `tz`); day and week keys are `YYYY-MM-DD` (weeks shift by 7
/// days, staying on Mondays); month keys are `YYYY-MM`. Never called for
/// [`RelationshipBucket::Episode`], which pairs by row index instead (see
/// [`signal_relationship`]).
fn shift_bucket_key(
    key: &str,
    bucket: RelationshipBucket,
    lag: i64,
    tz: &jiff::tz::TimeZone,
) -> Result<String, AlignedTableError> {
    let internal = |err: String| AlignedTableError::Internal(err);
    match bucket {
        RelationshipBucket::Hour => {
            let instant = OffsetDateTime::parse(key, &Rfc3339)
                .map_err(|err| internal(format!("unparseable hour bucket key {key:?}: {err}")))?;
            let shifted = instant
                .checked_add(time::Duration::hours(lag))
                .ok_or_else(|| internal("lag shifts hour bucket out of range".to_string()))?;
            let (_, label) = bucket_key(shifted, StatsBucket::Hour, tz)
                .map_err(|err| internal(err.to_string()))?;
            Ok(label)
        }
        RelationshipBucket::Episode => Err(internal(
            "episode buckets pair by row index, not key shifting".to_string(),
        )),
        RelationshipBucket::Day | RelationshipBucket::Week => {
            let date: jiff::civil::Date = key.parse().map_err(|err: jiff::Error| {
                internal(format!("unparseable bucket key {key:?}: {err}"))
            })?;
            let days = if bucket == RelationshipBucket::Week {
                lag * 7
            } else {
                lag
            };
            let shifted = date
                .checked_add(jiff::Span::new().days(days))
                .map_err(|err| internal(err.to_string()))?;
            Ok(shifted.to_string())
        }
        RelationshipBucket::Month => {
            let (year, month) = key
                .split_once('-')
                .ok_or_else(|| internal(format!("unparseable month key {key:?}")))?;
            let year: i64 = year
                .parse()
                .map_err(|_| internal(format!("unparseable month key {key:?}")))?;
            let month: i64 = month
                .parse()
                .map_err(|_| internal(format!("unparseable month key {key:?}")))?;
            let total = year * 12 + (month - 1) + lag;
            let shifted_year = total.div_euclid(12);
            let shifted_month = total.rem_euclid(12) + 1;
            Ok(format!("{shifted_year:04}-{shifted_month:02}"))
        }
    }
}

/// Pearson correlation of paired samples: `Σdx·dy / sqrt(Σdx²·Σdy²)`.
/// `None` when fewer than two pairs or either signal has zero variance.
fn pearson(pairs: &[(f64, f64)]) -> Option<f64> {
    if pairs.len() < 2 {
        return None;
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "bucket counts fit f64 without loss"
    )]
    let n = pairs.len() as f64;
    let x_mean = pairs.iter().map(|&(x, _)| x).sum::<f64>() / n;
    let y_mean = pairs.iter().map(|&(_, y)| y).sum::<f64>() / n;
    let (mut sxy, mut sxx, mut syy) = (0.0, 0.0, 0.0);
    for &(x, y) in pairs {
        let dx = x - x_mean;
        let dy = y - y_mean;
        sxy += dx * dy;
        sxx += dx * dx;
        syy += dy * dy;
    }
    if sxx == 0.0 || syy == 0.0 {
        return None;
    }
    Some(sxy / (sxx * syy).sqrt())
}

/// Spearman rank correlation: [`pearson`] over the rank-transformed
/// pairs. `None` when fewer than two pairs or either signal's ranks have
/// zero variance (every value tied).
fn spearman(pairs: &[(f64, f64)]) -> Option<f64> {
    if pairs.len() < 2 {
        return None;
    }
    let xs: Vec<f64> = pairs.iter().map(|&(x, _)| x).collect();
    let ys: Vec<f64> = pairs.iter().map(|&(_, y)| y).collect();
    let ranked: Vec<(f64, f64)> = ranks(&xs).into_iter().zip(ranks(&ys)).collect();
    pearson(&ranked)
}

/// 1-based ranks of a sample; tied values (by `f64::total_cmp` equality)
/// share the mean of the positions they occupy.
fn ranks(values: &[f64]) -> Vec<f64> {
    let mut order: Vec<usize> = (0..values.len()).collect();
    order.sort_by(|&a, &b| values[a].total_cmp(&values[b]));
    let mut out = vec![0.0; values.len()];
    let mut lo = 0;
    while lo < order.len() {
        let mut hi = lo;
        while hi + 1 < order.len()
            && values[order[hi + 1]].total_cmp(&values[order[lo]]) == std::cmp::Ordering::Equal
        {
            hi += 1;
        }
        #[allow(
            clippy::cast_precision_loss,
            reason = "bucket counts fit f64 without loss"
        )]
        let shared = (lo + hi) as f64 / 2.0 + 1.0;
        for &i in &order[lo..=hi] {
            out[i] = shared;
        }
        lo = hi + 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clinical::SYSTEM_LOINC;
    use crate::queries::aligned_table::{ColumnAggregate, EpisodeSpec};
    use crate::queries::test_support::{
        hr_interval, seed_interval_observations, seed_observations, IntervalObsSpec, ObsSpec,
    };
    use crate::queries::StatsField;
    use time::macros::datetime;

    const NOW: OffsetDateTime = datetime!(2026-07-01 00:00:00 UTC);

    fn approx(actual: Option<f64>, expected: f64) {
        let actual = actual.expect("value present");
        assert!(
            (actual - expected).abs() < 1e-12,
            "expected ~{expected}, got {actual}"
        );
    }

    // --- pure helper tests ---

    #[test]
    fn shift_day_key_by_positive_and_negative_lag() {
        let utc = jiff::tz::TimeZone::UTC;
        let shifted =
            shift_bucket_key("2026-01-31", RelationshipBucket::Day, 1, &utc).expect("shift");
        assert_eq!(shifted, "2026-02-01");
        let shifted =
            shift_bucket_key("2026-01-01", RelationshipBucket::Day, -1, &utc).expect("shift");
        assert_eq!(shifted, "2025-12-31");
    }

    #[test]
    fn shift_week_key_moves_whole_weeks() {
        // 2025-12-29 is a Monday; +1 week is the next Monday.
        let utc = jiff::tz::TimeZone::UTC;
        let shifted =
            shift_bucket_key("2025-12-29", RelationshipBucket::Week, 1, &utc).expect("shift");
        assert_eq!(shifted, "2026-01-05");
    }

    #[test]
    fn shift_month_key_crosses_year_boundary() {
        let utc = jiff::tz::TimeZone::UTC;
        let shifted =
            shift_bucket_key("2026-12", RelationshipBucket::Month, 1, &utc).expect("shift");
        assert_eq!(shifted, "2027-01");
        let shifted =
            shift_bucket_key("2026-01", RelationshipBucket::Month, -2, &utc).expect("shift");
        assert_eq!(shifted, "2025-11");
    }

    #[test]
    fn shift_hour_key_by_positive_lag_relabels_in_utc() {
        let utc = jiff::tz::TimeZone::UTC;
        let shifted = shift_bucket_key("2026-01-01T08:00:00Z", RelationshipBucket::Hour, 1, &utc)
            .expect("shift");
        assert_eq!(shifted, "2026-01-01T09:00:00Z");
    }

    #[test]
    fn pearson_matches_hand_computed_value() {
        // x = (1,2,3), y = (1,3,2): Σdx·dy = 1, Σdx² = Σdy² = 2 → r = 0.5.
        let r = pearson(&[(1.0, 1.0), (2.0, 3.0), (3.0, 2.0)]).expect("r");
        assert!((r - 0.5).abs() < 1e-12, "got {r}");
    }

    #[test]
    fn pearson_is_none_for_constant_signal_or_tiny_samples() {
        assert_eq!(pearson(&[(1.0, 5.0), (2.0, 5.0), (3.0, 5.0)]), None);
        assert_eq!(pearson(&[(1.0, 1.0)]), None);
        assert_eq!(pearson(&[]), None);
    }

    #[test]
    fn spearman_is_one_for_monotonic_but_curved_pairs() {
        // Ranks align perfectly (1,2,3,4 vs 1,2,3,4) → ρ = 1 exactly,
        // while Pearson on the raw values is visibly below 1.
        let pairs = [(1.0, 1.0), (2.0, 10.0), (3.0, 100.0), (4.0, 1000.0)];
        let rho = spearman(&pairs).expect("rho");
        assert!((rho - 1.0).abs() < 1e-12, "got {rho}");
        let r = pearson(&pairs).expect("r");
        assert!(r < 0.95, "raw Pearson should be sub-1, got {r}");
    }

    #[test]
    fn spearman_averages_tied_ranks() {
        // y = (1, 2, 2, 3): the tied 2s share rank 2.5, so the rank
        // deviations are (−1.5, 0, 0, 1.5) → ρ = 4.5/√(5·4.5) = √0.9.
        let pairs = [(1.0, 1.0), (2.0, 2.0), (3.0, 2.0), (4.0, 3.0)];
        let rho = spearman(&pairs).expect("rho");
        assert!((rho - 0.9_f64.sqrt()).abs() < 1e-12, "got {rho}");
    }

    #[test]
    fn spearman_is_none_for_constant_signal_or_tiny_samples() {
        // An all-tied signal has zero rank variance.
        assert_eq!(spearman(&[(1.0, 5.0), (2.0, 5.0), (3.0, 5.0)]), None);
        assert_eq!(spearman(&[(1.0, 1.0)]), None);
        assert_eq!(spearman(&[]), None);
    }

    // --- query tests ---

    /// Weight 380/400/420 and heart rate 60/80/70 on Jan 1/2/3.
    fn weight_hr_specs() -> Vec<ObsSpec> {
        let obs = |code, day, value| ObsSpec {
            coding_code: code,
            coding_display: None,
            effective_start: datetime!(2026-01-01 08:00:00 UTC) + time::Duration::days(day),
            value_quantity: Some(value),
            value_unit: None,
        };
        vec![
            obs("29463-7", 0, 380.0),
            obs("29463-7", 1, 400.0),
            obs("29463-7", 2, 420.0),
            obs("8867-4", 0, 60.0),
            obs("8867-4", 1, 80.0),
            obs("8867-4", 2, 70.0),
        ]
    }

    fn column(code: &str) -> ColumnSpec<'_> {
        ColumnSpec {
            coding_system: "http://loinc.org",
            coding_code: code,
            aggregate: ColumnAggregate::Mean,
            field: StatsField::Value,
        }
    }

    fn base_params<'a>() -> SignalRelationshipParams<'a> {
        SignalRelationshipParams {
            x: column("29463-7"),
            y: column("8867-4"),
            start: datetime!(2026-01-01 00:00:00 UTC),
            end: datetime!(2026-02-01 00:00:00 UTC),
            bucket: RelationshipBucket::Day,
            timezone: None,
            lag_buckets: 0,
            x_threshold: None,
            episode: None,
        }
    }

    #[tokio::test]
    async fn daily_pairs_produce_exact_pearson_r() {
        let (pool, _) = seed_observations(&weight_hr_specs()).await;
        let result = signal_relationship(&pool, NOW, base_params())
            .await
            .expect("query");
        // Deviations: x (−20, 0, 20), y (−10, 10, 0) → Σdx·dy = 200,
        // Σdx² = 800, Σdy² = 200 → r = 200 / √160000 = 0.5 exactly.
        assert_eq!(result.n_pairs, 3);
        approx(result.pearson_r, 0.5);
        // Ranks: x (1, 2, 3), y (1, 3, 2) → ρ = 1/√(2·2) = 0.5 too.
        approx(result.spearman_r, 0.5);
        approx(result.x_mean, 400.0);
        approx(result.y_mean, 70.0);
        approx(result.x_sd, 20.0);
        approx(result.y_sd, 10.0);
        assert_eq!(result.groups, None);
    }

    #[tokio::test]
    async fn lag_pairs_x_with_following_bucket_y() {
        let (pool, _) = seed_observations(&weight_hr_specs()).await;
        let mut params = base_params();
        params.lag_buckets = 1;
        let result = signal_relationship(&pool, NOW, params)
            .await
            .expect("query");
        // Pairs: (x Jan 1, y Jan 2) = (380, 80) and (x Jan 2, y Jan 3) =
        // (400, 70); Jan 3's x has no Jan 4 y → excluded. r = −1 exactly.
        assert_eq!(result.n_pairs, 2);
        approx(result.pearson_r, -1.0);
    }

    #[tokio::test]
    async fn buckets_missing_either_signal_are_excluded() {
        // Drop Jan 3's heart rate: only two complete pairs remain.
        let mut specs = weight_hr_specs();
        specs.truncate(5);
        let (pool, _) = seed_observations(&specs).await;
        let result = signal_relationship(&pool, NOW, base_params())
            .await
            .expect("query");
        assert_eq!(result.n_pairs, 2);
    }

    #[tokio::test]
    async fn threshold_groups_summarize_y_by_x_split() {
        let (pool, _) = seed_observations(&weight_hr_specs()).await;
        let mut params = base_params();
        params.x_threshold = Some(400.0);
        let result = signal_relationship(&pool, NOW, params)
            .await
            .expect("query");
        let groups = result.groups.expect("groups present");
        // x strictly below 400: only Jan 1 (380) → y = 60.
        assert_eq!(groups.x_below.count, 1);
        approx(groups.x_below.mean, 60.0);
        assert_eq!(groups.x_below.sd, None);
        // x at or above 400: Jan 2 and Jan 3 → y = 80, 70.
        assert_eq!(groups.x_at_or_above.count, 2);
        approx(groups.x_at_or_above.mean, 75.0);
        approx(groups.x_at_or_above.min, 70.0);
        approx(groups.x_at_or_above.max, 80.0);
        approx(groups.x_at_or_above.p50, 75.0);
    }

    #[tokio::test]
    async fn empty_window_reports_zero_pairs_all_null() {
        let (pool, _) = seed_observations(&[]).await;
        let result = signal_relationship(&pool, NOW, base_params())
            .await
            .expect("query");
        assert_eq!(result.n_pairs, 0);
        assert_eq!(result.pearson_r, None);
        assert_eq!(result.spearman_r, None);
        assert_eq!(result.x_mean, None);
        assert_eq!(result.y_sd, None);
    }

    #[tokio::test]
    async fn spearman_rewards_monotonic_curved_signals() {
        // Heart rate rises with weight but not linearly: the last jump
        // is much bigger. Ranks agree exactly → ρ = 1, Pearson r < 1.
        let obs = |code, day, value| ObsSpec {
            coding_code: code,
            coding_display: None,
            effective_start: datetime!(2026-01-01 08:00:00 UTC) + time::Duration::days(day),
            value_quantity: Some(value),
            value_unit: None,
        };
        let specs = vec![
            obs("29463-7", 0, 380.0),
            obs("29463-7", 1, 400.0),
            obs("29463-7", 2, 420.0),
            obs("29463-7", 3, 440.0),
            obs("8867-4", 0, 60.0),
            obs("8867-4", 1, 61.0),
            obs("8867-4", 2, 63.0),
            obs("8867-4", 3, 80.0),
        ];
        let (pool, _) = seed_observations(&specs).await;
        let result = signal_relationship(&pool, NOW, base_params())
            .await
            .expect("query");
        assert_eq!(result.n_pairs, 4);
        approx(result.spearman_r, 1.0);
        let r = result.pearson_r.expect("pearson present");
        assert!(r < 1.0, "raw Pearson should be sub-1, got {r}");
    }

    #[tokio::test]
    async fn episode_bucket_pairs_by_episode_index_with_lag() {
        // Three sleep episodes; x = total-sleep summary (93832-4), y = same.
        // With lag 1, episode i's x pairs with episode i+1's y: 2 pairs.
        let night = |d: i64, minutes: f64| IntervalObsSpec {
            coding_system: SYSTEM_LOINC,
            coding_code: "93832-4",
            effective_start: datetime!(2026-01-01 23:00:00 UTC) + time::Duration::days(d),
            effective_end: datetime!(2026-01-02 06:00:00 UTC) + time::Duration::days(d),
            value_quantity: minutes,
        };
        let (pool, _) =
            seed_interval_observations(&[night(0, 400.0), night(1, 410.0), night(2, 420.0)]).await;
        let col = ColumnSpec {
            coding_system: SYSTEM_LOINC,
            coding_code: "93832-4",
            aggregate: ColumnAggregate::Mean,
            field: StatsField::Value,
        };
        let result = signal_relationship(
            &pool,
            NOW,
            SignalRelationshipParams {
                x: col,
                y: col,
                start: datetime!(2026-01-01 00:00:00 UTC),
                end: datetime!(2026-01-10 00:00:00 UTC),
                bucket: RelationshipBucket::Episode,
                timezone: None,
                lag_buckets: 1,
                x_threshold: None,
                episode: Some(EpisodeSpec {
                    coding_system: SYSTEM_LOINC,
                    coding_code: "93832-4",
                    gap_seconds: 0,
                }),
            },
        )
        .await
        .expect("query");
        assert_eq!(result.n_pairs, 2);
    }

    #[tokio::test]
    async fn hour_bucket_lag_pairs_adjacent_hours() {
        // x at 08:xx and y at 09:xx; lag 1 pairs them.
        let (pool, _) = seed_interval_observations(&[
            hr_interval(
                datetime!(2026-01-01 08:00:00 UTC),
                datetime!(2026-01-01 08:05:00 UTC),
                100.0,
            ),
            hr_interval(
                datetime!(2026-01-01 09:00:00 UTC),
                datetime!(2026-01-01 09:05:00 UTC),
                60.0,
            ),
        ])
        .await;
        let col = ColumnSpec {
            coding_system: SYSTEM_LOINC,
            coding_code: "8867-4",
            aggregate: ColumnAggregate::Mean,
            field: StatsField::Value,
        };
        let result = signal_relationship(
            &pool,
            NOW,
            SignalRelationshipParams {
                x: col,
                y: col,
                start: datetime!(2026-01-01 00:00:00 UTC),
                end: datetime!(2026-01-02 00:00:00 UTC),
                bucket: RelationshipBucket::Hour,
                timezone: None,
                lag_buckets: 1,
                x_threshold: None,
                episode: None,
            },
        )
        .await
        .expect("query");
        assert_eq!(result.n_pairs, 1);
    }
}
