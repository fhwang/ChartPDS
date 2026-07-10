//! Resolve per-`(source, replay-day)` data confidence from the index.
//!
//! This is the single place that reads `source_state` / `source_day_state`
//! and dispatches to the pure per-adapter confidence functions. It has no
//! wall clock of its own — `now` is always injected so callers (and tests)
//! stay deterministic. The day key is the source's replay day
//! (`source_documents.document_date`), never the observation timestamp.

use std::collections::HashMap;

use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::index::{
    get_source_day_state, get_source_document_by_id, get_source_state, Observation,
};
use crate::sources::fitbit::confidence::fitbit_day_confidence;
use crate::sources::oura::confidence::oura_day_confidence;
use crate::sources::DayConfidence;

/// Format an `OffsetDateTime`'s calendar date as `YYYY-MM-DD`.
fn ymd(dt: OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02}",
        dt.year(),
        u8::from(dt.month()),
        dt.day()
    )
}

/// Resolve confidence for a set of `(source, replay-day)` keys.
///
/// Dispatches per source: `fitbit` uses the stability-based rule (frontier +
/// day-state), `oura` uses the time-based rule, and any other source is
/// `Confirmed` by policy (a finalized clinical document does not accrete
/// data). Keys for sources with no meaningful confidence model should simply
/// not be passed; if they are, they resolve to `Confirmed`.
///
/// # Errors
///
/// Returns `sqlx::Error` if reading `source_state` / `source_day_state` fails.
pub async fn resolve_source_day_confidence(
    pool: &SqlitePool,
    now: OffsetDateTime,
    keys: &[(String, String)],
) -> Result<HashMap<(String, String), DayConfidence>, sqlx::Error> {
    let today = ymd(now);
    let mut frontier_cache: HashMap<String, Option<String>> = HashMap::new();
    let mut out = HashMap::new();

    for (source, date) in keys {
        let confidence = match source.as_str() {
            "fitbit" => {
                if !frontier_cache.contains_key(source) {
                    let frontier = get_source_state(pool, source)
                        .await?
                        .and_then(|s| s.freshness_frontier_at);
                    frontier_cache.insert(source.clone(), frontier);
                }
                let frontier = frontier_cache
                    .get(source)
                    .and_then(std::option::Option::as_deref);
                let day_state = get_source_day_state(pool, source, date).await?;
                fitbit_day_confidence(&today, date, frontier, day_state.as_ref())
            }
            "oura" => oura_day_confidence(now, date),
            _ => DayConfidence::Confirmed,
        };
        out.insert((source.clone(), date.clone()), confidence);
    }

    Ok(out)
}

/// An observation paired with its day's confidence, serialized flat.
///
/// `#[serde(flatten)]` keeps every existing `Observation` field at the top
/// level and adds `confidence` as a sibling key, so the JSON stays additive.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ObservationWithConfidence {
    /// The underlying observation.
    #[serde(flatten)]
    pub observation: Observation,
    /// Confidence of the observation's source-day.
    pub confidence: DayConfidence,
}

/// Whether a source has a per-day confidence model (wearables). Other sources
/// (CCDA clinical documents) are `Confirmed` by policy.
fn is_wearable(source: &str) -> bool {
    source == "fitbit" || source == "oura"
}

/// Confidence for a document's `(source, document_date)`, applying policy:
/// a missing replay day or a non-wearable source is `Confirmed`.
fn confidence_for_doc(
    source: &str,
    document_date: Option<&str>,
    resolved: &HashMap<(String, String), DayConfidence>,
) -> DayConfidence {
    match document_date {
        Some(date) if is_wearable(source) => resolved
            .get(&(source.to_owned(), date.to_owned()))
            .copied()
            .unwrap_or(DayConfidence::Confirmed),
        _ => DayConfidence::Confirmed,
    }
}

/// Collect the distinct wearable `(source, replay-day)` keys from a set of
/// `(source, document_date)` pairs (skipping `None` dates and non-wearables).
fn wearable_keys<'a, I>(pairs: I) -> Vec<(String, String)>
where
    I: IntoIterator<Item = (&'a str, Option<&'a str>)>,
{
    let mut keys: Vec<(String, String)> = pairs
        .into_iter()
        .filter_map(|(source, date)| match date {
            Some(d) if is_wearable(source) => Some((source.to_owned(), d.to_owned())),
            _ => None,
        })
        .collect();
    keys.sort();
    keys.dedup();
    keys
}

/// Attach per-day confidence to a list of observations.
///
/// Looks up each observation's document `(source, document_date)`, resolves
/// the wearable source-days once, and maps every observation to its
/// confidence (CCDA / non-wearable / null replay-day → `Confirmed`).
///
/// # Errors
///
/// Returns `sqlx::Error` if any index read fails.
pub async fn annotate_observations(
    pool: &SqlitePool,
    now: OffsetDateTime,
    observations: Vec<Observation>,
) -> Result<Vec<ObservationWithConfidence>, sqlx::Error> {
    let mut doc_meta: HashMap<i64, (String, Option<String>)> = HashMap::new();
    for obs in &observations {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            doc_meta.entry(obs.source_document_id)
        {
            if let Some(doc) = get_source_document_by_id(pool, obs.source_document_id).await? {
                entry.insert((doc.source, doc.document_date));
            }
        }
    }

    let keys = wearable_keys(
        doc_meta
            .values()
            .map(|(source, date)| (source.as_str(), date.as_deref())),
    );
    let resolved = resolve_source_day_confidence(pool, now, &keys).await?;

    Ok(observations
        .into_iter()
        .map(|obs| {
            let confidence = match doc_meta.get(&obs.source_document_id) {
                Some((source, date)) => confidence_for_doc(source, date.as_deref(), &resolved),
                None => DayConfidence::Confirmed,
            };
            ObservationWithConfidence {
                observation: obs,
                confidence,
            }
        })
        .collect())
}

/// Roll up per-bucket confidence from `(bucket_day, source, document_date)`
/// contributions.
///
/// A bucket is `Provisional` if ANY contributing source-day is provisional;
/// otherwise `Confirmed`. The confidence day-key is the source's replay day
/// (`document_date`), while the bucket key is the observation's UTC calendar
/// day — for a midnight-crossing run these can differ, so the roll-up may
/// flag a bucket based on a neighboring source-day.
///
/// This roll-up is exact when the bucket key matches the day each
/// contribution is grouped by (as `duration_in_value_range` does: it buckets
/// each interval by its own day). Callers that attribute a multi-day span to
/// a single bucket (e.g. `longest_continuous_in_value_range`, which
/// attributes a run to its start day) may under-flag: a midnight-crossing
/// run spanning a confirmed pre-midnight document and a provisional
/// post-midnight document can leave the start-day bucket reading `confirmed`
/// despite containing provisional data.
///
/// # Errors
///
/// Returns `sqlx::Error` if resolving source-day confidence fails.
pub async fn roll_up_bucket_confidence(
    pool: &SqlitePool,
    now: OffsetDateTime,
    contributions: &[(String, String, Option<String>)],
) -> Result<HashMap<String, DayConfidence>, sqlx::Error> {
    let keys = wearable_keys(
        contributions
            .iter()
            .map(|(_, source, date)| (source.as_str(), date.as_deref())),
    );
    let resolved = resolve_source_day_confidence(pool, now, &keys).await?;

    let mut out: HashMap<String, DayConfidence> = HashMap::new();
    for (bucket, source, date) in contributions {
        let confidence = confidence_for_doc(source, date.as_deref(), &resolved);
        let entry = out
            .entry(bucket.clone())
            .or_insert(DayConfidence::Confirmed);
        if confidence == DayConfidence::Provisional {
            *entry = DayConfidence::Provisional;
        }
    }
    Ok(out)
}

/// Fetch each UTC-day bucket's contributing `(bucket_day, source, document_date)`
/// rows for the given selection filter.
///
/// Shared by the bucketed range queries so their aggregation filter and this
/// contribution filter cannot drift apart. Matches observations by
/// `coding_system`/`coding_code`, `effective_start` in `[start, end)`, a
/// non-null `effective_end`, and `value_quantity` within `[value_min, value_max]`,
/// grouped by UTC day + document.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub(crate) async fn contributions_for_filter(
    pool: &SqlitePool,
    coding_system: &str,
    coding_code: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
    value_min: f64,
    value_max: f64,
) -> Result<Vec<(String, String, Option<String>)>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT date(o.effective_start) AS "bucket!: String",
               sd.source AS "source!: String",
               sd.document_date AS "document_date?: String"
        FROM observations o
        JOIN source_documents sd ON o.source_document_id = sd.id
        WHERE o.coding_system = ?
          AND o.coding_code = ?
          AND o.effective_start >= ?
          AND o.effective_start < ?
          AND o.effective_end IS NOT NULL
          AND o.value_quantity >= ?
          AND o.value_quantity <= ?
        GROUP BY date(o.effective_start), sd.source, sd.document_date
        "#,
        coding_system,
        coding_code,
        start,
        end,
        value_min,
        value_max,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| (r.bucket, r.source, r.document_date))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::BlobKey;
    use crate::index::{
        insert_observation, insert_source_document, open_pool, upsert_source_day_state,
        upsert_source_state, NewObservation, NewSourceDayState, NewSourceDocument, NewSourceState,
    };
    use time::macros::datetime;

    async fn pool() -> SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    async fn set_frontier(pool: &SqlitePool, source: &str, frontier: &str) {
        upsert_source_state(
            pool,
            NewSourceState {
                source_name: source,
                last_sync_at: None,
                last_sync_status: None,
                last_error_message: None,
                last_error_reason: None,
                last_synced_window_end: None,
                freshness_frontier_at: Some(frontier),
                frontier_last_advanced_at: None,
                consecutive_sync_failures: 0,
            },
        )
        .await
        .expect("upsert source_state");
    }

    async fn set_day_state(
        pool: &SqlitePool,
        source: &str,
        date: &str,
        count: i64,
        prev: Option<i64>,
    ) {
        upsert_source_day_state(
            pool,
            NewSourceDayState {
                source_name: source,
                date,
                samples_count: count,
                samples_count_prev: prev,
                last_pulled_at: "2026-01-11T00:00:00Z",
            },
        )
        .await
        .expect("upsert source_day_state");
    }

    #[tokio::test]
    async fn fitbit_old_stable_frontier_past_is_confirmed() {
        let pool = pool().await;
        set_frontier(&pool, "fitbit", "2026-01-12T12:00:00Z").await;
        set_day_state(&pool, "fitbit", "2026-01-10", 100, Some(100)).await;
        let keys = [("fitbit".to_owned(), "2026-01-10".to_owned())];
        let out = resolve_source_day_confidence(&pool, datetime!(2026-01-20 00:00:00 UTC), &keys)
            .await
            .expect("resolve");
        assert_eq!(
            out[&("fitbit".to_owned(), "2026-01-10".to_owned())],
            DayConfidence::Confirmed
        );
    }

    #[tokio::test]
    async fn fitbit_no_frontier_is_provisional() {
        let pool = pool().await;
        set_day_state(&pool, "fitbit", "2026-01-10", 100, Some(100)).await;
        let keys = [("fitbit".to_owned(), "2026-01-10".to_owned())];
        let out = resolve_source_day_confidence(&pool, datetime!(2026-01-20 00:00:00 UTC), &keys)
            .await
            .expect("resolve");
        assert_eq!(
            out[&("fitbit".to_owned(), "2026-01-10".to_owned())],
            DayConfidence::Provisional
        );
    }

    #[tokio::test]
    async fn oura_old_day_is_confirmed_recent_is_provisional() {
        let pool = pool().await;
        let keys = [
            ("oura".to_owned(), "2026-01-10".to_owned()),
            ("oura".to_owned(), "2026-01-20".to_owned()),
        ];
        let out = resolve_source_day_confidence(&pool, datetime!(2026-01-20 12:00:00 UTC), &keys)
            .await
            .expect("resolve");
        assert_eq!(
            out[&("oura".to_owned(), "2026-01-10".to_owned())],
            DayConfidence::Confirmed
        );
        assert_eq!(
            out[&("oura".to_owned(), "2026-01-20".to_owned())],
            DayConfidence::Provisional
        );
    }

    #[tokio::test]
    async fn unknown_source_is_confirmed_by_policy() {
        let pool = pool().await;
        let keys = [("epic".to_owned(), "2026-01-10".to_owned())];
        let out = resolve_source_day_confidence(&pool, datetime!(2026-01-20 00:00:00 UTC), &keys)
            .await
            .expect("resolve");
        assert_eq!(
            out[&("epic".to_owned(), "2026-01-10".to_owned())],
            DayConfidence::Confirmed
        );
    }

    async fn seed_doc(
        pool: &SqlitePool,
        source: &str,
        document_date: Option<&str>,
        hex: &str,
    ) -> i64 {
        let key = BlobKey::from_hex_str(hex).expect("valid key");
        insert_source_document(
            pool,
            NewSourceDocument {
                archive_key: &key,
                kind: "test",
                source,
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
                document_date,
            },
        )
        .await
        .expect("insert doc")
    }

    async fn seed_obs(
        pool: &SqlitePool,
        doc_id: i64,
        start: OffsetDateTime,
    ) -> crate::index::Observation {
        let id = insert_observation(
            pool,
            NewObservation {
                source_document_id: doc_id,
                coding_system: "http://loinc.org",
                coding_code: "8867-4",
                coding_display: None,
                effective_start: start,
                effective_end: None,
                value_quantity: Some(72.0),
                value_string: None,
                value_unit: None,
            },
        )
        .await
        .expect("insert obs");
        crate::index::Observation {
            id,
            source_document_id: doc_id,
            coding_system: "http://loinc.org".to_owned(),
            coding_code: "8867-4".to_owned(),
            coding_display: None,
            effective_start: start,
            effective_end: None,
            value_quantity: Some(72.0),
            value_string: None,
            value_unit: None,
        }
    }

    #[tokio::test]
    async fn annotate_marks_fitbit_no_frontier_provisional_ccda_confirmed() {
        let pool = pool().await;
        let fitbit_doc = seed_doc(
            &pool,
            "fitbit",
            Some("2026-01-10"),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .await;
        let ccda_doc = seed_doc(
            &pool,
            "epic",
            None,
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        .await;
        let o1 = seed_obs(&pool, fitbit_doc, datetime!(2026-01-10 08:00:00 UTC)).await;
        let o2 = seed_obs(&pool, ccda_doc, datetime!(2026-01-10 09:00:00 UTC)).await;

        let annotated =
            annotate_observations(&pool, datetime!(2026-01-20 00:00:00 UTC), vec![o1, o2])
                .await
                .expect("annotate");

        assert_eq!(annotated[0].confidence, DayConfidence::Provisional);
        assert_eq!(annotated[1].confidence, DayConfidence::Confirmed);
    }

    #[tokio::test]
    async fn roll_up_bucket_is_provisional_if_any_contributor_provisional() {
        let pool = pool().await;
        // No frontier for fitbit → its day is provisional; ccda day is confirmed.
        let contributions = vec![
            ("2026-01-10".to_owned(), "epic".to_owned(), None),
            (
                "2026-01-10".to_owned(),
                "fitbit".to_owned(),
                Some("2026-01-10".to_owned()),
            ),
            ("2026-01-11".to_owned(), "epic".to_owned(), None),
        ];
        let map =
            roll_up_bucket_confidence(&pool, datetime!(2026-01-20 00:00:00 UTC), &contributions)
                .await
                .expect("roll up");
        assert_eq!(map["2026-01-10"], DayConfidence::Provisional);
        assert_eq!(map["2026-01-11"], DayConfidence::Confirmed);
    }

    #[tokio::test]
    async fn observation_history_reports_confirmed_for_stable_old_fitbit_day() {
        use crate::queries::observation_history;
        use crate::queries::CodingKey;

        let pool = pool().await;
        let doc = seed_doc(
            &pool,
            "fitbit",
            Some("2026-01-10"),
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        )
        .await;
        let _obs = seed_obs(&pool, doc, datetime!(2026-01-10 08:00:00 UTC)).await;

        // Frontier well past 2026-01-10 + 36h, and a stable two-pull day-state.
        set_frontier(&pool, "fitbit", "2026-01-12T12:00:00Z").await;
        set_day_state(&pool, "fitbit", "2026-01-10", 100, Some(100)).await;

        // "now" is 2026-01-20 → the day is outside the 5-day force-refresh window.
        let rows = observation_history(
            &pool,
            datetime!(2026-01-20 00:00:00 UTC),
            &[CodingKey {
                coding_system: "http://loinc.org",
                coding_code: "8867-4",
            }],
            None,
            None,
        )
        .await
        .expect("history");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].confidence, DayConfidence::Confirmed);
    }
}
