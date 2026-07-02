//! Multi-coding observation history with optional open-ended bounds.

use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::index::Observation;
use crate::queries::{annotate_observations, ObservationWithConfidence};

/// A `(system, code)` selector for [`observation_history`].
#[derive(Debug, Clone, Copy)]
pub struct CodingKey<'a> {
    /// FHIR coding system URI.
    pub coding_system: &'a str,
    /// Coding code within the system.
    pub coding_code: &'a str,
}

/// Fetch observations for any of `codings` whose `effective_start` falls within
/// the optional half-open bounds `[since, until)`. Either bound may be `None`
/// (open-ended); both `None` reads full history. Matching is by
/// `(coding_system, coding_code)`. Results are ordered by
/// `(coding_system, coding_code, effective_start)`.
///
/// An empty `codings` slice returns an empty vec without touching the database.
/// Each observation carries its source-day `confidence`.
///
/// # Errors
///
/// Returns `sqlx::Error` if any underlying query fails.
pub async fn observation_history(
    pool: &SqlitePool,
    now: OffsetDateTime,
    codings: &[CodingKey<'_>],
    since: Option<OffsetDateTime>,
    until: Option<OffsetDateTime>,
) -> Result<Vec<ObservationWithConfidence>, sqlx::Error> {
    let mut out = Vec::new();

    for coding in codings {
        let system = coding.coding_system;
        let code = coding.coding_code;
        let rows = sqlx::query!(
            r#"
            SELECT id AS "id!: i64",
                   source_document_id AS "source_document_id!: i64",
                   coding_system, coding_code, coding_display,
                   effective_start AS "effective_start: OffsetDateTime",
                   effective_end AS "effective_end?: OffsetDateTime",
                   value_quantity, value_string, value_unit
            FROM observations
            WHERE coding_system = ?
              AND coding_code = ?
              AND (? IS NULL OR effective_start >= ?)
              AND (? IS NULL OR effective_start <  ?)
            ORDER BY effective_start
            "#,
            system,
            code,
            since,
            since,
            until,
            until,
        )
        .fetch_all(pool)
        .await?;

        out.extend(rows.into_iter().map(|r| Observation {
            id: r.id,
            source_document_id: r.source_document_id,
            coding_system: r.coding_system,
            coding_code: r.coding_code,
            coding_display: r.coding_display,
            effective_start: r.effective_start,
            effective_end: r.effective_end,
            value_quantity: r.value_quantity,
            value_string: r.value_string,
            value_unit: r.value_unit,
        }));
    }

    out.sort_by(|a, b| {
        a.coding_system
            .cmp(&b.coding_system)
            .then_with(|| a.coding_code.cmp(&b.coding_code))
            .then_with(|| a.effective_start.cmp(&b.effective_start))
    });

    annotate_observations(pool, now, out).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queries::test_support::{seed_interval_observations, IntervalObsSpec};
    use time::macros::datetime;

    const LOINC: &str = "http://loinc.org";
    const AASM: &str = "https://chartpds.fhwang.net/coding/aasm/sleep-stage";

    async fn seed() -> SqlitePool {
        let (pool, _) = seed_interval_observations(&[
            IntervalObsSpec {
                coding_system: LOINC,
                coding_code: "8867-4",
                effective_start: datetime!(2026-01-01 00:00:00 UTC),
                effective_end: datetime!(2026-01-01 00:01:00 UTC),
                value_quantity: 60.0,
            },
            IntervalObsSpec {
                coding_system: LOINC,
                coding_code: "8867-4",
                effective_start: datetime!(2026-02-01 00:00:00 UTC),
                effective_end: datetime!(2026-02-01 00:01:00 UTC),
                value_quantity: 65.0,
            },
            IntervalObsSpec {
                coding_system: AASM,
                coding_code: "aasm-sleep-stage",
                effective_start: datetime!(2026-01-15 00:00:00 UTC),
                effective_end: datetime!(2026-01-15 00:05:00 UTC),
                value_quantity: 3.0,
            },
        ])
        .await;
        pool
    }

    #[tokio::test]
    async fn empty_codings_returns_empty() {
        let pool = seed().await;
        let rows = observation_history(&pool, datetime!(2026-06-01 00:00:00 UTC), &[], None, None)
            .await
            .expect("query");
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn multi_coding_full_history_ordered_by_system_code_time() {
        let pool = seed().await;
        let rows = observation_history(
            &pool,
            datetime!(2026-06-01 00:00:00 UTC),
            &[
                CodingKey {
                    coding_system: LOINC,
                    coding_code: "8867-4",
                },
                CodingKey {
                    coding_system: AASM,
                    coding_code: "aasm-sleep-stage",
                },
            ],
            None,
            None,
        )
        .await
        .expect("query");

        assert_eq!(rows.len(), 3);
        // http://loinc.org sorts before https://chartpds...
        assert_eq!(rows[0].observation.coding_system, LOINC);
        assert_eq!(
            rows[0].observation.effective_start,
            datetime!(2026-01-01 00:00:00 UTC)
        );
        assert_eq!(rows[1].observation.coding_system, LOINC);
        assert_eq!(
            rows[1].observation.effective_start,
            datetime!(2026-02-01 00:00:00 UTC)
        );
        assert_eq!(rows[2].observation.coding_system, AASM);
    }

    #[tokio::test]
    async fn since_only_is_open_ended_upper() {
        let pool = seed().await;
        let rows = observation_history(
            &pool,
            datetime!(2026-06-01 00:00:00 UTC),
            &[CodingKey {
                coding_system: LOINC,
                coding_code: "8867-4",
            }],
            Some(datetime!(2026-01-15 00:00:00 UTC)),
            None,
        )
        .await
        .expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].observation.effective_start,
            datetime!(2026-02-01 00:00:00 UTC)
        );
    }

    #[tokio::test]
    async fn until_only_is_open_ended_lower_and_exclusive() {
        let pool = seed().await;
        let rows = observation_history(
            &pool,
            datetime!(2026-06-01 00:00:00 UTC),
            &[CodingKey {
                coding_system: LOINC,
                coding_code: "8867-4",
            }],
            None,
            Some(datetime!(2026-02-01 00:00:00 UTC)),
        )
        .await
        .expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].observation.effective_start,
            datetime!(2026-01-01 00:00:00 UTC)
        );
    }
}
