//! Most-recent observation for a given coding.

use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::index::Observation;
use crate::queries::{annotate_observations, ObservationWithConfidence};

/// Fetch the most-recent observation matching the given coding.
///
/// "Most recent" means the row with the latest `effective_start`. Returns
/// `Ok(None)` if no observation matches `(coding_system, coding_code)`.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn latest_by_coding(
    pool: &SqlitePool,
    now: OffsetDateTime,
    coding_system: &str,
    coding_code: &str,
) -> Result<Option<ObservationWithConfidence>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT id AS "id!: i64",
               source_document_id AS "source_document_id!: i64",
               coding_system, coding_code, coding_display,
               effective_start AS "effective_start: OffsetDateTime",
               effective_end AS "effective_end?: OffsetDateTime",
               value_quantity, value_string, value_unit
        FROM observations
        WHERE coding_system = ? AND coding_code = ?
        ORDER BY effective_start DESC
        LIMIT 1
        "#,
        coding_system,
        coding_code,
    )
    .fetch_optional(pool)
    .await?;

    let Some(r) = row else {
        return Ok(None);
    };
    let observation = Observation {
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
    };
    let mut annotated = annotate_observations(pool, now, vec![observation]).await?;
    Ok(annotated.pop())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queries::test_support::{seed_observations, ObsSpec};
    use time::macros::datetime;

    const LOINC: &str = "http://loinc.org";

    #[tokio::test]
    async fn returns_none_when_no_observations_match_the_code() {
        let (pool, _) = seed_observations(&[ObsSpec {
            coding_code: "29463-7",
            coding_display: Some("Body Weight"),
            effective_start: datetime!(2026-01-01 12:00:00 UTC),
            value_quantity: Some(72.5),
            value_unit: Some("kg"),
        }])
        .await;

        let result = latest_by_coding(&pool, datetime!(2026-06-01 00:00:00 UTC), LOINC, "8302-2")
            .await
            .expect("query");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn returns_none_when_system_does_not_match() {
        let (pool, _) = seed_observations(&[ObsSpec {
            coding_code: "29463-7",
            coding_display: Some("Body Weight"),
            effective_start: datetime!(2026-01-01 12:00:00 UTC),
            value_quantity: Some(72.5),
            value_unit: Some("kg"),
        }])
        .await;

        let result = latest_by_coding(
            &pool,
            datetime!(2026-06-01 00:00:00 UTC),
            "https://chartpds.fhwang.net/coding/aasm/sleep-stage",
            "29463-7",
        )
        .await
        .expect("query");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn returns_the_only_observation_when_one_matches() {
        let (pool, _) = seed_observations(&[ObsSpec {
            coding_code: "29463-7",
            coding_display: Some("Body Weight"),
            effective_start: datetime!(2026-01-01 12:00:00 UTC),
            value_quantity: Some(72.5),
            value_unit: Some("kg"),
        }])
        .await;

        let result = latest_by_coding(&pool, datetime!(2026-06-01 00:00:00 UTC), LOINC, "29463-7")
            .await
            .expect("query");
        let obs = result.expect("row present");
        assert_eq!(obs.observation.value_quantity, Some(72.5));
        assert_eq!(
            obs.observation.effective_start,
            datetime!(2026-01-01 12:00:00 UTC)
        );
    }

    #[tokio::test]
    async fn returns_the_most_recent_when_multiple_match() {
        let (pool, _) = seed_observations(&[
            ObsSpec {
                coding_code: "29463-7",
                coding_display: Some("Body Weight"),
                effective_start: datetime!(2026-01-01 12:00:00 UTC),
                value_quantity: Some(72.5),
                value_unit: Some("kg"),
            },
            ObsSpec {
                coding_code: "29463-7",
                coding_display: Some("Body Weight"),
                effective_start: datetime!(2026-03-15 09:30:00 UTC),
                value_quantity: Some(71.0),
                value_unit: Some("kg"),
            },
            ObsSpec {
                coding_code: "29463-7",
                coding_display: Some("Body Weight"),
                effective_start: datetime!(2026-02-01 08:00:00 UTC),
                value_quantity: Some(71.8),
                value_unit: Some("kg"),
            },
        ])
        .await;

        let result = latest_by_coding(&pool, datetime!(2026-06-01 00:00:00 UTC), LOINC, "29463-7")
            .await
            .expect("query");
        let obs = result.expect("row present");
        assert_eq!(obs.observation.value_quantity, Some(71.0));
        assert_eq!(
            obs.observation.effective_start,
            datetime!(2026-03-15 09:30:00 UTC)
        );
    }

    #[tokio::test]
    async fn does_not_cross_codes() {
        let (pool, _) = seed_observations(&[
            ObsSpec {
                coding_code: "29463-7",
                coding_display: Some("Body Weight"),
                effective_start: datetime!(2026-01-01 12:00:00 UTC),
                value_quantity: Some(72.5),
                value_unit: Some("kg"),
            },
            ObsSpec {
                coding_code: "8302-2",
                coding_display: Some("Body height"),
                effective_start: datetime!(2026-03-01 12:00:00 UTC),
                value_quantity: Some(175.0),
                value_unit: Some("cm"),
            },
        ])
        .await;

        let weight = latest_by_coding(&pool, datetime!(2026-06-01 00:00:00 UTC), LOINC, "29463-7")
            .await
            .expect("query");
        assert_eq!(weight.unwrap().observation.value_quantity, Some(72.5));

        let height = latest_by_coding(&pool, datetime!(2026-06-01 00:00:00 UTC), LOINC, "8302-2")
            .await
            .expect("query");
        assert_eq!(height.unwrap().observation.value_quantity, Some(175.0));
    }
}
