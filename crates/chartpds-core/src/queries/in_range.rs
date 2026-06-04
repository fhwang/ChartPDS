//! Observations for a coding code within a time window.

use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::index::Observation;

/// Fetch every observation matching `code` with `effective_start` in the
/// half-open window `[start, end)`. Results are ordered by `effective_start`
/// ascending.
///
/// Today every observation has `coding_system = "http://loinc.org"`; once
/// non-LOINC codes land (e.g. AASM sleep stages) this signature will gain
/// a `coding_system` parameter.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn in_range(
    pool: &SqlitePool,
    code: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> Result<Vec<Observation>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT id AS "id!: i64",
               source_document_id AS "source_document_id!: i64",
               coding_system, coding_code, coding_display,
               effective_start AS "effective_start: OffsetDateTime",
               effective_end AS "effective_end?: OffsetDateTime",
               value_quantity, value_string, value_unit
        FROM observations
        WHERE coding_code = ?
          AND effective_start >= ?
          AND effective_start < ?
        ORDER BY effective_start
        "#,
        code,
        start,
        end,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| Observation {
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
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queries::test_support::{seed_observations, ObsSpec};
    use time::macros::datetime;

    fn seed_three_weights() -> [ObsSpec; 3] {
        [
            ObsSpec {
                coding_code: "29463-7",
                coding_display: Some("Body Weight"),
                effective_start: datetime!(2026-01-15 12:00:00 UTC),
                value_quantity: Some(72.5),
                value_unit: Some("kg"),
            },
            ObsSpec {
                coding_code: "29463-7",
                coding_display: Some("Body Weight"),
                effective_start: datetime!(2026-02-15 12:00:00 UTC),
                value_quantity: Some(72.0),
                value_unit: Some("kg"),
            },
            ObsSpec {
                coding_code: "29463-7",
                coding_display: Some("Body Weight"),
                effective_start: datetime!(2026-03-15 12:00:00 UTC),
                value_quantity: Some(71.5),
                value_unit: Some("kg"),
            },
        ]
    }

    #[tokio::test]
    async fn returns_all_matching_rows_in_order() {
        let (pool, _) = seed_observations(&seed_three_weights()).await;

        let rows = in_range(
            &pool,
            "29463-7",
            datetime!(2026-01-01 0:00:00 UTC),
            datetime!(2026-04-01 0:00:00 UTC),
        )
        .await
        .expect("query");

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].value_quantity, Some(72.5));
        assert_eq!(rows[1].value_quantity, Some(72.0));
        assert_eq!(rows[2].value_quantity, Some(71.5));
    }

    #[tokio::test]
    async fn excludes_rows_outside_the_window() {
        let (pool, _) = seed_observations(&seed_three_weights()).await;

        let rows = in_range(
            &pool,
            "29463-7",
            datetime!(2026-02-01 0:00:00 UTC),
            datetime!(2026-03-01 0:00:00 UTC),
        )
        .await
        .expect("query");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value_quantity, Some(72.0));
    }

    #[tokio::test]
    async fn start_is_inclusive_end_is_exclusive() {
        let (pool, _) = seed_observations(&seed_three_weights()).await;

        // start == 2026-02-15T12:00:00 (the exact row's effective_start)
        // end == 2026-03-15T12:00:00 (the exact row's effective_start)
        // Inclusive start, exclusive end → should match only the 2026-02-15 row.
        let rows = in_range(
            &pool,
            "29463-7",
            datetime!(2026-02-15 12:00:00 UTC),
            datetime!(2026-03-15 12:00:00 UTC),
        )
        .await
        .expect("query");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value_quantity, Some(72.0));
    }

    #[tokio::test]
    async fn does_not_cross_codes() {
        let mut specs = seed_three_weights().to_vec();
        specs.push(ObsSpec {
            coding_code: "8302-2",
            coding_display: Some("Body height"),
            effective_start: datetime!(2026-02-01 12:00:00 UTC),
            value_quantity: Some(175.0),
            value_unit: Some("cm"),
        });
        let (pool, _) = seed_observations(&specs).await;

        let rows = in_range(
            &pool,
            "8302-2",
            datetime!(2026-01-01 0:00:00 UTC),
            datetime!(2026-04-01 0:00:00 UTC),
        )
        .await
        .expect("query");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value_quantity, Some(175.0));
    }

    #[tokio::test]
    async fn empty_vec_when_no_rows_match() {
        let (pool, _) = seed_observations(&seed_three_weights()).await;

        let rows = in_range(
            &pool,
            "29463-7",
            datetime!(2027-01-01 0:00:00 UTC),
            datetime!(2027-12-31 0:00:00 UTC),
        )
        .await
        .expect("query");

        assert!(rows.is_empty());
    }
}
