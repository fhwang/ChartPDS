//! Number of observations per coding code.

use sqlx::SqlitePool;

/// A `(coding_code, count)` pair returned by [`counts_per_code`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CodeCount {
    /// The coding code (e.g. `"29463-7"` for body weight).
    pub coding_code: String,
    /// Number of observation rows with this code.
    pub count: i64,
}

/// Count observations grouped by coding code.
///
/// Returns one [`CodeCount`] per distinct `coding_code` present in the
/// observations table, ordered alphabetically by code.
///
/// Today every observation has `coding_system = "http://loinc.org"`; once
/// non-LOINC codes land (e.g. AASM sleep stages) the result shape may
/// gain a `coding_system` field on [`CodeCount`].
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn counts_per_code(pool: &SqlitePool) -> Result<Vec<CodeCount>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT coding_code AS "coding_code!: String",
               COUNT(*) AS "count!: i64"
        FROM observations
        GROUP BY coding_code
        ORDER BY coding_code
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| CodeCount {
            coding_code: r.coding_code,
            count: r.count,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queries::test_support::{seed_observations, ObsSpec};
    use time::macros::datetime;

    #[tokio::test]
    async fn empty_vec_when_no_observations() {
        let (pool, _) = seed_observations(&[]).await;
        let counts = counts_per_code(&pool).await.expect("query");
        assert!(counts.is_empty());
    }

    #[tokio::test]
    async fn returns_one_row_per_distinct_code() {
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
                effective_start: datetime!(2026-02-01 12:00:00 UTC),
                value_quantity: Some(72.0),
                value_unit: Some("kg"),
            },
            ObsSpec {
                coding_code: "8302-2",
                coding_display: Some("Body height"),
                effective_start: datetime!(2026-01-01 12:00:00 UTC),
                value_quantity: Some(175.0),
                value_unit: Some("cm"),
            },
        ])
        .await;

        let counts = counts_per_code(&pool).await.expect("query");
        assert_eq!(counts.len(), 2);

        let weight = counts
            .iter()
            .find(|c| c.coding_code == "29463-7")
            .expect("weight present");
        assert_eq!(weight.count, 2);

        let height = counts
            .iter()
            .find(|c| c.coding_code == "8302-2")
            .expect("height present");
        assert_eq!(height.count, 1);
    }

    #[tokio::test]
    async fn results_ordered_alphabetically_by_code() {
        let (pool, _) = seed_observations(&[
            ObsSpec {
                coding_code: "z-code",
                coding_display: None,
                effective_start: datetime!(2026-01-01 0:00:00 UTC),
                value_quantity: None,
                value_unit: None,
            },
            ObsSpec {
                coding_code: "a-code",
                coding_display: None,
                effective_start: datetime!(2026-01-01 0:00:00 UTC),
                value_quantity: None,
                value_unit: None,
            },
            ObsSpec {
                coding_code: "m-code",
                coding_display: None,
                effective_start: datetime!(2026-01-01 0:00:00 UTC),
                value_quantity: None,
                value_unit: None,
            },
        ])
        .await;

        let counts = counts_per_code(&pool).await.expect("query");
        assert_eq!(counts.len(), 3);
        assert_eq!(counts[0].coding_code, "a-code");
        assert_eq!(counts[1].coding_code, "m-code");
        assert_eq!(counts[2].coding_code, "z-code");
    }
}
