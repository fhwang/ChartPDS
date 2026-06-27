//! Per-coding discovery: which codings exist, how many, over what span.

use sqlx::SqlitePool;
use time::OffsetDateTime;

/// One discovered coding present in the observations table.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct MetricSummary {
    /// FHIR coding system URI (e.g. `"http://loinc.org"` or the AASM URI).
    pub coding_system: String,
    /// Coding code within the system (e.g. `"29463-7"`).
    pub coding_code: String,
    /// Number of observation rows for this `(system, code)`.
    pub count: i64,
    /// Earliest `effective_start` for this coding (RFC 3339 on the wire).
    #[serde(with = "time::serde::rfc3339")]
    pub first_effective_start: OffsetDateTime,
    /// Latest `effective_start` for this coding (RFC 3339 on the wire).
    #[serde(with = "time::serde::rfc3339")]
    pub last_effective_start: OffsetDateTime,
}

/// Discover the codings present in the store, grouped by `(system, code)`.
///
/// Returns one [`MetricSummary`] per distinct `(coding_system, coding_code)`,
/// ordered by system then code. `count` is the row count; `first/last_
/// effective_start` are the `MIN`/`MAX` of `effective_start` (lexical over the
/// stored RFC 3339 text — the same ordering assumption the other queries use).
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn counts_per_code(pool: &SqlitePool) -> Result<Vec<MetricSummary>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT coding_system AS "coding_system!: String",
               coding_code AS "coding_code!: String",
               COUNT(*)    AS "count!: i64",
               MIN(effective_start) AS "first_effective_start!: OffsetDateTime",
               MAX(effective_start) AS "last_effective_start!: OffsetDateTime"
        FROM observations
        GROUP BY coding_system, coding_code
        ORDER BY coding_system, coding_code
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| MetricSummary {
            coding_system: r.coding_system,
            coding_code: r.coding_code,
            count: r.count,
            first_effective_start: r.first_effective_start,
            last_effective_start: r.last_effective_start,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queries::test_support::{seed_interval_observations, IntervalObsSpec};
    use time::macros::datetime;

    #[tokio::test]
    async fn empty_vec_when_no_observations() {
        let (pool, _) = seed_interval_observations(&[]).await;
        let metrics = counts_per_code(&pool).await.expect("query");
        assert!(metrics.is_empty());
    }

    #[tokio::test]
    async fn groups_by_system_and_code_with_count_and_span() {
        let (pool, _) = seed_interval_observations(&[
            IntervalObsSpec {
                coding_system: "http://loinc.org",
                coding_code: "29463-7",
                effective_start: datetime!(2026-01-01 12:00:00 UTC),
                effective_end: datetime!(2026-01-01 12:05:00 UTC),
                value_quantity: 72.5,
            },
            IntervalObsSpec {
                coding_system: "http://loinc.org",
                coding_code: "29463-7",
                effective_start: datetime!(2026-03-01 12:00:00 UTC),
                effective_end: datetime!(2026-03-01 12:05:00 UTC),
                value_quantity: 72.0,
            },
            IntervalObsSpec {
                coding_system: "https://chartpds.fhwang.net/coding/aasm/sleep-stage",
                coding_code: "aasm-sleep-stage",
                effective_start: datetime!(2026-02-01 00:00:00 UTC),
                effective_end: datetime!(2026-02-01 00:05:00 UTC),
                value_quantity: 3.0,
            },
        ])
        .await;

        let metrics = counts_per_code(&pool).await.expect("query");
        assert_eq!(metrics.len(), 2);

        // Ordered by system then code. Lexically "http://loinc.org" sorts
        // before "https://chartpds..." (char 5 ':' (0x3A) < 's' (0x73)), so
        // the loinc weight comes first and the aasm sleep stage second.
        let weight = &metrics[0];
        assert_eq!(weight.coding_system, "http://loinc.org");
        assert_eq!(weight.coding_code, "29463-7");
        assert_eq!(weight.count, 2);
        assert_eq!(
            weight.first_effective_start,
            datetime!(2026-01-01 12:00:00 UTC)
        );
        assert_eq!(
            weight.last_effective_start,
            datetime!(2026-03-01 12:00:00 UTC)
        );

        assert_eq!(metrics[1].coding_code, "aasm-sleep-stage");
        assert_eq!(
            metrics[1].coding_system,
            "https://chartpds.fhwang.net/coding/aasm/sleep-stage"
        );
        assert_eq!(metrics[1].count, 1);
        assert_eq!(
            metrics[1].first_effective_start,
            datetime!(2026-02-01 00:00:00 UTC)
        );
        assert_eq!(
            metrics[1].last_effective_start,
            datetime!(2026-02-01 00:00:00 UTC)
        );
    }
}
