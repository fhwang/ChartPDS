//! Sync orchestration for Fitbit heart-rate data.
//!
//! Reads stored credentials, refreshes the access token, fetches heart-rate
//! data for a window of recent days, and ingests each day into the archive
//! and index. Days already in the index (duplicate `archive_key`) are skipped.

use sqlx::SqlitePool;

use super::api;
use super::confidence::fitbit_day_confidence;
use super::storage;
use crate::archive::Archive;
use crate::index;
use crate::sources;
use crate::sources::confidence::DayConfidence;
use crate::sources::oauth::{self, OAuthConfig, TokenSet};
use crate::sources::SyncResult;

/// Read and refresh the Fitbit OAuth access token.
///
/// Reads stored credentials, refreshes the access token, persists the
/// new token set, and returns the refreshed tokens.
async fn refresh_access_token(
    pool: &SqlitePool,
    http_client: &reqwest::Client,
    oauth_config: &OAuthConfig,
) -> sources::Result<TokenSet> {
    let creds = index::get_source_credentials(pool, "fitbit")
        .await
        .map_err(sources::Error::Database)?
        .ok_or_else(|| sources::Error::NoCredentials {
            reason: "no credentials found for fitbit — run the connect flow first".to_owned(),
        })?;

    let token_set: TokenSet =
        serde_json::from_str(&creds.credentials_json).map_err(|err| sources::Error::Parse {
            reason: format!("parsing stored credentials: {err}"),
        })?;

    let refreshed =
        oauth::refresh_token(http_client, oauth_config, &token_set.refresh_token).await?;

    let refreshed_json =
        serde_json::to_string(&refreshed).map_err(|err| sources::Error::Parse {
            reason: format!("serializing refreshed tokens: {err}"),
        })?;
    let now_str = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    index::upsert_source_credentials(
        pool,
        index::NewSourceCredentials {
            source_name: "fitbit",
            credentials_json: &refreshed_json,
            updated_at: &now_str,
        },
    )
    .await
    .map_err(sources::Error::Database)?;

    Ok(refreshed)
}

/// Sync heart-rate data for the most recent `window_days` days.
///
/// 1. Reads credentials and refreshes the access token.
/// 2. For each date in the window, checks confidence and skips
///    confirmed days.
/// 3. Fetches intraday heart-rate data, archives the raw JSON, and
///    inserts observations for provisional days.
/// 4. Updates `source_state` with the sync result and freshness frontier.
///
/// # Errors
///
/// Returns [`sources::Error`] if credentials are missing, the token refresh
/// fails, or an unrecoverable API/database error occurs. Individual day
/// failures due to duplicate archive keys are silently skipped.
pub async fn sync_recent_days(
    archive: &Archive,
    pool: &SqlitePool,
    http_client: &reqwest::Client,
    oauth_config: &OAuthConfig,
    window_days: i64,
) -> sources::Result<SyncResult> {
    let refreshed = refresh_access_token(pool, http_client, oauth_config).await?;

    // 1. Compute the date window.
    let dates = days_in_window(window_days);

    // 5. Load confidence inputs.
    let today = time::OffsetDateTime::now_utc().date();
    let today_str = today
        .format(&time::macros::format_description!("[year]-[month]-[day]"))
        .unwrap_or_default();
    let source_state = index::get_source_state(pool, "fitbit").await.ok().flatten();
    let freshness_frontier = source_state
        .as_ref()
        .and_then(|s| s.freshness_frontier_at.clone());
    let previous_advanced_at = source_state
        .as_ref()
        .and_then(|s| s.frontier_last_advanced_at.clone());

    // 6. Fetch + ingest each day, skipping confirmed days.
    let mut days_synced: i64 = 0;
    let mut total_samples: i64 = 0;
    let mut max_effective_start: Option<String> = None;

    for date in &dates {
        // Check confidence before fetching.
        let day_state = index::get_source_day_state(pool, "fitbit", date)
            .await
            .ok()
            .flatten();
        let confidence = fitbit_day_confidence(
            &today_str,
            date,
            freshness_frontier.as_deref(),
            day_state.as_ref(),
        );
        if confidence == DayConfidence::Confirmed {
            continue;
        }

        let result =
            api::fetch_intraday_heart_rate(http_client, &refreshed.access_token, date).await?;

        #[allow(clippy::cast_possible_wrap, reason = "sample count is always small")]
        let sample_count = result.samples.len() as i64;

        match storage::ingest_day(archive, pool, date, &result).await {
            Ok(_) => {
                days_synced += 1;
                total_samples += sample_count;
                // Track the max effective_start for freshness frontier.
                if let Some(last_sample) = result.samples.last() {
                    let eff = &last_sample.physical_time;
                    if max_effective_start.as_ref().is_none_or(|m| eff > m) {
                        max_effective_start = Some(eff.clone());
                    }
                }
            }
            Err(sources::Error::Database(ref db_err)) if is_unique_violation(db_err) => {
                // Day already ingested — skip silently.
            }
            Err(err) => return Err(err),
        }
    }

    // 7. Update source_state.
    // Compute new freshness frontier: if we ingested anything, use the max
    // effective_start seen; otherwise preserve the existing frontier.
    let new_frontier = max_effective_start.or_else(|| freshness_frontier.clone());

    let sync_at = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    // Stamp the advance timestamp only when the frontier value actually moved.
    let frontier_last_advanced_at = sources::confidence::frontier_advanced_at(
        freshness_frontier.as_deref(),
        new_frontier.as_deref(),
        previous_advanced_at.as_deref(),
        &sync_at,
    );
    index::upsert_source_state(
        pool,
        index::NewSourceState {
            source_name: "fitbit",
            last_sync_at: Some(&sync_at),
            last_sync_status: Some("ok"),
            last_error_message: None,
            last_error_reason: None,
            last_synced_window_end: dates.first().map(String::as_str),
            freshness_frontier_at: new_frontier.as_deref(),
            frontier_last_advanced_at: frontier_last_advanced_at.as_deref(),
            consecutive_sync_failures: 0,
        },
    )
    .await
    .map_err(sources::Error::Database)?;

    Ok(SyncResult {
        days_synced,
        total_samples,
    })
}

/// Generate a list of `YYYY-MM-DD` date strings for the last `n` days
/// (today included), most recent first.
fn days_in_window(n: i64) -> Vec<String> {
    let today = time::OffsetDateTime::now_utc().date();
    let format = time::macros::format_description!("[year]-[month]-[day]");
    (0..n)
        .filter_map(|i| {
            let date = today.checked_sub(time::Duration::days(i))?;
            date.format(&format).ok()
        })
        .collect()
}

/// Check whether a `sqlx::Error` is a UNIQUE constraint violation.
fn is_unique_violation(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err.message().contains("UNIQUE constraint failed"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn days_in_window_returns_expected_count() {
        let dates = days_in_window(3);
        assert_eq!(dates.len(), 3);

        // All dates should be in YYYY-MM-DD format.
        for d in &dates {
            assert_eq!(d.len(), 10, "date {d:?} should be YYYY-MM-DD");
            assert_eq!(&d[4..5], "-");
            assert_eq!(&d[7..8], "-");
        }

        // First date should be today or very close.
        let today = time::OffsetDateTime::now_utc().date();
        let format = time::macros::format_description!("[year]-[month]-[day]");
        let today_str = today.format(&format).unwrap();
        assert_eq!(dates[0], today_str);
    }

    #[test]
    fn days_in_window_zero_returns_empty() {
        let dates = days_in_window(0);
        assert!(dates.is_empty());
    }
}
