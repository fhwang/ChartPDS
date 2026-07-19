//! Fitbit / Google Health adapter.
//!
//! Fetches heart-rate data from Google Health's v4 API and converts the
//! point-in-time samples into interval observations suitable for the index.
//!
//! Setup: create a Google Cloud project with the Health API enabled and an
//! OAuth 2.0 "Desktop app" client, set `GOOGLE_HEALTH_CLIENT_ID` and
//! `GOOGLE_HEALTH_CLIENT_SECRET` in the server's environment, then call the
//! `source_connect` MCP tool with `source="fitbit"` (opens a browser
//! authorization; the server catches the callback and stores credentials)
//! and `source_sync` to pull recent data.

pub(crate) mod api;
pub mod confidence;
pub(crate) mod parser;
pub(crate) mod storage;
pub mod sync;

use crate::archive::Archive;
use crate::sources::oauth::OAuthConfig;
use crate::sources::{Source, SyncResult};

/// Fitbit/Google Health data source.
///
/// Wraps an HTTP client and OAuth configuration. The [`Source`] impl
/// delegates to [`sync::sync_recent_days`].
pub struct FitbitSource {
    /// Shared HTTP client for API calls.
    pub http_client: reqwest::Client,
    /// Google Health OAuth configuration.
    pub oauth_config: OAuthConfig,
}

impl Source for FitbitSource {
    fn name(&self) -> &'static str {
        "fitbit"
    }

    fn display_name(&self) -> &'static str {
        "Fitbit"
    }

    async fn sync(
        &self,
        archive: &Archive,
        pool: &sqlx::SqlitePool,
        window_days: i64,
    ) -> crate::sources::Result<SyncResult> {
        sync::sync_recent_days(
            archive,
            pool,
            &self.http_client,
            &self.oauth_config,
            window_days,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time check that `FitbitSource` implements `Source`.
    const _: () = {
        fn assert_source<T: Source>() {}
        fn check() {
            assert_source::<FitbitSource>();
        }
        let _ = check;
    };
}
