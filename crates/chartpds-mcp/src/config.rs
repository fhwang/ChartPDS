//! Runtime configuration for the MCP server.
//!
//! Read from environment variables; failures are surfaced via the
//! anyhow-flavored error path in `main`.

use std::path::PathBuf;

use anyhow::{bail, Context, Result};

const ENV_DATA_DIR: &str = "CHARTPDS_DATA_DIR";
const ENV_GOOGLE_HEALTH_CLIENT_ID: &str = "GOOGLE_HEALTH_CLIENT_ID";
const ENV_GOOGLE_HEALTH_CLIENT_SECRET: &str = "GOOGLE_HEALTH_CLIENT_SECRET";
const ENV_OURA_PAT: &str = "OURA_PERSONAL_ACCESS_TOKEN";
const ENV_SYNC_INTERVAL_SECS: &str = "CHARTPDS_SYNC_INTERVAL_SECS";

/// Configuration assembled from environment variables.
pub(crate) struct Config {
    /// Root directory for all `ChartPDS` data (archive + `SQLite` DB).
    data_dir: PathBuf,
    /// Google Health OAuth client ID (optional; needed only for the Fitbit adapter).
    pub(crate) google_health_client_id: Option<String>,
    /// Google Health OAuth client secret (optional; needed only for the Fitbit adapter).
    pub(crate) google_health_client_secret: Option<String>,
    /// Oura personal access token (optional; needed only for the Oura adapter).
    pub(crate) oura_personal_access_token: Option<String>,
    /// Sync daemon interval in seconds (default 300 = 5 minutes). Set to 0 to disable.
    pub(crate) sync_interval_secs: u64,
}

impl Config {
    /// Read configuration from the environment.
    ///
    /// # Errors
    ///
    /// Returns an error if `CHARTPDS_DATA_DIR` is unset or is not an
    /// absolute path.
    pub(crate) fn from_env() -> Result<Self> {
        let raw = std::env::var(ENV_DATA_DIR).with_context(|| {
            format!(
                "{ENV_DATA_DIR} must be set to an absolute directory path \
                 (e.g. /path/to/chartpds-data)"
            )
        })?;
        let data_dir = PathBuf::from(&raw);
        if !data_dir.is_absolute() {
            bail!(
                "{ENV_DATA_DIR} must be an absolute path (got {raw:?}). \
                 Tip: use the full path instead of ~"
            );
        }

        let google_health_client_id = std::env::var(ENV_GOOGLE_HEALTH_CLIENT_ID).ok();
        let google_health_client_secret = std::env::var(ENV_GOOGLE_HEALTH_CLIENT_SECRET).ok();
        let oura_personal_access_token = std::env::var(ENV_OURA_PAT).ok();
        let sync_interval_secs = std::env::var(ENV_SYNC_INTERVAL_SECS)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300);

        Ok(Self {
            data_dir,
            google_health_client_id,
            google_health_client_secret,
            oura_personal_access_token,
            sync_interval_secs,
        })
    }

    /// `SQLite` database URL derived from the data directory.
    pub(crate) fn db_url(&self) -> String {
        format!(
            "sqlite://{}?mode=rwc",
            self.data_dir.join("chartpds.db").display()
        )
    }

    /// Archive directory path derived from the data directory.
    pub(crate) fn archive_path(&self) -> PathBuf {
        self.data_dir.join("archive")
    }

    /// The data directory itself (for creating it at boot).
    pub(crate) fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }
}
