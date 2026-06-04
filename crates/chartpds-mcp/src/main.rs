//! `ChartPDS` MCP binary entry point.
//!
//! Reads `CHARTPDS_DATA_DIR`, opens the index pool and archive backend,
//! builds the MCP server, and runs it on stdio. Tools are registered in
//! `server.rs`.

mod config;
mod oauth_callback;
mod server;

use std::sync::Arc;

use anyhow::{Context, Result};
use chartpds_core::archive::Archive;
use chartpds_core::sources::oauth::OAuthConfig;
use object_store::local::LocalFileSystem;
use rmcp::{transport::stdio, ServiceExt};
use tracing_subscriber::{fmt, EnvFilter};

use crate::config::Config;
use crate::server::ChartPdsServer;

#[tokio::main]
async fn main() -> Result<()> {
    // Logs go to stderr so they don't interleave with the stdio MCP frames.
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    tracing::info!("starting ChartPDS MCP server");

    let config = Config::from_env()?;

    // Create the data directory and archive subdirectory.
    std::fs::create_dir_all(config.data_dir())
        .with_context(|| format!("creating data directory {}", config.data_dir().display()))?;
    std::fs::create_dir_all(config.archive_path()).with_context(|| {
        format!(
            "creating archive directory {}",
            config.archive_path().display()
        )
    })?;

    let pool = chartpds_core::index::open_pool(&config.db_url())
        .await
        .context("opening sqlite pool")?;

    let local_fs = LocalFileSystem::new_with_prefix(config.archive_path()).with_context(|| {
        format!(
            "opening local archive at {}",
            config.archive_path().display()
        )
    })?;
    let archive = Archive::new(Arc::new(local_fs));

    let oauth_config = match (
        config.google_health_client_id,
        config.google_health_client_secret,
    ) {
        (Some(client_id), Some(client_secret)) => Some(OAuthConfig {
            client_id,
            client_secret,
            token_url: "https://oauth2.googleapis.com/token".to_owned(),
        }),
        _ => None,
    };

    let http_client = reqwest::Client::new();

    let server = ChartPdsServer::new(
        pool.clone(),
        archive.clone(),
        oauth_config.clone(),
        http_client.clone(),
    );
    let service = server
        .serve(stdio())
        .await
        .context("starting MCP service")?;

    // Build source adapters from configuration.
    let fitbit_source = oauth_config.map(|oc| chartpds_core::sources::fitbit::FitbitSource {
        http_client: http_client.clone(),
        oauth_config: oc,
    });
    let oura_source =
        config
            .oura_personal_access_token
            .map(|pat| chartpds_core::sources::oura::OuraSource {
                http_client: http_client.clone(),
                access_token: pat,
            });

    let any_source = fitbit_source.is_some() || oura_source.is_some();

    // Spawn the sync daemon if any source is configured and the interval
    // is non-zero.
    if config.sync_interval_secs > 0 && any_source {
        let daemon_deps = chartpds_core::sync::TickDeps {
            archive,
            pool,
            fitbit: fitbit_source,
            oura: oura_source,
        };
        let interval = config.sync_interval_secs;
        tokio::spawn(async move {
            chartpds_core::sync::run_daemon(daemon_deps, interval).await;
        });
        tracing::info!(
            interval_secs = config.sync_interval_secs,
            "sync daemon started"
        );
    } else if config.sync_interval_secs == 0 {
        tracing::info!("sync daemon disabled (interval set to 0)");
    } else {
        tracing::info!("sync daemon disabled (no source credentials configured)");
    }

    service
        .waiting()
        .await
        .context("MCP service exited with error")?;

    Ok(())
}
