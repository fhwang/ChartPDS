//! Temporary local HTTP listener for the OAuth 2.0 loopback redirect.
//!
//! Listens on `127.0.0.1:8765` for Google's OAuth callback, exchanges the
//! authorization code for tokens, stores them in `source_credentials`, and
//! serves a "Success!" page to the browser.

use chartpds_core::sources::oauth::{exchange_authorization_code, OAuthConfig};
use sqlx::SqlitePool;
use time::format_description::well_known::Rfc3339;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// The redirect URI registered in the Google Cloud Console.
pub(crate) const REDIRECT_URI: &str = "http://127.0.0.1:8765/callback";

const SUCCESS_HTML: &str = "\
    HTTP/1.1 200 OK\r\n\
    Content-Type: text/html; charset=utf-8\r\n\
    Connection: close\r\n\
    \r\n\
    <html><body>\
    <h2>Authorization successful!</h2>\
    <p>You can close this tab and return to your MCP client.</p>\
    </body></html>";

const ERROR_HTML_PREFIX: &str = "\
    HTTP/1.1 400 Bad Request\r\n\
    Content-Type: text/html; charset=utf-8\r\n\
    Connection: close\r\n\
    \r\n\
    <html><body>\
    <h2>Authorization failed</h2>\
    <p>";
const ERROR_HTML_SUFFIX: &str = "</p></body></html>";

/// Start a background listener that waits for ONE OAuth callback, then shuts
/// down. The listener runs in a spawned tokio task and does not block the
/// caller.
pub(crate) fn spawn_callback_listener(
    pool: SqlitePool,
    http_client: reqwest::Client,
    oauth_config: OAuthConfig,
) {
    tokio::spawn(async move {
        if let Err(err) = run_callback_listener(pool, http_client, oauth_config).await {
            tracing::warn!(%err, "OAuth callback listener failed");
        }
    });
}

async fn run_callback_listener(
    pool: SqlitePool,
    http_client: reqwest::Client,
    oauth_config: OAuthConfig,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8765").await?;
    tracing::info!("OAuth callback listener started on 127.0.0.1:8765");

    // Accept exactly one connection, handle it, then shut down.
    let (mut stream, _addr) = listener.accept().await?;

    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf).await?;
    let request = String::from_utf8_lossy(&buf[..n]);

    // Extract the code from the query string.
    let code = extract_code_from_request(&request);

    if let Some(code) = code {
        match exchange_and_store(&pool, &http_client, &oauth_config, &code).await {
            Ok(()) => {
                stream.write_all(SUCCESS_HTML.as_bytes()).await?;
                tracing::info!("Google Health OAuth authorization completed successfully");
            }
            Err(err) => {
                let body = format!("{ERROR_HTML_PREFIX}{err}{ERROR_HTML_SUFFIX}");
                stream.write_all(body.as_bytes()).await?;
                tracing::warn!(%err, "OAuth token exchange failed");
            }
        }
    } else {
        let body = format!(
            "{ERROR_HTML_PREFIX}No authorization code found in the callback.{ERROR_HTML_SUFFIX}"
        );
        stream.write_all(body.as_bytes()).await?;
    }

    stream.shutdown().await?;
    Ok(())
}

fn extract_code_from_request(request: &str) -> Option<String> {
    // The first line looks like: GET /callback?code=AUTH_CODE&scope=... HTTP/1.1
    let first_line = request.lines().next()?;
    let path = first_line.split_whitespace().nth(1)?;
    let query = path.split('?').nth(1)?;
    for param in query.split('&') {
        if let Some(value) = param.strip_prefix("code=") {
            return Some(value.to_owned());
        }
    }
    None
}

async fn exchange_and_store(
    pool: &SqlitePool,
    http_client: &reqwest::Client,
    oauth_config: &OAuthConfig,
    code: &str,
) -> anyhow::Result<()> {
    let token_set =
        exchange_authorization_code(http_client, oauth_config, code, REDIRECT_URI).await?;

    let credentials_json = serde_json::to_string(&token_set)?;
    let now_str = time::OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default();

    chartpds_core::index::upsert_source_credentials(
        pool,
        chartpds_core::index::UpsertSourceCredentialsParams {
            source_name: "fitbit",
            credentials_json: &credentials_json,
            updated_at: &now_str,
        },
    )
    .await?;

    Ok(())
}
