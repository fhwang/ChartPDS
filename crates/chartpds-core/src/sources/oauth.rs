//! Shared OAuth 2.0 token management.

use serde::Deserialize;

use crate::sources::{Error, Result};

/// OAuth 2.0 client credentials.
#[derive(Clone)]
pub struct OAuthConfig {
    /// Application client ID.
    pub client_id: String,
    /// Application client secret.
    pub client_secret: String,
    /// Token endpoint URL (e.g. `https://api.fitbit.com/oauth2/token`).
    pub token_url: String,
}

/// A token set obtained from an OAuth 2.0 token endpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TokenSet {
    /// Bearer token for API requests.
    pub access_token: String,
    /// Long-lived token used to obtain new access tokens.
    pub refresh_token: String,
    /// RFC 3339 timestamp when `access_token` expires.
    pub expires_at: String,
}

/// Response shape from the token endpoint (subset we care about).
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: u64,
}

/// Refresh an OAuth 2.0 access token using a refresh token.
///
/// Posts to the token URL with `grant_type=refresh_token` and returns
/// a fresh `TokenSet`. The original `refresh_token` is carried forward
/// (Google's token endpoint doesn't rotate refresh tokens by default).
///
/// # Errors
///
/// - [`Error::ReauthRequired`] if the token endpoint returns 401 or
///   400 with `invalid_grant` (refresh token expired/revoked).
/// - [`Error::Transient`] for network errors, 429, or 5xx.
/// - [`Error::Parse`] if the response doesn't match the expected shape.
pub async fn refresh_token(
    client: &reqwest::Client,
    config: &OAuthConfig,
    refresh_token: &str,
) -> Result<TokenSet> {
    let params = [
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
        ("client_id", &config.client_id),
        ("client_secret", &config.client_secret),
    ];

    let response = client
        .post(&config.token_url)
        .form(&params)
        .send()
        .await
        .map_err(|err| Error::Transient {
            reason: format!("token refresh network error: {err}"),
        })?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(classify_token_error(status.as_u16(), &body));
    }

    let token_response: TokenResponse = response.json().await.map_err(|err| Error::Parse {
        reason: format!("token response parse error: {err}"),
    })?;

    let expires_at = {
        let now = time::OffsetDateTime::now_utc();
        #[allow(
            clippy::cast_possible_wrap,
            reason = "expires_in is always small (3600 typical)"
        )]
        let secs = token_response.expires_in as i64;
        let expiry = now + time::Duration::seconds(secs);
        expiry
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default()
    };

    Ok(TokenSet {
        access_token: token_response.access_token,
        refresh_token: refresh_token.to_owned(),
        expires_at,
    })
}

/// Exchange an authorization code for a token set.
///
/// Posts to the token URL with `grant_type=authorization_code` and returns
/// a fresh [`TokenSet`] containing both an access token and a refresh token.
///
/// # Errors
///
/// - [`Error::ReauthRequired`] if the token endpoint returns 401 or
///   400 with `invalid_grant` (code expired/invalid).
/// - [`Error::Transient`] for network errors, 429, or 5xx.
/// - [`Error::Parse`] if the response doesn't match the expected shape
///   or doesn't include a refresh token.
pub async fn exchange_authorization_code(
    client: &reqwest::Client,
    config: &OAuthConfig,
    code: &str,
    redirect_uri: &str,
) -> Result<TokenSet> {
    let params = [
        ("grant_type", "authorization_code"),
        ("code", code),
        ("client_id", &config.client_id),
        ("client_secret", &config.client_secret),
        ("redirect_uri", redirect_uri),
    ];

    let response = client
        .post(&config.token_url)
        .form(&params)
        .send()
        .await
        .map_err(|err| Error::Transient {
            reason: format!("token exchange network error: {err}"),
        })?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(classify_token_error(status.as_u16(), &body));
    }

    let token_response: TokenResponse = response.json().await.map_err(|err| Error::Parse {
        reason: format!("token exchange response parse error: {err}"),
    })?;

    let refresh_token = token_response.refresh_token.ok_or_else(|| Error::Parse {
        reason: "token exchange response did not include a refresh_token".to_owned(),
    })?;

    let expires_at = {
        let now = time::OffsetDateTime::now_utc();
        #[allow(
            clippy::cast_possible_wrap,
            reason = "expires_in is always small (3600 typical)"
        )]
        let secs = token_response.expires_in as i64;
        let expiry = now + time::Duration::seconds(secs);
        expiry
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default()
    };

    Ok(TokenSet {
        access_token: token_response.access_token,
        refresh_token,
        expires_at,
    })
}

/// Pure function: classify an HTTP error from the token endpoint.
pub(crate) fn classify_token_error(status: u16, body: &str) -> Error {
    if status == 401 || (status == 400 && body.contains("invalid_grant")) {
        return Error::ReauthRequired {
            reason: format!("token endpoint returned {status}: {body}"),
        };
    }
    if status == 429 || status >= 500 {
        return Error::Transient {
            reason: format!("token endpoint returned {status}: {body}"),
        };
    }
    Error::Parse {
        reason: format!("token endpoint returned {status}: {body}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_401_as_reauth_required() {
        let err = classify_token_error(401, "unauthorized");
        assert!(matches!(err, Error::ReauthRequired { .. }));
    }

    #[test]
    fn classify_400_invalid_grant_as_reauth_required() {
        let err = classify_token_error(400, r#"{"error":"invalid_grant"}"#);
        assert!(matches!(err, Error::ReauthRequired { .. }));
    }

    #[test]
    fn classify_400_other_as_parse_error() {
        let err = classify_token_error(400, r#"{"error":"invalid_request"}"#);
        assert!(matches!(err, Error::Parse { .. }));
    }

    #[test]
    fn classify_429_as_transient() {
        let err = classify_token_error(429, "too many requests");
        assert!(matches!(err, Error::Transient { .. }));
    }

    #[test]
    fn classify_500_as_transient() {
        let err = classify_token_error(500, "internal server error");
        assert!(matches!(err, Error::Transient { .. }));
    }

    #[test]
    fn classify_503_as_transient() {
        let err = classify_token_error(503, "service unavailable");
        assert!(matches!(err, Error::Transient { .. }));
    }
}
