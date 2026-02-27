// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! OAuth2 token manager for Microsoft Dataverse authentication.
//!
//! Implements the OAuth2 client credentials flow via Azure AD / Microsoft Entra ID,
//! matching the platform Dataverse source's authentication mechanism. Tokens are
//! cached and refreshed automatically before expiry (30-second buffer).

use anyhow::{Context, Result};
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// OAuth2 token response from Azure AD.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[allow(dead_code)]
    token_type: String,
    expires_in: u64,
}

/// Cached OAuth2 access token with expiry tracking.
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

/// Manages OAuth2 access tokens for Dataverse API authentication.
///
/// The token manager handles:
/// - Client credentials flow via Azure AD / Microsoft Entra ID
/// - Token caching to minimize authentication requests
/// - Automatic refresh when tokens are within 30 seconds of expiry
///
/// This matches the platform Dataverse source's authentication behavior
/// where `ServiceClient` handles token acquisition internally.
pub struct TokenManager {
    tenant_id: String,
    client_id: String,
    client_secret: String,
    scope: String,
    token_url: String,
    http_client: reqwest::Client,
    cached_token: Arc<RwLock<Option<CachedToken>>>,
}

impl TokenManager {
    /// Create a new token manager for the given Dataverse environment.
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - Azure AD tenant ID
    /// * `client_id` - Azure AD application (client) ID
    /// * `client_secret` - Client secret for the application
    /// * `environment_url` - Dataverse environment URL (e.g., `https://myorg.crm.dynamics.com`)
    pub fn new(
        tenant_id: &str,
        client_id: &str,
        client_secret: &str,
        environment_url: &str,
    ) -> Self {
        let url = url::Url::parse(environment_url).unwrap_or_else(|_| {
            url::Url::parse("https://placeholder.crm.dynamics.com")
                .expect("fallback URL should parse")
        });
        let scope = format!(
            "{}://{}/.default",
            url.scheme(),
            url.host_str().unwrap_or("")
        );
        let token_url = format!("https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",);

        Self {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            scope,
            token_url,
            http_client: reqwest::Client::new(),
            cached_token: Arc::new(RwLock::new(None)),
        }
    }

    /// Create a token manager with a custom token URL (for testing).
    pub fn with_token_url(
        tenant_id: &str,
        client_id: &str,
        client_secret: &str,
        environment_url: &str,
        token_url: &str,
    ) -> Self {
        let url = url::Url::parse(environment_url).unwrap_or_else(|_| {
            url::Url::parse("https://placeholder.crm.dynamics.com")
                .expect("fallback URL should parse")
        });
        let scope = format!(
            "{}://{}/.default",
            url.scheme(),
            url.host_str().unwrap_or("")
        );

        Self {
            tenant_id: tenant_id.to_string(),
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            scope,
            token_url: token_url.to_string(),
            http_client: reqwest::Client::new(),
            cached_token: Arc::new(RwLock::new(None)),
        }
    }

    /// Get a valid access token, refreshing if necessary.
    ///
    /// Returns a cached token if it is still valid (with a 30-second buffer),
    /// otherwise acquires a new token via the client credentials flow.
    pub async fn get_token(&self) -> Result<String> {
        // Check if we have a valid cached token
        {
            let cached = self.cached_token.read().await;
            if let Some(ref token) = *cached {
                if Instant::now() + Duration::from_secs(30) < token.expires_at {
                    return Ok(token.access_token.clone());
                }
            }
        }

        // Acquire a new token
        self.refresh_token().await
    }

    /// Acquire a new OAuth2 access token via client credentials flow.
    async fn refresh_token(&self) -> Result<String> {
        let params = [
            ("grant_type", "client_credentials"),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
            ("scope", &self.scope),
        ];

        let response = self
            .http_client
            .post(&self.token_url)
            .form(&params)
            .send()
            .await
            .context("Failed to send token request to Azure AD")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("OAuth2 token request failed with status {status}: {body}",);
        }

        let token_response: TokenResponse = response
            .json()
            .await
            .context("Failed to parse token response")?;

        let cached = CachedToken {
            access_token: token_response.access_token.clone(),
            expires_at: Instant::now() + Duration::from_secs(token_response.expires_in),
        };

        // Cache the token
        let mut cache = self.cached_token.write().await;
        *cache = Some(cached);

        Ok(token_response.access_token)
    }

    /// Get the tenant ID.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_manager_creation() {
        let tm = TokenManager::new(
            "tenant-123",
            "client-456",
            "secret-789",
            "https://myorg.crm.dynamics.com",
        );
        assert_eq!(tm.tenant_id(), "tenant-123");
        assert_eq!(tm.scope, "https://myorg.crm.dynamics.com/.default");
        assert!(tm.token_url.contains("tenant-123"));
    }

    #[test]
    fn test_token_manager_with_custom_url() {
        let tm = TokenManager::with_token_url(
            "t",
            "c",
            "s",
            "https://myorg.crm.dynamics.com",
            "http://localhost:8080/token",
        );
        assert_eq!(tm.token_url, "http://localhost:8080/token");
    }

    #[tokio::test]
    async fn test_token_cache_initially_empty() {
        let tm = TokenManager::new("t", "c", "s", "https://test.crm.dynamics.com");
        let cached = tm.cached_token.read().await;
        assert!(cached.is_none());
    }
}
