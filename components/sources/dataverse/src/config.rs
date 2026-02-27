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

//! Dataverse Source configuration.
//!
//! Configuration for the Microsoft Dataverse change tracking source,
//! which uses the OData Web API equivalent of `RetrieveEntityChangesRequest`
//! for polling-based change detection via delta links.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Default polling interval in milliseconds.
fn default_polling_interval_ms() -> u64 {
    5000
}

/// Default API version for Dataverse Web API.
fn default_api_version() -> String {
    "v9.2".to_string()
}

/// Default minimum polling interval.
fn default_min_interval_ms() -> u64 {
    500
}

/// Default maximum polling interval in seconds.
fn default_max_interval_seconds() -> u64 {
    30
}

/// Configuration for the Dataverse replication source.
///
/// Mirrors the platform Dataverse source configuration which uses
/// `RetrieveEntityChangesRequest` for change tracking. In the Rust/Web API
/// implementation, this is achieved via OData change tracking with
/// `Prefer: odata.track-changes` headers and delta links.
///
/// # Required Configuration
///
/// - `environment_url`: The Dataverse environment URL (e.g., `https://myorg.crm.dynamics.com`)
/// - `tenant_id`: Azure AD / Microsoft Entra ID tenant ID
/// - `client_id`: Azure AD application (client) ID
/// - `client_secret`: Azure AD client secret
/// - `entities`: List of entity logical names to monitor (e.g., `["account", "contact"]`)
///
/// # Optional Configuration
///
/// - `entity_set_overrides`: Override computed entity set names for specific entities
/// - `polling_interval_ms`: Polling interval in milliseconds (default: 5000)
/// - `min_interval_ms`: Minimum adaptive polling interval (default: 500)
/// - `max_interval_seconds`: Maximum adaptive polling interval (default: 30)
/// - `api_version`: Dataverse Web API version (default: `v9.2`)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataverseSourceConfig {
    /// Dataverse environment URL (e.g., `https://myorg.crm.dynamics.com`).
    pub environment_url: String,

    /// Azure AD / Microsoft Entra ID tenant ID for OAuth2 authentication.
    pub tenant_id: String,

    /// Azure AD application (client) ID.
    pub client_id: String,

    /// Azure AD client secret for OAuth2 client credentials flow.
    pub client_secret: String,

    /// List of entity logical names to monitor (e.g., `["account", "contact"]`).
    /// These are the singular logical names matching the platform's
    /// `RetrieveEntityChangesRequest.EntityName` parameter.
    pub entities: Vec<String>,

    /// Override the entity set name (Web API plural form) for specific entities.
    /// By default, entity set names are derived by appending 's' to the logical name.
    /// Use this for entities with non-standard pluralization.
    ///
    /// Example: `{"activityparty": "activityparties"}`
    #[serde(default)]
    pub entity_set_overrides: HashMap<String, String>,

    /// Per-entity column selection. If an entity is not in this map,
    /// all columns are retrieved (equivalent to `ColumnSet(true)` in the SDK).
    #[serde(default)]
    pub entity_columns: HashMap<String, Vec<String>>,

    /// Base polling interval in milliseconds. The source uses adaptive backoff
    /// similar to the platform's SyncWorker: starts at `min_interval_ms`,
    /// increases with multiplicative backoff when idle, resets on changes.
    #[serde(default = "default_polling_interval_ms")]
    pub polling_interval_ms: u64,

    /// Minimum adaptive polling interval in milliseconds (default: 500).
    /// Matches the platform's `MinIntervalMs = 500`.
    #[serde(default = "default_min_interval_ms")]
    pub min_interval_ms: u64,

    /// Maximum adaptive polling interval in seconds (default: 30).
    /// Matches the platform's `SingleEntityMaxIntervalMs / 1000`.
    #[serde(default = "default_max_interval_seconds")]
    pub max_interval_seconds: u64,

    /// Dataverse Web API version (default: `v9.2`).
    #[serde(default = "default_api_version")]
    pub api_version: String,
}

impl DataverseSourceConfig {
    /// Validate the configuration, returning an error if required fields are missing.
    pub fn validate(&self) -> Result<(), String> {
        if self.environment_url.is_empty() {
            return Err("environment_url is required".to_string());
        }
        if self.tenant_id.is_empty() {
            return Err("tenant_id is required".to_string());
        }
        if self.client_id.is_empty() {
            return Err("client_id is required".to_string());
        }
        if self.client_secret.is_empty() {
            return Err("client_secret is required".to_string());
        }
        if self.entities.is_empty() {
            return Err("entities list must not be empty".to_string());
        }
        Ok(())
    }

    /// Get the entity set name (plural form) for a given entity logical name.
    ///
    /// First checks `entity_set_overrides`, then falls back to appending 's'.
    /// This mirrors how the platform's `RetrieveEntityChangesRequest` uses
    /// entity logical names but the Web API requires entity set names.
    pub fn entity_set_name(&self, entity: &str) -> String {
        if let Some(override_name) = self.entity_set_overrides.get(entity) {
            override_name.clone()
        } else {
            format!("{entity}s")
        }
    }

    /// Get the `$select` clause for a given entity, or `None` for all columns.
    pub fn select_columns(&self, entity: &str) -> Option<String> {
        self.entity_columns.get(entity).map(|cols| cols.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config: DataverseSourceConfig = serde_json::from_str(
            r#"{
                "environment_url": "https://myorg.crm.dynamics.com",
                "tenant_id": "tenant-1",
                "client_id": "client-1",
                "client_secret": "secret-1",
                "entities": ["account"]
            }"#,
        )
        .expect("should deserialize");

        assert_eq!(config.polling_interval_ms, 5000);
        assert_eq!(config.min_interval_ms, 500);
        assert_eq!(config.max_interval_seconds, 30);
        assert_eq!(config.api_version, "v9.2");
        assert!(config.entity_set_overrides.is_empty());
        assert!(config.entity_columns.is_empty());
    }

    #[test]
    fn test_config_validation_success() {
        let config = DataverseSourceConfig {
            environment_url: "https://myorg.crm.dynamics.com".to_string(),
            tenant_id: "tenant-1".to_string(),
            client_id: "client-1".to_string(),
            client_secret: "secret-1".to_string(),
            entities: vec!["account".to_string()],
            entity_set_overrides: HashMap::new(),
            entity_columns: HashMap::new(),
            polling_interval_ms: 5000,
            min_interval_ms: 500,
            max_interval_seconds: 30,
            api_version: "v9.2".to_string(),
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_empty_url() {
        let config = DataverseSourceConfig {
            environment_url: String::new(),
            tenant_id: "t".to_string(),
            client_id: "c".to_string(),
            client_secret: "s".to_string(),
            entities: vec!["account".to_string()],
            entity_set_overrides: HashMap::new(),
            entity_columns: HashMap::new(),
            polling_interval_ms: 5000,
            min_interval_ms: 500,
            max_interval_seconds: 30,
            api_version: "v9.2".to_string(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_empty_entities() {
        let config = DataverseSourceConfig {
            environment_url: "https://test.crm.dynamics.com".to_string(),
            tenant_id: "t".to_string(),
            client_id: "c".to_string(),
            client_secret: "s".to_string(),
            entities: vec![],
            entity_set_overrides: HashMap::new(),
            entity_columns: HashMap::new(),
            polling_interval_ms: 5000,
            min_interval_ms: 500,
            max_interval_seconds: 30,
            api_version: "v9.2".to_string(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_entity_set_name_default() {
        let config = DataverseSourceConfig {
            environment_url: "https://test.crm.dynamics.com".to_string(),
            tenant_id: "t".to_string(),
            client_id: "c".to_string(),
            client_secret: "s".to_string(),
            entities: vec!["account".to_string()],
            entity_set_overrides: HashMap::new(),
            entity_columns: HashMap::new(),
            polling_interval_ms: 5000,
            min_interval_ms: 500,
            max_interval_seconds: 30,
            api_version: "v9.2".to_string(),
        };
        assert_eq!(config.entity_set_name("account"), "accounts");
        assert_eq!(config.entity_set_name("contact"), "contacts");
    }

    #[test]
    fn test_entity_set_name_override() {
        let mut overrides = HashMap::new();
        overrides.insert("activityparty".to_string(), "activityparties".to_string());
        let config = DataverseSourceConfig {
            environment_url: "https://test.crm.dynamics.com".to_string(),
            tenant_id: "t".to_string(),
            client_id: "c".to_string(),
            client_secret: "s".to_string(),
            entities: vec!["activityparty".to_string()],
            entity_set_overrides: overrides,
            entity_columns: HashMap::new(),
            polling_interval_ms: 5000,
            min_interval_ms: 500,
            max_interval_seconds: 30,
            api_version: "v9.2".to_string(),
        };
        assert_eq!(config.entity_set_name("activityparty"), "activityparties");
    }

    #[test]
    fn test_select_columns() {
        let mut cols = HashMap::new();
        cols.insert(
            "account".to_string(),
            vec!["name".to_string(), "revenue".to_string()],
        );
        let config = DataverseSourceConfig {
            environment_url: "https://test.crm.dynamics.com".to_string(),
            tenant_id: "t".to_string(),
            client_id: "c".to_string(),
            client_secret: "s".to_string(),
            entities: vec!["account".to_string()],
            entity_set_overrides: HashMap::new(),
            entity_columns: cols,
            polling_interval_ms: 5000,
            min_interval_ms: 500,
            max_interval_seconds: 30,
            api_version: "v9.2".to_string(),
        };
        assert_eq!(
            config.select_columns("account"),
            Some("name,revenue".to_string())
        );
        assert_eq!(config.select_columns("contact"), None);
    }

    #[test]
    fn test_config_round_trip_serialization() {
        let config = DataverseSourceConfig {
            environment_url: "https://test.crm.dynamics.com".to_string(),
            tenant_id: "tenant-1".to_string(),
            client_id: "client-1".to_string(),
            client_secret: "secret-1".to_string(),
            entities: vec!["account".to_string(), "contact".to_string()],
            entity_set_overrides: HashMap::new(),
            entity_columns: HashMap::new(),
            polling_interval_ms: 3000,
            min_interval_ms: 200,
            max_interval_seconds: 60,
            api_version: "v9.1".to_string(),
        };

        let json = serde_json::to_string(&config).expect("should serialize");
        let deserialized: DataverseSourceConfig =
            serde_json::from_str(&json).expect("should deserialize");

        assert_eq!(deserialized.environment_url, config.environment_url);
        assert_eq!(deserialized.entities, config.entities);
        assert_eq!(deserialized.polling_interval_ms, 3000);
        assert_eq!(deserialized.api_version, "v9.1");
    }
}
