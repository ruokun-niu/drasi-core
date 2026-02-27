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

//! Configuration for the Dataverse bootstrap provider.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for the Dataverse bootstrap provider.
///
/// The bootstrap provider fetches initial data from Dataverse entities using
/// the OData Web API. It pages through all records and sends them as
/// `SourceChange::Insert` events.
///
/// # Example
///
/// ```
/// use drasi_bootstrap_dataverse::DataverseBootstrapConfig;
///
/// let config = DataverseBootstrapConfig {
///     environment_url: "https://myorg.crm.dynamics.com".to_string(),
///     tenant_id: "00000000-0000-0000-0000-000000000001".to_string(),
///     client_id: "00000000-0000-0000-0000-000000000002".to_string(),
///     client_secret: "my-secret".to_string(),
///     entities: vec!["account".to_string(), "contact".to_string()],
///     entity_set_overrides: Default::default(),
///     entity_columns: Default::default(),
///     api_version: "v9.2".to_string(),
///     page_size: 5000,
/// };
/// assert!(config.validate().is_ok());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataverseBootstrapConfig {
    /// Dataverse environment URL (e.g., "https://myorg.crm.dynamics.com")
    pub environment_url: String,

    /// Azure AD tenant ID for authentication
    pub tenant_id: String,

    /// Azure AD application (client) ID
    pub client_id: String,

    /// Azure AD client secret
    pub client_secret: String,

    /// Entity logical names to bootstrap (e.g., ["account", "contact"])
    pub entities: Vec<String>,

    /// Override entity set names for non-standard pluralization
    /// Key: entity logical name, Value: entity set name
    #[serde(default)]
    pub entity_set_overrides: HashMap<String, String>,

    /// Per-entity column selection for `$select`
    /// Key: entity logical name, Value: list of column names
    #[serde(default)]
    pub entity_columns: HashMap<String, Vec<String>>,

    /// Dataverse Web API version (default: "v9.2")
    #[serde(default = "default_api_version")]
    pub api_version: String,

    /// Number of records per page (default: 5000)
    #[serde(default = "default_page_size")]
    pub page_size: usize,
}

fn default_api_version() -> String {
    "v9.2".to_string()
}

fn default_page_size() -> usize {
    5000
}

impl Default for DataverseBootstrapConfig {
    fn default() -> Self {
        Self {
            environment_url: String::new(),
            tenant_id: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
            entities: Vec::new(),
            entity_set_overrides: HashMap::new(),
            entity_columns: HashMap::new(),
            api_version: default_api_version(),
            page_size: default_page_size(),
        }
    }
}

impl DataverseBootstrapConfig {
    /// Validate the configuration.
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
            return Err("at least one entity is required".to_string());
        }
        Ok(())
    }

    /// Get the entity set name for a given entity logical name.
    ///
    /// Checks overrides first, then falls back to appending 's'.
    pub fn entity_set_name(&self, entity: &str) -> String {
        if let Some(override_name) = self.entity_set_overrides.get(entity) {
            override_name.clone()
        } else {
            format!("{entity}s")
        }
    }

    /// Get the `$select` columns for a specific entity.
    pub fn select_columns(&self, entity: &str) -> Option<String> {
        self.entity_columns
            .get(entity)
            .filter(|cols| !cols.is_empty())
            .map(|cols| cols.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = DataverseBootstrapConfig::default();
        assert_eq!(config.api_version, "v9.2");
        assert_eq!(config.page_size, 5000);
        assert!(config.entities.is_empty());
    }

    #[test]
    fn test_validate_success() {
        let config = DataverseBootstrapConfig {
            environment_url: "https://test.crm.dynamics.com".to_string(),
            tenant_id: "t".to_string(),
            client_id: "c".to_string(),
            client_secret: "s".to_string(),
            entities: vec!["account".to_string()],
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_missing_url() {
        let config = DataverseBootstrapConfig {
            tenant_id: "t".to_string(),
            client_id: "c".to_string(),
            client_secret: "s".to_string(),
            entities: vec!["account".to_string()],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_empty_entities() {
        let config = DataverseBootstrapConfig {
            environment_url: "https://test.crm.dynamics.com".to_string(),
            tenant_id: "t".to_string(),
            client_id: "c".to_string(),
            client_secret: "s".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_entity_set_name() {
        let config = DataverseBootstrapConfig {
            environment_url: "https://test.crm.dynamics.com".to_string(),
            tenant_id: "t".to_string(),
            client_id: "c".to_string(),
            client_secret: "s".to_string(),
            entities: vec!["account".to_string()],
            ..Default::default()
        };
        assert_eq!(config.entity_set_name("account"), "accounts");
    }

    #[test]
    fn test_entity_set_name_with_override() {
        let mut config = DataverseBootstrapConfig {
            environment_url: "https://test.crm.dynamics.com".to_string(),
            tenant_id: "t".to_string(),
            client_id: "c".to_string(),
            client_secret: "s".to_string(),
            entities: vec!["activityparty".to_string()],
            ..Default::default()
        };
        config
            .entity_set_overrides
            .insert("activityparty".to_string(), "activityparties".to_string());
        assert_eq!(config.entity_set_name("activityparty"), "activityparties");
    }

    #[test]
    fn test_select_columns() {
        let mut config = DataverseBootstrapConfig::default();
        config.entity_columns.insert(
            "account".to_string(),
            vec!["name".to_string(), "revenue".to_string()],
        );
        assert_eq!(
            config.select_columns("account"),
            Some("name,revenue".to_string())
        );
        assert_eq!(config.select_columns("contact"), None);
    }
}
