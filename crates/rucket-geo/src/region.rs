// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Region types and registry for cross-region replication.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::error::{GeoError, GeoResult};

/// Unique identifier for a region.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RegionId(pub String);

impl RegionId {
    /// Create a new region ID.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Get the region ID as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for RegionId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for RegionId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl std::fmt::Display for RegionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Endpoint configuration for a region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionEndpoint {
    /// Base URL for the region's API.
    pub url: String,
    /// Optional access key for authentication.
    pub access_key: Option<String>,
    /// Optional secret key for authentication.
    #[serde(skip_serializing)]
    pub secret_key: Option<String>,
    /// Connection timeout in milliseconds.
    pub timeout_ms: u64,
    /// Maximum retry attempts.
    pub max_retries: u32,
}

impl RegionEndpoint {
    /// Create a new region endpoint.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            access_key: None,
            secret_key: None,
            timeout_ms: 30_000,
            max_retries: 3,
        }
    }

    /// Set authentication credentials.
    pub fn with_credentials(
        mut self,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        self.access_key = Some(access_key.into());
        self.secret_key = Some(secret_key.into());
        self
    }

    /// Set connection timeout.
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Set maximum retry attempts.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }
}

/// Information about a region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Region {
    /// Unique identifier for this region.
    pub id: RegionId,
    /// Human-readable name.
    pub name: String,
    /// Endpoint configuration.
    pub endpoint: RegionEndpoint,
    /// Whether this is the local/home region.
    pub is_local: bool,
    /// Region priority for failover (lower = higher priority).
    pub priority: u32,
    /// Optional geographic location.
    pub location: Option<String>,
}

impl Region {
    /// Create a new region.
    pub fn new(id: impl Into<RegionId>, name: impl Into<String>, endpoint: RegionEndpoint) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            endpoint,
            is_local: false,
            priority: 100,
            location: None,
        }
    }

    /// Mark this as the local region.
    pub fn as_local(mut self) -> Self {
        self.is_local = true;
        self.priority = 0;
        self
    }

    /// Set the region priority.
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Set the geographic location.
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }
}

/// Registry of known regions.
#[derive(Debug)]
pub struct RegionRegistry {
    regions: DashMap<RegionId, Region>,
    local_region: Option<RegionId>,
}

impl RegionRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self { regions: DashMap::new(), local_region: None }
    }

    /// Create a registry with the local region.
    pub fn with_local(local_region: Region) -> Self {
        let mut registry = Self::new();
        registry.local_region = Some(local_region.id.clone());
        registry.regions.insert(local_region.id.clone(), local_region);
        registry
    }

    /// Register a new region.
    pub fn register(&self, region: Region) -> GeoResult<()> {
        if region.is_local && self.local_region.is_some() {
            return Err(GeoError::InvalidConfig(
                "Cannot register multiple local regions".to_string(),
            ));
        }
        self.regions.insert(region.id.clone(), region);
        Ok(())
    }

    /// Get a region by ID.
    pub fn get(&self, id: &RegionId) -> Option<Region> {
        self.regions.get(id).map(|r| r.clone())
    }

    /// Get the local region.
    pub fn local(&self) -> Option<Region> {
        self.local_region.as_ref().and_then(|id| self.get(id))
    }

    /// Get the local region ID.
    pub fn local_id(&self) -> Option<&RegionId> {
        self.local_region.as_ref()
    }

    /// List all registered regions.
    pub fn list(&self) -> Vec<Region> {
        self.regions.iter().map(|r| r.value().clone()).collect()
    }

    /// List remote regions (excluding local).
    pub fn remote_regions(&self) -> Vec<Region> {
        self.regions.iter().filter(|r| !r.value().is_local).map(|r| r.value().clone()).collect()
    }

    /// Remove a region by ID.
    pub fn remove(&self, id: &RegionId) -> Option<Region> {
        self.regions.remove(id).map(|(_, r)| r)
    }

    /// Check if a region is registered.
    pub fn contains(&self, id: &RegionId) -> bool {
        self.regions.contains_key(id)
    }

    /// Get the number of registered regions.
    pub fn len(&self) -> usize {
        self.regions.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }
}

impl Default for RegionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_id() {
        let id = RegionId::new("us-west-2");
        assert_eq!(id.as_str(), "us-west-2");
        assert_eq!(id.to_string(), "us-west-2");
    }

    #[test]
    fn test_region_endpoint() {
        let endpoint = RegionEndpoint::new("https://west.example.com")
            .with_credentials("access", "secret")
            .with_timeout(5000)
            .with_max_retries(5);

        assert_eq!(endpoint.url, "https://west.example.com");
        assert_eq!(endpoint.access_key, Some("access".to_string()));
        assert_eq!(endpoint.secret_key, Some("secret".to_string()));
        assert_eq!(endpoint.timeout_ms, 5000);
        assert_eq!(endpoint.max_retries, 5);
    }

    #[test]
    fn test_region() {
        let region =
            Region::new("us-west-2", "US West 2", RegionEndpoint::new("https://west.example.com"))
                .as_local()
                .with_location("Oregon, USA");

        assert_eq!(region.id.as_str(), "us-west-2");
        assert!(region.is_local);
        assert_eq!(region.priority, 0);
        assert_eq!(region.location, Some("Oregon, USA".to_string()));
    }

    #[test]
    fn test_region_registry() {
        let local =
            Region::new("us-east-1", "US East 1", RegionEndpoint::new("https://east.example.com"))
                .as_local();

        let registry = RegionRegistry::with_local(local);
        assert_eq!(registry.len(), 1);
        assert!(registry.local().is_some());

        let remote =
            Region::new("us-west-2", "US West 2", RegionEndpoint::new("https://west.example.com"));
        registry.register(remote).unwrap();

        assert_eq!(registry.len(), 2);
        assert_eq!(registry.remote_regions().len(), 1);
        assert!(registry.contains(&RegionId::new("us-west-2")));
    }

    #[test]
    fn test_cannot_register_multiple_local() {
        let local1 =
            Region::new("us-east-1", "US East 1", RegionEndpoint::new("https://east.example.com"))
                .as_local();

        let registry = RegionRegistry::with_local(local1);

        let local2 =
            Region::new("us-west-2", "US West 2", RegionEndpoint::new("https://west.example.com"))
                .as_local();

        let result = registry.register(local2);
        assert!(result.is_err());
    }
}
