//! Placement rules for CRUSH algorithm.
//!
//! Rules define how replicas are distributed across the cluster topology.
//! A rule is a sequence of steps that:
//! 1. Take a starting point in the hierarchy
//! 2. Choose items at specific failure domain levels
//! 3. Emit the final device selections

use serde::{Deserialize, Serialize};

use crate::bucket::FailureDomain;

/// A step in a placement rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuleStep {
    /// Start at the root of the hierarchy or a specific bucket.
    Take {
        /// Bucket name to start from (None = root).
        bucket: Option<String>,
    },

    /// Choose N distinct items at the specified failure domain level.
    ///
    /// For example, "Choose 3 hosts" will select 3 different hosts,
    /// ensuring replicas are spread across failure domains.
    Choose {
        /// Number of items to choose.
        count: usize,
        /// The failure domain level to choose from.
        domain: FailureDomain,
    },

    /// Choose N items, allowing firstN to be in the same failure domain.
    ///
    /// This is useful when you want some replicas to be co-located
    /// for performance (e.g., first 2 in same rack) but others spread out.
    ChooseLeaf {
        /// Number of items to choose.
        count: usize,
        /// The failure domain level for selection.
        domain: FailureDomain,
    },

    /// Emit the currently selected items as the final result.
    Emit,
}

/// A complete placement rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    /// Rule name.
    pub name: String,
    /// Minimum number of replicas this rule produces.
    pub min_size: usize,
    /// Maximum number of replicas this rule produces.
    pub max_size: usize,
    /// The steps in this rule.
    pub steps: Vec<RuleStep>,
}

impl Rule {
    /// Create a new empty rule.
    #[must_use]
    pub fn new(name: impl Into<String>, min_size: usize, max_size: usize) -> Self {
        Self { name: name.into(), min_size, max_size, steps: Vec::new() }
    }

    /// Add a step to this rule.
    #[must_use]
    pub fn with_step(mut self, step: RuleStep) -> Self {
        self.steps.push(step);
        self
    }

    /// Create a default replication rule.
    ///
    /// This rule:
    /// 1. Takes the root bucket
    /// 2. Chooses N devices, each from a distinct host (failure domain isolation)
    /// 3. Emits the result
    ///
    /// This ensures replicas are spread across different hosts.
    #[must_use]
    pub fn replicated(name: impl Into<String>, replica_count: usize) -> Self {
        Self::new(name, 1, replica_count)
            .with_step(RuleStep::Take { bucket: None })
            .with_step(RuleStep::ChooseLeaf { count: replica_count, domain: FailureDomain::Host })
            .with_step(RuleStep::Emit)
    }

    /// Create a zone-aware replication rule.
    ///
    /// This rule spreads replicas across different zones for maximum durability.
    #[must_use]
    pub fn zone_aware(name: impl Into<String>, replica_count: usize) -> Self {
        Self::new(name, 1, replica_count)
            .with_step(RuleStep::Take { bucket: None })
            .with_step(RuleStep::ChooseLeaf { count: replica_count, domain: FailureDomain::Zone })
            .with_step(RuleStep::Emit)
    }

    /// Create a rack-aware replication rule.
    ///
    /// This rule spreads replicas across different racks.
    #[must_use]
    pub fn rack_aware(name: impl Into<String>, replica_count: usize) -> Self {
        Self::new(name, 1, replica_count)
            .with_step(RuleStep::Take { bucket: None })
            .with_step(RuleStep::ChooseLeaf { count: replica_count, domain: FailureDomain::Rack })
            .with_step(RuleStep::Emit)
    }

    /// Create a simple rule that just picks devices (no failure domain awareness).
    ///
    /// Only suitable for testing or single-host deployments.
    #[must_use]
    pub fn simple(name: impl Into<String>, replica_count: usize) -> Self {
        Self::new(name, 1, replica_count)
            .with_step(RuleStep::Take { bucket: None })
            .with_step(RuleStep::Choose { count: replica_count, domain: FailureDomain::Device })
            .with_step(RuleStep::Emit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replicated_rule() {
        let rule = Rule::replicated("replicated-3", 3);

        assert_eq!(rule.name, "replicated-3");
        assert_eq!(rule.min_size, 1);
        assert_eq!(rule.max_size, 3);
        assert_eq!(rule.steps.len(), 3);

        assert!(matches!(rule.steps[0], RuleStep::Take { bucket: None }));
        assert!(matches!(
            rule.steps[1],
            RuleStep::ChooseLeaf { count: 3, domain: FailureDomain::Host }
        ));
    }

    #[test]
    fn test_zone_aware_rule() {
        let rule = Rule::zone_aware("zone-aware-3", 3);

        assert!(matches!(
            rule.steps[1],
            RuleStep::ChooseLeaf { count: 3, domain: FailureDomain::Zone }
        ));
    }

    #[test]
    fn test_custom_rule() {
        let rule = Rule::new("custom", 2, 4)
            .with_step(RuleStep::Take { bucket: Some("us-east".to_string()) })
            .with_step(RuleStep::Choose { count: 2, domain: FailureDomain::Rack })
            .with_step(RuleStep::ChooseLeaf { count: 2, domain: FailureDomain::Device })
            .with_step(RuleStep::Emit);

        assert_eq!(rule.name, "custom");
        assert_eq!(rule.steps.len(), 4);
    }
}
