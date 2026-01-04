//! S3-compatible bucket policy types and evaluation.
//!
//! This module provides types for parsing, validating, and evaluating
//! IAM-style bucket policies that control access to S3 resources.

use std::collections::HashMap;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};

use crate::error::{Error, S3ErrorCode};

/// The result of policy evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyDecision {
    /// An explicit Allow from a policy statement.
    Allow,
    /// An explicit Deny from a policy statement.
    Deny,
    /// No matching statement found (implicit deny).
    DefaultDeny,
}

/// A bucket policy document following AWS IAM policy format.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BucketPolicy {
    /// The policy language version (should be "2012-10-17").
    pub version: String,
    /// An optional identifier for the policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The policy statements.
    pub statement: Vec<Statement>,
}

/// A single policy statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Statement {
    /// An optional identifier for the statement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sid: Option<String>,
    /// Whether this statement allows or denies access.
    pub effect: Effect,
    /// The principal(s) this statement applies to.
    pub principal: Principal,
    /// The action(s) this statement covers.
    pub action: ActionSpec,
    /// The resource(s) this statement covers.
    pub resource: ResourceSpec,
    /// Optional conditions that must be met.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<Conditions>,
}

/// The effect of a policy statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    /// Allow the action.
    Allow,
    /// Deny the action.
    Deny,
}

/// The principal(s) a policy applies to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Principal {
    /// Wildcard - applies to everyone.
    #[serde(rename = "*")]
    Wildcard(WildcardPrincipal),
    /// Specific principals.
    Specific(PrincipalSpec),
}

/// Represents a wildcard principal "*".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WildcardPrincipal;

impl Serialize for WildcardPrincipal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str("*")
    }
}

impl<'de> Deserialize<'de> for WildcardPrincipal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "*" {
            Ok(WildcardPrincipal)
        } else {
            Err(serde::de::Error::custom("expected \"*\""))
        }
    }
}

/// Specific principal specification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PrincipalSpec {
    /// AWS account ARNs or account IDs.
    #[serde(default, rename = "AWS", skip_serializing_if = "Option::is_none")]
    pub aws: Option<StringOrArray>,
}

/// Either a single string or an array of strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringOrArray {
    /// A single string.
    Single(String),
    /// An array of strings.
    Array(Vec<String>),
}

impl StringOrArray {
    /// Returns the values as a vector of string slices.
    pub fn values(&self) -> Vec<&str> {
        match self {
            StringOrArray::Single(s) => vec![s.as_str()],
            StringOrArray::Array(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }

    /// Returns an iterator over the values.
    pub fn iter(&self) -> impl Iterator<Item = &str> + '_ {
        self.values().into_iter()
    }

    /// Returns true if the given value matches any of the patterns.
    pub fn matches(&self, value: &str) -> bool {
        match self {
            StringOrArray::Single(s) => wildcard_match(s, value),
            StringOrArray::Array(v) => v.iter().any(|s| wildcard_match(s, value)),
        }
    }
}

/// Action specification - single action or list of actions.
pub type ActionSpec = StringOrArray;

/// Resource specification - single resource or list of resources.
pub type ResourceSpec = StringOrArray;

/// Condition block containing condition operators.
pub type Conditions = HashMap<String, ConditionBlock>;

/// A condition block mapping condition keys to values.
pub type ConditionBlock = HashMap<String, ConditionValues>;

/// Condition values - single value or list of values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConditionValues {
    /// A single value.
    Single(ConditionValue),
    /// Multiple values (any must match).
    Array(Vec<ConditionValue>),
}

impl ConditionValues {
    /// Returns the values as an iterator.
    pub fn iter(&self) -> impl Iterator<Item = &ConditionValue> {
        match self {
            ConditionValues::Single(v) => std::slice::from_ref(v).iter(),
            ConditionValues::Array(v) => v.iter(),
        }
    }
}

/// A single condition value (string or boolean).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConditionValue {
    /// String value.
    String(String),
    /// Boolean value.
    Bool(bool),
}

impl ConditionValue {
    /// Returns the value as a string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ConditionValue::String(s) => Some(s),
            ConditionValue::Bool(_) => None,
        }
    }

    /// Returns the value as a boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ConditionValue::Bool(b) => Some(*b),
            ConditionValue::String(s) => match s.to_lowercase().as_str() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            },
        }
    }
}

/// S3 actions that can be controlled by policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum S3Action {
    // Object operations
    /// s3:GetObject
    GetObject,
    /// s3:GetObjectVersion
    GetObjectVersion,
    /// s3:PutObject
    PutObject,
    /// s3:DeleteObject
    DeleteObject,
    /// s3:DeleteObjectVersion
    DeleteObjectVersion,
    /// s3:GetObjectAcl
    GetObjectAcl,
    /// s3:PutObjectAcl
    PutObjectAcl,
    /// s3:GetObjectTagging
    GetObjectTagging,
    /// s3:PutObjectTagging
    PutObjectTagging,
    /// s3:DeleteObjectTagging
    DeleteObjectTagging,
    /// s3:GetObjectRetention
    GetObjectRetention,
    /// s3:PutObjectRetention
    PutObjectRetention,
    /// s3:GetObjectLegalHold
    GetObjectLegalHold,
    /// s3:PutObjectLegalHold
    PutObjectLegalHold,
    /// s3:BypassGovernanceRetention
    BypassGovernanceRetention,

    // Bucket operations
    /// s3:ListBucket
    ListBucket,
    /// s3:ListBucketVersions
    ListBucketVersions,
    /// s3:ListBucketMultipartUploads
    ListBucketMultipartUploads,
    /// s3:CreateBucket
    CreateBucket,
    /// s3:DeleteBucket
    DeleteBucket,
    /// s3:GetBucketLocation
    GetBucketLocation,
    /// s3:GetBucketVersioning
    GetBucketVersioning,
    /// s3:PutBucketVersioning
    PutBucketVersioning,
    /// s3:GetBucketPolicy
    GetBucketPolicy,
    /// s3:PutBucketPolicy
    PutBucketPolicy,
    /// s3:DeleteBucketPolicy
    DeleteBucketPolicy,
    /// s3:GetBucketCors
    GetBucketCors,
    /// s3:PutBucketCors
    PutBucketCors,
    /// s3:DeleteBucketCors
    DeleteBucketCors,
    /// s3:GetBucketTagging
    GetBucketTagging,
    /// s3:PutBucketTagging
    PutBucketTagging,
    /// s3:DeleteBucketTagging
    DeleteBucketTagging,
    /// s3:GetBucketObjectLockConfiguration
    GetBucketObjectLockConfiguration,
    /// s3:PutBucketObjectLockConfiguration
    PutBucketObjectLockConfiguration,
    /// s3:GetEncryptionConfiguration
    GetEncryptionConfiguration,
    /// s3:PutEncryptionConfiguration
    PutEncryptionConfiguration,
    /// s3:DeleteEncryptionConfiguration
    DeleteEncryptionConfiguration,

    // Multipart operations
    /// s3:AbortMultipartUpload
    AbortMultipartUpload,
    /// s3:ListMultipartUploadParts
    ListMultipartUploadParts,
}

impl S3Action {
    /// Returns the action string (e.g., "s3:GetObject").
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::GetObject => "s3:GetObject",
            Self::GetObjectVersion => "s3:GetObjectVersion",
            Self::PutObject => "s3:PutObject",
            Self::DeleteObject => "s3:DeleteObject",
            Self::DeleteObjectVersion => "s3:DeleteObjectVersion",
            Self::GetObjectAcl => "s3:GetObjectAcl",
            Self::PutObjectAcl => "s3:PutObjectAcl",
            Self::GetObjectTagging => "s3:GetObjectTagging",
            Self::PutObjectTagging => "s3:PutObjectTagging",
            Self::DeleteObjectTagging => "s3:DeleteObjectTagging",
            Self::GetObjectRetention => "s3:GetObjectRetention",
            Self::PutObjectRetention => "s3:PutObjectRetention",
            Self::GetObjectLegalHold => "s3:GetObjectLegalHold",
            Self::PutObjectLegalHold => "s3:PutObjectLegalHold",
            Self::BypassGovernanceRetention => "s3:BypassGovernanceRetention",
            Self::ListBucket => "s3:ListBucket",
            Self::ListBucketVersions => "s3:ListBucketVersions",
            Self::ListBucketMultipartUploads => "s3:ListBucketMultipartUploads",
            Self::CreateBucket => "s3:CreateBucket",
            Self::DeleteBucket => "s3:DeleteBucket",
            Self::GetBucketLocation => "s3:GetBucketLocation",
            Self::GetBucketVersioning => "s3:GetBucketVersioning",
            Self::PutBucketVersioning => "s3:PutBucketVersioning",
            Self::GetBucketPolicy => "s3:GetBucketPolicy",
            Self::PutBucketPolicy => "s3:PutBucketPolicy",
            Self::DeleteBucketPolicy => "s3:DeleteBucketPolicy",
            Self::GetBucketCors => "s3:GetBucketCors",
            Self::PutBucketCors => "s3:PutBucketCors",
            Self::DeleteBucketCors => "s3:DeleteBucketCors",
            Self::GetBucketTagging => "s3:GetBucketTagging",
            Self::PutBucketTagging => "s3:PutBucketTagging",
            Self::DeleteBucketTagging => "s3:DeleteBucketTagging",
            Self::GetBucketObjectLockConfiguration => "s3:GetBucketObjectLockConfiguration",
            Self::PutBucketObjectLockConfiguration => "s3:PutBucketObjectLockConfiguration",
            Self::GetEncryptionConfiguration => "s3:GetEncryptionConfiguration",
            Self::PutEncryptionConfiguration => "s3:PutEncryptionConfiguration",
            Self::DeleteEncryptionConfiguration => "s3:DeleteEncryptionConfiguration",
            Self::AbortMultipartUpload => "s3:AbortMultipartUpload",
            Self::ListMultipartUploadParts => "s3:ListMultipartUploadParts",
        }
    }

    /// Parse an action from string representation.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "s3:GetObject" => Some(Self::GetObject),
            "s3:GetObjectVersion" => Some(Self::GetObjectVersion),
            "s3:PutObject" => Some(Self::PutObject),
            "s3:DeleteObject" => Some(Self::DeleteObject),
            "s3:DeleteObjectVersion" => Some(Self::DeleteObjectVersion),
            "s3:GetObjectAcl" => Some(Self::GetObjectAcl),
            "s3:PutObjectAcl" => Some(Self::PutObjectAcl),
            "s3:GetObjectTagging" => Some(Self::GetObjectTagging),
            "s3:PutObjectTagging" => Some(Self::PutObjectTagging),
            "s3:DeleteObjectTagging" => Some(Self::DeleteObjectTagging),
            "s3:GetObjectRetention" => Some(Self::GetObjectRetention),
            "s3:PutObjectRetention" => Some(Self::PutObjectRetention),
            "s3:GetObjectLegalHold" => Some(Self::GetObjectLegalHold),
            "s3:PutObjectLegalHold" => Some(Self::PutObjectLegalHold),
            "s3:BypassGovernanceRetention" => Some(Self::BypassGovernanceRetention),
            "s3:ListBucket" => Some(Self::ListBucket),
            "s3:ListBucketVersions" => Some(Self::ListBucketVersions),
            "s3:ListBucketMultipartUploads" => Some(Self::ListBucketMultipartUploads),
            "s3:CreateBucket" => Some(Self::CreateBucket),
            "s3:DeleteBucket" => Some(Self::DeleteBucket),
            "s3:GetBucketLocation" => Some(Self::GetBucketLocation),
            "s3:GetBucketVersioning" => Some(Self::GetBucketVersioning),
            "s3:PutBucketVersioning" => Some(Self::PutBucketVersioning),
            "s3:GetBucketPolicy" => Some(Self::GetBucketPolicy),
            "s3:PutBucketPolicy" => Some(Self::PutBucketPolicy),
            "s3:DeleteBucketPolicy" => Some(Self::DeleteBucketPolicy),
            "s3:GetBucketCors" => Some(Self::GetBucketCors),
            "s3:PutBucketCors" => Some(Self::PutBucketCors),
            "s3:DeleteBucketCors" => Some(Self::DeleteBucketCors),
            "s3:GetBucketTagging" => Some(Self::GetBucketTagging),
            "s3:PutBucketTagging" => Some(Self::PutBucketTagging),
            "s3:DeleteBucketTagging" => Some(Self::DeleteBucketTagging),
            "s3:GetBucketObjectLockConfiguration" => Some(Self::GetBucketObjectLockConfiguration),
            "s3:PutBucketObjectLockConfiguration" => Some(Self::PutBucketObjectLockConfiguration),
            "s3:GetEncryptionConfiguration" => Some(Self::GetEncryptionConfiguration),
            "s3:PutEncryptionConfiguration" => Some(Self::PutEncryptionConfiguration),
            "s3:DeleteEncryptionConfiguration" => Some(Self::DeleteEncryptionConfiguration),
            "s3:AbortMultipartUpload" => Some(Self::AbortMultipartUpload),
            "s3:ListMultipartUploadParts" => Some(Self::ListMultipartUploadParts),
            _ => None,
        }
    }
}

impl std::fmt::Display for S3Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Context for policy evaluation.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// The principal making the request (ARN or "*" for anonymous).
    pub principal: String,
    /// Whether the request is from an anonymous user.
    pub is_anonymous: bool,
    /// The S3 action being performed.
    pub action: S3Action,
    /// The resource ARN (e.g., "arn:aws:s3:::bucket/key").
    pub resource: String,
    /// The bucket name.
    pub bucket: String,
    /// The object key (if applicable).
    pub key: Option<String>,
    /// Source IP address of the request.
    pub source_ip: Option<IpAddr>,
    /// Whether the request was made over HTTPS.
    pub secure_transport: bool,
    /// Additional context values for condition evaluation.
    pub context_values: HashMap<String, String>,
}

impl RequestContext {
    /// Creates a new request context.
    #[must_use]
    pub fn new(
        principal: String,
        is_anonymous: bool,
        action: S3Action,
        bucket: &str,
        key: Option<&str>,
    ) -> Self {
        let resource = if let Some(k) = key {
            format!("arn:aws:s3:::{bucket}/{k}")
        } else {
            format!("arn:aws:s3:::{bucket}")
        };

        Self {
            principal,
            is_anonymous,
            action,
            resource,
            bucket: bucket.to_string(),
            key: key.map(String::from),
            source_ip: None,
            secure_transport: true,
            context_values: HashMap::new(),
        }
    }

    /// Sets the source IP address.
    #[must_use]
    pub fn with_source_ip(mut self, ip: IpAddr) -> Self {
        self.source_ip = Some(ip);
        self
    }

    /// Sets whether the request uses secure transport.
    #[must_use]
    pub fn with_secure_transport(mut self, secure: bool) -> Self {
        self.secure_transport = secure;
        self
    }

    /// Adds a context value for condition evaluation.
    #[must_use]
    pub fn with_context_value(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context_values.insert(key.into(), value.into());
        self
    }
}

impl BucketPolicy {
    /// Parses a bucket policy from JSON.
    ///
    /// # Errors
    /// Returns an error if the JSON is invalid or doesn't match the policy schema.
    pub fn from_json(json: &str) -> Result<Self, Error> {
        serde_json::from_str(json).map_err(|e| {
            Error::s3(S3ErrorCode::MalformedPolicy, format!("Invalid policy JSON: {e}"))
        })
    }

    /// Serializes the policy to JSON.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    pub fn to_json(&self) -> Result<String, Error> {
        serde_json::to_string(self).map_err(|e| {
            Error::s3(S3ErrorCode::InternalError, format!("Failed to serialize policy: {e}"))
        })
    }

    /// Serializes the policy to pretty-printed JSON.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    pub fn to_json_pretty(&self) -> Result<String, Error> {
        serde_json::to_string_pretty(self).map_err(|e| {
            Error::s3(S3ErrorCode::InternalError, format!("Failed to serialize policy: {e}"))
        })
    }

    /// Validates the policy structure and semantics.
    ///
    /// # Errors
    /// Returns an error if the policy is invalid.
    pub fn validate(&self) -> Result<(), Error> {
        // Check version
        if self.version != "2012-10-17" && self.version != "2008-10-17" {
            return Err(Error::s3(
                S3ErrorCode::MalformedPolicy,
                format!("Invalid policy version: {}. Must be \"2012-10-17\"", self.version),
            ));
        }

        // Must have at least one statement
        if self.statement.is_empty() {
            return Err(Error::s3(
                S3ErrorCode::MalformedPolicy,
                "Policy must contain at least one statement",
            ));
        }

        // Validate each statement
        for (i, stmt) in self.statement.iter().enumerate() {
            stmt.validate().map_err(|e| {
                Error::s3(S3ErrorCode::MalformedPolicy, format!("Statement {i}: {e}"))
            })?;
        }

        Ok(())
    }

    /// Evaluates the policy for a given request context.
    ///
    /// The evaluation follows the standard IAM policy evaluation logic:
    /// 1. If any statement explicitly denies, return Deny
    /// 2. If any statement explicitly allows, return Allow
    /// 3. Otherwise, return DefaultDeny (implicit deny)
    #[must_use]
    pub fn evaluate(&self, ctx: &RequestContext) -> PolicyDecision {
        let mut has_allow = false;

        for stmt in &self.statement {
            match stmt.evaluate(ctx) {
                PolicyDecision::Deny => return PolicyDecision::Deny,
                PolicyDecision::Allow => has_allow = true,
                PolicyDecision::DefaultDeny => {}
            }
        }

        if has_allow {
            PolicyDecision::Allow
        } else {
            PolicyDecision::DefaultDeny
        }
    }
}

impl Statement {
    /// Validates the statement.
    fn validate(&self) -> Result<(), String> {
        // Validate resources are S3 ARNs
        for resource in self.resource.iter() {
            if !resource.starts_with("arn:aws:s3:::") && resource != "*" {
                return Err(format!("Invalid S3 resource ARN: {resource}"));
            }
        }

        // Validate actions start with s3:
        for action in self.action.iter() {
            if !action.starts_with("s3:") && action != "*" {
                return Err(format!("Invalid S3 action: {action}"));
            }
        }

        Ok(())
    }

    /// Evaluates this statement for the given request context.
    fn evaluate(&self, ctx: &RequestContext) -> PolicyDecision {
        // Check if principal matches
        if !self.matches_principal(ctx) {
            return PolicyDecision::DefaultDeny;
        }

        // Check if action matches
        if !self.matches_action(ctx.action) {
            return PolicyDecision::DefaultDeny;
        }

        // Check if resource matches
        if !self.matches_resource(&ctx.resource) {
            return PolicyDecision::DefaultDeny;
        }

        // Check conditions if present
        if let Some(ref conditions) = self.condition {
            if !self.evaluate_conditions(conditions, ctx) {
                return PolicyDecision::DefaultDeny;
            }
        }

        // Statement matches - return effect
        match self.effect {
            Effect::Allow => PolicyDecision::Allow,
            Effect::Deny => PolicyDecision::Deny,
        }
    }

    /// Checks if the principal matches the request context.
    fn matches_principal(&self, ctx: &RequestContext) -> bool {
        match &self.principal {
            Principal::Wildcard(_) => true,
            Principal::Specific(spec) => {
                if let Some(ref aws) = spec.aws {
                    // Check if any of the AWS principals match
                    aws.iter().any(|p| {
                        p == "*" || p == ctx.principal || {
                            // Handle account ID matching (e.g., "123456789012")
                            // and ARN matching (e.g., "arn:aws:iam::123456789012:root")
                            wildcard_match(p, &ctx.principal)
                        }
                    })
                } else {
                    false
                }
            }
        }
    }

    /// Checks if the action matches.
    fn matches_action(&self, action: S3Action) -> bool {
        let action_str = action.as_str();
        self.action.iter().any(|pattern| {
            pattern == "*" || pattern == "s3:*" || wildcard_match(pattern, action_str)
        })
    }

    /// Checks if the resource matches.
    fn matches_resource(&self, resource: &str) -> bool {
        self.resource.iter().any(|pattern| pattern == "*" || wildcard_match(pattern, resource))
    }

    /// Evaluates all conditions in the condition block.
    fn evaluate_conditions(&self, conditions: &Conditions, ctx: &RequestContext) -> bool {
        // All condition operators must match (AND)
        for (operator, block) in conditions {
            if !self.evaluate_condition_operator(operator, block, ctx) {
                return false;
            }
        }
        true
    }

    /// Evaluates a single condition operator block.
    fn evaluate_condition_operator(
        &self,
        operator: &str,
        block: &ConditionBlock,
        ctx: &RequestContext,
    ) -> bool {
        // All keys within an operator must match (AND)
        for (key, values) in block {
            let ctx_value = get_condition_value(key, ctx);
            if !evaluate_condition(operator, ctx_value.as_deref(), values) {
                return false;
            }
        }
        true
    }
}

/// Gets a condition value from the request context.
fn get_condition_value(key: &str, ctx: &RequestContext) -> Option<String> {
    match key {
        "aws:SourceIp" => ctx.source_ip.map(|ip| ip.to_string()),
        "aws:SecureTransport" => Some(ctx.secure_transport.to_string()),
        "s3:prefix" => ctx.key.clone(),
        "s3:x-amz-acl" => ctx.context_values.get("x-amz-acl").cloned(),
        _ => ctx.context_values.get(key).cloned(),
    }
}

/// Evaluates a condition with the given operator.
fn evaluate_condition(
    operator: &str,
    ctx_value: Option<&str>,
    condition_values: &ConditionValues,
) -> bool {
    match operator {
        // String conditions
        "StringEquals" => {
            if let Some(v) = ctx_value {
                condition_values.iter().any(|cv| cv.as_str() == Some(v))
            } else {
                false
            }
        }
        "StringNotEquals" => {
            if let Some(v) = ctx_value {
                !condition_values.iter().any(|cv| cv.as_str() == Some(v))
            } else {
                true
            }
        }
        "StringEqualsIgnoreCase" => {
            if let Some(v) = ctx_value {
                condition_values
                    .iter()
                    .any(|cv| cv.as_str().map(|s| s.eq_ignore_ascii_case(v)).unwrap_or(false))
            } else {
                false
            }
        }
        "StringNotEqualsIgnoreCase" => {
            if let Some(v) = ctx_value {
                !condition_values
                    .iter()
                    .any(|cv| cv.as_str().map(|s| s.eq_ignore_ascii_case(v)).unwrap_or(false))
            } else {
                true
            }
        }
        "StringLike" => {
            if let Some(v) = ctx_value {
                condition_values.iter().any(|cv| {
                    cv.as_str().map(|pattern| wildcard_match(pattern, v)).unwrap_or(false)
                })
            } else {
                false
            }
        }
        "StringNotLike" => {
            if let Some(v) = ctx_value {
                !condition_values.iter().any(|cv| {
                    cv.as_str().map(|pattern| wildcard_match(pattern, v)).unwrap_or(false)
                })
            } else {
                true
            }
        }

        // Boolean condition
        "Bool" => {
            if let Some(v) = ctx_value {
                let ctx_bool = v.eq_ignore_ascii_case("true");
                condition_values.iter().any(|cv| cv.as_bool() == Some(ctx_bool))
            } else {
                false
            }
        }

        // IP address conditions
        "IpAddress" => {
            if let Some(v) = ctx_value {
                condition_values
                    .iter()
                    .any(|cv| cv.as_str().map(|cidr| ip_matches_cidr(v, cidr)).unwrap_or(false))
            } else {
                false
            }
        }
        "NotIpAddress" => {
            if let Some(v) = ctx_value {
                !condition_values
                    .iter()
                    .any(|cv| cv.as_str().map(|cidr| ip_matches_cidr(v, cidr)).unwrap_or(false))
            } else {
                true
            }
        }

        // Null condition
        "Null" => condition_values.iter().any(|cv| {
            let expect_null = cv.as_bool().unwrap_or(false);
            (ctx_value.is_none()) == expect_null
        }),

        // Unknown operator - fail safe (deny)
        _ => false,
    }
}

/// Matches a string against a wildcard pattern (supports * and ?).
fn wildcard_match(pattern: &str, text: &str) -> bool {
    let mut pattern_chars = pattern.chars().peekable();
    let mut text_chars = text.chars().peekable();

    let mut pattern_stack: Vec<(
        std::iter::Peekable<std::str::Chars<'_>>,
        std::iter::Peekable<std::str::Chars<'_>>,
    )> = Vec::new();

    loop {
        match (pattern_chars.peek(), text_chars.peek()) {
            (Some('*'), _) => {
                pattern_chars.next();
                // Skip consecutive *s
                while pattern_chars.peek() == Some(&'*') {
                    pattern_chars.next();
                }
                if pattern_chars.peek().is_none() {
                    // Trailing * matches everything
                    return true;
                }
                // Save state for backtracking
                pattern_stack.push((pattern_chars.clone(), text_chars.clone()));
            }
            (Some('?'), Some(_)) => {
                pattern_chars.next();
                text_chars.next();
            }
            (Some(p), Some(t)) if *p == *t => {
                pattern_chars.next();
                text_chars.next();
            }
            (None, None) => return true,
            _ => {
                // Try backtracking
                if let Some((p, mut t)) = pattern_stack.pop() {
                    t.next(); // Consume one more character from text
                    if t.peek().is_some() {
                        pattern_chars = p;
                        text_chars = t;
                        pattern_stack.push((pattern_chars.clone(), text_chars.clone()));
                    } else {
                        // No more text to consume, try next backtrack point
                        continue;
                    }
                } else {
                    return false;
                }
            }
        }
    }
}

/// Checks if an IP address matches a CIDR block.
fn ip_matches_cidr(ip_str: &str, cidr: &str) -> bool {
    // Parse the IP address
    let ip: IpAddr = match ip_str.parse() {
        Ok(ip) => ip,
        Err(_) => return false,
    };

    // Parse CIDR notation (e.g., "192.168.1.0/24" or just "192.168.1.1")
    let (network_str, prefix_len) = if let Some((net, len)) = cidr.split_once('/') {
        let prefix: u8 = match len.parse() {
            Ok(p) => p,
            Err(_) => return false,
        };
        (net, prefix)
    } else {
        // Single IP address - use full prefix length
        match ip {
            IpAddr::V4(_) => (cidr, 32),
            IpAddr::V6(_) => (cidr, 128),
        }
    };

    let network: IpAddr = match network_str.parse() {
        Ok(net) => net,
        Err(_) => return false,
    };

    // Both must be same type
    match (ip, network) {
        (IpAddr::V4(ip), IpAddr::V4(net)) => {
            if prefix_len > 32 {
                return false;
            }
            let mask = if prefix_len == 0 { 0u32 } else { !0u32 << (32 - prefix_len) };
            let ip_bits = u32::from(ip);
            let net_bits = u32::from(net);
            (ip_bits & mask) == (net_bits & mask)
        }
        (IpAddr::V6(ip), IpAddr::V6(net)) => {
            if prefix_len > 128 {
                return false;
            }
            let mask = if prefix_len == 0 { 0u128 } else { !0u128 << (128 - prefix_len) };
            let ip_bits = u128::from(ip);
            let net_bits = u128::from(net);
            (ip_bits & mask) == (net_bits & mask)
        }
        _ => false, // Mismatched IP versions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_policy() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/*"
                }
            ]
        }"#;

        let policy = BucketPolicy::from_json(json).unwrap();
        assert_eq!(policy.version, "2012-10-17");
        assert_eq!(policy.statement.len(), 1);
        assert_eq!(policy.statement[0].effect, Effect::Allow);

        policy.validate().unwrap();
    }

    #[test]
    fn test_parse_policy_with_conditions() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/*",
                    "Condition": {
                        "IpAddress": {
                            "aws:SourceIp": "192.168.1.0/24"
                        }
                    }
                }
            ]
        }"#;

        let policy = BucketPolicy::from_json(json).unwrap();
        assert!(policy.statement[0].condition.is_some());
        policy.validate().unwrap();
    }

    #[test]
    fn test_parse_policy_with_multiple_actions() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["arn:aws:iam::123456789012:root"]},
                    "Action": ["s3:GetObject", "s3:PutObject"],
                    "Resource": ["arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket/*"]
                }
            ]
        }"#;

        let policy = BucketPolicy::from_json(json).unwrap();
        policy.validate().unwrap();
    }

    #[test]
    fn test_roundtrip_serialization() {
        let json = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::my-bucket/*"}]}"#;
        let policy = BucketPolicy::from_json(json).unwrap();
        let serialized = policy.to_json().unwrap();
        let reparsed = BucketPolicy::from_json(&serialized).unwrap();
        assert_eq!(reparsed.version, policy.version);
        assert_eq!(reparsed.statement.len(), policy.statement.len());
    }

    #[test]
    fn test_invalid_version() {
        let json = r#"{
            "Version": "2020-01-01",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/*"
                }
            ]
        }"#;

        let policy = BucketPolicy::from_json(json).unwrap();
        assert!(policy.validate().is_err());
    }

    #[test]
    fn test_empty_statement() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": []
        }"#;

        let policy = BucketPolicy::from_json(json).unwrap();
        assert!(policy.validate().is_err());
    }

    #[test]
    fn test_wildcard_match() {
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("foo*", "foobar"));
        assert!(wildcard_match("*bar", "foobar"));
        assert!(wildcard_match("foo*bar", "foobazbar"));
        assert!(wildcard_match("foo?bar", "fooxbar"));
        assert!(!wildcard_match("foo?bar", "fooxxbar"));
        assert!(wildcard_match("s3:Get*", "s3:GetObject"));
        assert!(wildcard_match("arn:aws:s3:::bucket/*", "arn:aws:s3:::bucket/key"));
    }

    #[test]
    fn test_ip_matches_cidr() {
        assert!(ip_matches_cidr("192.168.1.100", "192.168.1.0/24"));
        assert!(!ip_matches_cidr("192.168.2.100", "192.168.1.0/24"));
        assert!(ip_matches_cidr("10.0.0.1", "10.0.0.0/8"));
        assert!(ip_matches_cidr("192.168.1.1", "192.168.1.1"));
        assert!(!ip_matches_cidr("192.168.1.2", "192.168.1.1"));
    }

    #[test]
    fn test_evaluate_allow_policy() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/*"
                }
            ]
        }"#;

        let policy = BucketPolicy::from_json(json).unwrap();
        let ctx = RequestContext::new(
            "*".to_string(),
            true,
            S3Action::GetObject,
            "my-bucket",
            Some("test.txt"),
        );
        assert_eq!(policy.evaluate(&ctx), PolicyDecision::Allow);
    }

    #[test]
    fn test_evaluate_deny_policy() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:DeleteObject",
                    "Resource": "arn:aws:s3:::my-bucket/*"
                }
            ]
        }"#;

        let policy = BucketPolicy::from_json(json).unwrap();
        let ctx = RequestContext::new(
            "*".to_string(),
            true,
            S3Action::DeleteObject,
            "my-bucket",
            Some("test.txt"),
        );
        assert_eq!(policy.evaluate(&ctx), PolicyDecision::Deny);
    }

    #[test]
    fn test_evaluate_no_match() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::other-bucket/*"
                }
            ]
        }"#;

        let policy = BucketPolicy::from_json(json).unwrap();
        let ctx = RequestContext::new(
            "*".to_string(),
            true,
            S3Action::GetObject,
            "my-bucket",
            Some("test.txt"),
        );
        assert_eq!(policy.evaluate(&ctx), PolicyDecision::DefaultDeny);
    }

    #[test]
    fn test_evaluate_deny_overrides_allow() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:*",
                    "Resource": "arn:aws:s3:::my-bucket/*"
                },
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:DeleteObject",
                    "Resource": "arn:aws:s3:::my-bucket/*"
                }
            ]
        }"#;

        let policy = BucketPolicy::from_json(json).unwrap();

        // GetObject should be allowed
        let ctx = RequestContext::new(
            "*".to_string(),
            true,
            S3Action::GetObject,
            "my-bucket",
            Some("test.txt"),
        );
        assert_eq!(policy.evaluate(&ctx), PolicyDecision::Allow);

        // DeleteObject should be denied
        let ctx = RequestContext::new(
            "*".to_string(),
            true,
            S3Action::DeleteObject,
            "my-bucket",
            Some("test.txt"),
        );
        assert_eq!(policy.evaluate(&ctx), PolicyDecision::Deny);
    }

    #[test]
    fn test_evaluate_ip_condition() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/*",
                    "Condition": {
                        "IpAddress": {
                            "aws:SourceIp": "192.168.1.0/24"
                        }
                    }
                }
            ]
        }"#;

        let policy = BucketPolicy::from_json(json).unwrap();

        // Request from allowed IP
        let ctx = RequestContext::new(
            "*".to_string(),
            true,
            S3Action::GetObject,
            "my-bucket",
            Some("test.txt"),
        )
        .with_source_ip("192.168.1.100".parse().unwrap());
        assert_eq!(policy.evaluate(&ctx), PolicyDecision::Allow);

        // Request from different IP
        let ctx = RequestContext::new(
            "*".to_string(),
            true,
            S3Action::GetObject,
            "my-bucket",
            Some("test.txt"),
        )
        .with_source_ip("10.0.0.1".parse().unwrap());
        assert_eq!(policy.evaluate(&ctx), PolicyDecision::DefaultDeny);
    }

    #[test]
    fn test_evaluate_secure_transport_condition() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:*",
                    "Resource": "arn:aws:s3:::my-bucket/*",
                    "Condition": {
                        "Bool": {
                            "aws:SecureTransport": "false"
                        }
                    }
                }
            ]
        }"#;

        let policy = BucketPolicy::from_json(json).unwrap();

        // HTTPS request - should not match deny (condition doesn't match)
        let ctx = RequestContext::new(
            "*".to_string(),
            true,
            S3Action::GetObject,
            "my-bucket",
            Some("test.txt"),
        )
        .with_secure_transport(true);
        assert_eq!(policy.evaluate(&ctx), PolicyDecision::DefaultDeny);

        // HTTP request - should be denied
        let ctx = RequestContext::new(
            "*".to_string(),
            true,
            S3Action::GetObject,
            "my-bucket",
            Some("test.txt"),
        )
        .with_secure_transport(false);
        assert_eq!(policy.evaluate(&ctx), PolicyDecision::Deny);
    }
}
