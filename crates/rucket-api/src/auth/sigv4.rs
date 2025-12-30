// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! AWS Signature Version 4 validation.

use axum::http::{HeaderMap, Method};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::BTreeMap;

use rucket_core::config::AuthConfig;

type HmacSha256 = Hmac<Sha256>;

/// Errors that can occur during signature validation.
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    /// Missing Authorization header.
    #[error("missing Authorization header")]
    MissingAuthHeader,

    /// Invalid Authorization header format.
    #[error("invalid Authorization header format")]
    InvalidAuthHeader,

    /// Missing required header.
    #[error("missing required header: {0}")]
    MissingHeader(String),

    /// Invalid date format.
    #[error("invalid date format")]
    InvalidDate,

    /// Signature mismatch.
    #[error("signature does not match")]
    SignatureMismatch,

    /// Request expired.
    #[error("request has expired")]
    RequestExpired,

    /// Unknown access key.
    #[error("unknown access key")]
    UnknownAccessKey,
}

/// AWS Signature V4 validator.
pub struct SigV4Validator {
    access_key: String,
    secret_key: String,
    region: String,
    service: String,
}

impl SigV4Validator {
    /// Create a new validator.
    #[must_use]
    pub fn new(config: &AuthConfig) -> Self {
        Self {
            access_key: config.access_key.clone(),
            secret_key: config.secret_key.clone(),
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
        }
    }

    /// Validate an incoming request.
    ///
    /// # Errors
    ///
    /// Returns an error if the signature is invalid or missing.
    pub fn validate(
        &self,
        method: &Method,
        uri: &str,
        headers: &HeaderMap,
        payload_hash: &str,
    ) -> Result<(), ValidationError> {
        // Parse Authorization header
        let auth_header = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or(ValidationError::MissingAuthHeader)?;

        let parsed = self.parse_auth_header(auth_header)?;

        // Validate access key
        if parsed.access_key != self.access_key {
            return Err(ValidationError::UnknownAccessKey);
        }

        // Get date from headers
        let amz_date = headers
            .get("x-amz-date")
            .and_then(|v| v.to_str().ok())
            .ok_or(ValidationError::MissingHeader("x-amz-date".to_string()))?;

        // Parse and validate date
        let request_time = self.parse_amz_date(amz_date)?;
        self.validate_request_time(request_time)?;

        // Compute expected signature
        let canonical_request = self.create_canonical_request(
            method,
            uri,
            headers,
            &parsed.signed_headers,
            payload_hash,
        )?;

        let string_to_sign =
            self.create_string_to_sign(&canonical_request, amz_date, &parsed.credential_scope);

        let signature = self.calculate_signature(&string_to_sign, amz_date);

        // Compare signatures
        if !constant_time_eq(signature.as_bytes(), parsed.signature.as_bytes()) {
            return Err(ValidationError::SignatureMismatch);
        }

        Ok(())
    }

    fn parse_auth_header(&self, header: &str) -> Result<ParsedAuthHeader, ValidationError> {
        // Format: AWS4-HMAC-SHA256 Credential=.../..., SignedHeaders=..., Signature=...
        let header =
            header.strip_prefix("AWS4-HMAC-SHA256 ").ok_or(ValidationError::InvalidAuthHeader)?;

        let mut credential = None;
        let mut signed_headers = None;
        let mut signature = None;

        for part in header.split(", ") {
            if let Some(value) = part.strip_prefix("Credential=") {
                credential = Some(value.to_string());
            } else if let Some(value) = part.strip_prefix("SignedHeaders=") {
                signed_headers = Some(value.to_string());
            } else if let Some(value) = part.strip_prefix("Signature=") {
                signature = Some(value.to_string());
            }
        }

        let credential = credential.ok_or(ValidationError::InvalidAuthHeader)?;
        let signed_headers = signed_headers.ok_or(ValidationError::InvalidAuthHeader)?;
        let signature = signature.ok_or(ValidationError::InvalidAuthHeader)?;

        // Parse credential: ACCESS_KEY/DATE/REGION/SERVICE/aws4_request
        let parts: Vec<&str> = credential.split('/').collect();
        if parts.len() != 5 {
            return Err(ValidationError::InvalidAuthHeader);
        }

        Ok(ParsedAuthHeader {
            access_key: parts[0].to_string(),
            credential_scope: format!("{}/{}/{}/aws4_request", parts[1], parts[2], parts[3]),
            signed_headers,
            signature,
        })
    }

    fn parse_amz_date(&self, date: &str) -> Result<DateTime<Utc>, ValidationError> {
        DateTime::parse_from_str(date, "%Y%m%dT%H%M%SZ")
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|_| ValidationError::InvalidDate)
    }

    fn validate_request_time(&self, request_time: DateTime<Utc>) -> Result<(), ValidationError> {
        let now = Utc::now();
        let diff = (now - request_time).num_seconds().abs();

        // Allow 15 minute clock skew
        if diff > 900 {
            return Err(ValidationError::RequestExpired);
        }

        Ok(())
    }

    fn create_canonical_request(
        &self,
        method: &Method,
        uri: &str,
        headers: &HeaderMap,
        signed_headers: &str,
        payload_hash: &str,
    ) -> Result<String, ValidationError> {
        // Parse URI into path and query
        let (path, query) = uri.split_once('?').unwrap_or((uri, ""));

        // Canonical URI (path)
        let canonical_uri = self.normalize_uri_path(path);

        // Canonical query string
        let canonical_query = self.canonicalize_query_string(query);

        // Canonical headers
        let mut canonical_headers = String::new();
        let header_names: Vec<&str> = signed_headers.split(';').collect();

        for name in &header_names {
            let value = headers
                .get(*name)
                .and_then(|v| v.to_str().ok())
                .ok_or(ValidationError::MissingHeader((*name).to_string()))?;
            canonical_headers.push_str(&format!("{}:{}\n", name, value.trim()));
        }

        Ok(format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method.as_str(),
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash
        ))
    }

    fn normalize_uri_path(&self, path: &str) -> String {
        // URL-encode the path, keeping '/' unencoded
        if path.is_empty() {
            return "/".to_string();
        }

        let segments: Vec<&str> = path.split('/').collect();
        let encoded: Vec<String> = segments
            .iter()
            .map(|s| {
                percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC)
                    .to_string()
            })
            .collect();

        encoded.join("/")
    }

    fn canonicalize_query_string(&self, query: &str) -> String {
        if query.is_empty() {
            return String::new();
        }

        let mut params: BTreeMap<String, String> = BTreeMap::new();

        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                params.insert(key.to_string(), value.to_string());
            } else {
                params.insert(pair.to_string(), String::new());
            }
        }

        params.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&")
    }

    fn create_string_to_sign(
        &self,
        canonical_request: &str,
        amz_date: &str,
        credential_scope: &str,
    ) -> String {
        use sha2::Digest;

        let hash = hex::encode(sha2::Sha256::digest(canonical_request.as_bytes()));

        format!("AWS4-HMAC-SHA256\n{}\n{}\n{}", amz_date, credential_scope, hash)
    }

    fn calculate_signature(&self, string_to_sign: &str, amz_date: &str) -> String {
        let date = &amz_date[..8]; // YYYYMMDD

        // Derive signing key
        let k_date =
            self.hmac_sha256(format!("AWS4{}", self.secret_key).as_bytes(), date.as_bytes());
        let k_region = self.hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = self.hmac_sha256(&k_region, self.service.as_bytes());
        let k_signing = self.hmac_sha256(&k_service, b"aws4_request");

        // Calculate signature
        let signature = self.hmac_sha256(&k_signing, string_to_sign.as_bytes());

        hex::encode(signature)
    }

    fn hmac_sha256(&self, key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }
}

struct ParsedAuthHeader {
    access_key: String,
    credential_scope: String,
    signed_headers: String,
    signature: String,
}

/// Constant-time comparison to prevent timing attacks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    result == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_auth_header() {
        let config = AuthConfig {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        };
        let validator = SigV4Validator::new(&config);

        let header = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let parsed = validator.parse_auth_header(header).unwrap();

        assert_eq!(parsed.access_key, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(parsed.signed_headers, "host;x-amz-date");
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
    }
}
