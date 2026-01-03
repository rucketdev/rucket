//! AWS Signature Version 4 validation.
//!
//! Supports both header-based and query parameter-based (presigned URL) authentication.

use std::collections::BTreeMap;

use axum::http::{HeaderMap, Method};
use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, Mac};
use rucket_core::config::AuthConfig;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Maximum expiration time for presigned URLs (7 days in seconds).
const MAX_PRESIGNED_EXPIRES: i64 = 604800;

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

    /// Missing required query parameter.
    #[error("missing required query parameter: {0}")]
    MissingQueryParam(String),

    /// Invalid date format.
    #[error("invalid date format")]
    InvalidDate,

    /// Invalid expires value.
    #[error("invalid expires value")]
    InvalidExpires,

    /// Signature mismatch.
    #[error("signature does not match")]
    SignatureMismatch,

    /// Request expired.
    #[error("request has expired")]
    RequestExpired,

    /// Unknown access key.
    #[error("unknown access key")]
    UnknownAccessKey,

    /// Invalid algorithm.
    #[error("unsupported signature algorithm")]
    InvalidAlgorithm,
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

    /// Validate a presigned URL request.
    ///
    /// Presigned URLs use query parameters instead of headers for authentication:
    /// - X-Amz-Algorithm: AWS4-HMAC-SHA256
    /// - X-Amz-Credential: ACCESS_KEY/DATE/REGION/SERVICE/aws4_request
    /// - X-Amz-Date: YYYYMMDDTHHMMSSZ
    /// - X-Amz-Expires: seconds until expiration
    /// - X-Amz-SignedHeaders: semicolon-separated list of signed headers
    /// - X-Amz-Signature: the signature
    ///
    /// # Errors
    ///
    /// Returns an error if the signature is invalid, missing parameters, or expired.
    pub fn validate_presigned(
        &self,
        method: &Method,
        uri: &str,
        headers: &HeaderMap,
        query_params: &BTreeMap<String, String>,
    ) -> Result<(), ValidationError> {
        // Parse presigned parameters from query string
        let parsed = self.parse_presigned_params(query_params)?;

        // Validate algorithm
        if parsed.algorithm != "AWS4-HMAC-SHA256" {
            return Err(ValidationError::InvalidAlgorithm);
        }

        // Validate access key
        if parsed.access_key != self.access_key {
            return Err(ValidationError::UnknownAccessKey);
        }

        // Parse and validate date + expiration
        let request_time = self.parse_amz_date(&parsed.amz_date)?;
        self.validate_presigned_expiration(request_time, parsed.expires)?;

        // Build canonical request for presigned URL
        // For presigned URLs, the signature is NOT included in the canonical query string
        let canonical_request = self.create_presigned_canonical_request(
            method,
            uri,
            headers,
            query_params,
            &parsed.signed_headers,
        )?;

        let string_to_sign = self.create_string_to_sign(
            &canonical_request,
            &parsed.amz_date,
            &parsed.credential_scope,
        );

        let signature = self.calculate_signature(&string_to_sign, &parsed.amz_date);

        // Compare signatures
        if !constant_time_eq(signature.as_bytes(), parsed.signature.as_bytes()) {
            return Err(ValidationError::SignatureMismatch);
        }

        Ok(())
    }

    /// Check if a request is a presigned URL request by looking for signature in query params.
    #[must_use]
    pub fn is_presigned_request(query_params: &BTreeMap<String, String>) -> bool {
        query_params.contains_key("X-Amz-Signature")
    }

    fn parse_presigned_params(
        &self,
        params: &BTreeMap<String, String>,
    ) -> Result<ParsedPresignedParams, ValidationError> {
        let algorithm = params
            .get("X-Amz-Algorithm")
            .ok_or(ValidationError::MissingQueryParam("X-Amz-Algorithm".to_string()))?
            .clone();

        let credential = params
            .get("X-Amz-Credential")
            .ok_or(ValidationError::MissingQueryParam("X-Amz-Credential".to_string()))?;

        // URL-decode the credential (it may be percent-encoded)
        let credential = percent_encoding::percent_decode_str(credential)
            .decode_utf8()
            .map_err(|_| ValidationError::InvalidAuthHeader)?
            .to_string();

        let amz_date = params
            .get("X-Amz-Date")
            .ok_or(ValidationError::MissingQueryParam("X-Amz-Date".to_string()))?
            .clone();

        let expires_str = params
            .get("X-Amz-Expires")
            .ok_or(ValidationError::MissingQueryParam("X-Amz-Expires".to_string()))?;
        let expires: i64 = expires_str.parse().map_err(|_| ValidationError::InvalidExpires)?;

        // Validate expires is within allowed range
        if expires <= 0 || expires > MAX_PRESIGNED_EXPIRES {
            return Err(ValidationError::InvalidExpires);
        }

        let signed_headers = params
            .get("X-Amz-SignedHeaders")
            .ok_or(ValidationError::MissingQueryParam("X-Amz-SignedHeaders".to_string()))?
            .clone();

        let signature = params
            .get("X-Amz-Signature")
            .ok_or(ValidationError::MissingQueryParam("X-Amz-Signature".to_string()))?
            .clone();

        // Parse credential: ACCESS_KEY/DATE/REGION/SERVICE/aws4_request
        let parts: Vec<&str> = credential.split('/').collect();
        if parts.len() != 5 {
            return Err(ValidationError::InvalidAuthHeader);
        }

        Ok(ParsedPresignedParams {
            algorithm,
            access_key: parts[0].to_string(),
            credential_scope: format!("{}/{}/{}/aws4_request", parts[1], parts[2], parts[3]),
            amz_date,
            expires,
            signed_headers,
            signature,
        })
    }

    fn validate_presigned_expiration(
        &self,
        request_time: DateTime<Utc>,
        expires_seconds: i64,
    ) -> Result<(), ValidationError> {
        let now = Utc::now();
        let expiration_time = request_time + Duration::seconds(expires_seconds);

        if now > expiration_time {
            return Err(ValidationError::RequestExpired);
        }

        // Also check that the request time is not too far in the future (clock skew)
        if request_time > now + Duration::seconds(900) {
            return Err(ValidationError::RequestExpired);
        }

        Ok(())
    }

    fn create_presigned_canonical_request(
        &self,
        method: &Method,
        uri: &str,
        headers: &HeaderMap,
        query_params: &BTreeMap<String, String>,
        signed_headers: &str,
    ) -> Result<String, ValidationError> {
        // Parse URI into path (ignore query from URI, we use the parsed params)
        let path = uri.split('?').next().unwrap_or(uri);

        // Canonical URI (path)
        let canonical_uri = self.normalize_uri_path(path);

        // Canonical query string - exclude X-Amz-Signature but include all other params
        let canonical_query = self.canonicalize_presigned_query(query_params);

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

        // For presigned URLs, payload is typically UNSIGNED-PAYLOAD
        let payload_hash = "UNSIGNED-PAYLOAD";

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

    fn canonicalize_presigned_query(&self, params: &BTreeMap<String, String>) -> String {
        // Sort and encode query params, excluding X-Amz-Signature
        params
            .iter()
            .filter(|(k, _)| *k != "X-Amz-Signature")
            .map(|(k, v)| format!("{}={}", Self::uri_encode(k, true), Self::uri_encode(v, true)))
            .collect::<Vec<_>>()
            .join("&")
    }

    /// URI-encode a string according to AWS SigV4 requirements.
    /// Unreserved characters (A-Z, a-z, 0-9, hyphen, underscore, period, tilde) are not encoded.
    fn uri_encode(input: &str, encode_slash: bool) -> String {
        let mut result = String::new();
        for c in input.chars() {
            // Unreserved chars + optionally slash
            let is_unreserved =
                c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '~' || c == '.';
            let is_unencoded_slash = c == '/' && !encode_slash;

            if is_unreserved || is_unencoded_slash {
                result.push(c);
            } else {
                for byte in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", byte));
                }
            }
        }
        result
    }

    fn parse_auth_header(&self, header: &str) -> Result<ParsedAuthHeader, ValidationError> {
        // Format: AWS4-HMAC-SHA256 Credential=.../..., SignedHeaders=..., Signature=...
        // Some clients may omit space after comma, so we split on comma and trim
        let header =
            header.strip_prefix("AWS4-HMAC-SHA256 ").ok_or(ValidationError::InvalidAuthHeader)?;

        let mut credential = None;
        let mut signed_headers = None;
        let mut signature = None;

        for part in header.split(',') {
            let part = part.trim();
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
        // AWS date format: YYYYMMDD'T'HHMMSS'Z' (e.g., 20130524T000000Z)
        // The 'Z' is a literal character, not a timezone specifier
        use chrono::NaiveDateTime;

        // Remove the trailing 'Z' and parse as naive datetime
        let date_without_z = date.strip_suffix('Z').ok_or(ValidationError::InvalidDate)?;
        let naive = NaiveDateTime::parse_from_str(date_without_z, "%Y%m%dT%H%M%S")
            .map_err(|_| ValidationError::InvalidDate)?;
        Ok(naive.and_utc())
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
        // AWS SigV4 unreserved chars: A-Z, a-z, 0-9, hyphen, underscore, period, tilde
        if path.is_empty() {
            return "/".to_string();
        }

        let segments: Vec<&str> = path.split('/').collect();
        let encoded: Vec<String> = segments.iter().map(|s| Self::uri_encode(s, false)).collect();

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

struct ParsedPresignedParams {
    algorithm: String,
    access_key: String,
    credential_scope: String,
    amz_date: String,
    expires: i64,
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

        // With space after comma (standard format)
        let header = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let parsed = validator.parse_auth_header(header).unwrap();
        assert_eq!(parsed.access_key, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(parsed.signed_headers, "host;x-amz-date");

        // Without space after comma (some clients)
        let header_no_space = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-date,Signature=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let parsed = validator.parse_auth_header(header_no_space).unwrap();
        assert_eq!(parsed.access_key, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(parsed.signed_headers, "host;x-amz-date");
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
    }

    #[test]
    fn test_is_presigned_request() {
        let mut params = BTreeMap::new();
        assert!(!SigV4Validator::is_presigned_request(&params));

        params.insert("X-Amz-Signature".to_string(), "abc123".to_string());
        assert!(SigV4Validator::is_presigned_request(&params));
    }

    #[test]
    fn test_parse_presigned_params() {
        let config = AuthConfig {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        };
        let validator = SigV4Validator::new(&config);

        let mut params = BTreeMap::new();
        params.insert("X-Amz-Algorithm".to_string(), "AWS4-HMAC-SHA256".to_string());
        params.insert(
            "X-Amz-Credential".to_string(),
            "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request".to_string(),
        );
        params.insert("X-Amz-Date".to_string(), "20130524T000000Z".to_string());
        params.insert("X-Amz-Expires".to_string(), "86400".to_string());
        params.insert("X-Amz-SignedHeaders".to_string(), "host".to_string());
        params.insert("X-Amz-Signature".to_string(), "aaaa".to_string());

        let parsed = validator.parse_presigned_params(&params).unwrap();
        assert_eq!(parsed.algorithm, "AWS4-HMAC-SHA256");
        assert_eq!(parsed.access_key, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(parsed.expires, 86400);
        assert_eq!(parsed.signed_headers, "host");
    }

    #[test]
    fn test_parse_presigned_params_missing_algorithm() {
        let config = AuthConfig {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        };
        let validator = SigV4Validator::new(&config);

        let params = BTreeMap::new();
        let result = validator.parse_presigned_params(&params);
        assert!(matches!(result, Err(ValidationError::MissingQueryParam(_))));
    }

    #[test]
    fn test_parse_presigned_params_invalid_expires() {
        let config = AuthConfig {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        };
        let validator = SigV4Validator::new(&config);

        let mut params = BTreeMap::new();
        params.insert("X-Amz-Algorithm".to_string(), "AWS4-HMAC-SHA256".to_string());
        params.insert(
            "X-Amz-Credential".to_string(),
            "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request".to_string(),
        );
        params.insert("X-Amz-Date".to_string(), "20130524T000000Z".to_string());
        params.insert("X-Amz-Expires".to_string(), "9999999".to_string()); // Too long
        params.insert("X-Amz-SignedHeaders".to_string(), "host".to_string());
        params.insert("X-Amz-Signature".to_string(), "aaaa".to_string());

        let result = validator.parse_presigned_params(&params);
        assert!(matches!(result, Err(ValidationError::InvalidExpires)));
    }

    #[test]
    fn test_uri_encode() {
        assert_eq!(SigV4Validator::uri_encode("hello", true), "hello");
        assert_eq!(SigV4Validator::uri_encode("hello world", true), "hello%20world");
        assert_eq!(SigV4Validator::uri_encode("test/path", true), "test%2Fpath");
        assert_eq!(SigV4Validator::uri_encode("test/path", false), "test/path");
        assert_eq!(SigV4Validator::uri_encode("a-b_c.d~e", true), "a-b_c.d~e");
    }

    #[test]
    fn test_canonicalize_presigned_query() {
        let config = AuthConfig { access_key: "test".to_string(), secret_key: "test".to_string() };
        let validator = SigV4Validator::new(&config);

        let mut params = BTreeMap::new();
        params.insert("X-Amz-Algorithm".to_string(), "AWS4-HMAC-SHA256".to_string());
        params.insert("X-Amz-Signature".to_string(), "should-be-excluded".to_string());
        params.insert("foo".to_string(), "bar".to_string());

        let result = validator.canonicalize_presigned_query(&params);
        // X-Amz-Signature should be excluded
        assert!(!result.contains("X-Amz-Signature"));
        assert!(result.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(result.contains("foo=bar"));
    }
}
