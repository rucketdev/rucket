//! S3 Compatibility Test Harness
//!
//! Provides `S3TestContext` for spinning up an in-memory Rucket server
//! and testing S3 API operations against it.

use std::net::SocketAddr;
use std::sync::Arc;

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::list_object_versions::ListObjectVersionsOutput;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use aws_sdk_s3::Client;
use rand::Rng;
use rucket_api::create_router;
use rucket_core::config::{ApiCompatibilityMode, AuthConfig};
use rucket_storage::LocalStorage;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Test context providing an S3 client connected to an in-memory Rucket server.
pub struct S3TestContext {
    /// The S3 client for making requests.
    pub client: Client,
    /// The endpoint URL of the test server.
    pub endpoint: String,
    /// The default bucket name (if created).
    pub bucket: String,
    /// The server address.
    pub addr: SocketAddr,
    /// The auth config (if auth is enabled).
    pub auth_config: Option<AuthConfig>,
    _server_handle: JoinHandle<()>,
    _shutdown_tx: oneshot::Sender<()>,
    _temp_dir: TempDir,
}

impl S3TestContext {
    /// Create a new test context with an auto-generated bucket.
    pub async fn new() -> Self {
        let bucket = random_bucket_name();
        let mut ctx = Self::start_server(None, false).await;
        ctx.bucket = bucket.clone();
        ctx.create_bucket_internal(&bucket).await;
        ctx
    }

    /// Create a new test context with authentication enabled and an auto-generated bucket.
    pub async fn new_with_auth() -> Self {
        let auth = AuthConfig::default();
        let bucket = random_bucket_name();
        let mut ctx = Self::start_server(Some(auth), false).await;
        ctx.bucket = bucket.clone();
        ctx.create_bucket_internal(&bucket).await;
        ctx
    }

    /// Create a new test context with a specific bucket name.
    pub async fn with_bucket(name: &str) -> Self {
        let mut ctx = Self::start_server(None, false).await;
        ctx.bucket = name.to_string();
        ctx.create_bucket_internal(name).await;
        ctx
    }

    /// Create a new test context with versioning enabled on the default bucket.
    pub async fn with_versioning() -> Self {
        let ctx = Self::new().await;
        ctx.enable_versioning().await;
        ctx
    }

    /// Create a new test context with Object Lock enabled on the default bucket.
    /// Object Lock requires versioning, which is automatically enabled.
    pub async fn with_object_lock() -> Self {
        let bucket = random_bucket_name();
        let mut ctx = Self::start_server(None, false).await;
        ctx.bucket = bucket.clone();
        ctx.create_bucket_with_object_lock(&bucket).await;
        ctx
    }

    /// Create a new test context with SSE-S3 encryption enabled.
    pub async fn with_encryption() -> Self {
        let bucket = random_bucket_name();
        let mut ctx = Self::start_server(None, true).await;
        ctx.bucket = bucket.clone();
        ctx.create_bucket_internal(&bucket).await;
        ctx
    }

    /// Create a new test context with multiple buckets.
    pub async fn with_buckets(names: &[&str]) -> Self {
        let mut ctx = Self::start_server(None, false).await;
        if let Some(first) = names.first() {
            ctx.bucket = first.to_string();
        }
        for name in names {
            ctx.create_bucket_internal(name).await;
        }
        ctx
    }

    /// Create a test context without creating any buckets.
    pub async fn without_bucket() -> Self {
        Self::start_server(None, false).await
    }

    /// Start the test server and return the context.
    async fn start_server(auth_config: Option<AuthConfig>, enable_encryption: bool) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");

        let storage =
            LocalStorage::new_in_memory(data_dir, tmp_dir).await.expect("Failed to create storage");

        // Enable encryption if requested (32-byte test master key)
        let storage = if enable_encryption {
            let test_master_key = [0x42u8; 32]; // Test-only key
            storage.with_encryption(&test_master_key).expect("Failed to enable encryption")
        } else {
            storage
        };

        // 5 GiB max body size (S3 max single PUT)
        let app = create_router(
            Arc::new(storage),
            5 * 1024 * 1024 * 1024,
            ApiCompatibilityMode::Minio,
            false, // log_requests disabled for tests
            auth_config.as_ref(),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get local addr");

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("Server error");
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let endpoint = format!("http://{}", addr);
        let client = Self::create_client(&endpoint);

        Self {
            client,
            endpoint,
            bucket: String::new(),
            addr,
            auth_config,
            _server_handle: handle,
            _shutdown_tx: shutdown_tx,
            _temp_dir: temp_dir,
        }
    }

    /// Create an S3 client for the given endpoint.
    fn create_client(endpoint: &str) -> Client {
        let credentials = Credentials::new("rucket", "rucket123", None, None, "test");

        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .credentials_provider(credentials)
            .force_path_style(true)
            // Disable response checksum validation for test flexibility
            .response_checksum_validation(
                aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired,
            )
            .build();

        Client::from_conf(config)
    }

    /// Create a bucket (internal helper).
    async fn create_bucket_internal(&self, name: &str) {
        self.client.create_bucket().bucket(name).send().await.expect("Failed to create bucket");
    }

    /// Create a bucket.
    pub async fn create_bucket(&self, name: &str) {
        self.create_bucket_internal(name).await;
    }

    /// Delete a bucket.
    pub async fn delete_bucket(&self, name: &str) {
        self.client.delete_bucket().bucket(name).send().await.expect("Failed to delete bucket");
    }

    /// Put an object with the given key and data.
    pub async fn put(&self, key: &str, data: &[u8]) -> PutObjectOutput {
        self.put_in_bucket(&self.bucket, key, data).await
    }

    /// Put an object in a specific bucket.
    pub async fn put_in_bucket(&self, bucket: &str, key: &str, data: &[u8]) -> PutObjectOutput {
        let body = ByteStream::from(data.to_vec());
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .expect("Failed to put object")
    }

    /// Get an object's content.
    pub async fn get(&self, key: &str) -> Vec<u8> {
        self.get_from_bucket(&self.bucket, key).await
    }

    /// Get an object from a specific bucket.
    pub async fn get_from_bucket(&self, bucket: &str, key: &str) -> Vec<u8> {
        let response = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .expect("Failed to get object");
        response.body.collect().await.expect("Failed to read body").into_bytes().to_vec()
    }

    /// Get an object (full response).
    pub async fn get_object(&self, key: &str) -> GetObjectOutput {
        self.client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .expect("Failed to get object")
    }

    /// Head an object.
    pub async fn head(&self, key: &str) -> HeadObjectOutput {
        self.client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .expect("Failed to head object")
    }

    /// Delete an object.
    pub async fn delete(&self, key: &str) {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .expect("Failed to delete object");
    }

    /// Check if an object exists.
    pub async fn exists(&self, key: &str) -> bool {
        self.client.head_object().bucket(&self.bucket).key(key).send().await.is_ok()
    }

    /// Enable versioning on the default bucket.
    pub async fn enable_versioning(&self) {
        self.enable_versioning_on_bucket(&self.bucket).await;
    }

    /// Enable versioning on a specific bucket.
    pub async fn enable_versioning_on_bucket(&self, bucket: &str) {
        self.client
            .put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                VersioningConfiguration::builder().status(BucketVersioningStatus::Enabled).build(),
            )
            .send()
            .await
            .expect("Failed to enable versioning");
    }

    /// Suspend versioning on the default bucket.
    pub async fn suspend_versioning(&self) {
        self.client
            .put_bucket_versioning()
            .bucket(&self.bucket)
            .versioning_configuration(
                VersioningConfiguration::builder()
                    .status(BucketVersioningStatus::Suspended)
                    .build(),
            )
            .send()
            .await
            .expect("Failed to suspend versioning");
    }

    /// List objects in the default bucket (V2).
    pub async fn list_objects(&self) -> ListObjectsV2Output {
        self.client
            .list_objects_v2()
            .bucket(&self.bucket)
            .send()
            .await
            .expect("Failed to list objects")
    }

    /// List object versions in the default bucket.
    pub async fn list_versions(&self) -> ListObjectVersionsOutput {
        self.client
            .list_object_versions()
            .bucket(&self.bucket)
            .send()
            .await
            .expect("Failed to list versions")
    }

    // =========================================================================
    // Object Lock Methods
    // =========================================================================

    /// Create a bucket with Object Lock enabled.
    pub async fn create_bucket_with_object_lock(&self, name: &str) {
        self.client
            .create_bucket()
            .bucket(name)
            .object_lock_enabled_for_bucket(true)
            .send()
            .await
            .expect("Failed to create bucket with object lock");
    }

    /// Get Object Lock configuration for the default bucket.
    pub async fn get_object_lock_configuration(
        &self,
    ) -> aws_sdk_s3::operation::get_object_lock_configuration::GetObjectLockConfigurationOutput
    {
        self.client
            .get_object_lock_configuration()
            .bucket(&self.bucket)
            .send()
            .await
            .expect("Failed to get object lock configuration")
    }

    /// Put Object Lock configuration on the default bucket.
    pub async fn put_object_lock_configuration(
        &self,
        config: aws_sdk_s3::types::ObjectLockConfiguration,
    ) {
        self.client
            .put_object_lock_configuration()
            .bucket(&self.bucket)
            .object_lock_configuration(config)
            .send()
            .await
            .expect("Failed to put object lock configuration");
    }

    /// Get object retention for a specific object.
    pub async fn get_object_retention(
        &self,
        key: &str,
        version_id: Option<&str>,
    ) -> aws_sdk_s3::operation::get_object_retention::GetObjectRetentionOutput {
        let mut req = self.client.get_object_retention().bucket(&self.bucket).key(key);
        if let Some(vid) = version_id {
            req = req.version_id(vid);
        }
        req.send().await.expect("Failed to get object retention")
    }

    /// Put object retention on a specific object.
    pub async fn put_object_retention(
        &self,
        key: &str,
        retention: aws_sdk_s3::types::ObjectLockRetention,
        version_id: Option<&str>,
        bypass_governance: bool,
    ) {
        let mut req =
            self.client.put_object_retention().bucket(&self.bucket).key(key).retention(retention);
        if let Some(vid) = version_id {
            req = req.version_id(vid);
        }
        if bypass_governance {
            req = req.bypass_governance_retention(true);
        }
        req.send().await.expect("Failed to put object retention");
    }

    /// Get legal hold for a specific object.
    pub async fn get_object_legal_hold(
        &self,
        key: &str,
        version_id: Option<&str>,
    ) -> aws_sdk_s3::operation::get_object_legal_hold::GetObjectLegalHoldOutput {
        let mut req = self.client.get_object_legal_hold().bucket(&self.bucket).key(key);
        if let Some(vid) = version_id {
            req = req.version_id(vid);
        }
        req.send().await.expect("Failed to get object legal hold")
    }

    /// Put legal hold on a specific object.
    pub async fn put_object_legal_hold(
        &self,
        key: &str,
        legal_hold: aws_sdk_s3::types::ObjectLockLegalHold,
        version_id: Option<&str>,
    ) {
        let mut req = self
            .client
            .put_object_legal_hold()
            .bucket(&self.bucket)
            .key(key)
            .legal_hold(legal_hold);
        if let Some(vid) = version_id {
            req = req.version_id(vid);
        }
        req.send().await.expect("Failed to put object legal hold");
    }

    /// Delete an object with optional version ID and bypass governance.
    pub async fn delete_object_with_options(
        &self,
        key: &str,
        version_id: Option<&str>,
        bypass_governance: bool,
    ) -> Result<
        aws_sdk_s3::operation::delete_object::DeleteObjectOutput,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::delete_object::DeleteObjectError>,
    > {
        let mut req = self.client.delete_object().bucket(&self.bucket).key(key);
        if let Some(vid) = version_id {
            req = req.version_id(vid);
        }
        if bypass_governance {
            req = req.bypass_governance_retention(true);
        }
        req.send().await
    }
}

// =============================================================================
// Test Data Helpers
// =============================================================================

/// Generate random bytes of the given size.
pub fn random_bytes(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen()).collect()
}

/// Generate a random string of the given length.
pub fn random_string(len: usize) -> String {
    use rand::distributions::Alphanumeric;
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect::<String>()
        .to_lowercase()
}

/// Generate a random bucket name (S3-compliant).
pub fn random_bucket_name() -> String {
    format!("test-bucket-{}", Uuid::new_v4().to_string()[..8].to_lowercase())
}

/// Generate a random object key.
pub fn random_key() -> String {
    format!("object-{}", Uuid::new_v4().to_string()[..8].to_lowercase())
}

// =============================================================================
// Assertion Helpers
// =============================================================================

/// Assert that an object has the expected content.
pub async fn assert_object_content(ctx: &S3TestContext, key: &str, expected: &[u8]) {
    let content = ctx.get(key).await;
    assert_eq!(content.as_slice(), expected, "Object content mismatch for key: {}", key);
}

/// Assert that an object does not exist (404).
pub async fn assert_not_found(ctx: &S3TestContext, key: &str) {
    let result = ctx.client.head_object().bucket(&ctx.bucket).key(key).send().await;
    assert!(result.is_err(), "Object should not exist: {}", key);
}

/// Assert that a bucket is empty.
pub async fn assert_bucket_empty(ctx: &S3TestContext) {
    let response = ctx.list_objects().await;
    assert!(
        response.contents().is_empty(),
        "Bucket should be empty, found {} objects",
        response.contents().len()
    );
}

/// Assert that an error response has the expected error code.
pub fn assert_error_code<T, E: std::fmt::Debug>(result: &Result<T, E>, expected_code: &str) {
    match result {
        Ok(_) => panic!("Expected error with code {}, but got success", expected_code),
        Err(e) => {
            let error_str = format!("{:?}", e);
            assert!(
                error_str.contains(expected_code),
                "Expected error code {}, got: {}",
                expected_code,
                error_str
            );
        }
    }
}
