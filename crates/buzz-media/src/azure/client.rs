//! Azure Blob Storage REST client.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt as _;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_LENGTH, ETAG, IF_MATCH};
use reqwest::header::{IF_NONE_MATCH, RETRY_AFTER};
use reqwest::{Method, StatusCode};
use tokio::io::AsyncReadExt as _;

use super::auth::Authorizer;
use super::xml::{parse_delete_retention, parse_list_blobs};
use super::{AzureBlobConfig, AzureError};

/// Blob service REST version. 2020-02-10+ is required for permanent deletion
/// of soft-deleted versions; 2019-12-12+ for blob versioning.
const API_VERSION: &str = "2021-12-02";
/// Files above this size are uploaded as staged blocks of this size, so a
/// large video is never held in memory at once.
const BLOCK_SIZE: usize = 8 * 1024 * 1024;
const MAX_ATTEMPTS: u32 = 4;
const BACKOFF_BASE: Duration = Duration::from_millis(200);
const BACKOFF_CAP: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Per-read inactivity bound. No total timeout: media downloads stream for as
/// long as the client keeps reading.
const READ_TIMEOUT: Duration = Duration::from_secs(60);
const ERROR_BODY_LIMIT: u64 = 4 * 1024;
const XML_BODY_LIMIT: u64 = 32 * 1024 * 1024;

const X_MS_VERSION: HeaderName = HeaderName::from_static("x-ms-version");
const X_MS_DATE: HeaderName = HeaderName::from_static("x-ms-date");
const X_MS_BLOB_TYPE: HeaderName = HeaderName::from_static("x-ms-blob-type");
const X_MS_BLOB_CONTENT_TYPE: HeaderName = HeaderName::from_static("x-ms-blob-content-type");
const X_MS_RANGE: HeaderName = HeaderName::from_static("x-ms-range");
const X_MS_VERSION_ID: HeaderName = HeaderName::from_static("x-ms-version-id");
const X_MS_ERROR_CODE: HeaderName = HeaderName::from_static("x-ms-error-code");
const X_MS_DELETE_TYPE_PERMANENT: HeaderName =
    HeaderName::from_static("x-ms-delete-type-permanent");

/// A stream of blob body chunks.
pub type BlobStream = Pin<Box<dyn futures_core::Stream<Item = Result<Bytes, AzureError>> + Send>>;

/// Precondition for a blob write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutCondition<'a> {
    /// Unconditional overwrite.
    None,
    /// Create-only (`If-None-Match: *`).
    IfNoneMatchAny,
    /// Compare-and-swap on the current ETag (`If-Match`).
    IfMatch(&'a str),
}

/// Result of a successful blob write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOutcome {
    /// ETag of the written blob.
    pub etag: Option<String>,
    /// Version id created by the write when blob versioning is enabled.
    pub version_id: Option<String>,
}

/// Blob bytes plus the identity of the version they came from.
#[derive(Debug, Clone)]
pub struct BlobContent {
    /// Body bytes (or the requested range).
    pub bytes: Bytes,
    /// ETag of the version read, from the same response.
    pub etag: Option<String>,
}

/// Blob properties from `Get Blob Properties`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobProperties {
    /// Blob size in bytes.
    pub size: u64,
    /// Current ETag.
    pub etag: Option<String>,
    /// Current version id when blob versioning is enabled.
    pub version_id: Option<String>,
}

/// Outcome of a successful `Delete Blob`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteOutcome {
    /// `x-ms-delete-type-permanent`: `Some(false)` means the service kept a
    /// soft-deleted copy that still holds the bytes.
    pub permanent: Option<bool>,
}

/// One blob (or blob version) from `List Blobs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedBlob {
    /// Blob name.
    pub name: String,
    /// Version id, present when listing with versions on a versioned account.
    pub version_id: Option<String>,
    /// Whether this entry is the blob's current version.
    pub is_current_version: bool,
    /// Size in bytes.
    pub size: u64,
}

/// One `List Blobs` page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListBlobsPage {
    /// Blobs in name order (versions of one name are adjacent).
    pub blobs: Vec<ListedBlob>,
    /// Opaque continuation marker; `None` when the listing is complete.
    pub next_marker: Option<String>,
}

/// The account's blob soft-delete policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeleteRetention {
    /// Blob soft delete is enabled.
    pub enabled: bool,
    /// Soft-deleted versions and snapshots may be permanently deleted.
    pub allow_permanent_delete: bool,
}

/// Options for [`AzureBlobClient::get_blob`].
#[derive(Debug, Clone, Copy, Default)]
pub struct GetOptions<'a> {
    /// Inclusive byte range; `None` end reads to the end of the blob.
    pub range: Option<(u64, Option<u64>)>,
    /// Read this exact version instead of the current one.
    pub version_id: Option<&'a str>,
    /// Reject bodies larger than this many bytes with
    /// [`AzureError::TooLarge`] without buffering them.
    pub max_bytes: Option<u64>,
}

/// Options for [`AzureBlobClient::delete_blob`].
#[derive(Debug, Clone, Copy, Default)]
pub struct DeleteOptions<'a> {
    /// Delete this exact version instead of the current blob.
    pub version_id: Option<&'a str>,
    /// Delete only if the current ETag matches.
    pub if_match: Option<&'a str>,
    /// Permanently delete a soft-deleted version (`deletetype=permanent`).
    pub permanent: bool,
}

/// A cheaply cloneable Blob Storage client bound to one container.
#[derive(Clone)]
pub struct AzureBlobClient {
    http: reqwest::Client,
    service_url: url::Url,
    container_url: url::Url,
    authorizer: Arc<Authorizer>,
}

impl std::fmt::Debug for AzureBlobClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureBlobClient")
            .field("container_url", &self.container_url.as_str())
            .field("authorizer", &self.authorizer)
            .finish_non_exhaustive()
    }
}

struct Op {
    method: Method,
    url: url::Url,
    headers: HeaderMap,
    body: Option<Bytes>,
    idempotent: bool,
}

impl AzureBlobClient {
    /// Build a client from validated settings. No network I/O happens here;
    /// Entra credentials acquire their first token on the first request.
    pub fn new(config: &AzureBlobConfig) -> Result<Self, AzureError> {
        config.validate().map_err(AzureError::Config)?;
        let authorizer = Authorizer::new(config)?;
        let mut service_url = url::Url::parse(&config.endpoint_url())
            .map_err(|e| AzureError::Config(e.to_string()))?;
        service_url
            .path_segments_mut()
            .map_err(|()| AzureError::Config("endpoint cannot be a base URL".into()))?
            .pop_if_empty();
        let mut container_url = service_url.clone();
        container_url
            .path_segments_mut()
            .map_err(|()| AzureError::Config("endpoint cannot be a base URL".into()))?
            .push(&config.container);
        service_url
            .path_segments_mut()
            .map_err(|()| AzureError::Config("endpoint cannot be a base URL".into()))?
            .push("");
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .build()
            .map_err(|e| AzureError::Config(format!("http client: {e}")))?;
        Ok(Self {
            http,
            service_url,
            container_url,
            authorizer: Arc::new(authorizer),
        })
    }

    /// The container URL, for logs and diagnostics.
    pub fn container_url(&self) -> &str {
        self.container_url.as_str()
    }

    fn blob_url(&self, name: &str) -> Result<url::Url, AzureError> {
        if name.is_empty() {
            return Err(AzureError::Config("blob name must not be empty".into()));
        }
        let mut url = self.container_url.clone();
        url.path_segments_mut()
            .map_err(|()| AzureError::Config("endpoint cannot be a base URL".into()))?
            .extend(name.split('/'));
        Ok(url)
    }

    /// Write a block blob in one request.
    ///
    /// Conditional writes are never retried automatically: a retry after a
    /// lost response could misreport the caller's own success as a lost race.
    pub async fn put_blob(
        &self,
        name: &str,
        body: Bytes,
        content_type: &str,
        condition: PutCondition<'_>,
    ) -> Result<PutOutcome, AzureError> {
        let mut headers = HeaderMap::new();
        headers.insert(X_MS_BLOB_TYPE, HeaderValue::from_static("BlockBlob"));
        headers.insert(X_MS_BLOB_CONTENT_TYPE, header_value(content_type)?);
        match condition {
            PutCondition::None => {}
            PutCondition::IfNoneMatchAny => {
                headers.insert(IF_NONE_MATCH, HeaderValue::from_static("*"));
            }
            PutCondition::IfMatch(etag) => {
                headers.insert(IF_MATCH, header_value(etag)?);
            }
        }
        let response = self
            .send(Op {
                method: Method::PUT,
                url: self.blob_url(name)?,
                headers,
                body: Some(body),
                idempotent: condition == PutCondition::None,
            })
            .await?;
        Ok(put_outcome(response.headers()))
    }

    /// Upload a file, staging blocks for files larger than one block so
    /// memory stays bounded by the block size.
    pub async fn put_file(
        &self,
        name: &str,
        path: &Path,
        content_type: &str,
    ) -> Result<PutOutcome, AzureError> {
        let io =
            |e: std::io::Error| AzureError::Transport(format!("reading {}: {e}", path.display()));
        let mut file = tokio::fs::File::open(path).await.map_err(io)?;
        let len = file.metadata().await.map_err(io)?.len();
        if len <= BLOCK_SIZE as u64 {
            let mut buf = Vec::with_capacity(len as usize);
            file.read_to_end(&mut buf).await.map_err(io)?;
            return self
                .put_blob(name, Bytes::from(buf), content_type, PutCondition::None)
                .await;
        }

        let url = self.blob_url(name)?;
        let mut block_ids = Vec::new();
        loop {
            let mut chunk = BytesMut::with_capacity(BLOCK_SIZE);
            while chunk.len() < BLOCK_SIZE {
                if file.read_buf(&mut chunk).await.map_err(io)? == 0 {
                    break;
                }
            }
            if chunk.is_empty() {
                break;
            }
            // Block ids must share one length within a blob.
            let block_id = base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                format!("{:08}", block_ids.len()),
            );
            let mut block_url = url.clone();
            block_url
                .query_pairs_mut()
                .append_pair("comp", "block")
                .append_pair("blockid", &block_id);
            self.send(Op {
                method: Method::PUT,
                url: block_url,
                headers: HeaderMap::new(),
                body: Some(chunk.freeze()),
                idempotent: true,
            })
            .await?;
            block_ids.push(block_id);
        }

        let mut list = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>");
        for id in &block_ids {
            list.push_str("<Latest>");
            list.push_str(id);
            list.push_str("</Latest>");
        }
        list.push_str("</BlockList>");
        let mut list_url = url;
        list_url.query_pairs_mut().append_pair("comp", "blocklist");
        let mut headers = HeaderMap::new();
        headers.insert(X_MS_BLOB_CONTENT_TYPE, header_value(content_type)?);
        let response = self
            .send(Op {
                method: Method::PUT,
                url: list_url,
                headers,
                body: Some(Bytes::from(list)),
                idempotent: true,
            })
            .await?;
        Ok(put_outcome(response.headers()))
    }

    /// Read a blob (or a range / exact version of it).
    pub async fn get_blob(
        &self,
        name: &str,
        options: GetOptions<'_>,
    ) -> Result<BlobContent, AzureError> {
        let response = self.get_response(name, &options).await?;
        let etag = header_string(response.headers(), &ETAG);
        let bytes = read_body(response, options.max_bytes).await?;
        Ok(BlobContent { bytes, etag })
    }

    /// Stream a blob's body without buffering it.
    pub async fn get_blob_stream(&self, name: &str) -> Result<BlobStream, AzureError> {
        let response = self.get_response(name, &GetOptions::default()).await?;
        Ok(Box::pin(
            response
                .bytes_stream()
                .map(|chunk| chunk.map_err(AzureError::transport)),
        ))
    }

    async fn get_response(
        &self,
        name: &str,
        options: &GetOptions<'_>,
    ) -> Result<reqwest::Response, AzureError> {
        let mut url = self.blob_url(name)?;
        if let Some(version) = options.version_id {
            url.query_pairs_mut().append_pair("versionid", version);
        }
        let mut headers = HeaderMap::new();
        if let Some((start, end)) = options.range {
            let range = match end {
                Some(end) if end < start => {
                    return Err(AzureError::Config(format!("invalid range {start}-{end}")))
                }
                Some(end) => format!("bytes={start}-{end}"),
                None => format!("bytes={start}-"),
            };
            headers.insert(X_MS_RANGE, header_value(&range)?);
        }
        self.send(Op {
            method: Method::GET,
            url,
            headers,
            body: None,
            idempotent: true,
        })
        .await
    }

    /// Read blob properties; `Ok(None)` when the blob (or version) is absent.
    pub async fn head_blob(
        &self,
        name: &str,
        version_id: Option<&str>,
    ) -> Result<Option<BlobProperties>, AzureError> {
        let mut url = self.blob_url(name)?;
        if let Some(version) = version_id {
            url.query_pairs_mut().append_pair("versionid", version);
        }
        let response = match self
            .send(Op {
                method: Method::HEAD,
                url,
                headers: HeaderMap::new(),
                body: None,
                idempotent: true,
            })
            .await
        {
            Ok(response) => response,
            Err(error) if error.is_not_found() => return Ok(None),
            Err(error) => return Err(error),
        };
        let headers = response.headers();
        let size = header_string(headers, &CONTENT_LENGTH)
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| AzureError::Parse("blob properties missing Content-Length".into()))?;
        Ok(Some(BlobProperties {
            size,
            etag: header_string(headers, &ETAG),
            version_id: header_string(headers, &X_MS_VERSION_ID),
        }))
    }

    /// Delete a blob or one exact version of it.
    ///
    /// Errors keep their status: callers decide whether 404 means
    /// already-deleted.
    pub async fn delete_blob(
        &self,
        name: &str,
        options: DeleteOptions<'_>,
    ) -> Result<DeleteOutcome, AzureError> {
        let mut url = self.blob_url(name)?;
        if let Some(version) = options.version_id {
            url.query_pairs_mut().append_pair("versionid", version);
        }
        if options.permanent {
            url.query_pairs_mut().append_pair("deletetype", "permanent");
        }
        let mut headers = HeaderMap::new();
        if let Some(etag) = options.if_match {
            headers.insert(IF_MATCH, header_value(etag)?);
        }
        let response = self
            .send(Op {
                method: Method::DELETE,
                url,
                headers,
                body: None,
                // A retried conditional delete could misreport its own
                // success; exact-version and plain deletes converge.
                idempotent: options.if_match.is_none(),
            })
            .await?;
        let permanent =
            match header_string(response.headers(), &X_MS_DELETE_TYPE_PERMANENT).as_deref() {
                Some("true") => Some(true),
                Some("false") => Some(false),
                _ => None,
            };
        Ok(DeleteOutcome { permanent })
    }

    /// One page of a flat `List Blobs` listing.
    pub async fn list_blobs(
        &self,
        prefix: &str,
        marker: Option<&str>,
        max_results: usize,
        include_versions: bool,
    ) -> Result<ListBlobsPage, AzureError> {
        let mut url = self.container_url.clone();
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("restype", "container")
                .append_pair("comp", "list")
                .append_pair("maxresults", &max_results.clamp(1, 5000).to_string());
            if !prefix.is_empty() {
                query.append_pair("prefix", prefix);
            }
            if let Some(marker) = marker {
                query.append_pair("marker", marker);
            }
            if include_versions {
                query.append_pair("include", "versions");
            }
        }
        let response = self
            .send(Op {
                method: Method::GET,
                url,
                headers: HeaderMap::new(),
                body: None,
                idempotent: true,
            })
            .await?;
        let body = read_body(response, Some(XML_BODY_LIMIT)).await?;
        parse_list_blobs(&body)
    }

    /// Read the account's blob soft-delete policy.
    pub async fn get_delete_retention(&self) -> Result<DeleteRetention, AzureError> {
        let mut url = self.service_url.clone();
        url.query_pairs_mut()
            .append_pair("restype", "service")
            .append_pair("comp", "properties");
        let response = self
            .send(Op {
                method: Method::GET,
                url,
                headers: HeaderMap::new(),
                body: None,
                idempotent: true,
            })
            .await?;
        let body = read_body(response, Some(XML_BODY_LIMIT)).await?;
        parse_delete_retention(&body)
    }

    /// Create the container if it does not exist. Intended for local
    /// development and tests; production containers are provisioned by the
    /// operator.
    pub async fn create_container_if_missing(&self) -> Result<(), AzureError> {
        let mut url = self.container_url.clone();
        url.query_pairs_mut().append_pair("restype", "container");
        match self
            .send(Op {
                method: Method::PUT,
                url,
                headers: HeaderMap::new(),
                body: None,
                idempotent: true,
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if error.code() == Some("ContainerAlreadyExists") => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn send(&self, op: Op) -> Result<reqwest::Response, AzureError> {
        let mut attempt = 0u32;
        loop {
            let mut request = reqwest::Request::new(op.method.clone(), op.url.clone());
            let headers = request.headers_mut();
            *headers = op.headers.clone();
            headers.insert(X_MS_VERSION, HeaderValue::from_static(API_VERSION));
            headers.insert(
                X_MS_DATE,
                header_value(&httpdate::fmt_http_date(SystemTime::now()))?,
            );
            match &op.body {
                Some(body) => {
                    if body.is_empty() {
                        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
                    }
                    *request.body_mut() = Some(reqwest::Body::from(body.clone()));
                }
                None if op.method == Method::PUT => {
                    headers.insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
                }
                None => {}
            }
            self.authorizer.authorize(&mut request).await?;

            let (error, retry_after) = match self.http.execute(request).await {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) => {
                    let retry_after = retry_after(response.headers());
                    (status_error(response).await, retry_after)
                }
                Err(error) => (AzureError::transport(error), None),
            };
            attempt += 1;
            if !op.idempotent || attempt >= MAX_ATTEMPTS || !error.is_retryable() {
                return Err(error);
            }
            let delay = backoff(attempt, retry_after);
            tracing::debug!(
                method = %op.method,
                attempt,
                delay_ms = delay.as_millis() as u64,
                error = %error,
                "retrying azure blob request"
            );
            tokio::time::sleep(delay).await;
        }
    }
}

fn header_value(value: &str) -> Result<HeaderValue, AzureError> {
    HeaderValue::from_str(value)
        .map_err(|_| AzureError::Config(format!("invalid header value {value:?}")))
}

fn header_string(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn put_outcome(headers: &HeaderMap) -> PutOutcome {
    PutOutcome {
        etag: header_string(headers, &ETAG),
        version_id: header_string(headers, &X_MS_VERSION_ID),
    }
}

fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    header_string(headers, &RETRY_AFTER)
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

/// Full-jitter exponential backoff, capped; a server `Retry-After` is honored
/// up to the same cap.
fn backoff(attempt: u32, retry_after: Option<Duration>) -> Duration {
    let ceiling = BACKOFF_BASE
        .saturating_mul(1u32 << attempt.saturating_sub(1).min(16))
        .min(BACKOFF_CAP);
    let jittered = ceiling.mul_f64(rand::random::<f64>());
    match retry_after {
        Some(server) => server.min(BACKOFF_CAP).max(jittered),
        None => jittered,
    }
}

async fn status_error(response: reqwest::Response) -> AzureError {
    let status = response.status().as_u16();
    let code = header_string(response.headers(), &X_MS_ERROR_CODE);
    let message = match read_body(response, Some(ERROR_BODY_LIMIT)).await {
        Ok(body) => error_message(&body),
        Err(_) => String::new(),
    };
    let message = if message.is_empty() {
        StatusCode::from_u16(status)
            .ok()
            .and_then(|s| s.canonical_reason())
            .unwrap_or("request failed")
            .to_owned()
    } else {
        message
    };
    AzureError::Status {
        status,
        code,
        message,
    }
}

/// Extract `<Message>` from an Azure error body, falling back to the raw
/// (bounded) text.
fn error_message(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    match (text.find("<Message>"), text.find("</Message>")) {
        (Some(start), Some(end)) if start + 9 <= end => {
            text[start + 9..end].lines().next().unwrap_or("").to_owned()
        }
        _ => text.trim().to_owned(),
    }
}

/// Read a response body, rejecting it once it exceeds `max` bytes.
async fn read_body(mut response: reqwest::Response, max: Option<u64>) -> Result<Bytes, AzureError> {
    let declared = response.content_length();
    if let (Some(max), Some(size)) = (max, declared) {
        if size > max {
            return Err(AzureError::TooLarge { size, max });
        }
    }
    let mut body = BytesMut::with_capacity(
        declared
            .unwrap_or(0)
            .min(max.unwrap_or(u64::MAX))
            .min(BLOCK_SIZE as u64) as usize,
    );
    while let Some(chunk) = response.chunk().await.map_err(AzureError::transport)? {
        if let Some(max) = max {
            let size = (body.len() + chunk.len()) as u64;
            if size > max {
                return Err(AzureError::TooLarge { size, max });
            }
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_capped_and_honors_retry_after() {
        for attempt in 1..10 {
            assert!(backoff(attempt, None) <= BACKOFF_CAP);
        }
        assert_eq!(
            backoff(1, Some(Duration::from_secs(60))),
            BACKOFF_CAP,
            "server Retry-After is capped"
        );
        assert!(backoff(1, Some(Duration::from_secs(1))) >= Duration::from_secs(1));
    }

    #[test]
    fn error_message_prefers_message_element() {
        let body = b"<?xml version=\"1.0\"?><Error><Code>BlobNotFound</Code>\
            <Message>The specified blob does not exist.\nRequestId:1</Message></Error>";
        assert_eq!(error_message(body), "The specified blob does not exist.");
        assert_eq!(error_message(b"  plain  "), "plain");
    }

    #[test]
    fn urls_cover_path_style_and_nested_names() {
        let config = AzureBlobConfig {
            account: "devstoreaccount1".into(),
            container: "media".into(),
            endpoint: Some("http://127.0.0.1:10000/devstoreaccount1/".into()),
            auth: super::super::AzureAuthMode::SharedKey,
            account_key: Some("a2V5".into()),
            client_id: None,
        };
        let client = AzureBlobClient::new(&config).expect("client");
        assert_eq!(
            client.container_url(),
            "http://127.0.0.1:10000/devstoreaccount1/media"
        );
        assert_eq!(
            client.service_url.as_str(),
            "http://127.0.0.1:10000/devstoreaccount1/"
        );
        assert_eq!(
            client.blob_url("_meta/c 1/a.json").expect("url").as_str(),
            "http://127.0.0.1:10000/devstoreaccount1/media/_meta/c%201/a.json"
        );
        assert!(client.blob_url("").is_err());

        let default = AzureBlobClient::new(&AzureBlobConfig {
            account: "acct".into(),
            endpoint: None,
            ..config
        })
        .expect("default endpoint");
        assert_eq!(
            default.container_url(),
            "https://acct.blob.core.windows.net/media"
        );
        assert_eq!(
            default.service_url.as_str(),
            "https://acct.blob.core.windows.net/"
        );
    }
}
