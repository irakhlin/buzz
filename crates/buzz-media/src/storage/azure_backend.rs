//! Azure Blob Storage arms of [`super::MediaStorage`].
//!
//! S3 semantics the rest of Buzz relies on, mapped onto Blob Storage:
//!
//! - Missing objects read as [`MediaError::NotFound`]; deletes of missing
//!   objects succeed, like S3 `DeleteObject`.
//! - Version listings reuse the S3 two-marker cursor: Azure's single opaque
//!   `NextMarker` travels in the version-id marker slot and the last listed
//!   name in the key marker slot, so deletion's cursor checks hold unchanged.
//! - Unversioned blobs carry the S3 `"null"` version id.
//! - Exact-version deletion is permanent. Blob soft delete would keep the
//!   bytes recoverable, so deletion fails closed unless every removal can be
//!   made permanent.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::StreamExt;

use super::{
    BlobHeadMeta, BulkDeleteOutcome, ByteStream, ObjectVersionEntry, ObjectVersionKind,
    ObjectVersionRef, ObjectVersionsPage,
};
use crate::azure::{
    AzureBlobClient, AzureError, DeleteOptions, DeleteRetention, GetOptions, PutCondition,
};
use crate::error::MediaError;

/// Version id S3 reports for objects written without versioning.
const NULL_VERSION: &str = "null";
/// Concurrent deletes per bulk call (S3 `DeleteObjects` takes 1000 keys in
/// one request; Blob Storage needs one request per blob).
const DELETE_CONCURRENCY: usize = 16;
/// How long a soft-delete policy read is trusted by bulk deletes.
const RETENTION_TTL: Duration = Duration::from_secs(60);
/// Error code Blob Storage returns when a version delete targets the
/// blob's current version.
const ROOT_BLOB_CODE: &str = "OperationNotAllowedOnRootBlob";

/// Azure container plus the cached soft-delete policy that deletion needs.
pub(super) struct AzureMedia {
    client: AzureBlobClient,
    retention: Mutex<Option<(Instant, DeleteRetention)>>,
}

enum Removed {
    Deleted,
    AlreadyMissing,
}

struct Failure {
    code: String,
    message: String,
}

impl From<AzureError> for Failure {
    fn from(error: AzureError) -> Self {
        Self {
            code: error.code().unwrap_or("AzureError").to_owned(),
            message: error.to_string(),
        }
    }
}

fn soft_delete_failure(message: &str) -> Failure {
    Failure {
        code: "SoftDeleteEnabled".to_owned(),
        message: message.to_owned(),
    }
}

/// Reads that S3 reports as 404 become [`MediaError::NotFound`].
fn read_error(error: AzureError) -> MediaError {
    if error.is_not_found() {
        MediaError::NotFound
    } else {
        error.into()
    }
}

impl AzureMedia {
    pub(super) fn new(client: AzureBlobClient) -> Self {
        Self {
            client,
            retention: Mutex::new(None),
        }
    }

    pub(super) async fn put(
        &self,
        key: &str,
        bytes: &[u8],
        content_type: &str,
    ) -> Result<(), MediaError> {
        self.client
            .put_blob(
                key,
                Bytes::copy_from_slice(bytes),
                content_type,
                PutCondition::None,
            )
            .await?;
        Ok(())
    }

    pub(super) async fn put_file(
        &self,
        key: &str,
        path: &std::path::Path,
        content_type: &str,
    ) -> Result<(), MediaError> {
        self.client.put_file(key, path, content_type).await?;
        Ok(())
    }

    pub(super) async fn get(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Vec<u8>, MediaError> {
        let content = self
            .client
            .get_blob(
                key,
                GetOptions {
                    range: range.map(|(start, end)| (start, Some(end))),
                    ..GetOptions::default()
                },
            )
            .await
            .map_err(read_error)?;
        Ok(content.bytes.to_vec())
    }

    pub(super) async fn get_stream(&self, key: &str) -> Result<ByteStream, MediaError> {
        let stream = self.client.get_blob_stream(key).await.map_err(read_error)?;
        Ok(Box::pin(
            stream.map(|chunk| chunk.map_err(MediaError::from)),
        ))
    }

    pub(super) async fn head(&self, key: &str) -> Result<Option<BlobHeadMeta>, MediaError> {
        Ok(self
            .client
            .head_blob(key, None)
            .await?
            .map(|properties| BlobHeadMeta {
                size: properties.size,
            }))
    }

    pub(super) async fn delete(&self, key: &str) -> Result<(), MediaError> {
        match self.client.delete_blob(key, DeleteOptions::default()).await {
            Ok(_) => Ok(()),
            Err(error) if error.is_not_found() => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) async fn list_page(
        &self,
        prefix: &str,
        continuation_token: Option<String>,
        max_keys: usize,
    ) -> Result<crate::bucket_index::Page, MediaError> {
        let page = self
            .client
            .list_blobs(prefix, continuation_token.as_deref(), max_keys, false)
            .await?;
        Ok(crate::bucket_index::Page {
            objects: page
                .blobs
                .into_iter()
                .map(|blob| (blob.name, blob.size))
                .collect(),
            is_truncated: page.next_marker.is_some(),
            next_continuation_token: page.next_marker,
        })
    }

    pub(super) async fn list_versions_page(
        &self,
        prefix: &str,
        key_marker: Option<String>,
        version_id_marker: Option<String>,
        max_keys: usize,
    ) -> Result<ObjectVersionsPage, MediaError> {
        if key_marker.is_some() && version_id_marker.is_none() {
            return Err(MediaError::StorageError(
                "azure version listing resumes from the version-id marker; a key marker alone \
                 cannot be resumed"
                    .to_owned(),
            ));
        }
        let page = self
            .client
            .list_blobs(prefix, version_id_marker.as_deref(), max_keys, true)
            .await?;
        let is_truncated = page.next_marker.is_some();
        let next_key_marker = is_truncated.then(|| {
            page.blobs
                .last()
                .map(|blob| blob.name.clone())
                .or(key_marker)
                .unwrap_or_else(|| prefix.to_owned())
        });
        Ok(ObjectVersionsPage {
            entries: page
                .blobs
                .into_iter()
                .map(|blob| ObjectVersionEntry {
                    key: blob.name,
                    version_id: blob.version_id.unwrap_or_else(|| NULL_VERSION.to_owned()),
                    kind: ObjectVersionKind::Object,
                    size: blob.size,
                })
                .collect(),
            next_key_marker,
            next_version_id_marker: page.next_marker,
            is_truncated,
        })
    }

    pub(super) async fn get_small_version(
        &self,
        key: &str,
        version_id: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, MediaError> {
        let content = self
            .client
            .get_blob(
                key,
                GetOptions {
                    range: Some((0, Some(max_bytes as u64))),
                    version_id: (version_id != NULL_VERSION).then_some(version_id),
                    max_bytes: Some(max_bytes as u64 + 1),
                },
            )
            .await
            .map_err(|error| MediaError::StorageError(format!("version read failed: {error}")))?;
        if content.bytes.len() > max_bytes {
            return Err(MediaError::StorageError(
                "upload record exceeds size bound".into(),
            ));
        }
        Ok(content.bytes.to_vec())
    }

    /// Prove version listing works and that the account's soft-delete
    /// policy allows permanent removal, before deletion fences writers.
    pub(super) async fn preflight_deletion(&self, prefix: &str) -> Result<(), MediaError> {
        self.list_versions_page(prefix, None, None, 1).await?;
        let retention = self.client.get_delete_retention().await.map_err(|error| {
            MediaError::StorageError(format!(
                "cannot read the storage account's blob soft-delete policy ({error}); with \
                 Microsoft Entra ID the identity needs \
                 Microsoft.Storage/storageAccounts/blobServices/read (for example the Reader \
                 role on the storage account)"
            ))
        })?;
        self.remember_retention(retention);
        if retention.enabled && !retention.allow_permanent_delete {
            return Err(MediaError::StorageError(
                "blob soft delete is enabled on the storage account without permanent delete; \
                 deleted data would stay recoverable. Disable blob soft delete, or enable \
                 versioning and permanent delete (AllowPermanentDelete)"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    /// Delete current blobs by name, as S3 `DeleteObjects` without versions.
    pub(super) async fn delete_objects(&self, keys: &[String]) -> BulkDeleteOutcome {
        let versions = keys
            .iter()
            .map(|key| ObjectVersionRef {
                key: key.clone(),
                version_id: NULL_VERSION.to_owned(),
            })
            .collect::<Vec<_>>();
        self.delete_object_versions(&versions).await
    }

    /// Permanently delete exact versions. Per-version results fold into
    /// [`BulkDeleteOutcome`]; nothing here returns early on one failure.
    pub(super) async fn delete_object_versions(
        &self,
        versions: &[ObjectVersionRef],
    ) -> BulkDeleteOutcome {
        let mut outcome = BulkDeleteOutcome::default();
        if versions.is_empty() {
            return outcome;
        }
        let retention = match self.retention().await {
            Ok(retention) => retention,
            Err(failure) => {
                for version in versions {
                    outcome.failed.push((
                        version.key.clone(),
                        failure.code.clone(),
                        failure.message.clone(),
                    ));
                }
                return outcome;
            }
        };
        // Owned futures collected up front: a borrowing `map` closure held
        // inside the stream makes callers' futures fail higher-ranked `Send`.
        let removals = versions
            .iter()
            .map(|version| {
                let key = version.key.clone();
                let version_id = version.version_id.clone();
                async move {
                    let result = self.remove_version(&key, &version_id, retention).await;
                    (key, result)
                }
            })
            .collect::<Vec<_>>();
        let results = futures_util::stream::iter(removals)
            .buffer_unordered(DELETE_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        for (key, result) in results {
            match result {
                Ok(Removed::Deleted) => outcome.deleted += 1,
                Ok(Removed::AlreadyMissing) => outcome.already_missing += 1,
                Err(failure) => outcome.failed.push((key, failure.code, failure.message)),
            }
        }
        outcome
    }

    async fn retention(&self) -> Result<DeleteRetention, Failure> {
        if let Ok(guard) = self.retention.lock() {
            if let Some((read_at, retention)) = *guard {
                if read_at.elapsed() < RETENTION_TTL {
                    return Ok(retention);
                }
            }
        }
        let retention = self.client.get_delete_retention().await?;
        self.remember_retention(retention);
        Ok(retention)
    }

    fn remember_retention(&self, retention: DeleteRetention) {
        if let Ok(mut guard) = self.retention.lock() {
            *guard = Some((Instant::now(), retention));
        }
    }

    async fn remove_version(
        &self,
        key: &str,
        version_id: &str,
        retention: DeleteRetention,
    ) -> Result<Removed, Failure> {
        if retention.enabled && !retention.allow_permanent_delete {
            return Err(soft_delete_failure(
                "blob soft delete is enabled without permanent delete; refusing a delete that \
                 would stay recoverable",
            ));
        }
        if version_id == NULL_VERSION {
            if retention.enabled {
                // Soft-deleted base blobs cannot be purged, and a retry would
                // then see 404 and report the retained bytes as gone.
                return Err(soft_delete_failure(
                    "blob soft delete is enabled and this blob has no version id; it cannot \
                     be permanently deleted",
                ));
            }
            let removed = match self.client.delete_blob(key, DeleteOptions::default()).await {
                Ok(outcome) if outcome.permanent == Some(false) => {
                    return Err(soft_delete_failure(
                        "the service soft-deleted the blob instead of removing it",
                    ))
                }
                Ok(_) => Removed::Deleted,
                Err(error) if error.is_not_found() => Removed::AlreadyMissing,
                Err(error) => return Err(error.into()),
            };
            // On a versioned account, deleting a blob written before
            // versioning was enabled keeps its bytes as a new previous
            // version. Sweep even after a 404 so a retry finishes the job.
            self.remove_previous_versions(key, retention).await?;
            return Ok(removed);
        }
        match self.remove_exact_version(key, version_id, retention).await {
            Err(error) if error.code() == Some(ROOT_BLOB_CODE) => {
                self.demote_current_version(key, version_id).await?;
                Ok(self
                    .remove_exact_version(key, version_id, retention)
                    .await?)
            }
            result => Ok(result?),
        }
    }

    async fn remove_exact_version(
        &self,
        key: &str,
        version_id: &str,
        retention: DeleteRetention,
    ) -> Result<Removed, AzureError> {
        let exact = DeleteOptions {
            version_id: Some(version_id),
            ..DeleteOptions::default()
        };
        let permanent = match self.client.delete_blob(key, exact).await {
            Ok(outcome) => outcome.permanent,
            Err(error) if error.is_not_found() && !retention.enabled => {
                return Ok(Removed::AlreadyMissing)
            }
            // Under soft delete, 404 may mean an earlier attempt left the
            // version soft-deleted; purge it rather than trusting absence.
            Err(error) if error.is_not_found() => return self.purge(key, version_id).await,
            Err(error) => return Err(error),
        };
        let soft_deleted = permanent == Some(false) || (permanent.is_none() && retention.enabled);
        if soft_deleted {
            self.purge(key, version_id).await?;
        }
        Ok(Removed::Deleted)
    }

    /// Permanently delete every previous version of exactly `key`. A current
    /// version is a concurrent rewrite and stays.
    async fn remove_previous_versions(
        &self,
        key: &str,
        retention: DeleteRetention,
    ) -> Result<(), AzureError> {
        let mut marker = None;
        loop {
            let page = self
                .client
                .list_blobs(key, marker.as_deref(), 1000, true)
                .await?;
            // Names sort after their own prefix, so `key`'s versions come
            // first and any longer name ends the sweep.
            let mut past_key = false;
            for blob in page.blobs {
                if blob.name != key {
                    past_key = true;
                    break;
                }
                if blob.is_current_version {
                    continue;
                }
                if let Some(version_id) = blob.version_id {
                    self.remove_exact_version(key, &version_id, retention)
                        .await?;
                }
            }
            match page.next_marker {
                Some(next) if !past_key => marker = Some(next),
                _ => return Ok(()),
            }
        }
    }

    async fn purge(&self, key: &str, version_id: &str) -> Result<Removed, AzureError> {
        let options = DeleteOptions {
            version_id: Some(version_id),
            permanent: true,
            ..DeleteOptions::default()
        };
        match self.client.delete_blob(key, options).await {
            Ok(outcome) if outcome.permanent == Some(false) => Err(AzureError::Status {
                status: 409,
                code: Some("SoftDeleteRetained".to_owned()),
                message: "permanent delete left the version soft-deleted".to_owned(),
            }),
            Ok(_) => Ok(Removed::Deleted),
            Err(error) if error.is_not_found() => Ok(Removed::AlreadyMissing),
            Err(error) => Err(error),
        }
    }

    /// Turn `version_id` from the current version into a previous version by
    /// deleting the base blob, but only while it is still current: the
    /// ETag condition keeps a concurrent write from being deleted instead.
    async fn demote_current_version(&self, key: &str, version_id: &str) -> Result<(), Failure> {
        let Some(current) = self.client.head_blob(key, None).await? else {
            return Ok(());
        };
        if current.version_id.as_deref() != Some(version_id) {
            return Ok(());
        }
        let etag = current.etag.ok_or_else(|| Failure {
            code: "MissingETag".to_owned(),
            message: "blob properties had no ETag".to_owned(),
        })?;
        let options = DeleteOptions {
            if_match: Some(&etag),
            ..DeleteOptions::default()
        };
        match self.client.delete_blob(key, options).await {
            Ok(_) => Ok(()),
            Err(error) if error.is_not_found() => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::azure::{AzureAuthMode, AzureBlobConfig};

    const LISTING: &str = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
        <EnumerationResults><Blobs>\
        <Blob><Name>k</Name><VersionId>2024-01-01T00:00:00.0000000Z</VersionId>\
          <Properties><Content-Length>3</Content-Length></Properties></Blob>\
        <Blob><Name>k</Name><VersionId>2024-01-02T00:00:00.0000000Z</VersionId>\
          <IsCurrentVersion>true</IsCurrentVersion>\
          <Properties><Content-Length>3</Content-Length></Properties></Blob>\
        <Blob><Name>k.thumb</Name><VersionId>2024-01-03T00:00:00.0000000Z</VersionId>\
          <Properties><Content-Length>3</Content-Length></Properties></Blob>\
        </Blobs><NextMarker>more</NextMarker></EnumerationResults>";

    /// Serve `LISTING` for list calls and accept every delete permanently,
    /// recording each request as `METHOD target`.
    async fn fake_blob_service() -> (AzureMedia, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&requests);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut head = Vec::new();
                let mut buf = [0u8; 1024];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                }
                let head = String::from_utf8_lossy(&head);
                let line = head.lines().next().unwrap_or_default();
                let request = line.rsplit_once(' ').map_or(line, |(r, _)| r).to_owned();
                let response = if request.starts_with("GET ") {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        LISTING.len(),
                        LISTING
                    )
                } else {
                    "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\
                     x-ms-delete-type-permanent: true\r\nConnection: close\r\n\r\n"
                        .to_owned()
                };
                log.lock().expect("log").push(request);
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        let config = AzureBlobConfig {
            account: "devstoreaccount1".into(),
            container: "media".into(),
            endpoint: Some(format!("http://127.0.0.1:{port}/devstoreaccount1")),
            auth: AzureAuthMode::SharedKey,
            account_key: Some("a2V5".into()),
            client_id: None,
        };
        let client = AzureBlobClient::new(&config).expect("client");
        (AzureMedia::new(client), requests)
    }

    #[tokio::test]
    async fn null_version_delete_sweeps_the_version_it_leaves_behind() {
        let (media, requests) = fake_blob_service().await;

        let removed = media
            .remove_version("k", NULL_VERSION, DeleteRetention::default())
            .await;

        assert!(matches!(removed, Ok(Removed::Deleted)));
        // Only the previous version of exactly `k` is deleted: the current
        // version (a concurrent rewrite) and `k.thumb` stay, and the sweep
        // stops paging once the listing passes `k`.
        assert_eq!(
            *requests.lock().expect("log"),
            vec![
                "DELETE /devstoreaccount1/media/k".to_owned(),
                "GET /devstoreaccount1/media?restype=container&comp=list&maxresults=1000\
                 &prefix=k&include=versions"
                    .to_owned(),
                "DELETE /devstoreaccount1/media/k?versionid=2024-01-01T00%3A00%3A00.0000000Z"
                    .to_owned(),
            ]
        );
    }
}
