//! Live Azure Blob Storage coverage against Azurite.
//!
//! Exercises the Shared Key client and the `MediaStorage` Azure backend on the
//! wire: conditional writes, block uploads, ranged/bounded reads, versioned
//! listing, and exact-version deletion including the current version.
//!
//! ```bash
//! docker compose --profile azurite up -d azurite
//! cargo test -p buzz-media --test azurite_blob -- --ignored --nocapture
//! ```
//!
//! Azurite must run with blob versioning enabled
//! (`--accountConfigAsJson '{"isBlobVersioningEnabled":true}'`). The endpoint
//! is overridable with `BUZZ_AZURE_STORAGE_ENDPOINT`. Each test creates its
//! own container.

use bytes::Bytes;
use futures_util::StreamExt;

use buzz_media::azure::{
    AzureAuthMode, AzureBlobClient, AzureBlobConfig, AzureError, DeleteOptions, GetOptions,
    PutCondition,
};

// Azurite's documented public development account key.
const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

fn azurite_config(container: &str) -> AzureBlobConfig {
    AzureBlobConfig {
        account: "devstoreaccount1".into(),
        container: container.into(),
        endpoint: Some(
            std::env::var("BUZZ_AZURE_STORAGE_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:10000/devstoreaccount1".into()),
        ),
        auth: AzureAuthMode::SharedKey,
        account_key: Some(AZURITE_KEY.into()),
        client_id: None,
    }
}

fn unique_container(tag: &str) -> String {
    format!("buzz-{tag}-{}", uuid::Uuid::new_v4().simple())
        .chars()
        .take(63)
        .collect()
}

async fn client(tag: &str) -> AzureBlobClient {
    let client = AzureBlobClient::new(&azurite_config(&unique_container(tag))).expect("client");
    client
        .create_container_if_missing()
        .await
        .expect("create container (is Azurite running?)");
    client
}

#[tokio::test]
#[ignore = "requires Azurite on 127.0.0.1:10000"]
async fn client_round_trips_conditional_writes_and_reads() {
    let client = client("crud").await;
    let name = "community/abc/media/aa/bb/hash file.bin";

    let first = client
        .put_blob(
            name,
            Bytes::from_static(b"hello world"),
            "application/octet-stream",
            PutCondition::IfNoneMatchAny,
        )
        .await
        .expect("create");
    let etag = first.etag.clone().expect("etag on create");

    let err = client
        .put_blob(
            name,
            Bytes::from_static(b"other"),
            "application/octet-stream",
            PutCondition::IfNoneMatchAny,
        )
        .await
        .expect_err("second create-only write must lose");
    assert!(err.is_precondition_failed(), "{err:?}");

    let swapped = client
        .put_blob(
            name,
            Bytes::from_static(b"hello azure"),
            "text/plain",
            PutCondition::IfMatch(&etag),
        )
        .await
        .expect("cas with current etag");
    let err = client
        .put_blob(
            name,
            Bytes::from_static(b"stale"),
            "text/plain",
            PutCondition::IfMatch(&etag),
        )
        .await
        .expect_err("stale etag must lose");
    assert!(err.is_precondition_failed(), "{err:?}");

    let err = client
        .put_blob(
            "missing/pointer",
            Bytes::from_static(b"x"),
            "text/plain",
            PutCondition::IfMatch(&etag),
        )
        .await
        .expect_err("if-match on a missing blob must lose");
    assert!(
        err.is_precondition_failed() || err.is_not_found(),
        "{err:?}"
    );

    let full = client
        .get_blob(name, GetOptions::default())
        .await
        .expect("get");
    assert_eq!(&full.bytes[..], b"hello azure");
    assert_eq!(full.etag, swapped.etag);

    let range = client
        .get_blob(
            name,
            GetOptions {
                range: Some((6, Some(10))),
                ..GetOptions::default()
            },
        )
        .await
        .expect("range");
    assert_eq!(&range.bytes[..], b"azure");

    let err = client
        .get_blob(
            name,
            GetOptions {
                max_bytes: Some(4),
                ..GetOptions::default()
            },
        )
        .await
        .expect_err("bounded read");
    assert!(matches!(err, AzureError::TooLarge { .. }), "{err:?}");

    let mut stream = client.get_blob_stream(name).await.expect("stream");
    let mut streamed = Vec::new();
    while let Some(chunk) = stream.next().await {
        streamed.extend_from_slice(&chunk.expect("chunk"));
    }
    assert_eq!(streamed, b"hello azure");

    let props = client
        .head_blob(name, None)
        .await
        .expect("head")
        .expect("present");
    assert_eq!(props.size, 11);
    assert!(client
        .head_blob("nope", None)
        .await
        .expect("head")
        .is_none());
    let err = client
        .get_blob("nope", GetOptions::default())
        .await
        .expect_err("missing");
    assert!(err.is_not_found(), "{err:?}");

    let empty = client
        .put_blob("empty", Bytes::new(), "text/plain", PutCondition::None)
        .await;
    assert!(empty.is_ok(), "{empty:?}");
    assert_eq!(
        client
            .head_blob("empty", None)
            .await
            .expect("head")
            .expect("present")
            .size,
        0
    );
}

#[tokio::test]
#[ignore = "requires Azurite on 127.0.0.1:10000"]
async fn client_stages_blocks_for_large_files() {
    let client = client("blocks").await;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("large.bin");
    let len = 8 * 1024 * 1024 * 2 + 12345;
    let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    std::fs::write(&path, &data).expect("write");

    client
        .put_file("large/blob", &path, "video/mp4")
        .await
        .expect("block upload");
    let props = client
        .head_blob("large/blob", None)
        .await
        .expect("head")
        .expect("present");
    assert_eq!(props.size, len as u64);
    let tail = client
        .get_blob(
            "large/blob",
            GetOptions {
                range: Some((len as u64 - 10, None)),
                ..GetOptions::default()
            },
        )
        .await
        .expect("tail range");
    assert_eq!(&tail.bytes[..], &data[len - 10..]);
}

#[tokio::test]
#[ignore = "requires Azurite on 127.0.0.1:10000"]
async fn client_lists_and_deletes_exact_versions() {
    let client = client("versions").await;
    for body in ["v1", "v2", "v3"] {
        client
            .put_blob("p/a", Bytes::from(body), "text/plain", PutCondition::None)
            .await
            .expect("put version");
    }
    client
        .put_blob(
            "p/b",
            Bytes::from_static(b"b"),
            "text/plain",
            PutCondition::None,
        )
        .await
        .expect("put b");
    client
        .put_blob(
            "q/c",
            Bytes::from_static(b"c"),
            "text/plain",
            PutCondition::None,
        )
        .await
        .expect("put c");

    let mut all = Vec::new();
    let mut marker = None;
    loop {
        let page = client
            .list_blobs("p/", marker.as_deref(), 2, true)
            .await
            .expect("list versions");
        all.extend(page.blobs);
        marker = page.next_marker;
        if marker.is_none() {
            break;
        }
    }
    let a_versions: Vec<_> = all.iter().filter(|b| b.name == "p/a").collect();
    assert_eq!(a_versions.len(), 3, "{all:?}");
    assert_eq!(all.iter().filter(|b| b.name == "p/b").count(), 1);
    assert!(all.iter().all(|b| b.name.starts_with("p/")));
    assert_eq!(
        a_versions.iter().filter(|b| b.is_current_version).count(),
        1
    );

    let unversioned = client
        .list_blobs("p/", None, 100, false)
        .await
        .expect("list current");
    assert_eq!(unversioned.blobs.len(), 2);

    let old = a_versions
        .iter()
        .find(|b| !b.is_current_version)
        .and_then(|b| b.version_id.clone())
        .expect("historical version");
    let current = a_versions
        .iter()
        .find(|b| b.is_current_version)
        .and_then(|b| b.version_id.clone())
        .expect("current version");

    client
        .delete_blob(
            "p/a",
            DeleteOptions {
                version_id: Some(&old),
                ..DeleteOptions::default()
            },
        )
        .await
        .expect("delete historical version");
    let err = client
        .delete_blob(
            "p/a",
            DeleteOptions {
                version_id: Some(&old),
                ..DeleteOptions::default()
            },
        )
        .await
        .expect_err("already deleted");
    assert!(err.is_not_found(), "{err:?}");

    let current_delete = client
        .delete_blob(
            "p/a",
            DeleteOptions {
                version_id: Some(&current),
                ..DeleteOptions::default()
            },
        )
        .await;
    eprintln!("delete current version directly: {current_delete:?}");

    let retention = client.get_delete_retention().await;
    eprintln!("delete retention: {retention:?}");
}

async fn media_storage(tag: &str) -> buzz_media::MediaStorage {
    let azure = azurite_config(&unique_container(tag));
    AzureBlobClient::new(&azure)
        .expect("client")
        .create_container_if_missing()
        .await
        .expect("create container (is Azurite running?)");
    buzz_media::MediaStorage::new(&buzz_media::MediaConfig {
        s3_endpoint: String::new(),
        s3_access_key: String::new(),
        s3_secret_key: String::new(),
        s3_bucket: String::new(),
        s3_region: "us-east-1".into(),
        s3_addressing_style: buzz_media::S3AddressingStyle::Path,
        max_image_bytes: 1024,
        max_gif_bytes: 1024,
        max_video_bytes: 1024,
        max_file_bytes: 1024,
        public_base_url: "http://localhost:3000/media".into(),
        upload_records_enabled: false,
        upload_ip_header: None,
        upload_port_header: None,
        azure: Some(azure),
    })
    .expect("azure media storage")
}

async fn all_versions(
    storage: &buzz_media::MediaStorage,
    prefix: &str,
) -> Vec<buzz_media::ObjectVersionEntry> {
    let mut entries = Vec::new();
    let (mut key_marker, mut version_marker) = (None, None);
    loop {
        let page = storage
            .list_prefix_versions_page(prefix, key_marker, version_marker, 2)
            .await
            .expect("list versions page");
        assert!(page.entries.len() <= 2);
        entries.extend(page.entries);
        if !page.is_truncated {
            return entries;
        }
        assert!(
            page.next_key_marker.is_some() && page.next_version_id_marker.is_some(),
            "truncated pages carry both cursor halves"
        );
        key_marker = page.next_key_marker;
        version_marker = page.next_version_id_marker;
    }
}

#[tokio::test]
#[ignore = "requires Azurite on 127.0.0.1:10000"]
async fn media_storage_reads_lists_and_permanently_deletes_versions() {
    let storage = media_storage("media").await;

    storage
        .put("t/a", b"one", "text/plain")
        .await
        .expect("put a1");
    storage
        .put("t/a", b"two!", "text/plain")
        .await
        .expect("put a2");
    storage
        .put("t/b", b"bee", "text/plain")
        .await
        .expect("put b");
    storage
        .put("t/c", b"sea", "text/plain")
        .await
        .expect("put c");
    storage
        .put("u/keep", b"keep", "text/plain")
        .await
        .expect("put keep");
    storage.ping().await.expect("ping");

    assert_eq!(storage.get("t/a").await.expect("get"), b"two!");
    assert_eq!(storage.get_range("t/a", 1, 2).await.expect("range"), b"wo");
    assert!(storage.head("t/b").await.expect("head"));
    assert!(!storage.head("t/missing").await.expect("head missing"));
    let meta = storage
        .head_with_metadata("t/b")
        .await
        .expect("head meta")
        .expect("present");
    assert_eq!(meta.size, 3);
    assert!(matches!(
        storage.get("t/missing").await,
        Err(buzz_media::MediaError::NotFound)
    ));

    let mut keys = Vec::new();
    let mut token = None;
    loop {
        let page = storage
            .list_prefix_page("t/", token, 2)
            .await
            .expect("list page");
        keys.extend(page.objects.into_iter().map(|(key, _)| key));
        if !page.is_truncated {
            break;
        }
        token = page.next_continuation_token;
    }
    assert_eq!(keys, ["t/a", "t/b", "t/c"]);

    storage
        .preflight_version_listing("t/")
        .await
        .expect("preflight");
    let versions = all_versions(&storage, "t/").await;
    assert_eq!(versions.len(), 4, "{versions:?}");
    let refs = versions
        .iter()
        .map(|entry| buzz_media::ObjectVersionRef {
            key: entry.key.clone(),
            version_id: entry.version_id.clone(),
        })
        .collect::<Vec<_>>();
    let old_a = versions
        .iter()
        .find(|entry| entry.key == "t/a" && entry.size == 3)
        .expect("historical t/a");
    assert_eq!(
        storage
            .get_small_version("t/a", &old_a.version_id, 16)
            .await
            .expect("versioned read"),
        b"one"
    );

    let outcome = storage
        .delete_object_versions(&refs)
        .await
        .expect("delete versions");
    assert!(outcome.failed.is_empty(), "{outcome:?}");
    assert_eq!(outcome.deleted, 4);
    let retry = storage
        .delete_object_versions(&refs)
        .await
        .expect("retry delete versions");
    assert!(retry.failed.is_empty(), "{retry:?}");
    assert_eq!(retry.already_missing, 4);

    assert!(all_versions(&storage, "t/").await.is_empty());
    assert!(matches!(
        storage.get("t/a").await,
        Err(buzz_media::MediaError::NotFound)
    ));
    assert_eq!(storage.get("u/keep").await.expect("untouched"), b"keep");

    storage.delete("u/keep").await.expect("delete");
    storage
        .delete("u/keep")
        .await
        .expect("delete is idempotent");
}
