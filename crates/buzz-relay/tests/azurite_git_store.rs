//! Git object storage on Azure Blob Storage, against Azurite: pointer CAS,
//! bounded reads, and the startup conformance probe.
//!
//! ```bash
//! docker compose --profile azurite up -d azurite
//! cargo test -p buzz-relay --test azurite_git_store -- --ignored --test-threads=1
//! ```
//!
//! Azurite must listen on 127.0.0.1:10000 (override with
//! `BUZZ_AZURE_STORAGE_ENDPOINT`). Each test creates its own container.

use buzz_media::azure::{AzureAuthMode, AzureBlobClient, AzureBlobConfig};
use buzz_relay::api::git::store::{CasOutcome, GitStore, Precond, ProbeConfig, StoreError};
use sha2::{Digest, Sha256};

// Azurite's documented public development account key.
const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

async fn store() -> GitStore {
    let config = AzureBlobConfig {
        account: "devstoreaccount1".into(),
        container: format!("buzz-git-{}", uuid::Uuid::new_v4().simple()),
        endpoint: Some(
            std::env::var("BUZZ_AZURE_STORAGE_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:10000/devstoreaccount1".into()),
        ),
        auth: AzureAuthMode::SharedKey,
        account_key: Some(AZURITE_KEY.into()),
        client_id: None,
    };
    AzureBlobClient::new(&config)
        .expect("azure client")
        .create_container_if_missing()
        .await
        .expect("create container (is Azurite running?)");
    GitStore::new_azure(&config).expect("azure git store")
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[tokio::test]
#[ignore = "requires Azurite on 127.0.0.1:10000"]
async fn azure_pointer_cas_and_bounded_reads() {
    let st = store().await;

    let bytes = b"hello, git on azure".to_vec();
    let digest = sha256_hex(&bytes);
    let key = st.put_pack(&bytes).await.expect("put_pack");
    assert_eq!(key, format!("packs/{digest}"));
    assert_eq!(st.put_pack(&bytes).await.expect("idempotent"), key);
    let got = st.get_verified(&key, &digest).await.expect("verified");
    assert_eq!(&got[..], &bytes[..]);
    assert!(matches!(
        st.get_verified(&key, &"0".repeat(64)).await,
        Err(StoreError::DigestMismatch { .. })
    ));
    assert!(matches!(
        st.get_limited(&key, 4).await,
        Err(StoreError::ObjectTooLarge { size, max: 4, .. }) if size == bytes.len() as u64
    ));
    assert!(matches!(
        st.get("packs/missing").await,
        Err(StoreError::NotFound(_))
    ));
    assert!(st.get_idx(&digest, 1024).await.expect("idx miss").is_none());
    st.put_idx(&digest, b"idx").await.expect("put idx");
    st.put_idx(&digest, b"other")
        .await
        .expect("idx is create-only");
    assert_eq!(
        &st.get_idx(&digest, 1024).await.expect("idx").expect("hit")[..],
        b"idx"
    );

    let pkey = format!("pointers/{}.json", uuid::Uuid::new_v4());
    assert!(st.get_pointer(&pkey).await.expect("get none").is_none());
    let e1 = match st
        .put_pointer(&pkey, b"{\"m\":1}", Precond::IfNoneMatchStar)
        .await
        .expect("create")
    {
        CasOutcome::Won(e) => e,
        CasOutcome::LostRace => panic!("first create must win"),
    };
    assert_eq!(
        st.put_pointer(&pkey, b"{}", Precond::IfNoneMatchStar)
            .await
            .expect("second create"),
        CasOutcome::LostRace
    );
    let e2 = match st
        .put_pointer(&pkey, b"{\"m\":2}", Precond::IfMatch(e1.clone()))
        .await
        .expect("cas")
    {
        CasOutcome::Won(e) => e,
        CasOutcome::LostRace => panic!("fresh If-Match must win"),
    };
    assert_eq!(
        st.put_pointer(&pkey, b"{}", Precond::IfMatch(e1))
            .await
            .expect("stale cas"),
        CasOutcome::LostRace
    );
    let (etag_now, body) = st.get_pointer(&pkey).await.expect("get").expect("exists");
    assert_eq!(etag_now, e2, "GET ETag chains from the PUT ETag");
    assert_eq!(&body[..], b"{\"m\":2}");
}

#[tokio::test]
#[ignore = "requires Azurite on 127.0.0.1:10000"]
async fn azure_passes_conformance_probe() {
    let report = store()
        .await
        .run_conformance_probe(ProbeConfig {
            race_width: 16,
            race_rounds: 3,
        })
        .await
        .expect("conformance probe");
    assert_eq!(report.transport_drops, 0);
}
