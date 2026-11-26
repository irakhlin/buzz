//! Request authorization: Shared Key signing and Microsoft Entra ID bearer
//! tokens.

use std::fmt;
use std::sync::Arc;

use azure_core::credentials::TokenCredential;
use azure_identity::{
    ManagedIdentityCredential, ManagedIdentityCredentialOptions, UserAssignedId,
    WorkloadIdentityCredential,
};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use hmac::{Hmac, KeyInit, Mac};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use sha2::Sha256;

use super::{AzureAuthMode, AzureBlobConfig, AzureError};

/// OAuth scope for Azure Storage data-plane access.
const STORAGE_SCOPE: &str = "https://storage.azure.com/.default";

/// Signs or stamps outgoing Blob requests.
pub(crate) enum Authorizer {
    SharedKey { account: String, key: Vec<u8> },
    Bearer(Arc<dyn TokenCredential>),
}

impl fmt::Debug for Authorizer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SharedKey { account, .. } => f
                .debug_struct("SharedKey")
                .field("account", account)
                .finish_non_exhaustive(),
            Self::Bearer(credential) => f.debug_tuple("Bearer").field(credential).finish(),
        }
    }
}

impl Authorizer {
    pub(crate) fn new(config: &AzureBlobConfig) -> Result<Self, AzureError> {
        let federated = std::env::var_os("AZURE_FEDERATED_TOKEN_FILE").is_some();
        match config.resolved_auth(federated) {
            AzureAuthMode::SharedKey => {
                let key = config.account_key.as_deref().ok_or_else(|| {
                    AzureError::Config("shared key authorization requires an account key".into())
                })?;
                let key = BASE64
                    .decode(key)
                    .map_err(|_| AzureError::Config("account key is not valid base64".into()))?;
                Ok(Self::SharedKey {
                    account: config.account.clone(),
                    key,
                })
            }
            AzureAuthMode::WorkloadIdentity => {
                let credential = WorkloadIdentityCredential::new(None)
                    .map_err(|e| AzureError::Auth(format!("workload identity: {e}")))?;
                Ok(Self::Bearer(credential))
            }
            AzureAuthMode::ManagedIdentity | AzureAuthMode::Auto => {
                let options = ManagedIdentityCredentialOptions {
                    user_assigned_id: config.client_id.clone().map(UserAssignedId::ClientId),
                    ..Default::default()
                };
                let credential = ManagedIdentityCredential::new(Some(options))
                    .map_err(|e| AzureError::Auth(format!("managed identity: {e}")))?;
                Ok(Self::Bearer(credential))
            }
        }
    }

    /// Add the `Authorization` header to a fully built request.
    pub(crate) async fn authorize(&self, request: &mut reqwest::Request) -> Result<(), AzureError> {
        let value = match self {
            Self::SharedKey { account, key } => {
                let content_length = request
                    .body()
                    .and_then(reqwest::Body::as_bytes)
                    .map_or(0, <[u8]>::len);
                let string_to_sign = string_to_sign(
                    request.method().as_str(),
                    request.headers(),
                    request.url(),
                    account,
                    content_length,
                );
                let mut mac = Hmac::<Sha256>::new_from_slice(key)
                    .map_err(|_| AzureError::Config("invalid account key".into()))?;
                mac.update(string_to_sign.as_bytes());
                let signature = BASE64.encode(mac.finalize().into_bytes());
                format!("SharedKey {account}:{signature}")
            }
            Self::Bearer(credential) => {
                let token = credential
                    .get_token(&[STORAGE_SCOPE], None)
                    .await
                    .map_err(|e| AzureError::Auth(e.to_string()))?;
                format!("Bearer {}", token.token.secret())
            }
        };
        let mut value = HeaderValue::from_str(&value)
            .map_err(|_| AzureError::Auth("authorization header is not valid ASCII".into()))?;
        value.set_sensitive(true);
        request.headers_mut().insert(AUTHORIZATION, value);
        Ok(())
    }
}

/// Build the Shared Key string-to-sign for Blob service version 2015-02-21+.
///
/// See <https://learn.microsoft.com/rest/api/storageservices/authorize-with-shared-key>.
fn string_to_sign(
    method: &str,
    headers: &HeaderMap,
    url: &url::Url,
    account: &str,
    content_length: usize,
) -> String {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
    };
    let content_length = if content_length == 0 {
        String::new()
    } else {
        content_length.to_string()
    };
    let mut out = String::with_capacity(256);
    for line in [
        method,
        header("content-encoding"),
        header("content-language"),
        &content_length,
        header("content-md5"),
        header("content-type"),
        // `x-ms-date` is always sent, so the standard Date line is empty.
        "",
        header("if-modified-since"),
        header("if-match"),
        header("if-none-match"),
        header("if-unmodified-since"),
        header("range"),
    ] {
        out.push_str(line);
        out.push('\n');
    }

    let mut ms_headers: Vec<(&str, String)> = Vec::new();
    for name in headers.keys() {
        let name = name.as_str();
        if !name.starts_with("x-ms-") {
            continue;
        }
        let joined = headers
            .get_all(name)
            .iter()
            .map(|value| unfold(value.to_str().unwrap_or("")))
            .collect::<Vec<_>>()
            .join(",");
        ms_headers.push((name, joined));
    }
    ms_headers.sort_by(|a, b| a.0.cmp(b.0));
    for (name, value) in ms_headers {
        out.push_str(name);
        out.push(':');
        out.push_str(&value);
        out.push('\n');
    }

    out.push('/');
    out.push_str(account);
    out.push_str(url.path());
    let mut params: Vec<(String, Vec<String>)> = Vec::new();
    for (name, value) in url.query_pairs() {
        let name = name.to_lowercase();
        match params.iter_mut().find(|(existing, _)| *existing == name) {
            Some((_, values)) => values.push(value.into_owned()),
            None => params.push((name, vec![value.into_owned()])),
        }
    }
    params.sort_by(|a, b| a.0.cmp(&b.0));
    for (name, mut values) in params {
        values.sort();
        out.push('\n');
        out.push_str(&name);
        out.push(':');
        out.push_str(&values.join(","));
    }
    out
}

/// Trim a header value and collapse internal runs of whitespace, as Shared
/// Key canonicalization requires.
fn unfold(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_to_sign_matches_documented_canonical_form() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ms-version", HeaderValue::from_static("2021-12-02"));
        headers.insert(
            "x-ms-date",
            HeaderValue::from_static("Fri, 26 Jun 2015 23:39:12 GMT"),
        );
        headers.insert("x-ms-blob-type", HeaderValue::from_static("BlockBlob"));
        headers.insert("content-type", HeaderValue::from_static("image/png"));
        headers.insert("if-none-match", HeaderValue::from_static("*"));
        let url = url::Url::parse(
            "http://127.0.0.1:10000/devstoreaccount1/media/a%20b.png?comp=list&restype=container&Include=versions",
        )
        .expect("url");

        let signed = string_to_sign("PUT", &headers, &url, "devstoreaccount1", 5);
        assert_eq!(
            signed,
            "PUT\n\n\n5\n\nimage/png\n\n\n\n*\n\n\n\
             x-ms-blob-type:BlockBlob\n\
             x-ms-date:Fri, 26 Jun 2015 23:39:12 GMT\n\
             x-ms-version:2021-12-02\n\
             /devstoreaccount1/devstoreaccount1/media/a%20b.png\n\
             comp:list\ninclude:versions\nrestype:container"
        );

        let empty = string_to_sign("GET", &HeaderMap::new(), &url, "acct", 0);
        assert!(empty.starts_with("GET\n\n\n\n"), "{empty:?}");
    }

    #[tokio::test]
    async fn shared_key_authorization_is_deterministic_and_sensitive() {
        let authorizer = Authorizer::SharedKey {
            account: "acct".into(),
            key: b"secret".to_vec(),
        };
        let mut request = reqwest::Request::new(
            reqwest::Method::GET,
            url::Url::parse("https://acct.blob.core.windows.net/media/blob").expect("url"),
        );
        request
            .headers_mut()
            .insert("x-ms-date", HeaderValue::from_static("now"));
        authorizer.authorize(&mut request).await.expect("sign");
        let header = request.headers().get(AUTHORIZATION).expect("authorization");
        assert!(header.is_sensitive());
        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").expect("key");
        mac.update("GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:now\n/acct/media/blob".as_bytes());
        let expected = format!(
            "SharedKey acct:{}",
            BASE64.encode(mac.finalize().into_bytes())
        );
        assert_eq!(header.to_str().expect("ascii"), expected);
    }
}
