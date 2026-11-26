//! Azure Blob Storage backend shared by media and Git object storage.
//!
//! A deliberately small REST client over `reqwest` covering exactly the Blob
//! operations Buzz storage uses: conditional block-blob writes, block
//! uploads for large files, ranged and streaming reads, properties, version
//! listing, and exact-version deletion. Requests are authorized with either a
//! storage account key (Shared Key) or a Microsoft Entra ID token from
//! workload identity or managed identity.
//!
//! Configuration is selected with `BUZZ_STORAGE_BACKEND=azure`; see
//! [`AzureBlobConfig::from_env`] and `docs/azure-blob-storage.md`.

mod auth;
mod client;
mod error;
mod xml;

use std::fmt;
use std::str::FromStr;

use base64::Engine as _;

pub use client::{
    AzureBlobClient, BlobContent, BlobProperties, BlobStream, DeleteOptions, DeleteOutcome,
    DeleteRetention, GetOptions, ListBlobsPage, ListedBlob, PutCondition, PutOutcome,
};
pub use error::AzureError;

/// Environment variable that selects the object-store backend (`s3` or `azure`).
pub const STORAGE_BACKEND_ENV: &str = "BUZZ_STORAGE_BACKEND";

/// How the Azure client authorizes Blob Storage requests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AzureAuthMode {
    /// Shared Key when an account key is configured; otherwise workload
    /// identity when `AZURE_FEDERATED_TOKEN_FILE` is present; otherwise
    /// managed identity.
    #[default]
    Auto,
    /// Storage account key (HMAC-SHA256 Shared Key authorization).
    SharedKey,
    /// Microsoft Entra workload identity federation (for example AKS workload
    /// identity), configured by the standard `AZURE_TENANT_ID`,
    /// `AZURE_CLIENT_ID`, and `AZURE_FEDERATED_TOKEN_FILE` variables.
    WorkloadIdentity,
    /// Azure managed identity (system-assigned, or user-assigned when
    /// `AZURE_CLIENT_ID` is set).
    ManagedIdentity,
}

impl FromStr for AzureAuthMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "auto" => Ok(Self::Auto),
            "shared_key" => Ok(Self::SharedKey),
            "workload_identity" => Ok(Self::WorkloadIdentity),
            "managed_identity" => Ok(Self::ManagedIdentity),
            _ => Err(format!(
                "BUZZ_AZURE_STORAGE_AUTH must be 'auto', 'shared_key', 'workload_identity', or \
                 'managed_identity', got {value:?}"
            )),
        }
    }
}

/// Azure Blob Storage connection settings.
///
/// One container holds every object the relay writes, mirroring the single
/// S3 bucket layout: media blobs, sidecars, upload records, and Git objects
/// share the same key taxonomy.
#[derive(Clone, PartialEq, Eq, serde::Deserialize)]
pub struct AzureBlobConfig {
    /// Storage account name (3-24 lowercase letters and digits).
    pub account: String,
    /// Blob container name.
    pub container: String,
    /// Blob service endpoint. Defaults to
    /// `https://{account}.blob.core.windows.net`. Set it for sovereign clouds,
    /// private endpoints with custom DNS, or Azurite
    /// (`http://127.0.0.1:10000/devstoreaccount1`).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Authorization mode.
    #[serde(default)]
    pub auth: AzureAuthMode,
    /// Base64 storage account key for Shared Key authorization.
    #[serde(default)]
    pub account_key: Option<String>,
    /// Client ID of a user-assigned managed identity. Read from the standard
    /// `AZURE_CLIENT_ID` variable; workload identity reads it directly.
    #[serde(default)]
    pub client_id: Option<String>,
}

impl fmt::Debug for AzureBlobConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzureBlobConfig")
            .field("account", &self.account)
            .field("container", &self.container)
            .field("endpoint", &self.endpoint)
            .field("auth", &self.auth)
            .field(
                "account_key",
                &self.account_key.as_ref().map(|_| "<redacted>"),
            )
            .field("client_id", &self.client_id)
            .finish()
    }
}

impl AzureBlobConfig {
    /// Read Azure settings from the process environment.
    ///
    /// Returns `Ok(None)` when `BUZZ_STORAGE_BACKEND` is unset or `s3`.
    pub fn from_env() -> Result<Option<Self>, String> {
        Self::from_env_with(|name| std::env::var(name).ok())
    }

    /// Read Azure settings through `get_env`, for callers and tests that
    /// supply their own environment source.
    ///
    /// Recognized variables: `BUZZ_STORAGE_BACKEND`,
    /// `BUZZ_AZURE_STORAGE_ACCOUNT`, `BUZZ_AZURE_STORAGE_CONTAINER`,
    /// `BUZZ_AZURE_STORAGE_ENDPOINT`, `BUZZ_AZURE_STORAGE_AUTH`,
    /// `BUZZ_AZURE_STORAGE_ACCOUNT_KEY`, and `AZURE_CLIENT_ID`.
    pub fn from_env_with(get_env: impl Fn(&str) -> Option<String>) -> Result<Option<Self>, String> {
        let get = |name: &str| {
            get_env(name)
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        };
        match get(STORAGE_BACKEND_ENV).as_deref() {
            None | Some("s3") => return Ok(None),
            Some("azure") => {}
            Some(other) => {
                return Err(format!(
                    "{STORAGE_BACKEND_ENV} must be 's3' or 'azure', got {other:?}"
                ))
            }
        }
        let required = |name: &str| {
            get(name).ok_or_else(|| format!("{name} is required when {STORAGE_BACKEND_ENV}=azure"))
        };
        let config = Self {
            account: required("BUZZ_AZURE_STORAGE_ACCOUNT")?,
            container: required("BUZZ_AZURE_STORAGE_CONTAINER")?,
            endpoint: get("BUZZ_AZURE_STORAGE_ENDPOINT"),
            auth: get("BUZZ_AZURE_STORAGE_AUTH")
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or_default(),
            account_key: get("BUZZ_AZURE_STORAGE_ACCOUNT_KEY"),
            client_id: get("AZURE_CLIENT_ID"),
        };
        config.validate()?;
        Ok(Some(config))
    }

    /// The blob service endpoint without a trailing slash.
    pub fn endpoint_url(&self) -> String {
        match &self.endpoint {
            Some(endpoint) => endpoint.trim_end_matches('/').to_owned(),
            None => format!("https://{}.blob.core.windows.net", self.account),
        }
    }

    /// Resolve [`AzureAuthMode::Auto`] to a concrete mode.
    ///
    /// `federated_token_present` reports whether `AZURE_FEDERATED_TOKEN_FILE`
    /// is set, which is how workload identity webhooks announce themselves.
    pub fn resolved_auth(&self, federated_token_present: bool) -> AzureAuthMode {
        match self.auth {
            AzureAuthMode::Auto if self.account_key.is_some() => AzureAuthMode::SharedKey,
            AzureAuthMode::Auto if federated_token_present => AzureAuthMode::WorkloadIdentity,
            AzureAuthMode::Auto => AzureAuthMode::ManagedIdentity,
            explicit => explicit,
        }
    }

    /// Validate names, endpoint, and the credential combination.
    pub fn validate(&self) -> Result<(), String> {
        validate_account(&self.account)?;
        validate_container(&self.container)?;
        let endpoint = url::Url::parse(&self.endpoint_url())
            .map_err(|e| format!("BUZZ_AZURE_STORAGE_ENDPOINT is not a valid URL: {e}"))?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err("BUZZ_AZURE_STORAGE_ENDPOINT must use http or https".to_string());
        }
        if endpoint.query().is_some() || endpoint.fragment().is_some() {
            return Err(
                "BUZZ_AZURE_STORAGE_ENDPOINT must not contain a query string or fragment"
                    .to_string(),
            );
        }
        match (self.auth, &self.account_key) {
            (AzureAuthMode::SharedKey, None) => {
                return Err(
                    "BUZZ_AZURE_STORAGE_AUTH=shared_key requires BUZZ_AZURE_STORAGE_ACCOUNT_KEY"
                        .to_string(),
                )
            }
            (AzureAuthMode::WorkloadIdentity | AzureAuthMode::ManagedIdentity, Some(_)) => {
                return Err(
                    "BUZZ_AZURE_STORAGE_ACCOUNT_KEY is set but BUZZ_AZURE_STORAGE_AUTH selects \
                     Microsoft Entra ID; unset the key or use shared_key"
                        .to_string(),
                )
            }
            (_, Some(key)) => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(key)
                    .map_err(|_| {
                        "BUZZ_AZURE_STORAGE_ACCOUNT_KEY is not valid base64".to_string()
                    })?;
                if decoded.is_empty() {
                    return Err("BUZZ_AZURE_STORAGE_ACCOUNT_KEY is empty".to_string());
                }
            }
            (_, None) => {
                // Bearer tokens must never travel over plaintext HTTP.
                if endpoint.scheme() != "https" {
                    return Err("Microsoft Entra ID authorization requires an https \
                         BUZZ_AZURE_STORAGE_ENDPOINT"
                        .to_string());
                }
            }
        }
        Ok(())
    }
}

fn validate_account(account: &str) -> Result<(), String> {
    let valid = (3..=24).contains(&account.len())
        && account
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    if valid {
        Ok(())
    } else {
        Err(format!(
            "BUZZ_AZURE_STORAGE_ACCOUNT must be 3-24 lowercase letters or digits, got {account:?}"
        ))
    }
}

fn validate_container(container: &str) -> Result<(), String> {
    let bytes = container.as_bytes();
    let valid = (3..=63).contains(&bytes.len())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && !container.contains("--");
    if valid {
        Ok(())
    } else {
        Err(format!(
            "BUZZ_AZURE_STORAGE_CONTAINER must be 3-63 lowercase letters, digits, or single \
             hyphens, starting and ending with a letter or digit; got {container:?}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // Azurite's documented public development key.
    const KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn base() -> AzureBlobConfig {
        AzureBlobConfig {
            account: "acct".into(),
            container: "media".into(),
            endpoint: None,
            auth: AzureAuthMode::Auto,
            account_key: None,
            client_id: None,
        }
    }

    #[test]
    fn backend_selection_defaults_to_s3_and_rejects_unknown_values() {
        assert_eq!(AzureBlobConfig::from_env_with(env(&[])), Ok(None));
        assert_eq!(
            AzureBlobConfig::from_env_with(env(&[(STORAGE_BACKEND_ENV, "s3")])),
            Ok(None)
        );
        let err = AzureBlobConfig::from_env_with(env(&[(STORAGE_BACKEND_ENV, "gcs")]))
            .expect_err("unknown backend");
        assert!(err.contains("must be 's3' or 'azure'"), "{err}");
    }

    #[test]
    fn azure_backend_reads_and_validates_settings() {
        let config = AzureBlobConfig::from_env_with(env(&[
            (STORAGE_BACKEND_ENV, "azure"),
            ("BUZZ_AZURE_STORAGE_ACCOUNT", "devstoreaccount1"),
            ("BUZZ_AZURE_STORAGE_CONTAINER", "buzz-media"),
            (
                "BUZZ_AZURE_STORAGE_ENDPOINT",
                "http://127.0.0.1:10000/devstoreaccount1/",
            ),
            ("BUZZ_AZURE_STORAGE_ACCOUNT_KEY", KEY),
        ]))
        .expect("valid config")
        .expect("azure selected");
        assert_eq!(
            config.endpoint_url(),
            "http://127.0.0.1:10000/devstoreaccount1"
        );
        assert_eq!(config.resolved_auth(true), AzureAuthMode::SharedKey);
        assert!(!format!("{config:?}").contains(KEY), "key leaked in Debug");

        for missing in ["BUZZ_AZURE_STORAGE_ACCOUNT", "BUZZ_AZURE_STORAGE_CONTAINER"] {
            let mut pairs = vec![
                (STORAGE_BACKEND_ENV, "azure"),
                ("BUZZ_AZURE_STORAGE_ACCOUNT", "acct"),
                ("BUZZ_AZURE_STORAGE_CONTAINER", "media"),
            ];
            pairs.retain(|(name, _)| *name != missing);
            let err = AzureBlobConfig::from_env_with(env(&pairs)).expect_err("missing setting");
            assert!(err.contains(missing), "{err}");
        }
    }

    #[test]
    fn auto_auth_prefers_key_then_workload_then_managed_identity() {
        let mut config = base();
        assert_eq!(config.resolved_auth(true), AzureAuthMode::WorkloadIdentity);
        assert_eq!(config.resolved_auth(false), AzureAuthMode::ManagedIdentity);
        config.account_key = Some(KEY.into());
        assert_eq!(config.resolved_auth(true), AzureAuthMode::SharedKey);
    }

    #[test]
    fn invalid_combinations_fail_validation() {
        let cases: Vec<(&str, AzureBlobConfig)> = vec![
            (
                "ACCOUNT",
                AzureBlobConfig {
                    account: "Bad_Account".into(),
                    ..base()
                },
            ),
            (
                "CONTAINER",
                AzureBlobConfig {
                    container: "a--b".into(),
                    ..base()
                },
            ),
            (
                "requires BUZZ_AZURE_STORAGE_ACCOUNT_KEY",
                AzureBlobConfig {
                    auth: AzureAuthMode::SharedKey,
                    ..base()
                },
            ),
            (
                "selects",
                AzureBlobConfig {
                    auth: AzureAuthMode::ManagedIdentity,
                    account_key: Some(KEY.into()),
                    ..base()
                },
            ),
            (
                "base64",
                AzureBlobConfig {
                    account_key: Some("not base64!".into()),
                    ..base()
                },
            ),
            (
                "https",
                AzureBlobConfig {
                    endpoint: Some("http://acct.blob.example".into()),
                    ..base()
                },
            ),
            (
                "query",
                AzureBlobConfig {
                    endpoint: Some("https://acct.blob.example/?sv=1".into()),
                    ..base()
                },
            ),
        ];
        assert!(base().validate().is_ok());
        for (needle, config) in cases {
            let err = config.validate().expect_err(needle);
            assert!(err.contains(needle), "expected {needle:?} in {err}");
        }
    }

    #[test]
    fn auth_mode_parses_supported_values_only() {
        assert_eq!("auto".parse(), Ok(AzureAuthMode::Auto));
        assert_eq!("shared_key".parse(), Ok(AzureAuthMode::SharedKey));
        assert_eq!(
            "workload_identity".parse(),
            Ok(AzureAuthMode::WorkloadIdentity)
        );
        assert_eq!(
            "managed_identity".parse(),
            Ok(AzureAuthMode::ManagedIdentity)
        );
        assert!("SharedKey".parse::<AzureAuthMode>().is_err());
    }
}
