# Azure Blob Storage

Buzz stores media and Git objects in one object store. S3 and S3-compatible
services (MinIO, R2, and similar) are the default. Set
`BUZZ_STORAGE_BACKEND=azure` to use an Azure Blob Storage container instead.
The selection applies to every process that touches object storage:

| Process | Uses storage for |
|---|---|
| `buzz-relay` | Blossom media, thumbnails, upload records, and the Git object store (packs, indexes, manifest pointers) |
| `buzz-admin deletions drain` | Community deletion: listing and permanently deleting every object version under a community prefix |
| `buzz-admin user-deletions media-batches` | User media deletion |
| `buzz-admin storage-snapshot` | Storage accounting scans |

Media and Git objects share the container exactly as they share the S3
bucket; object keys are identical on both backends.

## Account requirements

- **Storage account kind:** general-purpose v2 or premium block blob, with
  hierarchical namespace **disabled**. Data Lake Storage (hierarchical
  namespace) accounts are not supported.
- **Container:** create it before starting Buzz, with anonymous access
  disabled. Buzz never creates the production container.
- **Blob versioning:** enable it. Deletion executors list and permanently
  delete every version under a prefix, as they do for a versioned S3 bucket.
  Without versioning, blobs report the S3 `null` version id and deletion acts on
  the base blob. On a versioned account, deleting a blob written before
  versioning was enabled keeps its bytes as a new previous version, so Buzz
  deletes that version too.
- **Blob soft delete:** Buzz deletion must be permanent. If blob soft delete is
  enabled, also enable permanent delete (`allowPermanentDelete` on the delete
  retention policy). Otherwise the deletion preflight and every delete fail
  closed with an error rather than leave recoverable data. Blobs that have no
  version id (written before versioning was enabled) cannot be permanently
  deleted while soft delete is on; those deletes also fail closed.
- **Immutability policies and legal holds** block deletion. Don't apply them to
  the Buzz container if you use the deletion executors.

## Configuration

| Variable | Required | Meaning |
|---|---|---|
| `BUZZ_STORAGE_BACKEND` | yes | `azure`. Unset or `s3` keeps the S3 backend. With `azure`, every `BUZZ_S3_*` variable is ignored. |
| `BUZZ_AZURE_STORAGE_ACCOUNT` | yes | Storage account name (3–24 lowercase letters or digits). |
| `BUZZ_AZURE_STORAGE_CONTAINER` | yes | Container name. |
| `BUZZ_AZURE_STORAGE_ENDPOINT` | no | Blob service endpoint. Defaults to `https://<account>.blob.core.windows.net`. Set it for sovereign clouds, private endpoints with custom DNS, or Azurite. |
| `BUZZ_AZURE_STORAGE_AUTH` | no | `auto` (default), `shared_key`, `workload_identity`, or `managed_identity`. |
| `BUZZ_AZURE_STORAGE_ACCOUNT_KEY` | for `shared_key` | Base64 storage account key. Treat it as a secret. |
| `AZURE_CLIENT_ID` | no | Client ID of a user-assigned managed identity, or the workload identity's application. |

Processes validate the configuration at startup and exit with a message naming
the variable at fault.

## Authentication

`auto` picks the first mode that applies: Shared Key when an account key is
set, workload identity when `AZURE_FEDERATED_TOKEN_FILE` is present, and
otherwise managed identity. Set the mode explicitly in production so a missing
credential fails instead of falling through to another mode.

- **Shared Key** (`shared_key`) signs each request with the account key. Use
  it for Azurite and for deployments without Microsoft Entra ID. Microsoft
  recommends Entra ID over Shared Key where you can use it.
- **Workload identity** (`workload_identity`) exchanges a Kubernetes service
  account token for a Microsoft Entra ID token. On AKS, enable the OIDC issuer
  and workload identity, create a federated credential for the service account,
  annotate the service account with `azure.workload.identity/client-id`, and
  label each pod `azure.workload.identity/use: "true"`. The webhook injects
  `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`, and the
  token volume, including into pods that disable service-account token
  automounting.
- **Managed identity** (`managed_identity`) reads tokens from the Azure
  instance metadata service. Set `AZURE_CLIENT_ID` to select a user-assigned
  identity; leave it unset for the system-assigned identity.

Microsoft Entra ID modes require an `https` endpoint, and setting an account key
together with an Entra ID mode is rejected.

## Role assignments

With Shared Key, the account key grants everything below. With Microsoft
Entra ID, assign roles to each workload's identity, scoped to the container
where the role allows it:

| Workload | Role |
|---|---|
| Relay | **Storage Blob Data Contributor** on the container |
| Storage accounting (`storage-snapshot`) | **Storage Blob Data Reader** on the container |
| Deletion executors (`deletions drain`, `user-deletions media-batches`) | **Storage Blob Data Owner** on the container, or a custom role with `blobs/read`, `blobs/write`, `blobs/delete`, `blobs/deleteBlobVersion/action`, and, when soft delete is on, `blobs/permanentDelete/action`; **plus** `Microsoft.Storage/storageAccounts/blobServices/read` on the storage account (for example **Reader**) |

The deletion preflight reads the account's soft-delete policy, which is why
deletion executors need `blobServices/read`; without it they stop before
deleting anything. Give deletion jobs a dedicated identity if the relay's
identity should not hold version-delete rights.

## Git object storage

The Git store keeps its S3 protocol on Azure (see
[git-on-object-storage.md](git-on-object-storage.md)):

- Create-only writes of packs and manifests use `If-None-Match: *`. Azure
  answers a lost create race with `409 BlobAlreadyExists` or
  `412 ConditionNotMet`; both count as a precondition failure.
- Manifest pointer updates use `If-Match` with the blob's ETag and lose the race
  with `412`.
- The startup conformance probe (`BUZZ_GIT_CONFORMANCE_PROBE`, on by default)
  admits the container against the compare-and-swap axiom with the same
  sequential checks and N-way races it runs on S3. A probe failure is fatal at
  startup.

Conditional writes are never retried automatically, so a timed-out write is
reported as an unknown outcome rather than silently replayed.

## Helm

Set `storage.backend: azure` and `storage.azure.*` in chart values; the chart
renders the variables above for the relay, the deletion drain job, and the
storage accounting job, and omits the `BUZZ_S3_*` variables. Put a Shared Key in
`storage.azure.accountKey` or as `BUZZ_AZURE_STORAGE_ACCOUNT_KEY` in
`secrets.existingSecret`. The bundled MinIO quickstart is S3-only. See the
[chart README](../deploy/charts/buzz/README.md#azure-blob-storage) for a
workload-identity example. The `deploy/compose` self-hosting bundle runs MinIO
and does not support Azure.

## Switching an existing deployment

Buzz does not migrate data between backends. To move a deployment, stop every
writer, copy every object to the container with its key unchanged, switch the
configuration, and start the relay. Copied blobs get new version ids and ETags;
the Git store reads the current ETag before each pointer update, so that is
safe. Enable versioning before copying, so every copied blob gets a version id.

## Local development and tests

[Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite)
emulates Blob Storage locally. The development compose file starts it with
versioning enabled and creates the `buzz-media` container:

```bash
docker compose --profile azurite up -d azurite azurite-init
```

Then set the Azure block from `.env.example` (`BUZZ_STORAGE_BACKEND=azure`,
account `devstoreaccount1`, endpoint `http://127.0.0.1:10000/devstoreaccount1`,
and Azurite's published development key).

The Azure backends have live tests that are ignored by default:

```bash
docker compose --profile azurite up -d azurite
cargo test -p buzz-media --test azurite_blob -- --ignored --test-threads=1
cargo test -p buzz-relay --test azurite_git_store -- --ignored --test-threads=1
```

CI runs both in the backend integration lane. Azurite does not enforce RBAC or
soft-delete policies, so validate role assignments and retention settings
against a real storage account before production use.
