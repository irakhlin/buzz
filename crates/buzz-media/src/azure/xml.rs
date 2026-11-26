//! Strict parsers for the Blob service XML responses Buzz consumes.
//!
//! Listings drive destructive cleanup, so anything outside the documented
//! flat-listing shape fails closed instead of being skipped.

use quick_xml::escape::unescape;
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;

use super::client::{DeleteRetention, ListBlobsPage, ListedBlob};
use super::AzureError;

fn parse_error(message: impl Into<String>) -> AzureError {
    AzureError::Parse(message.into())
}

fn xml_error(error: impl std::fmt::Display) -> AzureError {
    AzureError::Parse(error.to_string())
}

fn reader(xml: &[u8]) -> Reader<&[u8]> {
    let xml = xml.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(xml);
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    reader
}

fn read_text(reader: &mut Reader<&[u8]>, start: &BytesStart<'_>) -> Result<String, AzureError> {
    let raw = reader.read_text(start.to_end().name()).map_err(xml_error)?;
    unescape(&raw)
        .map(|text| text.into_owned())
        .map_err(xml_error)
}

fn skip(reader: &mut Reader<&[u8]>, start: &BytesStart<'_>) -> Result<(), AzureError> {
    reader
        .read_to_end(start.to_end().name())
        .map_err(xml_error)?;
    Ok(())
}

fn set_once<T>(slot: &mut Option<T>, value: T, field: &str) -> Result<(), AzureError> {
    if slot.is_some() {
        return Err(parse_error(format!("duplicate {field}")));
    }
    *slot = Some(value);
    Ok(())
}

fn parse_bool(value: &str, field: &str) -> Result<bool, AzureError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(parse_error(format!("invalid {field} {value:?}"))),
    }
}

/// Parse a flat `List Blobs` response (no delimiter).
pub(crate) fn parse_list_blobs(xml: &[u8]) -> Result<ListBlobsPage, AzureError> {
    let mut reader = reader(xml);
    let mut blobs = Vec::new();
    let mut next_marker: Option<String> = None;
    let mut blobs_seen = false;
    let mut root_seen = false;
    let mut root_closed = false;

    loop {
        match reader.read_event().map_err(xml_error)? {
            Event::Start(start) if !root_seen => {
                if start.local_name().as_ref() != b"EnumerationResults" {
                    return Err(parse_error("expected EnumerationResults root"));
                }
                root_seen = true;
            }
            Event::Start(start) if !root_closed => match start.local_name().as_ref() {
                b"Blobs" => {
                    if blobs_seen {
                        return Err(parse_error("duplicate Blobs"));
                    }
                    blobs_seen = true;
                    parse_blobs(&mut reader, &start, &mut blobs)?;
                }
                b"NextMarker" => set_once(
                    &mut next_marker,
                    read_text(&mut reader, &start)?,
                    "NextMarker",
                )?,
                b"EnumerationResults" => return Err(parse_error("nested EnumerationResults")),
                _ => skip(&mut reader, &start)?,
            },
            Event::Empty(start) if root_seen && !root_closed => match start.local_name().as_ref() {
                b"NextMarker" => set_once(&mut next_marker, String::new(), "NextMarker")?,
                b"Blobs" => {
                    if blobs_seen {
                        return Err(parse_error("duplicate Blobs"));
                    }
                    blobs_seen = true;
                }
                _ => {}
            },
            Event::Empty(start)
                if !root_seen && start.local_name().as_ref() == b"EnumerationResults" =>
            {
                return Err(parse_error("empty EnumerationResults"));
            }
            Event::End(end)
                if root_seen
                    && !root_closed
                    && end.local_name().as_ref() == b"EnumerationResults" =>
            {
                root_closed = true;
            }
            Event::Decl(_) if !root_seen => {}
            Event::Comment(_) | Event::PI(_) => {}
            Event::Eof if root_closed => break,
            Event::Eof => return Err(parse_error("incomplete List Blobs document")),
            _ => return Err(parse_error("unexpected content in List Blobs document")),
        }
    }
    if !blobs_seen {
        return Err(parse_error("List Blobs response missing Blobs"));
    }
    Ok(ListBlobsPage {
        blobs,
        next_marker: next_marker.filter(|marker| !marker.is_empty()),
    })
}

fn parse_blobs(
    reader: &mut Reader<&[u8]>,
    start: &BytesStart<'_>,
    blobs: &mut Vec<ListedBlob>,
) -> Result<(), AzureError> {
    loop {
        match reader.read_event().map_err(xml_error)? {
            Event::Start(child) => match child.local_name().as_ref() {
                b"Blob" => blobs.push(parse_blob(reader, &child)?),
                b"BlobPrefix" => return Err(parse_error("unexpected BlobPrefix in flat listing")),
                _ => return Err(parse_error("unexpected element inside Blobs")),
            },
            Event::Empty(_) => return Err(parse_error("unexpected empty element inside Blobs")),
            Event::End(end) if end.name().as_ref() == start.to_end().name().as_ref() => {
                return Ok(())
            }
            Event::Eof => return Err(parse_error("unexpected EOF inside Blobs")),
            Event::Comment(_) => {}
            _ => return Err(parse_error("unexpected content inside Blobs")),
        }
    }
}

fn parse_blob(
    reader: &mut Reader<&[u8]>,
    start: &BytesStart<'_>,
) -> Result<ListedBlob, AzureError> {
    let mut name = None;
    let mut version_id = None;
    let mut is_current = None;
    let mut size = None;
    loop {
        match reader.read_event().map_err(xml_error)? {
            Event::Start(child) => match child.local_name().as_ref() {
                b"Name" => {
                    let encoded = child
                        .attributes()
                        .filter_map(Result::ok)
                        .any(|attr| attr.key.local_name().as_ref() == b"Encoded");
                    if encoded {
                        return Err(parse_error("encoded blob names are not supported"));
                    }
                    set_once(&mut name, read_text(reader, &child)?, "Name")?;
                }
                b"VersionId" => set_once(&mut version_id, read_text(reader, &child)?, "VersionId")?,
                b"IsCurrentVersion" => {
                    let value = read_text(reader, &child)?;
                    set_once(
                        &mut is_current,
                        parse_bool(&value, "IsCurrentVersion")?,
                        "IsCurrentVersion",
                    )?;
                }
                b"Deleted" => {
                    if parse_bool(&read_text(reader, &child)?, "Deleted")? {
                        return Err(parse_error("unexpected soft-deleted blob in listing"));
                    }
                }
                b"Snapshot" => return Err(parse_error("unexpected snapshot in listing")),
                b"Properties" => {
                    set_once(&mut size, parse_properties(reader, &child)?, "Properties")?
                }
                _ => skip(reader, &child)?,
            },
            Event::Empty(child) => match child.local_name().as_ref() {
                b"Name" | b"VersionId" | b"Properties" | b"IsCurrentVersion" => {
                    return Err(parse_error("empty required Blob field"))
                }
                _ => {}
            },
            Event::End(end) if end.name().as_ref() == start.to_end().name().as_ref() => {
                let name = name.ok_or_else(|| parse_error("Blob missing Name"))?;
                if name.is_empty() {
                    return Err(parse_error("Blob has an empty Name"));
                }
                let size = size.ok_or_else(|| parse_error("Blob missing Properties"))?;
                return Ok(ListedBlob {
                    name,
                    version_id: version_id.filter(|id| !id.is_empty()),
                    is_current_version: is_current.unwrap_or(false),
                    size,
                });
            }
            Event::Eof => return Err(parse_error("unexpected EOF inside Blob")),
            _ => {}
        }
    }
}

fn parse_properties(reader: &mut Reader<&[u8]>, start: &BytesStart<'_>) -> Result<u64, AzureError> {
    let mut size = None;
    loop {
        match reader.read_event().map_err(xml_error)? {
            Event::Start(child) => match child.local_name().as_ref() {
                b"Content-Length" => {
                    let value = read_text(reader, &child)?;
                    let parsed = value
                        .parse::<u64>()
                        .map_err(|_| parse_error(format!("invalid Content-Length {value:?}")))?;
                    set_once(&mut size, parsed, "Content-Length")?;
                }
                _ => skip(reader, &child)?,
            },
            Event::End(end) if end.name().as_ref() == start.to_end().name().as_ref() => {
                return size.ok_or_else(|| parse_error("Blob Properties missing Content-Length"));
            }
            Event::Eof => return Err(parse_error("unexpected EOF inside Properties")),
            _ => {}
        }
    }
}

/// Parse the `DeleteRetentionPolicy` from `Get Blob Service Properties`.
pub(crate) fn parse_delete_retention(xml: &[u8]) -> Result<DeleteRetention, AzureError> {
    let mut reader = reader(xml);
    let mut root_seen = false;
    let mut policy = None;
    loop {
        match reader.read_event().map_err(xml_error)? {
            Event::Start(start) if !root_seen => {
                if start.local_name().as_ref() != b"StorageServiceProperties" {
                    return Err(parse_error("expected StorageServiceProperties root"));
                }
                root_seen = true;
            }
            Event::Start(start) if start.local_name().as_ref() == b"DeleteRetentionPolicy" => {
                set_once(
                    &mut policy,
                    parse_retention_policy(&mut reader, &start)?,
                    "DeleteRetentionPolicy",
                )?;
            }
            Event::Start(start) => skip(&mut reader, &start)?,
            Event::End(end)
                if root_seen && end.local_name().as_ref() == b"StorageServiceProperties" =>
            {
                return Ok(policy.unwrap_or_default());
            }
            Event::Eof => return Err(parse_error("incomplete service properties document")),
            _ => {}
        }
    }
}

fn parse_retention_policy(
    reader: &mut Reader<&[u8]>,
    start: &BytesStart<'_>,
) -> Result<DeleteRetention, AzureError> {
    let mut enabled = None;
    let mut allow_permanent_delete = None;
    loop {
        match reader.read_event().map_err(xml_error)? {
            Event::Start(child) => match child.local_name().as_ref() {
                b"Enabled" => set_once(
                    &mut enabled,
                    parse_bool(&read_text(reader, &child)?, "Enabled")?,
                    "Enabled",
                )?,
                b"AllowPermanentDelete" => set_once(
                    &mut allow_permanent_delete,
                    parse_bool(&read_text(reader, &child)?, "AllowPermanentDelete")?,
                    "AllowPermanentDelete",
                )?,
                _ => skip(reader, &child)?,
            },
            Event::End(end) if end.name().as_ref() == start.to_end().name().as_ref() => {
                return Ok(DeleteRetention {
                    enabled: enabled
                        .ok_or_else(|| parse_error("DeleteRetentionPolicy missing Enabled"))?,
                    allow_permanent_delete: allow_permanent_delete.unwrap_or(false),
                });
            }
            Event::Eof => return Err(parse_error("unexpected EOF inside DeleteRetentionPolicy")),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_versioned_listing_and_empty_next_marker() {
        let xml = "\u{feff}<?xml version=\"1.0\" encoding=\"utf-8\"?>\
            <EnumerationResults ServiceEndpoint=\"http://x/\" ContainerName=\"media\">\
            <Prefix>a/</Prefix><MaxResults>2</MaxResults>\
            <Blobs>\
              <Blob><Name>a/one&amp;two</Name><VersionId>2024-01-01T00:00:00.0000000Z</VersionId>\
                <IsCurrentVersion>true</IsCurrentVersion>\
                <Properties><Etag>0x1</Etag><Content-Length>12</Content-Length></Properties>\
                <OrMetadata /></Blob>\
              <Blob><Name>a/old</Name><VersionId>2023-01-01T00:00:00.0000000Z</VersionId>\
                <Properties><Content-Length>0</Content-Length></Properties></Blob>\
              <Blob><Name>a/plain</Name><Properties><Content-Length>3</Content-Length></Properties></Blob>\
            </Blobs><NextMarker /></EnumerationResults>";
        let page = parse_list_blobs(xml.as_bytes()).expect("listing");
        assert_eq!(page.next_marker, None);
        assert_eq!(
            page.blobs,
            vec![
                ListedBlob {
                    name: "a/one&two".into(),
                    version_id: Some("2024-01-01T00:00:00.0000000Z".into()),
                    is_current_version: true,
                    size: 12,
                },
                ListedBlob {
                    name: "a/old".into(),
                    version_id: Some("2023-01-01T00:00:00.0000000Z".into()),
                    is_current_version: false,
                    size: 0,
                },
                ListedBlob {
                    name: "a/plain".into(),
                    version_id: None,
                    is_current_version: false,
                    size: 3,
                },
            ]
        );

        let truncated = parse_list_blobs(
            b"<EnumerationResults><Blobs /><NextMarker>2!88!abc</NextMarker></EnumerationResults>",
        )
        .expect("empty page");
        assert!(truncated.blobs.is_empty());
        assert_eq!(truncated.next_marker.as_deref(), Some("2!88!abc"));
    }

    #[test]
    fn rejects_listings_outside_the_flat_versioned_shape() {
        let blob = |inner: &str| {
            format!("<EnumerationResults><Blobs><Blob>{inner}</Blob></Blobs><NextMarker/></EnumerationResults>")
        };
        let props = "<Properties><Content-Length>1</Content-Length></Properties>";
        let cases = vec![
            String::new(),
            "<ListVersionsResult/>".into(),
            "<EnumerationResults><NextMarker/></EnumerationResults>".into(),
            "<EnumerationResults><Blobs>".into(),
            "<EnumerationResults><Blobs><BlobPrefix><Name>a/</Name></BlobPrefix></Blobs></EnumerationResults>".into(),
            blob(&format!("<Name Encoded=\"true\">YQ==</Name>{props}")),
            blob(&format!("<Name>a</Name><Snapshot>2024</Snapshot>{props}")),
            blob(&format!("<Name>a</Name><Deleted>true</Deleted>{props}")),
            blob(&format!("<Name>a</Name><Name>b</Name>{props}")),
            blob("<Name>a</Name>"),
            blob("<Name>a</Name><Properties><Content-Length>x</Content-Length></Properties>"),
            blob(&format!("<Name>a</Name><IsCurrentVersion>yes</IsCurrentVersion>{props}")),
            blob(&format!("<Name></Name>{props}")),
        ];
        for case in cases {
            assert!(
                parse_list_blobs(case.as_bytes()).is_err(),
                "accepted {case}"
            );
        }
    }

    #[test]
    fn parses_delete_retention_policy() {
        let xml = b"<?xml version=\"1.0\"?><StorageServiceProperties>\
            <Logging><Version>1.0</Version></Logging>\
            <DeleteRetentionPolicy><Enabled>true</Enabled><Days>7</Days>\
            <AllowPermanentDelete>false</AllowPermanentDelete></DeleteRetentionPolicy>\
            </StorageServiceProperties>";
        assert_eq!(
            parse_delete_retention(xml).expect("policy"),
            DeleteRetention {
                enabled: true,
                allow_permanent_delete: false,
            }
        );
        assert_eq!(
            parse_delete_retention(b"<StorageServiceProperties></StorageServiceProperties>")
                .expect("default"),
            DeleteRetention::default()
        );
        assert!(parse_delete_retention(b"<Other/>").is_err());
        assert!(parse_delete_retention(
            b"<StorageServiceProperties><DeleteRetentionPolicy><Days>1</Days></DeleteRetentionPolicy></StorageServiceProperties>"
        )
        .is_err());
    }
}
