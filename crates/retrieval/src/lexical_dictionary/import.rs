//! Offline archive boundary. Never extract paths or resolve XML resources.
use sha2::{Digest, Sha256, Sha512};
use std::{collections::BTreeSet, io::Read, path::Component};

const MIB: u64 = 1024 * 1024;
const MAX_COMPRESSED: u64 = 16 * MIB;
const MAX_DECODED: u64 = 64 * MIB;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportError {
    InvalidArchive,
    Bounds,
    Identity,
    InvalidXml,
    Io,
}
impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidArchive => "invalid lexical archive",
            Self::Bounds => "lexical asset resource bound exceeded",
            Self::Identity => "lexical asset identity mismatch",
            Self::InvalidXml => "invalid lexical XML",
            Self::Io => "lexical asset IO unavailable",
        })
    }
}
impl std::error::Error for ImportError {}

type Result<T> = std::result::Result<T, ImportError>;
pub(super) struct Pins<'a> {
    pub(super) archive: &'a str,
    archive_sha512: &'a str,
    pub(super) tei: &'a str,
    pub(super) license: &'a str,
}
pub(super) const PINS: Pins<'static> = Pins {
    archive: "3c9f80c800d4810e34a80c82aca4e468fbc3180099d8b24a4fad7c61324015dc",
    archive_sha512: "ba08420434e6cdff8fd9ce697347aa4ecb214fabba7b67e7eafdc88f013d09fea0959a57c59b2b4dfce1664955078d3390111a4be4fd97ce1a946e3a8d408b5d",
    tei: "333a59ee913deabd66f724d404640c73d0dd53fddf681ae20d832e140c69845f",
    license: "41a8f57655bae359bea4a7e99c7a2aed171687d3dfb49614214538ba6dcbe75b",
};
pub(super) fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(super) struct Members {
    pub(super) tei: Vec<u8>,
    pub(super) license: Vec<u8>,
}

pub(super) fn read_bounded(reader: impl Read, bound: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(bound + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ImportError::Io)?;
    if bytes.len() as u64 > bound {
        return Err(ImportError::Bounds);
    }
    Ok(bytes)
}

pub(super) fn verified_members(bytes: &[u8], pins: &Pins<'_>) -> Result<Members> {
    if bytes.len() as u64 > MAX_COMPRESSED {
        return Err(ImportError::Bounds);
    }
    if sha256(bytes) != pins.archive
        || format!("{:x}", Sha512::digest(bytes)) != pins.archive_sha512
    {
        return Err(ImportError::Identity);
    }
    let stream = xz2::stream::Stream::new_stream_decoder(MAX_DECODED, 0)
        .map_err(|_| ImportError::InvalidArchive)?;
    let decoder = xz2::read::XzDecoder::new_stream(bytes, stream);
    // Drain the entire decompressor first: the TAR end marker must not hide
    // excessive trailing decoded data or an incomplete compressed stream.
    let decoded = read_bounded(decoder, MAX_DECODED)?;
    let mut archive = tar::Archive::new(decoded.as_slice());
    let mut seen = BTreeSet::new();
    let mut tei = None;
    let mut license = None;
    for item in archive
        .entries()
        .map_err(|_| ImportError::InvalidArchive)?
        .raw(true)
    {
        let mut item = item.map_err(|_| ImportError::InvalidArchive)?;
        if !item.header().entry_type().is_file() {
            return Err(ImportError::InvalidArchive);
        }
        let path = item
            .path()
            .map_err(|_| ImportError::InvalidArchive)?
            .into_owned();
        if path
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
        {
            return Err(ImportError::InvalidArchive);
        }
        let name = path.to_str().ok_or(ImportError::InvalidArchive)?;
        if name.contains('\\') || !seen.insert(name.to_owned()) || seen.len() > 1024 {
            return Err(ImportError::InvalidArchive);
        }
        if item.size() > MAX_DECODED {
            return Err(ImportError::Bounds);
        }
        match name {
            "eng-swe/eng-swe.tei" => tei = Some(read_bounded(&mut item, MAX_DECODED)?),
            "eng-swe/COPYING" => license = Some(read_bounded(&mut item, MIB)?),
            _ => {} // Skip other regular source members without extraction.
        }
    }
    let tei = tei.ok_or(ImportError::InvalidArchive)?;
    let license = license.ok_or(ImportError::InvalidArchive)?;
    if sha256(&tei) != pins.tei || sha256(&license) != pins.license {
        return Err(ImportError::Identity);
    }
    Ok(Members { tei, license })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn archive(members: &[(&str, &[u8], tar::EntryType)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (name, data, kind) in members {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_entry_type(*kind);
            header.set_cksum();
            builder.append_data(&mut header, name, *data).unwrap();
        }
        let tar = builder.into_inner().unwrap();
        let mut encoder = xz2::read::XzEncoder::new(tar.as_slice(), 1);
        let mut result = Vec::new();
        encoder.read_to_end(&mut result).unwrap();
        result
    }
    fn check(bytes: &[u8]) -> Result<Members> {
        verified_members(
            bytes,
            &Pins {
                archive: &sha256(bytes),
                archive_sha512: &format!("{:x}", Sha512::digest(bytes)),
                tei: &sha256(b"tei"),
                license: &sha256(b"license"),
            },
        )
    }
    #[test]
    fn archive_accepts_only_verified_regular_unique_members() {
        let regular = tar::EntryType::Regular;
        let bytes = archive(&[
            ("eng-swe/eng-swe.tei", b"tei", regular),
            ("eng-swe/COPYING", b"license", regular),
        ]);
        let members = check(&bytes).unwrap();
        assert_eq!(members.tei, b"tei");
        assert_eq!(members.license, b"license");
        assert!(matches!(
            verified_members(&bytes, &PINS),
            Err(ImportError::Identity)
        ));
        let duplicate = archive(&[
            ("eng-swe/eng-swe.tei", b"tei", regular),
            ("eng-swe/eng-swe.tei", b"tei", regular),
        ]);
        assert!(matches!(
            check(&duplicate),
            Err(ImportError::InvalidArchive)
        ));
        let link = archive(&[("eng-swe/eng-swe.tei", b"", tar::EntryType::Symlink)]);
        assert!(matches!(check(&link), Err(ImportError::InvalidArchive)));
        let missing = archive(&[("eng-swe/COPYING", b"license", regular)]);
        assert!(matches!(check(&missing), Err(ImportError::InvalidArchive)));
        let wrong = archive(&[
            ("eng-swe/eng-swe.tei", b"changed", regular),
            ("eng-swe/COPYING", b"license", regular),
        ]);
        assert!(matches!(check(&wrong), Err(ImportError::Identity)));
    }
    #[test]
    fn bounded_reads_reject_overflow_without_consuming_unbounded_input() {
        assert_eq!(
            read_bounded(b"12345".as_slice(), 4),
            Err(ImportError::Bounds)
        );
        assert_eq!(read_bounded(b"1234".as_slice(), 4).unwrap(), b"1234");
        assert!(check(b"not an xz stream").is_err());
    }
}

pub(super) fn parse_tei(bytes: &[u8]) -> Result<Vec<super::Edge>> {
    use super::{Edge, literal};
    use quick_xml::{Reader, events::Event};
    if bytes.len() as u64 > MAX_DECODED {
        return Err(ImportError::Bounds);
    }
    std::str::from_utf8(bytes).map_err(|_| ImportError::InvalidXml)?;
    let mut reader = Reader::from_reader(bytes);
    reader.config_mut().expand_empty_elements = true;
    let mut stack: Vec<(Vec<u8>, bool)> = Vec::new();
    let mut ordinal = 0u32;
    let mut entry_depth = None;
    let mut sources = Vec::new();
    let mut targets = Vec::new();
    // (kind, depth, text, nested-markup). Do not concatenate definitions/usage
    // hidden inside inline elements into a literal dictionary relation.
    let mut capture: Option<(bool, usize, String, bool)> = None;
    let mut edges = Vec::new();
    let mut edge_resident = 0usize;
    let mut root_seen = false;
    let mut root_closed = false;
    let mut doctype = false;
    loop {
        match reader.read_event().map_err(|_| ImportError::InvalidXml)? {
            Event::DocType(value) => {
                if doctype || root_seen || value.as_ref() != b"TEI SYSTEM \"freedict-P5.dtd\"" {
                    return Err(ImportError::InvalidXml);
                }
                doctype = true;
            }
            Event::Start(value) => {
                if stack.len() == 32 {
                    return Err(ImportError::Bounds);
                }
                let name = value.name().as_ref().to_vec();
                if name.contains(&b':') || root_closed {
                    return Err(ImportError::InvalidXml);
                }
                if stack.is_empty() {
                    if root_seen || name != b"TEI" {
                        return Err(ImportError::InvalidXml);
                    }
                    root_seen = true;
                }
                let mut trans = false;
                let mut swedish = false;
                for attribute in value.attributes() {
                    let a = attribute.map_err(|_| ImportError::InvalidXml)?;
                    if a.key.as_ref() == b"type" {
                        trans = a.value.as_ref() == b"trans";
                    }
                    if a.key.as_ref() == b"xml:lang" {
                        swedish = a.value.as_ref() == b"sv";
                    }
                }
                if let Some(c) = capture.as_mut() {
                    c.3 = true;
                }
                if name == b"entry" {
                    if entry_depth.is_some()
                        || !stack.iter().map(|x| x.0.as_slice()).eq([
                            b"TEI".as_slice(),
                            b"text",
                            b"body",
                        ])
                    {
                        return Err(ImportError::InvalidXml);
                    }
                    ordinal += 1;
                    if ordinal > 250_000 {
                        return Err(ImportError::Bounds);
                    }
                    entry_depth = Some(stack.len() + 1);
                }
                let parent = stack.last().map(|x| x.0.as_slice());
                let source = name == b"orth"
                    && parent == Some(b"form")
                    && entry_depth == Some(stack.len() - 1);
                let target = name == b"quote"
                    && stack.last().is_some_and(|x| x.0 == b"cit" && x.1)
                    && entry_depth.is_some_and(|depth| {
                        stack[depth..stack.len() - 1]
                            .iter()
                            .all(|x| x.0 == b"sense")
                    });
                if source || target {
                    capture = Some((source, stack.len() + 1, String::new(), false));
                }
                stack.push((name, trans && swedish));
            }
            Event::Text(value) => {
                let text = value.decode().map_err(|_| ImportError::InvalidXml)?;
                if stack.is_empty() && !text.trim().is_empty() {
                    return Err(ImportError::InvalidXml);
                }
                if let Some(c) = capture.as_mut() {
                    if c.2.len() + text.len() > 4096 {
                        c.3 = true;
                    } else {
                        c.2.push_str(&text);
                    }
                }
            }
            Event::CData(value) => {
                if let Some(c) = capture.as_mut() {
                    let text = value.decode().map_err(|_| ImportError::InvalidXml)?;
                    if c.2.len() + text.len() > 4096 {
                        c.3 = true;
                    } else {
                        c.2.push_str(&text);
                    }
                }
            }
            Event::GeneralRef(value) => {
                let character = if let Some(c) = value
                    .resolve_char_ref()
                    .map_err(|_| ImportError::InvalidXml)?
                {
                    c
                } else {
                    match value.as_ref() {
                        b"amp" => '&',
                        b"lt" => '<',
                        b"gt" => '>',
                        b"quot" => '"',
                        b"apos" => '\'',
                        _ => return Err(ImportError::InvalidXml),
                    }
                };
                if let Some(c) = capture.as_mut() {
                    if c.2.len() < 4096 {
                        c.2.push(character);
                    } else {
                        c.3 = true;
                    }
                }
            }
            Event::End(_) => {
                if capture.as_ref().is_some_and(|c| c.1 == stack.len()) {
                    let (source, _, text, nested) =
                        capture.take().ok_or(ImportError::InvalidXml)?;
                    if !nested && let Some(text) = literal(&text) {
                        let destination = if source { &mut sources } else { &mut targets };
                        if destination.len() >= 1_000_000 {
                            return Err(ImportError::Bounds);
                        }
                        destination.push(text);
                    }
                }
                if entry_depth == Some(stack.len()) {
                    sources.sort();
                    sources.dedup();
                    targets.sort();
                    targets.dedup();
                    for source in &sources {
                        for target in &targets {
                            if edges.len() + 2 > 1_000_000 {
                                return Err(ImportError::Bounds);
                            }
                            edge_resident = edge_resident
                                .checked_add(
                                    2 * (source.len()
                                        + target.len()
                                        + std::mem::size_of::<Edge>()
                                        + 128),
                                )
                                .ok_or(ImportError::Bounds)?;
                            if edge_resident > 256 * 1024 * 1024 {
                                return Err(ImportError::Bounds);
                            }
                            edges.push(Edge {
                                source: source.clone().into(),
                                target: target.clone().into(),
                                entry: ordinal,
                            });
                            edges.push(Edge {
                                source: target.clone().into(),
                                target: source.clone().into(),
                                entry: ordinal,
                            });
                        }
                    }
                    sources.clear();
                    targets.clear();
                    entry_depth = None;
                }
                stack.pop().ok_or(ImportError::InvalidXml)?;
                if stack.is_empty() {
                    root_closed = true;
                }
            }
            Event::Eof => break,
            Event::Decl(value) => {
                if root_seen
                    || value
                        .encoding()
                        .is_some_and(|v| v.is_err() || v.is_ok_and(|v| v.as_ref() != b"UTF-8"))
                {
                    return Err(ImportError::InvalidXml);
                }
            }
            Event::PI(_) | Event::Comment(_) => {} // Never dereferenced.
            Event::Empty(_) => return Err(ImportError::InvalidXml),
        }
    }
    if !root_closed || !stack.is_empty() || entry_depth.is_some() {
        return Err(ImportError::InvalidXml);
    }
    edges.sort_by(|a, b| (&a.source, &a.target, a.entry).cmp(&(&b.source, &b.target, b.entry)));
    edges.dedup_by(|a, b| a.source == b.source && a.target == b.target);
    Ok(edges)
}

#[cfg(test)]
mod xml_tests {
    use super::*;
    fn document(entry: &str) -> Vec<u8> {
        format!("<TEI><text><body>{entry}</body></text></TEI>").into_bytes()
    }
    #[test]
    fn only_literal_direct_forms_and_swedish_translations_become_relations() {
        let xml = document(
            r#"<entry><form><orth>harbour</orth></form><sense>
            <def>definition</def><cit type="example" xml:lang="sv"><quote>exempel</quote></cit>
            <cit type="trans" xml:lang="de"><quote>Hafen</quote></cit>
            <sense><cit type="trans" xml:lang="sv"><quote>hamn</quote><quote>skyddad vik</quote></cit></sense>
            </sense></entry>"#,
        );
        let edges = parse_tei(&xml).unwrap();
        assert_eq!(edges.len(), 4);
        assert!(edges.iter().any(|e| e.source.as_ref() == "harbour"
            && e.target.as_ref() == "hamn"
            && e.entry == 1));
        assert!(!edges.iter().any(|e| e.target.contains("definition")
            || e.target.contains("exempel")
            || e.target.contains("hafen")));
    }
    #[test]
    fn xml_entities_external_resources_and_unbounded_depth_are_rejected() {
        for prefix in [
            r#"<!DOCTYPE TEI SYSTEM "https://example.invalid/evil">"#,
            r#"<!DOCTYPE TEI [<!ENTITY evil "secret">]>"#,
        ] {
            let mut xml = prefix.as_bytes().to_vec();
            xml.extend(document(""));
            assert!(matches!(parse_tei(&xml), Err(ImportError::InvalidXml)));
        }
        let mut allowed = b"<!DOCTYPE TEI SYSTEM \"freedict-P5.dtd\">".to_vec();
        allowed.extend(document(""));
        assert!(parse_tei(&allowed).is_ok());
        assert!(
            parse_tei(&document(
                "<entry><form><orth>&unknown;</orth></form></entry>"
            ))
            .is_err()
        );
        assert!(parse_tei(&document("<xi:include href=\"file:///secret\"/>")).is_err());
        let deep = format!("{}{}", "<x>".repeat(33), "</x>".repeat(33));
        assert!(matches!(
            parse_tei(&document(&deep)),
            Err(ImportError::Bounds)
        ));
        assert!(parse_tei(b"<TEI><text>").is_err());
    }
}
