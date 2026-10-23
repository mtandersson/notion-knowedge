//! Canonical bounded binary bundle; immutable owned data after one verified read.
use super::import::{ImportError, PINS, parse_tei, read_bounded, sha256, verified_members};
use super::{Dictionary, Edge, PLANNER_VERSION, literal};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
};
const PAYLOAD_MAX: u64 = 128 * 1024 * 1024;
const RESIDENT_MAX: usize = 256 * 1024 * 1024;
const MAGIC: &[u8] = b"NKLEX001";
const ATTRIBUTION: &str = "FreeDict/WikDict English–Swedish 2025.11.23; Karl Bartel; Wiktionary via DBnary; CC-BY-SA-3.0 Unported. Literal pairs normalized and inverted; no source definitions/examples included. https://download.freedict.org/dictionaries/eng-swe/2025.11.23/ https://creativecommons.org/licenses/by-sa/3.0/legalcode";
type Result<T> = std::result::Result<T, ImportError>;
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Identity {
    schema: u32,
    planner: String,
    source_revision: String,
    source_sha256: String,
    lock_sha256: String,
    compiler: String,
    compiler_unicode: (u8, u8, u8),
    normalization: String,
    segmentation: String,
    archive_sha256: String,
    tei_sha256: String,
    license_sha256: String,
    attribution: String,
}
fn identity() -> Identity {
    let source = concat!(
        include_str!("../lexical_dictionary.rs"),
        include_str!("import.rs"),
        include_str!("bundle.rs"),
        include_str!("../../build.rs")
    );
    Identity {
        schema: 1,
        planner: PLANNER_VERSION.into(),
        source_revision: env!("NK_LEXICAL_SOURCE_REVISION").into(),
        source_sha256: sha256(source.as_bytes()),
        lock_sha256: sha256(include_bytes!("../../../../Cargo.lock")),
        compiler: env!("NK_LEXICAL_COMPILER").into(),
        compiler_unicode: char::UNICODE_VERSION,
        normalization: format!(
            "unicode-normalization=0.1.25;NFC;lowercase;NFC;{:?}",
            unicode_normalization::UNICODE_VERSION
        ),
        segmentation: "unicode-segmentation=1.13.3;Unicode=17.0.0;word-boundaries".into(),
        archive_sha256: PINS.archive.into(),
        tei_sha256: PINS.tei.into(),
        license_sha256: PINS.license.into(),
        attribution: ATTRIBUTION.into(),
    }
}
/// Public reproducibility data, never corpus/query content. Persist the digest in
/// trusted index binding; a digest supplied by a tool caller is not authority.
#[derive(Debug, Serialize)]
pub struct AssetInfo {
    pub sha256: String,
    pub payload_bytes: usize,
    pub relations: usize,
    pub importer_identity_sha256: String,
}

#[cfg(unix)]
fn regular_file(path: &Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| ImportError::Io)?;
    if !file.metadata().map_err(|_| ImportError::Io)?.is_file() {
        return Err(ImportError::Io);
    }
    Ok(file)
}
#[cfg(not(unix))]
fn regular_file(_: &Path) -> Result<File> {
    Err(ImportError::Io)
}

/// Offline only; no download, source/index mutation, or runtime installation.
/// Refuses an existing output. The bundle embeds original license/attribution.
pub fn prepare_archive(input: &Path, output: &Path) -> Result<AssetInfo> {
    let archive = read_bounded(regular_file(input)?, 16 * 1024 * 1024)?;
    let members = verified_members(&archive, &PINS)?;
    let edges = parse_tei(&members.tei)?;
    let bytes = encode(&edges, &members.license)?;
    // Round-trip the exact future runtime representation before publishing.
    let digest = sha256(&bytes);
    let dictionary = decode(&bytes, &digest)?;
    let info = AssetInfo {
        sha256: digest,
        payload_bytes: bytes.len(),
        relations: dictionary.edges.len(),
        importer_identity_sha256: sha256(
            &serde_json::to_vec(&identity()).map_err(|_| ImportError::Identity)?,
        ),
    };
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .map_err(|_| ImportError::Io)?;
    if file
        .write_all(&bytes)
        .and_then(|_| file.sync_all())
        .is_err()
    {
        drop(file);
        let _ = std::fs::remove_file(output);
        return Err(ImportError::Io);
    }
    Ok(info)
}
fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let length = u32::try_from(value.len()).map_err(|_| ImportError::Bounds)?;
    if output.len() + 4 + value.len() > PAYLOAD_MAX as usize {
        return Err(ImportError::Bounds);
    }
    output.extend(length.to_le_bytes());
    output.extend(value);
    Ok(())
}
fn encode(edges: &[Edge], license: &[u8]) -> Result<Vec<u8>> {
    let mut out = MAGIC.to_vec();
    put_bytes(
        &mut out,
        &serde_json::to_vec(&identity()).map_err(|_| ImportError::Identity)?,
    )?;
    put_bytes(&mut out, license)?;
    out.extend(
        u32::try_from(edges.len())
            .map_err(|_| ImportError::Bounds)?
            .to_le_bytes(),
    );
    for edge in edges {
        put_bytes(&mut out, edge.source.as_bytes())?;
        put_bytes(&mut out, edge.target.as_bytes())?;
        out.extend(edge.entry.to_le_bytes());
    }
    if out.len() > PAYLOAD_MAX as usize {
        return Err(ImportError::Bounds);
    }
    Ok(out)
}
struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(n).ok_or(ImportError::Bounds)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(ImportError::Identity)?;
        self.offset = end;
        Ok(value)
    }
    fn integer(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| ImportError::Identity)?,
        ))
    }
    fn field(&mut self, max: usize) -> Result<&'a [u8]> {
        let n = self.integer()? as usize;
        if n > max {
            return Err(ImportError::Bounds);
        }
        self.take(n)
    }
}
fn decode(bytes: &[u8], expected: &str) -> Result<Dictionary> {
    if bytes.len() > PAYLOAD_MAX as usize {
        return Err(ImportError::Bounds);
    }
    if sha256(bytes) != expected {
        return Err(ImportError::Identity);
    }
    let mut cursor = Cursor { bytes, offset: 0 };
    if cursor.take(MAGIC.len())? != MAGIC {
        return Err(ImportError::Identity);
    }
    let metadata = cursor.field(16 * 1024)?;
    let actual: Identity = serde_json::from_slice(metadata).map_err(|_| ImportError::Identity)?;
    if actual != identity()
        || serde_json::to_vec(&actual).map_err(|_| ImportError::Identity)? != metadata
    {
        return Err(ImportError::Identity);
    }
    if sha256(cursor.field(1024 * 1024)?) != PINS.license {
        return Err(ImportError::Identity);
    }
    let count = cursor.integer()? as usize;
    if count > 1_000_000 {
        return Err(ImportError::Bounds);
    }
    // Account encoded input plus all retained record/string bytes and generous
    // allocation bookkeeping BEFORE allocating the record vector or any string.
    let entries_start = cursor.offset;
    let mut resident = bytes
        .len()
        .checked_add(64 * 1024)
        .ok_or(ImportError::Bounds)?
        .checked_add(
            count
                .checked_mul(std::mem::size_of::<Edge>() + 128)
                .ok_or(ImportError::Bounds)?,
        )
        .ok_or(ImportError::Bounds)?;
    for _ in 0..count {
        for _ in 0..2 {
            let value = cursor.field(4096)?;
            resident = resident
                .checked_add(value.len())
                .ok_or(ImportError::Bounds)?;
            if resident > RESIDENT_MAX {
                return Err(ImportError::Bounds);
            }
            let value = std::str::from_utf8(value).map_err(|_| ImportError::Identity)?;
            if literal(value).as_deref() != Some(value) {
                return Err(ImportError::Identity);
            }
        }
        let entry = cursor.integer()?;
        if !(1..=250_000).contains(&entry) {
            return Err(ImportError::Identity);
        }
    }
    if cursor.offset != bytes.len() || resident > RESIDENT_MAX {
        return Err(ImportError::Bounds);
    }
    cursor.offset = entries_start;
    let mut edges = Vec::with_capacity(count);
    for _ in 0..count {
        let source = std::str::from_utf8(cursor.field(4096)?)
            .map_err(|_| ImportError::Identity)?
            .into();
        let target = std::str::from_utf8(cursor.field(4096)?)
            .map_err(|_| ImportError::Identity)?
            .into();
        let edge = Edge {
            source,
            target,
            entry: cursor.integer()?,
        };
        if edges
            .last()
            .is_some_and(|last: &Edge| (&last.source, &last.target) >= (&edge.source, &edge.target))
        {
            return Err(ImportError::Identity);
        }
        edges.push(edge);
    }
    Ok(Dictionary {
        edges: edges.into_boxed_slice(),
        manifest_digest: expected.into(),
    })
}
impl Dictionary {
    /// Verify the trusted digest and current importer contract, then own all
    /// immutable data. File replacement after load cannot alter this instance.
    pub fn load(path: &Path, expected_sha256: &str) -> Result<Self> {
        let file = regular_file(path)?;
        let size = file.metadata().map_err(|_| ImportError::Io)?.len();
        if size > PAYLOAD_MAX {
            return Err(ImportError::Bounds);
        }
        // Exact-size storage avoids Read::read_to_end's geometric spare capacity
        // escaping the resident budget. A concurrent size change is a failure.
        let mut bytes = vec![0; size as usize];
        let mut file = file;
        file.read_exact(&mut bytes).map_err(|_| ImportError::Io)?;
        let mut extra = [0];
        if file.read(&mut extra).map_err(|_| ImportError::Io)? != 0 {
            return Err(ImportError::Bounds);
        }
        decode(&bytes, expected_sha256)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malformed_payload_lengths_and_digest_are_rejected() {
        assert!(matches!(
            decode(b"NKLEX001", "wrong"),
            Err(ImportError::Identity)
        ));
        let mut bytes = MAGIC.to_vec();
        bytes.extend(u32::MAX.to_le_bytes());
        assert!(matches!(
            decode(&bytes, &sha256(&bytes)),
            Err(ImportError::Bounds)
        ));
        let mut bytes = MAGIC.to_vec();
        bytes.extend(10u32.to_le_bytes());
        assert!(matches!(
            decode(&bytes, &sha256(&bytes)),
            Err(ImportError::Identity)
        ));
        let mut bytes = MAGIC.to_vec();
        put_bytes(&mut bytes, b"{\"schema\":2}").unwrap();
        assert!(matches!(
            decode(&bytes, &sha256(&bytes)),
            Err(ImportError::Identity)
        ));
    }
    /// The caller supplies only the public pinned archive, never a corpus or
    /// credential. Optional because normal CI does not download external data.
    #[test]
    #[ignore = "requires NK_LEXICAL_TEST_ARCHIVE pointing to the pinned public archive"]
    fn pinned_public_asset_is_reproducible_and_loaded_data_is_immutable() {
        let archive = std::env::var_os("NK_LEXICAL_TEST_ARCHIVE").expect("public archive path");
        let directory =
            std::env::temp_dir().join(format!("nk-lexical-public-{}", std::process::id()));
        std::fs::create_dir(&directory).unwrap();
        let a = directory.join("a.bundle");
        let b = directory.join("b.bundle");
        let first = prepare_archive(Path::new(&archive), &a).unwrap();
        let second = prepare_archive(Path::new(&archive), &b).unwrap();
        assert_eq!(first.sha256, second.sha256);
        let bytes = std::fs::read(&a).unwrap();
        assert_eq!(bytes, std::fs::read(&b).unwrap());
        let dictionary = Dictionary::load(&a, &first.sha256).unwrap();
        assert_eq!(dictionary.manifest_digest(), first.sha256);
        assert!(dictionary.edges.len() > 100_000);
        assert!(Dictionary::load(&a, &"0".repeat(64)).is_err());
        assert!(prepare_archive(Path::new(&archive), &a).is_err()); // no overwrite
        let mut invalid = bytes.clone();
        *invalid.last_mut().unwrap() ^= 1;
        assert!(decode(&invalid, &first.sha256).is_err());
        let mut cursor = Cursor {
            bytes: &bytes,
            offset: MAGIC.len(),
        };
        let original = cursor.field(16 * 1024).unwrap();
        let mut changed: Identity = serde_json::from_slice(original).unwrap();
        changed.schema += 1;
        let mut versioned = MAGIC.to_vec();
        put_bytes(&mut versioned, &serde_json::to_vec(&changed).unwrap()).unwrap();
        versioned.extend(&bytes[cursor.offset..]);
        assert!(decode(&versioned, &sha256(&versioned)).is_err());
        std::fs::write(&a, b"replaced path contents").unwrap();
        assert!(Dictionary::load(&a, &first.sha256).is_err());
        assert_eq!(dictionary.edges.len(), first.relations); // owned snapshot
        assert!(
            dictionary
                .plan("unrecognisedinventedterm", false)
                .phrases()
                .is_empty()
        );
        #[cfg(unix)]
        {
            let link = directory.join("link.bundle");
            std::os::unix::fs::symlink(&b, &link).unwrap();
            assert!(Dictionary::load(&link, &first.sha256).is_err());
        }
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
