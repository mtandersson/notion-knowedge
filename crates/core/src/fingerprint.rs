//! Version-one fingerprints and conservative cross-run identity reconciliation.
use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};
use url::Url;

use crate::{
    chunking::ChunkDraft,
    indexed::{IndexedChunk, SchemaVersion},
};

/// Canonicalize line endings and recognized signed URL credentials only. All
/// other whitespace, markup, labels and semantic URL parameters remain content.
pub fn normalized_content(text: &str) -> String {
    let text = text.replace("\r\n", "\n").replace('\r', "\n");
    let mut result = String::new();
    let mut cursor = 0;
    while let Some(offset) = text[cursor..].find("http") {
        let start = cursor + offset;
        result.push_str(&text[cursor..start]);
        let end = text[start..]
            .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | '<' | '>' | ')' | ']'))
            .map_or(text.len(), |n| start + n);
        let raw = &text[start..end];
        result.push_str(&canonical_url(raw));
        cursor = end;
    }
    result.push_str(&text[cursor..]);
    result.trim_matches('\n').to_owned()
}

fn canonical_url(raw: &str) -> String {
    let Ok(url) = Url::parse(raw) else {
        return raw.into();
    };
    if !matches!(url.scheme(), "http" | "https") {
        return raw.into();
    }
    let Some(query_start) = raw.find('?') else {
        return raw.into();
    };
    let fragment_start = raw.find('#').unwrap_or(raw.len());
    if query_start > fragment_start {
        return raw.into();
    }
    let params: Vec<_> = raw[query_start + 1..fragment_start].split('&').collect();
    let key = |pair: &str| pair.split('=').next().unwrap_or("").to_ascii_lowercase();
    let aws = params.iter().any(|pair| key(pair) == "x-amz-signature");
    let google = params.iter().any(|pair| key(pair) == "x-goog-signature");
    if !aws && !google {
        return raw.into();
    }
    let volatile = |key: &str| {
        let key = key.to_ascii_lowercase();
        (aws && matches!(
            key.as_str(),
            "x-amz-signature"
                | "x-amz-credential"
                | "x-amz-date"
                | "x-amz-expires"
                | "x-amz-security-token"
                | "x-amz-algorithm"
                | "x-amz-signedheaders"
        )) || (google
            && matches!(
                key.as_str(),
                "x-goog-signature"
                    | "x-goog-credential"
                    | "x-goog-date"
                    | "x-goog-expires"
                    | "x-goog-algorithm"
                    | "x-goog-signedheaders"
            ))
    };
    let retained: Vec<_> = params
        .into_iter()
        .filter(|pair| !volatile(&key(pair)))
        .collect();
    let mut result = raw[..query_start].to_owned();
    if !retained.is_empty() {
        result.push('?');
        result.push_str(&retained.join("&"));
    }
    result.push_str(&raw[fragment_start..]);
    result
}

fn digest(domain: &str, parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    hash.update(domain.as_bytes());
    for part in parts {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    format!("{domain}:{:x}", hash.finalize())
}

/// SHA-256 of canonical searchable text, independent of timestamps/properties.
pub fn content_hash(text: &str) -> String {
    digest("nk-content-v1", &[&normalized_content(text)])
}

type Section = (String, Option<String>, Vec<String>);
fn section(chunk: &IndexedChunk) -> Section {
    (
        chunk.metadata.page_id.clone(),
        chunk.metadata.block_id.clone(),
        chunk.metadata.heading_path.clone(),
    )
}

/// Finalize drafts against the preceding snapshot. Links remain empty for the
/// subsequent link-extraction stage. Returned text is lossless original text;
/// only the fingerprint input is canonicalized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidPreviousSnapshot;

fn chunk_hash(text: &str, headings: &[String]) -> String {
    let normalized = normalized_content(text);
    let mut parts = vec![normalized.as_str()];
    parts.extend(headings.iter().map(String::as_str));
    digest("nk-content-v1", &parts)
}

pub fn identify_chunks(
    drafts: Vec<ChunkDraft>,
    previous: &[IndexedChunk],
) -> Result<Vec<IndexedChunk>, InvalidPreviousSnapshot> {
    let mut ids = BTreeSet::new();
    if previous.iter().any(|c| {
        !c.chunk_id.starts_with("nk-chunk-v1:")
            || !ids.insert(&c.chunk_id)
            || c.content_hash != chunk_hash(&c.text, &c.metadata.heading_path)
    }) {
        return Err(InvalidPreviousSnapshot);
    }
    let mut chunks: Vec<_> = drafts
        .into_iter()
        .map(|draft| IndexedChunk {
            schema_version: SchemaVersion::V1,
            chunk_id: String::new(),
            content_hash: chunk_hash(&draft.text, &draft.metadata.heading_path),
            metadata: draft.metadata,
            text: draft.text,
            links: Vec::new(),
        })
        .collect();
    let mut groups: BTreeMap<Section, Vec<usize>> = BTreeMap::new();
    for (i, chunk) in chunks.iter().enumerate() {
        groups.entry(section(chunk)).or_default().push(i);
    }
    let mut used_ids: BTreeSet<String> = previous.iter().map(|c| c.chunk_id.clone()).collect();
    for (key, indices) in groups {
        let mut candidates: Vec<_> = previous
            .iter()
            .filter(|c| section(c) == key && !c.chunk_id.is_empty())
            .collect();
        for &i in &indices {
            if let Some(n) = candidates
                .iter()
                .position(|old| old.content_hash == chunks[i].content_hash)
            {
                chunks[i].chunk_id = candidates.remove(n).chunk_id.clone();
            }
        }
        let unmatched: Vec<_> = indices
            .iter()
            .copied()
            .filter(|&i| chunks[i].chunk_id.is_empty())
            .collect();
        if unmatched.len() == 1 && candidates.len() == 1 {
            chunks[unmatched[0]].chunk_id = candidates[0].chunk_id.clone();
        }
        for i in unmatched {
            if !chunks[i].chunk_id.is_empty() {
                continue;
            }
            let mut parts = vec![key.0.as_str(), key.1.as_deref().unwrap_or("")];
            parts.extend(key.2.iter().map(String::as_str));
            parts.push(&chunks[i].content_hash);
            let base = digest("nk-chunk-v1", &parts);
            let mut suffix = 0;
            let mut id = base.clone();
            while used_ids.contains(&id) {
                suffix += 1;
                id = format!("{base}:{suffix}");
            }
            used_ids.insert(id.clone());
            chunks[i].chunk_id = id;
        }
    }
    Ok(chunks)
}
