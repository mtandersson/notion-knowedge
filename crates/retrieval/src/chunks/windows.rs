//! Stable internal vector IDs for byte ranges of one canonical text chunk.
use std::ops::Range;

use sha2::{Digest, Sha256};

use super::{ChunkTableError, IndexedChunk};

const PREFIX: &str = "nk-window-v1:";
const CHUNK: &str = "nk-chunk-v1:";

pub(super) struct Window<'a> {
    anchor: &'a str,
    fingerprint: &'a str,
    pub range: Range<usize>,
}

fn hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn valid_anchor(anchor: &str) -> bool {
    match anchor.split_once(':') {
        None => hex_digest(anchor),
        Some((hash, suffix)) => hex_digest(hash) && suffix.parse::<u64>().is_ok_and(|n| n > 0),
    }
}

pub(super) fn digest(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

impl Window<'_> {
    pub fn anchor(&self) -> String {
        format!("{CHUNK}{}", self.anchor)
    }

    pub fn matches(&self, canonical: &str, vector_text: &str) -> bool {
        canonical.get(self.range.clone()) == Some(vector_text)
            && digest(canonical) == self.fingerprint
    }
}

pub(super) fn parse(id: &str) -> Result<Option<Window<'_>>, ChunkTableError> {
    let Some(encoded) = id.strip_prefix(PREFIX) else {
        return Ok(None);
    };
    let invalid = || ChunkTableError::InvalidRows("invalid embedding window identity".into());
    let mut fields = encoded.split('~');
    let anchor = fields
        .next()
        .filter(|value| valid_anchor(value))
        .ok_or_else(invalid)?;
    let fingerprint = fields
        .next()
        .filter(|value| hex_digest(value))
        .ok_or_else(invalid)?;
    let start = fields
        .next()
        .and_then(|value| value.parse().ok())
        .ok_or_else(invalid)?;
    let end = fields
        .next()
        .and_then(|value| value.parse().ok())
        .ok_or_else(invalid)?;
    if start >= end || fields.next().is_some() {
        return Err(invalid());
    }
    Ok(Some(Window {
        anchor,
        fingerprint,
        range: start..end,
    }))
}

pub(super) fn anchor_pattern(id: &str) -> Option<String> {
    let anchor = id.strip_prefix(CHUNK).filter(|value| valid_anchor(value))?;
    Some(format!("{PREFIX}{anchor}~%"))
}

pub(super) fn row(
    chunk: &IndexedChunk,
    fingerprint: &str,
    range: Range<usize>,
) -> Result<IndexedChunk, ChunkTableError> {
    let invalid = || ChunkTableError::InvalidRows("invalid embedding window source".into());
    let anchor = chunk
        .chunk_id
        .strip_prefix(CHUNK)
        .filter(|value| valid_anchor(value))
        .ok_or_else(invalid)?;
    let text = chunk.text.get(range.clone()).ok_or_else(invalid)?;
    let mut row = chunk.clone();
    row.chunk_id = format!(
        "{PREFIX}{anchor}~{fingerprint}~{}~{}",
        range.start, range.end
    );
    row.text = text.to_owned();
    row.content_hash = notion_knowledge_core::fingerprint::content_hash(text);
    Ok(row)
}
