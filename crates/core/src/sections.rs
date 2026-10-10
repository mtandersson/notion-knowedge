//! Pure, lossless section selection for an eventual bounded Notion edit.
//!
//! This module NEVER mutates a page or sends a whole-page replacement. It
//! identifies one unambiguous literal ATX heading in authoritative Markdown,
//! computes its byte-accurate body range, and supplies a strict readback check.
//! Integration with Notion's supported range-edit operation belongs to #312.
use std::ops::Range;

use pulldown_cmark::{Event, HeadingLevel, Options, Parser, Tag};
use sha2::{Digest, Sha256};

/// Only exact, single-line, unformatted ATX heading labels are accepted.
/// A caller cannot choose a substring, byte offset or fuzzy match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionAnchor {
    pub level: u8,
    pub heading: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectionError {
    InvalidInput,
    NotFound,
    Ambiguous,
    Unsupported,
    NoChange,
}

impl std::fmt::Display for SectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "section selection failed: {self:?}")
    }
}
impl std::error::Error for SectionError {}

/// A proposal calculated from *complete, fresh* Markdown. The selected body
/// includes all nested lower-level headings until the next same/higher-level
/// heading; the identifying heading itself is never altered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionPlan {
    pub anchor: SectionAnchor,
    /// UTF-8 boundary offsets in the original Markdown, not Notion block IDs.
    pub body_range: Range<usize>,
    /// SHA-256 of original Markdown, for strict fresh-source comparison.
    pub original_sha256: String,
    /// SHA-256 of the proposed full Markdown, for readback verification.
    pub proposed_sha256: String,
    original_body: String,
    replacement: String,
    before: String,
    after: String,
}

impl SectionPlan {
    pub fn original_body(&self) -> &str {
        &self.original_body
    }

    pub fn replacement_body(&self) -> &str {
        &self.replacement
    }

    /// The original, byte-identical context before the selected body.
    pub fn untouched_prefix(&self) -> &str {
        &self.before
    }

    /// The original, byte-identical context after the selected body.
    pub fn untouched_suffix(&self) -> &str {
        &self.after
    }

    /// A readback passes only when every byte is exactly the planned output.
    /// This does not make an upstream write atomic or authorize a mutation.
    pub fn verify_readback(&self, markdown: &str) -> bool {
        markdown.len() == self.before.len() + self.replacement.len() + self.after.len()
            && markdown.starts_with(&self.before)
            && markdown[self.before.len()..].starts_with(&self.replacement)
            && markdown.ends_with(&self.after)
            && sha256(markdown) == self.proposed_sha256
    }

    /// Construct a candidate for local tests and diff previews ONLY. Do not
    /// send it to a whole-page replacement endpoint (see #312).
    pub fn preview(&self) -> String {
        let mut output =
            String::with_capacity(self.before.len() + self.replacement.len() + self.after.len());
        output.push_str(&self.before);
        output.push_str(&self.replacement);
        output.push_str(&self.after);
        output
    }
}

fn sha256(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

fn level_number(level: HeadingLevel) -> u8 {
    match level {
        HeadingLevel::H1 => 1,
        HeadingLevel::H2 => 2,
        HeadingLevel::H3 => 3,
        HeadingLevel::H4 => 4,
        HeadingLevel::H5 => 5,
        HeadingLevel::H6 => 6,
    }
}

fn valid_anchor(anchor: &SectionAnchor) -> bool {
    (1..=6).contains(&anchor.level)
        && !anchor.heading.is_empty()
        && anchor.heading.chars().count() <= 200
        && anchor.heading.trim() == anchor.heading
        && !anchor.heading.chars().any(|c| c.is_control())
        && !anchor.heading.contains('<')
        && !anchor.heading.contains('>')
        && !anchor.heading.contains('`')
        && !anchor.heading.starts_with('#')
}

/// Only use heading events returned by the CommonMark parser, and require that
/// the literal source line starts at column 0 with exactly "#...# title".
/// Fenced examples, blockquoted headings, setext headings, list headings,
/// inline markup and headings with attributes are not valid target anchors.
fn exact_heading(markdown: &str, start: usize, anchor: &SectionAnchor) -> bool {
    if start > markdown.len() || (start > 0 && markdown.as_bytes()[start - 1] != b'\n') {
        return false;
    }
    let line = &markdown[start..];
    let end = line.find('\n').unwrap_or(line.len());
    let line = line[..end].strip_suffix('\r').unwrap_or(&line[..end]);
    let expected = format!(
        "{} {}",
        "#".repeat(usize::from(anchor.level)),
        anchor.heading
    );
    line == expected
}

/// Plan only the section *body* (not its heading). Because text is sliced from
/// parser event byte offsets, all unrelated Markdown retains exact bytes,
/// including Unicode, whitespace and CRLF. No content is changed by planning.
pub fn plan_section(
    markdown: &str,
    anchor: SectionAnchor,
    replacement: &str,
) -> Result<SectionPlan, SectionError> {
    if !valid_anchor(&anchor)
        || markdown.len() > 1024 * 1024
        || replacement.len() > 200_000
        || replacement.contains('\0')
    {
        return Err(SectionError::InvalidInput);
    }

    // pulldown-cmark offsets refer to original UTF-8 byte positions. The
    // first exact ATX heading is not enough; scan the entire document for
    // duplicates, and only then compute a section boundary.
    let mut headings = Vec::<(usize, u8)>::new();
    let mut matches = Vec::<usize>::new();
    for (event, range) in Parser::new_ext(markdown, Options::all()).into_offset_iter() {
        if let Event::Start(Tag::Heading { level, .. }) = event {
            let depth = level_number(level);
            let start = range.start;
            // Only top-level heading markers can bound a page section.
            // Nested list/blockquote headings have a prefix or indentation.
            // Setext headings at column zero still bound a prior section.
            let top_level = start == 0 || markdown.as_bytes()[start - 1] == b'\n';
            if !top_level {
                continue;
            }
            if depth == anchor.level && exact_heading(markdown, start, &anchor) {
                matches.push(start);
            }
            headings.push((start, depth));
        }
    }
    let start = match matches.as_slice() {
        [] => return Err(SectionError::NotFound),
        [one] => *one,
        _ => return Err(SectionError::Ambiguous),
    };

    let heading_tail = &markdown[start..];
    let heading_end = heading_tail
        .find('\n')
        .map(|offset| start + offset + 1)
        .unwrap_or(markdown.len());
    if heading_end == markdown.len() && !markdown.ends_with('\n') && !replacement.is_empty() {
        return Err(SectionError::Unsupported);
    }

    let end = headings
        .iter()
        .filter(|(offset, depth)| *offset > start && *depth <= anchor.level)
        .map(|(offset, _)| *offset)
        .min()
        .unwrap_or(markdown.len());
    if end < heading_end || !markdown.is_char_boundary(end) {
        return Err(SectionError::Unsupported);
    }

    // Enhanced Notion XML cannot safely be reconstructed from an arbitrary
    // Markdown range edit. Explicitly deny destructive replacement of such
    // selected bodies; unaffected content outside the section is preserved.
    let original_body = &markdown[heading_end..end];
    if ["<unknown", "<page ", "<database ", "<data-source "]
        .iter()
        .any(|marker| original_body.contains(marker))
    {
        return Err(SectionError::Unsupported);
    }

    // A sibling heading must remain on its own line, not get merged into the
    // caller's last paragraph. The caller controls every newline explicitly.
    if end < markdown.len() && !replacement.is_empty() && !replacement.ends_with('\n') {
        return Err(SectionError::InvalidInput);
    }
    if original_body == replacement {
        return Err(SectionError::NoChange);
    }
    let before = markdown[..heading_end].to_owned();
    let after = markdown[end..].to_owned();
    let mut proposed = String::with_capacity(before.len() + replacement.len() + after.len());
    proposed.push_str(&before);
    proposed.push_str(replacement);
    proposed.push_str(&after);
    if proposed.len() > 1024 * 1024 {
        return Err(SectionError::InvalidInput);
    }
    Ok(SectionPlan {
        anchor,
        body_range: heading_end..end,
        original_sha256: sha256(markdown),
        proposed_sha256: sha256(&proposed),
        original_body: original_body.to_owned(),
        replacement: replacement.to_owned(),
        before,
        after,
    })
}
