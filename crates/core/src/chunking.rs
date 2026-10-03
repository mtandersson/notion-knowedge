//! Lossless semantic chunk drafts; hashing and stable IDs are a separate stage.
use std::ops::Range;

use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};

use crate::indexed::{IndexedDocument, IndexedMetadata};

/// Sizes count Unicode scalar values, not bytes or model-specific tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkConfig {
    pub target_chars: usize,
    pub overlap_chars: usize,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            target_chars: 2000,
            overlap_chars: 200,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidChunkConfig;

/// A contiguous, unmodified slice of the document and its citation metadata.
/// The caller supplies hashes, IDs and per-chunk extracted links in later stages.
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkDraft {
    pub metadata: IndexedMetadata,
    pub text: String,
    pub source_bytes: Range<usize>,
}

/// Split at top-level Markdown block boundaries, never inside lists, fences,
/// tables or inline markup. The target is soft: an oversized atomic block is
/// emitted intact. Overlap repeats complete trailing blocks only, within the
/// overlap budget, and never crosses a heading/section boundary.
pub fn chunk_document(
    document: &IndexedDocument,
    config: ChunkConfig,
) -> Result<Vec<ChunkDraft>, InvalidChunkConfig> {
    if config.target_chars == 0 || config.overlap_chars >= config.target_chars {
        return Err(InvalidChunkConfig);
    }
    let text = &document.text;
    let mut boundaries = vec![0];
    let mut headings = Vec::new();
    let mut depth = 0;
    let mut paragraph = None;
    let mut html_spans = Vec::new();
    let mut heading: Option<(usize, usize, String)> = None;
    for (event, bytes) in Parser::new_ext(
        text,
        Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS,
    )
    .into_offset_iter()
    {
        match event {
            Event::Start(tag) => {
                if depth == 0 {
                    if matches!(tag, Tag::Paragraph) {
                        paragraph = Some(bytes.clone());
                    }
                    boundaries.push(bytes.start);
                    if let Tag::Heading { level, .. } = tag {
                        heading = Some((bytes.start, level as usize, String::new()));
                    }
                }
                depth += 1;
            }
            Event::End(end) => {
                depth -= 1;
                if depth == 0
                    && matches!(end, TagEnd::Paragraph)
                    && let Some(range) = paragraph.take()
                {
                    let raw = &text[range.clone()];
                    // Only ordinary prose can be split without balancing markup.
                    if !raw.contains([
                        '*', '_', '`', '[', ']', '<', '>', '\\', '&', '!', '~', '-', '+', '#', '=',
                        '|',
                    ]) && !raw.contains("  ")
                        && !raw.contains('\t')
                        && !raw.split_whitespace().any(|word| {
                            word.strip_suffix('.')
                                .or_else(|| word.strip_suffix(')'))
                                .is_some_and(|prefix| {
                                    !prefix.is_empty() && prefix.chars().all(|c| c.is_ascii_digit())
                                })
                        })
                    {
                        let mut count = 0;
                        for (offset, ch) in raw.char_indices() {
                            count += 1;
                            if count
                                >= (config.target_chars / 2)
                                    .min(if config.overlap_chars == 0 {
                                        config.target_chars
                                    } else {
                                        config.overlap_chars / 2
                                    })
                                    .max(1)
                                && ch.is_whitespace()
                            {
                                boundaries.push(range.start + offset + ch.len_utf8());
                                count = 0;
                            }
                        }
                    }
                }
                if depth == 0
                    && matches!(end, TagEnd::Heading(_))
                    && let Some(value) = heading.take()
                {
                    headings.push(value);
                }
            }
            Event::Text(value) | Event::Code(value) => {
                if let Some((_, _, title)) = &mut heading {
                    title.push_str(&value);
                }
                if depth == 0 {
                    boundaries.push(bytes.start);
                }
            }
            Event::SoftBreak | Event::HardBreak => {
                if let Some((_, _, title)) = &mut heading {
                    title.push(' ');
                }
            }
            Event::Html(value) | Event::InlineHtml(value) => {
                let token = value.trim_start();
                if let Some(rest) = token.strip_prefix('<') {
                    let name: String = rest
                        .chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '-')
                        .collect();
                    if !name.is_empty()
                        && !token.trim_end().ends_with("/>")
                        && ![
                            "br", "hr", "img", "input", "meta", "link", "wbr", "area", "base",
                            "embed", "source", "track", "col", "param",
                        ]
                        .contains(&name.to_ascii_lowercase().as_str())
                    {
                        let close = format!("</{name}>");
                        if let Some(end) = text[bytes.start..].rfind(&close) {
                            html_spans.push(bytes.start..bytes.start + end + close.len());
                        } else {
                            // An incomplete container is safer as one oversized tail.
                            html_spans.push(bytes.start..text.len());
                        }
                    }
                }
                if depth == 0 {
                    boundaries.push(bytes.start);
                }
            }
            _ => {
                if depth == 0 {
                    boundaries.push(bytes.start);
                }
            }
        }
    }
    boundaries.retain(|p| {
        !html_spans
            .iter()
            .any(|span| *p > span.start && *p < span.end)
    });
    headings.retain(|(p, _, _)| {
        !html_spans
            .iter()
            .any(|span| *p >= span.start && *p < span.end)
    });
    boundaries.push(text.len());
    boundaries.sort_unstable();
    boundaries.dedup();
    let mut sections = Vec::new();
    let mut path: Vec<(usize, String)> = Vec::new();
    let mut start = 0;
    let mut active_path = document.metadata.heading_path.clone();
    for (offset, level, title) in headings {
        if start < offset {
            sections.push((start..offset, active_path));
        }
        path.retain(|(ancestor, _)| *ancestor < level);
        path.push((level, title));
        active_path = document
            .metadata
            .heading_path
            .iter()
            .cloned()
            .chain(path.iter().map(|(_, title)| title.clone()))
            .collect();
        start = offset;
    }
    if start < text.len() {
        sections.push((start..text.len(), active_path));
    }
    let mut chunks = Vec::new();
    for (section, path) in sections {
        let points: Vec<_> = boundaries
            .iter()
            .copied()
            .filter(|p| section.contains(p))
            .chain(std::iter::once(section.end))
            .collect();
        let mut first = 0;
        while first + 1 < points.len() {
            let mut last = first + 1;
            while last + 1 < points.len()
                && text[points[first]..points[last + 1]].chars().count() <= config.target_chars
            {
                last += 1;
            }
            let range = points[first]..points[last];
            if !text[range.clone()].trim().is_empty() {
                let mut metadata = document.metadata.clone();
                metadata.heading_path = path.clone();
                chunks.push(ChunkDraft {
                    metadata,
                    text: text[range.clone()].to_owned(),
                    source_bytes: range,
                });
            }
            if last + 1 == points.len() {
                break;
            }
            let mut next = last;
            while next > first + 1
                && text[points[next - 1]..points[last]].chars().count() <= config.overlap_chars
            {
                next -= 1;
            }
            first = next;
        }
    }
    Ok(chunks)
}
