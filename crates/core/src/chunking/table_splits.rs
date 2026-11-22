//! Candidate cuts for table sections that exceed embedding admission.
use pulldown_cmark::{Event, Options, Parser, Tag};

use super::html::HtmlSpans;

/// Ordered byte offsets in a single original chunk. Plain text around a
/// completed standalone HTML table is parsed separately, since CommonMark can
/// read the lines after its closing tag as part of the same raw HTML block.
/// Complete table rows and inline Markdown remain contiguous.
pub fn table_section_boundaries(text: &str) -> Vec<usize> {
    // Case-insensitive marker only to avoid parsing ordinary chunks twice.
    if !text
        .as_bytes()
        .windows(6)
        .any(|prefix| prefix.eq_ignore_ascii_case(b"<table"))
    {
        return Vec::new();
    }
    let mut html = HtmlSpans::tracking_tables();
    let mut depth = 0;
    let mut in_block = false;
    for (event, range) in markdown(text) {
        match event {
            Event::Start(tag) => {
                if depth == 0 {
                    in_block = matches!(tag, Tag::HtmlBlock | Tag::Paragraph);
                }
                depth += 1;
            }
            Event::End(_) => depth -= 1,
            Event::Html(_) | Event::InlineHtml(_) => {
                html.observe(
                    &text[range.clone()],
                    range.start,
                    depth == 0 || (depth == 1 && in_block),
                );
            }
            _ => {}
        }
    }
    let (mut protected, tables) = html.finish_tables(text.len());
    let mut outside_tables = Vec::new();
    let mut row_ends = Vec::new();
    let mut cursor = 0;
    for (table, rows) in tables
        .into_completed()
        .filter(|(table, _)| on_own_lines(text, table.start, table.end))
    {
        markdown_boundaries(text, cursor, table.start, &mut outside_tables);
        outside_tables.extend([table.start, table.end]);
        // Leave the final row and the closing table markup together.
        row_ends.extend(rows.iter().take(rows.len().saturating_sub(1)).copied());
        cursor = table.end;
    }
    if cursor == 0 {
        return Vec::new();
    }
    markdown_boundaries(text, cursor, text.len(), &mut outside_tables);
    // Sweep overlapping protected spans and sorted cuts once. Table row
    // cuts are explicitly allowed inside their enclosing table span.
    outside_tables.sort_unstable();
    protected.sort_unstable_by_key(|span| span.start);
    let mut spans = protected.into_iter().peekable();
    let mut protected_until = 0;
    outside_tables.retain(|&cut| {
        while let Some(span) = spans.next_if(|span| span.start < cut) {
            protected_until = protected_until.max(span.end);
        }
        cut >= protected_until
    });
    outside_tables.extend(row_ends);
    outside_tables.sort_unstable();
    outside_tables.dedup();
    let mut previous = 0;
    let last_non_whitespace = text.trim_end().len();
    outside_tables.retain(|&cut| {
        if cut >= last_non_whitespace || text[previous..cut].trim().is_empty() {
            return false;
        }
        previous = cut;
        true
    });
    outside_tables
}

fn on_own_lines(text: &str, start: usize, end: usize) -> bool {
    let before = text[..start].rsplit('\n').next().unwrap_or_default();
    let after = text[end..].split('\n').next().unwrap_or_default();
    before.trim().is_empty() && after.trim().is_empty()
}

fn markdown(text: &str) -> impl Iterator<Item = (Event<'_>, std::ops::Range<usize>)> {
    Parser::new_ext(
        text,
        Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS,
    )
    .into_offset_iter()
}

fn markdown_boundaries(text: &str, start: usize, end: usize, cuts: &mut Vec<usize>) {
    let slice = &text[start..end];
    let mut tags = Vec::new();
    for (event, range) in markdown(slice) {
        match event {
            Event::Start(tag) => {
                if tags.is_empty()
                    || (matches!(tags.as_slice(), [Tag::List(_)]) && tag == Tag::Item)
                {
                    cuts.push(start + range.start);
                }
                tags.push(tag);
            }
            Event::End(_) => {
                tags.pop();
            }
            Event::Text(_)
                if matches!(
                    tags.as_slice(),
                    [Tag::Paragraph]
                        | [Tag::List(_), Tag::Item]
                        | [Tag::List(_), Tag::Item, Tag::Paragraph]
                ) =>
            {
                let mut last = range.start;
                for (offset, ch) in slice[range.clone()].char_indices() {
                    let cut = range.start + offset + ch.len_utf8();
                    if ch.is_whitespace() && cut - last >= 128 && cut < range.end {
                        cuts.push(start + cut);
                        last = cut;
                    }
                }
            }
            _ => {}
        }
    }
}
