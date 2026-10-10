//! Deterministic, local relationship normalization. Never follows a target.
use std::ops::Range;

use crate::pages::{page_id, uuid};
use notion_knowledge_core::{
    backend::PageContent,
    indexed::{LinkTarget, PropertyValue},
};
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use quick_xml::{Reader, events::Event as XmlEvent};
use reqwest::Url;

/// Source-page identity is supplied by the containing PageContent. Byte ranges
/// refer to the original, lossless Markdown, not a rendered or rewritten copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReferenceSource {
    Markdown {
        bytes: Range<usize>,
        kind: String,
    },
    Relation {
        property_id: String,
        position: usize,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceDiagnostic {
    InvalidTarget,
    UnsupportedMention,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    pub source: ReferenceSource,
    pub raw_target: String,
    /// Syntactic identity only: existence, accessibility and crawl authority
    /// are not established by an extracted target.
    pub target: Option<LinkTarget>,
    pub diagnostic: Option<ReferenceDiagnostic>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relationships {
    /// Unique targets, first-occurrence order; suitable for indexed links.
    pub links: Vec<LinkTarget>,
    /// Every occurrence, including duplicates and unresolved references.
    pub references: Vec<Reference>,
}
fn target(raw: &str) -> Option<LinkTarget> {
    let url = Url::parse(raw).ok()?;
    if !matches!(url.scheme(), "https" | "http")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return None;
    }
    let host = url.host_str()?;
    let notion = host == "notion.so"
        || host.ends_with(".notion.so")
        || host == "notion.site"
        || host.ends_with(".notion.site")
        || host == "app.notion.com";
    if notion {
        let page_id = page_id(raw).ok()?.0;
        if let Some(fragment) = url.fragment().filter(|s| !s.is_empty()) {
            return Some(LinkTarget::Block {
                page_id,
                block_id: uuid(fragment)?,
            });
        }
        Some(LinkTarget::Page { page_id })
    } else {
        Some(LinkTarget::External {
            url: raw.to_owned(),
        })
    }
}
fn reference(
    source: ReferenceSource,
    raw_target: String,
    unsupported: bool,
    relation: bool,
) -> Reference {
    let target = if unsupported {
        None
    } else if relation {
        uuid(&raw_target).map(|page_id| LinkTarget::Page { page_id })
    } else {
        target(&raw_target)
    };
    let diagnostic = if unsupported {
        Some(ReferenceDiagnostic::UnsupportedMention)
    } else if target.is_none() {
        Some(ReferenceDiagnostic::InvalidTarget)
    } else {
        None
    };
    Reference {
        source,
        raw_target,
        target,
        diagnostic,
    }
}
struct EnhancedTag {
    bytes: Range<usize>,
    name: String,
    url: String,
    unsupported: bool,
}
// Scan only XML-shaped enhanced tags. Attribute parsing validates quoting and
// entities; this is not an HTML renderer and does not scrape arbitrary embeds.
fn enhanced(markdown: &str) -> Vec<EnhancedTag> {
    let mut tags = Vec::new();
    let mut cursor = 0;
    while let Some(relative) = markdown[cursor..].find('<') {
        let start = cursor + relative;
        let mut quote = None;
        let mut end = None;
        for (i, c) in markdown[start + 1..].char_indices() {
            match c {
                '\'' | '"' if quote == Some(c) => quote = None,
                '\'' | '"' if quote.is_none() => quote = Some(c),
                '>' if quote.is_none() => {
                    end = Some(start + i + 2);
                    break;
                }
                '\n' => break,
                _ => {}
            }
        }
        let end = end.unwrap_or_else(|| {
            markdown[start..]
                .find('\n')
                .map_or(markdown.len(), |n| start + n)
        });
        cursor = end;
        let raw = &markdown[start..end];
        let name = raw[1..]
            .split(|c: char| c.is_whitespace() || c == '/' || c == '>')
            .next()
            .unwrap_or("")
            .to_owned();
        if !matches!(name.as_str(), "page" | "database") && !name.starts_with("mention-") {
            continue;
        }
        if name == "mention-date" {
            continue;
        }
        let mut reader = Reader::from_str(raw);
        let Ok(XmlEvent::Start(tag) | XmlEvent::Empty(tag)) = reader.read_event() else {
            tags.push(EnhancedTag {
                bytes: start..end,
                unsupported: !matches!(name.as_str(), "page" | "mention-page"),
                name,
                url: raw.to_owned(),
            });
            continue;
        };
        let mut url = None;
        let mut malformed = false;
        for attribute in tag.attributes() {
            match attribute {
                Ok(attribute) if attribute.key.as_ref() == b"url" => {
                    match attribute.normalized_value(quick_xml::XmlVersion::Implicit1_0) {
                        Ok(value) => url = Some(value.into_owned()),
                        Err(_) => malformed = true,
                    }
                }
                Ok(_) => {}
                Err(_) => malformed = true,
            }
        }
        tags.push(EnhancedTag {
            bytes: start..end,
            unsupported: !matches!(name.as_str(), "page" | "mention-page"),
            name,
            url: if malformed {
                raw.to_owned()
            } else {
                url.unwrap_or_else(|| raw.to_owned())
            },
        });
    }
    tags
}
/// Extract graph-ready links and relation-property edges, retaining diagnostics.
/// Does not fetch targets, authorize traversal, persist a graph or hash content.
pub fn extract_relationships(content: &PageContent) -> Relationships {
    // Notion wrappers can make CommonMark treat their child text as an HTML
    // block. Mask enhanced tags (including closing/wrapper tags) with spaces,
    // preserving byte positions and newlines before parsing Markdown children.
    let tags = enhanced(&content.markdown);
    let mut masked = content.markdown.as_bytes().to_vec();
    let mut html_ranges = Vec::new();
    let mut ignored_ranges = Vec::new();
    for (event, range) in Parser::new_ext(&content.markdown, Options::all()).into_offset_iter() {
        if matches!(event, Event::Html(_) | Event::InlineHtml(_)) {
            html_ranges.push(range);
        }
    }
    // Comments can occur in the middle of a wrapper HTML event. Find their
    // actual spans, rather than assuming an event contains just one tag.
    let mut cursor = 0;
    while let Some(relative) = content.markdown[cursor..].find("<!--") {
        let start = cursor + relative;
        let end = content.markdown[start + 4..]
            .find("-->")
            .map_or(content.markdown.len(), |n| start + 4 + n + 3);
        if html_ranges
            .iter()
            .any(|range| range.start <= start && start < range.end)
        {
            for byte in &mut masked[start..end] {
                if *byte != b'\n' && *byte != b'\r' {
                    *byte = b' ';
                }
            }
            ignored_ranges.push(start..end);
        }
        cursor = end;
    }
    // Mask tag tokens within HTML events, leaving their Markdown children intact.
    for range in html_ranges {
        let mut inside = false;
        let mut quote = None;
        for byte in &mut masked[range] {
            let opening = *byte == b'<' && !inside;
            if opening {
                inside = true;
            }
            if inside {
                let value = *byte;
                if matches!(value, b'\'' | b'"') {
                    if quote == Some(value) {
                        quote = None;
                    } else if quote.is_none() {
                        quote = Some(value);
                    }
                }
                if value != b'\n' && value != b'\r' {
                    // A neutral text marker prevents same-line wrapper
                    // children becoming artificial indented code blocks.
                    *byte = if opening { b'x' } else { b' ' };
                }
                if value == b'>' && quote.is_none() {
                    inside = false;
                }
            }
        }
    }
    // Notion leading tabs encode physical children, not CommonMark indented
    // code. Quote markers preserve byte offsets while letting the parser see
    // nested child links and fenced code with its ordinary block semantics.
    for line in masked.split_mut(|byte| *byte == b'\n') {
        for byte in line.iter_mut().take_while(|byte| **byte == b'\t') {
            *byte = b'>';
        }
    }
    let masked = String::from_utf8(masked).expect("mask preserves UTF-8");
    let mut references = Vec::new();
    let mut code_ranges = ignored_ranges;
    let mut code_start = None;
    for (event, range) in Parser::new_ext(&masked, Options::all()).into_offset_iter() {
        match event {
            Event::Start(Tag::CodeBlock(_)) => code_start = Some(range.start),
            Event::End(TagEnd::CodeBlock) => {
                if let Some(start) = code_start.take() {
                    code_ranges.push(start..range.end);
                }
            }
            Event::Code(_) => code_ranges.push(range),
            Event::Start(Tag::Link { dest_url, .. } | Tag::Image { dest_url, .. }) => references
                .push(reference(
                    ReferenceSource::Markdown {
                        bytes: range,
                        kind: "link".into(),
                    },
                    dest_url.into_string(),
                    false,
                    false,
                )),
            _ => {}
        }
    }
    for tag in tags {
        let escaped = content.markdown[..tag.bytes.start]
            .bytes()
            .rev()
            .take_while(|b| *b == b'\\')
            .count()
            % 2
            == 1;
        if escaped
            || code_ranges
                .iter()
                .any(|r| r.start <= tag.bytes.start && tag.bytes.start < r.end)
        {
            continue;
        }
        references.push(reference(
            ReferenceSource::Markdown {
                bytes: tag.bytes,
                kind: tag.name,
            },
            tag.url,
            tag.unsupported,
            false,
        ));
    }
    references.sort_by_key(|r| match &r.source {
        ReferenceSource::Markdown { bytes, .. } => bytes.start,
        _ => 0,
    });
    for (property_id, value) in &content.page.properties {
        if let PropertyValue::PageIds(ids) = value {
            for (position, id) in ids.iter().enumerate() {
                references.push(reference(
                    ReferenceSource::Relation {
                        property_id: property_id.clone(),
                        position,
                    },
                    id.clone(),
                    false,
                    true,
                ));
            }
        }
    }
    let mut links = Vec::new();
    for target in references.iter().filter_map(|r| r.target.as_ref()) {
        if !links.contains(target) {
            links.push(target.clone());
        }
    }
    Relationships { links, references }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::backend::{Page, PageId};
    use std::collections::BTreeMap;
    const ID: &str = "12345678-1234-1234-1234-123456789abc";
    const RAW: &str = "12345678123412341234123456789ABC";
    fn content(markdown: String) -> PageContent {
        PageContent {
            page: Page {
                id: PageId(ID.into()),
                url: format!("https://notion.so/{ID}"),
                title: "Source".into(),
                last_edited_time: "2026-10-03T12:30:00Z".into(),
                archived: false,
                properties: BTreeMap::new(),
            },
            markdown,
        }
    }
    #[test]
    fn markdown_links_have_canonical_ids_block_targets_and_occurrence_provenance() {
        let source = content(format!(
            "[page](https://www.notion.so/Title-{RAW}?pvs=4)\n[block](https://notion.site/{RAW}#{RAW})\n[external][site]\n<https://example.test/auto>\n\n[site]: https://example.test/a?x=1&y=2\n[repeat](https://notion.so/{ID})"
        ));
        let report = extract_relationships(&source);
        assert_eq!(
            report.links,
            vec![
                LinkTarget::Page { page_id: ID.into() },
                LinkTarget::Block {
                    page_id: ID.into(),
                    block_id: ID.into()
                },
                LinkTarget::External {
                    url: "https://example.test/a?x=1&y=2".into()
                },
                LinkTarget::External {
                    url: "https://example.test/auto".into()
                }
            ]
        );
        assert_eq!(report.references.len(), 5);
        let ReferenceSource::Markdown { bytes, .. } = &report.references[0].source else {
            panic!()
        };
        assert!(source.markdown[bytes.clone()].starts_with("[page]"));
        assert_eq!(extract_relationships(&source), report);
    }
    #[test]
    fn enhanced_mentions_nested_children_and_relations_retain_distinct_provenance() {
        let mut source = content(format!(
            "<callout icon=\"🎯\">\n\t[child](https://example.test/child)\n\t<mention-page url=\"https://notion.so/{RAW}\">Page</mention-page>\n</callout>\n<page color=\"blue\" url='https://notion.so/{ID}'/>\n<mention-user url=\"user://person\">Ada</mention-user>\n<mention-database url=\"https://notion.so/{ID}\"/>"
        ));
        source.page.properties.insert(
            "stable-relation".into(),
            PropertyValue::PageIds(vec![RAW.into(), "broken".into(), ID.into()]),
        );
        let report = extract_relationships(&source);
        assert_eq!(
            report.links,
            vec![
                LinkTarget::External {
                    url: "https://example.test/child".into()
                },
                LinkTarget::Page { page_id: ID.into() }
            ]
        );
        assert_eq!(report.references.len(), 8);
        assert_eq!(
            report.references[3].diagnostic,
            Some(ReferenceDiagnostic::UnsupportedMention)
        );
        assert_eq!(
            report.references[4].diagnostic,
            Some(ReferenceDiagnostic::UnsupportedMention)
        );
        assert_eq!(
            report.references[5].source,
            ReferenceSource::Relation {
                property_id: "stable-relation".into(),
                position: 0
            }
        );
        assert_eq!(report.references[6].raw_target, "broken");
        assert_eq!(
            report.references[6].diagnostic,
            Some(ReferenceDiagnostic::InvalidTarget)
        );
    }
    #[test]
    fn code_and_escaped_markers_are_literals_while_unresolved_targets_remain_visible() {
        let source = content(format!(
            "`[literal](https://example.test/code)`\n`<mention-page url=\"https://notion.so/{ID}\"/>`\n```html\n<mention-page url=\"https://notion.so/{ID}\"/>\n[literal](https://example.test/fence)\n```\n\\<mention-page url=\"https://notion.so/{ID}\"/>\n\\[escaped](https://example.test/escaped)\n[broken](https://notion.so/not-an-id)\n[deceptive](https://notion.so.evil.test/{ID})\n[bad block](https://notion.so/{ID}#not-an-id)\n[relative](/unknown)\n<mention-page url=\"https://notion.so/broken\"/>\n<mention-page/>"
        ));
        let report = extract_relationships(&source);
        assert_eq!(report.references.len(), 6);
        assert_eq!(
            report.links,
            vec![LinkTarget::External {
                url: format!("https://notion.so.evil.test/{ID}")
            }]
        );
        assert_eq!(
            report
                .references
                .iter()
                .filter(|r| r.diagnostic == Some(ReferenceDiagnostic::InvalidTarget))
                .count(),
            5
        );
    }
    #[test]
    fn nested_fences_and_comments_do_not_create_relationships() {
        let source = content(format!(
            "<callout>\n\t```html\n\t<mention-page url=\"https://notion.so/{ID}\"/>\n\t[literal](https://example.test/code)\n\t```\n\t\t\t\t[deep child](https://example.test/child)\n</callout>\n<!-- <mention-page url=\"https://notion.so/{ID}\"/> [hidden](https://example.test/hidden) -->"
        ));
        let report = extract_relationships(&source);
        assert_eq!(
            report.links,
            vec![LinkTarget::External {
                url: "https://example.test/child".into()
            }]
        );
        assert_eq!(report.references.len(), 1);
    }

    #[test]
    fn same_line_wrapper_children_are_links_and_nested_comments_are_ignored() {
        let source = content(format!(
            "<callout>[child](https://example.test/child)</callout>\n<callout>\n<!-- <mention-page url=\"https://notion.so/{ID}\"/> [hidden](https://example.test/hidden) -->\n</callout>"
        ));
        let report = extract_relationships(&source);
        assert_eq!(
            report.links,
            vec![LinkTarget::External {
                url: "https://example.test/child".into()
            }]
        );
        assert_eq!(report.references.len(), 1);
        let ReferenceSource::Markdown { bytes, .. } = &report.references[0].source else {
            panic!()
        };
        assert_eq!(
            &source.markdown[bytes.clone()],
            "[child](https://example.test/child)"
        );
    }

    #[test]
    fn entities_and_malformed_attributes_are_not_silently_dropped() {
        let source = content(format!(
            "<mention-page url=\"https://notion.so/{ID}?x=1&amp;y=2\"/>\n<mention-page url=\"bad\" url=\"https://notion.so/{ID}\"/>\n<mention-date start=\"2026-10-03\"/>\n<mention-page url=\"unfinished"
        ));
        let report = extract_relationships(&source);
        assert_eq!(report.references.len(), 3);
        assert_eq!(
            report.references[0].target,
            Some(LinkTarget::Page { page_id: ID.into() })
        );
        assert_eq!(
            report.references[0].raw_target,
            format!("https://notion.so/{ID}?x=1&y=2")
        );
        assert!(report.references[1].target.is_none());
        assert!(report.references[1].raw_target.contains("url=\"bad\""));
        assert_eq!(
            report.references[2].diagnostic,
            Some(ReferenceDiagnostic::InvalidTarget)
        );
    }
}
