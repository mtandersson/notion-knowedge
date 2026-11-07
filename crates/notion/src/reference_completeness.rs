//! Classify copied source placeholders without resolving or following them.
use quick_xml::{Reader, events::Event};

const MAX_REFERENCES: usize = 128;

#[derive(Clone, Copy)]
struct Reference {
    body_start: usize,
    name: &'static str,
}

fn reference_name(name: &[u8]) -> Option<&'static str> {
    match name {
        b"synced_block_reference" => Some("synced_block_reference"),
        b"synced-block-reference" => Some("synced-block-reference"),
        _ => None,
    }
}

fn has_inline_body(mut body: &str) -> bool {
    loop {
        body = body.trim();
        if body.is_empty() {
            return false;
        }
        let Some(comment) = body.strip_prefix("<!--") else {
            return true;
        };
        let Some((_, rest)) = comment.split_once("-->") else {
            return false;
        };
        body = rest;
    }
}

/// Empty copied markup does not establish that its original is empty or even
/// readable. Keep it unavailable rather than publishing an unverified complete
/// page. Original synced blocks and ordinary empty pages remain valid.
pub(crate) fn unverifiable(markdown: &str) -> bool {
    if !markdown.contains("synced_block_reference") && !markdown.contains("synced-block-reference")
    {
        return false;
    }
    let mut literals = crate::links::literal_ranges(markdown);
    literals.sort_by_key(|range| range.start);
    let mut literal_index = 0;
    let mut pending: Vec<Reference> = Vec::new();
    let mut literal_tags: Vec<String> = Vec::new();
    let mut count = 0;
    let mut cursor = 0;
    while let Some(relative) = markdown[cursor..].find('<') {
        let start = cursor + relative;
        // Script/style bodies are raw text: embedded tag-looking strings must
        // not alter the literal state or swallow their actual closing tag.
        if let Some(name) = literal_tags
            .last()
            .filter(|name| matches!(name.as_str(), "script" | "style"))
        {
            if let Some(rest) = markdown[start..].strip_prefix("</") {
                let length = rest
                    .find(|ch: char| ch.is_whitespace() || ch == '>')
                    .unwrap_or(rest.len());
                if rest[..length].eq_ignore_ascii_case(name) {
                    let suffix = &rest[length..];
                    let trimmed = suffix.trim_start();
                    if trimmed.starts_with('>') {
                        cursor = start + 2 + length + suffix.len() - trimmed.len() + 1;
                        literal_tags.pop();
                        continue;
                    }
                }
            }
            cursor = start + 1;
            continue;
        }
        // Match existing Markdown escapes and ignore comments/code/fences,
        // including fences nested inside enhanced wrappers.
        let escaped = markdown[..start]
            .bytes()
            .rev()
            .take_while(|byte| *byte == b'\\')
            .count()
            % 2
            == 1;
        while literal_index < literals.len() && literals[literal_index].end <= start {
            literal_index += 1;
        }
        if escaped
            || literals
                .get(literal_index)
                .is_some_and(|range| range.contains(&start))
        {
            cursor = start + 1;
            continue;
        }
        let mut quote = None;
        let mut end = None;
        for (offset, ch) in markdown[start + 1..].char_indices() {
            match ch {
                '\'' | '"' if quote == Some(ch) => quote = None,
                '\'' | '"' if quote.is_none() => quote = Some(ch),
                '>' if quote.is_none() => {
                    end = Some(start + offset + 2);
                    break;
                }
                _ => {}
            }
        }
        let raw_end = end.unwrap_or(markdown.len());
        let raw = &markdown[start..raw_end];
        let name = raw[1..]
            .trim_start_matches('/')
            .split(|ch: char| ch.is_whitespace() || ch == '/' || ch == '>')
            .next()
            .unwrap_or("");
        let relevant = reference_name(name.as_bytes()).is_some();
        if end.is_none() {
            return relevant || !pending.is_empty();
        }
        cursor = raw_end;
        let mut reader = Reader::from_str(raw);
        reader.config_mut().check_end_names = false;
        reader.config_mut().allow_unmatched_ends = true;
        match reader.read_event() {
            Ok(Event::Start(tag) | Event::Empty(tag)) => {
                let tag_name = String::from_utf8_lossy(tag.name().as_ref()).to_ascii_lowercase();
                if matches!(tag_name.as_str(), "code" | "pre" | "script" | "style") {
                    if !raw.ends_with("/>") {
                        if literal_tags.len() == MAX_REFERENCES {
                            return true;
                        }
                        literal_tags.push(tag_name);
                    }
                    continue;
                }
                if !literal_tags.is_empty() {
                    continue;
                }
                let Some(name) = reference_name(tag.name().as_ref()) else {
                    continue;
                };
                count += 1;
                if count > MAX_REFERENCES || raw.ends_with("/>") {
                    return true;
                }
                let mut url = None;
                for attribute in tag.attributes() {
                    let Ok(attribute) = attribute else {
                        return true;
                    };
                    let Ok(value) = attribute.normalized_value(quick_xml::XmlVersion::Implicit1_0)
                    else {
                        return true;
                    };
                    if attribute.key.as_ref() == b"url" {
                        url = Some(value.into_owned());
                    } else if attribute.key.as_ref() == b"notice" && !value.trim().is_empty() {
                        return true;
                    }
                }
                if url.is_none_or(|url| {
                    reqwest::Url::parse(&url).is_err() || crate::pages::page_id(&url).is_err()
                }) {
                    return true;
                }
                pending.push(Reference {
                    body_start: raw_end,
                    name,
                });
            }
            Ok(Event::End(tag)) => {
                let tag_name = String::from_utf8_lossy(tag.name().as_ref()).to_ascii_lowercase();
                if literal_tags.last() == Some(&tag_name) {
                    literal_tags.pop();
                    continue;
                }
                if !literal_tags.is_empty() {
                    continue;
                }
                if let Some(name) = reference_name(tag.name().as_ref()) {
                    let Some(reference) = pending.pop() else {
                        return true;
                    };
                    if name != reference.name
                        || !has_inline_body(&markdown[reference.body_start..start])
                    {
                        return true;
                    }
                }
            }
            Err(_) if relevant && literal_tags.is_empty() => return true,
            Err(_) => cursor = start + 1,
            _ => {}
        }
    }
    !pending.is_empty()
}
