//! Stable cache identities for Notion-hosted media, never download capabilities.
//! Fresh `read_content` responses deliberately retain current signed URLs.
use std::{borrow::Cow, ops::Range};

use notion_knowledge_core::backend::PageContent;
use pulldown_cmark::{Event, Parser, Tag};
use quick_xml::{Reader, events::Event as XmlEvent};
use reqwest::Url;

const AUTH_FIELDS: &[&str] = &[
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-SignedHeaders",
    "X-Amz-Security-Token",
    "X-Amz-Signature",
];

pub(crate) fn canonical_content(mut content: PageContent) -> PageContent {
    if let Cow::Owned(markdown) = canonical_markdown(&content.markdown) {
        content.markdown = markdown;
    }
    content
}

// Preserve raw path, retained query fields, fragment and encoding. URL parsing
// validates the provider boundary; serializing the URL would rewrite more bytes.
fn media_identity(raw: &str, html: bool) -> Option<String> {
    let decoded = if html {
        raw.replace("&amp;", "&")
    } else {
        raw.to_owned()
    };
    let url = Url::parse(&decoded).ok()?;
    if url.scheme() != "https"
        || url.host_str() != Some("prod-files-secure.s3.us-west-2.amazonaws.com")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
    {
        return None;
    }
    let segments: Vec<_> = url.path_segments()?.collect();
    if segments.len() != 3
        || crate::pages::uuid(segments[0]).is_none()
        || crate::pages::uuid(segments[1]).is_none()
        || segments[2].is_empty()
    {
        return None;
    }
    let fields: Vec<_> = url.query_pairs().collect();
    // Recognize a complete presigned request, including exactly one of every
    // mandatory auth field. Unknown/non-auth fields remain content-bearing.
    for key in AUTH_FIELDS
        .iter()
        .filter(|key| **key != "X-Amz-Security-Token")
    {
        let values: Vec<_> = fields
            .iter()
            .filter(|(name, _)| name.as_ref() == *key)
            .collect();
        if values.len() != 1 || values[0].1.is_empty() {
            return None;
        }
    }
    if !fields
        .iter()
        .any(|(key, value)| key == "X-Amz-Algorithm" && value == "AWS4-HMAC-SHA256")
    {
        return None;
    }
    let (before_fragment, fragment) = raw
        .split_once('#')
        .map_or((raw, None), |(a, b)| (a, Some(b)));
    let (base, query) = before_fragment.split_once('?')?;
    let separator = if html && query.contains("&amp;") {
        "&amp;"
    } else {
        "&"
    };
    let retained: Vec<_> = query
        .split(separator)
        .filter(|field| {
            let key = field.split_once('=').map_or(*field, |(key, _)| key);
            !AUTH_FIELDS.contains(&key)
        })
        .collect();
    // Encoded auth keys or mixed HTML separators are intentionally unsupported.
    if retained.iter().any(|field| {
        let key = field.split_once('=').map_or(*field, |(key, _)| key);
        key.contains('%') || (html && field.contains('&'))
    }) {
        return None;
    }
    let mut identity = base.to_owned();
    if !retained.is_empty() {
        identity.push('?');
        identity.push_str(&retained.join(separator));
    }
    if let Some(fragment) = fragment {
        identity.push('#');
        identity.push_str(fragment);
    }
    Some(identity)
}

fn canonical_markdown(markdown: &str) -> Cow<'_, str> {
    if !markdown.contains("X-Amz-") {
        return Cow::Borrowed(markdown);
    }
    let mut edits: Vec<(Range<usize>, String)> = Vec::new();
    let mut literal_tags = Vec::new();
    for (event, range) in Parser::new(markdown).into_offset_iter() {
        match event {
            Event::Start(Tag::Image { dest_url, .. }) if literal_tags.is_empty() => {
                // Only inline renderer forms, never reference definitions or a
                // matching URL in a caption/title. Unsupported forms stay exact.
                let source = &markdown[range.clone()];
                for (prefix, suffix) in [("(", ")"), ("(<", ">)")] {
                    let ending = format!("]{prefix}{dest_url}{suffix}");
                    if source.ends_with(&ending) {
                        if let Some(identity) = media_identity(&dest_url, false) {
                            let start = range.end - suffix.len() - dest_url.len();
                            edits.push((start..start + dest_url.len(), identity));
                        }
                        break;
                    }
                }
            }
            Event::Html(_) | Event::InlineHtml(_) => {
                let source = &markdown[range.clone()];
                let mut reader = Reader::from_str(source);
                let mut local_edits = Vec::new();
                loop {
                    match reader.read_event() {
                        Ok(XmlEvent::Start(tag) | XmlEvent::Empty(tag)) => {
                            let name =
                                String::from_utf8_lossy(tag.name().as_ref()).to_ascii_lowercase();
                            if matches!(name.as_str(), "pre" | "code" | "script" | "style") {
                                literal_tags.push(name);
                                continue;
                            }
                            if !literal_tags.is_empty()
                                || !matches!(
                                    name.as_str(),
                                    "image" | "file" | "pdf" | "audio" | "video"
                                )
                            {
                                continue;
                            }
                            for attribute in tag.attributes() {
                                let Ok(attribute) = attribute else {
                                    local_edits.clear();
                                    break;
                                };
                                if !matches!(attribute.key.as_ref(), b"src" | b"source") {
                                    continue;
                                }
                                // Reader borrows the original source. Only edit an
                                // exact borrowed value; no unsafe pointer access.
                                let Cow::Borrowed(bytes) = attribute.value else {
                                    continue;
                                };
                                let Some(offset) =
                                    (bytes.as_ptr() as usize).checked_sub(source.as_ptr() as usize)
                                else {
                                    continue;
                                };
                                if source.as_bytes().get(offset..offset + bytes.len())
                                    != Some(bytes)
                                {
                                    continue;
                                }
                                let Ok(raw) = std::str::from_utf8(bytes) else {
                                    continue;
                                };
                                if let Some(identity) = media_identity(raw, true) {
                                    local_edits.push((
                                        range.start + offset..range.start + offset + bytes.len(),
                                        identity,
                                    ));
                                }
                            }
                        }
                        Ok(XmlEvent::End(tag)) => {
                            if literal_tags.last().is_some_and(|last| {
                                last.as_bytes().eq_ignore_ascii_case(tag.name().as_ref())
                            }) {
                                literal_tags.pop();
                            }
                        }
                        Ok(XmlEvent::Eof) => break,
                        Err(_) => {
                            local_edits.clear();
                            break;
                        }
                        _ => {}
                    }
                }
                edits.extend(local_edits);
            }
            _ => {}
        }
    }
    edits.sort_by_key(|(range, _)| range.start);
    let mut result = String::new();
    let mut cursor = 0;
    for (range, identity) in edits {
        if range.start < cursor {
            continue;
        }
        result.push_str(&markdown[cursor..range.start]);
        result.push_str(&identity);
        cursor = range.end;
    }
    if cursor == 0 {
        return Cow::Borrowed(markdown);
    }
    result.push_str(&markdown[cursor..]);
    Cow::Owned(result)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const BASE: &str = "https://prod-files-secure.s3.us-west-2.amazonaws.com/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222/example.png";
    pub(crate) fn signed(generation: &str) -> String {
        format!(
            "{BASE}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={generation}%2Fscope&X-Amz-Date={generation}&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Security-Token={generation}&X-Amz-Signature={generation}&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&x-amz-checksum-mode=ENABLED&x-id=GetObject"
        )
    }
    pub(crate) fn body(generation: &str) -> String {
        let url = signed(generation);
        format!(
            "# Attachments\n\n![Image caption]({url})\n\n<file src=\"{url}\">Document caption</file>\n"
        )
    }

    #[test]
    fn media_identity_ignores_only_transport_authentication() {
        let first = canonical_markdown(&body("first")).into_owned();
        assert_eq!(first, canonical_markdown(&body("second")));
        assert!(!first.contains("X-Amz-Signature"));
        for preserved in [
            "Image caption",
            "Document caption",
            BASE,
            "X-Amz-Content-Sha256=UNSIGNED-PAYLOAD",
            "x-amz-checksum-mode=ENABLED",
            "x-id=GetObject",
        ] {
            assert!(first.contains(preserved), "{preserved}");
        }
        for changed in [
            body("second").replace("example.png", "different.png"),
            body("second").replace("Image caption", "Changed caption"),
            body("second").replace("x-id=GetObject", "versionId=changed"),
            body("second") + "\nChanged text",
        ] {
            assert_ne!(first, canonical_markdown(&changed));
        }
        // Canonical cache identities are idempotent, not expiring URLs.
        assert_eq!(first, canonical_markdown(&first));
    }

    #[test]
    fn literal_text_links_and_unrecognized_urls_remain_exact() {
        let url = signed("first");
        for text in [
            format!("[user link]({url})"),
            format!("{url}\n"),
            format!("`![code]({url})`"),
            format!("```markdown\n{}\n```", body("first")),
            format!("    ![indented code]({url})\n"),
            format!("<pre>\n![code]({url})\n</pre>"),
            format!("<code>![code]({url})</code>"),
            format!("<!-- {} -->", body("first")),
            format!("<file caption='src=\"{url}\"'>Literal</file>"),
            body("first").replace(
                "prod-files-secure.s3.us-west-2.amazonaws.com",
                "other.s3.us-west-2.amazonaws.com",
            ),
            body("first").replace(".com/", ".com.evil.example/"),
            body("first").replace("https://", "https://user@"),
            body("first").replace("example.png?", "example.png?X-Amz-Signature=duplicate&"),
            body("first").replace("X-Amz-Signature=first", "missing=signature"),
        ] {
            assert_eq!(canonical_markdown(&text), text);
        }
    }

    #[test]
    fn html_entities_and_other_attributes_keep_original_bytes() {
        let url = signed("first").replace('&', "&amp;");
        let text = format!(
            "<file caption='keep > this' source=\"{url}&amp;versionId=one%2Btwo#fragment\">Caption</file>"
        );
        let expected = format!(
            "<file caption='keep > this' source=\"{BASE}?X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&amp;x-amz-checksum-mode=ENABLED&amp;x-id=GetObject&amp;versionId=one%2Btwo#fragment\">Caption</file>"
        );
        assert_eq!(canonical_markdown(&text), expected);
    }
}
