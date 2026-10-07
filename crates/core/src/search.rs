//! Provider-independent search ports. Indexed scope is adapter-owned.
use serde::Serialize;
use std::{future::Future, pin::Pin};

#[derive(Debug, Clone)]
pub struct SemanticQuery {
    pub query: String,
    pub limit: usize,
    pub page_ids: Option<Vec<String>>,
    pub root_page_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct LexicalQuery {
    pub query: String,
    pub limit: usize,
    pub page_ids: Option<Vec<String>>,
    pub root_page_ids: Option<Vec<String>>,
}
#[derive(Debug, Clone, Serialize)]
pub struct SearchSource {
    pub last_edited_time: String,
    pub page_id: String,
    pub chunk_id: String,
    pub url: String,
    pub title: String,
    pub heading_path: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_id: Option<String>,
}
#[derive(Debug, Clone, Serialize)]
pub struct SearchHit {
    pub text: String,
    pub score: f32,
    pub source: SearchSource,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub matched_paths: Vec<RetrievalPath>,
}
#[derive(Debug, Clone, Copy)]
pub struct SearchUnavailable;
pub type SearchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<SearchHit>, SearchUnavailable>> + Send + 'a>>;
pub trait SemanticSearch: Send + Sync {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_>;
}

pub trait LexicalSearch: Send + Sync {
    fn search(&self, query: LexicalQuery) -> SearchFuture<'_>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrievalPath {
    Semantic,
    Lexical,
}

pub trait HybridSearch: Send + Sync {
    fn search(&self, query: SemanticQuery) -> SearchFuture<'_>;
}

/// Redact URL-bearing tokens before taking a Unicode prefix. Link targets are
/// unnecessary in snippets: the stable page citation is provided separately.
/// All URL targets are omitted, including public links: signatures can live in
/// paths as well as query parameters, so a provider denylist cannot guarantee
/// safety. Markdown labels and stable source citations remain available.
pub fn snippet(text: &str, limit: usize) -> String {
    let mut output = String::new();
    for token in text.split_inclusive(char::is_whitespace) {
        let mut remaining = token;
        while let Some(start) = ["http://", "https://", "//"]
            .iter()
            .filter_map(|prefix| remaining.to_ascii_lowercase().find(prefix))
            .min()
        {
            let prefix = &remaining[..start];
            let decoded_prefix = decode_percent(prefix).to_ascii_lowercase();
            if decoded_prefix.contains("http:")
                || decoded_prefix.contains("https:")
                || decoded_prefix.contains("//")
            {
                output.push_str("[link omitted]");
            } else {
                output.push_str(prefix);
            }
            let end = remaining[start..]
                .find(|c: char| c.is_whitespace() || matches!(c, ')' | ']' | '>' | '\'' | '"'))
                .map_or(remaining.len(), |offset| start + offset);
            output.push_str("[link omitted]");
            remaining = &remaining[end..];
        }
        let decoded = decode_percent(remaining)
            .to_ascii_lowercase()
            .replace("&amp;", "&");
        if decoded.contains("http:") || decoded.contains("https:") || decoded.contains("//") {
            output.push_str("[link omitted]");
            if remaining.ends_with(char::is_whitespace) {
                output.push(' ');
            }
        } else {
            output.push_str(remaining);
        }
    }
    output.chars().take(limit).collect()
}

fn decode_percent(value: &str) -> String {
    let mut bytes = Vec::new();
    let mut input = value.as_bytes().iter().copied().peekable();
    while let Some(byte) = input.next() {
        if byte == b'%' {
            let first = input.next();
            let second = input.next();
            if let (Some(a), Some(b)) = (first, second)
                && let (Some(a), Some(b)) = ((a as char).to_digit(16), (b as char).to_digit(16))
            {
                bytes.push((a * 16 + b) as u8);
                continue;
            }
            bytes.push(byte);
            bytes.extend(first);
            bytes.extend(second);
        } else {
            bytes.push(byte);
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}
#[cfg(test)]
mod snippet_tests {
    use super::snippet;
    #[test]
    fn url_targets_are_removed_before_unicode_bounding() {
        for url in [
            "https://files.example/a?X-Amz-Signature=secret",
            "HTTPS://example/a?sig=secret",
            "//files.example/a?sig=secret",
            "https://res.cloudinary.com/demo/image/authenticated/s--secret--/sample",
            "https%3A%2F%2Fexample/a?signature=secret",
            "%48%54%54%50%53%3A%2F%2Fexample/a?%58%2D%41%4D%5A%2D%53%49%47%4E%41%54%55%52%45=secret",
        ] {
            for text in [
                url.to_string(),
                format!("![attachment]({url})"),
                format!("< {url} >"),
            ] {
                assert!(!snippet(&text, 2000).contains("secret"));
            }
        }
        assert_eq!(
            snippet(
                "[public](https://example/public)[file](https://files/a?sig=secret)",
                2000
            ),
            "[public]([link omitted])[file]([link omitted])"
        );
        assert_eq!(
            snippet(
                "[a](https://files/a?sig=one)[b](https://files/b?sig=two)",
                2000
            ),
            "[a]([link omitted])[b]([link omitted])"
        );
        assert!(
            !snippet(
                "[a](https%3A%2F%2Ffiles.example/a?sig=secret)[b](https://example/public)",
                2000
            )
            .contains("secret")
        );
        let long = format!(
            "Before [file](https://example/{}?sig=secret) after",
            "x".repeat(3000)
        );
        assert_eq!(snippet(&long, 2000), "Before [file]([link omitted]) after");
        assert_eq!(snippet("åäö😀plain", 4), "åäö😀");
        assert_eq!(snippet("https://example/secret?sig=secret", 4), "[lin");
        assert_eq!(
            snippet("See https://example/public now", 2000),
            "See [link omitted] now"
        );
    }
}
