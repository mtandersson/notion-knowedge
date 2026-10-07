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
/// Ordinary public links remain; recognizable credential-bearing targets are
/// removed in full, including their path, before truncation.
pub fn snippet(text: &str, limit: usize) -> String {
    let mut output = String::new();
    for token in text.split_inclusive(char::is_whitespace) {
        let mut remaining = token;
        while let Some(start) = remaining.to_ascii_lowercase().find("http") {
            output.push_str(&remaining[..start]);
            let end = remaining[start..]
                .find(|c: char| c.is_whitespace() || matches!(c, ')' | ']' | '>' | '\'' | '"'))
                .map_or(remaining.len(), |offset| start + offset);
            let raw = &remaining[start..end];
            let decoded = decode_percent(raw)
                .to_ascii_lowercase()
                .replace("&amp;", "&");
            output.push_str(if signed_target(&decoded) {
                "[link omitted]"
            } else {
                raw
            });
            remaining = &remaining[end..];
        }
        let decoded = decode_percent(remaining)
            .to_ascii_lowercase()
            .replace("&amp;", "&");
        if signed_target(&decoded) {
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
fn signed_target(token: &str) -> bool {
    let suspicious = |key: &str| {
        key.starts_with("x-amz-")
            || key.starts_with("x-goog-")
            || matches!(
                key,
                "signature"
                    | "sig"
                    | "awsaccesskeyid"
                    | "googleaccessid"
                    | "token"
                    | "access_token"
            )
    };
    if let Some(start) = token.find("http") {
        let raw = token[start..].trim_end_matches([')', ']', '>', '\'', '"']);
        if let Ok(url) = url::Url::parse(raw)
            && url
                .query_pairs()
                .any(|(key, _)| suspicious(&key.to_ascii_lowercase()))
        {
            return true;
        }
    }
    // Malformed or encoded suspicious URLs must not escape redaction.
    token
        .split(['?', '&', ';'])
        .any(|part| suspicious(part.split('=').next().unwrap_or("")))
}

#[cfg(test)]
mod snippet_tests {
    use super::snippet;
    #[test]
    fn url_targets_are_removed_before_unicode_bounding() {
        for url in [
            "https://files.example/a?X-Amz-Signature=secret",
            "HTTPS://example/a?sig=secret",
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
            "[public](https://example/public)[file]([link omitted])"
        );
        assert_eq!(
            snippet(
                "[a](https://files/a?sig=one)[b](https://files/b?sig=two)",
                2000
            ),
            "[a]([link omitted])[b]([link omitted])"
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
            "See https://example/public now"
        );
    }
}
