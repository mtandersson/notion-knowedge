//! Defensive formatting for operator-facing diagnostics.
//!
//! Never log raw request/response bodies, OAuth grants, or upstream errors.
//! This is a final safety net for short *diagnostic messages*, not permission
//! to log confidential payloads. Use it at stderr/logging boundaries.

const MARKER: &str = "[REDACTED]";

const SECRET_KEYS: &[&str] = &[
    "access_token",
    "refresh_token",
    "client_secret",
    "notion_token",
    "verification_token",
    "api_key",
    "x-api-key",
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "download_url",
    "signed_url",
    "file_url",
    "code_verifier",
    "state",
    "code",
    "x-amz-signature",
    "signature",
    "token",
];

fn starts_with_ci(value: &str, prefix: &str) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|start| start.eq_ignore_ascii_case(prefix))
}

fn boundary_before(text: &str, at: usize) -> bool {
    at == 0
        || text[..at]
            .chars()
            .next_back()
            .is_some_and(|ch| !ch.is_ascii_alphanumeric() && ch != '_')
}

fn opaque_end(text: &str, from: usize) -> usize {
    text[from..]
        .char_indices()
        .find(|(_, ch)| {
            ch.is_whitespace() || matches!(ch, '"' | '\'' | '<' | '>' | ',' | ';' | ')' | '}' | ']')
        })
        .map_or(text.len(), |(offset, _)| from + offset)
}

/// Remove URL capabilities, authentication schemes, known secret formats and
/// sensitive key/value fields, including JSON and header-like representations.
/// URLs are removed *whole*; retaining their path would leak private file IDs.
pub fn redact(message: &str) -> String {
    redact_with_secrets(message, &[])
}

/// Additionally remove credentials that lack any recognizable syntax, e.g.
/// operator-chosen webhook verification tokens. Empty strings are ignored.
pub fn redact_with_secrets(message: &str, secrets: &[&str]) -> String {
    let mut clean = message.to_owned();
    // Longest first avoids partial replacement of overlapping secret values.
    let mut known: Vec<_> = secrets.iter().copied().filter(|s| !s.is_empty()).collect();
    known.sort_unstable_by_key(|value| std::cmp::Reverse(value.len()));
    for value in known {
        clean = clean.replace(value, MARKER);
    }

    let mut output = String::with_capacity(clean.len());
    let mut index = 0;
    while index < clean.len() {
        let tail = &clean[index..];
        if starts_with_ci(tail, "https://") || starts_with_ci(tail, "http://") {
            // A URL can contain unescaped & and ;, so only stop at whitespace
            // or a closing string/HTML delimiter; never preserve its query.
            let end = clean[index..]
                .char_indices()
                .find(|(_, ch)| ch.is_whitespace() || matches!(ch, '"' | '\'' | '<' | '>'))
                .map_or(clean.len(), |(offset, _)| index + offset);
            output.push_str("[REDACTED_URL]");
            index = end;
            continue;
        }
        if boundary_before(&clean, index) {
            if let Some(scheme) = ["Bearer ", "Basic "]
                .iter()
                .copied()
                .find(|s| starts_with_ci(tail, s))
            {
                let start = index + scheme.len();
                let end = opaque_end(&clean, start);
                if end > start {
                    output.push_str(&clean[index..start]);
                    output.push_str(MARKER);
                    index = end;
                    continue;
                }
            }

            if starts_with_ci(tail, "secret_") || starts_with_ci(tail, "ntn_") {
                let end = opaque_end(&clean, index);
                if end > index {
                    output.push_str(MARKER);
                    index = end;
                    continue;
                }
            }

            let mut masked_field = false;
            for key in SECRET_KEYS {
                if !starts_with_ci(tail, key) {
                    continue;
                }
                let mut cursor = index + key.len();
                // Accept JSON's quoted key: "access_token": "...".
                if clean
                    .as_bytes()
                    .get(cursor)
                    .is_some_and(|b| *b == b'"' || *b == b'\'')
                {
                    cursor += 1;
                }
                while clean
                    .as_bytes()
                    .get(cursor)
                    .is_some_and(u8::is_ascii_whitespace)
                {
                    cursor += 1;
                }
                if !clean
                    .as_bytes()
                    .get(cursor)
                    .is_some_and(|b| *b == b':' || *b == b'=')
                {
                    continue;
                }
                cursor += 1;
                while clean
                    .as_bytes()
                    .get(cursor)
                    .is_some_and(u8::is_ascii_whitespace)
                {
                    cursor += 1;
                }
                let quote = match clean.as_bytes().get(cursor) {
                    Some(b'"') => Some('"'),
                    Some(b'\'') => Some('\''),
                    _ => None,
                };
                if quote.is_some() {
                    cursor += 1;
                }
                // Preserve the field name and delimiters, but never the value.
                let end = if matches!(*key, "authorization" | "proxy-authorization")
                    && (starts_with_ci(&clean[cursor..], "Bearer ")
                        || starts_with_ci(&clean[cursor..], "Basic "))
                {
                    let scheme_len = if starts_with_ci(&clean[cursor..], "Bearer ") {
                        "Bearer ".len()
                    } else {
                        "Basic ".len()
                    };
                    opaque_end(&clean, cursor + scheme_len)
                } else if let Some(quote) = quote {
                    clean[cursor..]
                        .find(quote)
                        .map_or(clean.len(), |offset| cursor + offset)
                } else {
                    opaque_end(&clean, cursor)
                };
                output.push_str(&clean[index..cursor]);
                output.push_str(MARKER);
                index = end;
                masked_field = true;
                break;
            }
            if masked_field {
                continue;
            }
        }
        let ch = tail.chars().next().expect("nonempty diagnostic tail");
        // Keep one log record per diagnostic; prevent newline injection.
        if ch == '\r' || ch == '\n' {
            output.push(' ');
        } else {
            output.push(ch);
        }
        index += ch.len_utf8();
    }
    output
}

/// Mask operator-configured secrets as well as common credentials and URLs.
/// Read the allowlist only; never dump the environment or its values to logs.
pub fn diagnostic(message: &str) -> String {
    let values: Vec<String> = [
        "NOTION_TOKEN",
        "NK_WEBHOOK_VERIFICATION_TOKEN",
        "NK_NOTION_OAUTH_CLIENT_SECRET",
    ]
    .iter()
    .filter_map(|name| std::env::var(name).ok())
    .filter(|value| !value.is_empty())
    .collect();
    let refs: Vec<&str> = values.iter().map(String::as_str).collect();
    redact_with_secrets(message, &refs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_notion_and_chatgpt_download_urls_are_removed_whole() {
        let message = "get https://s3.us-west-2.amazonaws.com/file?id=private&X-Amz-Signature=ABC;other=DEF and https://files.oaiusercontent.com/a/private?token=hello failed";
        let cleaned = redact(message);
        assert!(cleaned.contains("[REDACTED_URL]"));
        for secret in [
            "private",
            "ABC",
            "DEF",
            "hello",
            "s3.us-west",
            "files.oaiusercontent",
        ] {
            assert!(!cleaned.contains(secret), "{secret} leaked");
        }
    }

    #[test]
    fn authentication_headers_and_key_value_diagnostics_are_masked() {
        let message = "Authorization: Bearer tok-123, basic: Basic abc123, access_token=oauth456, refresh_token='refresh789' and \"client_secret\":\"client-xxx\"";
        let cleaned = redact(message);
        for secret in ["tok-123", "abc123", "oauth456", "refresh789", "client-xxx"] {
            assert!(!cleaned.contains(secret), "{secret} leaked");
        }
        assert!(cleaned.contains("Authorization: [REDACTED]"));
    }

    #[test]
    fn credential_prefixes_and_opaque_configured_tokens_are_masked() {
        let message = "notion ntn_long-token secret_old-token and arbitrary-opaque-key";
        let cleaned = redact_with_secrets(message, &["arbitrary-opaque-key", ""]);
        for secret in ["ntn_long-token", "secret_old-token", "arbitrary-opaque-key"] {
            assert!(!cleaned.contains(secret), "{secret} leaked");
        }
    }

    #[test]
    fn quoted_json_urls_callback_values_and_multiline_errors_are_safe() {
        let message = "{\"download_url\":\"https:\\/\\/example.com\\/signed?sig=opaque\", \"state\":\"STATECODE\", \"code\":\"ONETIME\"}\nnext line";
        let cleaned = redact(message);
        assert!(!cleaned.contains("opaque"));
        assert!(!cleaned.contains("STATECODE"));
        assert!(!cleaned.contains("ONETIME"));
        assert!(!cleaned.contains('\n'));
        assert!(cleaned.contains("next line"));
    }

    #[test]
    fn ordinary_errors_and_unicode_survive() {
        assert_eq!(
            redact("Timeout för sidan: försök igen"),
            "Timeout för sidan: försök igen"
        );
        assert_eq!(redact("tool failed: code 503"), "tool failed: code 503");
    }

    #[test]
    fn overlapping_known_secrets_do_not_leave_a_suffix() {
        assert_eq!(
            redact_with_secrets("credential=abcd1234", &["abcd", "abcd1234"]),
            "credential=[REDACTED]"
        );
    }
}
