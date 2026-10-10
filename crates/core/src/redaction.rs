//! Defense-in-depth redaction for diagnostic strings crossing a logging boundary.
//!
//! Prefer typed, allowlisted errors. Never pass request/response bodies, page
//! content or credentials to a logger in the first place. This sanitizer is a
//! final guard for accidentally included URLs and recognizable credentials.

const REDACTED_URL: &str = "[REDACTED_URL]";
const REDACTED_SECRET: &str = "[REDACTED_SECRET]";
const MAX_LOG_CHARS: usize = 4096;

const CREDENTIAL_KEYS: &[&str] = &[
    "authorization",
    "access_token",
    "refresh_token",
    "notion_token",
    "client_secret",
    "x-api-key",
    "api_key",
    "token",
    "secret",
];

fn has_prefix(value: &str, prefix: &str) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
}

fn terminates_url(character: char) -> bool {
    character.is_whitespace() || matches!(character, '"' | '\'' | '<' | '>')
}

fn terminates_value(character: char) -> bool {
    character.is_whitespace()
        || matches!(
            character,
            '"' | '\'' | '<' | '>' | ',' | ';' | '&' | ')' | ']' | '}'
        )
}

fn skip_secret_value(value: &str) -> Option<usize> {
    let first = value.chars().next()?;
    if first == '"' || first == '\'' {
        // Include the surrounding quote. Escaped quotes do not terminate JSON
        // string values (and are not emitted).
        let mut escaped = false;
        for (offset, ch) in value[first.len_utf8()..].char_indices() {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == first {
                return Some(first.len_utf8() + offset + ch.len_utf8());
            }
        }
        return Some(value.len());
    }

    let length = value.find(terminates_value).unwrap_or(value.len());
    (length != 0).then_some(length)
}

fn credential_length(value: &str, prev: Option<char>) -> Option<(usize, usize)> {
    if prev.is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-') {
        return None;
    }

    // Header-style Bearer credentials permit spaces or tabs as separators.
    if has_prefix(value, "bearer") && value.as_bytes().get(6).is_some_and(u8::is_ascii_whitespace) {
        let token_start = 6 + value[6..].len() - value[6..].trim_start().len();
        return skip_secret_value(&value[token_start..]).map(|n| (token_start + n, token_start));
    }

    for key in CREDENTIAL_KEYS {
        if !has_prefix(value, key) {
            continue;
        }
        let mut next = key.len();
        // Both "token=value" and JSON '"token": "value"' are supported.
        if value
            .as_bytes()
            .get(next)
            .is_some_and(|ch| *ch == b'"' || *ch == b'\'')
        {
            next += 1;
        }
        while value
            .as_bytes()
            .get(next)
            .is_some_and(u8::is_ascii_whitespace)
        {
            next += 1;
        }
        if !value
            .as_bytes()
            .get(next)
            .is_some_and(|ch| *ch == b':' || *ch == b'=')
        {
            continue;
        }
        next += 1;
        while value
            .as_bytes()
            .get(next)
            .is_some_and(u8::is_ascii_whitespace)
        {
            next += 1;
        }
        // Preserve the credential key for actionable configuration diagnostics.
        let value_start = next;
        if *key == "authorization" {
            // Unquoted header values contain an auth scheme and a credential.
            // Quoted values are consumed in full by skip_secret_value below.
            let suffix = &value[next..];
            if let Some(separator) = suffix.find(char::is_whitespace)
                && !suffix.starts_with(['"', '\''])
                && suffix[..separator]
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
            {
                next += separator;
                next += value[next..].len() - value[next..].trim_start().len();
            }
        }
        if let Some(length) = skip_secret_value(&value[next..]) {
            return Some((next + length, value_start));
        }
    }
    None
}

fn secret_prefix_length(value: &str, prev: Option<char>) -> Option<usize> {
    if prev.is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return None;
    }
    if !["ntn_", "secret_", "sk-"]
        .iter()
        .any(|prefix| has_prefix(value, prefix))
    {
        return None;
    }
    let len = value
        .find(|ch: char| !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.')))
        .unwrap_or(value.len());
    (len > 4).then_some(len)
}

/// Render arbitrary diagnostic text without complete HTTP(S) URLs, credential
/// fields or recognizable Notion/OpenAI token forms. Never use this function
/// to make unsafe raw payload logging acceptable; unknown token formats and
/// structured private content must not be logged at all.
///
/// Output is character-bounded so an upstream error cannot create unbounded
/// diagnostic output. Original UTF-8 text outside redacted spans is preserved.
pub fn redact_for_log(input: &str) -> String {
    let mut output = String::new();
    let mut index = 0;
    let mut last = None;
    let mut emitted = 0;

    while index < input.len() && emitted < MAX_LOG_CHARS {
        let value = &input[index..];
        let replacement = if has_prefix(value, "https://") || has_prefix(value, "http://") {
            let length = value.find(terminates_url).unwrap_or(value.len());
            Some((length, 0, REDACTED_URL))
        } else if let Some((length, prefix)) = credential_length(value, last) {
            Some((length, prefix, REDACTED_SECRET))
        } else {
            secret_prefix_length(value, last).map(|length| (length, 0, REDACTED_SECRET))
        };
        if let Some((length, prefix, substitute)) = replacement {
            // Avoid accepting an empty URL and getting stuck on malformed text.
            if length > 0 {
                for ch in value[..prefix].chars().chain(substitute.chars()) {
                    if emitted == MAX_LOG_CHARS {
                        break;
                    }
                    output.push(ch);
                    emitted += 1;
                }
                last = value[..length].chars().next_back();
                index += length;
                continue;
            }
        }
        let character = value.chars().next().expect("nonempty UTF-8 slice");
        output.push(character);
        emitted += 1;
        index += character.len_utf8();
        last = Some(character);
    }
    if index < input.len() {
        output.push_str("[TRUNCATED]");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::redact_for_log;

    #[test]
    fn removes_notion_and_bearer_secrets_from_diagnostics() {
        let log = "NOTION_TOKEN=secret_examplevalue authorization: Bearer ntn_examplevalue";
        let safe = redact_for_log(log);
        assert!(!safe.contains("examplevalue"));
        assert!(!safe.contains("secret_examplevalue"));
        assert!(!safe.contains("ntn_examplevalue"));
        assert!(safe.contains("[REDACTED_SECRET]"));
    }

    #[test]
    fn preserves_setting_names_and_removes_header_credentials() {
        let safe = redact_for_log("NOTION_TOKEN: invalid value");
        assert!(safe.contains("NOTION_TOKEN"));
        assert!(!safe.contains("invalid"));
        for input in [
            "Authorization: Basic c3ludGhldGljOmNyZWRlbnRpYWw=",
            "Authorization: Bearer\tfixture-credential",
            "Bearer\tfixture-credential",
            "Authorization: \"Basic fixture-credential\"",
        ] {
            let safe = redact_for_log(input);
            assert!(!safe.contains("fixture-credential"));
            assert!(!safe.contains("c3ludGhldGlj"));
            assert!(safe.contains("[REDACTED_SECRET]"));
        }
    }

    #[test]
    fn removes_json_grants_and_query_key_values() {
        let log = r#"{"access_token":"test-access-token","refresh_token":"test-refresh-token","client_secret":"test-client-secret"} token=another-token"#;
        let safe = redact_for_log(log);
        for secret in [
            "test-access-token",
            "test-refresh-token",
            "test-client-secret",
            "another-token",
        ] {
            assert!(!safe.contains(secret));
        }
    }

    #[test]
    fn removes_complete_chatgpt_and_notion_signed_urls() {
        let log = "download https://files.oaiusercontent.com/private/image.png?token=chatgpt-signature; notion https://prod-files-secure.s3.us-west-2.amazonaws.com/x?X-Amz-Signature=notion-signature";
        let safe = redact_for_log(log);
        assert!(!safe.contains("chatgpt-signature"));
        assert!(!safe.contains("notion-signature"));
        assert!(!safe.contains("files.oaiusercontent.com"));
        assert!(!safe.contains("prod-files-secure"));
        assert_eq!(safe.matches("[REDACTED_URL]").count(), 2);
    }

    #[test]
    fn preserves_nonsecret_unicode_and_bounds_the_output() {
        assert_eq!(
            redact_for_log("Sökning misslyckades: timeout"),
            "Sökning misslyckades: timeout"
        );
        let output = redact_for_log(&"x".repeat(5000));
        assert!(output.ends_with("[TRUNCATED]"));
        assert_eq!(output.len(), 4096 + "[TRUNCATED]".len());
        let padded = format!("token={}private-value trailing", " ".repeat(5000));
        let output = redact_for_log(&padded);
        assert!(output.chars().count() <= 4096 + "[TRUNCATED]".len());
        assert!(!output.contains("private-value"));
    }
}
